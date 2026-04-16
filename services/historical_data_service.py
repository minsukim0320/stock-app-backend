"""
과거 특정 날짜 기준의 데이터를 수집하는 서비스
백테스팅 시뮬레이션용 - 해당 시점에 존재했던 데이터만 사용 (look-ahead bias 방지)
"""
import asyncio
import threading
import pandas as pd
import yfinance as yf
import requests
from datetime import datetime, timedelta
from typing import Optional
from services.news_utils import deduplicate_news, format_news_for_prompt
from services.sec_edgar_service import get_filing_map, match_column_to_report_date

# 매크로 지표 티커
MACRO_TICKERS = {
    "VIX":    "^VIX",
    "TNX":    "^TNX",
    "KRW=X":  "KRW=X",
    "GLD":    "GLD",
    "CL=F":   "CL=F",
    "BZ=F":   "BZ=F",
    "DX=F":   "DX-Y.NYB",
    "SPY":    "SPY",
}


_yf_lock = threading.Lock()


def _yf_download(ticker, start: str, end: str, timeout_sec: int = 30) -> pd.DataFrame:
    """yfinance download with timeout protection, MultiIndex flattening, and thread-safety lock"""
    # 단일 문자열 → 리스트 변환 (yfinance 1.2+ 단일 문자열 호출 시 TypeError 방지)
    tickers_arg = [ticker] if isinstance(ticker, str) else list(ticker)
    is_single = len(tickers_arg) == 1
    try:
        with _yf_lock:
            df = yf.download(tickers_arg, start=start, end=end,
                             progress=False, auto_adjust=True, timeout=timeout_sec)
        if df.empty:
            return df
        # MultiIndex 평탄화 (단일 티커 리스트일 때)
        if isinstance(df.columns, pd.MultiIndex) and is_single:
            df.columns = df.columns.droplevel(1)
        return df
    except Exception as e:
        print(f"[ERROR] yf.download({tickers_arg}, {start}~{end}) failed: {e}")
        return pd.DataFrame()


def _safe_float(val) -> float:
    """pandas scalar/Series → float 안전 변환"""
    if hasattr(val, "item"):
        return float(val.item())
    return float(val)


def _nearest_close(ticker: str, target_date: str) -> Optional[float]:
    """target_date 전일(직전 거래일) 종가 반환 — look-ahead bias 방지"""
    try:
        dt = datetime.strptime(target_date, "%Y-%m-%d")
        start = (dt - timedelta(days=14)).strftime("%Y-%m-%d")
        end   = target_date  # target_date 미포함 (전일까지만)
        df = _yf_download(ticker, start, end)
        if df.empty:
            print(f"[WARN] No price data for {ticker} before {target_date}")
            return None
        filtered = df[df.index < dt].dropna()
        if filtered.empty:
            return None
        close_val = filtered["Close"].iloc[-1]
        return round(_safe_float(close_val), 4)
    except Exception as e:
        print(f"[ERROR] _nearest_close({ticker}, {target_date}): {e}")
        return None


def _batch_nearest_closes(tickers, target_date: str) -> dict:
    """
    여러 종목의 종가를 단일 yf.download 배치 호출로 수집 (thread-safe).
    tickers: dict {label: yf_ticker} (매크로용) 또는 list[str] (종목용)
    """
    if isinstance(tickers, dict):
        labels = list(tickers.keys())
        yf_tickers = list(tickers.values())
    else:
        labels = list(tickers)
        yf_tickers = list(tickers)

    if not yf_tickers:
        return {}

    dt = datetime.strptime(target_date, "%Y-%m-%d")
    start = (dt - timedelta(days=14)).strftime("%Y-%m-%d")
    end = target_date  # target_date 미포함 (전일까지만 — look-ahead bias 방지)

    try:
        with _yf_lock:
            df = yf.download(yf_tickers, start=start, end=end,
                             progress=False, auto_adjust=True, timeout=30)
    except Exception as e:
        print(f"[ERROR] batch yf.download({yf_tickers}) failed: {e}")
        df = pd.DataFrame()

    if df.empty:
        # 배치 실패 → 개별 fallback
        print(f"[WARN] Batch close download empty — falling back to individual downloads")
        result = {}
        for label, yf_ticker in zip(labels, yf_tickers):
            result[label] = _nearest_close(yf_ticker, target_date)
        return result

    is_multi = isinstance(df.columns, pd.MultiIndex)
    result = {}
    for label, yf_ticker in zip(labels, yf_tickers):
        try:
            if len(yf_tickers) == 1 and not is_multi:
                close_series = df["Close"]
            else:
                close_series = df[("Close", yf_ticker)]

            filtered = close_series[close_series.index < dt].dropna()
            if filtered.empty:
                result[label] = None
            else:
                result[label] = round(_safe_float(filtered.iloc[-1]), 4)
        except Exception as e:
            print(f"[WARN] batch close extraction failed for {label}/{yf_ticker}: {e}")
            result[label] = None

    # 배치 결과가 모두 None이면 → 개별 다운로드 fallback (Render 등 배치 차단 대응)
    if result and all(v is None for v in result.values()):
        print(f"[WARN] Batch download returned all None — falling back to individual downloads")
        for label, yf_ticker in zip(labels, yf_tickers):
            result[label] = _nearest_close(yf_ticker, target_date)

    return result


def _batch_historical_charts(tickers: list, target_date: str, months: int = 3) -> dict:
    """
    여러 종목의 OHLCV 차트를 단일 yf.download 배치 호출로 수집 (thread-safe).
    """
    if not tickers:
        return {}

    dt = datetime.strptime(target_date, "%Y-%m-%d")
    end = target_date  # target_date 미포함 (전일까지만 — look-ahead bias 방지)
    start = (dt - timedelta(days=months * 31)).strftime("%Y-%m-%d")

    try:
        with _yf_lock:
            df = yf.download(list(tickers), start=start, end=end,
                             progress=False, auto_adjust=True, timeout=60)
    except Exception as e:
        print(f"[ERROR] batch chart yf.download({tickers}) failed: {e}")
        df = pd.DataFrame()

    if df.empty:
        # 배치 실패 → 개별 fallback
        print(f"[WARN] Batch chart download empty — falling back to individual downloads")
        result = {}
        for ticker in tickers:
            result[ticker] = get_historical_chart(ticker, target_date, months)
        return result

    is_multi = isinstance(df.columns, pd.MultiIndex)
    result = {}
    for ticker in tickers:
        try:
            if len(tickers) == 1 and not is_multi:
                ticker_df = df
            else:
                ticker_df = df.xs(ticker, level=1, axis=1) if is_multi else df

            chart = []
            for date, row in ticker_df.iterrows():
                try:
                    chart.append({
                        "date":   date.strftime("%Y-%m-%d"),
                        "open":   round(_safe_float(row["Open"]), 2),
                        "high":   round(_safe_float(row["High"]), 2),
                        "low":    round(_safe_float(row["Low"]), 2),
                        "close":  round(_safe_float(row["Close"]), 2),
                        "volume": int(_safe_float(row["Volume"])),
                    })
                except Exception:
                    continue
            result[ticker] = chart
        except Exception as e:
            print(f"[WARN] batch chart extraction failed for {ticker}: {e}")
            result[ticker] = []

    # 모든 차트가 비어있으면 → 개별 다운로드 fallback (Render 등 배치 차단 대응)
    if result and all(not v for v in result.values()):
        print(f"[WARN] Batch chart download empty — falling back to individual downloads")
        for ticker in tickers:
            result[ticker] = get_historical_chart(ticker, target_date, months)

    return result


def get_historical_macro(target_date: str) -> dict:
    """target_date 기준 매크로 지표 수집"""
    result = {}
    for key, ticker in MACRO_TICKERS.items():
        price = _nearest_close(ticker, target_date)
        if price is not None:
            result[key] = price
    return result


def get_historical_price(ticker: str, target_date: str) -> Optional[float]:
    """특정 날짜 기준 종가"""
    return _nearest_close(ticker, target_date)


def get_historical_prices(tickers: list[str], target_date: str) -> dict:
    """여러 종목 과거 가격 일괄 조회"""
    return {t: _nearest_close(t, target_date) for t in tickers}


def get_historical_chart(ticker: str, target_date: str, months: int = 3) -> list[dict]:
    """target_date 기준 이전 months개월 OHLCV 차트 (look-ahead bias 방지)"""
    try:
        dt  = datetime.strptime(target_date, "%Y-%m-%d")
        end = target_date  # target_date 미포함 (전일까지만)
        start = (dt - timedelta(days=months * 31)).strftime("%Y-%m-%d")
        df = _yf_download(ticker, start, end)
        if df.empty:
            print(f"[WARN] No chart data for {ticker} ({start}~{end})")
            return []
        result = []
        for date, row in df.iterrows():
            try:
                result.append({
                    "date":   date.strftime("%Y-%m-%d"),
                    "open":   round(_safe_float(row["Open"]), 2),
                    "high":   round(_safe_float(row["High"]), 2),
                    "low":    round(_safe_float(row["Low"]), 2),
                    "close":  round(_safe_float(row["Close"]), 2),
                    "volume": int(_safe_float(row["Volume"])),
                })
            except Exception as e:
                print(f"[WARN] Chart row parse error for {ticker} on {date}: {e}")
                continue
        return result
    except Exception as e:
        print(f"[ERROR] get_historical_chart({ticker}, {target_date}): {e}")
        return []


def _get_row(df, keys):
    """재무제표에서 여러 후보 키 중 존재하는 첫 번째 row 반환"""
    if df is None or df.empty:
        return None
    for k in keys:
        if k in df.index:
            return df.loc[k]
    return None


def _val_at(row, col):
    """row에서 col(분기) 값을 float로 — 실패 시 None"""
    if row is None or col is None:
        return None
    try:
        v = row[col]
        if v is None or pd.isna(v):
            return None
        return float(v)
    except Exception:
        return None


def get_historical_fundamentals(ticker: str, target_date: str) -> dict:
    """
    target_date 이전 가장 최근 분기 재무제표 기반 펀더멘털 (실제 과거값 계산).
    PE/PB는 target_date 직전 종가 + 분기 재무제표로 재계산 — look-ahead bias 방지.
    """
    try:
        with _yf_lock:
            stock = yf.Ticker(ticker)
            try:
                qf = stock.quarterly_financials  # income statement
            except Exception:
                qf = None
            try:
                qb = stock.quarterly_balance_sheet
            except Exception:
                qb = None
            try:
                af = stock.financials  # annual income statement (YoY fallback용)
            except Exception:
                af = None

        dt = datetime.strptime(target_date, "%Y-%m-%d")

        # SEC EDGAR에서 target_date 이전에 실제 공시된 10-Q/10-K만 조회
        # → 미국 외 종목이면 빈 dict → 빈 펀더멘털로 조기 반환
        filing_map = get_filing_map(ticker, target_date)
        if not filing_map:
            print(f"[INFO] No SEC filings before {target_date} for {ticker} — fundamentals skipped (non-US or no prior filing)")
            return {
                "ticker": ticker.upper(),
                "_note": f"no_sec_filings_before_{target_date}",
            }

        # yfinance의 분기 컬럼(period-end Timestamp) → SEC reportDate 매칭
        # 매칭 실패 시 "공시된 적 없는 분기"로 간주하고 제외
        # filingDate 최신순으로 정렬해 latest_f/latest_b 결정
        def past_cols(df):
            if df is None or df.empty:
                return []
            matched = []
            for c in df.columns:
                rd = match_column_to_report_date(c, filing_map)
                if rd:
                    matched.append((c, filing_map[rd]))  # (column, filingDate)
            matched.sort(key=lambda x: x[1], reverse=True)
            return [c for (c, _) in matched]

        f_cols = past_cols(qf)
        b_cols = past_cols(qb)
        latest_f = f_cols[0] if f_cols else None
        latest_b = b_cols[0] if b_cols else None

        # 참고용: 사용한 최신 분기의 실제 공시일
        latest_filing_date = None
        if latest_b is not None:
            rd = match_column_to_report_date(latest_b, filing_map)
            latest_filing_date = filing_map.get(rd) if rd else None

        # ── 손익계산서 항목 ──
        net_income   = _get_row(qf, ["Net Income", "Net Income Common Stockholders"])
        revenue      = _get_row(qf, ["Total Revenue", "Operating Revenue"])
        gross_profit = _get_row(qf, ["Gross Profit"])
        op_income    = _get_row(qf, ["Operating Income", "Total Operating Income As Reported"])

        # ── 재무상태표 항목 ──
        total_assets      = _get_row(qb, ["Total Assets"])
        total_liabilities = _get_row(qb, ["Total Liabilities Net Minority Interest"])
        equity            = _get_row(qb, ["Stockholders Equity", "Common Stock Equity", "Total Equity Gross Minority Interest"])
        total_debt        = _get_row(qb, ["Total Debt"])
        current_assets    = _get_row(qb, ["Current Assets"])
        current_liab      = _get_row(qb, ["Current Liabilities"])
        shares_out        = _get_row(qb, ["Ordinary Shares Number", "Share Issued"])

        def r(v, dec=2):
            return round(v, dec) if v is not None else None

        # ── 4분기 합산(TTM) — 4개 분기 Net Income / Revenue ──
        def ttm(row):
            if row is None or len(f_cols) < 1:
                return None
            cols4 = f_cols[:4]
            try:
                vals = [float(row[c]) for c in cols4 if c in row.index and pd.notna(row[c])]
                return sum(vals) if vals else None
            except Exception:
                return None

        ttm_net_income = ttm(net_income)
        ttm_revenue    = ttm(revenue)
        ttm_gross      = ttm(gross_profit)
        ttm_op         = ttm(op_income)

        # 최신 분기 값
        q_shares = _val_at(shares_out, latest_b)
        q_equity = _val_at(equity, latest_b)
        q_assets = _val_at(total_assets, latest_b)
        q_debt   = _val_at(total_debt, latest_b)
        q_ca     = _val_at(current_assets, latest_b)
        q_cl     = _val_at(current_liab, latest_b)

        # ── 주가 기반 지표: target_date 직전 거래일 종가 사용 ──
        price = _nearest_close(ticker, target_date)
        market_cap = None
        if price and q_shares and q_shares > 0:
            market_cap = price * q_shares

        # EPS (TTM)
        trailing_eps = None
        if ttm_net_income is not None and q_shares and q_shares > 0:
            trailing_eps = ttm_net_income / q_shares

        # PE = price / EPS
        trailing_pe = None
        if price and trailing_eps and trailing_eps > 0:
            trailing_pe = price / trailing_eps

        # PB = price / (equity / shares)
        price_to_book = None
        if price and q_equity and q_shares and q_shares > 0:
            bvps = q_equity / q_shares
            if bvps > 0:
                price_to_book = price / bvps

        # ROE = TTM NI / equity
        roe = None
        if ttm_net_income is not None and q_equity and q_equity > 0:
            roe = ttm_net_income / q_equity * 100

        # ROA = TTM NI / total assets
        roa = None
        if ttm_net_income is not None and q_assets and q_assets > 0:
            roa = ttm_net_income / q_assets * 100

        # D/E = total debt / equity
        debt_to_equity = None
        if q_debt is not None and q_equity and q_equity > 0:
            debt_to_equity = q_debt / q_equity * 100  # 백분율

        # Current ratio
        current_ratio = None
        if q_ca and q_cl and q_cl > 0:
            current_ratio = q_ca / q_cl

        # Margins (TTM)
        gross_margins     = (ttm_gross / ttm_revenue * 100) if (ttm_gross and ttm_revenue) else None
        operating_margins = (ttm_op / ttm_revenue * 100)    if (ttm_op and ttm_revenue)    else None
        profit_margins    = (ttm_net_income / ttm_revenue * 100) if (ttm_net_income and ttm_revenue) else None

        # YoY 성장률 (최신분기 vs 4분기 전) — 부족 시 연간 재무제표로 fallback
        def yoy(row):
            if row is None or len(f_cols) < 5:
                return None
            cur = _val_at(row, f_cols[0])
            prv = _val_at(row, f_cols[4])
            if cur is None or prv is None or prv == 0:
                return None
            return (cur - prv) / abs(prv) * 100

        def yoy_annual(key_list):
            a_cols = past_cols(af)
            if len(a_cols) < 2:
                return None
            row = _get_row(af, key_list)
            if row is None:
                return None
            cur = _val_at(row, a_cols[0])
            prv = _val_at(row, a_cols[1])
            if cur is None or prv is None or prv == 0:
                return None
            return (cur - prv) / abs(prv) * 100

        revenue_growth  = yoy(revenue)  or yoy_annual(["Total Revenue", "Operating Revenue"])
        earnings_growth = yoy(net_income) or yoy_annual(["Net Income", "Net Income Common Stockholders"])

        return {
            "ticker":            ticker.upper(),
            "trailing_pe":       r(trailing_pe),
            "forward_pe":        None,  # 과거 시점의 forward EPS 추정치는 접근 불가
            "peg_ratio":         None,
            "price_to_book":     r(price_to_book),
            "trailing_eps":      r(trailing_eps),
            "forward_eps":       None,
            "revenue_growth":    r(revenue_growth),
            "earnings_growth":   r(earnings_growth),
            "gross_margins":     r(gross_margins),
            "operating_margins": r(operating_margins),
            "profit_margins":    r(profit_margins),
            "debt_to_equity":    r(debt_to_equity),
            "current_ratio":     r(current_ratio),
            "return_on_equity":  r(roe),
            "return_on_assets":  r(roa),
            "short_percent":     None,  # 과거 short interest는 yfinance에서 제공 안 됨
            "market_cap":        r(market_cap, 0),
            "_note":             f"computed_from_sec_filings_before_{target_date}",
            "_report_date":      latest_b.strftime("%Y-%m-%d") if latest_b else None,
            "_filed_date":       latest_filing_date,
        }
    except Exception as e:
        print(f"[WARN] Fundamentals failed for {ticker}: {e}")
        return {"ticker": ticker.upper()}


def get_historical_news(ticker: str, target_date: str, finnhub_api_key: str, days_before: int = 14) -> list[dict]:
    """Finnhub으로 target_date 이전 영어 뉴스 조회 (D 당일 제외 — look-ahead bias 방지)"""
    if not finnhub_api_key:
        print(f"[WARN] Finnhub API key empty — skipping news for {ticker}")
        return []
    try:
        dt   = datetime.strptime(target_date, "%Y-%m-%d")
        frm  = (dt - timedelta(days=days_before)).strftime("%Y-%m-%d")
        to   = (dt - timedelta(days=1)).strftime("%Y-%m-%d")  # D-1까지만
        clean_ticker = ticker.replace("=", "").replace("^", "")
        url = "https://finnhub.io/api/v1/company-news"
        params = {
            "symbol": clean_ticker,
            "from":   frm,
            "to":     to,
            "token":  finnhub_api_key,
        }
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 429:
            print(f"[WARN] Finnhub rate limit (429) for {ticker} — retrying after 3s")
            import time
            time.sleep(3)
            resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        raw = resp.json()
        if not isinstance(raw, list):
            print(f"[WARN] Finnhub returned non-list for {ticker}: {type(raw).__name__}")
            return []
        articles = raw
        print(f"[INFO] Finnhub news for {ticker}: {len(articles)} articles")
        result = []
        for a in articles[:30]:
            result.append({
                "title":        a.get("headline", ""),
                "summary":      a.get("summary", ""),
                "url":          a.get("url", ""),
                "source":       a.get("source", ""),
                "published_at": a.get("datetime", ""),
            })
        deduped = deduplicate_news(result)
        return format_news_for_prompt(deduped[:20])
    except Exception as e:
        print(f"[ERROR] Finnhub news failed for {ticker}: {e}")
        return []


def _serpapi_news_search(query: str, target_date: str, gl: str, hl: str, serpapi_key: str, days_before: int = 14) -> list[dict]:
    """
    SerpAPI Google News (tbm=nws) — tbs 파라미터로 날짜 범위 필터링.
    target_date 이전 days_before일 ~ D-1일까지 뉴스만 반환 (look-ahead bias 방지).
    """
    if not serpapi_key:
        return []
    try:
        dt = datetime.strptime(target_date, "%Y-%m-%d")
        cd_min = (dt - timedelta(days=days_before)).strftime("%m/%d/%Y")
        cd_max = (dt - timedelta(days=1)).strftime("%m/%d/%Y")  # D-1까지만
        params = {
            "engine": "google",
            "tbm": "nws",
            "q": query,
            "tbs": f"cdr:1,cd_min:{cd_min},cd_max:{cd_max}",
            "gl": gl,
            "hl": hl,
            "num": 50,
            "api_key": serpapi_key,
        }
        resp = requests.get("https://serpapi.com/search", params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        articles = data.get("news_results", [])
        raw = []
        for a in articles[:50]:
            raw.append({
                "title": a.get("title", ""),
                "summary": a.get("snippet", ""),
                "url": a.get("link", ""),
                "source": a.get("source", "") if isinstance(a.get("source"), str) else a.get("source", {}).get("name", ""),
                "published_at": a.get("date", ""),
            })
        deduped = deduplicate_news(raw)
        return format_news_for_prompt(deduped[:30])
    except Exception as e:
        print(f"[ERROR] SerpAPI news search failed ({query[:30]}): {e}")
        return []


def get_historical_international_news(target_date: str, serpapi_key: str = "", **_kwargs) -> list[dict]:
    """SerpAPI로 과거 국제 경제/금융 뉴스 조회 (날짜 범위 필터 적용)"""
    return _serpapi_news_search(
        query="global economy finance stock market",
        target_date=target_date,
        gl="us",
        hl="en",
        serpapi_key=serpapi_key,
    )


def get_historical_korean_politics_news(target_date: str, serpapi_key: str = "") -> list[dict]:
    """SerpAPI로 과거 한국 경제/정세 뉴스 조회 (날짜 범위 필터 적용)"""
    return _serpapi_news_search(
        query="한국 경제 금융 증권",
        target_date=target_date,
        gl="kr",
        hl="ko",
        serpapi_key=serpapi_key,
    )


async def get_full_historical_context(
    tickers: list[str],
    target_date: str,
    finnhub_api_key: str = "",
    serpapi_key: str = "",
) -> dict:
    """
    target_date 기준 전체 컨텍스트 수집.
    yf.download 배치 호출로 thread-safety 확보 + 성능 최적화.
    """
    has_finnhub = bool(finnhub_api_key)
    if not has_finnhub:
        print(f"[WARN] No Finnhub API key provided — news will be skipped for all tickers")

    # ── Phase 1: 배치 yf.download (매크로 + 종목가격 + 차트) ──────────
    # 각 배치 함수가 내부적으로 _yf_lock을 사용하므로 순차 실행되지만,
    # asyncio.to_thread로 감싸서 이벤트 루프를 블로킹하지 않음
    macro_future = asyncio.to_thread(_batch_nearest_closes, MACRO_TICKERS, target_date)
    prices_future = asyncio.to_thread(_batch_nearest_closes, tickers, target_date)
    charts_future = asyncio.to_thread(_batch_historical_charts, tickers, target_date, 3)

    batch_results = await asyncio.gather(
        macro_future, prices_future, charts_future,
        return_exceptions=True,
    )

    # 매크로
    if isinstance(batch_results[0], Exception):
        print(f"[ERROR] Macro batch failed: {batch_results[0]}")
        macro = {}
    else:
        macro = {k: v for k, v in batch_results[0].items() if v is not None}

    # 종목 가격
    if isinstance(batch_results[1], Exception):
        print(f"[ERROR] Prices batch failed: {batch_results[1]}")
        prices = {}
    else:
        prices = {k: v for k, v in batch_results[1].items() if v is not None}
        for t in tickers:
            if t not in prices:
                print(f"[WARN] No price for {t} on {target_date}")

    # 차트
    if isinstance(batch_results[2], Exception):
        print(f"[ERROR] Charts batch failed: {batch_results[2]}")
        charts = {t: [] for t in tickers}
    else:
        charts = batch_results[2]

    print(f"[INFO] Macro collected: {list(macro.keys())} ({len(macro)}/{len(MACRO_TICKERS)})")
    print(f"[INFO] Prices: {len(prices)}/{len(tickers)}, "
          f"Charts: {sum(1 for c in charts.values() if c)}/{len(tickers)}")

    # ── Phase 2: 펀더멘털 (yf.Ticker — Lock 보호, 순차) ──────────────
    fundamentals = {}
    for ticker in tickers:
        try:
            fund = await asyncio.to_thread(get_historical_fundamentals, ticker, target_date)
            fundamentals[ticker] = fund
        except Exception as e:
            print(f"[ERROR] Fundamentals for {ticker}: {e}")
            fundamentals[ticker] = {"ticker": ticker.upper()}

    # ── Phase 3: Finnhub 뉴스 (순차, rate limit 보호) ─────────────────
    news = {t: [] for t in tickers}
    if has_finnhub:
        for ticker in tickers:
            try:
                nl = await asyncio.to_thread(
                    get_historical_news, ticker, target_date, finnhub_api_key
                )
                news[ticker] = nl
                await asyncio.sleep(0.5)  # rate limit 보호
            except Exception as e:
                print(f"[ERROR] News fetch for {ticker}: {e}")
                news[ticker] = []

    # ── Phase 4: SerpAPI 뉴스 (국제 정세 + 한국 정세) ─────────────────
    intl_news = []
    korean_politics = []
    has_serpapi = bool(serpapi_key)

    if has_serpapi:
        try:
            intl_news = await asyncio.to_thread(
                get_historical_international_news, target_date, serpapi_key
            )
            print(f"[INFO] Historical intl news: {len(intl_news)} articles")
        except Exception as e:
            print(f"[ERROR] International news fetch failed: {e}")

        await asyncio.sleep(0.5)  # SerpAPI rate limit 보호

        try:
            korean_politics = await asyncio.to_thread(
                get_historical_korean_politics_news, target_date, serpapi_key
            )
            print(f"[INFO] Historical Korean politics news: {len(korean_politics)} articles")
        except Exception as e:
            print(f"[ERROR] Korean politics news fetch failed: {e}")
    else:
        print("[WARN] SerpAPI key not provided — skipping intl & Korean politics news")

    return {
        "target_date":      target_date,
        "macro":            macro,
        "prices":           prices,
        "charts":           charts,
        "fundamentals":     fundamentals,
        "news":             news,
        "intl_news":        intl_news,
        "korean_politics":  korean_politics,
    }
