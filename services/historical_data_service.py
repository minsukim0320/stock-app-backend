"""
과거 특정 날짜 기준의 데이터를 수집하는 서비스
백테스팅 시뮬레이션용 - 해당 시점에 존재했던 데이터만 사용 (look-ahead bias 방지)
"""
import asyncio
import os
import threading
import pandas as pd
import yfinance as yf
import requests
from datetime import datetime, timedelta
from typing import Optional
from dotenv import load_dotenv
from services.news_utils import deduplicate_news, format_news_for_prompt

load_dotenv()
SERPAPI_KEY = os.getenv("SERPAPI_KEY", "")

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


def get_historical_fundamentals(ticker: str, target_date: str) -> dict:
    """target_date 이전 가장 최근 분기 재무제표 기반 펀더멘털"""
    try:
        with _yf_lock:
            stock = yf.Ticker(ticker)
            info  = stock.info
            try:
                qf = stock.quarterly_financials
            except Exception:
                qf = None
        dt = datetime.strptime(target_date, "%Y-%m-%d")

        trailing_eps = None
        try:
            if qf is not None and not qf.empty:
                past_cols = [c for c in qf.columns if c.to_pydatetime().replace(tzinfo=None) <= dt]
                if past_cols:
                    latest = max(past_cols)
                    net_income = qf.loc["Net Income", latest] if "Net Income" in qf.index else None
                    shares = info.get("sharesOutstanding")
                    if net_income and shares and shares > 0:
                        trailing_eps = round(float(net_income) / float(shares), 2)
        except Exception:
            pass

        def safe(key, scale=1, decimals=2):
            val = info.get(key)
            if val is None or not isinstance(val, (int, float)):
                return None
            return round(val * scale, decimals)

        return {
            "ticker":            ticker.upper(),
            "trailing_pe":       safe("trailingPE"),
            "forward_pe":        safe("forwardPE"),
            "peg_ratio":         safe("pegRatio"),
            "price_to_book":     safe("priceToBook"),
            "trailing_eps":      trailing_eps or safe("trailingEps"),
            "forward_eps":       safe("forwardEps"),
            "revenue_growth":    safe("revenueGrowth",    scale=100),
            "earnings_growth":   safe("earningsGrowth",   scale=100),
            "gross_margins":     safe("grossMargins",     scale=100),
            "operating_margins": safe("operatingMargins", scale=100),
            "profit_margins":    safe("profitMargins",    scale=100),
            "debt_to_equity":    safe("debtToEquity"),
            "current_ratio":     safe("currentRatio"),
            "return_on_equity":  safe("returnOnEquity",   scale=100),
            "return_on_assets":  safe("returnOnAssets",   scale=100),
            "short_percent":     safe("shortPercentOfFloat", scale=100),
            "market_cap":        info.get("marketCap"),
            "_note": "fundamentals_approximate_current_values",
        }
    except Exception as e:
        print(f"[WARN] Fundamentals failed for {ticker}: {e}")
        return {"ticker": ticker.upper()}


def get_historical_news(ticker: str, target_date: str, finnhub_api_key: str, days_before: int = 14) -> list[dict]:
    """Finnhub으로 target_date 전후 영어 뉴스 조회"""
    if not finnhub_api_key:
        print(f"[WARN] Finnhub API key empty — skipping news for {ticker}")
        return []
    try:
        dt   = datetime.strptime(target_date, "%Y-%m-%d")
        frm  = (dt - timedelta(days=days_before)).strftime("%Y-%m-%d")
        to   = dt.strftime("%Y-%m-%d")
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


def _serpapi_news_search(query: str, target_date: str, gl: str, hl: str, days_before: int = 14) -> list[dict]:
    """
    SerpAPI Google News (tbm=nws) — tbs 파라미터로 날짜 범위 필터링.
    target_date 이전 days_before일 ~ target_date 사이의 뉴스만 반환.
    """
    if not SERPAPI_KEY:
        return []
    try:
        dt = datetime.strptime(target_date, "%Y-%m-%d")
        cd_min = (dt - timedelta(days=days_before)).strftime("%m/%d/%Y")
        cd_max = dt.strftime("%m/%d/%Y")
        params = {
            "engine": "google",
            "tbm": "nws",
            "q": query,
            "tbs": f"cdr:1,cd_min:{cd_min},cd_max:{cd_max}",
            "gl": gl,
            "hl": hl,
            "num": 50,
            "api_key": SERPAPI_KEY,
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


def get_historical_international_news(target_date: str, **_kwargs) -> list[dict]:
    """SerpAPI로 과거 국제 경제/금융 뉴스 조회 (날짜 범위 필터 적용)"""
    return _serpapi_news_search(
        query="global economy finance stock market",
        target_date=target_date,
        gl="us",
        hl="en",
    )


def get_historical_korean_politics_news(target_date: str) -> list[dict]:
    """SerpAPI로 과거 한국 경제/정세 뉴스 조회 (날짜 범위 필터 적용)"""
    return _serpapi_news_search(
        query="한국 경제 금융 증권",
        target_date=target_date,
        gl="kr",
        hl="ko",
    )


async def get_full_historical_context(
    tickers: list[str],
    target_date: str,
    finnhub_api_key: str = "",
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
    has_serpapi = bool(SERPAPI_KEY)

    if has_serpapi:
        try:
            intl_news = await asyncio.to_thread(
                get_historical_international_news, target_date
            )
            print(f"[INFO] Historical intl news: {len(intl_news)} articles")
        except Exception as e:
            print(f"[ERROR] International news fetch failed: {e}")

        await asyncio.sleep(0.5)  # SerpAPI rate limit 보호

        try:
            korean_politics = await asyncio.to_thread(
                get_historical_korean_politics_news, target_date
            )
            print(f"[INFO] Historical Korean politics news: {len(korean_politics)} articles")
        except Exception as e:
            print(f"[ERROR] Korean politics news fetch failed: {e}")
    else:
        print("[WARN] SERPAPI_KEY not set — skipping intl & Korean politics news")

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
