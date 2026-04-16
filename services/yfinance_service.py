import yfinance as yf
import pandas as pd
from services.news_utils import deduplicate_news, format_news_for_prompt

ECONOMY_KEYWORDS = [
    "stock", "market", "economy", "economic", "finance", "financial",
    "earnings", "revenue", "profit", "loss", "invest", "fund", "trade",
    "gdp", "inflation", "fed", "bank", "interest rate", "bond", "etf",
    "dividend", "nasdaq", "s&p", "shares", "quarter", "fiscal", "growth",
]


def get_stock_price(ticker: str) -> dict:
    """
    단일 종목 현재가 + 전일 대비. Yahoo가 /quoteSummary(stock.info/fast_info)를
    rate-limit하는 경우가 잦으므로 yf.download(period=5d)를 우선 사용.
    fast_info는 통화 정보 보강용으로만 best-effort 사용.
    """
    stock = yf.Ticker(ticker)

    # 1) yf.download는 chart API를 써서 rate-limit에 더 강함
    last_price = None
    prev_close = None
    try:
        df = yf.download([ticker], period="5d", progress=False,
                         auto_adjust=True, timeout=20)
        if not df.empty:
            if isinstance(df.columns, pd.MultiIndex):
                close = df["Close"][ticker].dropna()
            else:
                close = df["Close"].dropna()
            if not close.empty:
                last_price = float(close.iloc[-1])
                prev_close = float(close.iloc[-2]) if len(close) >= 2 else last_price
    except Exception as e:
        # 아래 fallback에서 재시도
        pass

    # 2) yf.download 실패 시 Ticker.history fallback
    if last_price is None:
        try:
            hist = stock.history(period="5d")
            if not hist.empty:
                last_price = float(hist["Close"].iloc[-1])
                prev_close = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else last_price
        except Exception:
            pass

    if last_price is None:
        raise Exception(f"{ticker}: 주가 데이터를 가져올 수 없습니다")

    # 3) 통화 — fast_info best-effort (실패해도 USD 가정)
    currency = "USD"
    try:
        info = stock.fast_info
        currency = getattr(info, "currency", None) or "USD"
    except Exception:
        pass

    change = last_price - prev_close
    change_pct = (change / prev_close * 100) if prev_close else 0

    return {
        "ticker": ticker.upper(),
        "price": round(last_price, 2),
        "change": round(change, 2),
        "change_percent": round(change_pct, 2),
        "currency": currency,
        "delay_minutes": None,  # 정확한 지연 측정은 별도 1m history 호출이 필요해 제거
    }


def get_stock_prices_batch(tickers: list[str]) -> dict:
    """
    여러 종목의 현재가를 yf.download 단일 배치로 수집 — 개별 호출 대비 10배+ 빠름.
    반환: {ticker: {price, change, change_percent, currency}}
    실패 종목은 결과에 포함되지 않음.
    """
    if not tickers:
        return {}
    clean = [t for t in tickers if t]
    try:
        df = yf.download(clean, period="5d", progress=False,
                         auto_adjust=True, timeout=30)
    except Exception as e:
        raise Exception(f"배치 가격 조회 실패: {e}")

    if df.empty:
        return {}

    is_multi = isinstance(df.columns, pd.MultiIndex)
    result = {}
    for t in clean:
        try:
            if is_multi:
                if t not in df["Close"].columns:
                    continue
                series = df["Close"][t].dropna()
            else:
                series = df["Close"].dropna()
            if series.empty:
                continue
            last = float(series.iloc[-1])
            prev = float(series.iloc[-2]) if len(series) >= 2 else last
            change = last - prev
            change_pct = (change / prev * 100) if prev else 0
            result[t.upper()] = {
                "ticker": t.upper(),
                "price": round(last, 2),
                "change": round(change, 2),
                "change_percent": round(change_pct, 2),
                "currency": "USD",
                "delay_minutes": None,
            }
        except Exception:
            continue
    return result


def get_chart_data(ticker: str, period: str = "1mo") -> list[dict]:
    stock = yf.Ticker(ticker)
    hist = stock.history(period=period)
    if hist.empty:
        raise Exception(f"{ticker}: 차트 데이터가 비어있습니다 (period={period})")
    result = []
    for date, row in hist.iterrows():
        result.append({
            "date": date.strftime("%Y-%m-%d"),
            "open": round(row["Open"], 2),
            "high": round(row["High"], 2),
            "low": round(row["Low"], 2),
            "close": round(row["Close"], 2),
            "volume": int(row["Volume"]),
        })
    return result


def get_english_news(ticker: str, limit: int = 40) -> list[dict]:
    stock = yf.Ticker(ticker)
    raw_news = stock.news or []
    raw = []
    for item in raw_news:
        content = item.get("content", {})
        title = content.get("title", "")
        summary = content.get("summary", "")
        combined = (title + " " + summary).lower()
        if not any(kw in combined for kw in ECONOMY_KEYWORDS):
            continue
        raw.append({
            "title": title,
            "summary": summary,
            "url": content.get("canonicalUrl", {}).get("url", ""),
            "source": content.get("provider", {}).get("displayName", ""),
            "published_at": content.get("pubDate", ""),
        })
    deduped = deduplicate_news(raw)
    return format_news_for_prompt(deduped[:limit])


def _get_row(df, keys):
    if df is None or df.empty:
        return None
    for k in keys:
        if k in df.index:
            return df.loc[k]
    return None


def _val_at(row, col):
    if row is None or col is None:
        return None
    try:
        v = row[col]
        if v is None or pd.isna(v):
            return None
        return float(v)
    except Exception:
        return None


def get_fundamentals(ticker: str) -> dict:
    """
    현재 시점 펀더멘털 — 분기 재무제표 + 최근 종가 기반으로 직접 계산.
    stock.info는 Yahoo가 자주 rate-limit(YFRateLimitError)하므로
    과거 시뮬레이션과 동일한 방식으로 통일해 안정성 확보.
    """
    stock = yf.Ticker(ticker)
    try:
        qf = stock.quarterly_financials
    except Exception:
        qf = None
    try:
        qb = stock.quarterly_balance_sheet
    except Exception:
        qb = None
    try:
        af = stock.financials
    except Exception:
        af = None

    # 최신 종가 — 5d history로 안전 조회 (fast_info는 불안정)
    try:
        hist = stock.history(period="5d")
        price = float(hist["Close"].iloc[-1]) if not hist.empty else None
    except Exception:
        price = None

    # 분기/연간 컬럼을 최신순으로
    def cols(df):
        if df is None or df.empty:
            return []
        return sorted(df.columns, reverse=True)

    f_cols = cols(qf)
    b_cols = cols(qb)
    a_cols = cols(af)
    latest_b = b_cols[0] if b_cols else None

    # ── 손익계산서 ──
    net_income   = _get_row(qf, ["Net Income", "Net Income Common Stockholders"])
    revenue      = _get_row(qf, ["Total Revenue", "Operating Revenue"])
    gross_profit = _get_row(qf, ["Gross Profit"])
    op_income    = _get_row(qf, ["Operating Income", "Total Operating Income As Reported"])

    # ── 재무상태표 ──
    total_assets   = _get_row(qb, ["Total Assets"])
    equity         = _get_row(qb, ["Stockholders Equity", "Common Stock Equity", "Total Equity Gross Minority Interest"])
    total_debt     = _get_row(qb, ["Total Debt"])
    current_assets = _get_row(qb, ["Current Assets"])
    current_liab   = _get_row(qb, ["Current Liabilities"])
    shares_out     = _get_row(qb, ["Ordinary Shares Number", "Share Issued"])

    def r(v, dec=2):
        return round(v, dec) if v is not None else None

    # TTM (최근 4분기 합산)
    def ttm(row):
        if row is None or not f_cols:
            return None
        try:
            vals = [float(row[c]) for c in f_cols[:4]
                    if c in row.index and pd.notna(row[c])]
            return sum(vals) if vals else None
        except Exception:
            return None

    ttm_ni     = ttm(net_income)
    ttm_rev    = ttm(revenue)
    ttm_gross  = ttm(gross_profit)
    ttm_op     = ttm(op_income)

    q_shares = _val_at(shares_out, latest_b)
    q_equity = _val_at(equity, latest_b)
    q_assets = _val_at(total_assets, latest_b)
    q_debt   = _val_at(total_debt, latest_b)
    q_ca     = _val_at(current_assets, latest_b)
    q_cl     = _val_at(current_liab, latest_b)

    market_cap   = price * q_shares if (price and q_shares and q_shares > 0) else None
    trailing_eps = ttm_ni / q_shares if (ttm_ni is not None and q_shares and q_shares > 0) else None
    trailing_pe  = price / trailing_eps if (price and trailing_eps and trailing_eps > 0) else None
    price_to_book = None
    if price and q_equity and q_shares and q_shares > 0:
        bvps = q_equity / q_shares
        if bvps > 0:
            price_to_book = price / bvps
    roe = ttm_ni / q_equity * 100 if (ttm_ni is not None and q_equity and q_equity > 0) else None
    roa = ttm_ni / q_assets * 100 if (ttm_ni is not None and q_assets and q_assets > 0) else None
    d2e = q_debt / q_equity * 100 if (q_debt is not None and q_equity and q_equity > 0) else None
    cur_ratio = q_ca / q_cl if (q_ca and q_cl and q_cl > 0) else None
    gm = (ttm_gross / ttm_rev * 100) if (ttm_gross and ttm_rev) else None
    om = (ttm_op / ttm_rev * 100)    if (ttm_op and ttm_rev)    else None
    pm = (ttm_ni / ttm_rev * 100)    if (ttm_ni is not None and ttm_rev) else None

    # YoY — 분기 우선, 부족 시 연간 fallback
    def yoy_q(row):
        if row is None or len(f_cols) < 5:
            return None
        cur = _val_at(row, f_cols[0])
        prv = _val_at(row, f_cols[4])
        if cur is None or prv is None or prv == 0:
            return None
        return (cur - prv) / abs(prv) * 100

    def yoy_a(key_list):
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

    rev_g  = yoy_q(revenue)    or yoy_a(["Total Revenue", "Operating Revenue"])
    earn_g = yoy_q(net_income) or yoy_a(["Net Income", "Net Income Common Stockholders"])

    return {
        "ticker":            ticker.upper(),
        "trailing_pe":       r(trailing_pe),
        "forward_pe":        None,   # stock.info 의존 — rate-limit로 제거
        "peg_ratio":         None,
        "price_to_book":     r(price_to_book),
        "trailing_eps":      r(trailing_eps),
        "forward_eps":       None,
        "revenue_growth":    r(rev_g),
        "earnings_growth":   r(earn_g),
        "gross_margins":     r(gm),
        "operating_margins": r(om),
        "profit_margins":    r(pm),
        "debt_to_equity":    r(d2e),
        "current_ratio":     r(cur_ratio),
        "return_on_equity":  r(roe),
        "return_on_assets":  r(roa),
        "short_percent":     None,
        "market_cap":        r(market_cap, 0),
    }
