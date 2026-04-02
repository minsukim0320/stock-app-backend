"""
과거 특정 날짜 기준의 데이터를 수집하는 서비스
백테스팅 시뮬레이션용 - 해당 시점에 존재했던 데이터만 사용 (look-ahead bias 방지)
"""
import yfinance as yf
import requests
from datetime import datetime, timedelta
from typing import Optional

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


def _nearest_close(ticker: str, target_date: str) -> Optional[float]:
    """target_date 이전 가장 가까운 거래일 종가 반환"""
    try:
        dt = datetime.strptime(target_date, "%Y-%m-%d")
        start = (dt - timedelta(days=10)).strftime("%Y-%m-%d")
        end   = (dt + timedelta(days=1)).strftime("%Y-%m-%d")
        df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
        if df.empty:
            return None
        # target_date 이하 날짜 중 가장 최근
        filtered = df[df.index <= dt]
        if filtered.empty:
            return None
        close_val = filtered["Close"].iloc[-1]
        # pandas scalar → float
        if hasattr(close_val, "item"):
            return round(float(close_val.item()), 4)
        return round(float(close_val), 4)
    except Exception:
        return None


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
    """target_date 기준 이전 months개월 OHLCV 차트 (선견지명 방지)"""
    try:
        dt  = datetime.strptime(target_date, "%Y-%m-%d")
        end = (dt + timedelta(days=1)).strftime("%Y-%m-%d")
        start = (dt - timedelta(days=months * 31)).strftime("%Y-%m-%d")
        df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
        if df.empty:
            return []
        result = []
        for date, row in df.iterrows():
            result.append({
                "date":   date.strftime("%Y-%m-%d"),
                "open":   round(float(row["Open"].item() if hasattr(row["Open"], "item") else row["Open"]), 2),
                "high":   round(float(row["High"].item() if hasattr(row["High"], "item") else row["High"]), 2),
                "low":    round(float(row["Low"].item()  if hasattr(row["Low"],  "item") else row["Low"]),  2),
                "close":  round(float(row["Close"].item() if hasattr(row["Close"], "item") else row["Close"]), 2),
                "volume": int(row["Volume"].item() if hasattr(row["Volume"], "item") else row["Volume"]),
            })
        return result
    except Exception:
        return []


def get_historical_fundamentals(ticker: str, target_date: str) -> dict:
    """target_date 이전 가장 최근 분기 재무제표 기반 펀더멘털"""
    try:
        stock = yf.Ticker(ticker)
        info  = stock.info  # 현재 기준이지만 P/E 등은 yfinance에서 과거 분기 미제공
        # yfinance는 현재 info만 제공하므로 재무제표로 근사치 계산
        dt = datetime.strptime(target_date, "%Y-%m-%d")

        # 분기 EPS (target_date 이전 가장 최근 분기)
        trailing_eps = None
        try:
            qf = stock.quarterly_financials
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
    except Exception:
        return {"ticker": ticker.upper()}


def get_historical_news(ticker: str, target_date: str, finnhub_api_key: str, days_before: int = 14) -> list[dict]:
    """Finnhub으로 target_date 전후 영어 뉴스 조회"""
    if not finnhub_api_key:
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
        resp.raise_for_status()
        articles = resp.json() if isinstance(resp.json(), list) else []
        result = []
        for a in articles[:20]:
            result.append({
                "title":        a.get("headline", ""),
                "summary":      a.get("summary", ""),
                "url":          a.get("url", ""),
                "source":       a.get("source", ""),
                "published_at": a.get("datetime", ""),
            })
        return result
    except Exception:
        return []


def get_full_historical_context(
    tickers: list[str],
    target_date: str,
    finnhub_api_key: str = "",
) -> dict:
    """
    target_date 기준 전체 컨텍스트 수집
    - macro: VIX, TNX, KRW/USD, 금, 유가 등
    - prices: 각 종목 종가
    - charts: 각 종목 3개월 OHLCV
    - fundamentals: 각 종목 펀더멘털
    - news: 각 종목 영어 뉴스 (Finnhub)
    """
    macro  = get_historical_macro(target_date)
    prices = get_historical_prices(tickers, target_date)
    charts = {}
    fundamentals = {}
    news = {}

    for ticker in tickers:
        charts[ticker]       = get_historical_chart(ticker, target_date, months=3)
        fundamentals[ticker] = get_historical_fundamentals(ticker, target_date)
        if finnhub_api_key:
            news[ticker] = get_historical_news(ticker, target_date, finnhub_api_key)

    return {
        "target_date":   target_date,
        "macro":         macro,
        "prices":        prices,
        "charts":        charts,
        "fundamentals":  fundamentals,
        "news":          news,
    }
