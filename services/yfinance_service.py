import yfinance as yf
from services.news_utils import deduplicate_news, format_news_for_prompt

ECONOMY_KEYWORDS = [
    "stock", "market", "economy", "economic", "finance", "financial",
    "earnings", "revenue", "profit", "loss", "invest", "fund", "trade",
    "gdp", "inflation", "fed", "bank", "interest rate", "bond", "etf",
    "dividend", "nasdaq", "s&p", "shares", "quarter", "fiscal", "growth",
]


def get_stock_price(ticker: str) -> dict:
    from datetime import datetime, timezone
    stock = yf.Ticker(ticker)
    info = stock.fast_info

    last_price = getattr(info, "last_price", None)
    prev_close = getattr(info, "previous_close", None)
    currency = getattr(info, "currency", None)

    # fast_info 실패 시 history fallback
    if last_price is None or prev_close is None:
        hist = stock.history(period="5d")
        if hist.empty:
            raise Exception(f"{ticker}: 주가 데이터를 가져올 수 없습니다 (fast_info 및 history 모두 실패)")
        last_price = float(hist["Close"].iloc[-1])
        prev_close = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else last_price
        if currency is None:
            try:
                currency = stock.info.get("currency", "USD")
            except Exception:
                currency = "USD"

    # yfinance free tier는 15분 지연 가능 → 마지막 1분봉의 timestamp와
    # 현재 UTC 시각 차이로 대략적인 delay를 계산해 반환 (클라이언트 stale 표시용)
    delay_minutes = None
    try:
        hist_1m = stock.history(period="1d", interval="1m")
        if not hist_1m.empty:
            last_ts = hist_1m.index[-1].to_pydatetime()
            if last_ts.tzinfo is None:
                last_ts = last_ts.replace(tzinfo=timezone.utc)
            delta = datetime.now(timezone.utc) - last_ts
            delay_minutes = max(0, int(delta.total_seconds() // 60))
    except Exception:
        pass

    change = last_price - prev_close
    change_pct = (change / prev_close * 100) if prev_close else 0

    return {
        "ticker": ticker.upper(),
        "price": round(last_price, 2),
        "change": round(change, 2),
        "change_percent": round(change_pct, 2),
        "currency": currency or "USD",
        "delay_minutes": delay_minutes,
    }


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


def get_fundamentals(ticker: str) -> dict:
    stock = yf.Ticker(ticker)
    info = stock.info

    def safe(key, scale=1, decimals=2):
        val = info.get(key)
        if val is None or not isinstance(val, (int, float)):
            return None
        return round(val * scale, decimals)

    return {
        "ticker": ticker.upper(),
        "trailing_pe":        safe("trailingPE"),
        "forward_pe":         safe("forwardPE"),
        "peg_ratio":          safe("pegRatio"),
        "price_to_book":      safe("priceToBook"),
        "trailing_eps":       safe("trailingEps"),
        "forward_eps":        safe("forwardEps"),
        "revenue_growth":     safe("revenueGrowth",     scale=100),
        "earnings_growth":    safe("earningsGrowth",    scale=100),
        "gross_margins":      safe("grossMargins",      scale=100),
        "operating_margins":  safe("operatingMargins",  scale=100),
        "profit_margins":     safe("profitMargins",     scale=100),
        "debt_to_equity":     safe("debtToEquity"),
        "current_ratio":      safe("currentRatio"),
        "return_on_equity":   safe("returnOnEquity",    scale=100),
        "return_on_assets":   safe("returnOnAssets",    scale=100),
        "short_percent":      safe("shortPercentOfFloat", scale=100),
        "market_cap":         info.get("marketCap"),
    }
