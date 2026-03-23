import yfinance as yf
from services.news_utils import deduplicate_news, format_news_for_prompt

ECONOMY_KEYWORDS = [
    "stock", "market", "economy", "economic", "finance", "financial",
    "earnings", "revenue", "profit", "loss", "invest", "fund", "trade",
    "gdp", "inflation", "fed", "bank", "interest rate", "bond", "etf",
    "dividend", "nasdaq", "s&p", "shares", "quarter", "fiscal", "growth",
]


def get_stock_price(ticker: str) -> dict:
    stock = yf.Ticker(ticker)
    info = stock.fast_info
    return {
        "ticker": ticker.upper(),
        "price": round(info.last_price, 2),
        "change": round(info.last_price - info.previous_close, 2),
        "change_percent": round((info.last_price - info.previous_close) / info.previous_close * 100, 2),
        "currency": info.currency,
    }


def get_chart_data(ticker: str, period: str = "1mo") -> list[dict]:
    stock = yf.Ticker(ticker)
    hist = stock.history(period=period)
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
