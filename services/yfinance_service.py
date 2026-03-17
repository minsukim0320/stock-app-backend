import yfinance as yf


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


def get_english_news(ticker: str, limit: int = 20) -> list[dict]:
    stock = yf.Ticker(ticker)
    raw_news = stock.news or []
    result = []
    for item in raw_news[:limit]:
        content = item.get("content", {})
        result.append({
            "title": content.get("title", ""),
            "summary": content.get("summary", ""),
            "url": content.get("canonicalUrl", {}).get("url", ""),
            "source": content.get("provider", {}).get("displayName", ""),
            "published_at": content.get("pubDate", ""),
        })
    return result
