import os
import re
import requests
from dotenv import load_dotenv

load_dotenv()

NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET")

# 티커 → 한국어 검색 키워드 매핑
TICKER_KO_MAP = {
    "AAPL": "애플 주식",
    "TSLA": "테슬라 주식",
    "MSFT": "마이크로소프트 주식",
    "GOOGL": "구글 주식",
    "AMZN": "아마존 주식",
    "NVDA": "엔비디아 주식",
    "META": "메타 주식",
}

FINANCE_KEYWORDS_KO = [
    "주식", "증권", "금융", "경제", "투자", "펀드", "시장", "나스닥",
    "코스피", "etf", "배당", "실적", "매출", "수익", "주가", "상장",
    "채권", "환율", "금리", "인플레이션", "무역", "수출", "수입",
]


def get_korean_news(ticker: str, limit: int = 100) -> list[dict]:
    keyword = TICKER_KO_MAP.get(ticker.upper(), f"{ticker} 주식")
    url = "https://openapi.naver.com/v1/search/news.json"
    headers = {
        "X-Naver-Client-Id": NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
    }
    params = {
        "query": keyword,
        "display": limit,
        "sort": "sim",
    }
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    items = response.json().get("items", [])

    result = []
    for item in items:
        title = _strip_html(item.get("title", ""))
        summary = _strip_html(item.get("description", ""))
        combined = (title + " " + summary).lower()
        if not any(kw in combined for kw in FINANCE_KEYWORDS_KO):
            continue
        result.append({
            "title": title,
            "summary": summary,
            "url": item.get("originallink") or item.get("link", ""),
            "source": "",
            "published_at": item.get("pubDate", ""),
        })
    return result


def _strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text)
