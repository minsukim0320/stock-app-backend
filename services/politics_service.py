import os
import re
import requests
from dotenv import load_dotenv
from services.news_utils import deduplicate_news, format_news_for_prompt

load_dotenv()

NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET")

KOREAN_ECONOMY_KEYWORD = "한국 경제 금융 증권"


def get_korean_politics_news(limit: int = 100) -> list[dict]:
    url = "https://openapi.naver.com/v1/search/news.json"
    headers = {
        "X-Naver-Client-Id": NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
    }
    params = {
        "query": KOREAN_ECONOMY_KEYWORD,
        "display": limit,
        "sort": "sim",
    }
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    items = response.json().get("items", [])
    raw = [
        {
            "title": _strip_html(item.get("title", "")),
            "summary": _strip_html(item.get("description", "")),
            "url": item.get("originallink") or item.get("link", ""),
            "source": "Naver News",
            "published_at": item.get("pubDate", ""),
        }
        for item in items
    ]
    return format_news_for_prompt(deduplicate_news(raw))


def get_international_news(limit: int = 50, serpapi_key: str = "") -> list[dict]:
    """SerpAPI Google News로 국제 경제/금융 뉴스 수집 (클라이언트 제공 키 사용)"""
    if not serpapi_key:
        print("[WARN] SerpAPI key not provided — skipping international news")
        return []
    try:
        params = {
            "engine": "google",
            "tbm": "nws",
            "q": "global economy finance stock market",
            "gl": "us",
            "hl": "en",
            "num": limit,
            "api_key": serpapi_key,
        }
        resp = requests.get("https://serpapi.com/search", params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        articles = data.get("news_results", [])
        raw = []
        for a in articles:
            raw.append({
                "title": a.get("title", ""),
                "summary": a.get("snippet", ""),
                "url": a.get("link", ""),
                "source": a.get("source", "") if isinstance(a.get("source"), str) else a.get("source", {}).get("name", ""),
                "published_at": a.get("date", ""),
            })
        deduped = deduplicate_news(raw)
        return format_news_for_prompt(deduped[:limit])
    except Exception as e:
        print(f"[ERROR] SerpAPI international news failed: {e}")
        return []


def _strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text)
