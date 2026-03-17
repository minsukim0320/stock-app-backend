import os
import re
import requests
import feedparser
from dotenv import load_dotenv

load_dotenv()

NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET")

KOREAN_POLITICS_KEYWORD = "한국 정치 외교 경제"

INTERNATIONAL_RSS_FEEDS = [
    "https://feeds.bbci.co.uk/news/world/rss.xml",
    "https://feeds.reuters.com/reuters/worldNews",
]


def get_korean_politics_news(limit: int = 100) -> list[dict]:
    url = "https://openapi.naver.com/v1/search/news.json"
    headers = {
        "X-Naver-Client-Id": NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
    }
    params = {
        "query": KOREAN_POLITICS_KEYWORD,
        "display": limit,
        "sort": "sim",
    }
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    items = response.json().get("items", [])
    return [
        {
            "title": _strip_html(item.get("title", "")),
            "summary": _strip_html(item.get("description", "")),
            "url": item.get("originallink") or item.get("link", ""),
            "source": "Naver News",
            "published_at": item.get("pubDate", ""),
        }
        for item in items
    ]


def get_international_news(limit: int = 50) -> list[dict]:
    results = []
    for feed_url in INTERNATIONAL_RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries:
                results.append({
                    "title": entry.get("title", ""),
                    "summary": _strip_html(entry.get("summary", "")),
                    "url": entry.get("link", ""),
                    "source": feed.feed.get("title", ""),
                    "published_at": entry.get("published", ""),
                })
        except Exception:
            continue
    return results[:limit]


def _strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text)
