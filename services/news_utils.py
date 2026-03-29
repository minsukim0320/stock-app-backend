import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime


def _parse_date(date_str: str) -> datetime:
    """ISO 및 RFC 2822 형식의 날짜 문자열을 datetime으로 변환합니다."""
    if not date_str:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        # RFC 2822 형식 (Naver: "Mon, 15 Jan 2024 10:30:00 +0900")
        return parsedate_to_datetime(date_str).astimezone(timezone.utc)
    except Exception:
        pass
    try:
        # ISO 형식 (yfinance: "2024-01-15T10:30:00Z" 또는 "2024-01-15T10:30:00+00:00")
        return datetime.fromisoformat(date_str.replace('Z', '+00:00')).astimezone(timezone.utc)
    except Exception:
        pass
    return datetime.min.replace(tzinfo=timezone.utc)


def deduplicate_news(articles: list[dict], threshold: float = 0.55) -> list[dict]:
    """
    제목 기반 Jaccard 유사도로 중복 기사를 제거합니다.
    - 중복 기사의 요약을 병합하여 AI가 모든 정보를 받을 수 있도록 합니다.
    - 더 최신 기사가 발견되면 메타데이터(날짜, URL)를 업데이트합니다.
    - 중복 횟수는 '_mention_count' 필드로 기록해 리포트 가중치에 활용합니다.
    """
    def tokenize(text: str) -> set:
        return set(re.sub(r'[^\w]', ' ', text.lower()).split())

    unique: list[dict] = []

    for article in articles:
        title_words = tokenize(article.get('title', ''))
        if not title_words:
            unique.append({**article, '_mention_count': 1})
            continue

        matched = False
        for kept in unique:
            kept_words = tokenize(kept.get('title', ''))
            if not kept_words:
                continue
            intersection = len(title_words & kept_words)
            union = len(title_words | kept_words)
            if union > 0 and intersection / union >= threshold:
                kept['_mention_count'] = kept.get('_mention_count', 1) + 1

                # 중복 기사 요약 병합 (새로운 내용이 있으면 추가)
                new_summary = article.get('summary', '').strip()
                existing_summary = kept.get('summary', '')
                if new_summary and new_summary not in existing_summary:
                    kept['summary'] = existing_summary + f"\n[추가보도] {new_summary}"

                # 더 최신 기사로 메타데이터 업데이트
                kept_date = _parse_date(kept.get('published_at', ''))
                new_date = _parse_date(article.get('published_at', ''))
                if new_date > kept_date:
                    kept['published_at'] = article['published_at']
                    kept['url'] = article.get('url', kept.get('url', ''))
                    kept['source'] = article.get('source', kept.get('source', ''))

                matched = True
                break

        if not matched:
            unique.append({**article, '_mention_count': 1})

    return unique


def format_news_for_prompt(articles: list[dict]) -> list[dict]:
    """
    정렬 우선순위:
    1. 최신 기사 우선 (published_at 내림차순)
    2. 같은 날짜 내에서 _mention_count 내림차순 (더 많이 보도된 기사 우선)
    2회 이상 언급된 기사에 [반복 N회] 태그를 추가합니다.
    """
    def sort_key(a):
        date = _parse_date(a.get('published_at', ''))
        count = a.get('_mention_count', 1)
        return (date, count)

    sorted_articles = sorted(articles, key=sort_key, reverse=True)
    result = []
    for a in sorted_articles:
        count = a.get('_mention_count', 1)
        title = a.get('title', '')
        if count >= 2:
            title = f"[반복 {count}회] {title}"
        result.append({
            'title': title,
            'summary': a.get('summary', ''),
            'url': a.get('url', ''),
            'source': a.get('source', ''),
            'published_at': a.get('published_at', ''),
        })
    return result
