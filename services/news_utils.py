import re


def deduplicate_news(articles: list[dict], threshold: float = 0.55) -> list[dict]:
    """
    제목 기반 Jaccard 유사도로 중복 기사를 제거합니다.
    유사도가 threshold 이상이면 중복으로 판단하고 먼저 나온 기사만 유지합니다.
    중복 횟수는 '_mention_count' 필드로 기록해 리포트 가중치에 활용합니다.
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
                matched = True
                break

        if not matched:
            unique.append({**article, '_mention_count': 1})

    return unique


def format_news_for_prompt(articles: list[dict]) -> list[dict]:
    """
    _mention_count가 2 이상인 기사에 [반복 N회] 태그를 제목에 추가합니다.
    Gemini가 반복 언급 이슈에 더 높은 중요도를 부여하도록 유도합니다.
    """
    result = []
    for a in articles:
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
    # mention_count 내림차순 정렬 (중요 이슈 먼저)
    result.sort(key=lambda x: -articles[result.index(x)].get('_mention_count', 1))
    return result
