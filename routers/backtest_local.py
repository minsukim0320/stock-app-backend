"""
백테스트 라우터 — 과거 AI 시뮬레이션용 엔드포인트 (미사용 — main.py에 마운트되지 않음)

WARNING: 이 모듈은 yf.Ticker().history()를 asyncio.to_thread + asyncio.gather로 병렬 호출하지만,
yfinance는 thread-safe하지 않아 데이터 오염이 발생합니다.
재사용 시 historical_data_service.py의 _yf_lock + 배치 패턴을 적용해야 합니다.

- POST /backtest/historical-context  : target_date 기준 과거 데이터 수집 (병렬)
- GET  /backtest/forward-chart/{ticker} : 진입일 이후 실제 주가
- GET  /backtest/forward-forex          : 진입일 이후 KRW/USD 환율
"""
import asyncio
from fastapi import APIRouter
from pydantic import BaseModel
import yfinance as yf
from datetime import datetime, timedelta
from services.finnhub_service import get_company_news_before, get_basic_financials

router = APIRouter(prefix="/backtest", tags=["backtest"])

# 매크로 지표 티커
MACRO_TICKERS = {
    "VIX": "^VIX",
    "TNX": "^TNX",
    "KRW=X": "KRW=X",
    "GLD": "GLD",
    "CL=F": "CL=F",
    "BZ=F": "BZ=F",
    "DX=F": "DX=F",
    "SPY": "SPY",
}


class HistoricalContextRequest(BaseModel):
    tickers: list[str]
    target_date: str        # yyyy-MM-dd
    finnhub_api_key: str = ""


# ── 동기 헬퍼 (thread pool에서 실행) ────────────────────────────────────────

def _get_price_at_date(ticker: str, target_date: str) -> float | None:
    """target_date 당일 또는 가장 가까운 이전 거래일 종가 반환"""
    try:
        end_dt = datetime.strptime(target_date, "%Y-%m-%d")
        start_dt = end_dt - timedelta(days=7)
        hist = yf.Ticker(ticker).history(
            start=start_dt.strftime("%Y-%m-%d"),
            end=(end_dt + timedelta(days=1)).strftime("%Y-%m-%d"),
        )
        if hist.empty:
            return None
        hist.index = hist.index.tz_localize(None) if hist.index.tzinfo else hist.index
        filtered = hist[hist.index <= end_dt]
        if filtered.empty:
            return None
        return round(float(filtered["Close"].iloc[-1]), 2)
    except Exception:
        return None


def _get_chart_before_date(ticker: str, target_date: str, period_days: int = 180) -> list[dict]:
    """target_date 이전 차트 데이터 (기술적 분석용)"""
    try:
        end_dt = datetime.strptime(target_date, "%Y-%m-%d")
        start_dt = end_dt - timedelta(days=period_days)
        hist = yf.Ticker(ticker).history(
            start=start_dt.strftime("%Y-%m-%d"),
            end=(end_dt + timedelta(days=1)).strftime("%Y-%m-%d"),
        )
        if hist.empty:
            return []
        hist.index = hist.index.tz_localize(None) if hist.index.tzinfo else hist.index
        result = []
        for date, row in hist.iterrows():
            if date.date() > end_dt.date():
                continue
            result.append({
                "date": date.strftime("%Y-%m-%d"),
                "open": round(float(row["Open"]), 2),
                "high": round(float(row["High"]), 2),
                "low": round(float(row["Low"]), 2),
                "close": round(float(row["Close"]), 2),
                "volume": int(row["Volume"]),
            })
        return result
    except Exception:
        return []


# ── 비동기 래퍼 ──────────────────────────────────────────────────────────────

async def _async_price(ticker: str, target_date: str) -> tuple[str, float | None]:
    price = await asyncio.to_thread(_get_price_at_date, ticker, target_date)
    return ticker, price


async def _async_chart(ticker: str, target_date: str) -> tuple[str, list[dict]]:
    chart = await asyncio.to_thread(_get_chart_before_date, ticker, target_date, 180)
    return ticker, chart


async def _async_fundamentals(ticker: str, api_key: str) -> tuple[str, dict]:
    fund = await asyncio.to_thread(get_basic_financials, ticker, api_key)
    return ticker, fund


async def _async_news(ticker: str, target_date: str, api_key: str) -> tuple[str, list]:
    news = await asyncio.to_thread(
        get_company_news_before, ticker, target_date, api_key,
        30, 20,
    )
    return ticker, news


async def _async_intl_news(target_date: str, api_key: str) -> list[dict]:
    import requests as req_lib
    def _fetch():
        end_dt = datetime.strptime(target_date, "%Y-%m-%d")
        start_dt = end_dt - timedelta(days=14)
        try:
            resp = req_lib.get(
                "https://finnhub.io/api/v1/news",
                headers={"X-Finnhub-Token": api_key},
                params={"category": "general", "minId": 0},
                timeout=15,
            )
            if resp.status_code != 200:
                return []
            results = []
            for a in (resp.json() or []):
                ts = a.get("datetime", 0)
                if not ts:
                    continue
                article_dt = datetime.fromtimestamp(ts)
                if article_dt.date() > end_dt.date():
                    continue
                if article_dt.date() < start_dt.date():
                    continue
                results.append({
                    "title": a.get("headline", ""),
                    "summary": a.get("summary", ""),
                    "url": a.get("url", ""),
                    "source": a.get("source", ""),
                    "published_at": article_dt.strftime("%Y-%m-%d"),
                })
            return results
        except Exception:
            return []

    return await asyncio.to_thread(_fetch)


# ── 엔드포인트 ────────────────────────────────────────────────────────────────

@router.post("/historical-context")
async def historical_context(req: HistoricalContextRequest):
    """
    target_date 기준 과거 시장 데이터 수집.
    매크로·종목별 데이터를 asyncio.gather로 병렬 수집 → 타임아웃 대폭 단축.
    뉴스는 target_date 이전 30일치만 — 미래 정보 누출 없음.
    """
    target_date = req.target_date
    has_finnhub = bool(req.finnhub_api_key)

    # ── 1. 매크로 지표 병렬 수집 ───────────────────────────────────────────
    macro_tasks = [_async_price(yticker, target_date) for yticker in MACRO_TICKERS.values()]
    macro_results = await asyncio.gather(*macro_tasks, return_exceptions=True)

    macro: dict[str, float] = {}
    for key, result in zip(MACRO_TICKERS.keys(), macro_results):
        if isinstance(result, Exception) or result is None:
            continue
        _, price = result
        if price is not None:
            macro[key] = price

    # ── 2. 종목별 데이터 병렬 수집 ────────────────────────────────────────
    tickers = req.tickers

    # 가격 + 차트 — 항상 수집
    price_tasks = [_async_price(t, target_date) for t in tickers]
    chart_tasks = [_async_chart(t, target_date) for t in tickers]

    # 펀더멘털 + 뉴스 — Finnhub 키 있을 때만
    fund_tasks = (
        [_async_fundamentals(t, req.finnhub_api_key) for t in tickers]
        if has_finnhub else []
    )
    news_tasks = (
        [_async_news(t, target_date, req.finnhub_api_key) for t in tickers]
        if has_finnhub else []
    )

    # 국제 정세 뉴스
    intl_task = (
        _async_intl_news(target_date, req.finnhub_api_key)
        if has_finnhub else asyncio.sleep(0, result=[])
    )

    # 모두 동시에 실행
    all_results = await asyncio.gather(
        asyncio.gather(*price_tasks, return_exceptions=True),
        asyncio.gather(*chart_tasks, return_exceptions=True),
        asyncio.gather(*fund_tasks, return_exceptions=True) if fund_tasks else asyncio.sleep(0, result=[]),
        asyncio.gather(*news_tasks, return_exceptions=True) if news_tasks else asyncio.sleep(0, result=[]),
        intl_task,
        return_exceptions=True,
    )

    price_res, chart_res, fund_res, news_res, intl_raw = all_results

    # 결과 dict 조립
    prices: dict[str, float] = {}
    for r in (price_res or []):
        if isinstance(r, Exception): continue
        ticker, price = r
        if price is not None:
            prices[ticker] = price

    charts: dict[str, list] = {}
    for r in (chart_res or []):
        if isinstance(r, Exception): continue
        ticker, chart = r
        charts[ticker] = chart

    fundamentals: dict[str, dict] = {}
    for r in (fund_res or []):
        if isinstance(r, Exception): continue
        ticker, fund = r
        fundamentals[ticker] = fund

    news: dict[str, list] = {}
    for t in tickers:
        news[t] = []
    for r in (news_res or []):
        if isinstance(r, Exception): continue
        ticker, n = r
        news[ticker] = n

    intl_news = intl_raw if isinstance(intl_raw, list) else []

    return {
        "target_date": target_date,
        "macro": macro,
        "prices": prices,
        "charts": charts,
        "fundamentals": fundamentals,
        "news": news,
        "intl_news": intl_news[:30],
    }


@router.get("/forward-chart/{ticker}")
def forward_chart(ticker: str, from_date: str, days: int = 120):
    """
    from_date 이후 실제 주가 데이터 (triple-barrier 시뮬레이션용).
    """
    try:
        start_dt = datetime.strptime(from_date, "%Y-%m-%d")
        end_dt = start_dt + timedelta(days=days + 5)
        hist = yf.Ticker(ticker).history(
            start=from_date,
            end=end_dt.strftime("%Y-%m-%d"),
        )
        if hist.empty:
            return []
        hist.index = hist.index.tz_localize(None) if hist.index.tzinfo else hist.index
        result = []
        for date, row in hist.iterrows():
            result.append({
                "date": date.strftime("%Y-%m-%d"),
                "open": round(float(row["Open"]), 2),
                "high": round(float(row["High"]), 2),
                "low": round(float(row["Low"]), 2),
                "close": round(float(row["Close"]), 2),
                "volume": int(row["Volume"]),
            })
            if len(result) >= days:
                break
        return result
    except Exception:
        return []


@router.get("/forward-forex")
def forward_forex(from_date: str, days: int = 120):
    """
    from_date 이후 KRW/USD 환율 데이터.
    """
    try:
        start_dt = datetime.strptime(from_date, "%Y-%m-%d")
        end_dt = start_dt + timedelta(days=days + 5)
        hist = yf.Ticker("KRW=X").history(
            start=from_date,
            end=end_dt.strftime("%Y-%m-%d"),
        )
        if hist.empty:
            return []
        hist.index = hist.index.tz_localize(None) if hist.index.tzinfo else hist.index
        result = []
        for date, row in hist.iterrows():
            result.append({
                "date": date.strftime("%Y-%m-%d"),
                "rate": round(float(row["Close"]), 2),
            })
            if len(result) >= days:
                break
        return result
    except Exception:
        return []
