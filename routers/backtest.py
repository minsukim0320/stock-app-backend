import asyncio
from fastapi import APIRouter, Query
from pydantic import BaseModel
from typing import Optional
from services.historical_data_service import (
    get_full_historical_context, get_historical_chart,
    get_historical_news,
    _yf_download, _safe_float,
)

router = APIRouter(prefix="/backtest", tags=["backtest"])


class HistoricalContextRequest(BaseModel):
    tickers: list[str]
    target_date: str          # yyyy-MM-dd
    finnhub_api_key: str = ""
    serpapi_key: str = ""


@router.post("/historical-context")
async def historical_context(req: HistoricalContextRequest):
    """
    target_date 기준 백테스팅용 컨텍스트 수집 — asyncio.gather 병렬화
    """
    return await get_full_historical_context(
        tickers=req.tickers,
        target_date=req.target_date,
        finnhub_api_key=req.finnhub_api_key,
        serpapi_key=req.serpapi_key,
    )


class HistoricalNewsRequest(BaseModel):
    tickers: list[str]
    target_date: str
    finnhub_api_key: str = ""


@router.post("/historical-news")
async def historical_news(req: HistoricalNewsRequest):
    """
    target_date 기준 Finnhub 영어 뉴스만 경량 조회 (차트·펀더멘털 제외).
    sector universe 보조 종목용 — SectorAnalyst 헤드라인 입력에 사용.
    """
    if not req.finnhub_api_key:
        return {"news": {t: [] for t in req.tickers}, "intl_news": []}

    news = {}
    for ticker in req.tickers:
        try:
            nl = await asyncio.to_thread(
                get_historical_news, ticker, req.target_date, req.finnhub_api_key
            )
            news[ticker] = nl
            await asyncio.sleep(0.3)  # rate limit 보호
        except Exception as e:
            print(f"[ERROR] historical-news {ticker}: {e}")
            news[ticker] = []

    return {"news": news}


@router.get("/forward-chart/{ticker}")
def forward_chart(
    ticker: str,
    from_date: str = Query(..., description="시뮬레이션 진입일 (yyyy-MM-dd)"),
    days: int = Query(60, description="진입 이후 조회할 거래일 수"),
):
    """
    from_date 이후 실제 주가 데이터 반환 — 목표가/손절가 도달 여부 시뮬레이션용
    """
    from datetime import datetime, timedelta

    try:
        dt    = datetime.strptime(from_date, "%Y-%m-%d")
        start = (dt + timedelta(days=1)).strftime("%Y-%m-%d")
        end   = (dt + timedelta(days=days * 2)).strftime("%Y-%m-%d")
        df = _yf_download(ticker, start, end, timeout_sec=30)
        if df.empty:
            print(f"[WARN] forward_chart: no data for {ticker} from {from_date}")
            return []
        result = []
        for date, row in df.iterrows():
            try:
                result.append({
                    "date":   date.strftime("%Y-%m-%d"),
                    "open":   round(_safe_float(row["Open"]), 2),
                    "high":   round(_safe_float(row["High"]), 2),
                    "low":    round(_safe_float(row["Low"]), 2),
                    "close":  round(_safe_float(row["Close"]), 2),
                    "volume": int(_safe_float(row["Volume"])),
                })
            except Exception as e:
                print(f"[WARN] forward_chart row parse error for {ticker}: {e}")
                continue
        return result[:days]
    except Exception as e:
        print(f"[ERROR] forward_chart({ticker}, {from_date}): {e}")
        return []


@router.get("/forward-forex")
def forward_forex(
    from_date: str = Query(..., description="시뮬레이션 진입일 (yyyy-MM-dd)"),
    days: int = Query(60, description="조회할 거래일 수"),
):
    """
    from_date 이후 KRW/USD 환율 데이터 반환 — P&L 환율 반영용
    """
    from datetime import datetime, timedelta

    try:
        dt    = datetime.strptime(from_date, "%Y-%m-%d")
        start = (dt + timedelta(days=1)).strftime("%Y-%m-%d")
        end   = (dt + timedelta(days=days * 2)).strftime("%Y-%m-%d")
        df = _yf_download("KRW=X", start, end, timeout_sec=30)
        if df.empty:
            print(f"[WARN] forward_forex: no data for KRW=X from {from_date}")
            return []
        result = []
        for date, row in df.iterrows():
            try:
                result.append({
                    "date":  date.strftime("%Y-%m-%d"),
                    "rate":  round(_safe_float(row["Close"]), 2),
                })
            except Exception as e:
                print(f"[WARN] forward_forex row parse error: {e}")
                continue
        return result[:days]
    except Exception as e:
        print(f"[ERROR] forward_forex({from_date}): {e}")
        return []
