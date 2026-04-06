from fastapi import APIRouter, Query
from pydantic import BaseModel
from typing import Optional
from services.historical_data_service import get_full_historical_context, get_historical_chart

router = APIRouter(prefix="/backtest", tags=["backtest"])


class HistoricalContextRequest(BaseModel):
    tickers: list[str]
    target_date: str          # yyyy-MM-dd
    finnhub_api_key: str = ""


@router.post("/historical-context")
async def historical_context(req: HistoricalContextRequest):
    """
    target_date 기준 백테스팅용 컨텍스트 수집 — asyncio.gather 병렬화
    - 매크로 지표 (VIX, TNX, KRW/USD, 금, 유가)
    - 각 종목 종가
    - 각 종목 3개월 OHLCV 차트
    - 각 종목 펀더멘털
    - 각 종목 영어 뉴스 (Finnhub 키 제공 시)
    """
    return await get_full_historical_context(
        tickers=req.tickers,
        target_date=req.target_date,
        finnhub_api_key=req.finnhub_api_key,
    )


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
    import yfinance as yf

    try:
        dt    = datetime.strptime(from_date, "%Y-%m-%d")
        start = (dt + timedelta(days=1)).strftime("%Y-%m-%d")
        end   = (dt + timedelta(days=days * 2)).strftime("%Y-%m-%d")  # 거래일 기준이므로 여유 있게
        df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
        if df.empty:
            return []
        result = []
        for date, row in df.iterrows():
            result.append({
                "date":   date.strftime("%Y-%m-%d"),
                "open":   round(float(row["Open"].item()   if hasattr(row["Open"],   "item") else row["Open"]),   2),
                "high":   round(float(row["High"].item()   if hasattr(row["High"],   "item") else row["High"]),   2),
                "low":    round(float(row["Low"].item()    if hasattr(row["Low"],    "item") else row["Low"]),    2),
                "close":  round(float(row["Close"].item()  if hasattr(row["Close"],  "item") else row["Close"]),  2),
                "volume": int(row["Volume"].item() if hasattr(row["Volume"], "item") else row["Volume"]),
            })
        return result[:days]
    except Exception as e:
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
    import yfinance as yf

    try:
        dt    = datetime.strptime(from_date, "%Y-%m-%d")
        start = (dt + timedelta(days=1)).strftime("%Y-%m-%d")
        end   = (dt + timedelta(days=days * 2)).strftime("%Y-%m-%d")
        df = yf.download("KRW=X", start=start, end=end, progress=False, auto_adjust=True)
        if df.empty:
            return []
        result = []
        for date, row in df.iterrows():
            result.append({
                "date":  date.strftime("%Y-%m-%d"),
                "rate":  round(float(row["Close"].item() if hasattr(row["Close"], "item") else row["Close"]), 2),
            })
        return result[:days]
    except Exception:
        return []
