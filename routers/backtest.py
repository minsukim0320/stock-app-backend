from fastapi import APIRouter, Query
from pydantic import BaseModel
from typing import Optional
from services.historical_data_service import get_full_historical_context, get_historical_chart, _yf_download, _safe_float

router = APIRouter(prefix="/backtest", tags=["backtest"])


class HistoricalContextRequest(BaseModel):
    tickers: list[str]
    target_date: str          # yyyy-MM-dd
    finnhub_api_key: str = ""


@router.post("/historical-context")
async def historical_context(req: HistoricalContextRequest):
    """
    target_date 기준 백테스팅용 컨텍스트 수집 — asyncio.gather 병렬화
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
