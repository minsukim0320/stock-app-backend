import traceback
import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from services.yfinance_service import (
    get_stock_price, get_stock_prices_batch, get_english_news,
    get_chart_data, get_charts_batch, get_fundamentals,
)
from services.naver_service import get_korean_news
from services.politics_service import get_korean_politics_news, get_international_news

router = APIRouter(prefix="/stocks", tags=["stocks"])

_log = logging.getLogger("stockapp.server")


def _log_err(where: str, e: Exception):
    _log.error(f"{where}: {type(e).__name__}: {e}\n{traceback.format_exc()}")


class PricesBatchRequest(BaseModel):
    tickers: list[str]


class ChartsBatchRequest(BaseModel):
    tickers: list[str]
    period: str = "1y"


@router.get("/{ticker}/price")
def stock_price(ticker: str):
    try:
        return get_stock_price(ticker)
    except Exception as e:
        _log_err(f"stock_price({ticker})", e)
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/prices")
def stock_prices_batch(req: PricesBatchRequest):
    """여러 종목 현재가 배치 조회 — 개별 호출 대비 10배+ 빠름 (Yahoo에 1번만 호출)"""
    try:
        return get_stock_prices_batch(req.tickers)
    except Exception as e:
        _log_err(f"stock_prices_batch({len(req.tickers)} tickers)", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{ticker}/chart")
def stock_chart(ticker: str, period: str = "1mo"):
    try:
        return get_chart_data(ticker, period)
    except Exception as e:
        _log_err(f"stock_chart({ticker}, {period})", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/charts-batch")
def stock_charts_batch(req: ChartsBatchRequest):
    """여러 종목 차트 배치 조회 — 개별 호출 대비 10배+ 빠름. 지수 스캔용."""
    try:
        return get_charts_batch(req.tickers, req.period)
    except Exception as e:
        _log_err(f"stock_charts_batch({len(req.tickers)} tickers)", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{ticker}/fundamentals")
def stock_fundamentals(ticker: str):
    try:
        return get_fundamentals(ticker)
    except Exception as e:
        _log_err(f"stock_fundamentals({ticker})", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/news/korean-politics")
def korean_politics_news(limit: int = 40):
    try:
        return get_korean_politics_news(limit)
    except Exception as e:
        _log_err(f"korean_politics_news(limit={limit})", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/news/international")
def international_news(limit: int = 40, serpapi_key: str = ""):
    try:
        return get_international_news(limit, serpapi_key=serpapi_key)
    except Exception as e:
        _log_err(f"international_news(limit={limit})", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{ticker}/news/en")
def english_news(ticker: str, limit: int = 40):
    try:
        return get_english_news(ticker, limit)
    except Exception as e:
        _log_err(f"english_news({ticker})", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{ticker}/news/ko")
def korean_news(ticker: str, limit: int = 40):
    try:
        return get_korean_news(ticker, limit)
    except Exception as e:
        _log_err(f"korean_news({ticker})", e)
        raise HTTPException(status_code=500, detail=str(e))
