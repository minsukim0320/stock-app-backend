from fastapi import APIRouter, HTTPException
from services.yfinance_service import get_stock_price, get_english_news, get_chart_data
from services.naver_service import get_korean_news
from services.politics_service import get_korean_politics_news, get_international_news

router = APIRouter(prefix="/stocks", tags=["stocks"])


@router.get("/{ticker}/price")
def stock_price(ticker: str):
    try:
        return get_stock_price(ticker)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/{ticker}/chart")
def stock_chart(ticker: str, period: str = "1mo"):
    try:
        return get_chart_data(ticker, period)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/news/korean-politics")
def korean_politics_news(limit: int = 40):
    try:
        return get_korean_politics_news(limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/news/international")
def international_news(limit: int = 40):
    try:
        return get_international_news(limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{ticker}/news/en")
def english_news(ticker: str, limit: int = 40):
    try:
        return get_english_news(ticker, limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{ticker}/news/ko")
def korean_news(ticker: str, limit: int = 40):
    try:
        return get_korean_news(ticker, limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
