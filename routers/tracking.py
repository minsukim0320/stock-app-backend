from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Any, Optional
from services.tracking_service import (
    get_all,
    add_recommendations,
    update_recommendation,
    get_reliability,
    save_reliability,
)

router = APIRouter(prefix="/tracking", tags=["tracking"])


class RecommendationsBody(BaseModel):
    recommendations: list[dict[str, Any]]


class ReliabilityBody(BaseModel):
    total: int = 0
    completed: int = 0
    wins: int = 0
    losses: int = 0
    pending: int = 0
    win_rate: float = 0.0
    avg_return: float = 0.0
    avg_win_return: float = 0.0
    avg_loss_return: float = 0.0
    success_patterns: list[str] = []
    failure_patterns: list[str] = []
    feedback_text: str = ""


class PatchBody(BaseModel):
    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None
    return_percent: Optional[float] = None
    is_success: Optional[bool] = None
    ai_critique: Optional[str] = None
    thesis_feedback: Optional[str] = None


@router.get("/{device_id}")
def get_tracking_data(device_id: str):
    return get_all(device_id)


@router.post("/{device_id}/recommendations")
def post_recommendations(device_id: str, body: RecommendationsBody):
    return add_recommendations(device_id, body.recommendations)


@router.patch("/{device_id}/recommendations/{rec_id}")
def patch_recommendation(device_id: str, rec_id: str, body: PatchBody):
    patch = {k: v for k, v in body.model_dump().items() if v is not None}
    if not patch:
        raise HTTPException(status_code=400, detail="No fields to update")
    return update_recommendation(device_id, rec_id, patch)


@router.get("/{device_id}/reliability")
def get_reliability_summary(device_id: str):
    result = get_reliability(device_id)
    if result is None:
        raise HTTPException(status_code=404, detail="No reliability data")
    return result


@router.post("/{device_id}/reliability")
def post_reliability_summary(device_id: str, body: ReliabilityBody):
    return save_reliability(device_id, body.model_dump())
