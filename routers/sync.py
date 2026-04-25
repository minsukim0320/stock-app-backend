import json
import os
import hashlib
from typing import Any
from fastapi import APIRouter, HTTPException, Request

router = APIRouter(prefix="/sync", tags=["sync"])

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "sync_data")
ALLOWED_KEYS = {"history", "hist_sim", "portfolios", "exits"}


def _email_dir(email: str) -> str:
    email = (email or "").strip().lower()
    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="invalid email")
    digest = hashlib.sha256(email.encode("utf-8")).hexdigest()[:24]
    path = os.path.join(DATA_DIR, digest)
    os.makedirs(path, exist_ok=True)
    return path


def _file_path(email: str, key: str) -> str:
    if key not in ALLOWED_KEYS:
        raise HTTPException(status_code=404, detail="unknown key")
    return os.path.join(_email_dir(email), f"{key}.json")


@router.get("/{key}")
def get_sync(key: str, email: str):
    path = _file_path(email, key)
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


@router.post("/{key}")
async def post_sync(key: str, email: str, request: Request):
    path = _file_path(email, key)
    try:
        body: Any = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid json body")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(body, f, ensure_ascii=False)
    return {"ok": True}
