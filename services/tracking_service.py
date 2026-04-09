import json
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "tracking_data")


def _ensure_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def _file_path(device_id: str) -> str:
    safe_id = "".join(c for c in device_id if c.isalnum() or c in "-_")
    return os.path.join(DATA_DIR, f"{safe_id}.json")


def _default_data() -> dict:
    return {"recommendations": [], "reliability": None}


def load_data(device_id: str) -> dict:
    _ensure_dir()
    path = _file_path(device_id)
    if not os.path.exists(path):
        return _default_data()
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return _default_data()


def save_data(device_id: str, data: dict):
    _ensure_dir()
    path = _file_path(device_id)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_all(device_id: str) -> dict:
    return load_data(device_id)


def add_recommendations(device_id: str, new_recs: list) -> dict:
    data = load_data(device_id)
    existing_ids = {r["id"] for r in data["recommendations"]}
    added = 0
    for rec in new_recs:
        if rec.get("id") not in existing_ids:
            data["recommendations"].append(rec)
            existing_ids.add(rec["id"])
            added += 1
    save_data(device_id, data)
    return {"added": added, "total": len(data["recommendations"])}


def update_recommendation(device_id: str, rec_id: str, patch: dict) -> dict:
    data = load_data(device_id)
    for rec in data["recommendations"]:
        if rec.get("id") == rec_id:
            for k, v in patch.items():
                rec[k] = v
            save_data(device_id, data)
            return {"updated": True}
    return {"updated": False, "error": "not_found"}


def get_reliability(device_id: str):
    data = load_data(device_id)
    return data.get("reliability")


def save_reliability(device_id: str, reliability: dict) -> dict:
    data = load_data(device_id)
    data["reliability"] = reliability
    save_data(device_id, data)
    return {"ok": True}


def clear_all(device_id: str) -> dict:
    """디바이스의 모든 추천/신뢰도 데이터 삭제"""
    _ensure_dir()
    path = _file_path(device_id)
    if os.path.exists(path):
        try:
            os.remove(path)
            return {"ok": True, "removed": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}
    return {"ok": True, "removed": False}
