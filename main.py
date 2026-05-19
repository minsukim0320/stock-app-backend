from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from starlette.middleware.base import BaseHTTPMiddleware
from routers.stocks import router as stocks_router
from routers.tracking import router as tracking_router
from routers.backtest import router as backtest_router
from routers.sync import router as sync_router
from services.memory_monitor import mem_log, get_rss_mb, CRITICAL_THRESHOLD_MB
import os
import sys
import gc
import logging
import traceback
import time
from logging.handlers import RotatingFileHandler

# ── 서버 측 로그 설정 ────────────────────────────────────────────────
SERVER_LOG_FILE = os.path.join(os.path.dirname(__file__), "server.log")
SERVER_LOG_MAX_BYTES = 2 * 1024 * 1024  # 2MB

_server_logger = logging.getLogger("stockapp.server")
_server_logger.setLevel(logging.INFO)
_server_logger.propagate = False

_file_handler = RotatingFileHandler(
    SERVER_LOG_FILE, maxBytes=SERVER_LOG_MAX_BYTES, backupCount=1, encoding="utf-8"
)
_fmt = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
_file_handler.setFormatter(_fmt)
_server_logger.addHandler(_file_handler)

_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(_fmt)
_server_logger.addHandler(_console_handler)


def server_log(msg: str, level: str = "INFO"):
    """다른 모듈에서 import해서 사용하는 서버 로거 헬퍼"""
    getattr(_server_logger, level.lower())(msg)


app = FastAPI(title="US Stock News API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── 모든 요청과 에러를 로깅하는 미들웨어 ──────────────────────────────
# 메모리 무거운 엔드포인트 — 진단 로그 + 응답 후 강제 GC
_HEAVY_PATH_SEGMENTS = ("/chart", "/charts-batch", "/news", "/backtest",
                        "/fundamentals", "/historical-context")


class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # 로그 조회 엔드포인트는 스킵 (무한 루프 방지)
        if request.url.path in ("/server-logs", "/logs"):
            return await call_next(request)

        path = request.url.path
        is_heavy = any(seg in path for seg in _HEAVY_PATH_SEGMENTS)
        start = time.monotonic()
        rss_before = None
        if is_heavy:
            rss_before = get_rss_mb()
            mem_log(f"REQ START {request.method} {path}")
        try:
            response = await call_next(request)
            elapsed = time.monotonic() - start
            # 4xx/5xx는 WARN, 그 외는 조용히 INFO (30초 이상만)
            if response.status_code >= 400:
                _server_logger.warning(
                    f"{request.method} {path} → HTTP {response.status_code} ({elapsed:.2f}s)"
                )
            elif elapsed > 30:
                _server_logger.info(
                    f"SLOW {request.method} {path} ({elapsed:.2f}s)"
                )
            # OOM 방지: 메모리 많이 쓰는 엔드포인트 응답 후 명시적 GC + 진단 로그.
            # Render free tier 512MB 한도 — yfinance DataFrame 누적 차단.
            if is_heavy:
                gc.collect()
                rss_after = get_rss_mb()
                if rss_after is not None and rss_before is not None:
                    delta = rss_after - rss_before
                    tag = "MEM🔥" if rss_after >= CRITICAL_THRESHOLD_MB else "MEM"
                    _server_logger.info(
                        f"[{tag}] REQ END {request.method} {path} "
                        f"({elapsed:.2f}s, {rss_before:.0f}→{rss_after:.0f}MB, "
                        f"Δ{'+' if delta >= 0 else ''}{delta:.1f}MB)"
                    )
            return response
        except Exception as exc:
            elapsed = time.monotonic() - start
            tb = traceback.format_exc()
            rss_now = get_rss_mb()
            mem_str = f" RSS={rss_now:.0f}MB" if rss_now is not None else ""
            _server_logger.error(
                f"UNHANDLED {type(exc).__name__} at {request.method} {path} "
                f"({elapsed:.2f}s){mem_str}: {exc}\n{tb}"
            )
            return JSONResponse(status_code=500, content={"detail": str(exc)})


app.add_middleware(LoggingMiddleware)

_server_logger.info("Server starting up — logging initialized")
mem_log("Server startup baseline (after imports)")


app.include_router(stocks_router)
app.include_router(tracking_router)
app.include_router(backtest_router)
app.include_router(sync_router)

LOG_FILE = os.path.join(os.path.dirname(__file__), "client_errors.log")
MAX_BYTES = 1 * 1024 * 1024  # 1MB


class LogEntry(BaseModel):
    timestamp: str
    message: str
    error: str
    stack: str = ""


@app.post("/logs")
def receive_log(entry: LogEntry):
    line = f"[{entry.timestamp}] {entry.message}\nERROR: {entry.error}\n{entry.stack}\n---\n"
    try:
        if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > MAX_BYTES:
            with open(LOG_FILE, "r", encoding="utf-8") as f:
                content = f.read()
            content = content[len(content) // 2:]
            with open(LOG_FILE, "w", encoding="utf-8") as f:
                f.write(content)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass
    return {"ok": True}


@app.get("/logs")
def get_logs():
    if not os.path.exists(LOG_FILE):
        return {"logs": ""}
    with open(LOG_FILE, "r", encoding="utf-8") as f:
        return {"logs": f.read()}


@app.delete("/logs")
def clear_logs():
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)
    return {"ok": True}


@app.get("/server-logs")
def get_server_logs(lines: int = 300):
    """서버 측 로그 조회 — Python stdout/stderr + 처리되지 않은 예외 + traceback 포함"""
    if not os.path.exists(SERVER_LOG_FILE):
        return {"logs": "", "size": 0}
    try:
        with open(SERVER_LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
            all_lines = f.readlines()
        tail = all_lines[-lines:] if lines > 0 else all_lines
        return {
            "logs": "".join(tail),
            "total_lines": len(all_lines),
            "returned_lines": len(tail),
        }
    except Exception as e:
        return {"logs": f"[ERROR reading log] {e}", "error": str(e)}


@app.delete("/server-logs")
def clear_server_logs():
    if os.path.exists(SERVER_LOG_FILE):
        os.remove(SERVER_LOG_FILE)
    return {"ok": True}


@app.get("/")
def root():
    return {"message": "US Stock News API is running"}
