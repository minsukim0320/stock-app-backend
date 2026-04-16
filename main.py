from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from routers.stocks import router as stocks_router
from routers.tracking import router as tracking_router
from routers.backtest import router as backtest_router
import os
import sys
import logging
import traceback
from logging.handlers import RotatingFileHandler
from datetime import datetime

# ── 서버 측 로그 설정 ────────────────────────────────────────────────
SERVER_LOG_FILE = os.path.join(os.path.dirname(__file__), "server.log")
SERVER_LOG_MAX_BYTES = 2 * 1024 * 1024  # 2MB

_server_logger = logging.getLogger("server")
_server_logger.setLevel(logging.INFO)
_server_handler = RotatingFileHandler(
    SERVER_LOG_FILE, maxBytes=SERVER_LOG_MAX_BYTES, backupCount=1, encoding="utf-8"
)
_server_handler.setFormatter(
    logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
)
_server_logger.addHandler(_server_handler)
# 콘솔에도 출력 (Render stdout에서 계속 보이도록)
_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(
    logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
)
_server_logger.addHandler(_console_handler)


class _StdoutTee:
    """print() 출력을 원래 stdout + server.log 양쪽에 기록"""
    def __init__(self, original, level=logging.INFO):
        self.original = original
        self.level = level
        self._buffer = ""

    def write(self, msg):
        self.original.write(msg)
        self._buffer += msg
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            if line.strip():
                # 콘솔 핸들러가 중복 출력하지 않도록 파일만 기록
                _server_handler.emit(
                    logging.LogRecord(
                        "server", self.level, "", 0, line, None, None
                    )
                )

    def flush(self):
        self.original.flush()


sys.stdout = _StdoutTee(sys.__stdout__)
sys.stderr = _StdoutTee(sys.__stderr__, level=logging.ERROR)


app = FastAPI(title="US Stock News API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── 모든 핸들되지 않은 예외를 server.log에 기록 ─────────────────────
@app.exception_handler(Exception)
async def _log_unhandled(request: Request, exc: Exception):
    tb = traceback.format_exc()
    _server_logger.error(
        f"Unhandled {type(exc).__name__} at {request.method} {request.url.path}: {exc}\n{tb}"
    )
    return JSONResponse(status_code=500, content={"detail": str(exc)})


app.include_router(stocks_router)
app.include_router(tracking_router)
app.include_router(backtest_router)

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
