from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from routers.stocks import router as stocks_router
import os

app = FastAPI(title="US Stock News API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(stocks_router)

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


@app.get("/")
def root():
    return {"message": "US Stock News API is running"}
