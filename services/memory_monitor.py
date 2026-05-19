"""
OOM 진단용 메모리 모니터링 헬퍼.

각 phase에서 mem_log("label") 호출 시 RSS(MB) + 변화량 + 514MB(Render free) 대비 % 출력.
psutil 미설치 환경에선 자동으로 no-op (서비스 자체는 죽지 않음).

출력 경로: 'stockapp.server' logger (main.py가 설정한 file + stdout 핸들러 모두 사용).
→ /server-logs 엔드포인트로 외부에서 진단 가능.
"""
import ctypes
import logging
import os
import threading
from typing import Optional

_logger = logging.getLogger("stockapp.server")

try:
    import psutil
    _proc = psutil.Process(os.getpid())
    _HAS_PSUTIL = True
except Exception:
    _proc = None
    _HAS_PSUTIL = False

# Linux glibc malloc_trim — gc.collect() 후 free된 heap 영역을 OS로 반환.
# Python 기본 할당자(pymalloc + glibc)는 free된 메모리를 process arena에 보유하므로
# gc.collect()를 해도 RSS가 줄지 않는다. malloc_trim(0)을 호출해야 OS에 반환됨.
try:
    _libc = ctypes.CDLL("libc.so.6")
    _libc.malloc_trim.argtypes = [ctypes.c_size_t]
    _libc.malloc_trim.restype = ctypes.c_int
    _HAS_MALLOC_TRIM = True
except Exception:
    _libc = None
    _HAS_MALLOC_TRIM = False


def trim_memory() -> bool:
    """glibc malloc_trim(0) 호출 — gc.collect() 직후에 부르면 RSS 대폭 감소 가능."""
    if not _HAS_MALLOC_TRIM:
        return False
    try:
        _libc.malloc_trim(0)
        return True
    except Exception:
        return False

# Render free tier 메모리 한도 (MB)
RENDER_LIMIT_MB = 512
# OOM 위험 임계 (이 이상이면 WARN/CRITICAL 레벨로 로깅)
WARN_THRESHOLD_MB = 380
CRITICAL_THRESHOLD_MB = 450

_lock = threading.Lock()
_last_rss_mb: Optional[float] = None


def get_rss_mb() -> Optional[float]:
    """현재 프로세스 RSS(MB). psutil 없으면 None."""
    if not _HAS_PSUTIL:
        return None
    try:
        return _proc.memory_info().rss / (1024 * 1024)
    except Exception:
        return None


def mem_log(label: str, force_print: bool = True) -> Optional[float]:
    """
    메모리 사용량 로그 출력 — server.log(file) + stdout 양쪽에 기록.
    - label: 구분용 라벨 (예: 'historical-context Phase 1 시작')
    - 반환: 현재 RSS(MB) or None

    출력 예시:
      [MEM]  185.3 MB ( 36%, Δ+12.4) | Phase 1 차트 fetch 완료
      [MEM!] 395.1 MB ( 77%, Δ+45.2) | Phase 2 펀더멘털 완료 (WARN)
      [MEM*] 478.2 MB ( 93%, Δ+83.1) | 응답 직렬화 직전 (CRITICAL)
    """
    global _last_rss_mb
    rss = get_rss_mb()
    if rss is None:
        if force_print:
            _logger.warning(f"[MEM?] (psutil unavailable) | {label}")
        return None

    with _lock:
        delta = (rss - _last_rss_mb) if _last_rss_mb is not None else 0.0
        _last_rss_mb = rss

    pct = rss / RENDER_LIMIT_MB * 100
    sign = "+" if delta >= 0 else ""
    msg = f"{rss:6.1f} MB ({pct:3.0f}%, Δ{sign}{delta:5.1f}) | {label}"

    # 임계별로 로그 레벨 달리해서 server.log + stdout 양쪽에 기록.
    # _server_logger는 main.py에서 file handler + stdout handler 모두 부착.
    if rss >= CRITICAL_THRESHOLD_MB:
        _logger.error(f"[MEM*CRIT] {msg}")
    elif rss >= WARN_THRESHOLD_MB:
        _logger.warning(f"[MEM!WARN] {msg}")
    else:
        _logger.info(f"[MEM] {msg}")
    return rss


def reset_baseline():
    """delta 계산용 baseline 리셋 (요청 시작 시 호출)."""
    global _last_rss_mb
    with _lock:
        _last_rss_mb = get_rss_mb()
