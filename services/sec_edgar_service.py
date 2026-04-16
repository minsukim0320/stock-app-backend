"""
SEC EDGAR 공시일 조회 서비스 — 과거 시뮬레이션 펀더멘털의 look-ahead bias 완전 제거.

yfinance quarterly_financials 컬럼은 분기 period-end 날짜(예: 2026-01-31)인데,
실제 10-Q 공시는 보통 period-end 후 30~45일 지나야 나옴. 이 갭이 look-ahead bias.

이 모듈은 SEC EDGAR의 실제 공시일(filingDate)을 조회해서,
target_date 시점에 실제로 공개된 분기만 골라 쓸 수 있게 해준다.

- 미국 종목(10-Q, 10-K): 정확한 공시일 적용
- 미국 외 종목: CIK 매핑 없음 → 빈 dict 반환 → 호출 측에서 빈 펀더멘털 처리
"""
import requests
import threading
from datetime import datetime, timedelta
from typing import Optional

# SEC는 User-Agent 헤더 요구 (Ban 방지)
_USER_AGENT = "stockapp-research contact@stockapp.local"
_CIK_MAP_URL = "https://www.sec.gov/files/company_tickers.json"
_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"

# 프로세스 수명 동안 공유되는 캐시 (SEC 데이터는 거의 변경되지 않음)
_cik_cache: Optional[dict] = None
_filings_cache: dict = {}
_cache_lock = threading.Lock()


def _load_cik_map() -> dict:
    """티커 → CIK(10자리 zero-padded) 매핑. 서버당 1회만 조회 후 메모리 캐싱."""
    global _cik_cache
    if _cik_cache is not None:
        return _cik_cache
    with _cache_lock:
        if _cik_cache is not None:
            return _cik_cache
        try:
            resp = requests.get(
                _CIK_MAP_URL,
                headers={"User-Agent": _USER_AGENT},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            _cik_cache = {
                row["ticker"].upper(): f"{row['cik_str']:010d}"
                for row in data.values()
            }
            print(f"[INFO] SEC CIK map loaded: {len(_cik_cache)} tickers")
        except Exception as e:
            print(f"[ERROR] SEC CIK map load failed: {e}")
            _cik_cache = {}
    return _cik_cache


def _fetch_filings(cik: str) -> list:
    """CIK의 10-Q/10-K 공시 이력 조회. (form, filingDate, reportDate) 튜플 리스트. 캐싱."""
    if cik in _filings_cache:
        return _filings_cache[cik]
    with _cache_lock:
        if cik in _filings_cache:
            return _filings_cache[cik]
        try:
            resp = requests.get(
                _SUBMISSIONS_URL.format(cik=cik),
                headers={"User-Agent": _USER_AGENT},
                timeout=15,
            )
            resp.raise_for_status()
            recent = resp.json().get("filings", {}).get("recent", {})
            forms = recent.get("form", [])
            fds = recent.get("filingDate", [])
            rds = recent.get("reportDate", [])
            result = [
                (f, fd, rd)
                for f, fd, rd in zip(forms, fds, rds)
                if f in ("10-Q", "10-K") and fd and rd
            ]
            _filings_cache[cik] = result
        except Exception as e:
            print(f"[ERROR] SEC filings fetch failed for CIK {cik}: {e}")
            _filings_cache[cik] = []
    return _filings_cache[cik]


def get_filing_map(ticker: str, target_date: str) -> dict:
    """
    {reportDate: filingDate} — target_date 이전에 실제로 공시된 10-Q/10-K만.

    - ticker가 SEC에 없으면 (미국 외 종목) 빈 dict
    - target_date 이전에 공시된 게 전혀 없어도 빈 dict

    반환 예시:
      {
        "2025-10-26": "2025-11-20",  # period end → filing date
        "2025-07-27": "2025-08-22",
        ...
      }
    """
    cik_map = _load_cik_map()
    cik = cik_map.get(ticker.upper())
    if not cik:
        return {}
    filings = _fetch_filings(cik)
    return {
        rd: fd
        for (_, fd, rd) in filings
        if fd < target_date
    }


def match_column_to_report_date(col_ts, filing_map: dict, tolerance_days: int = 7) -> Optional[str]:
    """
    yfinance quarterly_financials 컬럼(pd.Timestamp) → SEC reportDate 매칭.
    yfinance가 보고 period-end를 1~2일 오차로 표기하는 경우 있어 tolerance로 흡수.

    매칭되면 filing_map의 키(reportDate 문자열) 반환, 아니면 None.
    """
    if not filing_map:
        return None
    try:
        col_dt = col_ts.to_pydatetime().replace(tzinfo=None) if hasattr(col_ts, "to_pydatetime") else col_ts
    except Exception:
        return None
    col_str = col_dt.strftime("%Y-%m-%d")
    # 1) exact match 우선
    if col_str in filing_map:
        return col_str
    # 2) tolerance 범위에서 가장 가까운 것
    best = None
    best_diff = timedelta(days=tolerance_days + 1)
    for rd_str in filing_map:
        try:
            rd_dt = datetime.strptime(rd_str, "%Y-%m-%d")
        except Exception:
            continue
        diff = abs(col_dt - rd_dt)
        if diff <= timedelta(days=tolerance_days) and diff < best_diff:
            best = rd_str
            best_diff = diff
    return best
