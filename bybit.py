from __future__ import annotations

import time
from typing import Any

import pandas as pd
import requests

BASE_URL = "https://api.bybit.com"
_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": "HJ-Trader/0.4", "Accept": "application/json"})


class BybitError(RuntimeError):
    pass


def _get(path: str, params: dict[str, Any] | None = None, retries: int = 3) -> dict[str, Any]:
    last: Exception | None = None
    for attempt in range(retries):
        try:
            response = _SESSION.get(BASE_URL + path, params=params or {}, timeout=12)
            response.raise_for_status()
            payload = response.json()
            if payload.get("retCode") != 0:
                raise BybitError(payload.get("retMsg", "Bybit API 오류"))
            return payload
        except (requests.RequestException, ValueError, BybitError) as exc:
            last = exc
            if attempt < retries - 1:
                time.sleep(attempt + 1)
    raise BybitError(f"Bybit 데이터를 가져오지 못했습니다: {last}")


def list_usdt_perpetual_symbols() -> list[str]:
    out: list[str] = []
    cursor = ""
    while True:
        params: dict[str, Any] = {"category": "linear", "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        result = _get("/v5/market/instruments-info", params)["result"]
        out.extend(
            row["symbol"]
            for row in result.get("list", [])
            if row.get("quoteCoin") == "USDT"
            and row.get("contractType") == "LinearPerpetual"
            and row.get("status") == "Trading"
        )
        cursor = result.get("nextPageCursor", "")
        if not cursor:
            break
    return sorted(set(out))


def get_linear_tickers() -> pd.DataFrame:
    """USDT 무기한 전체 티커를 24시간 등락률 순으로 반환합니다."""
    rows = _get("/v5/market/tickers", {"category": "linear"})["result"].get("list", [])
    if not rows:
        raise BybitError("전체 티커 데이터가 없습니다.")

    frame = pd.DataFrame(rows)
    if frame.empty or "symbol" not in frame:
        raise BybitError("전체 티커 형식이 올바르지 않습니다.")

    frame = frame[frame["symbol"].astype(str).str.endswith("USDT")].copy()
    for column in ["lastPrice", "price24hPcnt", "turnover24h", "volume24h"]:
        if column not in frame:
            frame[column] = 0.0
        frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0.0)

    frame["change_24h_pct"] = frame["price24hPcnt"] * 100.0
    frame = frame.sort_values(
        ["change_24h_pct", "turnover24h"],
        ascending=[False, False],
    ).reset_index(drop=True)
    frame["gainer_rank"] = frame.index + 1
    return frame


def get_top_gainers(limit: int = 30) -> list[dict[str, Any]]:
    """24시간 상승률 상위 USDT 무기한 종목을 반환합니다."""
    safe_limit = max(1, int(limit))
    frame = get_linear_tickers().head(safe_limit)
    return frame[
        ["symbol", "lastPrice", "change_24h_pct", "turnover24h", "gainer_rank"]
    ].to_dict("records")


def get_ticker(symbol: str) -> float:
    rows = _get(
        "/v5/market/tickers",
        {"category": "linear", "symbol": symbol.upper()},
    )["result"].get("list", [])
    if not rows:
        raise BybitError(f"{symbol}: 현재가가 없습니다.")
    return float(rows[0]["lastPrice"])


def get_klines(symbol: str, interval: str = "15", limit: int = 240) -> pd.DataFrame:
    rows = _get(
        "/v5/market/kline",
        {
            "category": "linear",
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": limit,
        },
    )["result"].get("list", [])
    if not rows:
        raise BybitError(f"{symbol}: 캔들 데이터가 없습니다.")

    frame = pd.DataFrame(
        rows,
        columns=["start_time", "open", "high", "low", "close", "volume", "turnover"],
    )
    for column in ["open", "high", "low", "close", "volume", "turnover"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["start_time"] = pd.to_datetime(
        frame["start_time"].astype("int64"), unit="ms", utc=True
    )
    return frame.sort_values("start_time").reset_index(drop=True)
