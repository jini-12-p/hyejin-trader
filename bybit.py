from __future__ import annotations

import time
from typing import Any

import pandas as pd
import requests

BASE_URL = "https://api.bybit.com"
_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": "HJ-Trader/0.1"})


class BybitError(RuntimeError):
    pass


def _get(path: str, params: dict[str, Any], retries: int = 3) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            response = _SESSION.get(BASE_URL + path, params=params, timeout=12)
            response.raise_for_status()
            payload = response.json()
            if payload.get("retCode") != 0:
                raise BybitError(payload.get("retMsg", "Bybit API error"))
            return payload
        except (requests.RequestException, ValueError, BybitError) as exc:
            last_error = exc
            if attempt < retries - 1:
                time.sleep(0.7 * (attempt + 1))
    raise BybitError(f"Bybit 데이터를 가져오지 못했습니다: {last_error}")


def list_usdt_perpetual_symbols() -> list[str]:
    payload = _get(
        "/v5/market/instruments-info",
        {"category": "linear", "limit": 1000},
    )
    rows = payload["result"]["list"]
    return sorted(
        row["symbol"]
        for row in rows
        if row.get("quoteCoin") == "USDT"
        and row.get("contractType") == "LinearPerpetual"
        and row.get("status") == "Trading"
    )


def get_klines(symbol: str, interval: str = "15", limit: int = 240) -> pd.DataFrame:
    payload = _get(
        "/v5/market/kline",
        {
            "category": "linear",
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": limit,
        },
    )
    rows = payload["result"]["list"]
    if not rows:
        raise BybitError(f"{symbol}: 캔들 데이터가 없습니다.")

    columns = [
        "start_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "turnover",
    ]
    df = pd.DataFrame(rows, columns=columns)
    numeric = ["open", "high", "low", "close", "volume", "turnover"]
    df[numeric] = df[numeric].astype(float)
    df["start_time"] = pd.to_datetime(
        df["start_time"].astype("int64"), unit="ms", utc=True
    )
    return df.sort_values("start_time").reset_index(drop=True)
