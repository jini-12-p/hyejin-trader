from __future__ import annotations

import time
from typing import Any

import pandas as pd
import requests

BASE_URL = "https://api.bybit.com"

_SESSION = requests.Session()
_SESSION.headers.update(
    {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
    }
)


class BybitError(RuntimeError):
    pass


def _get(
    path: str,
    params: dict[str, Any] | None = None,
    retries: int = 3,
) -> dict[str, Any]:
    last_error: Exception | None = None

    for attempt in range(retries):
        try:
            response = _SESSION.get(
                BASE_URL + path,
                params=params or {},
                timeout=15,
            )
            response.raise_for_status()
            payload = response.json()

            if payload.get("retCode") != 0:
                raise BybitError(
                    f"Bybit 오류 {payload.get('retCode')}: "
                    f"{payload.get('retMsg', '알 수 없는 오류')}"
                )

            return payload
        except (requests.RequestException, ValueError, BybitError) as exc:
            last_error = exc
            if attempt < retries - 1:
                time.sleep(1.0 * (attempt + 1))

    raise BybitError(f"Bybit 데이터를 가져오지 못했습니다: {last_error}")


def list_usdt_perpetual_symbols() -> list[str]:
    symbols: list[str] = []
    cursor = ""

    while True:
        params: dict[str, Any] = {
            "category": "linear",
            "limit": 1000,
        }
        if cursor:
            params["cursor"] = cursor

        payload = _get("/v5/market/instruments-info", params)
        result = payload.get("result", {})
        rows = result.get("list", [])

        symbols.extend(
            row["symbol"]
            for row in rows
            if row.get("quoteCoin") == "USDT"
            and row.get("contractType") == "LinearPerpetual"
            and row.get("status") == "Trading"
        )

        cursor = result.get("nextPageCursor", "")
        if not cursor:
            break

    return sorted(set(symbols))


def get_klines(
    symbol: str,
    interval: str = "15",
    limit: int = 240,
) -> pd.DataFrame:
    payload = _get(
        "/v5/market/kline",
        {
            "category": "linear",
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": limit,
        },
    )

    rows = payload.get("result", {}).get("list", [])
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
    numeric_columns = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "turnover",
    ]
    df[numeric_columns] = df[numeric_columns].astype(float)

    df["start_time"] = pd.to_datetime(
        df["start_time"].astype("int64"),
        unit="ms",
        utc=True,
    )

    return (
        df[
            [
                "start_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "turnover",
            ]
        ]
        .sort_values("start_time")
        .reset_index(drop=True)
    )
