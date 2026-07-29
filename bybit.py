from __future__ import annotations

import time

from typing import Any

import pandas as pd

import requests

BASE_URL = "https://fapi.binance.com"

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

) -> Any:

    last_error: Exception | None = None

    for attempt in range(retries):

        try:

            response = _SESSION.get(

                BASE_URL + path,

                params=params or {},

                timeout=15,

            )

            response.raise_for_status()

            return response.json()

        except (requests.RequestException, ValueError) as exc:

            last_error = exc

            if attempt < retries - 1:

                time.sleep(1.0 * (attempt + 1))

    raise BybitError(

        f"Binance 선물 데이터를 가져오지 못했습니다: {last_error}"

    )

def list_usdt_perpetual_symbols() -> list[str]:

    payload = _get("/fapi/v1/exchangeInfo")

    rows = payload["symbols"]

    return sorted(

        row["symbol"]

        for row in rows

        if row.get("quoteAsset") == "USDT"

        and row.get("contractType") == "PERPETUAL"

        and row.get("status") == "TRADING"

    )

def get_klines(

    symbol: str,

    interval: str = "15",

    limit: int = 240,

) -> pd.DataFrame:

    interval_map = {

        "1": "1m",

        "3": "3m",

        "5": "5m",

        "15": "15m",

        "30": "30m",

        "60": "1h",

        "240": "4h",

        "D": "1d",

    }

    binance_interval = interval_map.get(interval, interval)

    rows = _get(

        "/fapi/v1/klines",

        {

            "symbol": symbol.upper(),

            "interval": binance_interval,

            "limit": limit,

        },

    )

    if not rows:

        raise BybitError(f"{symbol}: 캔들 데이터가 없습니다.")

    columns = [

        "start_time",

        "open",

        "high",

        "low",

        "close",

        "volume",

        "close_time",

        "quote_volume",

        "trade_count",

        "taker_buy_base",

        "taker_buy_quote",

        "ignore",

    ]

    df = pd.DataFrame(rows, columns=columns)

    numeric_columns = [

        "open",

        "high",

        "low",

        "close",

        "volume",

        "quote_volume",

    ]

    df[numeric_columns] = df[numeric_columns].astype(float)

    df["turnover"] = df["quote_volume"]

    df["start_time"] = pd.to_datetime(

        df["start_time"].astype("int64"),

        unit="ms",

        utc=True,

    )

    return df[

        [

            "start_time",

            "open",

            "high",

            "low",

            "close",

            "volume",

            "turnover",

        ]

    ].sort_values("start_time").reset_index(drop=True)
