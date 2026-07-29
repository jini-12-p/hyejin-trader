from __future__ import annotations

import time

from typing import Any

import pandas as pd

import requests

BASE_URLS = [

    "https://api.bybit.com",

    "https://api.bytick.com",

]

_SESSION = requests.Session()

_SESSION.headers.update(

    {

        "User-Agent": (

            "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) "

            "AppleWebKit/605.1.15 Version/18.0 Mobile/15E148 Safari/604.1"

        ),

        "Accept": "application/json",

    }

)

class BybitError(RuntimeError):

    pass

def _get(

    path: str,

    params: dict[str, Any],

    retries: int = 2,

) -> dict[str, Any]:

    errors: list[str] = []

    for base_url in BASE_URLS:

        for attempt in range(retries):

            try:

                response = _SESSION.get(

                    base_url + path,

                    params=params,

                    timeout=15,

                )

                if response.status_code == 403:

                    raise BybitError(

                        f"{base_url}에서 403 차단됨"

                    )

                response.raise_for_status()

                payload = response.json()

                if payload.get("retCode") != 0:

                    raise BybitError(

                        payload.get("retMsg", "Bybit API 오류")

                    )

                return payload

            except (requests.RequestException, ValueError, BybitError) as exc:

                errors.append(

                    f"{base_url} "

                    f"{attempt + 1}/{retries}회: {exc}"

                )

                if attempt < retries - 1:

                    time.sleep(1.0 * (attempt + 1))

    error_text = " | ".join(errors)

    raise BybitError(

        "Bybit 공식 API 주소에 모두 접속하지 못했습니다. "

        f"{error_text}"

    )

def list_usdt_perpetual_symbols() -> list[str]:

    payload = _get(

        "/v5/market/instruments-info",

        {

            "category": "linear",

            "limit": 1000,

        },

    )

    rows = payload["result"]["list"]

    return sorted(

        row["symbol"]

        for row in rows

        if row.get("quoteCoin") == "USDT"

        and row.get("contractType") == "LinearPerpetual"

        and row.get("status") == "Trading"

    )

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

    return df.sort_values("start_time").reset_index(drop=True)
