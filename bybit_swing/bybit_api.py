from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from typing import Any
from urllib.parse import urlencode

import pandas as pd
import requests


class BybitSwingError(RuntimeError):
    pass


class BybitSwingClient:
    """Bybit V5 REST client for USDT perpetual futures.

    PAPER mode uses public market data only. Private order methods are included for
    a later subaccount rollout; withdrawal endpoints are intentionally absent.
    """

    def __init__(self, api_key: str = "", secret_key: str = "", demo: bool = True):
        self.api_key = api_key or os.getenv("BYBIT_SWING_API_KEY", "")
        self.secret_key = secret_key or os.getenv("BYBIT_SWING_API_SECRET", "")
        self.demo = demo
        self.base_url = os.getenv("BYBIT_SWING_BASE_URL", "https://api.bybit.com")
        self.session = requests.Session()

    @property
    def private_configured(self) -> bool:
        return bool(self.api_key and self.secret_key)

    def _request(self, method: str, path: str, params: dict[str, Any] | None = None,
                 payload: dict[str, Any] | None = None, private: bool = False) -> dict[str, Any]:
        params = params or {}
        payload = payload or {}
        headers: dict[str, str] = {"Content-Type": "application/json"}
        body = ""
        query = urlencode({k: v for k, v in params.items() if v is not None})
        if private:
            if not self.private_configured:
                raise BybitSwingError("Bybit 서브계정 API 키/시크릿이 설정되지 않았습니다.")
            ts = str(int(time.time() * 1000))
            recv_window = "5000"
            sign_payload = query if method.upper() == "GET" else json.dumps(payload, separators=(",", ":"))
            signature = hmac.new(
                self.secret_key.encode(),
                f"{ts}{self.api_key}{recv_window}{sign_payload}".encode(),
                hashlib.sha256,
            ).hexdigest()
            headers.update({
                "X-BAPI-API-KEY": self.api_key,
                "X-BAPI-TIMESTAMP": ts,
                "X-BAPI-RECV-WINDOW": recv_window,
                "X-BAPI-SIGN": signature,
            })
            body = sign_payload if method.upper() != "GET" else ""
        url = self.base_url + path + (("?" + query) if query and method.upper() == "GET" else "")
        try:
            response = self.session.request(method, url, data=body or None, headers=headers, timeout=15)
            response.raise_for_status()
            data = response.json()
        except Exception as exc:
            raise BybitSwingError(f"Bybit 요청 실패: {exc}") from exc
        if int(data.get("retCode", -1)) != 0:
            raise BybitSwingError(f"Bybit 오류 {data.get('retCode')}: {data.get('retMsg')}")
        return data.get("result", {})

    def candles(self, symbol: str, interval: str = "15", limit: int = 200) -> pd.DataFrame:
        interval_map = {"5m": "5", "15m": "15", "1H": "60", "1h": "60", "60": "60"}
        iv = interval_map.get(str(interval), str(interval))
        result = self._request("GET", "/v5/market/kline", {
            "category": "linear", "symbol": symbol, "interval": iv, "limit": min(int(limit), 1000)
        })
        rows = result.get("list", [])
        columns = ["ts", "open", "high", "low", "close", "volume", "turnover"]
        frame = pd.DataFrame(rows, columns=columns[:len(rows[0])] if rows else columns)
        if frame.empty:
            return frame
        for col in ["open", "high", "low", "close", "volume", "turnover"]:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
        frame["start_time"] = pd.to_datetime(pd.to_numeric(frame["ts"]), unit="ms", utc=True)
        frame["confirm"] = "1"
        return frame.sort_values("start_time").reset_index(drop=True)

    def ticker(self, symbol: str) -> dict[str, Any]:
        result = self._request("GET", "/v5/market/tickers", {"category": "linear", "symbol": symbol})
        rows = result.get("list", [])
        if not rows:
            return {}
        row = dict(rows[0])
        return {**row, "last": row.get("lastPrice", "0")}

    def tickers(self, inst_type: str = "linear") -> list[dict[str, Any]]:
        result = self._request("GET", "/v5/market/tickers", {"category": "linear"})
        return list(result.get("list", []))

    def instruments(self, inst_type: str = "linear") -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        cursor = ""
        while True:
            params = {"category": "linear", "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            result = self._request("GET", "/v5/market/instruments-info", params)
            rows.extend(result.get("list", []))
            cursor = str(result.get("nextPageCursor") or "")
            if not cursor:
                return rows

    def balance(self, ccy: str = "USDT") -> float:
        result = self._request("GET", "/v5/account/wallet-balance", {"accountType": "UNIFIED", "coin": ccy}, private=True)
        for account in result.get("list", []):
            for coin in account.get("coin", []):
                if coin.get("coin") == ccy:
                    return float(coin.get("walletBalance") or coin.get("equity") or 0)
        return 0.0

    def set_leverage(self, symbol: str, leverage: int, margin_mode: str = "isolated", pos_side: str = "long") -> None:
        # Margin mode is configured on the subaccount; leverage is set per symbol.
        self._request("POST", "/v5/position/set-leverage", payload={
            "category": "linear", "symbol": symbol,
            "buyLeverage": str(leverage), "sellLeverage": str(leverage)
        }, private=True)

    def place_market_order(self, symbol: str, side: str, size: str, margin_mode: str = "isolated",
                           pos_side: str = "long", reduce_only: bool = False, client_order_id: str = "") -> dict[str, Any]:
        payload: dict[str, Any] = {
            "category": "linear", "symbol": symbol,
            "side": "Buy" if side.lower() == "buy" else "Sell",
            "orderType": "Market", "qty": str(size),
            "reduceOnly": bool(reduce_only), "timeInForce": "IOC",
            "positionIdx": 1,
        }
        if client_order_id:
            payload["orderLinkId"] = client_order_id[:36]
        result = self._request("POST", "/v5/order/create", payload=payload, private=True)
        return result
