from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode

import pandas as pd
import requests


class OKXError(RuntimeError):
    pass


class OKXClient:
    """Small OKX REST v5 client.

    demo=True adds the official x-simulated-trading header. Withdraw endpoints are
    intentionally not implemented.
    """

    def __init__(self, api_key: str = "", secret_key: str = "", passphrase: str = "", demo: bool = True):
        self.api_key = api_key or os.getenv("OKX_API_KEY", "")
        self.secret_key = secret_key or os.getenv("OKX_SECRET_KEY", "")
        self.passphrase = passphrase or os.getenv("OKX_PASSPHRASE", "")
        self.demo = demo
        self.base_url = os.getenv("OKX_BASE_URL", "https://www.okx.com")
        self.session = requests.Session()

    @property
    def private_configured(self) -> bool:
        return bool(self.api_key and self.secret_key and self.passphrase)

    @staticmethod
    def _timestamp() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    def _headers(self, method: str, request_path: str, body: str = "") -> dict[str, str]:
        ts = self._timestamp()
        prehash = f"{ts}{method.upper()}{request_path}{body}"
        signature = base64.b64encode(
            hmac.new(self.secret_key.encode(), prehash.encode(), hashlib.sha256).digest()
        ).decode()
        headers = {
            "Content-Type": "application/json",
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": ts,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
        }
        if self.demo:
            headers["x-simulated-trading"] = "1"
        return headers

    def _request(self, method: str, path: str, params: dict[str, Any] | None = None,
                 payload: dict[str, Any] | None = None, private: bool = False) -> list[dict[str, Any]]:
        params = params or {}
        query = urlencode({k: v for k, v in params.items() if v is not None})
        request_path = path + (f"?{query}" if query else "")
        body = json.dumps(payload, separators=(",", ":")) if payload else ""
        headers = self._headers(method, request_path, body) if private else ({"x-simulated-trading": "1"} if self.demo else {})
        if private and not self.private_configured:
            raise OKXError("OKX API 키/시크릿/패스프레이즈가 설정되지 않았습니다.")
        try:
            response = self.session.request(
                method, self.base_url + request_path, data=body or None, headers=headers, timeout=15
            )
            response.raise_for_status()
            data = response.json()
        except Exception as exc:
            raise OKXError(f"OKX 요청 실패: {exc}") from exc
        if str(data.get("code", "0")) != "0":
            raise OKXError(f"OKX 오류 {data.get('code')}: {data.get('msg')}")
        return data.get("data", [])

    def candles(self, inst_id: str, bar: str = "1H", limit: int = 200) -> pd.DataFrame:
        rows = self._request("GET", "/api/v5/market/candles", {"instId": inst_id, "bar": bar, "limit": limit})
        columns = ["ts", "open", "high", "low", "close", "volume", "vol_ccy", "vol_quote", "confirm"]
        frame = pd.DataFrame(rows, columns=columns[: len(rows[0])] if rows else columns)
        if frame.empty:
            return frame
        for col in ["open", "high", "low", "close", "volume", "vol_ccy", "vol_quote"]:
            if col in frame:
                frame[col] = pd.to_numeric(frame[col], errors="coerce")
        frame["start_time"] = pd.to_datetime(pd.to_numeric(frame["ts"]), unit="ms", utc=True)
        frame = frame.sort_values("start_time").reset_index(drop=True)
        return frame

    def ticker(self, inst_id: str) -> dict[str, Any]:
        rows = self._request("GET", "/api/v5/market/ticker", {"instId": inst_id})
        return rows[0] if rows else {}

    def tickers(self, inst_type: str = "SWAP") -> list[dict[str, Any]]:
        """Return all public tickers for an instrument type."""
        return self._request("GET", "/api/v5/market/tickers", {"instType": inst_type})

    def instruments(self, inst_type: str = "SWAP") -> list[dict[str, Any]]:
        """Return currently listed public instruments."""
        return self._request("GET", "/api/v5/public/instruments", {"instType": inst_type})

    def balance(self, ccy: str = "USDT") -> float:
        rows = self._request("GET", "/api/v5/account/balance", {"ccy": ccy}, private=True)
        if not rows:
            return 0.0
        for detail in rows[0].get("details", []):
            if detail.get("ccy") == ccy:
                return float(detail.get("availBal") or detail.get("availEq") or 0)
        return 0.0

    def positions(self, inst_type: str = "SWAP") -> list[dict[str, Any]]:
        return self._request("GET", "/api/v5/account/positions", {"instType": inst_type}, private=True)

    def set_leverage(self, inst_id: str, leverage: int, margin_mode: str = "isolated", pos_side: str = "long") -> None:
        payload = {"instId": inst_id, "lever": str(leverage), "mgnMode": margin_mode, "posSide": pos_side}
        self._request("POST", "/api/v5/account/set-leverage", payload=payload, private=True)

    def place_market_order(self, inst_id: str, side: str, size: str, margin_mode: str = "isolated",
                           pos_side: str = "long", reduce_only: bool = False, client_order_id: str = "") -> dict[str, Any]:
        payload: dict[str, Any] = {
            "instId": inst_id,
            "tdMode": margin_mode,
            "side": side,
            "ordType": "market",
            "sz": str(size),
            "posSide": pos_side,
            "reduceOnly": reduce_only,
        }
        if client_order_id:
            payload["clOrdId"] = client_order_id[:32]
        rows = self._request("POST", "/api/v5/trade/order", payload=payload, private=True)
        if not rows:
            raise OKXError("주문 응답이 비어 있습니다.")
        row = rows[0]
        if str(row.get("sCode", "0")) != "0":
            raise OKXError(f"주문 실패 {row.get('sCode')}: {row.get('sMsg')}")
        return row
