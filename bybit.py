from __future__ import annotations

import time
import os
import json
import hmac
import hashlib
from decimal import Decimal, ROUND_DOWN
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


# ─────────────────────────────────────────────
# Private API (Unified Trading / USDT perpetual)
# ─────────────────────────────────────────────
def _load_env_file() -> None:
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if not os.path.exists(env_path):
        return
    try:
        with open(env_path, "r", encoding="utf-8") as fh:
            for raw in fh:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip())
    except OSError:
        return


def private_api_configured() -> bool:
    _load_env_file()
    return bool(os.getenv("BYBIT_API_KEY") and os.getenv("BYBIT_API_SECRET"))


def _private_request(method: str, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    _load_env_file()
    api_key = os.getenv("BYBIT_API_KEY", "").strip()
    api_secret = os.getenv("BYBIT_API_SECRET", "").strip()
    if not api_key or not api_secret:
        raise BybitError("BYBIT_API_KEY 또는 BYBIT_API_SECRET이 없습니다.")

    method = method.upper()
    params = params or {}
    timestamp = str(int(time.time() * 1000))
    recv_window = "5000"
    if method == "GET":
        from urllib.parse import urlencode
        payload_text = urlencode(sorted((k, str(v)) for k, v in params.items()))
    else:
        payload_text = json.dumps(params, separators=(",", ":"), ensure_ascii=False)

    sign_payload = timestamp + api_key + recv_window + payload_text
    signature = hmac.new(
        api_secret.encode("utf-8"), sign_payload.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": recv_window,
        "X-BAPI-SIGN": signature,
        "Content-Type": "application/json",
    }
    try:
        if method == "GET":
            response = _SESSION.get(BASE_URL + path, params=params, headers=headers, timeout=15)
        else:
            response = _SESSION.post(BASE_URL + path, data=payload_text, headers=headers, timeout=15)
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        raise BybitError(f"Bybit 비공개 API 통신 오류: {exc}") from exc
    if payload.get("retCode") != 0:
        raise BybitError(f"{payload.get('retCode')}: {payload.get('retMsg', 'Bybit API 오류')}")
    return payload


def get_unified_wallet_balance() -> float:
    payload = _private_request("GET", "/v5/account/wallet-balance", {"accountType": "UNIFIED", "coin": "USDT"})
    rows = payload.get("result", {}).get("list", [])
    if not rows:
        return 0.0
    for coin in rows[0].get("coin", []):
        if coin.get("coin") == "USDT":
            return float(coin.get("walletBalance") or 0.0)
    return 0.0



def get_open_linear_positions() -> list[dict[str, Any]]:
    """Unified 계정의 열려 있는 USDT 선물 포지션을 전부 반환합니다.

    Bybit 앱/PC/이 프로그램 어디서 진입했는지는 구분하지 않고 실제 계정 상태를
    원본으로 사용합니다. 이 함수는 조회만 하며 TP/SL을 생성하거나 변경하지 않습니다.
    """
    positions: list[dict[str, Any]] = []
    cursor = ""
    while True:
        params: dict[str, Any] = {
            "category": "linear",
            "settleCoin": "USDT",
            "limit": 200,
        }
        if cursor:
            params["cursor"] = cursor
        payload = _private_request("GET", "/v5/position/list", params)
        result = payload.get("result", {})
        for row in result.get("list", []):
            size = float(row.get("size") or 0.0)
            side = str(row.get("side") or "")
            if size <= 0 or side not in {"Buy", "Sell"}:
                continue
            avg_price = float(row.get("avgPrice") or 0.0)
            mark_price = float(row.get("markPrice") or 0.0)
            leverage = float(row.get("leverage") or 0.0)
            position_value = float(row.get("positionValue") or 0.0)
            if position_value <= 0 and mark_price > 0:
                position_value = size * mark_price
            margin_estimate = position_value / leverage if leverage > 0 else 0.0
            if avg_price > 0:
                direction = 1.0 if side == "Buy" else -1.0
                price_pnl_pct = ((mark_price / avg_price) - 1.0) * 100.0 * direction
            else:
                price_pnl_pct = 0.0
            positions.append({
                "symbol": str(row.get("symbol") or ""),
                "side": "LONG" if side == "Buy" else "SHORT",
                "size": size,
                "avg_price": avg_price,
                "mark_price": mark_price,
                "leverage": leverage,
                "position_idx": int(row.get("positionIdx") or 0),
                "position_value": position_value,
                "margin_estimate": margin_estimate,
                "unrealised_pnl": float(row.get("unrealisedPnl") or 0.0),
                "price_pnl_pct": price_pnl_pct,
                "take_profit": float(row.get("takeProfit") or 0.0),
                "stop_loss": float(row.get("stopLoss") or 0.0),
                "liq_price": float(row.get("liqPrice") or 0.0),
                "updated_time": str(row.get("updatedTime") or ""),
            })
        cursor = str(result.get("nextPageCursor") or "")
        if not cursor:
            break
    return sorted(positions, key=lambda item: (item["side"], item["symbol"]))

def get_linear_position(symbol: str) -> dict[str, Any] | None:
    payload = _private_request("GET", "/v5/position/list", {"category": "linear", "symbol": symbol.upper()})
    rows = payload.get("result", {}).get("list", [])
    candidates = [r for r in rows if r.get("side") == "Buy" and float(r.get("size") or 0) > 0]
    if not candidates:
        return None
    row = candidates[0]
    return {
        "symbol": row.get("symbol"),
        "size": float(row.get("size") or 0.0),
        "avg_price": float(row.get("avgPrice") or 0.0),
        "mark_price": float(row.get("markPrice") or 0.0),
        "leverage": float(row.get("leverage") or 0.0),
        "position_idx": int(row.get("positionIdx") or 0),
        "unrealised_pnl": float(row.get("unrealisedPnl") or 0.0),
    }


def _instrument_rules(symbol: str) -> tuple[Decimal, Decimal, Decimal]:
    payload = _get("/v5/market/instruments-info", {"category": "linear", "symbol": symbol.upper()})
    rows = payload.get("result", {}).get("list", [])
    if not rows:
        raise BybitError(f"{symbol}: 주문 규칙을 찾지 못했습니다.")
    row = rows[0]
    lot = row.get("lotSizeFilter", {})
    price_filter = row.get("priceFilter", {})
    qty_step = Decimal(str(lot.get("qtyStep") or "0.001"))
    min_qty = Decimal(str(lot.get("minOrderQty") or qty_step))
    tick_size = Decimal(str(price_filter.get("tickSize") or "0.0001"))
    return qty_step, min_qty, tick_size


def _round_down(value: float, step: Decimal) -> str:
    dec = Decimal(str(value))
    rounded = (dec / step).to_integral_value(rounding=ROUND_DOWN) * step
    return format(rounded.normalize(), "f")


def _round_price(value: float, tick: Decimal) -> str:
    dec = Decimal(str(value))
    rounded = (dec / tick).to_integral_value(rounding=ROUND_DOWN) * tick
    return format(rounded.normalize(), "f")


def place_long_market_with_risk(
    symbol: str,
    margin_usdt: float,
    leverage: int,
    stop_price: float,
    tp_pct: float = 1.2,
    position_idx: int = 1,
) -> dict[str, Any]:
    """시장가 LONG 후 실제 평단 기준 TP와 구조 손절을 등록합니다.

    margin_usdt는 증거금이며, 주문 명목금액은 margin_usdt × leverage입니다.
    position_idx=1은 헤지모드 LONG입니다. 원웨이 계정이면 0을 사용합니다.
    """
    symbol = symbol.upper().strip()
    if margin_usdt <= 0 or leverage <= 0:
        raise BybitError("증거금과 레버리지는 0보다 커야 합니다.")
    current = get_ticker(symbol)
    qty_step, min_qty, tick_size = _instrument_rules(symbol)
    raw_qty = margin_usdt * leverage / current
    qty_text = _round_down(raw_qty, qty_step)
    if Decimal(qty_text) < min_qty:
        raise BybitError(f"최소 주문수량은 {min_qty}입니다. 증거금을 늘려주세요.")

    # 레버리지 설정. 이미 같은 값이면 Bybit가 오류를 줄 수 있어 해당 문구만 무시합니다.
    try:
        _private_request("POST", "/v5/position/set-leverage", {
            "category": "linear", "symbol": symbol,
            "buyLeverage": str(leverage), "sellLeverage": str(leverage),
        })
    except BybitError as exc:
        if "not modified" not in str(exc).lower() and "110043" not in str(exc):
            raise

    order_payload = {
        "category": "linear",
        "symbol": symbol,
        "side": "Buy",
        "orderType": "Market",
        "qty": qty_text,
        "timeInForce": "IOC",
        "positionIdx": int(position_idx),
    }
    order_result = _private_request("POST", "/v5/order/create", order_payload).get("result", {})

    # 시장가 체결 반영 대기
    position = None
    for _ in range(12):
        time.sleep(0.5)
        position = get_linear_position(symbol)
        if position and position.get("avg_price", 0) > 0:
            break
    if not position:
        raise BybitError("주문은 접수됐지만 체결 포지션을 아직 확인하지 못했습니다. Bybit 앱에서 확인하세요.")

    avg_price = float(position["avg_price"])
    actual_idx = int(position.get("position_idx", position_idx))
    tp_price = avg_price * (1 + tp_pct / 100)
    tp_text = _round_price(tp_price, tick_size)
    stop_text = _round_price(stop_price, tick_size)
    if float(stop_text) <= 0 or float(stop_text) >= avg_price:
        raise BybitError("손절가가 실제 평단보다 낮지 않아 TP/SL 등록을 중단했습니다. 포지션은 Bybit 앱에서 확인하세요.")

    _private_request("POST", "/v5/position/trading-stop", {
        "category": "linear",
        "symbol": symbol,
        "tpslMode": "Full",
        "positionIdx": actual_idx,
        "takeProfit": tp_text,
        "stopLoss": stop_text,
        "tpTriggerBy": "MarkPrice",
        "slTriggerBy": "MarkPrice",
    })
    return {
        "order_id": order_result.get("orderId", ""),
        "symbol": symbol,
        "qty": float(position["size"]),
        "avg_price": avg_price,
        "leverage": float(position.get("leverage") or leverage),
        "tp_price": float(tp_text),
        "stop_price": float(stop_text),
        "position_idx": actual_idx,
    }
