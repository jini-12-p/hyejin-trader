from __future__ import annotations

import json
import math
import sqlite3
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from Okx_swing.okx_api import OKXClient, OKXError

DB_PATH = Path(__file__).with_name("okx_swing_bot.db")
CONFIG_PATH = Path(__file__).with_name("config.json")


@dataclass
class SwingConfig:
    symbols: tuple[str, ...] = ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP")
    mode: str = "paper"  # paper | demo | live
    leverage: int = 2
    margin_mode: str = "isolated"
    max_positions: int = 1
    first_margin_usdt: float = 5.0
    dca1_margin_usdt: float = 5.0
    dca2_margin_usdt: float = 8.0
    dca1_drop_pct: float = 2.0
    dca2_drop_pct: float = 4.0
    tp1_pct: float = 1.5
    tp2_pct: float = 3.0
    hard_stop_pct: float = 6.0
    max_hold_hours: int = 72
    min_balance_to_trade: float = 90.0
    emergency_stop_balance: float = 85.0
    scan_seconds: int = 60

    @classmethod
    def load(cls) -> "SwingConfig":
        if not CONFIG_PATH.exists():
            cfg = cls()
            CONFIG_PATH.write_text(json.dumps(asdict(cfg), ensure_ascii=False, indent=2), encoding="utf-8")
            return cfg
        raw = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        if "symbols" in raw:
            raw["symbols"] = tuple(raw["symbols"])
        return cls(**raw)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=20)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS bot_positions (
            symbol TEXT PRIMARY KEY, status TEXT NOT NULL, opened_at TEXT NOT NULL,
            updated_at TEXT NOT NULL, avg_price REAL NOT NULL, total_qty REAL NOT NULL,
            total_margin REAL NOT NULL, dca_count INTEGER NOT NULL DEFAULT 0,
            tp1_done INTEGER NOT NULL DEFAULT 0, last_price REAL,
            unrealized_pct REAL, note TEXT
        );
        CREATE TABLE IF NOT EXISTS bot_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL, symbol TEXT,
            event TEXT NOT NULL, price REAL, qty REAL, mode TEXT, details TEXT
        );
        CREATE TABLE IF NOT EXISTS bot_state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        """)


def log_event(symbol: str, event: str, price: float = 0, qty: float = 0, mode: str = "", details: str = "") -> None:
    with db() as conn:
        conn.execute(
            "INSERT INTO bot_events(ts,symbol,event,price,qty,mode,details) VALUES(?,?,?,?,?,?,?)",
            (utc_now(), symbol, event, price, qty, mode, details),
        )


def indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["ema20"] = out["close"].ewm(span=20, adjust=False).mean()
    out["ema60"] = out["close"].ewm(span=60, adjust=False).mean()
    delta = out["close"].diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / 14, adjust=False).mean().replace(0, math.nan)
    out["rsi"] = (100 - 100 / (1 + gain / loss)).fillna(50)
    out["vol_avg"] = out["volume"].rolling(20).mean()
    return out


def entry_signal(client: OKXClient, symbol: str) -> tuple[bool, dict[str, Any]]:
    h1 = indicators(client.candles(symbol, "1H", 180))
    h4 = indicators(client.candles(symbol, "4H", 120))
    if len(h1) < 70 or len(h4) < 70:
        return False, {"reason": "캔들 부족"}
    # last candle can still be open; use last confirmed candle when confirm is available.
    c1 = h1[h1.get("confirm", "1").astype(str) == "1"] if "confirm" in h1 else h1.iloc[:-1]
    c4 = h4[h4.get("confirm", "1").astype(str) == "1"] if "confirm" in h4 else h4.iloc[:-1]
    if len(c1) < 2 or len(c4) < 2:
        return False, {"reason": "마감봉 부족"}
    row, prev, row4 = c1.iloc[-1], c1.iloc[-2], c4.iloc[-1]
    trend4 = row4.ema20 > row4.ema60 and row4.ema20 >= c4.iloc[-2].ema20
    trend1 = row.ema20 > row.ema60 and row.ema20 >= prev.ema20
    pullback = c1.tail(8).low.min() <= row.ema20 * 1.01
    bullish = row.close > row.open and row.close > prev.high
    rsi_ok = 48 <= row.rsi <= 68
    volume_ok = pd.notna(row.vol_avg) and row.volume >= row.vol_avg * 0.9
    ok = all([trend4, trend1, pullback, bullish, rsi_ok, volume_ok])
    return ok, {
        "price": float(row.close), "trend4": trend4, "trend1": trend1,
        "pullback": pullback, "bullish": bullish, "rsi": float(row.rsi),
        "volume_ok": volume_ok,
    }


def structure_alive(client: OKXClient, symbol: str) -> tuple[bool, dict[str, Any]]:
    h1 = indicators(client.candles(symbol, "1H", 120))
    h4 = indicators(client.candles(symbol, "4H", 100))
    if len(h1) < 65 or len(h4) < 65:
        return False, {"reason": "캔들 부족"}
    r1, r4 = h1.iloc[-2], h4.iloc[-2]
    recent_low = float(h1.iloc[-14:-2].low.min())
    alive = bool(r4.close >= r4.ema60 and r1.close >= recent_low and r1.ema20 >= r1.ema60)
    return alive, {"recent_low": recent_low, "h1_close": float(r1.close), "h4_ema60": float(r4.ema60)}


def qty_from_margin(price: float, margin_usdt: float, leverage: int) -> float:
    return max(0.0, margin_usdt * leverage / price)


class SwingBot:
    def __init__(self, config: SwingConfig | None = None):
        self.cfg = config or SwingConfig.load()
        # paper does not need credentials; demo/live do.
        self.client = OKXClient(demo=self.cfg.mode != "live")
        init_db()

    def open_rows(self) -> list[sqlite3.Row]:
        with db() as conn:
            return conn.execute("SELECT * FROM bot_positions WHERE status='OPEN' ORDER BY opened_at").fetchall()

    def _execute(self, symbol: str, side: str, qty: float, reduce_only: bool = False) -> None:
        if self.cfg.mode == "paper":
            return
        if self.cfg.mode == "live" and not self.client.private_configured:
            raise OKXError("LIVE 모드인데 API 설정이 없습니다.")
        self.client.set_leverage(symbol, self.cfg.leverage, self.cfg.margin_mode, "long")
        self.client.place_market_order(
            symbol, side, f"{qty:.8f}", self.cfg.margin_mode, "long", reduce_only,
            client_order_id=f"HJ{int(time.time())}{symbol[:4]}"
        )

    def _add_position(self, symbol: str, price: float, margin: float, dca: bool = False) -> None:
        qty = qty_from_margin(price, margin, self.cfg.leverage)
        self._execute(symbol, "buy", qty)
        with db() as conn:
            row = conn.execute("SELECT * FROM bot_positions WHERE symbol=?", (symbol,)).fetchone()
            if row:
                old_qty, old_avg = float(row["total_qty"]), float(row["avg_price"])
                total_qty = old_qty + qty
                avg = (old_avg * old_qty + price * qty) / total_qty
                conn.execute(
                    "UPDATE bot_positions SET avg_price=?,total_qty=?,total_margin=total_margin+?,dca_count=dca_count+1,updated_at=?,note=? WHERE symbol=?",
                    (avg, total_qty, margin, utc_now(), "조건부 물타기", symbol),
                )
            else:
                conn.execute(
                    "INSERT INTO bot_positions(symbol,status,opened_at,updated_at,avg_price,total_qty,total_margin,dca_count,tp1_done,last_price,unrealized_pct,note) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (symbol, "OPEN", utc_now(), utc_now(), price, qty, margin, 0, 0, price, 0.0, "BUY-P 스윙 진입"),
                )
        log_event(symbol, "DCA" if dca else "ENTRY", price, qty, self.cfg.mode, f"margin={margin}")

    def _close_qty(self, row: sqlite3.Row, price: float, fraction: float, reason: str) -> None:
        qty = float(row["total_qty"]) * fraction
        self._execute(row["symbol"], "sell", qty, reduce_only=True)
        remaining = max(0.0, float(row["total_qty"]) - qty)
        with db() as conn:
            if remaining <= 1e-12:
                conn.execute("UPDATE bot_positions SET status='CLOSED',total_qty=0,updated_at=?,last_price=?,note=? WHERE symbol=?",
                             (utc_now(), price, reason, row["symbol"]))
            else:
                conn.execute("UPDATE bot_positions SET total_qty=?,tp1_done=1,updated_at=?,last_price=?,note=? WHERE symbol=?",
                             (remaining, utc_now(), price, reason, row["symbol"]))
        log_event(row["symbol"], reason, price, qty, self.cfg.mode)

    def manage(self) -> None:
        for row in self.open_rows():
            symbol = row["symbol"]
            ticker = self.client.ticker(symbol)
            price = float(ticker.get("last") or 0)
            if price <= 0:
                continue
            avg = float(row["avg_price"])
            pnl = (price / avg - 1) * 100
            age_h = (datetime.now(timezone.utc) - datetime.fromisoformat(row["opened_at"])).total_seconds() / 3600
            alive, details = structure_alive(self.client, symbol)
            with db() as conn:
                conn.execute("UPDATE bot_positions SET last_price=?,unrealized_pct=?,updated_at=? WHERE symbol=?",
                             (price, pnl, utc_now(), symbol))

            if pnl <= -self.cfg.hard_stop_pct or not alive or age_h >= self.cfg.max_hold_hours:
                reason = "HARD_STOP" if pnl <= -self.cfg.hard_stop_pct else "STRUCTURE_EXIT" if not alive else "TIME_EXIT"
                self._close_qty(row, price, 1.0, reason)
                continue
            if int(row["tp1_done"]) == 0 and pnl >= self.cfg.tp1_pct:
                self._close_qty(row, price, 0.5, "TP1")
                continue
            if int(row["tp1_done"]) == 1 and pnl >= self.cfg.tp2_pct:
                self._close_qty(row, price, 1.0, "TP2")
                continue

            dca_count = int(row["dca_count"])
            if alive and dca_count == 0 and pnl <= -self.cfg.dca1_drop_pct:
                self._add_position(symbol, price, self.cfg.dca1_margin_usdt, dca=True)
            elif alive and dca_count == 1 and pnl <= -self.cfg.dca2_drop_pct:
                self._add_position(symbol, price, self.cfg.dca2_margin_usdt, dca=True)

    def scan_entries(self) -> None:
        open_symbols = {r["symbol"] for r in self.open_rows()}
        if len(open_symbols) >= self.cfg.max_positions:
            return
        if self.cfg.mode != "paper":
            balance = self.client.balance("USDT")
            if balance <= self.cfg.emergency_stop_balance:
                log_event("", "EMERGENCY_STOP", mode=self.cfg.mode, details=f"balance={balance}")
                return
            if balance < self.cfg.min_balance_to_trade:
                return
        for symbol in self.cfg.symbols:
            if symbol in open_symbols:
                continue
            ok, details = entry_signal(self.client, symbol)
            log_event(symbol, "SCAN_OK" if ok else "SCAN_WAIT", float(details.get("price", 0)), mode=self.cfg.mode,
                      details=json.dumps(details, ensure_ascii=False))
            if ok:
                self._add_position(symbol, float(details["price"]), self.cfg.first_margin_usdt)
                break

    def run_once(self) -> None:
        self.manage()
        self.scan_entries()

    def run_forever(self) -> None:
        log_event("", "BOT_START", mode=self.cfg.mode, details=json.dumps(asdict(self.cfg), ensure_ascii=False))
        while True:
            try:
                self.run_once()
            except Exception as exc:
                log_event("", "ERROR", mode=self.cfg.mode, details=str(exc))
            time.sleep(max(30, self.cfg.scan_seconds))
