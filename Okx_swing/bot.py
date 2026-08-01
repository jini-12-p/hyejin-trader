from __future__ import annotations

import json
import math
import sqlite3
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from Okx_swing.okx_api import OKXClient, OKXError

DB_PATH = Path(__file__).with_name("okx_swing_bot.db")
CONFIG_PATH = Path(__file__).with_name("config.json")
KST = timezone(timedelta(hours=9))


@dataclass
class DailyConfig:
    symbols: tuple[str, ...] = (
        "BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "XRP-USDT-SWAP",
        "DOGE-USDT-SWAP", "SUI-USDT-SWAP", "AVAX-USDT-SWAP", "LINK-USDT-SWAP",
        "PEPE-USDT-SWAP", "WIF-USDT-SWAP", "BONK-USDT-SWAP", "APT-USDT-SWAP",
    )
    mode: str = "paper"  # paper | demo | live
    leverage: int = 5
    margin_mode: str = "isolated"
    max_positions: int = 1
    max_daily_entries: int = 3
    position_margin_usdt: float = 54.0
    tp1_pct: float = 1.5
    tp2_pct: float = 3.0
    hard_stop_pct: float = 1.5
    breakeven_stop_pct: float = 0.1
    max_hold_hours: int = 3
    daily_loss_limit_usdt: float = 6.0
    min_balance_to_trade: float = 90.0
    emergency_stop_balance: float = 85.0
    scan_seconds: int = 60

    @classmethod
    def load(cls) -> "DailyConfig":
        if not CONFIG_PATH.exists():
            cfg = cls()
            CONFIG_PATH.write_text(json.dumps(asdict(cfg), ensure_ascii=False, indent=2), encoding="utf-8")
            return cfg
        raw = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        if "symbols" in raw:
            raw["symbols"] = tuple(raw["symbols"])
        allowed = set(cls.__dataclass_fields__)
        return cls(**{k: v for k, v in raw.items() if k in allowed})


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def today_kst() -> str:
    return datetime.now(KST).date().isoformat()


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=20)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_column(conn: sqlite3.Connection, table: str, name: str, ddl: str) -> None:
    columns = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
    if name not in columns:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")


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
        _ensure_column(conn, "bot_positions", "strategy", "TEXT DEFAULT 'P'")
        _ensure_column(conn, "bot_positions", "realized_pnl", "REAL DEFAULT 0")
        _ensure_column(conn, "bot_positions", "entry_date_kst", "TEXT")
        _ensure_column(conn, "bot_events", "strategy", "TEXT")
        _ensure_column(conn, "bot_events", "realized_pnl", "REAL DEFAULT 0")


def log_event(symbol: str, event: str, price: float = 0, qty: float = 0, mode: str = "",
              details: str = "", strategy: str = "", realized_pnl: float = 0.0) -> None:
    with db() as conn:
        conn.execute(
            "INSERT INTO bot_events(ts,symbol,event,price,qty,mode,details,strategy,realized_pnl) VALUES(?,?,?,?,?,?,?,?,?)",
            (utc_now(), symbol, event, float(price), float(qty), mode, details, strategy, float(realized_pnl)),
        )


def indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["ema9"] = out["close"].ewm(span=9, adjust=False).mean()
    out["ema20"] = out["close"].ewm(span=20, adjust=False).mean()
    out["ema60"] = out["close"].ewm(span=60, adjust=False).mean()
    delta = out["close"].diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / 14, adjust=False).mean().replace(0, math.nan)
    out["rsi"] = (100 - 100 / (1 + gain / loss)).fillna(50)
    out["vol_avg"] = out["volume"].rolling(20).mean()
    return out


def confirmed(df: pd.DataFrame) -> pd.DataFrame:
    if "confirm" in df:
        c = df[df["confirm"].astype(str) == "1"]
        return c if len(c) >= 3 else df.iloc[:-1]
    return df.iloc[:-1]


def candidate_signal(client: OKXClient, symbol: str) -> tuple[str | None, float, dict[str, Any]]:
    m15 = confirmed(indicators(client.candles(symbol, "15m", 220)))
    h1 = confirmed(indicators(client.candles(symbol, "1H", 140)))
    if len(m15) < 70 or len(h1) < 70:
        return None, 0.0, {"reason": "캔들 부족"}

    row, prev = m15.iloc[-1], m15.iloc[-2]
    hrow, hprev = h1.iloc[-1], h1.iloc[-2]
    price = float(row.close)
    volume_ratio = float(row.volume / row.vol_avg) if pd.notna(row.vol_avg) and row.vol_avg > 0 else 0.0

    # 급등·급락 직후 추격 진입 방지. 변동성 알트는 허용하되 1시간 6% 이상 급변은 제외한다.
    one_hour_move = abs(float(row.close / m15.iloc[-5].close - 1)) * 100
    candle_range = abs(float(row.high / row.low - 1)) * 100 if float(row.low) > 0 else 99.0
    not_extreme = bool(one_hour_move <= 6.0 and candle_range <= 4.0)

    # P형(완화): 1시간 추세가 상승이거나 최소 횡보이고, 15분 흐름이 회복되는 눌림 반등.
    h1_up = bool(hrow.ema20 > hrow.ema60 and hrow.ema20 >= hprev.ema20)
    h1_flat = bool(hrow.ema20 >= hrow.ema60 * 0.995 and hrow.ema20 >= hprev.ema20 * 0.998)
    m15_recovering = bool(row.ema9 >= row.ema20 or row.close >= row.ema20)
    trend_ok = bool(h1_up or (h1_flat and m15_recovering))
    pullback = bool(m15.tail(10).low.min() <= row.ema20 * 1.008)
    rebound = bool(row.close > row.open and row.close > prev.high and row.close >= row.ema9)
    p_rsi = bool(40 <= row.rsi <= 70)
    p_score = (30 if trend_ok else 0) + (20 if pullback else 0) + (25 if rebound else 0) + \
              (15 if p_rsi else 0) + min(10, volume_ratio * 7)
    p_ok = bool(trend_ok and pullback and rebound and p_rsi and not_extreme and p_score >= 70)

    # R형(완화): RSI 35 전후 과매도 후 저점 방어와 반등 확인. 거래량은 필수가 아닌 점수 항목이다.
    recent_rsi_min = float(m15.tail(12).rsi.min())
    oversold = bool(recent_rsi_min <= 35 or row.rsi <= 38)
    reversal = bool(row.close > row.open and row.close > prev.high and row.rsi > prev.rsi)
    not_crashing = bool(hrow.close >= hrow.ema60 * 0.955 and hrow.rsi >= 26)
    low_hold = bool(row.low >= m15.tail(8).low.min() * 0.997)
    r_score = (30 if oversold else 0) + (30 if reversal else 0) + (20 if not_crashing else 0) + \
              (10 if low_hold else 0) + min(10, volume_ratio * 7)
    r_ok = bool(oversold and reversal and not_crashing and low_hold and not_extreme and r_score >= 70)

    strategy: str | None = None
    score = 0.0
    if p_ok and p_score >= r_score:
        strategy, score = "P", p_score
    elif r_ok:
        strategy, score = "R", r_score

    details = {
        "price": price, "strategy": strategy, "score": round(float(score), 2),
        "p_ok": bool(p_ok), "r_ok": bool(r_ok), "trend_ok": trend_ok,
        "h1_up": h1_up, "h1_flat": h1_flat, "pullback": pullback, "rebound": rebound,
        "rsi": round(float(row.rsi), 2), "recent_rsi_min": round(recent_rsi_min, 2),
        "volume_ratio": round(volume_ratio, 2), "not_crashing": not_crashing,
        "not_extreme": not_extreme, "one_hour_move_pct": round(one_hour_move, 2),
    }
    return strategy, float(score), details


def qty_from_margin(price: float, margin_usdt: float, leverage: int) -> float:
    return max(0.0, margin_usdt * leverage / price)


class DailyBot:
    def __init__(self, config: DailyConfig | None = None):
        self.cfg = config or DailyConfig.load()
        self.client = OKXClient(demo=self.cfg.mode != "live")
        init_db()

    def open_rows(self) -> list[sqlite3.Row]:
        with db() as conn:
            return conn.execute("SELECT * FROM bot_positions WHERE status='OPEN' ORDER BY opened_at").fetchall()

    def daily_entries(self) -> int:
        day = today_kst()
        with db() as conn:
            return int(conn.execute(
                "SELECT COUNT(*) FROM bot_events WHERE event='ENTRY' AND substr(datetime(ts,'+9 hours'),1,10)=?", (day,)
            ).fetchone()[0])

    def daily_realized(self) -> float:
        day = today_kst()
        with db() as conn:
            value = conn.execute(
                "SELECT COALESCE(SUM(realized_pnl),0) FROM bot_events WHERE substr(datetime(ts,'+9 hours'),1,10)=?", (day,)
            ).fetchone()[0]
        return float(value or 0)

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

    def _open(self, symbol: str, price: float, strategy: str, score: float) -> None:
        qty = qty_from_margin(price, self.cfg.position_margin_usdt, self.cfg.leverage)
        self._execute(symbol, "buy", qty)
        with db() as conn:
            conn.execute("DELETE FROM bot_positions WHERE symbol=?", (symbol,))
            conn.execute(
                """INSERT INTO bot_positions(
                    symbol,status,opened_at,updated_at,avg_price,total_qty,total_margin,dca_count,tp1_done,
                    last_price,unrealized_pct,note,strategy,realized_pnl,entry_date_kst
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (symbol, "OPEN", utc_now(), utc_now(), price, qty, self.cfg.position_margin_usdt, 0, 0,
                 price, 0.0, f"{strategy}형 데일리 진입", strategy, 0.0, today_kst()),
            )
        log_event(symbol, "ENTRY", price, qty, self.cfg.mode,
                  f"score={score:.1f}; margin={self.cfg.position_margin_usdt}", strategy)

    def _close(self, row: sqlite3.Row, price: float, fraction: float, reason: str) -> None:
        current_qty = float(row["total_qty"])
        qty = current_qty * fraction
        self._execute(row["symbol"], "sell", qty, reduce_only=True)
        pnl_usdt = (price - float(row["avg_price"])) * qty
        remaining = max(0.0, current_qty - qty)
        total_realized = float(row["realized_pnl"] or 0) + pnl_usdt
        with db() as conn:
            if remaining <= 1e-12:
                conn.execute(
                    "UPDATE bot_positions SET status='CLOSED',total_qty=0,updated_at=?,last_price=?,note=?,realized_pnl=? WHERE symbol=?",
                    (utc_now(), price, reason, total_realized, row["symbol"]),
                )
            else:
                conn.execute(
                    "UPDATE bot_positions SET total_qty=?,tp1_done=1,updated_at=?,last_price=?,note=?,realized_pnl=? WHERE symbol=?",
                    (remaining, utc_now(), price, reason, total_realized, row["symbol"]),
                )
        log_event(row["symbol"], reason, price, qty, self.cfg.mode, strategy=row["strategy"] or "", realized_pnl=pnl_usdt)

    def manage(self) -> None:
        for row in self.open_rows():
            price = float(self.client.ticker(row["symbol"]).get("last") or 0)
            if price <= 0:
                continue
            avg = float(row["avg_price"])
            pnl_pct = (price / avg - 1) * 100
            age_h = (datetime.now(timezone.utc) - datetime.fromisoformat(row["opened_at"])).total_seconds() / 3600
            with db() as conn:
                conn.execute("UPDATE bot_positions SET last_price=?,unrealized_pct=?,updated_at=? WHERE symbol=?",
                             (price, pnl_pct, utc_now(), row["symbol"]))

            # TP1 후에는 본전 아래로 다시 밀리면 잔량 정리.
            if int(row["tp1_done"]) == 1 and pnl_pct <= -self.cfg.breakeven_stop_pct:
                self._close(row, price, 1.0, "BE_EXIT")
            elif pnl_pct <= -self.cfg.hard_stop_pct:
                self._close(row, price, 1.0, "STOP")
            elif age_h >= self.cfg.max_hold_hours:
                self._close(row, price, 1.0, "TIME_EXIT")
            elif int(row["tp1_done"]) == 0 and pnl_pct >= self.cfg.tp1_pct:
                self._close(row, price, 0.5, "TP1")
            elif int(row["tp1_done"]) == 1 and pnl_pct >= self.cfg.tp2_pct:
                self._close(row, price, 1.0, "TP2")

    def scan_entries(self) -> None:
        if len(self.open_rows()) >= self.cfg.max_positions:
            return
        if self.daily_entries() >= self.cfg.max_daily_entries:
            return
        if self.daily_realized() <= -abs(self.cfg.daily_loss_limit_usdt):
            log_event("", "DAILY_STOP", mode=self.cfg.mode, details=f"pnl={self.daily_realized():.2f}")
            return
        if self.cfg.mode != "paper":
            balance = self.client.balance("USDT")
            if balance <= self.cfg.emergency_stop_balance:
                log_event("", "EMERGENCY_STOP", mode=self.cfg.mode, details=f"balance={balance}")
                return
            if balance < self.cfg.min_balance_to_trade:
                return

        candidates: list[tuple[float, str, str, dict[str, Any]]] = []
        for symbol in self.cfg.symbols:
            try:
                strategy, score, details = candidate_signal(self.client, symbol)
                log_event(symbol, "SCAN_OK" if strategy else "SCAN_WAIT", float(details.get("price", 0)),
                          mode=self.cfg.mode, details=json.dumps(details, ensure_ascii=False), strategy=strategy or "")
                if strategy:
                    candidates.append((score, symbol, strategy, details))
            except Exception as exc:
                # 특정 종목의 일시적 API/상장 상태 문제 때문에 전체 스캔이 멈추지 않게 한다.
                log_event(symbol, "SCAN_ERROR", mode=self.cfg.mode, details=str(exc))

        if candidates:
            score, symbol, strategy, details = max(candidates, key=lambda x: x[0])
            self._open(symbol, float(details["price"]), strategy, score)

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


# 기존 실행 파일(run_okx_swing_bot.py)과 호환
SwingBot = DailyBot
SwingConfig = DailyConfig
