from __future__ import annotations

import csv
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "hyejin_trader.db"
COMPLETED_CSV = BASE_DIR / "completed_trades.csv"
LOG_PATH = BASE_DIR / "trade_log.txt"
STRATEGY_VERSION = "v1.1"

COMPLETED_FIELDS = [
    "trade_id", "symbol", "entry_time_utc", "entry_price", "recommendation_rank",
    "recommendation_score", "pullback_score", "rsi", "signal", "signal_candle_time_utc",
    "tp_price", "stop_price", "exit_type", "exit_price", "exit_time_utc",
    "holding_minutes", "pnl_pct", "memo", "strategy_version", "tracking",
]


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=20, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_trade_id(symbol: str, when: str | None = None) -> str:
    dt = datetime.fromisoformat(when) if when else datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return f"{symbol}_{dt.astimezone(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')[:-3]}"


def write_log(message: str) -> None:
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(f"{utc_now()} | {message}\n")


def init_trade_tables() -> None:
    with db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS completed_trades (
            trade_id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            entry_time_utc TEXT NOT NULL,
            entry_price REAL NOT NULL,
            recommendation_rank INTEGER,
            recommendation_score REAL,
            pullback_score REAL,
            rsi REAL,
            signal TEXT,
            signal_candle_time_utc TEXT,
            tp_price REAL,
            stop_price REAL,
            exit_type TEXT NOT NULL,
            exit_price REAL NOT NULL,
            exit_time_utc TEXT NOT NULL,
            holding_minutes REAL,
            pnl_pct REAL,
            memo TEXT,
            strategy_version TEXT NOT NULL,
            tracking INTEGER NOT NULL DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS post_exit_tracking (
            trade_id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            exit_time_utc TEXT NOT NULL,
            exit_price REAL NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            last_price REAL,
            max_price REAL,
            min_price REAL,
            max_move_pct REAL,
            min_move_pct REAL,
            bars_after_exit INTEGER NOT NULL DEFAULT 0,
            updated_at_utc TEXT,
            FOREIGN KEY(trade_id) REFERENCES completed_trades(trade_id)
        );
        """)

        existing = {r["name"] for r in conn.execute("PRAGMA table_info(positions)")}
        additions = {
            "trade_id": "TEXT",
            "recommendation_rank": "INTEGER",
            "recommendation_score": "REAL",
            "pullback_score": "REAL",
            "rsi": "REAL",
            "signal": "TEXT",
            "signal_candle_time_utc": "TEXT",
            "tp_price_at_entry": "REAL",
            "strategy_version": "TEXT DEFAULT 'v1.1'",
        }
        for name, ddl in additions.items():
            if name not in existing:
                conn.execute(f"ALTER TABLE positions ADD COLUMN {name} {ddl}")


def save_open_position(
    *, symbol: str, entry_price: float, stop_price: float, tp_pct: float,
    recommendation_rank: int | None, recommendation_score: float | None,
    pullback_score: float | None, rsi: float | None, signal: str | None,
    signal_candle_time_utc: str | None,
) -> str:
    entry_time = utc_now()
    trade_id = make_trade_id(symbol, entry_time)
    tp_price = entry_price * (1 + tp_pct / 100)
    with db() as conn:
        conn.execute("""
        INSERT INTO positions(
            symbol, entry_price, entry_time_utc, tp_pct, stop_price, status,
            trade_id, recommendation_rank, recommendation_score, pullback_score,
            rsi, signal, signal_candle_time_utc, tp_price_at_entry, strategy_version
        ) VALUES(?,?,?,?,?,'OPEN',?,?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol) DO UPDATE SET
            entry_price=excluded.entry_price,
            entry_time_utc=excluded.entry_time_utc,
            tp_pct=excluded.tp_pct,
            stop_price=excluded.stop_price,
            status='OPEN',
            trade_id=excluded.trade_id,
            recommendation_rank=excluded.recommendation_rank,
            recommendation_score=excluded.recommendation_score,
            pullback_score=excluded.pullback_score,
            rsi=excluded.rsi,
            signal=excluded.signal,
            signal_candle_time_utc=excluded.signal_candle_time_utc,
            tp_price_at_entry=excluded.tp_price_at_entry,
            strategy_version=excluded.strategy_version
        """, (
            symbol, entry_price, entry_time, tp_pct, stop_price, trade_id,
            recommendation_rank, recommendation_score, pullback_score, rsi,
            signal, signal_candle_time_utc, tp_price, STRATEGY_VERSION,
        ))
    write_log(f"OPEN | {trade_id} | {symbol} | entry={entry_price} | tp={tp_price} | stop={stop_price}")
    return trade_id


def _append_completed_csv(record: dict) -> None:
    file_exists = COMPLETED_CSV.exists() and COMPLETED_CSV.stat().st_size > 0
    with COMPLETED_CSV.open("a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=COMPLETED_FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerow({k: record.get(k) for k in COMPLETED_FIELDS})


def complete_position(symbol: str, exit_type: str, exit_price: float, memo: str = "") -> dict:
    exit_time = utc_now()
    with db() as conn:
        row = conn.execute("SELECT * FROM positions WHERE symbol=? AND status='OPEN'", (symbol,)).fetchone()
        if not row:
            raise ValueError(f"{symbol}: 관리 중인 포지션을 찾을 수 없습니다.")
        p = dict(row)
        trade_id = p.get("trade_id") or make_trade_id(symbol, p["entry_time_utc"])
        entry_price = float(p["entry_price"])
        pnl_pct = (float(exit_price) - entry_price) / entry_price * 100
        entry_dt = datetime.fromisoformat(p["entry_time_utc"])
        exit_dt = datetime.fromisoformat(exit_time)
        holding_minutes = max(0.0, (exit_dt - entry_dt).total_seconds() / 60)
        tp_price = p.get("tp_price_at_entry") or entry_price * (1 + float(p["tp_pct"]) / 100)

        record = {
            "trade_id": trade_id,
            "symbol": symbol,
            "entry_time_utc": p["entry_time_utc"],
            "entry_price": entry_price,
            "recommendation_rank": p.get("recommendation_rank"),
            "recommendation_score": p.get("recommendation_score"),
            "pullback_score": p.get("pullback_score"),
            "rsi": p.get("rsi"),
            "signal": p.get("signal"),
            "signal_candle_time_utc": p.get("signal_candle_time_utc"),
            "tp_price": tp_price,
            "stop_price": p["stop_price"],
            "exit_type": exit_type,
            "exit_price": float(exit_price),
            "exit_time_utc": exit_time,
            "holding_minutes": round(holding_minutes, 2),
            "pnl_pct": round(pnl_pct, 4),
            "memo": memo.strip(),
            "strategy_version": p.get("strategy_version") or STRATEGY_VERSION,
            "tracking": 1,
        }

        conn.execute("""
        INSERT OR REPLACE INTO completed_trades(
            trade_id,symbol,entry_time_utc,entry_price,recommendation_rank,
            recommendation_score,pullback_score,rsi,signal,signal_candle_time_utc,
            tp_price,stop_price,exit_type,exit_price,exit_time_utc,holding_minutes,
            pnl_pct,memo,strategy_version,tracking
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, tuple(record[k] for k in COMPLETED_FIELDS))
        conn.execute("UPDATE positions SET status=? WHERE symbol=?", (exit_type, symbol))
        conn.execute("""
        INSERT OR REPLACE INTO post_exit_tracking(
            trade_id,symbol,exit_time_utc,exit_price,active,last_price,max_price,min_price,
            max_move_pct,min_move_pct,bars_after_exit,updated_at_utc
        ) VALUES(?,?,?,?,1,?,?,?,?,?,0,?)
        """, (trade_id, symbol, exit_time, float(exit_price), float(exit_price),
              float(exit_price), float(exit_price), 0.0, 0.0, exit_time))

    _append_completed_csv(record)
    write_log(
        f"CLOSE | {trade_id} | {symbol} | type={exit_type} | exit={exit_price} | "
        f"pnl={record['pnl_pct']:+.4f}% | tracking=True"
    )
    return record
