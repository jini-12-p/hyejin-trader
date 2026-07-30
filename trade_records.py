from __future__ import annotations

import csv
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DB_PATH = Path(__file__).with_name("hyejin_trader.db")
COMPLETED_CSV = Path(__file__).with_name("completed_trades.csv")
LOG_PATH = Path(__file__).with_name("trade_log.txt")
STRATEGY_VERSION = "v1.1"

CSV_FIELDS = [
    "trade_id", "symbol", "entry_time_utc", "entry_price", "recommendation_rank",
    "recommendation_score", "pullback_score", "rsi", "signal", "signal_candle_time_utc",
    "tp_pct", "tp_price", "stop_price", "exit_type", "exit_price", "exit_time_utc",
    "holding_minutes", "pnl_pct", "memo", "strategy_version",
]


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=20, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def migrate_trade_schema() -> None:
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
            tp_pct REAL,
            tp_price REAL,
            stop_price REAL,
            exit_type TEXT NOT NULL,
            exit_price REAL NOT NULL,
            exit_time_utc TEXT NOT NULL,
            holding_minutes REAL,
            pnl_pct REAL,
            memo TEXT,
            strategy_version TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS exit_tracking (
            trade_id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            exit_time_utc TEXT NOT NULL,
            exit_price REAL NOT NULL,
            bars_target INTEGER NOT NULL DEFAULT 12,
            bars_tracked INTEGER NOT NULL DEFAULT 0,
            highest_price REAL,
            lowest_price REAL,
            last_price REAL,
            tp_after_exit INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'TRACKING',
            updated_at_utc TEXT
        );
        """)
        cols = _columns(conn, "positions")
        additions = {
            "trade_id": "TEXT",
            "recommendation_rank": "INTEGER",
            "recommendation_score": "REAL",
            "pullback_score": "REAL",
            "rsi": "REAL",
            "signal": "TEXT",
            "signal_candle_time_utc": "TEXT",
            "strategy_version": "TEXT",
        }
        for name, sql_type in additions.items():
            if name not in cols:
                conn.execute(f"ALTER TABLE positions ADD COLUMN {name} {sql_type}")


def make_trade_id(symbol: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")[:-3]
    return f"{symbol.upper()}_{stamp}"


def write_log(message: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(f"[{now}] {message}\n")


def _append_csv(row: dict[str, Any]) -> None:
    exists = COMPLETED_CSV.exists() and COMPLETED_CSV.stat().st_size > 0
    with COMPLETED_CSV.open("a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if not exists:
            writer.writeheader()
        writer.writerow({key: row.get(key) for key in CSV_FIELDS})


def complete_trade(symbol: str, exit_type: str, exit_price: float, memo: str = "") -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    with db() as conn:
        pos = conn.execute("SELECT * FROM positions WHERE symbol=? AND status='OPEN'", (symbol,)).fetchone()
        if not pos:
            raise ValueError(f"{symbol}: 관리 중인 OPEN 포지션이 없습니다.")

        entry_price = float(pos["entry_price"])
        entry_time = datetime.fromisoformat(pos["entry_time_utc"].replace("Z", "+00:00"))
        if entry_time.tzinfo is None:
            entry_time = entry_time.replace(tzinfo=timezone.utc)
        trade_id = pos["trade_id"] or make_trade_id(symbol)
        holding_minutes = max(0.0, (now - entry_time.astimezone(timezone.utc)).total_seconds() / 60)
        pnl_pct = (float(exit_price) - entry_price) / entry_price * 100
        tp_price = entry_price * (1 + float(pos["tp_pct"]) / 100)

        row = {
            "trade_id": trade_id,
            "symbol": symbol,
            "entry_time_utc": pos["entry_time_utc"],
            "entry_price": entry_price,
            "recommendation_rank": pos["recommendation_rank"],
            "recommendation_score": pos["recommendation_score"],
            "pullback_score": pos["pullback_score"],
            "rsi": pos["rsi"],
            "signal": pos["signal"],
            "signal_candle_time_utc": pos["signal_candle_time_utc"],
            "tp_pct": float(pos["tp_pct"]),
            "tp_price": tp_price,
            "stop_price": float(pos["stop_price"]),
            "exit_type": exit_type,
            "exit_price": float(exit_price),
            "exit_time_utc": now.isoformat(),
            "holding_minutes": round(holding_minutes, 2),
            "pnl_pct": round(pnl_pct, 4),
            "memo": memo.strip(),
            "strategy_version": pos["strategy_version"] or STRATEGY_VERSION,
        }

        conn.execute("""INSERT OR REPLACE INTO completed_trades(
            trade_id,symbol,entry_time_utc,entry_price,recommendation_rank,recommendation_score,
            pullback_score,rsi,signal,signal_candle_time_utc,tp_pct,tp_price,stop_price,
            exit_type,exit_price,exit_time_utc,holding_minutes,pnl_pct,memo,strategy_version
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", tuple(row[k] for k in CSV_FIELDS))
        conn.execute("UPDATE positions SET status=? WHERE symbol=?", (exit_type, symbol))
        conn.execute("""INSERT OR REPLACE INTO exit_tracking(
            trade_id,symbol,exit_time_utc,exit_price,bars_target,bars_tracked,
            highest_price,lowest_price,last_price,tp_after_exit,status,updated_at_utc
        ) VALUES(?,?,?,?,12,0,?,?,?,0,'TRACKING',?)""",
        (trade_id, symbol, now.isoformat(), float(exit_price), float(exit_price), float(exit_price), float(exit_price), now.isoformat()))

    _append_csv(row)
    write_log(f"CLOSE {trade_id} {symbol} {exit_type} exit={exit_price} pnl={pnl_pct:.4f}%")
    return row
