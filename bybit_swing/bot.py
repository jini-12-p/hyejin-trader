from __future__ import annotations

import csv
import json
import math
import sqlite3
import time
import os
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from bybit_swing.bybit_api import BybitSwingClient, BybitSwingError

DB_PATH = Path(__file__).with_name("bybit_swing_bot.db")
CONFIG_PATH = Path(__file__).with_name("config.json")
KST = timezone(timedelta(hours=9))
SCAN_REJECTED_CSV_PATH = Path(__file__).with_name("scan_rejected.csv")


@dataclass
class DailyConfig:
    # API 선별 실패 때 사용할 안전한 기본 감시 목록
    symbols: tuple[str, ...] = (
        "SOLUSDT", "XRPUSDT", "DOGEUSDT", "SUIUSDT",
        "AVAXUSDT", "LINKUSDT", "PEPEUSDT", "WIFUSDT",
        "BONKUSDT", "APTUSDT", "NEARUSDT", "ARBUSDT",
    )
    # 24시간 거래대금·변동성으로 실제 감시 종목을 자동 선별
    dynamic_universe: bool = True
    universe_size: int = 15
    universe_refresh_minutes: int = 15
    top_gainers_pool_size: int = 50
    min_change_24h_pct: float = 2.0
    min_quote_volume_24h_usdt: float = 1500000.0
    min_range_24h_pct: float = 3.0
    max_range_24h_pct: float = 80.0
    max_abs_change_24h_pct: float = 70.0
    max_spread_pct: float = 0.22
    # 데일리 3시간 전략에 맞게 최근 1~4시간 실제 움직임도 검사한다.
    recent_volatility_prefilter_size: int = 40
    min_recent_4h_range_pct: float = 1.2
    min_avg_hourly_range_pct: float = 0.50
    max_recent_1h_move_pct: float = 10.0
    min_recent_1h_move_pct: float = 0.50
    # 장기 스윙에 더 어울리는 느린 종목은 데일리 후보에서 제외한다.
    slow_symbol_exclusions: tuple[str, ...] = (
        "XRPUSDT", "AVAXUSDT", "LINKUSDT", "LTCUSDT",
        "BCHUSDT", "DOTUSDT", "ETCUSDT", "ATOMUSDT",
        "TRXUSDT", "TONUSDT", "FILUSDT", "AAVEUSDT",
    )
    non_crypto_base_exclusions: tuple[str, ...] = (
        "AAPL", "ABBV", "ABT", "AMAT", "AMD", "AMZN", "ASML", "AVGO",
        "BA", "BABA", "BAC", "BRK", "CAT", "COIN", "COST", "CRM", "CVX",
        "DIS", "GOOG", "GOOGL", "GS", "HD", "IBM", "INTC", "JNJ", "JPM",
        "KO", "LLY", "MA", "META", "MMM", "MRK", "MSFT", "MSTR", "MU",
        "NFLX", "NKE", "NVDA", "ORCL", "PEP", "PFE", "PLTR", "PYPL",
        "QCOM", "SBUX", "SKHYNIX", "SNDK", "SOXL", "SPY", "TSLA", "TSM",
        "UNH", "V", "WMT", "XAU", "XAG", "AAOI", "CRWV", "AXTI",
    )
    candidate_pool: tuple[str, ...] = (
        "SOLUSDT", "XRPUSDT", "DOGEUSDT", "SUIUSDT",
        "AVAXUSDT", "LINKUSDT", "PEPEUSDT", "WIFUSDT",
        "BONKUSDT", "APTUSDT", "NEARUSDT", "ARBUSDT",
        "OPUSDT", "SEIUSDT", "INJUSDT", "TIAUSDT",
        "FILUSDT", "LTCUSDT", "BCHUSDT", "DOTUSDT",
        "UNIUSDT", "AAVEUSDT", "ETCUSDT", "ATOMUSDT",
        "TRXUSDT", "TONUSDT", "SHIBUSDT", "ORDIUSDT",
        "JUPUSDT", "PYTHUSDT", "ENAUSDT", "ONDOUSDT",
        "RENDERUSDT", "FETUSDT", "WLDUSDT", "GALAUSDT",
    )
    mode: str = "paper"  # paper | demo | live
    leverage: int = 5
    margin_mode: str = "isolated"
    max_positions: int = 2
    max_daily_entries: int = 0  # 0이면 PAPER 데이터 수집 중 횟수 제한 없음
    position_margin_usdt: float = 54.0
    tp1_pct: float = 1.5
    tp2_pct: float = 3.0
    hard_stop_pct: float = 1.5
    breakeven_stop_pct: float = 0.1
    staged_stop_enabled: bool = True
    stage1_stop_pct: float = 1.5
    stage1_stop_fraction: float = 0.5
    final_stop_pct: float = 2.3
    recovery_exit_loss_pct: float = 0.3
    max_hold_hours: int = 3
    daily_loss_limit_usdt: float = 12.0
    max_consecutive_losses: int = 3
    loss_cooldown_minutes: int = 60
    paper_consecutive_loss_warning_only: bool = True
    same_symbol_cooldown_minutes: int = 30
    min_balance_to_trade: float = 90.0
    emergency_stop_balance: float = 85.0
    scan_seconds: int = 60
    manage_seconds: float = 1.0
    paper_fill_at_trigger: bool = True
    telegram_enabled: bool = False
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    telegram_notify_entry: bool = True
    telegram_notify_cycle: bool = True
    telegram_notify_exit: bool = True
    telegram_notify_error: bool = True
    rebound_add_enabled: bool = True
    rebound_arm_drawdown_pct: float = 0.6
    rebound_add_margin_usdt: float = 27.0
    rebound_exit_buffer_pct: float = 0.10
    max_cycle_adds: int = 2
    flat_exit_minutes: int = 60
    flat_min_favorable_pct: float = 0.40
    min_pullback_from_high_pct: float = 0.30
    max_pullback_from_high_pct: float = 6.00
    min_entry_candle_gain_pct: float = 0.15
    max_entry_candle_gain_pct: float = 0.90
    # v4.0.13: 이미 여러 봉 오른 뒤 EMA9에서 멀어진 추격 진입을 제한한다.
    max_ema9_distance_pct: float = 1.00
    late_rise_streak_bars: int = 3
    late_rise_streak_max_ema9_distance_pct: float = 0.55
    min_close_location_pct: float = 65.0
    max_upper_wick_ratio: float = 0.35
    max_near_high_pct: float = 0.15
    min_rebound_from_low_pct: float = 0.25
    rebound_min_volume_ratio: float = 0.9
    entry_min_volume_ratio: float = 0.70
    require_rebound_confirmation_candle: bool = True
    reject_three_bar_volume_decline: bool = True
    rebound_min_rsi: float = 44.0
    hj_pattern_enabled: bool = True
    hj_min_volume_ratio: float = 0.75
    hj_min_current_gain_pct: float = 0.45
    hj_min_body_recovery_pct: float = 0.55
    hj_min_lower_wick_body_ratio: float = 0.80
    hj_min_trend_score: int = 3

    @classmethod
    def load(cls) -> "DailyConfig":
        if not CONFIG_PATH.exists():
            cfg = cls()
            CONFIG_PATH.write_text(json.dumps(asdict(cfg), ensure_ascii=False, indent=2), encoding="utf-8")
            return cfg
        raw = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        if "symbols" in raw:
            raw["symbols"] = tuple(raw["symbols"])
        if "candidate_pool" in raw:
            raw["candidate_pool"] = tuple(raw["candidate_pool"])
        if "slow_symbol_exclusions" in raw:
            raw["slow_symbol_exclusions"] = tuple(raw["slow_symbol_exclusions"])
        if "non_crypto_base_exclusions" in raw:
            raw["non_crypto_base_exclusions"] = tuple(raw["non_crypto_base_exclusions"])
        allowed = set(cls.__dataclass_fields__)
        return cls(**{k: v for k, v in raw.items() if k in allowed})


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def trading_day() -> str:
    """한국시간 오전 9시(UTC 00:00)를 기준으로 거래일을 나눈다."""
    return datetime.now(timezone.utc).date().isoformat()


def today_kst() -> str:
    # 이전 DB 컬럼명/호출과의 호환용 별칭
    return trading_day()


SCAN_REJECTED_FIELDS = [
    "time_kst", "symbol", "result", "strategy", "score", "price",
    "rejected_conditions", "rsi", "ema9", "ema20", "ema60",
    "volume_ratio", "change_24h_pct", "recent_1h_move_pct",
    "recent_4h_range_pct", "pullback_from_high_pct",
    "rebound_from_low_pct", "entry_candle_gain_pct",
    "distance_to_recent_high_pct", "ema9_distance_pct",
    "rising_close_streak", "late_entry_ok", "close_location_pct",
    "upper_wick_ratio", "confirmation_hold", "data_complete",
]

def ensure_scan_rejected_csv() -> None:
    """봇 시작 즉시 CSV 파일과 헤더를 만든다."""
    SCAN_REJECTED_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    if SCAN_REJECTED_CSV_PATH.exists() and SCAN_REJECTED_CSV_PATH.stat().st_size > 0:
        return
    with SCAN_REJECTED_CSV_PATH.open("w", encoding="utf-8-sig", newline="") as fh:
        csv.DictWriter(fh, fieldnames=SCAN_REJECTED_FIELDS).writeheader()
        fh.flush()
        os.fsync(fh.fileno())

def append_scan_record(symbol: str, strategy: str | None, score: float, details: dict[str, Any]) -> None:
    """진입 후보의 통과/탈락 사유를 CSV에 즉시 누적한다."""
    ensure_scan_rejected_csv()
    fields = SCAN_REJECTED_FIELDS
    row = {
        "time_kst": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "symbol": symbol,
        "result": "SCAN_OK" if strategy else "SCAN_WAIT",
        "strategy": strategy or "",
        "score": round(float(score), 2),
        "price": details.get("price", ""),
        "rejected_conditions": ",".join(details.get("rejected_conditions") or []),
    }
    for key in fields:
        if key not in row:
            row[key] = details.get(key, "")
    try:
        write_header = not SCAN_REJECTED_CSV_PATH.exists() or SCAN_REJECTED_CSV_PATH.stat().st_size == 0
        with SCAN_REJECTED_CSV_PATH.open("a", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
            if write_header:
                writer.writeheader()
            writer.writerow(row)
            fh.flush()
            os.fsync(fh.fileno())
    except Exception as exc:
        log_event(symbol, "SCAN_CSV_ERROR", mode="paper", details=str(exc))


def append_entry_record(
    symbol: str,
    result: str,
    strategy: str,
    score: float,
    price: float,
    message: str = "",
) -> None:
    """진입 시도/성공/오류를 기존 SCAN CSV에 같은 열 구조로 기록한다."""
    ensure_scan_rejected_csv()
    row = {key: "" for key in SCAN_REJECTED_FIELDS}
    row.update({
        "time_kst": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "symbol": symbol,
        "result": result,
        "strategy": strategy or "",
        "score": round(float(score), 2),
        "price": price,
        "rejected_conditions": message,
    })
    try:
        with SCAN_REJECTED_CSV_PATH.open("a", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(
                fh, fieldnames=SCAN_REJECTED_FIELDS, extrasaction="ignore"
            )
            writer.writerow(row)
            fh.flush()
            os.fsync(fh.fileno())
    except Exception as exc:
        log_event(
            symbol, "ENTRY_CSV_ERROR", price,
            mode="paper", details=f"{type(exc).__name__}: {exc}",
            strategy=strategy,
        )


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
        CREATE TABLE IF NOT EXISTS stop_reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            strategy TEXT,
            stop_event TEXT NOT NULL,
            stop_ts TEXT NOT NULL,
            entry_price REAL,
            stop_price REAL NOT NULL,
            pnl_at_stop_pct REAL,
            price_15m REAL,
            pct_15m REAL,
            price_30m REAL,
            pct_30m REAL,
            price_60m REAL,
            pct_60m REAL,
            price_120m REAL,
            pct_120m REAL,
            price_180m REAL,
            pct_180m REAL,
            review_label TEXT,
            completed INTEGER NOT NULL DEFAULT 0,
            UNIQUE(trade_id, stop_event, stop_ts)
        );
        """)
        _ensure_column(conn, "bot_positions", "strategy", "TEXT DEFAULT 'P'")
        _ensure_column(conn, "bot_positions", "realized_pnl", "REAL DEFAULT 0")
        _ensure_column(conn, "bot_positions", "entry_date_kst", "TEXT")
        _ensure_column(conn, "bot_positions", "base_entry_price", "REAL")
        _ensure_column(conn, "bot_positions", "base_qty", "REAL")
        _ensure_column(conn, "bot_positions", "add_qty", "REAL DEFAULT 0")
        _ensure_column(conn, "bot_positions", "add_price", "REAL DEFAULT 0")
        _ensure_column(conn, "bot_positions", "lowest_price", "REAL")
        _ensure_column(conn, "bot_positions", "highest_price", "REAL")
        _ensure_column(conn, "bot_positions", "cycle_anchor_price", "REAL")
        _ensure_column(conn, "bot_positions", "trade_id", "TEXT")
        _ensure_column(conn, "bot_positions", "stop_stage1_done", "INTEGER DEFAULT 0")
        _ensure_column(conn, "bot_events", "strategy", "TEXT")
        _ensure_column(conn, "bot_events", "realized_pnl", "REAL DEFAULT 0")
        _ensure_column(conn, "bot_events", "trade_id", "TEXT")


def state_get(key: str, default: str = "") -> str:
    with db() as conn:
        row = conn.execute("SELECT value FROM bot_state WHERE key=?", (key,)).fetchone()
    return str(row[0]) if row else default


def state_set(key: str, value: str) -> None:
    with db() as conn:
        conn.execute(
            "INSERT INTO bot_state(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )


def state_flag(key: str, default: bool = False) -> bool:
    value = state_get(key, "1" if default else "0").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _kst_stamp(iso_ts: str | None = None) -> str:
    try:
        dt = datetime.fromisoformat(iso_ts) if iso_ts else datetime.now(timezone.utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(KST).strftime("%m/%d %H:%M:%S")
    except Exception:
        return datetime.now(KST).strftime("%m/%d %H:%M:%S")


def telegram_notify(text: str) -> None:
    """텔레그램 알림. 토큰은 환경변수를 우선 사용하며 실패해도 봇 거래는 계속한다."""
    try:
        cfg = DailyConfig.load()
        if not cfg.telegram_enabled:
            return
        token = (os.getenv("TELEGRAM_BOT_TOKEN") or cfg.telegram_bot_token or "").strip()
        chat_id = (os.getenv("TELEGRAM_CHAT_ID") or cfg.telegram_chat_id or "").strip()
        if not token or not chat_id:
            return
        payload = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode("utf-8")
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            resp.read(1)
    except Exception:
        # 알림 장애가 주문/포지션 관리를 막지 않도록 삼킨다.
        return


def _telegram_event_message(symbol: str, event: str, price: float, details: str, realized_pnl: float) -> str | None:
    cfg = DailyConfig.load()
    entry_events = {"ENTRY"}
    cycle_events = {"REBOUND_ADD", "CYCLE_REDUCE"}
    exit_events = {"TP1", "TP2", "STOP", "BE_EXIT", "FLAT_EXIT_75M", "TIME_EXIT"}
    error_events = {"ERROR", "SCAN_ERROR", "REBOUND_CHECK_ERROR", "BOT_SAFE_STOP"}
    if event in entry_events and not cfg.telegram_notify_entry:
        return None
    if event in cycle_events and not cfg.telegram_notify_cycle:
        return None
    if event in exit_events and not cfg.telegram_notify_exit:
        return None
    if event in error_events and not cfg.telegram_notify_error:
        return None
    if event not in entry_events | cycle_events | exit_events | error_events:
        return None
    try:
        d = json.loads(details or "{}")
    except Exception:
        d = {}
    labels = {
        "ENTRY": "신규 진입", "REBOUND_ADD": "순환추가", "CYCLE_REDUCE": "추가분 회수",
        "TP1": "TP1 익절", "TP2": "TP2 익절", "STOP": "손절",
        "BE_EXIT": "본절 보호 종료", "FLAT_EXIT_75M": "정체 종료", "TIME_EXIT": "시간 종료",
        "ERROR": "봇 오류", "SCAN_ERROR": "스캔 오류", "REBOUND_CHECK_ERROR": "반등 확인 오류",
        "BOT_SAFE_STOP": "안전 종료 완료",
    }
    lines = [f"[{_kst_stamp()} KST] {labels.get(event, event)}", f"종목: {symbol or '-'}"]
    if price:
        lines.append(f"가격: {price:.10g}")
    if event == "ENTRY":
        lines += [
            f"증거금: {float(d.get('margin_usdt', 0)):.2f} USDT · 레버리지 {d.get('leverage', '')}배",
            f"점수: {float(d.get('score', 0)):.2f} · RSI {d.get('rsi', '-')} · 거래량비 {d.get('volume_ratio', '-')}배",
            f"24h: {d.get('change_24h_pct', '-')}% · 최근1h: {d.get('recent_1h_move_pct', '-')}%",
        ]
    elif event == "REBOUND_ADD":
        lines.append(f"평단: {float(d.get('previous_avg', 0)):.10g} → {float(d.get('new_avg', 0)):.10g}")
    elif event == "CYCLE_REDUCE":
        lines.append(f"회수손익: {realized_pnl:+.2f} USDT")
    elif event in exit_events:
        lines.append(f"이번 손익: {realized_pnl:+.2f} USDT")
        lines.append(f"거래 누적: {float(d.get('trade_total_realized_pnl', realized_pnl)):+.2f} USDT")
    elif details:
        lines.append(str(details)[:500])
    return "\n".join(lines)


def log_event(symbol: str, event: str, price: float = 0, qty: float = 0, mode: str = "",
              details: str = "", strategy: str = "", realized_pnl: float = 0.0,
              trade_id: str = "") -> None:
    ts = utc_now()
    with db() as conn:
        conn.execute(
            "INSERT INTO bot_events(ts,symbol,event,price,qty,mode,details,strategy,realized_pnl,trade_id) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (ts, symbol, event, float(price), float(qty), mode, details, strategy, float(realized_pnl), trade_id),
        )
    msg = _telegram_event_message(symbol, event, float(price), details, float(realized_pnl))
    if msg:
        telegram_notify(msg)


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
    """Bybit kline의 마지막 행은 진행 중인 봉이므로 항상 제외한다.

    이전 버전은 API에서 임의로 넣은 confirm=1 값을 신뢰해 미완성 봉을
    진입 판단에 사용했고, 이 때문에 진입 직후 신호가 뒤집힐 수 있었다.
    """
    return df.iloc[:-1].copy() if len(df) > 1 else df.iloc[0:0].copy()


def candidate_signal(client: BybitSwingClient, symbol: str, cfg: DailyConfig) -> tuple[str | None, float, dict[str, Any]]:
    """기존 반등형(P)과 혜진 추세지속형(HJ)을 독립적으로 평가한다."""
    raw15 = indicators(client.candles(symbol, "15m", 220))
    raw1h = indicators(client.candles(symbol, "1H", 140))
    m15 = confirmed(raw15)
    h1 = confirmed(raw1h)
    if len(m15) < 70 or len(h1) < 70 or len(raw15) < 71:
        return None, 0.0, {"reason": "캔들 부족"}

    # 기존 반등 전략은 마감봉 기준
    row, prev = m15.iloc[-1], m15.iloc[-2]
    prevprev = m15.iloc[-3]
    hrow, hprev = h1.iloc[-1], h1.iloc[-2]

    price = float(row.close)
    volume_ratio = float(row.volume / row.vol_avg) if pd.notna(row.vol_avg) and row.vol_avg > 0 else 0.0
    recent = m15.tail(16)
    recent_high = float(recent.iloc[:-1].high.max())
    recent_low = float(recent.low.min())
    pullback_from_high = (recent_high / price - 1) * 100 if price > 0 else 99.0
    rebound_from_low = (price / recent_low - 1) * 100 if recent_low > 0 else 0.0
    entry_candle_gain = (float(row.close) / float(row.open) - 1) * 100 if float(row.open) > 0 else 99.0
    distance_to_high = (recent_high / price - 1) * 100 if price > 0 else 0.0
    one_hour_move = abs(float(row.close / m15.iloc[-5].close - 1)) * 100
    candle_range = abs(float(row.high / row.low - 1)) * 100 if float(row.low) > 0 else 99.0

    h1_up = bool(hrow.ema20 > hrow.ema60 and hrow.ema20 >= hprev.ema20)
    pullback_ok = bool(cfg.min_pullback_from_high_pct <= pullback_from_high <= cfg.max_pullback_from_high_pct)
    not_chasing = bool(entry_candle_gain <= cfg.max_entry_candle_gain_pct and distance_to_high >= cfg.max_near_high_pct)
    rebound = bool(row.close > row.open and row.close > prev.close and row.close >= row.ema9)
    momentum_ok = bool(40 <= row.rsi <= 75 and row.rsi >= prev.rsi)
    volume_ok = bool(volume_ratio >= cfg.entry_min_volume_ratio)
    not_extreme = bool(one_hour_move <= cfg.max_recent_1h_move_pct and candle_range <= 4.0)

    rebound_setup = bool(prev.close > prev.open and prev.close > prevprev.close and prev.close >= prev.ema9)
    confirmation_hold = bool(row.low >= prev.low and row.close >= prev.close * 0.998 and row.close > row.open)
    rebound = bool(rebound and (not cfg.require_rebound_confirmation_candle or (rebound_setup and confirmation_hold)))
    recent_volumes = m15["volume"].tail(3).tolist()
    volume_declining_3 = bool(len(recent_volumes) == 3 and recent_volumes[0] > recent_volumes[1] > recent_volumes[2])
    volume_trend_ok = bool(not cfg.reject_three_bar_volume_decline or not volume_declining_3)
    movement_ok = bool(one_hour_move >= cfg.min_recent_1h_move_pct)

    # 약한 횡보 반등은 감점, 실제 상승 추세는 가점
    ema_ordered = bool(row.ema9 > row.ema20 > row.ema60)
    ema9_rising = bool(row.ema9 > prev.ema9 > prevprev.ema9)
    higher_lows = bool(row.low > prev.low and prev.low >= prevprev.low)
    higher_highs = bool(row.high > prev.high and prev.high >= prevprev.high)
    trend_bonus = (
        (8 if ema_ordered else -8)
        + (6 if ema9_rising else -4)
        + (4 if higher_lows else 0)
        + (4 if higher_highs else 0)
    )

    p_score = (
        (25 if h1_up else 0)
        + (25 if pullback_ok else 0)
        + (25 if rebound else 0)
        + (10 if momentum_ok else 0)
        + min(10, volume_ratio * 7)
        + min(5, rebound_from_low * 4)
        + trend_bonus
    )
    p_ok = bool(
        h1_up and pullback_ok and not_chasing and rebound and momentum_ok
        and volume_ok and volume_trend_ok and movement_ok and not_extreme
        and rebound_from_low >= cfg.min_rebound_from_low_pct and p_score >= 65
    )

    # HJ 패턴은 진행 중인 현재 15분봉을 사용한다.
    # 강한 추세에서 긴 아래꼬리 음봉 뒤 양봉이 몸통을 회복하거나,
    # 연속 양봉 뒤 현재 장대양봉이 힘 있게 확장하는 경우를 별도로 잡는다.
    live = raw15.iloc[-1]
    last = raw15.iloc[-2]
    before = raw15.iloc[-3]
    live_price = float(live.close)
    live_gain = (live_price / float(live.open) - 1) * 100 if float(live.open) > 0 else 0.0
    live_volume_ratio = float(live.volume / live.vol_avg) if pd.notna(live.vol_avg) and live.vol_avg > 0 else 0.0

    last_body = abs(float(last.close - last.open))
    last_lower_wick = max(0.0, min(float(last.open), float(last.close)) - float(last.low))
    lower_wick_ratio = last_lower_wick / max(last_body, 1e-12)
    last_bearish = bool(last.close < last.open)
    body_recovery = (
        (live_price - float(last.close)) / max(float(last.open - last.close), 1e-12)
        if last_bearish else 0.0
    )

    live_ema_ordered = bool(live.ema9 > live.ema20 > live.ema60)
    live_ema_rising = bool(live.ema9 > last.ema9 and live.ema20 >= last.ema20)
    live_above_ema9 = bool(live_price >= live.ema9)
    recent_high_rising = bool(last.high >= before.high or live.high > last.high)
    recent_low_holding = bool(last.low >= before.low * 0.995 or live.low >= last.low)
    live_bullish = bool(live.close > live.open)

    continuation_three_bulls = bool(
        before.close > before.open
        and last.close > last.open
        and live_bullish
        and live_gain >= cfg.hj_min_current_gain_pct
        and live.close > last.close
    )
    wick_reversal = bool(
        last_bearish
        and lower_wick_ratio >= cfg.hj_min_lower_wick_body_ratio
        and live_bullish
        and body_recovery >= cfg.hj_min_body_recovery_pct
    )

    hj_trend_checks = [
        h1_up,
        live_ema_ordered,
        live_ema_rising,
        live_above_ema9,
        recent_high_rising,
        recent_low_holding,
    ]
    hj_trend_score = sum(1 for x in hj_trend_checks if x)
    hj_wick_volume_ok = bool(wick_reversal and live_volume_ratio >= 0.35)
    hj_continuation_volume_ok = bool(
        continuation_three_bulls and live_volume_ratio >= 0.60
    )
    hj_volume_ok = bool(hj_wick_volume_ok or hj_continuation_volume_ok)
    hj_momentum_ok = bool(45 <= float(live.rsi) <= 90)
    hj_pattern_ok = bool(wick_reversal or continuation_three_bulls)
    hj_ok = bool(
        cfg.hj_pattern_enabled
        and hj_pattern_ok
        and hj_trend_score >= cfg.hj_min_trend_score
        and hj_volume_ok
        and hj_momentum_ok
        and live_gain <= 8.0
    )
    hj_score = (
        hj_trend_score * 10
        + (20 if wick_reversal else 0)
        + (20 if continuation_three_bulls else 0)
        + min(15, live_volume_ratio * 8)
        + min(10, max(0.0, live_gain) * 4)
    )

    if hj_ok and (not p_ok or hj_score >= p_score):
        strategy = "HJ"
        score = float(hj_score)
        selected_price = live_price
    elif p_ok:
        strategy = "P"
        score = float(p_score)
        selected_price = price
    else:
        strategy = None
        score = float(max(p_score, hj_score))
        selected_price = live_price

    rejected = []
    if not strategy:
        if not p_ok:
            if not rebound:
                rejected.append("rebound")
            if not h1_up:
                rejected.append("h1_up")
            if not volume_ok:
                rejected.append("volume_ok")
            if p_score < 65:
                rejected.append("p_score")
        if not hj_ok:
            if not hj_pattern_ok:
                rejected.append("hj_pattern")
            if hj_trend_score < cfg.hj_min_trend_score:
                rejected.append("hj_trend")
            if not hj_volume_ok:
                rejected.append("hj_volume")

    details = {
        "price": selected_price,
        "strategy": strategy,
        "score": round(float(score), 2),
        "entry_reason": (
            "긴꼬리 음봉 후 양봉 몸통회복" if strategy == "HJ" and wick_reversal
            else "연속 양봉 후 장대양봉 확장" if strategy == "HJ"
            else "기존 반등 확인" if strategy == "P"
            else ""
        ),
        "h1_up": h1_up,
        "pullback_ok": pullback_ok,
        "rebound": rebound,
        "not_chasing": not_chasing,
        "volume_ok": volume_ok,
        "volume_trend_ok": volume_trend_ok,
        "movement_ok": movement_ok,
        "rsi": round(float(live.rsi if strategy == "HJ" else row.rsi), 2),
        "volume_ratio": round(float(live_volume_ratio if strategy == "HJ" else volume_ratio), 2),
        "pullback_from_high_pct": round(pullback_from_high, 2),
        "rebound_from_low_pct": round(rebound_from_low, 2),
        "entry_candle_gain_pct": round(float(live_gain if strategy == "HJ" else entry_candle_gain), 2),
        "distance_to_recent_high_pct": round(distance_to_high, 2),
        "one_hour_move_pct": round(one_hour_move, 2),
        "volume_declining_3": volume_declining_3,
        "confirmation_hold": confirmation_hold,
        "ema_ordered": ema_ordered,
        "ema9_rising": ema9_rising,
        "higher_lows": higher_lows,
        "higher_highs": higher_highs,
        "hj_wick_reversal": wick_reversal,
        "hj_continuation_three_bulls": continuation_three_bulls,
        "hj_trend_score": hj_trend_score,
        "hj_body_recovery_pct": round(body_recovery * 100, 2),
        "hj_lower_wick_body_ratio": round(lower_wick_ratio, 2),
        "rejected_conditions": list(dict.fromkeys(rejected)),
    }
    return strategy, float(score), details


def same_risk_group(symbol: str, open_symbols: set[str]) -> bool:
    return symbol in MEME_SYMBOLS and any(s in MEME_SYMBOLS for s in open_symbols)


class DailyBot:
    def __init__(self, config: DailyConfig | None = None):
        self.cfg = config or DailyConfig.load()
        self.client = BybitSwingClient(demo=self.cfg.mode != "live")
        init_db()
        ensure_scan_rejected_csv()

    def _saved_active_symbols(self) -> list[str]:
        try:
            value = json.loads(state_get("active_symbols", "[]"))
            return [str(x) for x in value if str(x)]
        except Exception:
            return []

    def active_symbols(self) -> list[str]:
        """24시간 상승률 상위 종목에서 유동성과 최근 움직임을 확인해 선별한다."""
        if not self.cfg.dynamic_universe:
            return list(self.cfg.symbols)

        now_ts = time.time()
        try:
            refreshed = float(state_get("universe_refreshed_at", "0") or 0)
        except Exception:
            refreshed = 0.0
        saved = self._saved_active_symbols()
        if saved and now_ts - refreshed < max(300, self.cfg.universe_refresh_minutes * 60):
            return saved

        pool = set(self.cfg.candidate_pool)
        excluded = set(self.cfg.slow_symbol_exclusions)
        ticker_ranked: list[tuple[float, str, dict[str, float]]] = []
        for ticker in self.client.tickers("SWAP"):
            symbol = str(ticker.get("symbol") or "")
            normalized = symbol.upper().replace("-", "").replace("_", "")
            base = normalized[:-4] if normalized.endswith("USDT") else normalized
            blocked_bases = {str(x).upper().replace("-", "").replace("_", "")
                             for x in self.cfg.non_crypto_base_exclusions}
            # 1000AXTIUSDT, AXTI-USDT 같은 변형도 차단한다.
            non_crypto_match = any(
                base == blocked or base.endswith(blocked) or blocked in base
                for blocked in blocked_bases
            )
            if (
                symbol in excluded
                or non_crypto_match
                or not normalized.endswith("USDT")
            ):
                if non_crypto_match:
                    log_event(symbol, "NON_CRYPTO_EXCLUDED", mode=self.cfg.mode, details=f"base={base}")
                continue
            try:
                last = float(ticker.get("lastPrice") or 0)
                open24 = float(ticker.get("prevPrice24h") or 0)
                high = float(ticker.get("highPrice24h") or 0)
                low = float(ticker.get("lowPrice24h") or 0)
                bid = float(ticker.get("bid1Price") or 0)
                ask = float(ticker.get("ask1Price") or 0)
                quote_vol = float(ticker.get("turnover24h") or 0)
                if min(last, open24, high, low) <= 0:
                    continue
                range_pct = (high / low - 1) * 100
                change_pct = (last / open24 - 1) * 100
                spread_pct = ((ask - bid) / last * 100) if bid > 0 and ask >= bid else 99.0
                if quote_vol < self.cfg.min_quote_volume_24h_usdt:
                    continue
                if not (self.cfg.min_range_24h_pct <= range_pct <= self.cfg.max_range_24h_pct):
                    continue
                if change_pct < self.cfg.min_change_24h_pct or change_pct > self.cfg.max_abs_change_24h_pct:
                    continue
                if spread_pct > self.cfg.max_spread_pct:
                    continue
                liquidity_score = math.log10(max(quote_vol, 1.0)) * 4
                gainer_score = min(change_pct, 35.0) * 8
                movement_score = min(range_pct, 14.0) * 3
                spread_penalty = spread_pct * 90
                ticker_score = liquidity_score + gainer_score + movement_score - spread_penalty
                ticker_ranked.append((ticker_score, symbol, {
                    "quote_volume": quote_vol, "range_pct": range_pct,
                    "change_pct": change_pct, "spread_pct": spread_pct,
                }))
            except (TypeError, ValueError, ZeroDivisionError):
                continue

        ticker_ranked.sort(reverse=True, key=lambda x: x[0])
        prefiltered = ticker_ranked[: max(self.cfg.top_gainers_pool_size, self.cfg.universe_size)]
        ranked: list[tuple[float, str, dict[str, float]]] = []
        for ticker_score, symbol, details in prefiltered:
            try:
                m15 = confirmed(self.client.candles(symbol, "15m", 24))
                if len(m15) < 16:
                    continue
                recent16 = m15.tail(16)
                recent4h_range = (float(recent16.high.max()) / float(recent16.low.min()) - 1) * 100
                hourly_ranges: list[float] = []
                for idx in range(0, 16, 4):
                    block = recent16.iloc[idx:idx + 4]
                    if len(block) == 4 and float(block.low.min()) > 0:
                        hourly_ranges.append((float(block.high.max()) / float(block.low.min()) - 1) * 100)
                avg_hourly_range = sum(hourly_ranges) / len(hourly_ranges) if hourly_ranges else 0.0
                recent1h_move = abs(float(recent16.iloc[-1].close / recent16.iloc[-5].close - 1)) * 100
                if recent4h_range < self.cfg.min_recent_4h_range_pct:
                    continue
                if avg_hourly_range < self.cfg.min_avg_hourly_range_pct:
                    continue
                if recent1h_move > self.cfg.max_recent_1h_move_pct:
                    continue
                if recent1h_move < self.cfg.min_recent_1h_move_pct:
                    continue
                intraday_score = min(recent4h_range, 8.0) * 10 + min(avg_hourly_range, 3.0) * 12
                final_score = ticker_score + intraday_score
                ranked.append((final_score, symbol, {
                    **details,
                    "recent_4h_range_pct": recent4h_range,
                    "avg_hourly_range_pct": avg_hourly_range,
                    "recent_1h_move_pct": recent1h_move,
                }))
            except Exception as exc:
                log_event(symbol, "UNIVERSE_VOL_ERROR", mode=self.cfg.mode, details=str(exc))

        ranked.sort(reverse=True, key=lambda x: x[0])
        selected = [symbol for _, symbol, _ in ranked[: max(1, self.cfg.universe_size)]]
        if not selected:
            selected = list(self.cfg.symbols)
        state_set("active_symbols", json.dumps(selected, ensure_ascii=False))
        state_set("universe_refreshed_at", str(now_ts))
        state_set("universe_details", json.dumps(
            [{"symbol": symbol, "score": round(score, 2), **details} for score, symbol, details in ranked[: self.cfg.universe_size]],
            ensure_ascii=False,
        ))
        log_event("", "UNIVERSE_REFRESH", mode=self.cfg.mode, details=json.dumps(selected, ensure_ascii=False))
        return selected

    def open_rows(self) -> list[sqlite3.Row]:
        with db() as conn:
            return conn.execute("SELECT * FROM bot_positions WHERE status='OPEN' ORDER BY opened_at").fetchall()

    def daily_entries(self) -> int:
        day = trading_day()
        with db() as conn:
            return int(conn.execute(
                "SELECT COUNT(*) FROM bot_events WHERE event='ENTRY' AND substr(ts,1,10)=?", (day,)
            ).fetchone()[0])

    def daily_realized(self) -> float:
        day = trading_day()
        with db() as conn:
            value = conn.execute(
                "SELECT COALESCE(SUM(realized_pnl),0) FROM bot_events WHERE substr(ts,1,10)=?", (day,)
            ).fetchone()[0]
        return float(value or 0)

    def consecutive_losses(self) -> int:
        """실제 손절(STOP/BE_EXIT)만 연속손실로 센다.

        정체 종료(FLAT_EXIT)와 시간 종료(TIME_EXIT)는 데이터 수집용 중립 종료로
        간주해 카운트에서 제외한다. 수익 거래가 나오면 연속손실은 즉시 0으로
        초기화된다.
        """
        day = trading_day()
        with db() as conn:
            rows = conn.execute(
                """SELECT realized_pnl, note FROM bot_positions
                   WHERE status='CLOSED' AND substr(updated_at,1,10)=?
                   ORDER BY updated_at DESC""",
                (day,),
            ).fetchall()
        count = 0
        for row in rows:
            pnl = float(row["realized_pnl"] or 0)
            reason = str(row["note"] or "").upper()
            if pnl > 0 or reason in {"TP1", "TP2"}:
                break
            if reason in {"STOP", "BE_EXIT"} and pnl < 0:
                count += 1
                continue
            if reason.startswith("FLAT_EXIT") or reason == "TIME_EXIT":
                continue
            # 알 수 없는 음수 종료는 안전하게 손절로 계산한다.
            if pnl < 0:
                count += 1
                continue
            break
        return count

    def loss_cooldown_active(self) -> bool:
        """연속 손절 3회 뒤 설정 시간 동안 신규 진입을 쉬어 시장 국면 전환을 기다린다."""
        if self.consecutive_losses() < self.cfg.max_consecutive_losses:
            return False
        with db() as conn:
            row = conn.execute(
                """SELECT updated_at FROM bot_positions
                   WHERE status='CLOSED' AND note IN ('STOP','BE_EXIT')
                   ORDER BY updated_at DESC LIMIT 1"""
            ).fetchone()
        if not row or not row["updated_at"]:
            return False
        try:
            last_loss = datetime.fromisoformat(row["updated_at"])
            return datetime.now(timezone.utc) - last_loss < timedelta(minutes=max(0, self.cfg.loss_cooldown_minutes))
        except (TypeError, ValueError):
            return False

    def symbol_in_cooldown(self, symbol: str) -> bool:
        """같은 종목을 종료한 뒤 설정 시간 동안 재진입하지 않는다."""
        minutes = max(0, int(self.cfg.same_symbol_cooldown_minutes))
        if minutes <= 0:
            return False
        with db() as conn:
            row = conn.execute(
                """SELECT updated_at FROM bot_positions
                   WHERE symbol=? AND status='CLOSED'""",
                (symbol,),
            ).fetchone()
        if not row or not row["updated_at"]:
            return False
        try:
            closed_at = datetime.fromisoformat(row["updated_at"])
            return datetime.now(timezone.utc) - closed_at < timedelta(minutes=minutes)
        except (TypeError, ValueError):
            return False

    def _execute(self, symbol: str, side: str, qty: float, reduce_only: bool = False) -> None:
        if self.cfg.mode == "paper":
            return
        if self.cfg.mode == "live" and not self.client.private_configured:
            raise BybitSwingError("LIVE 모드인데 API 설정이 없습니다.")
        self.client.set_leverage(symbol, self.cfg.leverage, self.cfg.margin_mode, "long")
        self.client.place_market_order(
            symbol, side, f"{qty:.8f}", self.cfg.margin_mode, "long", reduce_only,
            client_order_id=f"HJ{int(time.time())}{symbol[:4]}"
        )

    def _open(self, symbol: str, price: float, strategy: str, score: float, signal_details: dict[str, Any] | None = None) -> None:
        qty = qty_from_margin(price, self.cfg.position_margin_usdt, self.cfg.leverage)
        self._execute(symbol, "buy", qty)
        trade_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
        with db() as conn:
            conn.execute("DELETE FROM bot_positions WHERE symbol=?", (symbol,))
            conn.execute(
                """INSERT INTO bot_positions(
                    symbol,status,opened_at,updated_at,avg_price,total_qty,total_margin,dca_count,tp1_done,
                    last_price,unrealized_pct,note,strategy,realized_pnl,entry_date_kst,
                    base_entry_price,base_qty,add_qty,add_price,lowest_price,highest_price,cycle_anchor_price,trade_id,stop_stage1_done
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (symbol, "OPEN", utc_now(), utc_now(), price, qty, self.cfg.position_margin_usdt, 0, 0,
                 price, 0.0, f"{strategy}형 데일리 진입", strategy, 0.0, trading_day(),
                 price, qty, 0.0, 0.0, price, price, price, trade_id, 0),
            )
        signal_details = signal_details or {}
        details = json.dumps({
            "entry_price": price, "margin_usdt": self.cfg.position_margin_usdt,
            "leverage": self.cfg.leverage, "qty": qty, "score": round(score, 2),
            "strategy": strategy,
            "rsi": signal_details.get("rsi"),
            "ema9": signal_details.get("ema9"),
            "ema20": signal_details.get("ema20"),
            "ema60": signal_details.get("ema60"),
            "volume_ratio": signal_details.get("volume_ratio"),
            "change_24h_pct": signal_details.get("change_24h_pct"),
            "recent_1h_move_pct": signal_details.get("recent_1h_move_pct"),
            "recent_4h_range_pct": signal_details.get("recent_4h_range_pct"),
            "pullback_pct": signal_details.get("pullback_pct"),
            "rebound_pct": signal_details.get("rebound_pct"),
            "h1_up": signal_details.get("h1_up"),
            "rebound": signal_details.get("rebound"),
            "not_chasing": signal_details.get("not_chasing"),
            "entry_reason": signal_details.get("reason") or signal_details.get("entry_reason") or "조건 통과",
            "signal_snapshot": signal_details,
        }, ensure_ascii=False)
        log_event(symbol, "ENTRY", price, qty, self.cfg.mode, details, strategy, trade_id=trade_id)

    def _rebound_add(self, row: sqlite3.Row, price: float) -> None:
        add_qty = qty_from_margin(price, self.cfg.rebound_add_margin_usdt, self.cfg.leverage)
        if add_qty <= 0:
            return
        self._execute(row["symbol"], "buy", add_qty)
        old_qty = float(row["total_qty"])
        old_avg = float(row["avg_price"])
        new_qty = old_qty + add_qty
        new_avg = (old_avg * old_qty + price * add_qty) / new_qty
        with db() as conn:
            conn.execute(
                """UPDATE bot_positions SET avg_price=?,total_qty=?,total_margin=?,dca_count=?,
                   add_qty=?,add_price=?,updated_at=?,last_price=?,note=? WHERE symbol=?""",
                (new_avg, new_qty, float(row["total_margin"]) + self.cfg.rebound_add_margin_usdt,
                 int(row["dca_count"] or 0) + 1, add_qty, price, utc_now(), price,
                 "반등 확인 후 순환 추가진입", row["symbol"]),
            )
        details = json.dumps({"add_price": price, "add_margin_usdt": self.cfg.rebound_add_margin_usdt,
                              "add_qty": add_qty, "previous_avg": old_avg, "new_avg": new_avg,
                              "cycle_no": int(row["dca_count"] or 0) + 1}, ensure_ascii=False)
        log_event(row["symbol"], "REBOUND_ADD", price, add_qty, self.cfg.mode,
                  details=details, strategy=row["strategy"] or "", trade_id=row["trade_id"] or "")

    def _cycle_reduce(self, row: sqlite3.Row, price: float) -> None:
        add_qty = float(row["add_qty"] or 0)
        if add_qty <= 0:
            return
        self._execute(row["symbol"], "sell", add_qty, reduce_only=True)
        pnl_usdt = (price - float(row["add_price"] or row["avg_price"])) * add_qty
        remaining = max(0.0, float(row["total_qty"]) - add_qty)
        base_price = float(row["base_entry_price"] or row["avg_price"])
        total_realized = float(row["realized_pnl"] or 0) + pnl_usdt
        with db() as conn:
            conn.execute(
                """UPDATE bot_positions SET avg_price=?,total_qty=?,total_margin=?,add_qty=0,add_price=0,
                   updated_at=?,last_price=?,note=?,realized_pnl=?,lowest_price=?,highest_price=?,cycle_anchor_price=? WHERE symbol=?""",
                (base_price, remaining, max(0.0, float(row["total_margin"]) - self.cfg.rebound_add_margin_usdt),
                 utc_now(), price, "순환 추가분 정리 · 최초 물량 유지", total_realized,
                 price, price, price, row["symbol"]),
            )
        details = json.dumps({"add_entry_price": float(row["add_price"] or 0), "reduce_price": price,
                              "reduced_qty": add_qty, "avg_before_reduce": float(row["avg_price"]),
                              "restored_base_avg": base_price, "remaining_qty": remaining,
                              "cycle_realized_pnl": pnl_usdt}, ensure_ascii=False)
        log_event(row["symbol"], "CYCLE_REDUCE", price, add_qty, self.cfg.mode,
                  details=details, strategy=row["strategy"] or "", realized_pnl=pnl_usdt,
                  trade_id=row["trade_id"] or "")

    def _close(self, row: sqlite3.Row, price: float, fraction: float, reason: str,
               detected_price: float | None = None, trigger_price: float | None = None) -> None:
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
                if reason == "TP1":
                    conn.execute(
                        "UPDATE bot_positions SET total_qty=?,tp1_done=1,updated_at=?,last_price=?,note=?,realized_pnl=? WHERE symbol=?",
                        (remaining, utc_now(), price, reason, total_realized, row["symbol"]),
                    )
                elif reason == "STOP_HALF":
                    # 1차 손절은 TP1 완료로 처리하지 않는다. 그렇지 않으면 다음 틱에 BE_EXIT가 잘못 발동한다.
                    conn.execute(
                        "UPDATE bot_positions SET total_qty=?,stop_stage1_done=1,updated_at=?,last_price=?,note=?,realized_pnl=? WHERE symbol=?",
                        (remaining, utc_now(), price, reason, total_realized, row["symbol"]),
                    )
                else:
                    conn.execute(
                        "UPDATE bot_positions SET total_qty=?,updated_at=?,last_price=?,note=?,realized_pnl=? WHERE symbol=?",
                        (remaining, utc_now(), price, reason, total_realized, row["symbol"]),
                    )
        details = json.dumps({"exit_price": price,
                              "detected_market_price": detected_price if detected_price is not None else price,
                              "configured_trigger_price": trigger_price,
                              "avg_at_exit": float(row["avg_price"]),
                              "base_entry_price": float(row["base_entry_price"] or row["avg_price"]),
                              "closed_qty": qty, "fraction": fraction, "remaining_qty": remaining,
                              "step_realized_pnl": pnl_usdt, "trade_total_realized_pnl": total_realized,
                              "reason": reason}, ensure_ascii=False)
        log_event(row["symbol"], reason, price, qty, self.cfg.mode, details=details,
                  strategy=row["strategy"] or "", realized_pnl=pnl_usdt,
                  trade_id=row["trade_id"] or "")
        self._register_stop_review(row, reason, price)

    def _register_stop_review(self, row: sqlite3.Row, stop_event: str, stop_price: float) -> None:
        """손절 발생 후 15·30·60·120·180분 가격을 자동 추적한다."""
        if stop_event not in {"STOP_HALF", "FINAL_STOP", "STOP", "BE_EXIT"}:
            return
        entry_price = float(row["base_entry_price"] or row["avg_price"] or 0)
        pnl_at_stop_pct = ((stop_price / entry_price) - 1) * 100 if entry_price > 0 else None
        with db() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO stop_reviews(
                    trade_id,symbol,strategy,stop_event,stop_ts,entry_price,stop_price,pnl_at_stop_pct
                ) VALUES(?,?,?,?,?,?,?,?)""",
                (
                    str(row["trade_id"] or ""),
                    str(row["symbol"]),
                    str(row["strategy"] or ""),
                    stop_event,
                    utc_now(),
                    entry_price,
                    float(stop_price),
                    pnl_at_stop_pct,
                ),
            )

    def update_stop_reviews(self) -> None:
        """미완료 손절 리뷰를 현재 시세로 채우고 3시간 뒤 자동 분류한다."""
        milestones = (
            (15, "price_15m", "pct_15m"),
            (30, "price_30m", "pct_30m"),
            (60, "price_60m", "pct_60m"),
            (120, "price_120m", "pct_120m"),
            (180, "price_180m", "pct_180m"),
        )
        with db() as conn:
            pending = conn.execute(
                "SELECT * FROM stop_reviews WHERE completed=0 ORDER BY stop_ts"
            ).fetchall()

        for review in pending:
            try:
                stopped_at = datetime.fromisoformat(str(review["stop_ts"]))
                if stopped_at.tzinfo is None:
                    stopped_at = stopped_at.replace(tzinfo=timezone.utc)
                elapsed_min = (datetime.now(timezone.utc) - stopped_at).total_seconds() / 60
                due = [
                    (m, pcol, pctcol)
                    for m, pcol, pctcol in milestones
                    if elapsed_min >= m and review[pcol] is None
                ]
                if not due:
                    continue

                current_price = float(self.client.ticker(review["symbol"]).get("last") or 0)
                if current_price <= 0:
                    continue
                stop_price = float(review["stop_price"])
                pct_vs_stop = ((current_price / stop_price) - 1) * 100 if stop_price > 0 else 0.0

                updates = []
                values = []
                for _, pcol, pctcol in due:
                    updates.extend([f"{pcol}=?", f"{pctcol}=?"])
                    values.extend([current_price, pct_vs_stop])

                if elapsed_min >= 180:
                    if pct_vs_stop >= 1.5:
                        label = "아까운 손절"
                    elif pct_vs_stop <= -1.5:
                        label = "좋은 손절"
                    else:
                        label = "애매한 손절"
                    updates.extend(["review_label=?", "completed=1"])
                    values.append(label)

                values.append(int(review["id"]))
                with db() as conn:
                    conn.execute(
                        f"UPDATE stop_reviews SET {', '.join(updates)} WHERE id=?",
                        values,
                    )
            except Exception as exc:
                log_event(
                    str(review["symbol"] or ""),
                    "STOP_REVIEW_ERROR",
                    mode=self.cfg.mode,
                    details=str(exc),
                    trade_id=str(review["trade_id"] or ""),
                )

    def manage(self) -> None:
        for row in self.open_rows():
            price = float(self.client.ticker(row["symbol"]).get("last") or 0)
            if price <= 0:
                continue
            avg = float(row["avg_price"])
            base_price = float(row["base_entry_price"] or avg)
            pnl_pct = (price / avg - 1) * 100
            base_pnl_pct = (price / base_price - 1) * 100
            age_h = (datetime.now(timezone.utc) - datetime.fromisoformat(row["opened_at"])).total_seconds() / 3600
            lowest = min(float(row["lowest_price"] or price), price)
            highest = max(float(row["highest_price"] or price), price)
            with db() as conn:
                conn.execute("UPDATE bot_positions SET last_price=?,unrealized_pct=?,lowest_price=?,highest_price=?,updated_at=? WHERE symbol=?",
                             (price, base_pnl_pct, lowest, highest, utc_now(), row["symbol"]))

            # 추가분을 보유 중이면, 혼합평단 + 소폭 버퍼 회복 시 추가 수량만큼 우선 정리한다.
            if float(row["add_qty"] or 0) > 0:
                cycle_target = avg * (1 + self.cfg.rebound_exit_buffer_pct / 100)
                if price >= cycle_target:
                    self._cycle_reduce(row, price)
                    continue

            # 순환 추가분을 이미 회수했다면 다시 밀린 뒤 새 반등이 확인될 때 최대 설정 횟수까지 반복한다.
            if (self.cfg.rebound_add_enabled and float(row["add_qty"] or 0) <= 0
                    and int(row["dca_count"] or 0) < self.cfg.max_cycle_adds
                    and int(row["tp1_done"] or 0) == 0
                    and int(row["stop_stage1_done"] or 0) == 0):
                anchor = float(row["cycle_anchor_price"] or base_price)
                drawdown_pct = (lowest / anchor - 1) * 100
                if drawdown_pct <= -abs(self.cfg.rebound_arm_drawdown_pct):
                    try:
                        ok, details = rebound_add_signal(self.client, row["symbol"], self.cfg)
                        if ok:
                            self._rebound_add(row, float(details.get("price") or price))
                            continue
                    except Exception as exc:
                        log_event(row["symbol"], "REBOUND_CHECK_ERROR", mode=self.cfg.mode, details=str(exc))

            # 손절과 목표가는 최초 진입가 기준으로 관리한다.
            # PAPER에서는 조회 주기 사이 급변으로 계획 손절폭을 초과해 기록하지 않도록
            # 최초 터치 가격(설정 트리거가)을 체결가로 사용하고, 감지 당시 시장가는 별도 기록한다.
            def paper_fill(trigger: float) -> float:
                if self.cfg.mode == "paper" and self.cfg.paper_fill_at_trigger:
                    return trigger
                return price

            stop_stage1_done = int(row["stop_stage1_done"] or 0)
            if int(row["tp1_done"]) == 1 and base_pnl_pct <= -self.cfg.breakeven_stop_pct:
                trigger = base_price * (1 - self.cfg.breakeven_stop_pct / 100)
                self._close(row, paper_fill(trigger), 1.0, "BE_EXIT", price, trigger)
            elif self.cfg.staged_stop_enabled and stop_stage1_done == 0 and base_pnl_pct <= -self.cfg.stage1_stop_pct:
                trigger = base_price * (1 - self.cfg.stage1_stop_pct / 100)
                self._close(row, paper_fill(trigger), self.cfg.stage1_stop_fraction, "STOP_HALF", price, trigger)
            elif self.cfg.staged_stop_enabled and stop_stage1_done == 1 and base_pnl_pct <= -self.cfg.final_stop_pct:
                trigger = base_price * (1 - self.cfg.final_stop_pct / 100)
                self._close(row, paper_fill(trigger), 1.0, "FINAL_STOP", price, trigger)
            elif self.cfg.staged_stop_enabled and stop_stage1_done == 1 and base_pnl_pct >= -self.cfg.recovery_exit_loss_pct:
                # 1차 손절 뒤 본절권으로 회복하면 남은 물량을 실제 감지가격에 정리한다.
                trigger = base_price * (1 - self.cfg.recovery_exit_loss_pct / 100)
                self._close(row, price, 1.0, "RECOVERY_EXIT", price, trigger)
            elif (not self.cfg.staged_stop_enabled) and base_pnl_pct <= -self.cfg.hard_stop_pct:
                trigger = base_price * (1 - self.cfg.hard_stop_pct / 100)
                self._close(row, paper_fill(trigger), 1.0, "STOP", price, trigger)
            elif (age_h * 60 >= self.cfg.flat_exit_minutes
                  and (highest / base_price - 1) * 100 < self.cfg.flat_min_favorable_pct):
                self._close(row, price, 1.0, "FLAT_EXIT_75M", price, None)
            elif age_h >= self.cfg.max_hold_hours:
                self._close(row, price, 1.0, "TIME_EXIT", price, None)
            elif int(row["tp1_done"]) == 0 and base_pnl_pct >= self.cfg.tp1_pct:
                trigger = base_price * (1 + self.cfg.tp1_pct / 100)
                self._close(row, paper_fill(trigger), 0.5, "TP1", price, trigger)
            elif int(row["tp1_done"]) == 1 and base_pnl_pct >= self.cfg.tp2_pct:
                trigger = base_price * (1 + self.cfg.tp2_pct / 100)
                self._close(row, paper_fill(trigger), 1.0, "TP2", price, trigger)

    def scan_entries(self) -> None:
        if state_flag("pause_new_entries", False):
            return
        if len(self.open_rows()) >= self.cfg.max_positions:
            return
        if self.cfg.max_daily_entries > 0 and self.daily_entries() >= self.cfg.max_daily_entries:
            return
        # PAPER 긴급복구: 과거 손익/연속손절 기록이 신규 진입을 영구 차단하지 않게 한다.
        if self.cfg.mode != "paper":
            if self.daily_realized() <= -abs(self.cfg.daily_loss_limit_usdt):
                log_event("", "DAILY_STOP", mode=self.cfg.mode,
                          details=f"pnl={self.daily_realized():.2f}")
                return
            losses = self.consecutive_losses()
            if losses >= self.cfg.max_consecutive_losses and self.loss_cooldown_active():
                log_event("", "LOSS_COOLDOWN", mode=self.cfg.mode,
                          details=f"losses={losses}; {self.cfg.loss_cooldown_minutes}분 신규 진입 휴식")
                return
        if self.cfg.mode != "paper":
            balance = self.client.balance("USDT")
            if balance <= self.cfg.emergency_stop_balance:
                log_event("", "EMERGENCY_STOP", mode=self.cfg.mode, details=f"balance={balance}")
                return
            if balance < self.cfg.min_balance_to_trade:
                return

        candidates: list[tuple[float, str, str, dict[str, Any]]] = []
        for symbol in self.active_symbols():
            if self.symbol_in_cooldown(symbol):
                log_event(symbol, "COOLDOWN_WAIT", mode=self.cfg.mode,
                          details=f"{self.cfg.same_symbol_cooldown_minutes}분 재진입 대기")
                continue
            try:
                strategy, score, details = candidate_signal(self.client, symbol, self.cfg)
                log_event(symbol, "SCAN_OK" if strategy else "SCAN_WAIT", float(details.get("price", 0)),
                          mode=self.cfg.mode, details=json.dumps(details, ensure_ascii=False), strategy=strategy or "")
                append_scan_record(symbol, strategy, score, details)
                if strategy:
                    candidates.append((score, symbol, strategy, details))
            except Exception as exc:
                # 특정 종목의 일시적 API/상장 상태 문제 때문에 전체 스캔이 멈추지 않게 한다.
                log_event(symbol, "SCAN_ERROR", mode=self.cfg.mode, details=str(exc))

        if candidates:
            open_symbols = {str(r["symbol"]) for r in self.open_rows()}
            slots = max(0, self.cfg.max_positions - len(open_symbols))
            entries_left = (max(0, self.cfg.max_daily_entries - self.daily_entries())
                            if self.cfg.max_daily_entries > 0 else slots)
            for score, symbol, strategy, details in sorted(
                candidates, reverse=True, key=lambda x: x[0]
            ):
                if slots <= 0 or entries_left <= 0:
                    break
                if symbol in open_symbols or same_risk_group(symbol, open_symbols):
                    continue

                price = float(details["price"])
                append_entry_record(
                    symbol, "ENTRY_ATTEMPT", strategy, score, price
                )
                log_event(
                    symbol, "ENTRY_ATTEMPT", price,
                    mode=self.cfg.mode,
                    details=json.dumps({
                        "strategy": strategy,
                        "score": score,
                        "slots": slots,
                    }, ensure_ascii=False),
                    strategy=strategy,
                )
                try:
                    self._open(symbol, price, strategy, score, details)
                except Exception as exc:
                    message = f"{type(exc).__name__}: {exc}"
                    append_entry_record(
                        symbol, "ENTRY_ERROR", strategy, score, price, message
                    )
                    log_event(
                        symbol, "ENTRY_ERROR", price,
                        mode=self.cfg.mode, details=message,
                        strategy=strategy,
                    )
                    continue

                append_entry_record(
                    symbol, "ENTRY_SUCCESS", strategy, score, price
                )
                open_symbols.add(symbol)
                slots -= 1
                entries_left -= 1

    def run_once(self) -> None:
        self.manage()
        self.update_stop_reviews()
        self.scan_entries()

    def run_forever(self) -> None:
        state_set("bot_process_status", "RUNNING")
        log_event("", "BOT_START", mode=self.cfg.mode, details=json.dumps(asdict(self.cfg), ensure_ascii=False))
        next_scan_at = 0.0
        while True:
            loop_started = time.monotonic()
            try:
                # 보유 포지션 TP/SL 관리는 1초 주기로, 신규 후보 스캔은 별도 주기로 분리한다.
                self.manage()
                self.update_stop_reviews()
                if state_flag("shutdown_when_flat", False) and not self.open_rows():
                    state_set("bot_process_status", "STOPPED")
                    state_set("shutdown_when_flat", "0")
                    log_event("", "BOT_SAFE_STOP", mode=self.cfg.mode, details="포지션 0 확인 후 안전 종료")
                    break
                now_mono = time.monotonic()
                if now_mono >= next_scan_at:
                    self.scan_entries()
                    next_scan_at = now_mono + max(30.0, float(self.cfg.scan_seconds))
            except Exception as exc:
                log_event("", "ERROR", mode=self.cfg.mode, details=str(exc))
            elapsed = time.monotonic() - loop_started
            time.sleep(max(0.1, float(self.cfg.manage_seconds) - elapsed))


# 기존 실행 파일(run_okx_swing_bot.py)과 호환
SwingBot = DailyBot
SwingConfig = DailyConfig
