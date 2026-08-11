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
BOT_RUNTIME_VERSION = "RC-v4.3.24-UBGuardAndLateTrendFailure"

# HJ 신고점 돌파 예외는 한 번의 순간 스파이크로 열지 않는다.
# 같은 종목이 다음 스캔에서도 돌파 상태를 유지해야 "확인된 돌파"로 인정한다.
_HJ_BREAKOUT_CONFIRM: dict[str, dict[str, float]] = {}



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
    max_recent_1h_move_pct: float = 6.0
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
    hj_position_margin_usdt: float = 36.0
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
    same_symbol_cooldown_minutes: int = 90
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
    rebound_add_margin_usdt: float = 27.0
    hj_rebound_add_margin_usdt: float = 18.0
    hj_structure_stop_enabled: bool = True

    # 구조손절 재설계:
    # - 고정 USDT 손절은 사용하지 않는다.
    # - 확정 15분봉 기준으로 "상승구조가 실제로 깨졌는지"를 본다.
    # - 급락/추세붕괴는 1개 확정봉으로도 종료 가능.
    # - 모든 구조 조건을 놓쳤을 때만 최종 재난손절을 사용한다.
    structure_break_low_pct: float = 0.50
    structure_rsi_weak: float = 45.0
    structure_rsi_crash: float = 40.0
    structure_emergency_stop_pct: float = 8.0

    # 진입 직후 실패판정: 첫 10~15분만 별도 감시
    early_failure_enabled: bool = True
    early_failure_window_minutes: int = 15
    early_failure_min_age_minutes: int = 10
    early_failure_ema_reclaim_buffer_pct: float = 0.10

    # 진행 중 15분봉의 가짜 양봉 진입 차단
    live_reversal_min_prev_body_recovery: float = 0.50
    live_reversal_min_body_pct: float = 0.20
    live_reversal_max_upper_wick_to_body: float = 1.50
    live_reversal_require_5m_bullish: bool = True

    # 2026-08-11: 손실 사례(BEAT/ARB/H, PUMPFUN/ALT/1000NEIROCTO) 기반 진입 품질 보강
    # RSI 하나만으로 과열을 차단하지 않고, 고점근접+과열/2차급등 조합일 때만 HJ를 차단한다.
    hj_high_zone_max_distance_pct: float = 0.50
    hj_high_zone_overheat_rsi: float = 75.0
    hj_high_zone_min_bb_excess_pct: float = 0.25
    hj_second_push_min_rebound_pct: float = 4.0
    hj_second_push_min_live_gain_pct: float = 0.90

    # P형은 약한/늦은 반등만 골라 차단한다. 강한 추세 자체(RSI 고점)는 허용한다.
    p_require_ema_ordered: bool = True
    p_near_high_weak_max_distance_pct: float = 1.00
    p_near_high_weak_min_gain_pct: float = 0.60
    p_near_high_weak_min_rsi: float = 65.0
    p_faded_spike_min_pullback_pct: float = 2.00
    p_faded_spike_min_rebound_pct: float = 5.00
    p_faded_spike_min_gain_pct: float = 0.60

    # Early Failure(10~15분) 이후 구조손절까지 비는 구간을 메우는 5분봉 빠른 실패판정.
    # 고정 USDT 손절이 아니라, 실제 5분 구조 붕괴가 동반될 때만 동작한다.
    fast_failure_window_minutes: int = 45
    fast_failure_rsi_max: float = 48.0
    fast_failure_low_break_pct: float = 0.30

    rebound_exit_buffer_pct: float = 0.10
    max_cycle_adds: int = 2
    rebound_min_drawdown_pct: float = 1.5  # 이보다 얕은 구간에서는 물타기 금지(arm 트리거가 아님)
    flat_exit_minutes: int = 60
    flat_min_favorable_pct: float = 0.40
    # 정체종료는 실제 횡보일 때만 허용한다.
    # 진입가 대비 손실이 이보다 크면 정체가 아니라 하락 진행으로 보고 FLAT_EXIT를 금지한다.
    flat_max_loss_pct: float = 1.5
    flat_max_recent_range_pct: float = 3.0
    flat_max_ema20_distance_pct: float = 1.0
    flat_rsi_min: float = 40.0
    flat_rsi_max: float = 60.0
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
    hj_max_rsi: float = 89.99
    bb_chase_soft_pct: float = 0.50
    bb_chase_hard_pct: float = 1.00
    bb_chase_soft_rsi: float = 85.0
    bb_chase_soft_candle_gain_pct: float = 2.0
    bb_chase_soft_candle_range_pct: float = 3.0
    bb_chase_soft_volume_ratio: float = 2.0

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

def rotate_scan_csv_if_needed(max_bytes: int = 5_000_000, keep_rows: int = 3000) -> None:
    """SCAN CSV가 커지면 전체 원본은 날짜별 archive로 옮기고 최근 행만 유지한다."""
    try:
        if not SCAN_REJECTED_CSV_PATH.exists() or SCAN_REJECTED_CSV_PATH.stat().st_size <= max_bytes:
            return
        archive_dir = SCAN_REJECTED_CSV_PATH.parent / "scan_archive"
        archive_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
        archive_path = archive_dir / f"scan_rejected_{stamp}_KST.csv"
        raw = SCAN_REJECTED_CSV_PATH.read_bytes()
        archive_path.write_bytes(raw)
        text = raw.decode("utf-8-sig", errors="replace").splitlines()
        header = text[:1]
        recent = text[-keep_rows:] if len(text) > keep_rows + 1 else text[1:]
        SCAN_REJECTED_CSV_PATH.write_text("\n".join(header + recent) + "\n", encoding="utf-8-sig")
    except Exception as exc:
        log_event("", "SCAN_ROTATE_ERROR", mode="paper", details=str(exc))


def append_scan_record(symbol: str, strategy: str | None, score: float, details: dict[str, Any]) -> None:
    """진입 후보의 통과/탈락 사유를 CSV에 즉시 누적한다."""
    ensure_scan_rejected_csv()
    rotate_scan_csv_if_needed()
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
    rotate_scan_csv_if_needed()
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
        _ensure_column(conn, "bot_positions", "last_add_15m_bucket", "TEXT")
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
    out["bb_mid"] = out["close"].rolling(20).mean()
    bb_std = out["close"].rolling(20).std(ddof=0)
    out["bb_upper"] = out["bb_mid"] + 2 * bb_std
    out["bb_lower"] = out["bb_mid"] - 2 * bb_std
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

    # P형 손실 패턴 보강:
    # 1) EMA 정배열이 아닌 상태의 반등(1000NEIROCTO형)
    # 2) 최근 고점 바로 아래에서 강한 양봉처럼 보이지만 새 고점은 못 만드는 재진입(PUMPFUN형)
    # 3) 이미 급등 후 2%+ 밀린 뒤 5%+ 되돌림 끝자락에서 다시 강하게 들어가는 경우(ALT형)
    p_ema_quality_ok = bool((not cfg.p_require_ema_ordered) or ema_ordered)
    p_near_high_weak_reentry = bool(
        distance_to_high <= cfg.p_near_high_weak_max_distance_pct
        and not higher_highs
        and entry_candle_gain >= cfg.p_near_high_weak_min_gain_pct
        and float(row.rsi) >= cfg.p_near_high_weak_min_rsi
    )
    p_faded_spike_reentry = bool(
        pullback_from_high >= cfg.p_faded_spike_min_pullback_pct
        and rebound_from_low >= cfg.p_faded_spike_min_rebound_pct
        and entry_candle_gain >= cfg.p_faded_spike_min_gain_pct
    )
    p_entry_quality_ok = bool(
        p_ema_quality_ok
        and not p_near_high_weak_reentry
        and not p_faded_spike_reentry
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
        and p_entry_quality_ok
        and rebound_from_low >= cfg.min_rebound_from_low_pct and p_score >= 65
    )

    # HJ 패턴은 진행 중인 현재 15분봉을 사용한다.
    # 강한 추세에서 긴 아래꼬리 음봉 뒤 양봉이 몸통을 회복하거나,
    # 연속 양봉 뒤 현재 장대양봉이 힘 있게 확장하는 경우를 별도로 잡는다.
    live = raw15.iloc[-1]
    quality_ok, quality_details = live_candle_quality_ok(client, symbol, raw15, cfg)
    if not quality_ok:
        quality_details = dict(quality_details)
        quality_details["rejected_conditions"] = ["live_candle_quality"]
        return None, 0.0, quality_details

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

    # HJ 롱 추세 필터:
    # 하락추세 속 단순 엔골핑은 제외하고 상승 흐름 안의 반등만 허용한다.
    hj_price_above_ema20 = bool(live_price > float(live.ema20))
    hj_ema20_above_ema60 = bool(float(live.ema20) > float(live.ema60))
    hj_ema20_rising_3 = bool(
        float(live.ema20) >= float(last.ema20) >= float(before.ema20)
    )
    live_rsi = float(live.rsi)
    hj_rsi_ok = bool(48.0 <= live_rsi <= cfg.hj_max_rsi)

    # 볼린저 상단 과돌파 추격 방지:
    # 1% 이상은 무조건 차단, 0.5~1%는 RSI/봉크기/거래량 과열이 동반될 때 차단한다.
    live_bb_upper = float(live.bb_upper) if pd.notna(live.bb_upper) else 0.0
    bb_upper_excess_pct = (live_price / live_bb_upper - 1) * 100 if live_bb_upper > 0 else 0.0
    live_candle_range_pct = (float(live.high) / float(live.low) - 1) * 100 if float(live.low) > 0 else 99.0
    bb_hard_chase = bool(bb_upper_excess_pct >= cfg.bb_chase_hard_pct)
    bb_soft_overheat = bool(
        bb_upper_excess_pct >= cfg.bb_chase_soft_pct
        and (
            live_rsi >= cfg.bb_chase_soft_rsi
            or live_gain >= cfg.bb_chase_soft_candle_gain_pct
            or live_candle_range_pct >= cfg.bb_chase_soft_candle_range_pct
            or live_volume_ratio >= cfg.bb_chase_soft_volume_ratio
        )
    )
    hj_bb_chase_ok = bool(not bb_hard_chase and not bb_soft_overheat)
    hj_volatility_ok = bool(one_hour_move < cfg.max_recent_1h_move_pct)

    # HJ는 진행 중인 15분봉 가격 기준으로 고점권/2차 급등을 판단한다.
    # P형용 확정봉 distance_to_high/rebound_from_low를 재사용하지 않는다.
    live_recent_high = float(m15.tail(16).high.max())
    live_recent_low = float(m15.tail(16).low.min())
    live_distance_to_high = (live_recent_high / live_price - 1) * 100 if live_price > 0 else 99.0
    live_rebound_from_low = (live_price / live_recent_low - 1) * 100 if live_recent_low > 0 else 0.0

    # 직전 고점 돌파 예외는 "한 번 찍은 스파이크"로 열지 않는다.
    # 첫 감지 후 다음 스캔(최소 30초 뒤)에서도 돌파 상태를 유지해야 확인된 돌파로 인정한다.
    # 이렇게 하면 ACE형 순간 고점 찌르기는 줄이고, SQD형 지속 돌파는 살린다.
    hj_breakout_raw = bool(
        live_price > live_recent_high * 1.002
        and float(live.high) > live_recent_high * 1.003
        and live_bullish
    )
    now_mono = time.monotonic()
    breakout_state = _HJ_BREAKOUT_CONFIRM.get(symbol)
    hj_fresh_breakout = False
    hj_breakout_confirm_age_sec = 0.0
    if hj_breakout_raw:
        if breakout_state is None:
            _HJ_BREAKOUT_CONFIRM[symbol] = {
                "first_seen": now_mono,
                "last_seen": now_mono,
                "count": 1.0,
            }
        else:
            first_seen = float(breakout_state.get("first_seen", now_mono))
            last_seen = float(breakout_state.get("last_seen", first_seen))
            if now_mono - last_seen > 150.0:
                first_seen = now_mono
                breakout_state["count"] = 1.0
            else:
                breakout_state["count"] = float(breakout_state.get("count", 1.0)) + 1.0
            breakout_state["first_seen"] = first_seen
            breakout_state["last_seen"] = now_mono
            _HJ_BREAKOUT_CONFIRM[symbol] = breakout_state
            hj_breakout_confirm_age_sec = now_mono - first_seen
            hj_fresh_breakout = bool(
                float(breakout_state.get("count", 0.0)) >= 2.0
                and hj_breakout_confirm_age_sec >= 30.0
            )
    else:
        _HJ_BREAKOUT_CONFIRM.pop(symbol, None)

    hj_near_recent_high = bool(live_distance_to_high <= cfg.hj_high_zone_max_distance_pct)
    hj_overheat_high_zone = bool(
        hj_near_recent_high
        and not hj_fresh_breakout
        and live_rsi >= cfg.hj_high_zone_overheat_rsi
        and bb_upper_excess_pct >= cfg.hj_high_zone_min_bb_excess_pct
    )
    hj_second_push_high_zone = bool(
        hj_near_recent_high
        and not hj_fresh_breakout
        and live_rebound_from_low >= cfg.hj_second_push_min_rebound_pct
        and live_gain >= cfg.hj_second_push_min_live_gain_pct
    )
    hj_high_zone_ok = bool(not hj_overheat_high_zone and not hj_second_push_high_zone)

    recent_bodies = raw15["close"].sub(raw15["open"]).abs().iloc[-12:-2]
    median_body = float(recent_bodies.median()) if len(recent_bodies) else 0.0
    last_large_bearish = bool(
        last_bearish
        and median_body > 0
        and last_body >= median_body * 1.8
    )

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
    hj_momentum_ok = bool(48 <= live_rsi <= cfg.hj_max_rsi)
    hj_pattern_ok = bool(wick_reversal or continuation_three_bulls)

    # COOKIE + CYS형 방어:
    # 급등 뒤 최근 고점 부근에서 재진입하는데 거래량 확인이 약한 경우 차단한다.
    # COOKIE는 wick_reversal, CYS는 continuation_three_bulls로 들어왔기 때문에
    # 두 HJ 패턴 모두 검사한다.
    # 단순 RSI 차단이 아니며, 30초 이상 확인된 강한 신고점 돌파(hj_fresh_breakout)는 예외로 살린다.
    hj_weak_volume_high_reentry = bool(
        (wick_reversal or continuation_three_bulls)
        and not hj_fresh_breakout
        and live_distance_to_high <= 2.00
        and live_rsi >= 62.0
        and live_volume_ratio < 0.90
    )

    # UB형 고점권 소진 방어:
    # 최근 고점 1% 이내인데 새 고점을 만들지 못하고(higher_highs=False),
    # RSI는 과열권이며 continuation 패턴으로 다시 잡히는 경우 차단.
    # 거래량이 강해도 신고점 확인이 없으면 진입하지 않는다.
    hj_high_zone_exhaustion = bool(
        continuation_three_bulls
        and not hj_fresh_breakout
        and live_distance_to_high <= 1.00
        and live_rsi >= 70.0
        and not higher_highs
    )
    hj_trend_filter_ok = bool(
        hj_price_above_ema20
        and hj_ema20_above_ema60
        and hj_ema20_rising_3
        and hj_rsi_ok
        and not last_large_bearish
    )
    hj_ok = bool(
        cfg.hj_pattern_enabled
        and hj_pattern_ok
        and hj_trend_score >= cfg.hj_min_trend_score
        and hj_volume_ok
        and hj_momentum_ok
        and hj_trend_filter_ok
        and hj_bb_chase_ok
        and hj_volatility_ok
        and hj_high_zone_ok
        and not hj_weak_volume_high_reentry
        and not hj_high_zone_exhaustion
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
            if not p_ema_quality_ok:
                rejected.append("p_ema_not_ordered")
            if p_near_high_weak_reentry:
                rejected.append("p_near_high_weak_reentry")
            if p_faded_spike_reentry:
                rejected.append("p_faded_spike_reentry")
            if p_score < 65:
                rejected.append("p_score")
        if not hj_ok:
            if not hj_pattern_ok:
                rejected.append("hj_pattern")
            if hj_trend_score < cfg.hj_min_trend_score:
                rejected.append("hj_trend")
            if not hj_volume_ok:
                rejected.append("hj_volume")
            if not hj_price_above_ema20:
                rejected.append("hj_price_below_ema20")
            if not hj_ema20_above_ema60:
                rejected.append("hj_ema20_below_ema60")
            if not hj_ema20_rising_3:
                rejected.append("hj_ema20_not_rising")
            if not hj_rsi_ok:
                rejected.append("hj_rsi_out_of_range")
            if not hj_bb_chase_ok:
                rejected.append("hj_bb_upper_chase")
            if not hj_volatility_ok:
                rejected.append("hj_extreme_1h_volatility")
            if not hj_high_zone_ok:
                rejected.append("hj_high_zone_reentry")
            if hj_weak_volume_high_reentry:
                rejected.append("hj_weak_volume_high_reentry")
            if hj_high_zone_exhaustion:
                rejected.append("hj_high_zone_exhaustion")
            if last_large_bearish:
                rejected.append("hj_after_large_bearish")

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
        "rebound_from_low_pct": round(float(live_rebound_from_low if strategy == "HJ" else rebound_from_low), 2),
        "entry_candle_gain_pct": round(float(live_gain if strategy == "HJ" else entry_candle_gain), 2),
        "distance_to_recent_high_pct": round(float(live_distance_to_high if strategy == "HJ" else distance_to_high), 2),
        "hj_breakout_raw": bool(hj_breakout_raw),
        "hj_fresh_breakout_confirmed": bool(hj_fresh_breakout),
        "hj_breakout_confirm_age_sec": round(float(hj_breakout_confirm_age_sec), 1),
        "one_hour_move_pct": round(one_hour_move, 2),
        "volume_declining_3": volume_declining_3,
        "confirmation_hold": confirmation_hold,
        "ema_ordered": ema_ordered,
        "ema9_rising": ema9_rising,
        "higher_lows": higher_lows,
        "higher_highs": higher_highs,
        "p_ema_quality_ok": p_ema_quality_ok,
        "p_near_high_weak_reentry": p_near_high_weak_reentry,
        "p_faded_spike_reentry": p_faded_spike_reentry,
        "p_entry_quality_ok": p_entry_quality_ok,
        "hj_wick_reversal": wick_reversal,
        "hj_continuation_three_bulls": continuation_three_bulls,
        "hj_trend_score": hj_trend_score,
        "hj_trend_filter_ok": hj_trend_filter_ok,
        "hj_price_above_ema20": hj_price_above_ema20,
        "hj_ema20_above_ema60": hj_ema20_above_ema60,
        "hj_ema20_rising_3": hj_ema20_rising_3,
        "hj_rsi_ok": hj_rsi_ok,
        "hj_max_rsi": cfg.hj_max_rsi,
        "bb_upper": round(live_bb_upper, 10) if live_bb_upper > 0 else None,
        "bb_upper_excess_pct": round(bb_upper_excess_pct, 2),
        "hj_bb_chase_ok": hj_bb_chase_ok,
        "hj_volatility_ok": hj_volatility_ok,
        "hj_near_recent_high": hj_near_recent_high,
        "hj_overheat_high_zone": hj_overheat_high_zone,
        "hj_second_push_high_zone": hj_second_push_high_zone,
        "hj_high_zone_ok": hj_high_zone_ok,
        "hj_weak_volume_high_reentry": hj_weak_volume_high_reentry,
        "hj_high_zone_exhaustion": hj_high_zone_exhaustion,
        "hj_fresh_breakout": hj_fresh_breakout,
        "hj_live_distance_to_high_pct": round(live_distance_to_high, 2),
        "hj_live_rebound_from_low_pct": round(live_rebound_from_low, 2),
        "live_candle_range_pct": round(live_candle_range_pct, 2),
        "hj_last_large_bearish": last_large_bearish,
        "hj_body_recovery_pct": round(body_recovery * 100, 2),
        "hj_lower_wick_body_ratio": round(lower_wick_ratio, 2),
        "rejected_conditions": list(dict.fromkeys(rejected)),
    }
    return strategy, float(score), details


# 위험그룹 목록이 정의되지 않아 SCAN_OK 직후 NameError가 발생하던 문제 수정
MEME_SYMBOLS: set[str] = set()



def live_candle_quality_ok(client, symbol, raw15, cfg):
    """15분봉 마감 전 진입은 유지하되, 잠깐 양봉인 약한 반등은 차단."""
    try:
        if raw15 is None or len(raw15) < 3:
            return False, {"reason": "15m_insufficient"}

        prev = raw15.iloc[-2]
        live = raw15.iloc[-1]

        o = float(live.open)
        c = float(live.close)
        h = float(live.high)

        bullish = c > o
        body = max(c - o, 0.0)
        body_pct = (body / o) * 100.0 if o > 0 else 0.0
        body_ok = body_pct >= float(cfg.live_reversal_min_body_pct)

        upper_wick = max(h - max(o, c), 0.0)
        wick_ratio = upper_wick / body if body > 0 else 999.0
        wick_ok = wick_ratio <= float(cfg.live_reversal_max_upper_wick_to_body)

        prev_o = float(prev.open)
        prev_c = float(prev.close)
        prev_bearish = prev_c < prev_o
        recovery_ratio = 1.0
        recovery_ok = True
        if prev_bearish:
            prev_body = prev_o - prev_c
            recovered = max(c - prev_c, 0.0)
            recovery_ratio = recovered / prev_body if prev_body > 0 else 1.0
            recovery_ok = recovery_ratio >= float(cfg.live_reversal_min_prev_body_recovery)

        five_ok = True
        if bool(cfg.live_reversal_require_5m_bullish):
            m5 = confirmed(indicators(client.candles(symbol, "5m", 20)))
            if len(m5) < 1:
                five_ok = False
            else:
                last5 = m5.iloc[-1]
                five_ok = float(last5.close) > float(last5.open)

        ok = bool(bullish and body_ok and wick_ok and recovery_ok and five_ok)
        return ok, {
            "bullish": bullish,
            "body_pct": round(body_pct, 4),
            "body_ok": body_ok,
            "upper_wick_to_body": round(wick_ratio, 4),
            "wick_ok": wick_ok,
            "prev_bearish": prev_bearish,
            "prev_body_recovery": round(recovery_ratio, 4),
            "recovery_ok": recovery_ok,
            "five_ok": five_ok,
        }
    except Exception as exc:
        return False, {"reason": "quality_check_error", "error": str(exc)}


def rebound_add_signal(client: BybitSwingClient, symbol: str, cfg: DailyConfig) -> tuple[bool, dict[str, Any]]:
    """15분 구조와 거래량이 함께 회복될 때만 반등 추가한다."""
    m5 = confirmed(indicators(client.candles(symbol, "5m", 140)))
    m15 = confirmed(indicators(client.candles(symbol, "15m", 100)))
    if len(m5) < 50 or len(m15) < 30:
        return False, {"reason": "반등 캔들 부족"}
    row5, prev5 = m5.iloc[-1], m5.iloc[-2]
    row15, prev15 = m15.iloc[-1], m15.iloc[-2]
    recent_low = float(m5.tail(12).low.min())
    rebound_pct = (float(row5.close) / recent_low - 1) * 100 if recent_low > 0 else 0.0
    vol_ratio = float(row15.volume / row15.vol_avg) if pd.notna(row15.vol_avg) and row15.vol_avg > 0 else 0.0
    prior_15m_high = float(m15.iloc[-4:-1].high.max())
    # 단일 양봉 하나를 반등으로 보지 않는다: 마감된 5분봉 2개가 연속 양봉이어야 한다.
    two_bullish_5m = bool(
        prev5.close > prev5.open
        and row5.close > row5.open
        and row5.close >= prev5.close
    )
    bullish = bool(two_bullish_5m and row15.close > row15.open)
    break_structure = bool(row15.close > prior_15m_high and row5.close > prev5.high)
    rsi_ok = bool(row15.rsi >= cfg.rebound_min_rsi and row15.rsi > prev15.rsi)
    ema_ok = bool(row15.close >= row15.ema9 and row15.ema9 >= row15.ema20)
    volume_ok = bool(vol_ratio >= cfg.rebound_min_volume_ratio)
    rebound_ok = bool(rebound_pct >= cfg.min_rebound_from_low_pct)
    ok = bool(bullish and break_structure and rsi_ok and ema_ok and volume_ok and rebound_ok)
    return ok, {
        "price": float(row5.close), "bullish": bullish, "two_bullish_5m": two_bullish_5m,
        "break_structure": break_structure, "rsi_ok": rsi_ok, "ema_ok": ema_ok, "volume_ok": volume_ok,
        "rebound_ok": rebound_ok, "rsi": round(float(row15.rsi), 2),
        "volume_ratio": round(vol_ratio, 2), "rebound_pct": round(rebound_pct, 2),
    }



def early_failure_signal(
    client: BybitSwingClient,
    symbol: str,
    opened_at: str,
    cfg: DailyConfig,
) -> tuple[bool, dict[str, Any]]:
    """진입 후 10~45분의 실패를 5분봉 구조로 감시한다.

    - 10~15분: 기존의 매우 엄격한 초기 실패 조건을 유지한다.
    - 15~45분: BEAT처럼 Early Failure 창을 지나 구조손절까지 늦어지는 공백을 메운다.
      고정 손실금액이 아니라 EMA20 회복 실패 + 연속 저점하락 + 실제 저점 이탈 + RSI 약화가
      함께 나타날 때만 종료한다.
    """
    try:
        opened = datetime.fromisoformat(opened_at)
        if opened.tzinfo is None:
            opened = opened.replace(tzinfo=timezone.utc)
    except Exception:
        return False, {"reason": "opened_at parse failed"}

    age_min = (datetime.now(timezone.utc) - opened).total_seconds() / 60.0
    if age_min < cfg.early_failure_min_age_minutes:
        return False, {"reason": "too_early", "age_min": round(age_min, 2)}
    if age_min > cfg.fast_failure_window_minutes:
        return False, {"reason": "window_passed", "age_min": round(age_min, 2)}

    m5 = confirmed(indicators(client.candles(symbol, "5m", 80)))
    if len(m5) < 6:
        return False, {"reason": "5m candles insufficient"}

    a, b, c = m5.iloc[-3], m5.iloc[-2], m5.iloc[-1]
    prev4 = m5.iloc[-4]

    lower_lows = bool(float(a.low) > float(b.low) > float(c.low))

    buffer = abs(float(cfg.early_failure_ema_reclaim_buffer_pct)) / 100
    fail_reclaim = bool(
        float(b.close) < float(b.ema20) * (1 + buffer)
        and float(c.close) < float(c.ema20) * (1 + buffer)
    )

    volume_fading = bool(float(c.volume) <= float(b.volume) <= float(a.volume))
    bearish_pressure = bool(
        float(b.close) < float(b.open) or float(c.close) < float(c.open)
    )
    # ARIA형: 실패가 커질 때 거래량이 줄지 않고 오히려 매도 거래량이 급증할 수 있다.
    # 거래량 감소 OR 강한 음봉 거래량 확대 중 하나를 실패 증거로 인정한다.
    c_vol_avg = float(c.vol_avg) if pd.notna(c.vol_avg) and float(c.vol_avg) > 0 else 0.0
    sell_volume_surge = bool(
        float(c.close) < float(c.open)
        and float(c.volume) >= float(b.volume) * 1.20
        and (c_vol_avg <= 0 or float(c.volume) >= c_vol_avg * 1.10)
    )
    early_volume_failure = bool(volume_fading or sell_volume_surge)

    # 10~15분: 정상 눌림 보호를 위해 구조 조건은 유지하되,
    # 거래량은 "감소"뿐 아니라 "매도 급증"도 실패로 인정한다.
    strict_early = bool(
        age_min <= cfg.early_failure_window_minutes
        and lower_lows and fail_reclaim and early_volume_failure and bearish_pressure
    )

    # 15~45분 빠른 실패: 거래량 감소를 필수로 두지 않는다.
    # 실제 급락은 매도 거래량이 커질 수 있기 때문이다.
    low_break_ratio = abs(float(cfg.fast_failure_low_break_pct)) / 100
    prior_low = min(float(prev4.low), float(a.low))
    meaningful_low_break = bool(float(c.close) < prior_low * (1 - low_break_ratio))
    rsi_weak = bool(
        float(c.rsi) <= float(cfg.fast_failure_rsi_max)
        and float(c.rsi) < float(b.rsi)
    )
    ema9_bearish = bool(float(c.ema9) < float(c.ema20) or float(c.ema9) < float(b.ema9))

    # 15~45분: CAP형처럼 핵심 구조가 이미 무너졌는데 보조조건 하나 때문에
    # 기존 구조손절까지 끌고 가지 않도록 한다.
    # 핵심 3조건(EMA20 회복 실패 + 약세 압력 + 의미 있는 저점 이탈)은 필수,
    # 보조 3조건(lower lows / RSI 약화 / EMA9 약화) 중 2개 이상이면 빠른 실패로 본다.
    fast_weakness_score = int(lower_lows) + int(rsi_weak) + int(ema9_bearish)
    fast_failure = bool(
        age_min > cfg.early_failure_window_minutes
        and fail_reclaim
        and bearish_pressure
        and meaningful_low_break
        and fast_weakness_score >= 2
    )

    # COOKIE형 25~45분 지연 손절 보완:
    # EMA20 아래에서 약세가 이어지고 RSI/EMA도 꺾였는데,
    # 기존 meaningful_low_break 한 조건 때문에 구조손절까지 끌지 않도록 보조 경로를 둔다.
    # 여전히 고정 손실금액 손절은 사용하지 않는다.
    late_accelerated_failure = bool(
        age_min >= 25.0
        and fail_reclaim
        and bearish_pressure
        and ema9_bearish
        and float(c.rsi) <= 48.0
        and (meaningful_low_break or lower_lows or sell_volume_surge)
    )

    ok = bool(strict_early or fast_failure or late_accelerated_failure)
    failure_type = (
        "EARLY_10_15" if strict_early
        else "FAST_15_45" if fast_failure
        else "FAST_LATE_25_45" if late_accelerated_failure
        else ""
    )

    return ok, {
        "age_min": round(age_min, 2),
        "failure_type": failure_type,
        "lower_lows": lower_lows,
        "fail_reclaim_ema20": fail_reclaim,
        "volume_fading": volume_fading,
        "sell_volume_surge": sell_volume_surge,
        "early_volume_failure": early_volume_failure,
        "bearish_pressure": bearish_pressure,
        "meaningful_low_break": meaningful_low_break,
        "rsi_weak": rsi_weak,
        "ema9_bearish": ema9_bearish,
        "fast_weakness_score": fast_weakness_score,
        "late_accelerated_failure": late_accelerated_failure,
        "last_close": float(c.close),
        "last_ema20": float(c.ema20),
        "last_rsi": round(float(c.rsi), 2),
        "prior_low": prior_low,
    }

def late_trend_failure_signal(
    client: BybitClient,
    symbol: str,
    entry_ts_ms: int,
    tp1_done: bool,
) -> tuple[bool, dict[str, Any]]:
    """45분 이후 TP1 미체결 포지션의 추세실패를 기존 구조손절보다 앞서 감지."""
    if tp1_done:
        return False, {"reason": "tp1_done"}

    now_ms = int(time.time() * 1000)
    age_min = max(0.0, (now_ms - int(entry_ts_ms)) / 60000.0)
    if age_min < 45.0:
        return False, {"reason": "too_early", "age_min": round(age_min, 1)}

    df15 = add_indicators(client.klines(symbol, "15", 80))
    if len(df15) < 8:
        return False, {"reason": "not_enough_15m", "age_min": round(age_min, 1)}

    closed = df15.iloc[:-1].copy()
    if len(closed) < 5:
        return False, {"reason": "not_enough_closed_15m", "age_min": round(age_min, 1)}

    c = closed.iloc[-1]
    b = closed.iloc[-2]
    recent = closed.iloc[-4:-1]

    close_below_ema20 = bool(float(c.close) < float(c.ema20) * 0.999)
    ema5_bearish = bool(float(c.ema5) < float(b.ema5))
    ema10_bearish = bool(float(c.ema10) < float(b.ema10))
    ema_fast_bearish = bool(ema5_bearish and ema10_bearish)
    rsi_not_recovered = bool(float(c.rsi) <= 48.0 and float(c.rsi) <= float(b.rsi))
    recent_low = float(recent.low.min()) if len(recent) else float(b.low)
    low_rebreak = bool(float(c.close) < recent_low * 0.997)
    bearish = bool(float(c.close) < float(c.open))

    weakness_score = int(ema_fast_bearish) + int(rsi_not_recovered) + int(low_rebreak) + int(bearish)
    ok = bool(close_below_ema20 and weakness_score >= 3)

    details = {
        "reason": "LATE_TREND_FAILURE_45M_PLUS" if ok else "late_trend_hold",
        "age_min": round(age_min, 1),
        "close_below_ema20": close_below_ema20,
        "ema5_bearish": ema5_bearish,
        "ema10_bearish": ema10_bearish,
        "ema_fast_bearish": ema_fast_bearish,
        "rsi_not_recovered": rsi_not_recovered,
        "low_rebreak": low_rebreak,
        "bearish": bearish,
        "weakness_score": weakness_score,
        "close": float(c.close),
        "ema20": float(c.ema20),
        "rsi": float(c.rsi),
    }
    return ok, details


def hj_structure_broken(
    client: BybitSwingClient,
    symbol: str,
    cfg: DailyConfig,
    base_price: float | None = None,
    live_price: float | None = None,
) -> tuple[bool, dict[str, Any]]:
    """HJ/P 공통 구조손절 재설계.

    목적:
    - 단순 눌림은 버틴다.
    - 차트가 실제 하락 추세로 전환되면 기존보다 빠르게 종료한다.
    - 고정 USDT 손절은 쓰지 않는다.
    - 최종 재난손절은 구조판단을 놓친 비정상 급락에서만 사용한다.

    구조붕괴 판단은 확정 15분봉 기준:
    1) 종가가 EMA20 아래
    2) EMA9 < EMA20 또는 EMA9 하락
    3) 직전 저점 의미 있게 이탈
    4) RSI 약세 및 하락
    5) 음봉 마감

    강한 붕괴는 1개 확정봉에서 즉시 종료한다.
    일반 붕괴는 위 조건 중 4개 이상이면 종료한다.
    """
    m15 = confirmed(indicators(client.candles(symbol, "15m", 120)))
    if len(m15) < 30:
        return False, {"reason": "구조 캔들 부족"}

    last = m15.iloc[-1]
    prev = m15.iloc[-2]

    close = float(last.close)
    open_ = float(last.open)
    ema9 = float(last.ema9)
    ema20 = float(last.ema20)
    prev_ema9 = float(prev.ema9)
    prev_low = float(prev.low)
    rsi = float(last.rsi)
    prev_rsi = float(prev.rsi)

    below_ema20 = bool(close < ema20)
    ema9_below_ema20 = bool(ema9 < ema20)
    ema9_falling = bool(ema9 < prev_ema9)
    bearish = bool(close < open_)
    rsi_weak = bool(rsi <= float(cfg.structure_rsi_weak) and rsi < prev_rsi)

    low_break_ratio = abs(float(cfg.structure_break_low_pct)) / 100
    lower_low_break = bool(close < prev_low * (1 - low_break_ratio))

    checks = {
        "below_ema20": below_ema20,
        "ema9_below_or_falling": bool(ema9_below_ema20 or ema9_falling),
        "lower_low_break": lower_low_break,
        "rsi_weak": rsi_weak,
        "bearish": bearish,
    }
    score = sum(1 for v in checks.values() if v)

    # 강한 붕괴: EMA20 아래 + 직전 저점 이탈 + RSI 40 이하 + 음봉
    crash_break = bool(
        below_ema20
        and lower_low_break
        and bearish
        and rsi <= float(cfg.structure_rsi_crash)
    )

    # 일반 구조붕괴: 핵심 구조가 무너지면서 5개 중 4개 이상
    normal_break = bool(
        below_ema20
        and lower_low_break
        and score >= 4
    )

    # 최종 재난손절: 구조신호가 늦더라도 최초 진입가 대비 -8% 이상이면 종료
    emergency_break = False
    emergency_pnl_pct = None
    if base_price and float(base_price) > 0:
        mark = float(live_price if live_price is not None else close)
        emergency_pnl_pct = (mark / float(base_price) - 1) * 100
        emergency_break = bool(
            emergency_pnl_pct <= -abs(float(cfg.structure_emergency_stop_pct))
        )

    broken = bool(crash_break or normal_break or emergency_break)
    stop_type = (
        "EMERGENCY" if emergency_break
        else "CRASH" if crash_break
        else "TREND_BREAK" if normal_break
        else ""
    )

    return broken, {
        "price": float(live_price if emergency_break and live_price is not None else close),
        "stop_type": stop_type,
        "structure_score": score,
        "below_ema20": below_ema20,
        "ema9_below_ema20": ema9_below_ema20,
        "ema9_falling": ema9_falling,
        "lower_low_break": lower_low_break,
        "rsi_weak": rsi_weak,
        "bearish": bearish,
        "rsi": round(rsi, 2),
        "ema9": ema9,
        "ema20": ema20,
        "previous_low": prev_low,
        "emergency_pnl_pct": round(emergency_pnl_pct, 2) if emergency_pnl_pct is not None else None,
    }



def flat_exit_signal(client: BybitSwingClient, symbol: str, base_price: float, cfg: DailyConfig) -> tuple[bool, dict[str, Any]]:
    """정체 종료는 '손실 확대 중'이 아니라 실제 횡보일 때만 허용한다."""
    m15 = confirmed(indicators(client.candles(symbol, "15m", 100)))
    if len(m15) < 25:
        return False, {"reason": "정체 캔들 부족"}

    last = m15.iloc[-1]
    recent4 = m15.tail(4)

    close = float(last.close)
    ema20 = float(last.ema20)
    rsi = float(last.rsi)

    current_pnl_pct = (close / float(base_price) - 1) * 100 if float(base_price) > 0 else -99.0
    recent_low = float(recent4.low.min())
    recent_high = float(recent4.high.max())
    recent_range_pct = (recent_high / recent_low - 1) * 100 if recent_low > 0 else 99.0
    ema20_distance_pct = abs(close / ema20 - 1) * 100 if ema20 > 0 else 99.0

    loss_ok = bool(current_pnl_pct >= -abs(cfg.flat_max_loss_pct))
    range_ok = bool(recent_range_pct <= cfg.flat_max_recent_range_pct)
    ema_near = bool(ema20_distance_pct <= cfg.flat_max_ema20_distance_pct)
    rsi_mid = bool(cfg.flat_rsi_min <= rsi <= cfg.flat_rsi_max)

    ok = bool(loss_ok and range_ok and ema_near and rsi_mid)
    return ok, {
        "price": close,
        "current_pnl_pct": round(current_pnl_pct, 2),
        "recent_range_pct": round(recent_range_pct, 2),
        "ema20_distance_pct": round(ema20_distance_pct, 2),
        "rsi": round(rsi, 2),
        "loss_ok": loss_ok,
        "range_ok": range_ok,
        "ema_near": ema_near,
        "rsi_mid": rsi_mid,
    }


def qty_from_margin(price: float, margin_usdt: float, leverage: float) -> float:
    """증거금과 레버리지로 주문 수량을 계산한다."""
    price = float(price)
    if price <= 0:
        raise ValueError(f"invalid price: {price}")
    return (float(margin_usdt) * float(leverage)) / price


def same_risk_group(symbol: str, open_symbols: set[str]) -> bool:
    return symbol in MEME_SYMBOLS and any(s in MEME_SYMBOLS for s in open_symbols)


class DailyBot:
    def __init__(self, config: DailyConfig | None = None):
        self.cfg = config or DailyConfig.load()
        self.client = BybitSwingClient(demo=self.cfg.mode != "live")
        init_db()
        ensure_scan_rejected_csv()
        state_set("runtime_version", BOT_RUNTIME_VERSION)
        state_set("runtime_started_at", datetime.now(timezone.utc).isoformat())
        state_set("runtime_bot_file", str(Path(__file__).resolve()))

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
        entry_margin = self.cfg.hj_position_margin_usdt if strategy == "HJ" else self.cfg.position_margin_usdt
        qty = qty_from_margin(price, entry_margin, self.cfg.leverage)
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
                (symbol, "OPEN", utc_now(), utc_now(), price, qty, entry_margin, 0, 0,
                 price, 0.0, f"{strategy}형 데일리 진입", strategy, 0.0, trading_day(),
                 price, qty, 0.0, 0.0, price, price, price, trade_id, 0),
            )
        signal_details = signal_details or {}
        details = json.dumps({
            "entry_price": price, "margin_usdt": entry_margin,
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
        old_avg = float(row["avg_price"])
        # HJ/P 공통 안전장치: 반등이 확인되어도 현재 평단 이상에서는 절대 추가하지 않는다.
        if float(price) >= old_avg:
            return
        add_margin = self.cfg.hj_rebound_add_margin_usdt if str(row["strategy"] or "") == "HJ" else self.cfg.rebound_add_margin_usdt
        add_qty = qty_from_margin(price, add_margin, self.cfg.leverage)
        if add_qty <= 0:
            return
        self._execute(row["symbol"], "buy", add_qty)
        old_qty = float(row["total_qty"])
        new_qty = old_qty + add_qty
        new_avg = (old_avg * old_qty + price * add_qty) / new_qty
        now = datetime.now(timezone.utc)
        add_bucket = str(int(now.timestamp()) // 900)
        with db() as conn:
            conn.execute(
                """UPDATE bot_positions SET avg_price=?,total_qty=?,total_margin=?,dca_count=?,
                   add_qty=?,add_price=?,updated_at=?,last_price=?,note=?,last_add_15m_bucket=? WHERE symbol=?""",
                (new_avg, new_qty, float(row["total_margin"]) + add_margin,
                 int(row["dca_count"] or 0) + 1, add_qty, price, utc_now(), price,
                 "반등 확인 후 순환 추가진입", add_bucket, row["symbol"]),
            )
        details = json.dumps({"add_price": price, "add_margin_usdt": add_margin,
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
                (base_price, remaining, max(0.0, float(row["total_margin"]) - (self.cfg.hj_rebound_add_margin_usdt if str(row["strategy"] or "") == "HJ" else self.cfg.rebound_add_margin_usdt)),
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
        if stop_event not in {"STOP_HALF", "FINAL_STOP", "STOP", "HJ_STRUCTURE_STOP", "BE_EXIT", "FLAT_EXIT_75M", "TIME_EXIT", "MANUAL_EXIT"}:
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
        manual_request = state_get("manual_exit_request", "")
        try:
            manual_payload = json.loads(manual_request) if manual_request else {}
        except Exception:
            manual_payload = {}
        manual_symbol = str(manual_payload.get("symbol") or "")

        for row in self.open_rows():
            price = float(self.client.ticker(row["symbol"]).get("last") or 0)
            if price <= 0:
                continue
            if manual_symbol and str(row["symbol"]) == manual_symbol:
                self._close(row, price, 1.0, "MANUAL_EXIT", price, None)
                state_set("manual_exit_request", "")
                log_event(row["symbol"], "MANUAL_EXIT_ACK", price, mode=self.cfg.mode,
                          details=json.dumps(manual_payload, ensure_ascii=False),
                          strategy=row["strategy"] or "", trade_id=row["trade_id"] or "")
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

            # 물타기 시간/깊이 안전장치
            # 1) 진입한 15분봉에서는 물타기 금지
            # 2) 같은 15분봉에서는 최대 1회만 허용
            # 3) 현재 가격이 최초 평단 대비 -1.5%보다 얕으면 무조건 금지
            #    (-1.5% 도달 자체가 물타기 트리거는 아니며, 이후 반등 조건을 별도로 모두 통과해야 한다.)
            # 4) 마감된 5분봉 2개 연속 양봉 + 기존 15분 구조/RSI/EMA/거래량 반등 조건 유지
            if (self.cfg.rebound_add_enabled and float(row["add_qty"] or 0) <= 0
                    and int(row["dca_count"] or 0) < self.cfg.max_cycle_adds
                    and int(row["tp1_done"] or 0) == 0
                    and int(row["stop_stage1_done"] or 0) == 0):
                try:
                    now = datetime.now(timezone.utc)
                    current_15m_bucket = str(int(now.timestamp()) // 900)
                    opened_at = datetime.fromisoformat(row["opened_at"])
                    if opened_at.tzinfo is None:
                        opened_at = opened_at.replace(tzinfo=timezone.utc)
                    entry_15m_bucket = str(int(opened_at.timestamp()) // 900)
                    last_add_15m_bucket = str(row["last_add_15m_bucket"] or "")

                    # 진입봉 및 같은 15분봉 재물타기 차단
                    timing_ok = (
                        current_15m_bucket != entry_15m_bucket
                        and current_15m_bucket != last_add_15m_bucket
                    )

                    # 현재 가격 기준 최소 눌림폭. 이 수치 도달은 'arm'이 아니라 단순 금지선이다.
                    current_drawdown_pct = (price / base_price - 1) * 100
                    deep_enough_now = current_drawdown_pct <= -abs(self.cfg.rebound_min_drawdown_pct)

                    if timing_ok and deep_enough_now:
                        ok, details = rebound_add_signal(self.client, row["symbol"], self.cfg)
                        if ok:
                            add_price = float(details.get("price") or price)
                            add_drawdown_pct = (add_price / base_price - 1) * 100
                            if (add_price < avg
                                    and add_drawdown_pct <= -abs(self.cfg.rebound_min_drawdown_pct)):
                                self._rebound_add(row, add_price)
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
            strategy = str(row["strategy"] or "P")
            total_qty = float(row["total_qty"] or 0)
            unrealized_usdt = (price - avg) * total_qty

            # TP 터치 보강:
            # 현재가만 보지 않고, 이미 저장된 최고가 + (진입 2분 경과 후) 최근 1분봉 high도 확인한다.
            # TP를 한 번이라도 터치했다면 BE/구조손절/시간종료보다 TP를 먼저 처리한다.
            # PAPER는 설정된 TP 가격으로 체결 처리하고,
            # DEMO/LIVE는 터치가 뒤늦게 감지된 경우 현재 시장가로 즉시 reduce-only 청산한다.
            tp_observed_high = max(price, highest)
            try:
                if age_h * 3600 >= 120:
                    m1 = self.client.candles(row["symbol"], "1m", 3)
                    if m1 is not None and len(m1) > 0 and "high" in m1.columns:
                        tp_observed_high = max(
                            tp_observed_high,
                            float(pd.to_numeric(m1["high"], errors="coerce").dropna().tail(2).max())
                        )
            except Exception as exc:
                log_event(
                    row["symbol"], "TP_TOUCH_CHECK_ERROR", mode=self.cfg.mode,
                    details=str(exc), strategy=strategy, trade_id=row["trade_id"] or ""
                )

            tp1_trigger = base_price * (1 + self.cfg.tp1_pct / 100)
            tp2_trigger = base_price * (1 + self.cfg.tp2_pct / 100)

            if int(row["tp1_done"]) == 0 and tp_observed_high >= tp1_trigger:
                fill = paper_fill(tp1_trigger) if self.cfg.mode == "paper" else price
                self._close(row, fill, 0.5, "TP1", price, tp1_trigger)
                continue

            if int(row["tp1_done"]) == 1 and tp_observed_high >= tp2_trigger:
                fill = paper_fill(tp2_trigger) if self.cfg.mode == "paper" else price
                self._close(row, fill, 1.0, "TP2", price, tp2_trigger)
                continue

            # TP가 터치되지 않았을 때만 BE/구조손절/시간종료를 확인한다.

            # 진입 후 10~45분 실패판정:
            # 10~15분은 기존 엄격조건, 15~45분은 5분 구조붕괴가 명확할 때만 빠르게 종료한다.
            # 고정 USDT 손절은 사용하지 않는다.
            if self.cfg.early_failure_enabled and int(row["tp1_done"] or 0) == 0:
                try:
                    early_fail, early_details = early_failure_signal(
                        self.client, row["symbol"], row["opened_at"], self.cfg
                    )
                except Exception as exc:
                    early_fail, early_details = False, {"error": str(exc)}
                    log_event(
                        row["symbol"], "EARLY_FAILURE_CHECK_ERROR",
                        mode=self.cfg.mode, details=str(exc),
                        strategy=strategy, trade_id=row["trade_id"] or ""
                    )

                if early_fail:
                    failure_type = str(early_details.get("failure_type") or "UNKNOWN")
                    audit_event = {
                        "EARLY_10_15": "EARLY_FAILURE_10_15_TRIGGER",
                        "FAST_15_45": "FAST_FAILURE_15_45_TRIGGER",
                        "FAST_LATE_25_45": "FAST_FAILURE_LATE_25_45_TRIGGER",
                    }.get(failure_type, "EARLY_FAILURE_TRIGGER")
                    log_event(
                        row["symbol"], audit_event,
                        price=price, mode=self.cfg.mode,
                        details=json.dumps(early_details, ensure_ascii=False),
                        strategy=strategy, trade_id=row["trade_id"] or ""
                    )
                    # UI/기존 통계 호환을 위해 최종 종료 이벤트명은 유지한다.
                    reason = "HJ_STRUCTURE_STOP" if strategy == "HJ" else "STOP"
                    self._close(row, price, 1.0, reason, price, price)
                    continue

                # 45분 이후 전용 추세실패:
                # UB/CYS형처럼 초반 45분을 버틴 뒤 TP1 없이 추세가 무너지는 거래를
                # 기존 구조손절보다 먼저 종료한다.
                late_fail, late_details = late_trend_failure_signal(
                    self.client,
                    row["symbol"],
                    int(row["entry_ts_ms"] or 0),
                    bool(row["tp1_done"]),
                )
                if late_fail:
                    log_event(
                        row["symbol"], "LATE_TREND_FAILURE_45M_TRIGGER",
                        price=price, mode=self.cfg.mode,
                        details=json.dumps(late_details, ensure_ascii=False),
                        strategy=strategy, trade_id=row["trade_id"] or ""
                    )
                    reason = "HJ_STRUCTURE_STOP" if strategy == "HJ" else "STOP"
                    self._close(row, price, 1.0, reason, price, price)
                    continue

            if int(row["tp1_done"]) == 1 and base_pnl_pct <= -self.cfg.breakeven_stop_pct:
                trigger = base_price * (1 - self.cfg.breakeven_stop_pct / 100)
                self._close(row, paper_fill(trigger), 1.0, "BE_EXIT", price, trigger)
                continue

            # HJ/P 공통 구조손절:
            # 확정 15분봉의 EMA20/EMA9/저점/RSI/음봉 구조가 실제로 무너질 때 전량 종료한다.
            if self.cfg.hj_structure_stop_enabled:
                try:
                    broken, structure = hj_structure_broken(
                        self.client, row["symbol"], self.cfg,
                        base_price=base_price, live_price=price
                    )
                except Exception as exc:
                    broken, structure = False, {"error": str(exc)}
                    log_event(row["symbol"], "HJ_STRUCTURE_CHECK_ERROR", mode=self.cfg.mode, details=str(exc),
                              strategy=strategy, trade_id=row["trade_id"] or "")
                if broken:
                    trigger = float(structure.get("price") or price)
                    stop_type = str(structure.get("stop_type") or "TREND_BREAK")
                    reason = "HJ_STRUCTURE_STOP" if strategy == "HJ" else "STOP"

                    log_event(
                        row["symbol"], f"STRUCTURE_{stop_type}_TRIGGER",
                        price=price, mode=self.cfg.mode,
                        details=json.dumps(structure, ensure_ascii=False),
                        strategy=strategy, trade_id=row["trade_id"] or ""
                    )

                    self._close(row, price, 1.0, reason, price, trigger)
                    continue

            # HJ/P 공통 종료:
            # FLAT_EXIT는 실제 횡보일 때만 허용한다.
            # C98처럼 이미 크게 하락한 포지션을 '정체 종료'로 처리하지 않는다.
            flat_due = (
                age_h * 60 >= self.cfg.flat_exit_minutes
                and (highest / base_price - 1) * 100 < self.cfg.flat_min_favorable_pct
            )
            if flat_due:
                try:
                    flat_ok, flat_details = flat_exit_signal(
                        self.client, row["symbol"], base_price, self.cfg
                    )
                except Exception as exc:
                    flat_ok, flat_details = False, {"error": str(exc)}
                    log_event(
                        row["symbol"], "FLAT_EXIT_CHECK_ERROR", mode=self.cfg.mode,
                        details=str(exc), strategy=strategy, trade_id=row["trade_id"] or ""
                    )

                if flat_ok:
                    self._close(row, price, 1.0, "FLAT_EXIT_75M", price, None)
                    continue
                else:
                    # 정체 조건이 아니면 종료하지 않고 구조손절/시간종료 관리로 계속 넘긴다.
                    # FLAT_EXIT_BLOCKED는 관리 루프마다 DB에 쌓지 않고 trade_id별 15분에 1번만 기록한다.
                    # 진단 이벤트가 실제 매매기록을 밀어내는 문제를 방지한다.
                    now_dt = datetime.now(timezone.utc)
                    flat_bucket = f"{now_dt:%Y%m%d%H}{now_dt.minute // 15}"
                    flat_state_key = f"flat_exit_blocked_bucket:{row['trade_id'] or row['symbol']}"
                    if state_get(flat_state_key, "") != flat_bucket:
                        log_event(
                            row["symbol"], "FLAT_EXIT_BLOCKED", price=price, mode=self.cfg.mode,
                            details=json.dumps(flat_details, ensure_ascii=False),
                            strategy=strategy, trade_id=row["trade_id"] or ""
                        )
                        state_set(flat_state_key, flat_bucket)

            if age_h >= self.cfg.max_hold_hours:
                self._close(row, price, 1.0, "TIME_EXIT", price, None)

    def scan_entries(self) -> None:
        """긴급복구: SCAN_OK가 나오면 같은 스캔 안에서 바로 진입한다.

        기존처럼 모든 후보를 모은 뒤 별도 진입 루프로 넘기지 않아
        SCAN_OK 이후 발생하던 공통 오류를 우회한다.
        """
        if state_flag("pause_new_entries", False):
            append_entry_record("", "ENTRY_BLOCKED", "", 0, 0, "pause_new_entries=1")
            return
        if self.loss_cooldown_active():
            losses = self.consecutive_losses()
            state_set("loss_cooldown_active", "1")
            append_entry_record("", "ENTRY_BLOCKED", "", 0, 0, f"loss_cooldown=1; consecutive_losses={losses}")
            return
        state_set("loss_cooldown_active", "0")

        open_rows = self.open_rows()
        open_symbols = {str(r["symbol"]) for r in open_rows}
        slots = max(0, int(self.cfg.max_positions) - len(open_symbols))
        if slots <= 0:
            append_entry_record("", "ENTRY_BLOCKED", "", 0, 0, "slots=0")
            return

        for symbol in self.active_symbols():
            if slots <= 0:
                break
            if symbol in open_symbols:
                continue
            if self.symbol_in_cooldown(symbol):
                continue

            try:
                strategy, score, details = candidate_signal(
                    self.client, symbol, self.cfg
                )
                price = float(details.get("price", 0) or 0)
                log_event(
                    symbol,
                    "SCAN_OK" if strategy else "SCAN_WAIT",
                    price,
                    mode=self.cfg.mode,
                    details=json.dumps(details, ensure_ascii=False),
                    strategy=strategy or "",
                )
                append_scan_record(symbol, strategy, score, details)

                if not strategy:
                    continue
                if same_risk_group(symbol, open_symbols):
                    continue

                append_entry_record(
                    symbol, "ENTRY_ATTEMPT", strategy, score, price
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
                        mode=self.cfg.mode,
                        details=message,
                        strategy=strategy,
                    )
                    continue

                append_entry_record(
                    symbol, "ENTRY_SUCCESS", strategy, score, price
                )
                open_symbols.add(symbol)
                slots -= 1

            except Exception as exc:
                message = f"{type(exc).__name__}: {exc}"
                log_event(
                    symbol, "SCAN_ERROR", mode=self.cfg.mode, details=message
                )
                append_entry_record(
                    symbol, "SCAN_ERROR", "", 0, 0, message
                )

    def run_once(self) -> None:
        self.manage()
        self.update_stop_reviews()
        self.scan_entries()

    def run_forever(self) -> None:
        state_set("bot_process_status", "RUNNING")
        state_set("runtime_version", BOT_RUNTIME_VERSION)
        state_set("runtime_started_at", datetime.now(timezone.utc).isoformat())
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
