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
    # API 선별 실패 때 사용할 안전한 기본 감시 목록
    symbols: tuple[str, ...] = (
        "SOL-USDT-SWAP", "XRP-USDT-SWAP", "DOGE-USDT-SWAP", "SUI-USDT-SWAP",
        "AVAX-USDT-SWAP", "LINK-USDT-SWAP", "PEPE-USDT-SWAP", "WIF-USDT-SWAP",
        "BONK-USDT-SWAP", "APT-USDT-SWAP", "NEAR-USDT-SWAP", "ARB-USDT-SWAP",
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
    min_avg_hourly_range_pct: float = 0.35
    max_recent_1h_move_pct: float = 10.0
    # 장기 스윙에 더 어울리는 느린 종목은 데일리 후보에서 제외한다.
    slow_symbol_exclusions: tuple[str, ...] = (
        "XRP-USDT-SWAP", "AVAX-USDT-SWAP", "LINK-USDT-SWAP", "LTC-USDT-SWAP",
        "BCH-USDT-SWAP", "DOT-USDT-SWAP", "ETC-USDT-SWAP", "ATOM-USDT-SWAP",
        "TRX-USDT-SWAP", "TON-USDT-SWAP", "FIL-USDT-SWAP", "AAVE-USDT-SWAP",
    )
    non_crypto_base_exclusions: tuple[str, ...] = (
        "AAPL", "ABBV", "ABT", "AMAT", "AMD", "AMZN", "ASML", "AVGO",
        "BA", "BABA", "BAC", "BRK", "CAT", "COIN", "COST", "CRM", "CVX",
        "DIS", "GOOG", "GOOGL", "GS", "HD", "IBM", "INTC", "JNJ", "JPM",
        "KO", "LLY", "MA", "META", "MMM", "MRK", "MSFT", "MSTR", "MU",
        "NFLX", "NKE", "NVDA", "ORCL", "PEP", "PFE", "PLTR", "PYPL",
        "QCOM", "SBUX", "SKHYNIX", "SNDK", "SOXL", "SPY", "TSLA", "TSM",
        "UNH", "V", "WMT", "XAU", "XAG",
    )
    candidate_pool: tuple[str, ...] = (
        "SOL-USDT-SWAP", "XRP-USDT-SWAP", "DOGE-USDT-SWAP", "SUI-USDT-SWAP",
        "AVAX-USDT-SWAP", "LINK-USDT-SWAP", "PEPE-USDT-SWAP", "WIF-USDT-SWAP",
        "BONK-USDT-SWAP", "APT-USDT-SWAP", "NEAR-USDT-SWAP", "ARB-USDT-SWAP",
        "OP-USDT-SWAP", "SEI-USDT-SWAP", "INJ-USDT-SWAP", "TIA-USDT-SWAP",
        "FIL-USDT-SWAP", "LTC-USDT-SWAP", "BCH-USDT-SWAP", "DOT-USDT-SWAP",
        "UNI-USDT-SWAP", "AAVE-USDT-SWAP", "ETC-USDT-SWAP", "ATOM-USDT-SWAP",
        "TRX-USDT-SWAP", "TON-USDT-SWAP", "SHIB-USDT-SWAP", "ORDI-USDT-SWAP",
        "JUP-USDT-SWAP", "PYTH-USDT-SWAP", "ENA-USDT-SWAP", "ONDO-USDT-SWAP",
        "RENDER-USDT-SWAP", "FET-USDT-SWAP", "WLD-USDT-SWAP", "GALA-USDT-SWAP",
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
    max_hold_hours: int = 3
    daily_loss_limit_usdt: float = 12.0
    max_consecutive_losses: int = 3
    same_symbol_cooldown_minutes: int = 30
    min_balance_to_trade: float = 90.0
    emergency_stop_balance: float = 85.0
    scan_seconds: int = 60
    rebound_add_enabled: bool = True
    rebound_arm_drawdown_pct: float = 0.6
    rebound_add_margin_usdt: float = 27.0
    rebound_exit_buffer_pct: float = 0.10
    max_cycle_adds: int = 2
    flat_exit_minutes: int = 60
    flat_min_favorable_pct: float = 0.40
    min_pullback_from_high_pct: float = 0.30
    max_pullback_from_high_pct: float = 6.00
    max_entry_candle_gain_pct: float = 1.40
    max_near_high_pct: float = 0.15
    min_rebound_from_low_pct: float = 0.25
    rebound_min_volume_ratio: float = 0.9
    rebound_min_rsi: float = 44.0

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
        _ensure_column(conn, "bot_positions", "base_entry_price", "REAL")
        _ensure_column(conn, "bot_positions", "base_qty", "REAL")
        _ensure_column(conn, "bot_positions", "add_qty", "REAL DEFAULT 0")
        _ensure_column(conn, "bot_positions", "add_price", "REAL DEFAULT 0")
        _ensure_column(conn, "bot_positions", "lowest_price", "REAL")
        _ensure_column(conn, "bot_positions", "highest_price", "REAL")
        _ensure_column(conn, "bot_positions", "cycle_anchor_price", "REAL")
        _ensure_column(conn, "bot_positions", "trade_id", "TEXT")
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


def log_event(symbol: str, event: str, price: float = 0, qty: float = 0, mode: str = "",
              details: str = "", strategy: str = "", realized_pnl: float = 0.0,
              trade_id: str = "") -> None:
    with db() as conn:
        conn.execute(
            "INSERT INTO bot_events(ts,symbol,event,price,qty,mode,details,strategy,realized_pnl,trade_id) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (utc_now(), symbol, event, float(price), float(qty), mode, details, strategy, float(realized_pnl), trade_id),
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


def candidate_signal(client: OKXClient, symbol: str, cfg: DailyConfig) -> tuple[str | None, float, dict[str, Any]]:
    """24시간 상승 종목이 충분히 눌린 뒤 재반등할 때만 진입한다."""
    m15 = confirmed(indicators(client.candles(symbol, "15m", 220)))
    h1 = confirmed(indicators(client.candles(symbol, "1H", 140)))
    if len(m15) < 70 or len(h1) < 70:
        return None, 0.0, {"reason": "캔들 부족"}

    row, prev = m15.iloc[-1], m15.iloc[-2]
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
    momentum_ok = bool(row.rsi >= 40 and row.rsi <= 75 and row.rsi >= prev.rsi)
    volume_ok = bool(volume_ratio >= 0.75)
    not_extreme = bool(one_hour_move <= cfg.max_recent_1h_move_pct and candle_range <= 4.0)

    score = (
        (25 if h1_up else 0)
        + (25 if pullback_ok else 0)
        + (25 if rebound else 0)
        + (10 if momentum_ok else 0)
        + min(10, volume_ratio * 7)
        + min(5, rebound_from_low * 4)
    )
    ok = bool(h1_up and pullback_ok and not_chasing and rebound and momentum_ok and volume_ok and not_extreme and rebound_from_low >= cfg.min_rebound_from_low_pct and score >= 65)
    strategy = "P" if ok else None
    details = {
        "price": price, "strategy": strategy, "score": round(float(score), 2),
        "h1_up": h1_up, "pullback_ok": pullback_ok, "rebound": rebound,
        "not_chasing": not_chasing, "volume_ok": volume_ok,
        "rsi": round(float(row.rsi), 2), "volume_ratio": round(volume_ratio, 2),
        "pullback_from_high_pct": round(pullback_from_high, 2),
        "rebound_from_low_pct": round(rebound_from_low, 2),
        "entry_candle_gain_pct": round(entry_candle_gain, 2),
        "distance_to_recent_high_pct": round(distance_to_high, 2),
        "one_hour_move_pct": round(one_hour_move, 2),
    }
    return strategy, float(score), details


def rebound_add_signal(client: OKXClient, symbol: str, cfg: DailyConfig) -> tuple[bool, dict[str, Any]]:
    """횡보성 작은 반등은 제외하고, 15분 구조와 거래량이 함께 회복될 때만 순환 추가한다."""
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

    bullish = bool(row5.close > row5.open and row15.close > row15.open)
    break_structure = bool(row15.close > prior_15m_high and row5.close > prev5.high)
    rsi_ok = bool(row15.rsi >= cfg.rebound_min_rsi and row15.rsi > prev15.rsi)
    ema_ok = bool(row15.close >= row15.ema9 and row15.ema9 >= row15.ema20)
    volume_ok = bool(vol_ratio >= cfg.rebound_min_volume_ratio)
    rebound_ok = bool(rebound_pct >= cfg.min_rebound_from_low_pct)
    ok = bool(bullish and break_structure and rsi_ok and ema_ok and volume_ok and rebound_ok)
    return ok, {
        "price": float(row5.close), "bullish": bullish, "break_structure": break_structure,
        "rsi_ok": rsi_ok, "ema_ok": ema_ok, "volume_ok": volume_ok,
        "rebound_ok": rebound_ok, "rsi": round(float(row15.rsi), 2),
        "volume_ratio": round(vol_ratio, 2), "rebound_pct": round(rebound_pct, 2),
    }


def qty_from_margin(price: float, margin_usdt: float, leverage: int) -> float:
    return max(0.0, margin_usdt * leverage / price)


MEME_SYMBOLS = {"PEPE-USDT-SWAP", "WIF-USDT-SWAP", "BONK-USDT-SWAP", "SHIB-USDT-SWAP", "DOGE-USDT-SWAP"}

def same_risk_group(symbol: str, open_symbols: set[str]) -> bool:
    return symbol in MEME_SYMBOLS and any(s in MEME_SYMBOLS for s in open_symbols)


class DailyBot:
    def __init__(self, config: DailyConfig | None = None):
        self.cfg = config or DailyConfig.load()
        self.client = OKXClient(demo=self.cfg.mode != "live")
        init_db()

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
            symbol = str(ticker.get("instId") or "")
            base = symbol.split("-", 1)[0].upper() if symbol else ""
            if (
                symbol in excluded
                or base in set(self.cfg.non_crypto_base_exclusions)
                or not symbol.endswith("-USDT-SWAP")
            ):
                continue
            try:
                last = float(ticker.get("last") or 0)
                open24 = float(ticker.get("open24h") or 0)
                high = float(ticker.get("high24h") or 0)
                low = float(ticker.get("low24h") or 0)
                bid = float(ticker.get("bidPx") or 0)
                ask = float(ticker.get("askPx") or 0)
                base_vol = float(ticker.get("volCcy24h") or 0)
                if min(last, open24, high, low) <= 0:
                    continue
                quote_vol = base_vol * last
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
        """오늘 종료된 거래를 최신순으로 보고 연속 손실 횟수를 센다."""
        day = trading_day()
        with db() as conn:
            rows = conn.execute(
                """SELECT realized_pnl FROM bot_positions
                   WHERE status='CLOSED' AND substr(updated_at,1,10)=?
                   ORDER BY updated_at DESC""",
                (day,),
            ).fetchall()
        count = 0
        for row in rows:
            if float(row["realized_pnl"] or 0) < 0:
                count += 1
            else:
                break
        return count

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
            raise OKXError("LIVE 모드인데 API 설정이 없습니다.")
        self.client.set_leverage(symbol, self.cfg.leverage, self.cfg.margin_mode, "long")
        self.client.place_market_order(
            symbol, side, f"{qty:.8f}", self.cfg.margin_mode, "long", reduce_only,
            client_order_id=f"HJ{int(time.time())}{symbol[:4]}"
        )

    def _open(self, symbol: str, price: float, strategy: str, score: float) -> None:
        qty = qty_from_margin(price, self.cfg.position_margin_usdt, self.cfg.leverage)
        self._execute(symbol, "buy", qty)
        trade_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
        with db() as conn:
            conn.execute("DELETE FROM bot_positions WHERE symbol=?", (symbol,))
            conn.execute(
                """INSERT INTO bot_positions(
                    symbol,status,opened_at,updated_at,avg_price,total_qty,total_margin,dca_count,tp1_done,
                    last_price,unrealized_pct,note,strategy,realized_pnl,entry_date_kst,
                    base_entry_price,base_qty,add_qty,add_price,lowest_price,highest_price,cycle_anchor_price,trade_id
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (symbol, "OPEN", utc_now(), utc_now(), price, qty, self.cfg.position_margin_usdt, 0, 0,
                 price, 0.0, f"{strategy}형 데일리 진입", strategy, 0.0, trading_day(),
                 price, qty, 0.0, 0.0, price, price, price, trade_id),
            )
        details = json.dumps({
            "entry_price": price, "margin_usdt": self.cfg.position_margin_usdt,
            "leverage": self.cfg.leverage, "qty": qty, "score": round(score, 2)
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
        details = json.dumps({"exit_price": price, "avg_at_exit": float(row["avg_price"]),
                              "base_entry_price": float(row["base_entry_price"] or row["avg_price"]),
                              "closed_qty": qty, "fraction": fraction, "remaining_qty": remaining,
                              "step_realized_pnl": pnl_usdt, "trade_total_realized_pnl": total_realized,
                              "reason": reason}, ensure_ascii=False)
        log_event(row["symbol"], reason, price, qty, self.cfg.mode, details=details,
                  strategy=row["strategy"] or "", realized_pnl=pnl_usdt,
                  trade_id=row["trade_id"] or "")

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
                    and int(row["tp1_done"] or 0) == 0):
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
            if int(row["tp1_done"]) == 1 and base_pnl_pct <= -self.cfg.breakeven_stop_pct:
                self._close(row, price, 1.0, "BE_EXIT")
            elif base_pnl_pct <= -self.cfg.hard_stop_pct:
                self._close(row, price, 1.0, "STOP")
            elif (age_h * 60 >= self.cfg.flat_exit_minutes
                  and (highest / base_price - 1) * 100 < self.cfg.flat_min_favorable_pct):
                self._close(row, price, 1.0, "FLAT_EXIT_75M")
            elif age_h >= self.cfg.max_hold_hours:
                self._close(row, price, 1.0, "TIME_EXIT")
            elif int(row["tp1_done"]) == 0 and base_pnl_pct >= self.cfg.tp1_pct:
                self._close(row, price, 0.5, "TP1")
            elif int(row["tp1_done"]) == 1 and base_pnl_pct >= self.cfg.tp2_pct:
                self._close(row, price, 1.0, "TP2")

    def scan_entries(self) -> None:
        if state_flag("pause_new_entries", False):
            return
        if len(self.open_rows()) >= self.cfg.max_positions:
            return
        if self.cfg.max_daily_entries > 0 and self.daily_entries() >= self.cfg.max_daily_entries:
            return
        if self.daily_realized() <= -abs(self.cfg.daily_loss_limit_usdt):
            log_event("", "DAILY_STOP", mode=self.cfg.mode, details=f"pnl={self.daily_realized():.2f}")
            return
        if self.consecutive_losses() >= self.cfg.max_consecutive_losses:
            log_event("", "CONSECUTIVE_LOSS_STOP", mode=self.cfg.mode,
                      details=f"losses={self.consecutive_losses()}")
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
            for score, symbol, strategy, details in sorted(candidates, reverse=True, key=lambda x: x[0]):
                if slots <= 0 or entries_left <= 0:
                    break
                if symbol in open_symbols or same_risk_group(symbol, open_symbols):
                    continue
                self._open(symbol, float(details["price"]), strategy, score)
                open_symbols.add(symbol)
                slots -= 1
                entries_left -= 1

    def run_once(self) -> None:
        self.manage()
        self.scan_entries()

    def run_forever(self) -> None:
        state_set("bot_process_status", "RUNNING")
        log_event("", "BOT_START", mode=self.cfg.mode, details=json.dumps(asdict(self.cfg), ensure_ascii=False))
        while True:
            try:
                self.manage()
                if state_flag("shutdown_when_flat", False) and not self.open_rows():
                    state_set("bot_process_status", "STOPPED")
                    state_set("shutdown_when_flat", "0")
                    log_event("", "BOT_SAFE_STOP", mode=self.cfg.mode, details="포지션 0 확인 후 안전 종료")
                    break
                self.scan_entries()
            except Exception as exc:
                log_event("", "ERROR", mode=self.cfg.mode, details=str(exc))
            time.sleep(max(30, self.cfg.scan_seconds))


# 기존 실행 파일(run_okx_swing_bot.py)과 호환
SwingBot = DailyBot
SwingConfig = DailyConfig
