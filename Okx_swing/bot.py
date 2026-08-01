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
    universe_size: int = 12
    universe_refresh_minutes: int = 30
    min_quote_volume_24h_usdt: float = 10000000.0
    min_range_24h_pct: float = 2.5
    max_range_24h_pct: float = 20.0
    max_abs_change_24h_pct: float = 18.0
    max_spread_pct: float = 0.18
    # 데일리 3시간 전략에 맞게 최근 1~4시간 실제 움직임도 검사한다.
    recent_volatility_prefilter_size: int = 24
    min_recent_4h_range_pct: float = 1.8
    min_avg_hourly_range_pct: float = 0.55
    max_recent_1h_move_pct: float = 6.0
    # 장기 스윙에 더 어울리는 느린 종목은 데일리 후보에서 제외한다.
    slow_symbol_exclusions: tuple[str, ...] = (
        "XRP-USDT-SWAP", "AVAX-USDT-SWAP", "LINK-USDT-SWAP", "LTC-USDT-SWAP",
        "BCH-USDT-SWAP", "DOT-USDT-SWAP", "ETC-USDT-SWAP", "ATOM-USDT-SWAP",
        "TRX-USDT-SWAP", "TON-USDT-SWAP", "FIL-USDT-SWAP", "AAVE-USDT-SWAP",
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
    max_positions: int = 1
    max_daily_entries: int = 6
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
        _ensure_column(conn, "bot_positions", "base_entry_price", "REAL")
        _ensure_column(conn, "bot_positions", "base_qty", "REAL")
        _ensure_column(conn, "bot_positions", "add_qty", "REAL DEFAULT 0")
        _ensure_column(conn, "bot_positions", "add_price", "REAL DEFAULT 0")
        _ensure_column(conn, "bot_positions", "lowest_price", "REAL")
        _ensure_column(conn, "bot_events", "strategy", "TEXT")
        _ensure_column(conn, "bot_events", "realized_pnl", "REAL DEFAULT 0")


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


def rebound_add_signal(client: OKXClient, symbol: str) -> tuple[bool, dict[str, Any]]:
    """하락 뒤 실제 반등이 확인될 때만 순환 추가진입한다."""
    m5 = confirmed(indicators(client.candles(symbol, "5m", 120)))
    if len(m5) < 40:
        return False, {"reason": "5m 캔들 부족"}
    row, prev = m5.iloc[-1], m5.iloc[-2]
    recent_low = float(m5.tail(8).low.min())
    bullish = bool(row.close > row.open)
    break_prev = bool(row.close > prev.high)
    rsi_turn = bool(row.rsi > prev.rsi and row.rsi >= 32)
    ema_recover = bool(row.close >= row.ema9)
    low_hold = bool(row.low >= recent_low * 0.998)
    ok = bool(bullish and break_prev and rsi_turn and ema_recover and low_hold)
    return ok, {
        "price": float(row.close), "bullish": bullish, "break_prev": break_prev,
        "rsi_turn": rsi_turn, "ema_recover": ema_recover, "low_hold": low_hold,
        "rsi": round(float(row.rsi), 2),
    }


def qty_from_margin(price: float, margin_usdt: float, leverage: int) -> float:
    return max(0.0, margin_usdt * leverage / price)


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
        """유동성 + 24시간 변동성 + 최근 1~4시간 움직임으로 데일리 알트를 선별한다."""
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
            if symbol not in pool or symbol in excluded or not symbol.endswith("-USDT-SWAP"):
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
                if abs(change_pct) > self.cfg.max_abs_change_24h_pct:
                    continue
                if spread_pct > self.cfg.max_spread_pct:
                    continue
                liquidity_score = math.log10(max(quote_vol, 1.0)) * 7
                movement_score = min(range_pct, 14.0) * 5
                spread_penalty = spread_pct * 90
                ticker_score = liquidity_score + movement_score - spread_penalty
                ticker_ranked.append((ticker_score, symbol, {
                    "quote_volume": quote_vol, "range_pct": range_pct,
                    "change_pct": change_pct, "spread_pct": spread_pct,
                }))
            except (TypeError, ValueError, ZeroDivisionError):
                continue

        ticker_ranked.sort(reverse=True, key=lambda x: x[0])
        prefiltered = ticker_ranked[: max(self.cfg.universe_size, self.cfg.recent_volatility_prefilter_size)]
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

    def consecutive_losses(self) -> int:
        """오늘 종료된 거래를 최신순으로 보고 연속 손실 횟수를 센다."""
        day = today_kst()
        with db() as conn:
            rows = conn.execute(
                """SELECT realized_pnl FROM bot_positions
                   WHERE status='CLOSED' AND substr(datetime(updated_at,'+9 hours'),1,10)=?
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
        with db() as conn:
            conn.execute("DELETE FROM bot_positions WHERE symbol=?", (symbol,))
            conn.execute(
                """INSERT INTO bot_positions(
                    symbol,status,opened_at,updated_at,avg_price,total_qty,total_margin,dca_count,tp1_done,
                    last_price,unrealized_pct,note,strategy,realized_pnl,entry_date_kst,
                    base_entry_price,base_qty,add_qty,add_price,lowest_price
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (symbol, "OPEN", utc_now(), utc_now(), price, qty, self.cfg.position_margin_usdt, 0, 0,
                 price, 0.0, f"{strategy}형 데일리 진입", strategy, 0.0, today_kst(),
                 price, qty, 0.0, 0.0, price),
            )
        log_event(symbol, "ENTRY", price, qty, self.cfg.mode,
                  f"score={score:.1f}; margin={self.cfg.position_margin_usdt}", strategy)

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
                """UPDATE bot_positions SET avg_price=?,total_qty=?,total_margin=?,dca_count=1,
                   add_qty=?,add_price=?,updated_at=?,last_price=?,note=? WHERE symbol=?""",
                (new_avg, new_qty, float(row["total_margin"]) + self.cfg.rebound_add_margin_usdt,
                 add_qty, price, utc_now(), price, "반등 확인 후 순환 추가진입", row["symbol"]),
            )
        log_event(row["symbol"], "REBOUND_ADD", price, add_qty, self.cfg.mode,
                  details=f"blended_avg={new_avg:.8f}", strategy=row["strategy"] or "")

    def _cycle_reduce(self, row: sqlite3.Row, price: float) -> None:
        add_qty = float(row["add_qty"] or 0)
        if add_qty <= 0:
            return
        self._execute(row["symbol"], "sell", add_qty, reduce_only=True)
        pnl_usdt = (price - float(row["avg_price"])) * add_qty
        remaining = max(0.0, float(row["total_qty"]) - add_qty)
        base_price = float(row["base_entry_price"] or row["avg_price"])
        total_realized = float(row["realized_pnl"] or 0) + pnl_usdt
        with db() as conn:
            conn.execute(
                """UPDATE bot_positions SET avg_price=?,total_qty=?,total_margin=?,add_qty=0,add_price=0,
                   updated_at=?,last_price=?,note=?,realized_pnl=? WHERE symbol=?""",
                (base_price, remaining, max(0.0, float(row["total_margin"]) - self.cfg.rebound_add_margin_usdt),
                 utc_now(), price, "순환 추가분 정리 · 최초 물량 유지", total_realized, row["symbol"]),
            )
        log_event(row["symbol"], "CYCLE_REDUCE", price, add_qty, self.cfg.mode,
                  details="추가 수량만큼 정리", strategy=row["strategy"] or "", realized_pnl=pnl_usdt)

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
            base_price = float(row["base_entry_price"] or avg)
            pnl_pct = (price / avg - 1) * 100
            base_pnl_pct = (price / base_price - 1) * 100
            age_h = (datetime.now(timezone.utc) - datetime.fromisoformat(row["opened_at"])).total_seconds() / 3600
            lowest = min(float(row["lowest_price"] or price), price)
            with db() as conn:
                conn.execute("UPDATE bot_positions SET last_price=?,unrealized_pct=?,lowest_price=?,updated_at=? WHERE symbol=?",
                             (price, base_pnl_pct, lowest, utc_now(), row["symbol"]))

            # 추가분을 보유 중이면, 혼합평단 + 소폭 버퍼 회복 시 추가 수량만큼 우선 정리한다.
            if int(row["dca_count"] or 0) == 1 and float(row["add_qty"] or 0) > 0:
                cycle_target = avg * (1 + self.cfg.rebound_exit_buffer_pct / 100)
                if price >= cycle_target:
                    self._cycle_reduce(row, price)
                    continue

            # 하락 자체가 아니라, 일정 하락을 겪은 뒤 5분봉 반등이 확인될 때만 1회 추가한다.
            if (self.cfg.rebound_add_enabled and int(row["dca_count"] or 0) == 0
                    and int(row["tp1_done"] or 0) == 0):
                drawdown_pct = (lowest / base_price - 1) * 100
                if drawdown_pct <= -abs(self.cfg.rebound_arm_drawdown_pct):
                    try:
                        ok, details = rebound_add_signal(self.client, row["symbol"])
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
            elif age_h >= self.cfg.max_hold_hours:
                self._close(row, price, 1.0, "TIME_EXIT")
            elif int(row["tp1_done"]) == 0 and base_pnl_pct >= self.cfg.tp1_pct:
                self._close(row, price, 0.5, "TP1")
            elif int(row["tp1_done"]) == 1 and base_pnl_pct >= self.cfg.tp2_pct:
                self._close(row, price, 1.0, "TP2")

    def scan_entries(self) -> None:
        if len(self.open_rows()) >= self.cfg.max_positions:
            return
        if self.daily_entries() >= self.cfg.max_daily_entries:
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
