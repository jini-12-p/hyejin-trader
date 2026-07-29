from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class StrategySettings:
    fast_len: int = 20
    slow_len: int = 60
    rsi_len: int = 14
    buy_rsi_min: float = 48.0
    buy_rsi_max: float = 70.0
    vol_len: int = 20
    buy_vol_mult: float = 1.0
    bb_len: int = 20
    bb_mult: float = 2.0
    pullback_bars: int = 8
    ema_tolerance_pct: float = 0.8
    min_pullback_score: int = 5
    enable_reversal: bool = True
    reversal_lookback: int = 12
    reversal_rsi_level: float = 50.0
    reversal_vol_mult: float = 1.25
    reversal_body_mult: float = 1.20
    allow_early_reversal: bool = True
    take_profit_pct: float = 1.2
    stop_rsi_max: float = 45.0
    stop_vol_mult: float = 1.1
    entry_tolerance_pct: float = 0.30


@dataclass
class ScanResult:
    symbol: str
    signal: str
    signal_now: bool
    buy_price: float
    max_entry_price: float
    current_price: float
    entry_status: str
    pullback_score: int
    current_score: int
    rsi: float
    candle_time_utc: str
    reasons: str

    def to_dict(self) -> dict:
        return asdict(self)


def _rsi(close: pd.Series, length: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
    avg_loss = loss.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    value = 100 - (100 / (1 + rs))
    return value.fillna(100.0)


def _session_vwap(df: pd.DataFrame) -> pd.Series:
    # TradingView ta.vwap(hlc3)와 최대한 가깝게 UTC 일자별로 재시작합니다.
    typical = (df["high"] + df["low"] + df["close"]) / 3.0
    day = df["start_time"].dt.floor("D")
    cumulative_pv = (typical * df["volume"]).groupby(day).cumsum()
    cumulative_volume = df["volume"].groupby(day).cumsum().replace(0, np.nan)
    return cumulative_pv / cumulative_volume


def add_indicators(df: pd.DataFrame, s: StrategySettings) -> pd.DataFrame:
    out = df.copy()
    out["ema_fast"] = out["close"].ewm(span=s.fast_len, adjust=False).mean()
    out["ema_slow"] = out["close"].ewm(span=s.slow_len, adjust=False).mean()
    out["vwap"] = _session_vwap(out)
    out["rsi"] = _rsi(out["close"], s.rsi_len)
    out["vol_avg"] = out["volume"].rolling(s.vol_len).mean()
    out["avg_body"] = (out["close"] - out["open"]).abs().rolling(s.vol_len).mean()
    out["bb_basis"] = out["close"].rolling(s.bb_len).mean()
    # Pine ta.stdev는 population stdev에 해당하므로 ddof=0
    out["bb_dev"] = out["close"].rolling(s.bb_len).std(ddof=0) * s.bb_mult
    out["bb_upper"] = out["bb_basis"] + out["bb_dev"]
    out["bb_lower"] = out["bb_basis"] - out["bb_dev"]
    return out


def analyze_symbol(
    symbol: str,
    raw_df: pd.DataFrame,
    settings: StrategySettings | None = None,
) -> ScanResult:
    s = settings or StrategySettings()
    df = add_indicators(raw_df, s)

    if len(df) < max(s.slow_len, s.reversal_lookback, s.vol_len) + 3:
        raise ValueError(f"{symbol}: 계산에 필요한 캔들이 부족합니다.")

    i = len(df) - 1
    row = df.iloc[i]
    prev = df.iloc[i - 1]

    trend_ok = row.ema_fast > row.ema_slow
    slope_ok = row.ema_fast >= prev.ema_fast
    price_ok = row.close > row.ema_fast and row.close > row.vwap
    rsi_ok = s.buy_rsi_min <= row.rsi <= s.buy_rsi_max
    volume_ok = pd.notna(row.vol_avg) and row.volume >= row.vol_avg * s.buy_vol_mult
    recent_low = df["low"].iloc[i - s.pullback_bars + 1 : i + 1].min()
    pullback_ok = recent_low <= row.ema_fast * (1 + s.ema_tolerance_pct / 100)

    # V1에서는 BTC 필터를 꺼둔 Pine 기본값과 동일하게 1점으로 처리합니다.
    btc_ok = True
    pullback_score = sum(
        int(x)
        for x in [
            trend_ok,
            slope_ok,
            price_ok,
            rsi_ok,
            volume_ok,
            pullback_ok,
            btc_ok,
        ]
    )
    bullish_breakout = row.close > prev.high and row.close > row.open
    pullback_setup = (
        pullback_score >= s.min_pullback_score
        and trend_ok
        and bullish_breakout
        and row.close < row.bb_upper
    )

    recent = df.iloc[i - s.reversal_lookback + 1 : i + 1]
    recent_bottom = recent["low"].min()
    bottom_near_lower = (recent["low"] - recent["bb_lower"]).min() <= 0
    expanded = row.close >= recent_bottom * 1.006
    bull_body = row.close - row.open
    strong_bull = (
        row.close > row.open
        and pd.notna(row.avg_body)
        and bull_body >= row.avg_body * s.reversal_body_mult
    )
    rsi_cross = prev.rsi <= s.reversal_rsi_level < row.rsi
    rsi_rising = row.rsi > s.reversal_rsi_level and row.rsi > prev.rsi
    reversal_volume = (
        pd.notna(row.vol_avg)
        and row.volume >= row.vol_avg * s.reversal_vol_mult
    )
    reclaim_fast = (
        (prev.close <= prev.ema_fast and row.close > row.ema_fast)
        or (row.close > row.ema_fast and prev.close <= prev.ema_fast)
    )
    reclaim_vwap = (
        (prev.close <= prev.vwap and row.close > row.vwap)
        or (row.close > row.vwap and prev.close <= prev.vwap)
    )
    ema_turning_up = row.ema_fast > prev.ema_fast
    early_trend_ok = ema_turning_up if s.allow_early_reversal else trend_ok
    reversal_setup = (
        s.enable_reversal
        and bottom_near_lower
        and expanded
        and strong_bull
        and (rsi_cross or rsi_rising)
        and reversal_volume
        and (reclaim_fast or reclaim_vwap)
        and early_trend_ok
        and btc_ok
        and row.close < row.bb_upper
    )

    signal = "BUY-R" if reversal_setup else "BUY-P" if pullback_setup else "-"
    signal_now = signal != "-"
    buy_price = float(row.close)
    max_entry = buy_price * (1 + s.entry_tolerance_pct / 100)

    structure_broken = (
        row.close < (recent_bottom if signal == "BUY-R" else row.ema_slow)
        or row.rsi < s.stop_rsi_max
    )
    falling_now = row.close < row.open or row.close < row.ema_fast or not slope_ok
    if not signal_now:
        entry_status = "NO SIGNAL"
    elif structure_broken:
        entry_status = "PASS"
    elif falling_now:
        entry_status = "WAIT"
    else:
        entry_status = "ENTRY OK"

    hold_ema = slope_ok
    hold_zone = row.close >= row.ema_fast or row.close >= row.vwap
    hold_rsi = s.stop_rsi_max <= row.rsi <= s.buy_rsi_max + 5
    hold_btc = btc_ok
    hold_no_dump = not (
        row.close < row.open
        and pd.notna(row.vol_avg)
        and row.volume >= row.vol_avg * s.stop_vol_mult
    )
    current_score = sum(
        int(x) for x in [hold_ema, hold_zone, hold_rsi, hold_btc, hold_no_dump]
    )

    reason_items = []
    if trend_ok:
        reason_items.append("EMA20>EMA60")
    if slope_ok:
        reason_items.append("EMA20 상승")
    if price_ok:
        reason_items.append("EMA·VWAP 위")
    if rsi_ok:
        reason_items.append("RSI 정상")
    if volume_ok:
        reason_items.append("거래량 통과")
    if pullback_ok:
        reason_items.append("눌림 확인")

    return ScanResult(
        symbol=symbol,
        signal=signal,
        signal_now=signal_now,
        buy_price=buy_price,
        max_entry_price=float(max_entry),
        current_price=float(row.close),
        entry_status=entry_status,
        pullback_score=int(pullback_score),
        current_score=int(current_score),
        rsi=float(row.rsi),
        candle_time_utc=row.start_time.isoformat(),
        reasons=", ".join(reason_items) or "-",
    )
