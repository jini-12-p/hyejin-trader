from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone

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
    max_stop_pct: float = 5.0
    stop_buffer_pct: float = 0.10
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
    pullback_score_10: float
    current_score_10: float
    rsi: float
    candle_time_utc: str
    stop_price: float
    stop_pct: float
    tp_price: float
    reasons: str
    fail_reasons: str

    def to_dict(self) -> dict:
        return asdict(self)


def _rsi(close: pd.Series, length: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
    avg_loss = loss.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return (100 - (100 / (1 + rs))).fillna(100.0)


def _session_vwap(df: pd.DataFrame) -> pd.Series:
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
    out["bb_dev"] = out["close"].rolling(s.bb_len).std(ddof=0) * s.bb_mult
    out["bb_upper"] = out["bb_basis"] + out["bb_dev"]
    out["bb_lower"] = out["bb_basis"] - out["bb_dev"]
    return out


def _closed_only(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Bybit의 마지막 행은 진행 중인 15분봉일 수 있으므로 마감봉만 사용합니다."""
    if raw_df.empty:
        return raw_df
    now = pd.Timestamp(datetime.now(timezone.utc))
    interval = pd.Timedelta(minutes=15)
    mask = raw_df["start_time"] + interval <= now
    closed = raw_df.loc[mask].copy()
    return closed if len(closed) >= 70 else raw_df.iloc[:-1].copy()



def evaluate_live_entry(
    signal_now: bool,
    signal_price: float,
    max_entry_price: float,
    live_open: float,
    live_price: float,
) -> tuple[str, bool, float]:
    """마감 BUY 후보 이후 현재 진행봉이 실제 진입 가능한지 최종 판단합니다."""
    if signal_price <= 0:
        return "가격 확인 오류", False, 0.0

    diff_pct = (live_price - signal_price) / signal_price * 100
    live_is_bullish = live_price > live_open

    if not signal_now:
        return "대기", False, diff_pct
    if live_price > max_entry_price:
        return "추격 금지", False, diff_pct
    if live_price < signal_price:
        return "눌림 중 · 진입 보류", False, diff_pct
    if not live_is_bullish:
        return "진행봉 음봉 · 진입 보류", False, diff_pct
    return "실시간 진입 가능", True, diff_pct


def analyze_symbol(symbol: str, raw_df: pd.DataFrame, settings: StrategySettings | None = None) -> ScanResult:
    s = settings or StrategySettings()
    df = add_indicators(_closed_only(raw_df), s)
    if len(df) < max(s.slow_len, s.reversal_lookback, s.vol_len) + 3:
        raise ValueError(f"{symbol}: 계산에 필요한 마감 캔들이 부족합니다.")

    i = len(df) - 1
    row, prev = df.iloc[i], df.iloc[i - 1]
    trend_ok = row.ema_fast > row.ema_slow
    slope_ok = row.ema_fast >= prev.ema_fast
    price_ok = row.close > row.ema_fast and row.close > row.vwap
    rsi_ok = s.buy_rsi_min <= row.rsi <= s.buy_rsi_max
    volume_ok = pd.notna(row.vol_avg) and row.volume >= row.vol_avg * s.buy_vol_mult
    recent_low = df["low"].iloc[i - s.pullback_bars + 1:i + 1].min()
    pullback_ok = recent_low <= row.ema_fast * (1 + s.ema_tolerance_pct / 100)
    btc_ok = True
    checks = [trend_ok, slope_ok, price_ok, rsi_ok, volume_ok, pullback_ok, btc_ok]
    pullback_raw = sum(int(x) for x in checks)
    pullback_score_10 = round(pullback_raw / 7 * 10, 1)

    # TradingView BUY-P 핵심: 마감 양봉이 직전 봉 고가를 돌파해야 합니다.
    bullish_breakout = row.close > prev.high and row.close > row.open
    pullback_setup = (
        pullback_raw >= s.min_pullback_score
        and trend_ok
        and bullish_breakout
        and row.close < row.bb_upper
        and rsi_ok
    )

    recent = df.iloc[i - s.reversal_lookback + 1:i + 1]
    recent_bottom = recent["low"].min()
    bottom_near_lower = (recent["low"] - recent["bb_lower"]).min() <= 0
    expanded = row.close >= recent_bottom * 1.006
    bull_body = row.close - row.open
    strong_bull = row.close > row.open and pd.notna(row.avg_body) and bull_body >= row.avg_body * s.reversal_body_mult
    rsi_cross = prev.rsi <= s.reversal_rsi_level < row.rsi
    rsi_rising = row.rsi > s.reversal_rsi_level and row.rsi > prev.rsi
    reversal_volume = pd.notna(row.vol_avg) and row.volume >= row.vol_avg * s.reversal_vol_mult
    reclaim_fast = prev.close <= prev.ema_fast and row.close > row.ema_fast
    reclaim_vwap = prev.close <= prev.vwap and row.close > row.vwap
    early_trend_ok = row.ema_fast > prev.ema_fast if s.allow_early_reversal else trend_ok
    reversal_setup = (
        s.enable_reversal and bottom_near_lower and expanded and strong_bull
        and (rsi_cross or rsi_rising) and reversal_volume
        and (reclaim_fast or reclaim_vwap) and early_trend_ok
        and row.close < row.bb_upper and rsi_ok
    )

    signal = "BUY-R" if reversal_setup else "BUY-P" if pullback_setup else "-"
    signal_now = signal != "-"
    buy_price = float(row.close)
    max_entry = buy_price * (1 + s.entry_tolerance_pct / 100)

    # 신호는 마감봉으로 판단하지만, 실제 진입 상태는 최신 진행봉으로 확인합니다.
    live_source = raw_df.sort_values("start_time") if not raw_df.empty else raw_df
    live_row = live_source.iloc[-1] if not live_source.empty else row
    current_price = float(live_row["close"])
    live_open = float(live_row["open"])
    live_is_bullish = current_price > live_open

    previous_bearish = df.iloc[max(0, i - 6):i]
    previous_bearish = previous_bearish[previous_bearish["close"] < previous_bearish["open"]]
    bearish_low = float(previous_bearish.iloc[-1]["low"]) if not previous_bearish.empty else float(recent_low)
    six_low = float(df["low"].iloc[max(0, i - 5):i + 1].min())
    structural_low = min(bearish_low, six_low)
    stop_price = structural_low * (1 - s.stop_buffer_pct / 100)
    stop_pct = max(0.0, (buy_price - stop_price) / buy_price * 100)
    stop_too_wide = stop_pct > s.max_stop_pct
    tp_price = buy_price * (1 + s.take_profit_pct / 100)

    hold_checks = [
        slope_ok,
        row.close >= row.ema_fast or row.close >= row.vwap,
        s.stop_rsi_max <= row.rsi <= s.buy_rsi_max + 5,
        btc_ok,
        not (row.close < row.open and pd.notna(row.vol_avg) and row.volume >= row.vol_avg * s.stop_vol_mult),
    ]
    current_score_10 = round(sum(int(x) for x in hold_checks) / 5 * 10, 1)

    entry_status, _, _ = evaluate_live_entry(
        signal_now=signal_now,
        signal_price=buy_price,
        max_entry_price=max_entry,
        live_open=live_open,
        live_price=current_price,
    )

    labels = ["EMA20>EMA60", "EMA20 상승", "EMA·VWAP 위", "RSI 정상", "거래량 통과", "눌림 확인", "BTC 필터"]
    reasons = [name for name, ok in zip(labels, checks) if ok]
    failed = [name for name, ok in zip(labels, checks) if not ok]
    if not bullish_breakout:
        failed.append("양봉·직전 고점 돌파 없음")
    if signal_now and current_price < buy_price:
        failed.append("현재가가 신호가 아래 눌림")
    elif signal_now and not live_is_bullish:
        failed.append("현재 진행봉 음봉")
    if row.close >= row.bb_upper:
        failed.append("볼린저 상단 과열")
    if stop_too_wide:
        failed.append(f"구조 손절폭 {stop_pct:.2f}% (참고)")

    return ScanResult(
        symbol=symbol,
        signal=signal,
        signal_now=signal_now,
        buy_price=buy_price,
        max_entry_price=float(max_entry),
        current_price=current_price,
        entry_status=entry_status,
        pullback_score_10=pullback_score_10,
        current_score_10=current_score_10,
        rsi=float(row.rsi),
        candle_time_utc=row.start_time.isoformat(),
        stop_price=float(stop_price),
        stop_pct=float(stop_pct),
        tp_price=float(tp_price),
        reasons=", ".join(reasons) or "-",
        fail_reasons=", ".join(failed) or "-",
    )
