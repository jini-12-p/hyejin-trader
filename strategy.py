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

    # v2 재상승 확인 진입
    small_bear_body_mult: float = 0.90
    large_bear_body_mult: float = 1.35
    engulf_body_ratio: float = 1.00
    recovery_bars: int = 3
    recovery_min_green: int = 2
    recovery_close_ratio: float = 0.70
    confirmation_vol_mult: float = 0.90


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



def _entry_confirmation(df: pd.DataFrame, i: int, s: StrategySettings) -> tuple[bool, str, list[str]]:
    """
    네 수동 진입 패턴을 코드화합니다.

    1) 작은 음봉 뒤 양봉이 음봉 몸통을 회복하면 진입 허용
    2) 큰 음봉 뒤에는 즉시 진입 금지
    3) 큰 음봉 뒤 2개 이상 회복 양봉 + 낙폭 70% 이상 회복,
       또는 큰 음봉 고가 돌파 시 진입 허용
    """
    row = df.iloc[i]
    prev = df.iloc[i - 1]
    avg_body = float(row.avg_body) if pd.notna(row.avg_body) and row.avg_body > 0 else 0.0

    row_green = row.close > row.open
    volume_returned = (
        pd.notna(row.vol_avg)
        and row.volume >= row.vol_avg * s.confirmation_vol_mult
    )
    breakout = row_green and row.close > prev.high

    prev_body = abs(float(prev.close - prev.open))
    prev_bearish = prev.close < prev.open
    small_bear = prev_bearish and avg_body > 0 and prev_body <= avg_body * s.small_bear_body_mult
    large_bear = prev_bearish and avg_body > 0 and prev_body >= avg_body * s.large_bear_body_mult

    bullish_engulf = (
        small_bear
        and row_green
        and row.open <= prev.close
        and row.close >= prev.open
        and (row.close - row.open) >= prev_body * s.engulf_body_ratio
    )

    lookback_start = max(0, i - s.recovery_bars)
    recent_before = df.iloc[lookback_start:i]
    large_bear_rows = recent_before[
        (recent_before["close"] < recent_before["open"])
        & ((recent_before["open"] - recent_before["close"]) >= recent_before["avg_body"] * s.large_bear_body_mult)
    ]

    recovery_confirmed = False
    large_bear_breakout = False
    if not large_bear_rows.empty:
        bear = large_bear_rows.iloc[-1]
        after_bear = df.loc[bear.name + 1:df.index[i]]
        green_count = int((after_bear["close"] > after_bear["open"]).sum())

        bear_drop = float(bear.open - bear.close)
        recovered = float(row.close - bear.close)
        recovery_ratio = recovered / bear_drop if bear_drop > 0 else 0.0

        recovery_confirmed = (
            row_green
            and green_count >= s.recovery_min_green
            and recovery_ratio >= s.recovery_close_ratio
            and row.close > prev.close
        )
        large_bear_breakout = row_green and row.close > float(bear.high)

    confirmed = bullish_engulf or breakout or recovery_confirmed or large_bear_breakout

    notes: list[str] = []
    if bullish_engulf:
        notes.append("작은 음봉 몸통 회복")
    if breakout:
        notes.append("직전 고점 돌파")
    if recovery_confirmed:
        notes.append("큰 음봉 후 재상승 회복")
    if large_bear_breakout:
        notes.append("큰 음봉 고가 돌파")
    if volume_returned:
        notes.append("거래량 복귀")

    if large_bear and not large_bear_breakout:
        return False, "재상승 대기", notes + ["큰 음봉 직후 진입 금지"]

    if confirmed and volume_returned:
        return True, "BUY", notes

    if confirmed and not volume_returned:
        return False, "재상승 대기", notes + ["거래량 복귀 대기"]

    return False, "재상승 대기", notes


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

    bullish_breakout = row.close > prev.high and row.close > row.open
    entry_confirmed, confirmation_status, confirmation_notes = _entry_confirmation(df, i, s)
    pullback_setup = pullback_raw >= s.min_pullback_score and trend_ok and row.close < row.bb_upper and rsi_ok

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

    if not signal_now:
        entry_status = "대기"
    elif row.close < row.ema_fast or not slope_ok:
        entry_status = "관찰 BUY"
    elif entry_confirmed:
        entry_status = "BUY"
    else:
        entry_status = confirmation_status

    labels = ["EMA20>EMA60", "EMA20 상승", "EMA·VWAP 위", "RSI 정상", "거래량 통과", "눌림 확인", "BTC 필터"]
    reasons = [name for name, ok in zip(labels, checks) if ok]
    reasons.extend(confirmation_notes)
    failed = [name for name, ok in zip(labels, checks) if not ok]
    if signal_now and not entry_confirmed:
        failed.append("재상승 확인 전")
    if not bullish_breakout and not entry_confirmed:
        failed.append("직전 고점 돌파 없음")
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
        current_price=buy_price,
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
