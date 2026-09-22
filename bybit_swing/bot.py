from __future__ import annotations

import csv
import json
import math
import hmac
import hashlib
import sqlite3
import time
import threading
import queue
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
BOT_RUNTIME_VERSION = "RC-v4.3.89-ManualPShadow-TP20-Stop4"

# HJ 신고점 돌파 예외는 한 번의 순간 스파이크로 열지 않는다.
# 같은 종목이 다음 스캔에서도 돌파 상태를 유지해야 "확인된 돌파"로 인정한다.
_HJ_BREAKOUT_CONFIRM: dict[str, dict[str, float]] = {}



class _BoundedReadClient:
    """Bybit read API calls get a hard wall-clock timeout.

    A network/library call can occasionally stall longer than the HTTP timeout.
    The trading loop must never wait forever for ticker/candle/universe reads.
    Only read methods are wrapped; order/write methods are forwarded unchanged.
    """

    READ_METHODS = {"ticker", "tickers", "candles"}

    def __init__(self, client: Any, timeout_seconds: float = 8.0):
        self._client = client
        self._timeout_seconds = float(timeout_seconds)
        self._guard_lock = threading.Lock()
        self._inflight: dict[str, threading.Thread] = {}

    def _bounded(self, name: str, *args: Any, **kwargs: Any) -> Any:
        key = name + "|" + repr(args) + "|" + repr(sorted(kwargs.items()))
        with self._guard_lock:
            prior = self._inflight.get(key)
            if prior is not None and prior.is_alive():
                raise BybitSwingError(f"{name} previous call still running; skipped to protect main loop")

        q: queue.Queue[tuple[bool, Any]] = queue.Queue(maxsize=1)

        def worker() -> None:
            try:
                q.put((True, getattr(self._client, name)(*args, **kwargs)))
            except BaseException as exc:
                q.put((False, exc))

        t = threading.Thread(target=worker, name=f"bybit-read-{name}", daemon=True)
        with self._guard_lock:
            self._inflight[key] = t
        t.start()
        t.join(self._timeout_seconds)
        if t.is_alive():
            raise BybitSwingError(f"{name} timed out after {self._timeout_seconds:.1f}s; main loop protected")

        with self._guard_lock:
            if self._inflight.get(key) is t:
                self._inflight.pop(key, None)

        try:
            ok, payload = q.get_nowait()
        except queue.Empty as exc:
            raise BybitSwingError(f"{name} finished without a result") from exc
        if ok:
            return payload
        raise payload

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._client, name)
        if name in self.READ_METHODS and callable(attr):
            return lambda *args, **kwargs: self._bounded(name, *args, **kwargs)
        return attr


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
    position_margin_usdt: float = 27.0
    hj_position_margin_usdt: float = 36.0
    tp1_pct: float = 1.5
    tp2_pct: float = 3.0
    # 실전에서는 진입 직후 Bybit 거래소에 TP1/TP2 reduce-only 지정가를 선주문한다.
    exchange_tp_preorders_enabled: bool = True
    exchange_tp_sync_seconds: float = 5.0
    hard_stop_pct: float = 1.5
    # TP1 체결 후 남은 물량은 Bybit 거래소에 실제 평단보다 위쪽으로 보호 스탑을 건다.
    # 0.15%는 시장가 진입/스탑 청산 수수료와 소폭 슬리피지 여유를 둔 기본값이다.
    breakeven_stop_pct: float = 0.15
    staged_stop_enabled: bool = True
    stage1_stop_pct: float = 1.5
    stage1_stop_fraction: float = 0.5
    final_stop_pct: float = 2.3
    recovery_exit_loss_pct: float = 0.3
    max_hold_hours: int = 3
    daily_loss_limit_usdt: float = 12.0
    max_consecutive_losses: int = 3
    loss_cooldown_minutes: int = 0
    paper_consecutive_loss_warning_only: bool = True
    # v4.3.53: 준P형은 실제 주문 없이 Shadow 데이터만 수집한다.
    # 정식 P형 핵심 안전조건은 유지하고, 점수 90+에서 soft 조건 딱 1개만 부족한 후보만 추적.
    junp_shadow_enabled: bool = False
    junp_shadow_min_score: float = 90.0
    junp_shadow_same_symbol_cooldown_minutes: int = 90
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
    rebound_add_margin_usdt: float = 13.5
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
    # v4.3.51: P형은 진행 중 15분봉이 이미 너무 많이 오른 뒤의 추격 진입을 막는다.
    # 기본 P형 조건/점수/TP/손절은 그대로 두고 live candle 상승률 상한만 적용한다.
    p_live_max_gain_pct: float = 0.50

    # v4.3.58 연구/가상검증 모드. LIVE 주문조건에는 사용하지 않는다.
    research_shadow_enabled: bool = True
    research_shadow_same_symbol_cooldown_minutes: int = 90
    research_new_score_min: float = 90.0
    research_strength_rsi_min: float = 62.0
    research_strength_rebound_min_pct: float = 3.0
    research_strength_live_gain_min_pct: float = 0.32
    research_live_cap_050_pct: float = 0.50
    research_live_cap_060_pct: float = 0.60
    research_nearmiss_max_failed_checks: int = 2
    research_nearmiss_min_old_score: float = 90.0

    # v4.3.59 새 P/JUNP v2 Shadow 기준. LIVE 주문에는 사용하지 않는다.
    # 8/28 누적 연구에서 TP2군과 STOP군 차이가 반복된 추세 지속력(1h 방향, EMA slope/gap, rebound)을
    # 중심으로 점수화하고, 단일 old-P boolean 하나 때문에 강한 후보가 탈락하지 않게 설계한다.
    research_pv2_total_min: float = 62.0
    research_pv2_component_pass_min: int = 3
    research_pv2_signal_pass_min: int = 4
    research_junpv2_total_min: float = 54.0
    research_junpv2_component_pass_min: int = 2
    research_junpv2_signal_pass_min: int = 3
    # v4.3.60: v2.1은 순간 모멘텀보다 추세+구조의 지속성을 우선한다.
    research_pv21_total_min: float = 68.0
    research_pv21_persistence_min: float = 46.0
    research_pv21_signal_pass_min: int = 4
    research_junpv21_total_min: float = 58.0
    research_junpv21_persistence_min: float = 38.0
    research_junpv21_signal_pass_min: int = 4
    research_pv21_max_overheat_risk_count: int = 1
    # v4.3.61: P/JUNP v2.2 Shadow. v2.1의 전체 점수는 유지하되 실패 유형을 두 갈래로 최소 보정한다.
    # 1) 약한/미완성 구조, 2) 강하지만 이미 과진행된 끝물. LIVE 주문에는 사용하지 않는다.
    research_pv22_total_min: float = 68.0
    research_pv22_persistence_min: float = 46.0
    research_pv22_signal_pass_min: int = 4
    research_junpv22_total_min: float = 60.0
    research_junpv22_persistence_min: float = 42.0
    research_junpv22_signal_pass_min: int = 4
    research_pv22_max_heat_count: int = 2
    research_pv22_max_structure_weak_count: int = 2
    # v4.3.63 P_V23 research-only dual filter. P_V22 remains unchanged as control.
    research_pv23_weak_ema20_slope_max: float = 0.25
    research_pv23_weak_ema_gap_max: float = 1.00
    research_pv23_rsi_delta_spike_min: float = 8.00
    # v4.3.64 P_V24 research-only. LIVE/P_V22/P_V23 판정은 그대로 둔다.
    research_pv24_confirm_min_minutes: int = 5
    research_pv24_confirm_max_minutes: int = 20
    research_pv24_recovery_from_low_min_pct: float = 0.35
    research_pv24_live_gain_min_pct: float = -0.10
    research_pv24_max_adverse_before_confirm_pct: float = 1.50
    research_pv24_same_setup_lock_minutes: int = 45
    research_pv24_max_entries_per_15m: int = 2
    research_pv24_stop_pause_window_minutes: int = 30
    research_pv24_stop_pause_count: int = 2
    research_pv24_max_open_positions: int = 4
    # v4.3.66 P_V25 research-only: P_V22가 고른 15분 후보를 확정 5분봉 재가속으로 실행한다.
    research_pv25_confirm_min_minutes: int = 5
    research_pv25_confirm_max_minutes: int = 20
    research_pv25_max_adverse_before_confirm_pct: float = 1.50
    research_pv25_same_setup_lock_minutes: int = 45
    research_pv25_max_entries_per_15m: int = 2
    research_pv25_max_open_positions: int = 4

    # v4.3.67 P_V26 research-only: V25의 5분 재가속 골격/TP/4슬롯/rolling cap은 그대로 유지하고
    # 손실 꼬리를 만든 3개 유형(A형, STOP 동일종목 재진입, TP1 미도달 장기실패)만 최소 보정한다.
    research_pv26_enabled: bool = True
    research_pv26_max_entries_per_15m: int = 2
    research_pv26_max_open_positions: int = 4
    research_pv26_stop_reentry_cooldown_minutes: int = 180
    # A형 사전차단: V25 완료표본에서 APT/TIA/AGI/LIT/CHIP STOP 5건을 잡고 완료 성공거래는 0건 차단.
    # 동일 데이터에서 찾은 조건이므로 V26에서는 BLOCKED_GHOST를 반드시 병행해 전진검증한다.
    research_pv26_a_live_gain_max_pct: float = 0.40
    research_pv26_a_prev2_gain_max_pct: float = 1.00
    research_pv26_a_prev3_gain_min_pct: float = 0.20
    research_pv26_a_ema_gap_min_pct: float = 0.50
    # 장기실패: 단순 시간손절 금지. TP1 미도달 + MFE<1% + 손실확대 + 15분 구조훼손이
    # 2회 연속 확인될 때만 종료하며, 종료 후 원래 V25 경로를 ghost로 끝까지 추적한다.
    research_pv26_late_failure_enabled: bool = True
    research_pv26_late_mfe_max_pct: float = 1.00
    research_pv26_late_stage1_age_minutes: int = 80
    research_pv26_late_stage1_pnl_pct: float = -1.50
    research_pv26_late_stage2_age_minutes: int = 120
    research_pv26_late_stage2_pnl_pct: float = -1.00
    research_pv26_late_confirmations: int = 2
    research_pv26_ghost_tracking_enabled: bool = True
    research_pv26_stop_ghost_enabled: bool = True
    research_pv26_stop_ghost_minutes: int = 180
    research_junp_variants_enabled: bool = False
    # v4.3.69: V27 검증부터 오래된 연구 신규진입은 중지한다.
    # 기존 열린 Shadow는 관리루프에서 정상 종료시키되, 신규 P_V22/P_V25/near-miss는 만들지 않는다.
    research_legacy_variants_enabled: bool = False
    research_pv25_control_enabled: bool = False
    # V26 control은 유지한다. 기존 V27/V27-FilterOnly 신규진입은 v4.3.72부터 OFF하고
    # 이미 열린 Shadow만 끝까지 관리한다.
    research_pv27_enabled: bool = False
    research_pv27_max_entries_per_15m: int = 2
    research_pv27_max_open_positions: int = 4
    research_pv27_stall_prev2_gain_min_pct: float = 0.70
    research_pv27_stall_rsi_change_prev1_max: float = 3.40
    research_pv27_stall_score_min: float = 70.0
    research_pv27_stall_prev1_gain_max_pct: float = 2.00
    research_pv27_a1_score_min: float = 70.0
    research_pv27_a1_prev1_gain_max_pct: float = 1.50
    research_pv27_ghost_tracking_enabled: bool = True
    research_pv27_filter_only_enabled: bool = False

    # v4.3.72 P_V27_2 research-only:
    # 9/3~9/5 전진 Shadow + FilterOnly 전수분석에서 반복된 두 좁은 손실패턴만 차단한다.
    # 1) HIGH_SCORE_RSI_LAG: 점수는 매우 높지만 RSI가 64 미만인 채 직전 2봉 전 상승이 0.70%+인 모멘텀 불일치.
    #    누적 완료표본 5건 모두 STOP.
    # 2) WEAK_STRUCTURE: P_V2 점수<55 + EMA9-20 gap<0.50% + EMA20 slope<0.20%.
    #    누적 완료표본 3건 모두 음수종료(2 STOP + 1 FLAT-).
    # 기존 V27의 넓은 STALL/A1 차단은 사용하지 않는다. BTC/ETH telemetry는 기록만 하고 진입차단에는 사용하지 않는다.
    research_pv272_enabled: bool = False
    research_pv272_max_entries_per_15m: int = 2
    research_pv272_max_open_positions: int = 4
    research_pv272_high_score_min: float = 89.0
    research_pv272_rsi_max: float = 64.0
    research_pv272_prev2_gain_min_pct: float = 0.70
    research_pv272_weak_score_max: float = 55.0
    research_pv272_weak_ema_gap_max_pct: float = 0.50
    research_pv272_weak_ema20_slope_max_pct: float = 0.20
    research_pv272_ghost_tracking_enabled: bool = True
    research_pv272_filter_only_enabled: bool = False

    # v4.3.73 P_V27_3 research-only:
    # V27-2의 HIGH_SCORE_RSI_LAG / WEAK_STRUCTURE 신규 차단은 종료하고,
    # 최근 V26 실제 STOP에서 반복된 '직전 강한 impulse + RSI 급등 + EMA 분리 부족' 한 패턴만 전진검증한다.
    # 조건은 의도적으로 단순하게 유지하고 BTC/ETH telemetry는 계속 기록만 한다.
    # 과거 데이터에서 고수익 거래까지 차단될 수 있으므로 이 조건은 LIVE 적용이 아니라 Shadow/FilterOnly 검증 전용이다.
    research_pv273_enabled: bool = False
    research_pv273_max_entries_per_15m: int = 2
    research_pv273_max_open_positions: int = 4
    research_pv273_score_max: float = 80.0
    research_pv273_prev2_gain_min_pct: float = 1.00
    research_pv273_rsi_change_prev1_min: float = 5.00
    research_pv273_ema_gap_max_pct: float = 1.00
    research_pv273_ghost_tracking_enabled: bool = True
    research_pv273_filter_only_enabled: bool = True

    # v4.3.74 P_V27_4 research-only:
    # V27-3 impulse hard block은 새 버전에는 계승하지 않는다.
    # V25의 확정 5분 재가속 골격을 그대로 쓰고, 네 가지 구조지표가 모두 하단일 때만
    # 아주 좁은 WEAK_STRUCTURE_4OF4 후보로 차단한다. 하나라도 살아 있으면 통과시켜
    # 빠른 TP/BE를 과도하게 죽이지 않는 것이 목적이다.
    # 설계시점 재검증: V25/V26 actual-live path에서는 이 보수적 4-of-4가 소수만 차단했고
    # 최근 V27-3 전진표본에서는 NOM/TAO/XLM 같은 TP1 미도달군을 선택적으로 잡았다.
    # 아직 전진검증 전이므로 LIVE 적용 금지. BLOCK_GHOST/STOP_GHOST로 180분 사후경로를 반드시 남긴다.
    research_pv274_enabled: bool = False
    research_pv274_max_entries_per_15m: int = 2
    research_pv274_max_open_positions: int = 4
    research_pv274_ema20_slope_max_pct: float = 0.25
    research_pv274_ema_gap_max_pct: float = 0.80
    research_pv274_persistence_max: float = 55.0
    research_pv274_rebound_max_pct: float = 4.00
    research_pv274_required_weak_count: int = 4
    research_pv274_ghost_tracking_enabled: bool = True
    research_pv274_filter_only_enabled: bool = False
    research_pv274_stop_ghost_enabled: bool = True
    research_pv274_post_track_minutes: int = 180

    # v4.3.75 P_V27_4_1 research-only:
    # V25의 확정 5분 재가속 진입은 그대로 사용하고, V27-4의 4-of-4 hard block은 사용하지 않는다.
    # 과거/전진 재분석에서 slope+gap+persistence 약세군은 위험도는 높지만 TP도 다수 포함되어 hard block이 부적합했다.
    # 따라서 세 핵심 구조지표가 모두 약하면 진입은 유지하되 WEAK_WATCH로 표시하고 5/10/15분 경로를 자동 기록한다.
    # STOP은 별도 STOP_GHOST로 180분 추적해 진입실패와 손절문제를 계속 분리한다. LIVE 적용 금지.
    research_pv2741_enabled: bool = True
    research_pv2741_max_entries_per_15m: int = 2
    research_pv2741_max_open_positions: int = 4
    research_pv2741_ema20_slope_max_pct: float = 0.25
    research_pv2741_ema_gap_max_pct: float = 0.80
    research_pv2741_persistence_max: float = 55.0
    research_pv2741_watch_checkpoints_min: tuple[int, ...] = (5, 10, 15)
    research_pv2741_stop_ghost_enabled: bool = True
    research_pv2741_post_track_minutes: int = 180
    # v4.3.78: 과거 35개 실패유형 분류/태깅은 완전히 제거. V27.4.1은 V25 control + weak-watch만 남긴다.
    # v4.3.78 P_V27_4_2 research-only:
    # V25/V27.4.1의 확정 5분 재가속 진입은 그대로 두고, 9/8~9/11 Forward 100건에서
    # 정상 TP/BE 오차단 원인을 범위/측정시점 관점으로 보정한 고정 진입필터만 추가 검증한다.
    # 중요: P_V27_4_2의 종료/STOP 관리는 V27.4.1과 동일한 기존 공통관리이며, V27-1 staged stop은 섞지 않는다.
    # V27-1은 계속 V26 진입을 1:1 복제하는 별도 stop-only 비교군으로 유지한다.
    # 필터 차단 거래는 BLOCK_GHOST로 기존 공통관리 경로를 추적하고, 4.2 STOP은 STOP_GHOST로 180분 회복을 추적한다.
    # LIVE 주문에는 사용하지 않는 연구 Shadow 전용이다.
    research_pv2742_enabled: bool = True
    research_pv2742_max_entries_per_15m: int = 2
    research_pv2742_max_open_positions: int = 4
    research_pv2742_block_ghost_enabled: bool = True
    research_pv2742_stop_ghost_enabled: bool = True
    research_pv2742_post_track_minutes: int = 180

    # v4.3.79 P_V27_4_3 research-only:
    # V25 진입 + V27.4.2 Forward에서 TP2/BE 오차단을 줄이도록 범위만 좁힌 R2 진입필터.
    # 손절/BE/TP 관리는 기존 공통관리 그대로 유지한다. 차단은 BLOCK_GHOST, STOP은 180분 STOP_GHOST로 추적.
    research_pv2743_enabled: bool = True
    research_pv2743_max_entries_per_15m: int = 2
    research_pv2743_max_open_positions: int = 4
    research_pv2743_block_ghost_enabled: bool = True
    research_pv2743_stop_ghost_enabled: bool = True
    research_pv2743_post_track_minutes: int = 180

    # v4.3.79 P_V27_4_4 research-only:
    # 진입은 V25/V27.4.1과 동일. TP1(+1.5%) 50% 익절 뒤 BE(+0.15%) 도달 시
    # 남은 50%의 절반만 보호청산(원포지션 25%)하고 마지막 25%는 기존 공통관리로 계속 추적한다.
    # LIVE 주문에는 절대 적용하지 않는 Shadow 비교군이다.
    research_pv2744_enabled: bool = True
    research_pv2744_max_entries_per_15m: int = 2
    research_pv2744_max_open_positions: int = 4
    research_pv2744_be_close_fraction_of_remaining: float = 0.50

    # v4.3.81 P_V27_4_5 research-only:
    # 진입은 V25/V27.4.1과 동일하고, TP1 전 손절은 V27-1 staged/disaster 엔진을 그대로 사용한다.
    # TP1(+1.5%) 50% 익절 후 BE(+0.15%) 도달 시 원포지션 25%를 보호청산하고,
    # 마지막 25%만 최대 15분 Recovery: +1.0% 회복익절 / -2.5% 재난컷 / 15분 timeout.
    # LIVE 주문에는 절대 적용하지 않는 Shadow 비교군이다.
    research_pv2745_enabled: bool = True
    research_pv2745_max_entries_per_15m: int = 2
    research_pv2745_max_open_positions: int = 4
    research_pv2745_be_close_fraction_of_remaining: float = 0.50
    research_pv2745_recovery_target_pct: float = 1.00
    research_pv2745_recovery_hard_stop_pct: float = 2.50
    research_pv2745_recovery_timeout_minutes: float = 15.0

    # v4.3.82 P_V27_4_6 research-only:
    # V25 진입기회는 그대로 사용하고, 진입축만 재검증하기 위한 새 Entry Risk Guard 비교군.
    # 4.3을 덮어쓰지 않고 별도 Shadow로 병렬 관찰한다. 종료관리는 4.3/4.1과 같은 기존 공통관리.
    # 최신 Forward(PONS TP2 / POWR BE / ARC TP2) 오차단을 피하기 위해 A조건에 1h signed move >= 0 가드를 추가했다.
    # Block 조건 = A(EMA gap delta 약함 + RSI 상승 + 1h 양수) OR B(EMA9 약함 + ETH15 양수) OR 기존 C3 OR 기존 C5.
    # LIVE 주문에는 절대 적용하지 않는다.
    research_pv2746_enabled: bool = True
    research_pv2746_max_entries_per_15m: int = 2
    research_pv2746_max_open_positions: int = 4
    research_pv2746_block_ghost_enabled: bool = True
    research_pv2746_stop_ghost_enabled: bool = True
    research_pv2746_post_track_minutes: int = 180

    # 연구 Shadow도 LIVE 기본 동일종목 재진입 제한(모든 종료 후 90분)을 최소 기준으로 맞춘다.
    # STOP/LATE는 기존 V26 연구의 180분 cooldown이 더 강하므로 그대로 유지한다.
    research_live_same_symbol_cooldown_minutes: int = 90
    # V27-1: V26이 실제로 연 Shadow를 1:1 복제하고 손절 엔진만 변경한다.
    # 현재 V26 live-price 표본에서 3분/+0.5%가 5분 대기보다 손실 확대가 작았다.
    research_pv271_enabled: bool = True
    research_pv271_stage_fraction: float = 0.50
    research_pv271_rebound_from_signal_pct: float = 0.50
    research_pv271_wait_minutes: float = 3.0
    research_pv271_disaster_stop_pct: float = 3.0

    # v4.3.80 P_V27_1R research-only: P_V27_1의 진입/직접 -3% DISASTER/첫 50% staged stop은 그대로.
    # staged stop에서 REBOUND가 확인된 경우만 남은 50%를 즉시 닫지 않고 5분 Recovery Watch를 준다.
    # -2.60% 재난컷, -1.00% 회복확인, 확인 후 TP1(+1.5%)에서 남은 50% 전량 회수.
    # Recovery Confirm 뒤 최대 120분까지만 유지한다. LIVE에는 절대 적용하지 않는다.
    research_pv271r_enabled: bool = True
    research_pv271r_watch_minutes: float = 5.0
    research_pv271r_confirm_drawdown_pct: float = 1.00
    research_pv271r_hard_stop_pct: float = 2.60
    research_pv271r_max_hold_minutes: float = 120.0

    # v4.3.83 Parallel Entry/Stop Lab (research-only, LIVE order untouched)
    # 한 V25 confirmed opportunity에서 손절 5경로와 진입 7경로를 병렬 비교한다.
    # 손절 경로는 P_STOP_CONTROL의 portfolio schedule을 공통으로 사용해 진입표본을 동일하게 유지한다.
    # 진입 경로는 각 필터별 독립 portfolio schedule(15m 2-cap / 4 slots / LIVE90 / STOP180)을 사용한다.
    research_parallel_lab_enabled: bool = False
    research_parallel_max_entries_per_15m: int = 2
    research_parallel_max_open_positions: int = 4

    # v4.3.86 Final Forward 4-way research-only comparison. LIVE 주문에는 절대 사용하지 않는다.
    # CONTROL: V25 confirmed + 기존 보호 + 기존 TP(1.5/3/BE) + V27-1 stop.
    # CORE: SAFE entry + TP1.8 full + 4-stage early-loss overlay + V27-1 fallback.
    # MKT50/MKT100: CORE에 BTC/ETH 4h flat-regime guard를 50% size / 100% entry-off로 적용.
    research_final_forward_enabled: bool = True
    research_final_max_entries_per_15m: int = 2
    research_final_max_open_positions: int = 4
    research_final_tp_pct: float = 1.80
    research_final_market_flat_abs_4h_pct: float = 0.08
    research_final_10m_pnl_pct: float = -1.00
    research_final_10m_mfe_max_pct: float = 0.10
    research_final_15m_pnl_pct: float = -1.50
    research_final_25m_mfe_min_pct: float = 0.30
    research_final_25m_mfe_max_pct: float = 1.20
    research_final_25m_pnl_pct: float = -1.20
    research_final_30m_pnl_pct: float = -0.90
    research_final_30m_delta5_pct: float = -0.30

    # v4.3.88 Fixed Shadow (research-only; LIVE/order path untouched).
    # 최종 고정안: V25 -> SAFE(C/RN/RS) -> SAFE 특수레짐 비위험군 RELAX
    # -> MKT100 -> TP +1.8% full -> 현 Final stop overlay + V27-1 fallback
    # -> MFE +1.2% ARM 후 확정 5m 종가가 진입가 대비 -1.15% 이하이면 FULL 보호종료.
    research_fixed_shadow_enabled: bool = True
    research_fixed_safe_relax_window_short_hours: float = 14.0
    research_fixed_safe_relax_window_long_hours: float = 18.0
    research_fixed_safe_relax_edge_pctp: float = 8.0
    research_fixed_safe_relax_min_safe_n: int = 5
    research_fixed_safe_relax_min_pass_n: int = 5
    research_fixed_micro_market_avg15_max_pct: float = -0.05
    research_fixed_micro_market_ema_gap_max_pct: float = 1.20
    research_fixed_pp_arm_mfe_pct: float = 1.20
    research_fixed_pp_close_pct: float = -1.15

    # STOP EARLY50: 현재 TIER1보다 한 단계 빠른 50% Stage1 후보.
    research_parallel_early_min_age_minutes: float = 15.0
    research_parallel_early_max_age_minutes: float = 45.0
    research_parallel_early_drawdown_pct: float = 1.50
    research_parallel_early_mfe_max_pct: float = 0.50

    # STOP DISASTER50: 기존 Stage1 없이 -3% 직행하는 경로의 중간 50% 축소.
    research_parallel_disaster_stage_pct: float = 2.50

    # STOP LATE50: 오래 못 가는 저-MFE 실패를 기존 80m LATE보다 앞에서 50% 축소.
    research_parallel_late_min_age_minutes: float = 60.0
    research_parallel_late_pnl_pct: float = -1.00
    research_parallel_late_mfe_max_pct: float = 0.75
    research_parallel_late_confirmations: int = 2

    # V26 검증 화면/CSV는 V22/V25/V26 중심으로 보기 위해 P_V23/P_V24 신규 연구진입은 기본 OFF.
    # 코드 자체는 남겨 두어 필요하면 config.json에서 다시 켤 수 있다.
    research_v23_v24_enabled: bool = False

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
    # v4.3.35 P형 과확장 진입 방어: 8/14~8/16 P형 실제 진입 비교에서
    # AIO(-13.99)만 해당했던 "3연속 상승 + 저점대비 과확장 + 최근고점 코앞" 조합을 차단한다.
    p_overextended_continuation_min_rebound_pct: float = 10.00
    p_overextended_continuation_max_high_distance_pct: float = 0.50
    # P형 대손실 백업 가드. 고정손절 단독이 아니라 5분 구조붕괴와 함께 쓴다.
    p_catastrophic_guard_enabled: bool = True
    p_catastrophic_guard_min_age_minutes: float = 15.0
    p_catastrophic_guard_max_age_minutes: float = 45.0
    p_catastrophic_guard_drawdown_pct: float = 2.50
    # v4.3.39: 손실이 깊어질수록 필요한 구조 확인을 단계적으로 완화한다.
    # 정상 눌림을 고정손절로 자르지 않으면서 -6~-8 USDT 꼬리손실을 줄이기 위한 공통 기준.
    adaptive_loss_tier1_pct: float = 1.50
    adaptive_loss_tier2_pct: float = 2.00
    adaptive_loss_tier3_pct: float = 2.50

    # v4.3.41: 진입 직후 3~15분 급락 전용 가드.
    # 고정손절 단독이 아니라 -2.5% 이상 급락 + 확정 5분 구조 약화가 함께 있을 때만 종료한다.
    early_crash_guard_enabled: bool = True
    early_crash_min_age_minutes: float = 3.0
    early_crash_max_age_minutes: float = 15.0
    early_crash_drawdown_pct: float = 2.50

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

    # v4.3.55: TP1 미도달 장기 정체형 조기종료.
    # 75분 이상 지났는데 현재 수익 진행이 거의 없고 15분 모멘텀이 실제로 약해질 때만 종료한다.
    # 단순 시간종료가 아니라 약화 확인을 함께 요구해 느리게 올라가는 정상 거래는 최대한 보존한다.
    stalled_weak_exit_enabled: bool = True
    stalled_weak_exit_minutes: int = 75
    stalled_weak_max_current_pnl_pct: float = 0.25
    stalled_weak_min_weakness_score: int = 3
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
    # v4.3.42: HJ wick reversal 진입은 OFF. P형과 HJ A급 continuation은 유지한다.
    hj_wick_enabled: bool = False
    # v4.3.41: HJ continuation 전체는 계속 차단하되, A급 continuation만 제한적으로 허용한다.
    # A급 기준: 3연속 양봉 + 거래량비 >= 1.0 + higher-low + 1시간 상승 + EMA 정배열.
    hj_continuation_enabled: bool = False
    hj_a_continuation_min_volume_ratio: float = 1.00
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
        cfg = cls(**{k: v for k, v in raw.items() if k in allowed})
        # V27: 기존 config.json 값과 무관하게 JUNP/구형 연구 신규 Shadow는 OFF.
        cfg.junp_shadow_enabled = False
        cfg.research_junp_variants_enabled = False
        cfg.research_legacy_variants_enabled = False
        cfg.research_pv25_control_enabled = False
        # v4.3.83: 기존 연구 variant는 신규진입을 중지하고 열린 Shadow만 끝까지 관리한다.
        # 새 데이터는 V25 Master 기반 Parallel Entry/Stop Lab 한 축으로 통일한다.
        cfg.research_pv26_enabled = False
        cfg.research_pv272_enabled = False
        cfg.research_pv272_filter_only_enabled = False
        cfg.research_pv273_enabled = False
        cfg.research_pv273_filter_only_enabled = False
        cfg.research_pv274_enabled = False
        cfg.research_pv274_filter_only_enabled = False
        cfg.research_pv274_stop_ghost_enabled = True
        cfg.research_pv2741_enabled = False
        cfg.research_pv2741_stop_ghost_enabled = True
        cfg.research_pv2742_enabled = False
        cfg.research_pv2742_block_ghost_enabled = True
        cfg.research_pv2742_stop_ghost_enabled = True
        cfg.research_pv2743_enabled = False
        cfg.research_pv2743_block_ghost_enabled = True
        cfg.research_pv2743_stop_ghost_enabled = True
        cfg.research_pv2744_enabled = False
        cfg.research_pv2745_enabled = False
        cfg.research_pv2746_enabled = False
        cfg.research_pv2746_block_ghost_enabled = True
        cfg.research_pv2746_stop_ghost_enabled = True
        cfg.research_pv271r_enabled = False
        # v4.3.86: 과거 Parallel Lab은 신규진입 OFF. 열린 Shadow는 관리루프에서 끝까지 마무리한다.
        cfg.research_parallel_lab_enabled = False
        cfg.research_final_forward_enabled = True
        cfg.research_fixed_shadow_enabled = True
        return cfg


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
    # v4.3.46: P 최종판정의 실제 boolean을 CSV에도 남겨 원인 분석 가능하게 한다.
    "h1_up", "pullback_ok", "rebound", "not_chasing", "momentum_ok",
    "volume_ok", "volume_trend_ok", "movement_ok", "not_extreme",
    "ema_ordered", "ema9_rising", "higher_lows", "higher_highs",
    "p_ema_quality_ok", "p_entry_quality_ok",
    "p_near_high_weak_reentry", "p_faded_spike_reentry",
    "p_overextended_continuation",
    # v4.3.53: 준P Shadow 후보/결과 분석용. 실제 P 매매 데이터와는 분리된다.
    "p_score", "p_live_gain_ok", "junp_shadow_candidate", "junp_missing_condition",
    # v4.3.58: 연구용 full telemetry. 나중에 빠진 값 때문에 재분석이 막히지 않도록
    # 원본 상태 + 새 점수/Strength + 0.50/0.60 가설 + near-miss를 모두 저장한다.
    "live_price", "live_candle_gain_pct", "p_live_cap050_ok", "p_live_cap060_ok",
    "live_body_pct", "live_upper_wick_ratio", "live_lower_wick_ratio", "live_volume_ratio",
    "rsi_prev", "rsi_delta", "one_hour_signed_move_pct", "one_hour_move_pct",
    "ema9_slope_pct", "ema20_slope_pct", "ema9_ema20_gap_pct", "ema20_ema60_gap_pct",
    "new_p_score", "new_score_structure", "new_score_trend", "new_score_live",
    "new_score_rsi", "new_score_market_structure", "new_score_volume",
    "strength_rsi_ok", "strength_rebound_ok", "strength_live_ok", "strength_pass_count",
    "strength_2of3_ok", "p_current_shadow_candidate", "p_strength050_shadow_candidate",
    "p_strength060_shadow_candidate", "p_newscore060_shadow_candidate",
    "junp_old_shadow_candidate", "junp_new_shadow_candidate",
    "junp_old_missing_condition", "junp_new_missing_condition",
    "p_failed_check_count", "p_failed_checks", "research_nearmiss_candidate",
    # v4.3.59 P/JUNP v2 full telemetry
    "p_v2_score", "p_v2_trend_score", "p_v2_structure_score", "p_v2_momentum_score",
    "p_v2_soft_score", "p_v2_signal_pass_count", "p_v2_component_pass_count",
    "p_v2_candidate", "junp_v2_candidate", "junp_v2_missing_components",
    "p_v21_persistence_score", "p_v21_overheat_risk_count", "p_v21_overheat_flags",
    "p_v21_candidate", "junp_v21_candidate", "junp_v21_missing_reason",
    # v4.3.61 P/JUNP v2.2: 구조 미완성 + 복합 과진행을 분리 기록
    "p_v22_heat_count", "p_v22_heat_flags", "p_v22_late_extension",
    "p_v22_structure_weak_count", "p_v22_structure_weak_flags", "p_v22_structure_incomplete",
    "p_v22_candidate", "junp_v22_candidate", "junp_v22_missing_reason",
    # v4.3.63 P_V23: P_V22를 대조군으로 유지하고 두 STOP 선택 필터를 동시에 검증한다.
    "p_v23_weak_trend_block", "p_v23_rsi_spike_block", "p_v23_block_reason", "p_v23_candidate",
    # v4.3.64 P_V24 confirm/setup telemetry
    "p_setup_id", "p_v24_watch_candidate", "p_v24_immediate_candidate", "p_v24_confirm_state",
    # v4.3.66 P_V25 15m-candidate + confirmed-5m execution telemetry
    "p_v25_setup_id", "p_v25_confirm_state", "p_v25_5m_bullish",
    "p_v25_prev_high_break", "p_v25_closed_5m_price",
    # v4.3.67 P_V26 loss-guard / ghost telemetry
    "p_v26_confirm_state", "p_v26_block_reason", "p_v26_a_filter",
    "p_v26_cooldown_active", "p_v26_late_fail_streak", "p_v26_ghost_type",
    "p_v26_5m_bullish", "p_v26_prev_high_break", "p_v26_closed_5m_price",
    # v4.3.69 V27 entry-only / V27-1 stop-only telemetry
    "p_v27_confirm_state", "p_v27_block_reason", "p_v27_stall_block",
    "p_v27_a1_high_conf_block", "p_v27_ghost_type", "p_v27_live_entry_price",
    "p_v27_closed_5m_price", "p_v271_confirm_state", "p_v271_source_shadow_id",
    "p_v271_stop_stage_active", "p_v271_stop_signal_price", "p_v271_stop_reason",
    # v4.3.80 V27-1R staged-rebound recovery telemetry
    "p_v271r_confirm_state", "p_v271r_source_shadow_id", "p_v271r_recovery_watch_active",
    "p_v271r_recovery_confirmed", "p_v271r_recovery_reason", "p_v271r_recovery_started_at",
    # v4.3.71 V27 FilterOnly: V26 실제진입을 모체로 필터 자체 효과만 1:1 비교.
    "p_v27fo_confirm_state", "p_v27fo_block_reason", "p_v27fo_source_shadow_id",
    "p_v27fo_filter_pass", "p_v27fo_stall_block", "p_v27fo_a1_high_conf_block",
    # v4.3.72 V27-2 / FilterOnly: 좁은 두 손실패턴만 차단.
    "p_v272_confirm_state", "p_v272_block_reason", "p_v272_high_score_rsi_lag_block",
    "p_v272_weak_structure_block", "p_v272_ghost_type", "p_v272_live_entry_price",
    "p_v272_closed_5m_price",
    "p_v272fo_confirm_state", "p_v272fo_block_reason", "p_v272fo_source_shadow_id",
    "p_v272fo_filter_pass", "p_v272fo_high_score_rsi_lag_block", "p_v272fo_weak_structure_block",
    # v4.3.73 V27-3 / FilterOnly: impulse + RSI 급등 대비 EMA 분리 부족 패턴 전진검증.
    "p_v273_confirm_state", "p_v273_block_reason", "p_v273_impulse_separation_lag_block",
    "p_v273_ghost_type", "p_v273_live_entry_price", "p_v273_closed_5m_price",
    "p_v273fo_confirm_state", "p_v273fo_block_reason", "p_v273fo_source_shadow_id",
    "p_v273fo_filter_pass", "p_v273fo_impulse_separation_lag_block",
    # v4.3.74 V27-4: 4-of-4 weak-structure + entry/stop 180m ghost telemetry.
    "p_v274_confirm_state", "p_v274_block_reason", "p_v274_weak_structure_block",
    "p_v274_weak_count", "p_v274_weak_flags", "p_v274_ghost_type",
    "p_v274_source_shadow_id", "p_v274_ghost_classification",
    "p_v274_live_entry_price", "p_v274_closed_5m_price",
    "p_v274fo_confirm_state", "p_v274fo_block_reason", "p_v274fo_source_shadow_id",
    "p_v274fo_filter_pass", "p_v274fo_weak_structure_block",
    "p_v274fo_weak_count", "p_v274fo_weak_flags",
    # v4.3.75+ V27-4.1: V25 no-hard-block control + weak-watch telemetry only.
    "p_v2741_confirm_state", "p_v2741_weak_watch", "p_v2741_weak_count", "p_v2741_weak_flags",
    "p_v2741_ghost_type", "p_v2741_source_shadow_id", "p_v2741_ghost_classification",
    "p_v2741_live_entry_price", "p_v2741_closed_5m_price",
    # v4.3.78 V27-4.2: adjusted entry-filter telemetry. Legacy 35-pattern telemetry is removed.
    "p_v2742_confirm_state", "p_v2742_filter_block", "p_v2742_filter_reasons",
    "p_v2742_a9_match", "p_v2742_a9_rule_ids", "p_v2742_b25_v2",
    "p_v2742_c1_adj", "p_v2742_c2_v2", "p_v2742_c3_adj", "p_v2742_c5_adj",
    "p_v2742_c4_enabled", "p_v2742_c6_enabled",
    "p_v2742_filter_version", "p_v2742_filter_frozen",
    "p_v2742_live_entry_price", "p_v2742_closed_5m_price",
    "p_v2742_is_block_ghost", "p_v2742_ghost_type", "p_v2742_source_shadow_id",
    # v4.3.79 V27-4.3: R2 range-refined entry filter telemetry.
    "p_v2743_confirm_state", "p_v2743_filter_block", "p_v2743_filter_reasons",
    "p_v2743_a9_match", "p_v2743_a9_rule_ids", "p_v2743_b25_v2",
    "p_v2743_c1_adj", "p_v2743_c2_v2", "p_v2743_c3_adj", "p_v2743_c5_adj",
    "p_v2743_filter_version", "p_v2743_filter_frozen",
    "p_v2743_live_entry_price", "p_v2743_closed_5m_price",
    "p_v2743_is_block_ghost", "p_v2743_ghost_type", "p_v2743_source_shadow_id", "p_v2743_ghost_classification",
    # v4.3.79 V27-4.4: V25 entry + partial BE telemetry.
    "p_v2744_confirm_state", "p_v2744_live_entry_price", "p_v2744_closed_5m_price",
    "p_v2744_be_partial_done", "p_v2744_be_partial_price",
    "p_v2744_be_partial_original_fraction", "p_v2744_final_original_fraction",
    "p_v2744_strategy_gross_pct",
    # v4.3.81 V27-4.5: V25 entry + V27-1 stop + 15m BE Recovery telemetry.
    "p_v2745_confirm_state", "p_v2745_live_entry_price", "p_v2745_closed_5m_price",
    "p_v2745_be_partial_done", "p_v2745_be_partial_price",
    "p_v2745_be_partial_original_fraction", "p_v2745_final_original_fraction",
    "p_v2745_be_recovery_active", "p_v2745_be_recovery_started_at",
    "p_v2745_be_recovery_finish_reason", "p_v2745_be_recovery_exit_pct",
    "p_v2745_strategy_gross_pct",
    # v4.3.82 V27-4.6: new Entry Risk Guard telemetry.
    "p_v2746_confirm_state", "p_v2746_filter_block", "p_v2746_filter_reasons",
    "p_v2746_a_gap_rsi_h1", "p_v2746_b_ema9_eth", "p_v2746_c3_adj", "p_v2746_c5_adj",
    "p_v2746_filter_version", "p_v2746_filter_frozen",
    "p_v2746_live_entry_price", "p_v2746_closed_5m_price",
    "p_v2746_is_block_ghost", "p_v2746_ghost_type", "p_v2746_source_shadow_id", "p_v2746_ghost_classification",
    # v4.3.70 telemetry-only: BTC/ETH 시장상태. 진입/STOP/TP 판정에는 절대 사용하지 않는다.
    "market_snapshot_time_kst", "market_telemetry_status",
    "btc_5m_change_pct", "btc_15m_change_pct", "btc_30m_change_pct",
    "btc_1h_change_pct", "btc_4h_change_pct", "btc_1h_high_pullback_pct",
    "eth_5m_change_pct", "eth_15m_change_pct", "eth_30m_change_pct",
    "eth_1h_change_pct", "eth_4h_change_pct", "eth_1h_high_pullback_pct",
    "btc_eth_both_down_15m",
    # v4.3.62 telemetry only: 진입 직전 3개 확정 15분봉의 힘이 가속/둔화되는지 추적.
    # P_V22/JUNP_V22 진입 판정에는 사용하지 않는다.
    "prev1_candle_gain_pct", "prev2_candle_gain_pct", "prev3_candle_gain_pct",
    "rsi_prev2", "rsi_prev3", "rsi_change_prev1", "rsi_change_prev2",
    "ema9_slope_prev1_pct", "ema9_slope_prev2_pct",
    "ema20_slope_prev1_pct", "ema20_slope_prev2_pct",
    "ema9_ema20_gap_prev1_pct", "ema9_ema20_gap_prev2_pct", "ema9_ema20_gap_prev3_pct",
    "close_change_prev1_pct", "close_change_prev2_pct", "close_change_prev3_pct",
    "high_change_prev1_pct", "high_change_prev2_pct", "high_change_prev3_pct",
    "low_change_prev1_pct", "low_change_prev2_pct", "low_change_prev3_pct",
    "ema9_slope_delta_pct", "ema20_slope_delta_pct", "ema_gap_delta_pct",
    "rsi_rollover_3bar", "price_gain_decelerating_3bar", "ema_force_decelerating",
    "research_variants",
    "live_quality_ok", "live_quality_reason", "live_quality_bullish", "live_quality_body_ok",
    "live_quality_wick_ok", "live_quality_prev_bearish", "live_quality_prev_body_recovery",
    "live_quality_recovery_ok", "live_quality_5m_bullish",
    # v4.3.83 parallel entry/stop lab telemetry
    "parallel_lab_group", "parallel_lab_rule", "parallel_lab_filter_block",
    "parallel_lab_filter_flags", "parallel_lab_source_setup_id", "parallel_lab_master_variant",
    "parallel_lab_stop_trigger", "parallel_lab_stop_signal_pct", "parallel_lab_late_streak",
    # v4.3.86 final forward telemetry
    "final_fwd_variant", "final_source_setup_id", "final_safe_block", "final_safe_flags",
    "final_market_guard", "final_market_mode", "final_size_mult",
    "final_stop_stage", "final_remaining_frac", "final_realized_weighted_pct",
    "final_strategy_gross_pct",
    # v4.3.88 fixed-shadow telemetry
    "fixed_safe_micro_risk", "fixed_safe_regime_relax_on", "fixed_safe_relaxed",
    "fixed_safe_effective_block", "fixed_safe_regime_edge_pctp",
    "fixed_safe_14h_safe_n", "fixed_safe_14h_safe_pos_pct", "fixed_safe_14h_pass_n", "fixed_safe_14h_pass_pos_pct", "fixed_safe_14h_diff_pctp",
    "fixed_safe_18h_safe_n", "fixed_safe_18h_safe_pos_pct", "fixed_safe_18h_pass_n", "fixed_safe_18h_pass_pos_pct", "fixed_safe_18h_diff_pctp",
    "fixed_pp_armed", "fixed_pp_arm_mfe_pct", "fixed_pp_last_close_pct", "fixed_pp_triggered",
    "shadow_id", "research_variant", "shadow_entry_time", "mfe_pct", "mae_pct",
    "shadow_age_min", "shadow_exit_pct", "shadow_stop_type",
]

def ensure_scan_rejected_csv() -> None:
    """봇 시작 즉시 CSV를 준비한다. 기존 기록은 보존하면서 신규 컬럼만 안전하게 확장한다."""
    SCAN_REJECTED_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not SCAN_REJECTED_CSV_PATH.exists() or SCAN_REJECTED_CSV_PATH.stat().st_size == 0:
        with SCAN_REJECTED_CSV_PATH.open("w", encoding="utf-8-sig", newline="") as fh:
            csv.DictWriter(fh, fieldnames=SCAN_REJECTED_FIELDS).writeheader()
            fh.flush()
            os.fsync(fh.fileno())
        return

    # v4.3.46: 예전 헤더의 CSV에 신규 컬럼을 그대로 append하면 열이 어긋난다.
    # 헤더가 다를 때만 기존 행을 읽어 신규 헤더로 1회 마이그레이션한다.
    try:
        with SCAN_REJECTED_CSV_PATH.open("r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            old_fields = list(reader.fieldnames or [])
            if old_fields == SCAN_REJECTED_FIELDS:
                return
            rows = list(reader)
        tmp = SCAN_REJECTED_CSV_PATH.with_suffix(".csv.tmp")
        with tmp.open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=SCAN_REJECTED_FIELDS, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow({key: row.get(key, "") for key in SCAN_REJECTED_FIELDS})
            fh.flush()
            os.fsync(fh.fileno())
        tmp.replace(SCAN_REJECTED_CSV_PATH)
    except Exception as exc:
        log_event("", "SCAN_CSV_MIGRATE_ERROR", mode="paper", details=str(exc))

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
    extra: dict[str, Any] | None = None,
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
    if extra:
        for key, value in extra.items():
            if key in row:
                row[key] = value
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
    """SQLite connection with lock tolerance.

    Trading/entry logic is untouched.  This only prevents short-lived SQLite
    write contention from terminating the bot process.
    """
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


_DB_FALLBACK_LOG = "/tmp/bybit_swing_fallback.log"


def _db_fallback_log(tag: str, message: str) -> None:
    """Best-effort file fallback; logging failure must never stop the bot."""
    try:
        stamp = datetime.now(timezone.utc).isoformat()
        with open(_DB_FALLBACK_LOG, "a", encoding="utf-8") as f:
            f.write(f"{stamp}\t{tag}\t{message}\n")
    except Exception:
        pass


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
        CREATE TABLE IF NOT EXISTS be_shadow_reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id TEXT NOT NULL UNIQUE,
            symbol TEXT NOT NULL,
            strategy TEXT,
            opened_at TEXT NOT NULL,
            shadow_started_at TEXT NOT NULL,
            entry_price REAL NOT NULL,
            be_exit_price REAL NOT NULL,
            tp2_price REAL NOT NULL,
            highest_price REAL,
            lowest_price REAL,
            last_price REAL,
            last_checked_at TEXT,
            last_structure_bucket TEXT,
            result TEXT,
            result_ts TEXT,
            result_price REAL,
            result_details TEXT,
            completed INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS junp_shadow_reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shadow_id TEXT NOT NULL UNIQUE,
            symbol TEXT NOT NULL,
            opened_at TEXT NOT NULL,
            entry_ts_ms INTEGER NOT NULL,
            entry_price REAL NOT NULL,
            p_score REAL NOT NULL,
            missing_condition TEXT NOT NULL,
            tp1_done INTEGER NOT NULL DEFAULT 0,
            tp1_ts TEXT,
            tp1_price REAL,
            tp2_price REAL NOT NULL,
            be_price REAL NOT NULL,
            highest_price REAL,
            lowest_price REAL,
            last_price REAL,
            last_checked_at TEXT,
            last_5m_bucket TEXT,
            last_15m_bucket TEXT,
            result TEXT,
            result_ts TEXT,
            result_price REAL,
            result_details TEXT,
            completed INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS research_shadow_reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shadow_id TEXT NOT NULL UNIQUE,
            variant TEXT NOT NULL,
            symbol TEXT NOT NULL,
            opened_at TEXT NOT NULL,
            entry_ts_ms INTEGER NOT NULL,
            entry_price REAL NOT NULL,
            old_p_score REAL,
            new_p_score REAL,
            missing_condition TEXT,
            snapshot_json TEXT,
            tp1_done INTEGER NOT NULL DEFAULT 0,
            tp1_ts TEXT,
            tp1_price REAL,
            tp2_price REAL NOT NULL,
            be_price REAL NOT NULL,
            highest_price REAL,
            lowest_price REAL,
            last_price REAL,
            last_checked_at TEXT,
            last_5m_bucket TEXT,
            last_15m_bucket TEXT,
            result TEXT,
            result_ts TEXT,
            result_price REAL,
            mfe_pct REAL DEFAULT 0,
            mae_pct REAL DEFAULT 0,
            result_details TEXT,
            completed INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS research_pv24_setups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            setup_id TEXT NOT NULL UNIQUE, symbol TEXT NOT NULL,
            first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL,
            trigger_price REAL NOT NULL, lowest_price REAL NOT NULL, last_price REAL NOT NULL,
            block_reason TEXT, snapshot_json TEXT, status TEXT NOT NULL DEFAULT 'WATCH',
            confirmed_at TEXT, confirmed_price REAL, expires_at TEXT, note TEXT
        );
        CREATE TABLE IF NOT EXISTS research_pv25_setups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            setup_id TEXT NOT NULL UNIQUE, symbol TEXT NOT NULL,
            first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL,
            trigger_price REAL NOT NULL, lowest_price REAL NOT NULL, last_price REAL NOT NULL,
            snapshot_json TEXT, status TEXT NOT NULL DEFAULT 'WATCH',
            last_5m_bucket TEXT, confirmed_at TEXT, confirmed_price REAL,
            expires_at TEXT, note TEXT
        );
        CREATE TABLE IF NOT EXISTS research_be_shadow_reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shadow_id TEXT NOT NULL UNIQUE, variant TEXT NOT NULL, symbol TEXT NOT NULL,
            opened_at TEXT NOT NULL, shadow_started_at TEXT NOT NULL,
            entry_price REAL NOT NULL, be_exit_price REAL NOT NULL, tp2_price REAL NOT NULL,
            highest_price REAL, lowest_price REAL, last_price REAL, last_checked_at TEXT,
            result TEXT, result_ts TEXT, result_price REAL, result_details TEXT,
            completed INTEGER NOT NULL DEFAULT 0
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
        # v4.3.31: 45분 이후 추세실패 판단에서 사용하는 진입시각(ms) 컬럼 보장.
        # 기존 DB에는 이 컬럼이 없을 수 있어, 누락 시 sqlite.Row 접근에서
        # IndexError: No item with that key 가 발생할 수 있었다.
        _ensure_column(conn, "bot_positions", "entry_ts_ms", "INTEGER")
        _ensure_column(conn, "bot_events", "strategy", "TEXT")
        _ensure_column(conn, "bot_events", "realized_pnl", "REAL DEFAULT 0")
        _ensure_column(conn, "bot_events", "trade_id", "TEXT")
        _ensure_column(conn, "research_shadow_reviews", "setup_id", "TEXT")
        _ensure_column(conn, "research_shadow_reviews", "v26_late_fail_streak", "INTEGER DEFAULT 0")


def state_get(key: str, default: str = "") -> str:
    with db() as conn:
        row = conn.execute("SELECT value FROM bot_state WHERE key=?", (key,)).fetchone()
    return str(row[0]) if row else default


def state_set(key: str, value: str) -> None:
    last_exc: Exception | None = None
    for attempt in range(5):
        try:
            with db() as conn:
                conn.execute(
                    "INSERT INTO bot_state(key,value) VALUES(?,?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (key, value),
                )
            return
        except sqlite3.OperationalError as exc:
            # A transient SQLite writer lock must not terminate the bot loop.
            if "locked" not in str(exc).lower():
                raise
            last_exc = exc
            if attempt < 4:
                time.sleep(0.25 * (attempt + 1))

    _db_fallback_log(
        "STATE_SET_DB_LOCK",
        f"key={key!r} value={value!r} error={last_exc!r}",
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
    # DBLight1: SCAN telemetry is already preserved in scan_rejected.csv.
    # Avoid duplicating the large SCAN_OK/SCAN_WAIT details JSON in bot_events.
    # Trading / Shadow / entry / exit logic is unchanged.
    if event in {"SCAN_OK", "SCAN_WAIT"}:
        return

    ts = utc_now()
    db_error: Exception | None = None
    written = False

    for attempt in range(5):
        try:
            with db() as conn:
                conn.execute(
                    "INSERT INTO bot_events(ts,symbol,event,price,qty,mode,details,strategy,realized_pnl,trade_id) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (ts, symbol, event, float(price), float(qty), mode, details, strategy, float(realized_pnl), trade_id),
                )
            written = True
            break
        except sqlite3.OperationalError as exc:
            db_error = exc
            if "locked" in str(exc).lower() and attempt < 4:
                time.sleep(0.25 * (attempt + 1))
                continue
            break
        except Exception as exc:
            db_error = exc
            break

    if not written:
        _db_fallback_log(
            "LOG_EVENT_DB_FAIL",
            (
                f"symbol={symbol!r} event={event!r} price={float(price)!r} "
                f"qty={float(qty)!r} mode={mode!r} strategy={strategy!r} "
                f"trade_id={trade_id!r} error={db_error!r} details={details!r}"
            ),
        )

    # Telegram notification remains best-effort and independent of DB logging.
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
    quality_details = dict(quality_details or {})

    last = raw15.iloc[-2]
    before = raw15.iloc[-3]
    live_price = float(live.close)
    live_gain = (live_price / float(live.open) - 1) * 100 if float(live.open) > 0 else 0.0
    # v4.3.51: P형 live candle 추격 상한.
    # 0.20% 이상 반등 확인은 기존 live_candle_quality_ok()가 유지하고,
    # 0.50%를 넘겨 이미 진행된 봉에서는 P형 신규진입만 보류한다.
    p_live_gain_ok = bool(live_gain <= float(cfg.p_live_max_gain_pct))
    live_volume_ratio = float(live.volume / live.vol_avg) if pd.notna(live.vol_avg) and live.vol_avg > 0 else 0.0

    # v4.3.58 연구용 점수: 기존 '형태 점수'는 보존하고, 실제 추세 생명력/추진력에
    # 더 큰 비중을 둔 새 점수를 병행 기록한다. LIVE 주문 판정에는 아직 사용하지 않는다.
    one_hour_signed_move = (float(row.close) / float(m15.iloc[-5].close) - 1) * 100 if float(m15.iloc[-5].close) > 0 else 0.0
    rsi_now = float(row.rsi)
    rsi_prev = float(prev.rsi)
    rsi_delta = rsi_now - rsi_prev
    ema9_slope_pct = (float(row.ema9) / float(prev.ema9) - 1) * 100 if float(prev.ema9) > 0 else 0.0
    ema20_slope_pct = (float(row.ema20) / float(prev.ema20) - 1) * 100 if float(prev.ema20) > 0 else 0.0
    ema9_ema20_gap_pct = (float(row.ema9) / float(row.ema20) - 1) * 100 if float(row.ema20) > 0 else 0.0
    ema20_ema60_gap_pct = (float(row.ema20) / float(row.ema60) - 1) * 100 if float(row.ema60) > 0 else 0.0

    # v4.3.62 telemetry only: latest confirmed bar(row)와 그 직전 2개 확정봉의
    # candle/RSI/EMA/고저점 변화를 저장한다. 신호 판정에는 사용하지 않는다.
    prev3 = m15.iloc[-4]

    def _pct_change(now_value: float, prev_value: float) -> float:
        return (float(now_value) / float(prev_value) - 1) * 100 if float(prev_value) != 0 else 0.0

    def _bar_gain(bar) -> float:
        return _pct_change(float(bar.close), float(bar.open)) if float(bar.open) != 0 else 0.0

    prev1_candle_gain_pct = _bar_gain(row)
    prev2_candle_gain_pct = _bar_gain(prev)
    prev3_candle_gain_pct = _bar_gain(prevprev)

    rsi_prev2 = float(prevprev.rsi)
    rsi_prev3 = float(prev3.rsi)
    rsi_change_prev1 = float(prev.rsi) - float(prevprev.rsi)
    rsi_change_prev2 = float(prevprev.rsi) - float(prev3.rsi)

    ema9_slope_prev1_pct = _pct_change(float(prev.ema9), float(prevprev.ema9))
    ema9_slope_prev2_pct = _pct_change(float(prevprev.ema9), float(prev3.ema9))
    ema20_slope_prev1_pct = _pct_change(float(prev.ema20), float(prevprev.ema20))
    ema20_slope_prev2_pct = _pct_change(float(prevprev.ema20), float(prev3.ema20))

    ema9_ema20_gap_prev1_pct = _pct_change(float(prev.ema9), float(prev.ema20))
    ema9_ema20_gap_prev2_pct = _pct_change(float(prevprev.ema9), float(prevprev.ema20))
    ema9_ema20_gap_prev3_pct = _pct_change(float(prev3.ema9), float(prev3.ema20))

    close_change_prev1_pct = _pct_change(float(row.close), float(prev.close))
    close_change_prev2_pct = _pct_change(float(prev.close), float(prevprev.close))
    close_change_prev3_pct = _pct_change(float(prevprev.close), float(prev3.close))
    high_change_prev1_pct = _pct_change(float(row.high), float(prev.high))
    high_change_prev2_pct = _pct_change(float(prev.high), float(prevprev.high))
    high_change_prev3_pct = _pct_change(float(prevprev.high), float(prev3.high))
    low_change_prev1_pct = _pct_change(float(row.low), float(prev.low))
    low_change_prev2_pct = _pct_change(float(prev.low), float(prevprev.low))
    low_change_prev3_pct = _pct_change(float(prevprev.low), float(prev3.low))

    ema9_slope_delta_pct = ema9_slope_pct - ema9_slope_prev1_pct
    ema20_slope_delta_pct = ema20_slope_pct - ema20_slope_prev1_pct
    ema_gap_delta_pct = ema9_ema20_gap_pct - ema9_ema20_gap_prev1_pct
    rsi_rollover_3bar = bool(rsi_change_prev2 > 0 and rsi_change_prev1 > 0 and rsi_delta < 0)
    price_gain_decelerating_3bar = bool(
        close_change_prev3_pct > close_change_prev2_pct > close_change_prev1_pct
    )
    ema_force_decelerating = bool(
        ema9_slope_pct < ema9_slope_prev1_pct and ema20_slope_pct < ema20_slope_prev1_pct
    )

    new_score_structure = (15 if h1_up else 0) + (10 if pullback_ok else 0) + (10 if rebound else 0) + (10 if p_ema_quality_ok else 0)
    new_score_trend = min(18.0, max(0.0, one_hour_signed_move) * 6.0) + (6 if ema9_rising else 0)
    if 0.32 <= live_gain <= 0.60:
        new_score_live = 14.0
    elif 0.20 <= live_gain < 0.32:
        new_score_live = 7.0
    elif 0.60 < live_gain <= 0.90:
        new_score_live = 6.0
    else:
        new_score_live = 0.0
    if 62.0 <= rsi_now <= 72.0 and rsi_delta >= 0:
        new_score_rsi = 12.0
    elif 58.0 <= rsi_now < 62.0 and rsi_delta >= 0:
        new_score_rsi = 6.0
    elif 72.0 < rsi_now <= 75.0 and rsi_delta >= 0:
        new_score_rsi = 8.0
    else:
        new_score_rsi = 2.0 if rsi_delta > 0 else 0.0
    new_score_market_structure = (6 if higher_lows else 0) + (6 if higher_highs else 0) + min(12.0, max(0.0, rebound_from_low) * 2.4)
    new_score_volume = min(8.0, max(0.0, volume_ratio) * 5.0) + (4 if volume_trend_ok else 0)
    new_p_score = float(new_score_structure + new_score_trend + new_score_live + new_score_rsi + new_score_market_structure + new_score_volume)

    strength_rsi_ok = bool(rsi_now >= float(cfg.research_strength_rsi_min))
    strength_rebound_ok = bool(rebound_from_low >= float(cfg.research_strength_rebound_min_pct))
    strength_live_ok = bool(live_gain >= float(cfg.research_strength_live_gain_min_pct))
    strength_pass_count = int(strength_rsi_ok) + int(strength_rebound_ok) + int(strength_live_ok)
    strength_2of3_ok = bool(strength_pass_count >= 2)
    p_live_cap050_ok = bool(live_gain <= float(cfg.research_live_cap_050_pct))
    p_live_cap060_ok = bool(live_gain <= float(cfg.research_live_cap_060_pct))

    # v4.3.59 P v2: “모양”보다 추세가 실제로 살아 있는지를 중심으로 별도 점수화한다.
    # 각 항목을 연속 점수로 만들어 RSI/volume 한 조건의 경계값 때문에 좋은 후보가 잘리지 않게 한다.
    p_v2_trend_score = (
        min(15.0, max(0.0, one_hour_signed_move) * 6.0)
        + min(10.0, max(0.0, ema9_slope_pct) * 25.0)
        + min(10.0, max(0.0, ema20_slope_pct) * 30.0)
        + min(10.0, max(0.0, ema9_ema20_gap_pct) * 6.67)
    )
    p_v2_structure_score = (
        min(15.0, max(0.0, rebound_from_low) * 2.0)
        + (5.0 if higher_highs else 0.0)
        + (5.0 if higher_lows else 0.0)
    )
    if 62.0 <= rsi_now <= 72.0:
        p_v2_rsi_level_score = 12.0
    elif 58.0 <= rsi_now < 62.0:
        p_v2_rsi_level_score = 8.0
    elif 54.0 <= rsi_now < 58.0:
        p_v2_rsi_level_score = 4.0
    elif 72.0 < rsi_now <= 75.0:
        p_v2_rsi_level_score = 8.0
    elif rsi_now > 75.0:
        p_v2_rsi_level_score = 3.0
    else:
        p_v2_rsi_level_score = 0.0
    p_v2_rsi_delta_score = max(-3.0, min(5.0, rsi_delta * 1.5))
    p_v2_live_score = 5.0 if 0.20 <= live_gain <= 0.60 else (2.0 if -0.10 <= live_gain <= 0.90 else 0.0)
    p_v2_momentum_score = p_v2_rsi_level_score + p_v2_rsi_delta_score + p_v2_live_score
    p_v2_soft_score = (
        (3.0 if pullback_ok else 0.0)
        + (3.0 if rebound else 0.0)
        + (2.0 if momentum_ok else 0.0)
        + (2.0 if volume_ok else 0.0)
        + (2.0 if volume_trend_ok else 0.0)
        + (3.0 if quality_ok else 0.0)
        + (3.0 if not_chasing else 0.0)
    )
    p_v2_score = float(p_v2_trend_score + p_v2_structure_score + p_v2_momentum_score + p_v2_soft_score)
    p_v2_signal_checks = {
        "1h_positive_force": one_hour_signed_move >= 0.75,
        "ema9_rising_force": ema9_slope_pct >= 0.12,
        "ema20_rising_force": ema20_slope_pct >= 0.08,
        "ema_gap_alive": ema9_ema20_gap_pct >= 0.20,
        "rebound_force": rebound_from_low >= 3.50,
        "rsi_level": rsi_now >= 56.0,
        "rsi_not_fading": rsi_delta >= -1.0,
    }
    p_v2_signal_pass_count = sum(int(v) for v in p_v2_signal_checks.values())
    p_v2_component_checks = {
        "trend": p_v2_trend_score >= 20.0,
        "structure": p_v2_structure_score >= 10.0,
        "momentum": p_v2_momentum_score >= 8.0,
        "soft_quality": p_v2_soft_score >= 10.0,
    }
    p_v2_component_pass_count = sum(int(v) for v in p_v2_component_checks.values())

    # v4.3.60 P v2.1 research layer:
    # 누적 Shadow에서 TP2군은 trend+structure가 더 강했고, momentum/RSI 급등은 STOP과 잘 구분되지 않았다.
    # 그래서 기존 v2 점수는 보존하되, 진입판정에는 "지속력(persistence)" 최소치와 복합 과열위험을 추가한다.
    p_v21_persistence_score = float(p_v2_trend_score + p_v2_structure_score)
    p_v21_overheat_flags_list = []
    if one_hour_signed_move > 5.0:
        p_v21_overheat_flags_list.append("1h_gt_5")
    if live_gain > 0.40:
        p_v21_overheat_flags_list.append("live_gt_040")
    if rsi_now > 72.0 and rsi_delta > 3.0:
        p_v21_overheat_flags_list.append("rsi_hot_accel")
    if rebound_from_low > 15.0 and one_hour_signed_move > 4.0:
        p_v21_overheat_flags_list.append("rebound_extended")
    p_v21_overheat_risk_count = len(p_v21_overheat_flags_list)
    p_v21_overheat_flags = ",".join(p_v21_overheat_flags_list)

    # v4.3.61 P v2.2 연구용 보정.
    # 단일 RSI/EMA 컷이 아니라, 여러 과진행 신호가 동시에 겹칠 때만 끝물로 판정한다.
    p_v22_heat_flags_list = []
    if rsi_now >= 75.0:
        p_v22_heat_flags_list.append("rsi_ge_75")
    if rebound_from_low >= 12.0:
        p_v22_heat_flags_list.append("rebound_ge_12")
    if ema9_ema20_gap_pct >= 2.0:
        p_v22_heat_flags_list.append("ema_gap_ge_2")
    if one_hour_signed_move >= 3.5:
        p_v22_heat_flags_list.append("1h_ge_3p5")
    if live_gain >= 0.45:
        p_v22_heat_flags_list.append("live_ge_045")
    p_v22_heat_count = len(p_v22_heat_flags_list)
    p_v22_heat_flags = ",".join(p_v22_heat_flags_list)
    p_v22_late_extension = bool(
        p_v22_heat_count > int(cfg.research_pv22_max_heat_count)
        or (rsi_now >= 78.0 and rsi_delta >= 4.0 and p_v22_heat_count >= 2)
    )

    # 구조 미완성도 한 숫자로 자르지 않는다. gap/EMA slope/구조점수/1h 힘이 여러 개 동시에 약할 때만 차단한다.
    p_v22_structure_weak_flags_list = []
    if ema9_ema20_gap_pct < 0.05:
        p_v22_structure_weak_flags_list.append("ema_gap_lt_005")
    if ema20_slope_pct < 0.06:
        p_v22_structure_weak_flags_list.append("ema20_slope_lt_006")
    if ema9_slope_pct < 0.08:
        p_v22_structure_weak_flags_list.append("ema9_slope_lt_008")
    if p_v2_structure_score < 12.0:
        p_v22_structure_weak_flags_list.append("structure_lt_12")
    if one_hour_signed_move < 0.40:
        p_v22_structure_weak_flags_list.append("1h_lt_040")
    p_v22_structure_weak_count = len(p_v22_structure_weak_flags_list)
    p_v22_structure_weak_flags = ",".join(p_v22_structure_weak_flags_list)
    p_v22_structure_incomplete = bool(
        p_v22_structure_weak_count > int(cfg.research_pv22_max_structure_weak_count)
        or (ema9_ema20_gap_pct <= 0.0 and (ema20_slope_pct <= 0.0 or p_v2_structure_score < 14.0))
    )

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
    # v4.3.42: wick 패턴 자체는 진단용으로 계산하되 실제 진입 신호에서는 비활성화한다.
    hj_wick_reversal_active = bool(cfg.hj_wick_enabled and wick_reversal)
    hj_wick_volume_ok = bool(hj_wick_reversal_active and live_volume_ratio >= 0.35)

    # v4.3.41 A급 continuation 강화:
    # 전체 continuation은 계속 OFF. 거래량/higher-low에 더해
    # 1시간 상승 흐름과 현재 EMA 정배열이 모두 확인되는 경우만 제한적으로 허용한다.
    hj_a_continuation = bool(
        continuation_three_bulls
        and live_volume_ratio >= cfg.hj_a_continuation_min_volume_ratio
        and higher_lows
        and h1_up
        and live_ema_ordered
    )
    hj_continuation_volume_ok = bool(
        continuation_three_bulls
        and (cfg.hj_continuation_enabled or hj_a_continuation)
        and live_volume_ratio >= 0.60
    )
    hj_volume_ok = bool(hj_wick_volume_ok or hj_continuation_volume_ok)
    hj_momentum_ok = bool(48 <= live_rsi <= cfg.hj_max_rsi)
    hj_pattern_ok = bool(
        hj_wick_reversal_active
        or (cfg.hj_continuation_enabled and continuation_three_bulls)
        or hj_a_continuation
    )
    hj_continuation_disabled = bool(
        continuation_three_bulls
        and not wick_reversal
        and not cfg.hj_continuation_enabled
        and not hj_a_continuation
    )

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


    # CYS형 wick reversal 오진 방어:
    # 연속 하락/이평 하락 상태에서 진행 중 15분봉의 순간 아래꼬리 반등만으로
    # HJ가 상승전환으로 오인하지 않도록, wick_reversal은 추세회복 확인을 요구한다.
    hj_wick_reversal_trend_recovery = bool(
        ema9_rising
        or rebound
        or (higher_lows and live_price >= float(live.ema9))
    )

    # 급락-급반등 대형 변동봉에서는 wick_reversal 단독 신호를 차단.
    # CYS 사례 live_candle_range 약 5%를 기준으로 4% 이상은 과도 변동으로 본다.
    hj_wick_reversal_oversized_candle = bool(
        wick_reversal
        and live_candle_range_pct >= 4.0
        and not hj_fresh_breakout
    )

    hj_wick_reversal_trend_fail = bool(
        wick_reversal
        and not hj_fresh_breakout
        and not hj_wick_reversal_trend_recovery
    )

    # GWEI형 1차 방어:
    # wick_reversal로 보이지만 실제 아래꼬리가 몸통보다도 짧고(비율 < 1),
    # higher-low / higher-high / rebound 중 어느 구조회복도 없는 경우는
    # 단순 흔들림을 반등으로 오인한 것으로 보고 차단한다.
    hj_wick_reversal_weak_structure = bool(
        wick_reversal
        and not hj_fresh_breakout
        and lower_wick_ratio < 1.00
        and not rebound
        and not higher_lows
        and not higher_highs
    )

    # GWEI형 2차 재진입 방어:
    # 거래량이 매우 약하고 감소 중이며 EMA 정렬까지 무너진 wick reversal은
    # 가격 한 번 튄 것만으로 재진입하지 않는다.
    # BTW/RE처럼 거래량비가 낮아도 volume trend 또는 EMA 구조가 살아있는 경우는 보존한다.
    hj_wick_reversal_faded_reentry = bool(
        wick_reversal
        and not hj_fresh_breakout
        and live_volume_ratio < 0.45
        and volume_declining_3
        and not volume_trend_ok
        and not ema_ordered
        and not rebound
        and not higher_lows
        and not higher_highs
    )

    # v4.3.34 HJ continuation 대손실 꼬리 방어.
    # 8/14~8/16 실제 continuation 진입을 대입해, 저거래량(<0.70)에서만
    # 반복된 약한 구조/과확장/취약 fresh-breakout 패턴을 제한한다.
    # 강한 거래량 continuation과 기존 수익 continuation은 그대로 보존한다.
    hj_continuation_weak_structure = bool(
        continuation_three_bulls
        and live_volume_ratio < 0.70
        and not hj_fresh_breakout
        and not rebound
        and not higher_lows
        and not higher_highs
    )
    hj_continuation_weak_h1_near_high = bool(
        continuation_three_bulls
        and live_volume_ratio < 0.70
        and not hj_fresh_breakout
        and not h1_up
        and live_distance_to_high <= 2.00
    )
    hj_continuation_oversized_low_volume = bool(
        continuation_three_bulls
        and live_volume_ratio < 0.70
        and not hj_fresh_breakout
        and h1_up
        and live_candle_range_pct >= 4.00
        and live_distance_to_high <= 4.00
    )
    hj_continuation_weak_fresh_breakout = bool(
        continuation_three_bulls
        and live_volume_ratio < 0.70
        and hj_fresh_breakout
        and not higher_lows
        and not higher_highs
    )
    # v4.3.37 BICO형 고점 과확장 fresh-breakout 방어.
    # 8/14~8/16 continuation 실거래 비교에서, 최근 저점 대비 10%+ 반등 후
    # 고점 바로 위/근처를 저거래량(<1.0)·1시간 추세 약세 상태로 재돌파한 BICO 대손실만
    # 추가로 걸리고 기존 수익 continuation은 보존되는 좁은 조건이다.
    hj_continuation_overextended_fresh_breakout = bool(
        continuation_three_bulls
        and hj_fresh_breakout
        and rebound_from_low >= 10.00
        and distance_to_high <= 0.50
        and live_volume_ratio < 1.00
        and not h1_up
    )
    # v4.3.35 P형 과확장 continuation 방어.
    # P는 확정 15분봉 기반 반등 전략이지만, 진입 시점의 진행봉까지 3연속 상승으로 이어지고
    # 최근 4시간 저점에서 이미 10% 이상 올라온 상태에서 최근 고점 0.5% 이내라면
    # AIO처럼 상승 말단을 잡을 위험이 커서 P 진입만 차단한다.
    p_overextended_continuation = bool(
        continuation_three_bulls
        and rebound_from_low >= cfg.p_overextended_continuation_min_rebound_pct
        and distance_to_high <= cfg.p_overextended_continuation_max_high_distance_pct
    )
    if p_overextended_continuation:
        p_ok = False

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
        and not hj_wick_reversal_trend_fail
        and not hj_wick_reversal_oversized_candle
        and not hj_wick_reversal_weak_structure
        and not hj_wick_reversal_faded_reentry
        and not hj_continuation_weak_structure
        and not hj_continuation_weak_h1_near_high
        and not hj_continuation_oversized_low_volume
        and not hj_continuation_weak_fresh_breakout
        and not hj_continuation_overextended_fresh_breakout
        and live_gain <= 8.0
    )
    # v4.3.43 P-only trial: HJ 진입만 비활성화하고 P형 로직은 그대로 유지한다.
    hj_ok = False

    hj_score = (
        hj_trend_score * 10
        + (20 if hj_wick_reversal_active else 0)
        + (20 if continuation_three_bulls else 0)
        + min(15, live_volume_ratio * 8)
        + min(10, max(0.0, live_gain) * 4)
    )

    if hj_ok and (not p_ok or hj_score >= p_score):
        strategy = "HJ"
        score = float(hj_score)
        selected_price = live_price
    elif p_ok and p_live_gain_ok and quality_ok:
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
            if not pullback_ok:
                rejected.append("pullback_ok")
            if not not_chasing:
                rejected.append("not_chasing")
            if not momentum_ok:
                rejected.append("momentum_ok")
            if not volume_ok:
                rejected.append("volume_ok")
            if not volume_trend_ok:
                rejected.append("volume_trend_ok")
            if not movement_ok:
                rejected.append("movement_ok")
            if not not_extreme:
                rejected.append("not_extreme")
            if not p_ema_quality_ok:
                rejected.append("p_ema_not_ordered")
            if not p_entry_quality_ok:
                rejected.append("p_entry_quality_ok")
            if p_near_high_weak_reentry:
                rejected.append("p_near_high_weak_reentry")
            if p_faded_spike_reentry:
                rejected.append("p_faded_spike_reentry")
            if p_overextended_continuation:
                rejected.append("p_overextended_continuation")
            if not quality_ok:
                rejected.append("live_candle_quality")
            if not p_live_gain_ok:
                rejected.append("p_live_gain_over_0_50pct")
            if p_score < 65:
                rejected.append("p_score")
        if not hj_ok:
            if hj_continuation_disabled:
                rejected.append("hj_continuation_disabled")
            elif not hj_pattern_ok:
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
            if hj_wick_reversal_trend_fail:
                rejected.append("hj_wick_reversal_trend_fail")
            if hj_wick_reversal_oversized_candle:
                rejected.append("hj_wick_reversal_oversized_candle")
            if hj_wick_reversal_weak_structure:
                rejected.append("hj_wick_reversal_weak_structure")
            if hj_wick_reversal_faded_reentry:
                rejected.append("hj_wick_reversal_faded_reentry")
            if hj_continuation_weak_structure:
                rejected.append("hj_continuation_weak_structure")
            if hj_continuation_weak_h1_near_high:
                rejected.append("hj_continuation_weak_h1_near_high")
            if hj_continuation_oversized_low_volume:
                rejected.append("hj_continuation_oversized_low_volume")
            if hj_continuation_weak_fresh_breakout:
                rejected.append("hj_continuation_weak_fresh_breakout")
            if hj_continuation_overextended_fresh_breakout:
                rejected.append("hj_continuation_overextended_fresh_breakout")
            if last_large_bearish:
                rejected.append("hj_after_large_bearish")

    # v4.3.58 준P/새P 연구 판정. LIVE 주문 조건에는 절대 사용하지 않는다.
    junp_soft_checks = {
        "rebound": bool(rebound),
        "momentum_ok": bool(momentum_ok),
        "volume_ok": bool(volume_ok),
        "volume_trend_ok": bool(volume_trend_ok),
        "p_entry_quality_ok": bool(p_entry_quality_ok),
    }
    junp_missing = [name for name, ok in junp_soft_checks.items() if not ok]
    junp_hard_base_ok = bool(
        h1_up and pullback_ok and not_chasing and movement_ok and not_extreme
        and p_ema_quality_ok and quality_ok
        and rebound_from_low >= cfg.min_rebound_from_low_pct
    )
    junp_hard_ok = bool(junp_hard_base_ok and p_live_gain_ok)
    junp_shadow_candidate = bool(
        cfg.junp_shadow_enabled and not p_ok
        and p_score >= float(cfg.junp_shadow_min_score)
        and junp_hard_ok and len(junp_missing) == 1
    )
    junp_missing_condition = junp_missing[0] if junp_shadow_candidate else ""

    # 현재 P의 필수조건에서 점수/상한만 분리해 연구 가설을 동시에 판정한다.
    p_required_checks = {
        "h1_up": h1_up, "pullback_ok": pullback_ok, "not_chasing": not_chasing,
        "rebound": rebound, "momentum_ok": momentum_ok, "volume_ok": volume_ok,
        "volume_trend_ok": volume_trend_ok, "movement_ok": movement_ok,
        "not_extreme": not_extreme, "p_entry_quality_ok": p_entry_quality_ok,
        "rebound_min": rebound_from_low >= cfg.min_rebound_from_low_pct,
        "live_candle_quality": quality_ok,
    }
    p_failed_checks = [name for name, ok in p_required_checks.items() if not bool(ok)]
    p_base_checks_ok = bool(not p_failed_checks)
    p_current_shadow_candidate = bool(p_base_checks_ok and p_score >= 65 and p_live_cap050_ok)
    p_strength050_shadow_candidate = bool(p_current_shadow_candidate and strength_2of3_ok)
    p_strength060_shadow_candidate = bool(p_base_checks_ok and p_score >= 65 and strength_2of3_ok and p_live_cap060_ok)
    p_newscore060_shadow_candidate = bool(p_base_checks_ok and new_p_score >= float(cfg.research_new_score_min) and strength_2of3_ok and p_live_cap060_ok)

    junp_old_shadow_candidate = bool(
        cfg.research_shadow_enabled and not p_current_shadow_candidate
        and p_score >= float(cfg.junp_shadow_min_score)
        and junp_hard_base_ok and p_live_cap050_ok and len(junp_missing) == 1
    )
    junp_new_shadow_candidate = bool(
        cfg.research_shadow_enabled and not p_newscore060_shadow_candidate
        and new_p_score >= float(cfg.research_new_score_min)
        and junp_hard_base_ok and p_live_cap060_ok and len(junp_missing) == 1
    )
    research_nearmiss_candidate = bool(
        cfg.research_shadow_enabled and 1 <= len(p_failed_checks) <= int(cfg.research_nearmiss_max_failed_checks)
        and (p_score >= float(cfg.research_nearmiss_min_old_score) or new_p_score >= float(cfg.research_new_score_min) or strength_2of3_ok)
    )

    # v4.3.60 새 P v2.1 / 준P v2.1 Shadow. 실제 LIVE P 판정(p_ok)은 그대로 유지한다.
    # v2의 폭넓은 후보군은 유지하되, 순간 모멘텀보다 trend+structure 지속력을 우선하고
    # 여러 과열 신호가 동시에 겹친 끝물 후보만 제한한다.
    p_v2_hard_ok = bool(h1_up and movement_ok and not_extreme and p_live_cap060_ok)
    p_v2_candidate = bool(
        cfg.research_shadow_enabled and p_v2_hard_ok
        and p_v2_score >= float(cfg.research_pv2_total_min)
        and p_v2_component_pass_count >= int(cfg.research_pv2_component_pass_min)
        and p_v2_signal_pass_count >= int(cfg.research_pv2_signal_pass_min)
    )
    junp_v2_candidate = bool(
        cfg.research_shadow_enabled and p_v2_hard_ok and not p_v2_candidate
        and p_v2_score >= float(cfg.research_junpv2_total_min)
        and p_v2_component_pass_count >= int(cfg.research_junpv2_component_pass_min)
        and p_v2_signal_pass_count >= int(cfg.research_junpv2_signal_pass_min)
    )
    junp_v2_missing_components = ",".join(name for name, ok in p_v2_component_checks.items() if not ok) if junp_v2_candidate else ""

    p_v21_candidate = bool(
        cfg.research_shadow_enabled and p_v2_hard_ok
        and p_v2_score >= float(cfg.research_pv21_total_min)
        and p_v21_persistence_score >= float(cfg.research_pv21_persistence_min)
        and p_v2_signal_pass_count >= int(cfg.research_pv21_signal_pass_min)
        and p_v21_overheat_risk_count <= int(cfg.research_pv21_max_overheat_risk_count)
    )
    junp_v21_candidate = bool(
        cfg.research_shadow_enabled and p_v2_hard_ok and not p_v21_candidate
        and p_v2_score >= float(cfg.research_junpv21_total_min)
        and p_v21_persistence_score >= float(cfg.research_junpv21_persistence_min)
        and p_v2_signal_pass_count >= int(cfg.research_junpv21_signal_pass_min)
        and p_v21_overheat_risk_count <= int(cfg.research_pv21_max_overheat_risk_count)
    )
    if junp_v21_candidate:
        _miss = []
        if p_v2_score < float(cfg.research_pv21_total_min): _miss.append("p_total")
        if p_v21_persistence_score < float(cfg.research_pv21_persistence_min): _miss.append("p_persistence")
        if p_v2_signal_pass_count < int(cfg.research_pv21_signal_pass_min): _miss.append("p_signal")
        junp_v21_missing_reason = ",".join(_miss) or "below_p_v21"
    else:
        junp_v21_missing_reason = ""

    # v4.3.61 P/JUNP v2.2 Shadow. v2.1은 비교 telemetry로 남기되 신규 Shadow 등록은 v2.2만 한다.
    p_v22_candidate = bool(
        cfg.research_shadow_enabled and p_v2_hard_ok
        and p_v2_score >= float(cfg.research_pv22_total_min)
        and p_v21_persistence_score >= float(cfg.research_pv22_persistence_min)
        and p_v2_signal_pass_count >= int(cfg.research_pv22_signal_pass_min)
        and not p_v22_structure_incomplete
        and not p_v22_late_extension
    )
    junp_v22_candidate = bool(
        cfg.research_shadow_enabled and p_v2_hard_ok and not p_v22_candidate
        and p_v2_score >= float(cfg.research_junpv22_total_min)
        and p_v21_persistence_score >= float(cfg.research_junpv22_persistence_min)
        and p_v2_signal_pass_count >= int(cfg.research_junpv22_signal_pass_min)
        and p_v2_structure_score >= 12.0
        and ema9_ema20_gap_pct > -0.05
        and ema20_slope_pct > 0.0
        and not p_v22_structure_incomplete
        and not p_v22_late_extension
    )
    if junp_v22_candidate:
        _miss22 = []
        if p_v2_score < float(cfg.research_pv22_total_min): _miss22.append("p_total")
        if p_v21_persistence_score < float(cfg.research_pv22_persistence_min): _miss22.append("p_persistence")
        if p_v2_signal_pass_count < int(cfg.research_pv22_signal_pass_min): _miss22.append("p_signal")
        junp_v22_missing_reason = ",".join(_miss22) or "below_p_v22"
    else:
        junp_v22_missing_reason = ""

    # v4.3.63 P_V23 research-only.
    # 전체 P_V22를 더 조이는 대신, 과거 P_V22 결과에서 STOP에 선택적으로 몰린 두 패턴만 동시에 차단한다.
    # 1) 중기 추세 받침 약함: EMA20 slope < 0.25% AND EMA9-20 gap < 1.00%
    # 2) 순간 RSI 과가속: RSI delta >= 8.0
    # 실제 LIVE 주문/기존 P_V22/JUNP_V22 판정은 변경하지 않는다.
    p_v23_weak_trend_block = bool(
        ema20_slope_pct < float(cfg.research_pv23_weak_ema20_slope_max)
        and ema9_ema20_gap_pct < float(cfg.research_pv23_weak_ema_gap_max)
    )
    p_v23_rsi_spike_block = bool(
        rsi_delta >= float(cfg.research_pv23_rsi_delta_spike_min)
    )
    _v23_reasons = []
    if p_v23_weak_trend_block:
        _v23_reasons.append("weak_ema20_and_gap")
    if p_v23_rsi_spike_block:
        _v23_reasons.append("rsi_delta_ge_8")
    p_v23_block_reason = ",".join(_v23_reasons)
    p_v23_candidate = bool(
        p_v22_candidate
        and not p_v23_weak_trend_block
        and not p_v23_rsi_spike_block
    )

    # v4.3.65 P_V24: 모든 P_V22 후보를 같은 방식으로 WATCH -> 회복확인 후 진입한다.
    # V23 통과 후보도 즉시 진입시키지 않는다.
    _setup_bucket = int(datetime.now(timezone.utc).timestamp()) // 900
    p_setup_id = f"P65SET-{symbol}-{_setup_bucket}"
    p_v24_immediate_candidate = False
    p_v24_watch_candidate = bool(p_v22_candidate)

    research_variants = []
    if cfg.research_legacy_variants_enabled:
        if p_current_shadow_candidate: research_variants.append("P_CURRENT")
        if p_strength050_shadow_candidate: research_variants.append("P_STRENGTH_050")
        if p_strength060_shadow_candidate: research_variants.append("P_STRENGTH_060")
        if p_newscore060_shadow_candidate: research_variants.append("P_NEWSCORE_060")
        if p_v22_candidate: research_variants.append("P_V22")
        if research_nearmiss_candidate: research_variants.append("REJECT_NEARMISS")
    if cfg.research_junp_variants_enabled and junp_old_shadow_candidate: research_variants.append("JUNP_OLD")
    if cfg.research_junp_variants_enabled and junp_new_shadow_candidate: research_variants.append("JUNP_NEW")
    if cfg.research_v23_v24_enabled and p_v23_candidate: research_variants.append("P_V23")
    if cfg.research_junp_variants_enabled and junp_v22_candidate: research_variants.append("JUNP_V22")

    details = {
        "price": selected_price,
        "live_price": float(live_price),
        "strategy": strategy,
        "score": round(float(score), 2),
        "p_score": round(float(p_score), 2),
        "new_p_score": round(float(new_p_score), 2),
        "new_score_structure": round(float(new_score_structure), 2),
        "new_score_trend": round(float(new_score_trend), 2),
        "new_score_live": round(float(new_score_live), 2),
        "new_score_rsi": round(float(new_score_rsi), 2),
        "new_score_market_structure": round(float(new_score_market_structure), 2),
        "new_score_volume": round(float(new_score_volume), 2),
        "strength_rsi_ok": strength_rsi_ok,
        "strength_rebound_ok": strength_rebound_ok,
        "strength_live_ok": strength_live_ok,
        "strength_pass_count": strength_pass_count,
        "strength_2of3_ok": strength_2of3_ok,
        "p_current_shadow_candidate": p_current_shadow_candidate,
        "p_strength050_shadow_candidate": p_strength050_shadow_candidate,
        "p_strength060_shadow_candidate": p_strength060_shadow_candidate,
        "p_newscore060_shadow_candidate": p_newscore060_shadow_candidate,
        "junp_old_shadow_candidate": junp_old_shadow_candidate,
        "junp_new_shadow_candidate": junp_new_shadow_candidate,
        "junp_old_missing_condition": junp_missing[0] if junp_old_shadow_candidate and junp_missing else "",
        "junp_new_missing_condition": junp_missing[0] if junp_new_shadow_candidate and junp_missing else "",
        "p_failed_check_count": len(p_failed_checks),
        "p_failed_checks": ",".join(p_failed_checks),
        "research_nearmiss_candidate": research_nearmiss_candidate,
        "p_v2_score": round(float(p_v2_score), 2),
        "p_v2_trend_score": round(float(p_v2_trend_score), 2),
        "p_v2_structure_score": round(float(p_v2_structure_score), 2),
        "p_v2_momentum_score": round(float(p_v2_momentum_score), 2),
        "p_v2_soft_score": round(float(p_v2_soft_score), 2),
        "p_v2_signal_pass_count": int(p_v2_signal_pass_count),
        "p_v2_component_pass_count": int(p_v2_component_pass_count),
        "p_v2_candidate": bool(p_v2_candidate),
        "junp_v2_candidate": bool(junp_v2_candidate),
        "junp_v2_missing_components": junp_v2_missing_components,
        "p_v21_persistence_score": round(float(p_v21_persistence_score), 2),
        "p_v21_overheat_risk_count": int(p_v21_overheat_risk_count),
        "p_v21_overheat_flags": p_v21_overheat_flags,
        "p_v21_candidate": bool(p_v21_candidate),
        "junp_v21_candidate": bool(junp_v21_candidate),
        "junp_v21_missing_reason": junp_v21_missing_reason,
        "p_v22_heat_count": int(p_v22_heat_count),
        "p_v22_heat_flags": p_v22_heat_flags,
        "p_v22_late_extension": bool(p_v22_late_extension),
        "p_v22_structure_weak_count": int(p_v22_structure_weak_count),
        "p_v22_structure_weak_flags": p_v22_structure_weak_flags,
        "p_v22_structure_incomplete": bool(p_v22_structure_incomplete),
        "p_v22_candidate": bool(p_v22_candidate),
        "junp_v22_candidate": bool(junp_v22_candidate),
        "junp_v22_missing_reason": junp_v22_missing_reason,
        "p_v23_weak_trend_block": bool(p_v23_weak_trend_block),
        "p_v23_rsi_spike_block": bool(p_v23_rsi_spike_block),
        "p_v23_block_reason": p_v23_block_reason,
        "p_v23_candidate": bool(p_v23_candidate),
        "p_setup_id": p_setup_id,
        "p_v24_watch_candidate": bool(p_v24_watch_candidate),
        "p_v24_immediate_candidate": bool(p_v24_immediate_candidate),
        "p_v24_confirm_state": "WATCH" if p_v24_watch_candidate else "",
        "prev1_candle_gain_pct": round(float(prev1_candle_gain_pct), 4),
        "prev2_candle_gain_pct": round(float(prev2_candle_gain_pct), 4),
        "prev3_candle_gain_pct": round(float(prev3_candle_gain_pct), 4),
        "rsi_prev2": round(float(rsi_prev2), 3),
        "rsi_prev3": round(float(rsi_prev3), 3),
        "rsi_change_prev1": round(float(rsi_change_prev1), 3),
        "rsi_change_prev2": round(float(rsi_change_prev2), 3),
        "ema9_slope_prev1_pct": round(float(ema9_slope_prev1_pct), 4),
        "ema9_slope_prev2_pct": round(float(ema9_slope_prev2_pct), 4),
        "ema20_slope_prev1_pct": round(float(ema20_slope_prev1_pct), 4),
        "ema20_slope_prev2_pct": round(float(ema20_slope_prev2_pct), 4),
        "ema9_ema20_gap_prev1_pct": round(float(ema9_ema20_gap_prev1_pct), 4),
        "ema9_ema20_gap_prev2_pct": round(float(ema9_ema20_gap_prev2_pct), 4),
        "ema9_ema20_gap_prev3_pct": round(float(ema9_ema20_gap_prev3_pct), 4),
        "close_change_prev1_pct": round(float(close_change_prev1_pct), 4),
        "close_change_prev2_pct": round(float(close_change_prev2_pct), 4),
        "close_change_prev3_pct": round(float(close_change_prev3_pct), 4),
        "high_change_prev1_pct": round(float(high_change_prev1_pct), 4),
        "high_change_prev2_pct": round(float(high_change_prev2_pct), 4),
        "high_change_prev3_pct": round(float(high_change_prev3_pct), 4),
        "low_change_prev1_pct": round(float(low_change_prev1_pct), 4),
        "low_change_prev2_pct": round(float(low_change_prev2_pct), 4),
        "low_change_prev3_pct": round(float(low_change_prev3_pct), 4),
        "ema9_slope_delta_pct": round(float(ema9_slope_delta_pct), 4),
        "ema20_slope_delta_pct": round(float(ema20_slope_delta_pct), 4),
        "ema_gap_delta_pct": round(float(ema_gap_delta_pct), 4),
        "rsi_rollover_3bar": bool(rsi_rollover_3bar),
        "price_gain_decelerating_3bar": bool(price_gain_decelerating_3bar),
        "ema_force_decelerating": bool(ema_force_decelerating),
        "research_variants": ",".join(research_variants),
        "junp_shadow_candidate": bool(junp_shadow_candidate),
        "junp_missing_condition": junp_missing_condition,
        "entry_reason": (
            "긴꼬리 음봉 후 양봉 몸통회복" if strategy == "HJ" and hj_wick_reversal_active
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
        "momentum_ok": momentum_ok,
        "not_extreme": not_extreme,
        "rsi": round(float(live.rsi if strategy == "HJ" else row.rsi), 2),
        "rsi_prev": round(rsi_prev, 2),
        "rsi_delta": round(rsi_delta, 3),
        "volume_ratio": round(float(live_volume_ratio if strategy == "HJ" else volume_ratio), 2),
        "live_volume_ratio": round(float(live_volume_ratio), 4),
        "pullback_from_high_pct": round(pullback_from_high, 2),
        "rebound_from_low_pct": round(float(live_rebound_from_low if strategy == "HJ" else rebound_from_low), 2),
        "entry_candle_gain_pct": round(float(live_gain if strategy == "HJ" else entry_candle_gain), 2),
        # v4.3.51: P형에서도 실제 진행봉 상태를 사후 검증할 수 있도록 별도 기록.
        "live_price": round(float(live_price), 10),
        "live_candle_gain_pct": round(float(live_gain), 4),
        "p_live_max_gain_pct": round(float(cfg.p_live_max_gain_pct), 4),
        "p_live_gain_ok": bool(p_live_gain_ok),
        "p_live_cap050_ok": p_live_cap050_ok,
        "p_live_cap060_ok": p_live_cap060_ok,
        "live_body_pct": round(float((max(float(live.close)-float(live.open),0.0)/float(live.open)*100) if float(live.open)>0 else 0.0), 4),
        "live_upper_wick_ratio": round(float((max(float(live.high)-max(float(live.open),float(live.close)),0.0)/max(abs(float(live.close)-float(live.open)),1e-12))), 4),
        "live_lower_wick_ratio": round(float((max(min(float(live.open),float(live.close))-float(live.low),0.0)/max(abs(float(live.close)-float(live.open)),1e-12))), 4),
        "distance_to_recent_high_pct": round(float(live_distance_to_high if strategy == "HJ" else distance_to_high), 2),
        "hj_breakout_raw": bool(hj_breakout_raw),
        "hj_fresh_breakout_confirmed": bool(hj_fresh_breakout),
        "hj_breakout_confirm_age_sec": round(float(hj_breakout_confirm_age_sec), 1),
        "one_hour_move_pct": round(one_hour_move, 2),
        "one_hour_signed_move_pct": round(one_hour_signed_move, 4),
        "ema9_slope_pct": round(ema9_slope_pct, 4),
        "ema20_slope_pct": round(ema20_slope_pct, 4),
        "ema9_ema20_gap_pct": round(ema9_ema20_gap_pct, 4),
        "ema20_ema60_gap_pct": round(ema20_ema60_gap_pct, 4),
        "volume_declining_3": volume_declining_3,
        "confirmation_hold": confirmation_hold,
        "ema_ordered": ema_ordered,
        "ema9_rising": ema9_rising,
        "higher_lows": higher_lows,
        "higher_highs": higher_highs,
        "p_ema_quality_ok": p_ema_quality_ok,
        "p_near_high_weak_reentry": p_near_high_weak_reentry,
        "p_faded_spike_reentry": p_faded_spike_reentry,
        "p_overextended_continuation": p_overextended_continuation,
        "p_entry_quality_ok": p_entry_quality_ok,
        "hj_wick_reversal": wick_reversal,
        "hj_wick_enabled": bool(cfg.hj_wick_enabled),
        "hj_wick_reversal_active": hj_wick_reversal_active,
        "hj_continuation_three_bulls": continuation_three_bulls,
        "hj_continuation_enabled": bool(cfg.hj_continuation_enabled),
        "hj_a_continuation": hj_a_continuation,
        "hj_a_continuation_min_volume_ratio": float(cfg.hj_a_continuation_min_volume_ratio),
        "hj_a_continuation_require_h1_up": True,
        "hj_a_continuation_require_ema_ordered": True,
        "hj_continuation_disabled": hj_continuation_disabled,
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
        "hj_wick_reversal_trend_recovery": hj_wick_reversal_trend_recovery,
        "hj_wick_reversal_trend_fail": hj_wick_reversal_trend_fail,
        "hj_wick_reversal_oversized_candle": hj_wick_reversal_oversized_candle,
        "hj_wick_reversal_weak_structure": hj_wick_reversal_weak_structure,
        "hj_wick_reversal_faded_reentry": hj_wick_reversal_faded_reentry,
        "hj_continuation_weak_structure": hj_continuation_weak_structure,
        "hj_continuation_weak_h1_near_high": hj_continuation_weak_h1_near_high,
        "hj_continuation_oversized_low_volume": hj_continuation_oversized_low_volume,
        "hj_continuation_weak_fresh_breakout": hj_continuation_weak_fresh_breakout,
        "hj_continuation_overextended_fresh_breakout": hj_continuation_overextended_fresh_breakout,
        "hj_fresh_breakout": hj_fresh_breakout,
        "hj_live_distance_to_high_pct": round(live_distance_to_high, 2),
        "hj_live_rebound_from_low_pct": round(live_rebound_from_low, 2),
        "live_candle_range_pct": round(live_candle_range_pct, 2),
        "hj_last_large_bearish": last_large_bearish,
        "hj_body_recovery_pct": round(body_recovery * 100, 2),
        "hj_lower_wick_body_ratio": round(lower_wick_ratio, 2),
        "live_quality_ok": bool(quality_ok),
        "live_quality_reason": str(quality_details.get("reason") or ""),
        "live_quality_bullish": quality_details.get("bullish", ""),
        "live_quality_body_ok": quality_details.get("body_ok", ""),
        "live_quality_wick_ok": quality_details.get("wick_ok", ""),
        "live_quality_prev_bearish": quality_details.get("prev_bearish", ""),
        "live_quality_prev_body_recovery": quality_details.get("prev_body_recovery", ""),
        "live_quality_recovery_ok": quality_details.get("recovery_ok", ""),
        "live_quality_5m_bullish": quality_details.get("five_ok", ""),
        "rejected_conditions": list(dict.fromkeys((["live_candle_quality"] if not quality_ok else []) + rejected)),
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

def _adaptive_loss_structure(
    m5: pd.DataFrame,
    drawdown_pct: float,
    tier1_pct: float = 1.50,
    tier2_pct: float = 2.00,
    tier3_pct: float = 2.50,
) -> tuple[bool, dict[str, Any]]:
    """확정 5분봉 기반 단계형 손실 가드.

    손실이 얕을 때는 강한 구조붕괴를 요구하고, 손실이 깊어질수록 필요한
    확인 개수를 줄인다. 단순 고정 -N% 손절은 사용하지 않는다.
    """
    if len(m5) < 6:
        return False, {"reason": "5m candles insufficient"}

    a, b, c = m5.iloc[-3], m5.iloc[-2], m5.iloc[-1]
    two_below_ema9 = bool(float(b.close) < float(b.ema9) and float(c.close) < float(c.ema9))
    close_below_ema20 = bool(float(c.close) < float(c.ema20) * 0.998)
    ema9_falling = bool(float(c.ema9) < float(b.ema9) < float(a.ema9))
    lower_lows = bool(float(c.low) < float(b.low) <= float(a.low))
    bearish = bool(float(c.close) < float(c.open))
    rsi_weakening = bool(float(c.rsi) < float(b.rsi) and float(c.rsi) <= 50.0)
    structure_score = int(close_below_ema20) + int(lower_lows) + int(rsi_weakening)

    dd = abs(min(0.0, float(drawdown_pct)))
    tier = "NONE"
    ok = False

    if dd >= tier3_pct:
        # 이미 깊게 밀렸다면 5분 구조가 두 축에서 무너진 것만 확인해 빠르게 종료.
        tier = "TIER3_DEEP"
        ok = bool(
            (two_below_ema9 and ema9_falling)
            or (close_below_ema20 and lower_lows)
            or (ema9_falling and lower_lows and rsi_weakening)
        )
    elif dd >= tier2_pct:
        # 중간 손실: EMA9 이탈/하락은 유지하되 현재봉 음봉까지 모두 기다리지는 않는다.
        tier = "TIER2_MEDIUM"
        ok = bool(two_below_ema9 and ema9_falling and structure_score >= 1)
    elif dd >= tier1_pct:
        # 얕은 손실: 정상 눌림 오판 방지를 위해 강한 구조붕괴만 종료.
        tier = "TIER1_SHALLOW"
        ok = bool(two_below_ema9 and ema9_falling and bearish and structure_score >= 2)

    return ok, {
        "tier": tier,
        "drawdown_pct": round(float(drawdown_pct), 3),
        "two_below_ema9": two_below_ema9,
        "close_below_ema20": close_below_ema20,
        "ema9_falling": ema9_falling,
        "lower_lows": lower_lows,
        "bearish": bearish,
        "rsi_weakening": rsi_weakening,
        "structure_score": structure_score,
        "last_close": float(c.close),
        "last_ema9": float(c.ema9),
        "last_ema20": float(c.ema20),
        "last_rsi": round(float(c.rsi), 2),
    }


def early_crash_failure_signal(
    client: BybitSwingClient,
    symbol: str,
    opened_at: str,
    base_price: float,
    live_price: float,
    cfg: DailyConfig,
) -> tuple[bool, dict[str, Any]]:
    """v4.3.41: 진입 후 3~15분 급락만 잡는 구조형 조기 종료.

    -2.5% 이상 급락이 먼저 발생해야 하며, 확정 5분봉에서 최소 2개의
    약세 구조가 함께 확인될 때만 종료한다. 정상적인 얕은 눌림은 대상이 아니다.
    """
    if not cfg.early_crash_guard_enabled or base_price <= 0 or live_price <= 0:
        return False, {"reason": "disabled_or_bad_price"}
    try:
        opened = datetime.fromisoformat(opened_at)
        if opened.tzinfo is None:
            opened = opened.replace(tzinfo=timezone.utc)
    except Exception:
        return False, {"reason": "opened_at parse failed"}

    age_min = (datetime.now(timezone.utc) - opened).total_seconds() / 60.0
    if age_min < cfg.early_crash_min_age_minutes:
        return False, {"reason": "too_early", "age_min": round(age_min, 2)}
    if age_min > cfg.early_crash_max_age_minutes:
        return False, {"reason": "window_passed", "age_min": round(age_min, 2)}

    drawdown_pct = (live_price / base_price - 1.0) * 100.0
    if drawdown_pct > -abs(cfg.early_crash_drawdown_pct):
        return False, {
            "reason": "drawdown_not_deep",
            "age_min": round(age_min, 2),
            "drawdown_pct": round(drawdown_pct, 3),
        }

    m5 = confirmed(indicators(client.candles(symbol, "5m", 80)))
    if len(m5) < 4:
        return False, {"reason": "5m candles insufficient"}

    a, b, c = m5.iloc[-3], m5.iloc[-2], m5.iloc[-1]
    below_ema9 = bool(float(c.close) < float(c.ema9))
    below_ema20 = bool(float(c.close) < float(c.ema20))
    ema9_falling = bool(float(c.ema9) < float(b.ema9))
    bearish = bool(float(c.close) < float(c.open))
    lower_low = bool(float(c.low) < float(b.low))
    rsi_weakening = bool(float(c.rsi) < float(b.rsi))

    structure_score = sum(int(x) for x in (
        below_ema9, below_ema20, ema9_falling, bearish, lower_low, rsi_weakening
    ))
    trend_break = bool(below_ema9 or below_ema20)
    pressure = bool(bearish or lower_low or ema9_falling)
    ok = bool(structure_score >= 2 and trend_break and pressure)

    return ok, {
        "reason": "EARLY_CRASH_FAILURE" if ok else "early_crash_hold",
        "age_min": round(age_min, 2),
        "drawdown_pct": round(drawdown_pct, 3),
        "structure_score": structure_score,
        "below_ema9": below_ema9,
        "below_ema20": below_ema20,
        "ema9_falling": ema9_falling,
        "bearish": bearish,
        "lower_low": lower_low,
        "rsi_weakening": rsi_weakening,
        "last_close": float(c.close),
        "last_ema9": float(c.ema9),
        "last_ema20": float(c.ema20),
        "last_rsi": round(float(c.rsi), 2),
    }



def p_catastrophic_failure_signal(
    client: BybitSwingClient,
    symbol: str,
    opened_at: str,
    base_price: float,
    live_price: float,
    cfg: DailyConfig,
) -> tuple[bool, dict[str, Any]]:
    """P형 단계형 대손실 가드 (15~45분, TP1 이전).

    v4.3.39: -2.5%까지 무조건 기다리던 구조를 없애고, -1.5/-2.0/-2.5%
    단계별로 구조 확인 강도를 조절한다. 고정손절 단독으로는 종료하지 않는다.
    """
    if not cfg.p_catastrophic_guard_enabled or base_price <= 0 or live_price <= 0:
        return False, {"reason": "disabled_or_bad_price"}
    try:
        opened = datetime.fromisoformat(opened_at)
        if opened.tzinfo is None:
            opened = opened.replace(tzinfo=timezone.utc)
    except Exception:
        return False, {"reason": "opened_at parse failed"}

    age_min = (datetime.now(timezone.utc) - opened).total_seconds() / 60.0
    if age_min < cfg.p_catastrophic_guard_min_age_minutes:
        return False, {"reason": "too_early", "age_min": round(age_min, 2)}
    if age_min > cfg.p_catastrophic_guard_max_age_minutes:
        return False, {"reason": "window_passed", "age_min": round(age_min, 2)}

    drawdown_pct = (live_price / base_price - 1.0) * 100.0
    if drawdown_pct > -abs(cfg.adaptive_loss_tier1_pct):
        return False, {
            "reason": "drawdown_not_deep",
            "age_min": round(age_min, 2),
            "drawdown_pct": round(drawdown_pct, 3),
        }

    m5 = confirmed(indicators(client.candles(symbol, "5m", 80)))
    ok, details = _adaptive_loss_structure(
        m5,
        drawdown_pct,
        cfg.adaptive_loss_tier1_pct,
        cfg.adaptive_loss_tier2_pct,
        cfg.adaptive_loss_tier3_pct,
    )
    details.update({
        "reason": "P_CATASTROPHIC_FAILURE" if ok else "p_catastrophic_hold",
        "age_min": round(age_min, 2),
    })
    return ok, details


def hj_catastrophic_failure_signal(
    client: BybitSwingClient,
    symbol: str,
    opened_at: str,
    base_price: float,
    live_price: float,
    cfg: DailyConfig,
) -> tuple[bool, dict[str, Any]]:
    """HJ형 단계형 대손실 가드 (15~90분, TP1 이전).

    v4.3.39: P형과 동일한 단계형 구조를 사용해 깊은 손실일수록 더 적은
    구조 확인으로 종료한다. HJ continuation OFF 등 진입 로직은 변경하지 않는다.
    """
    if base_price <= 0 or live_price <= 0:
        return False, {"reason": "bad_price"}
    try:
        opened = datetime.fromisoformat(opened_at)
        if opened.tzinfo is None:
            opened = opened.replace(tzinfo=timezone.utc)
    except Exception:
        return False, {"reason": "opened_at parse failed"}

    age_min = (datetime.now(timezone.utc) - opened).total_seconds() / 60.0
    if age_min < 15.0:
        return False, {"reason": "too_early", "age_min": round(age_min, 2)}
    if age_min > 90.0:
        return False, {"reason": "window_passed", "age_min": round(age_min, 2)}

    drawdown_pct = (live_price / base_price - 1.0) * 100.0
    if drawdown_pct > -abs(cfg.adaptive_loss_tier1_pct):
        return False, {
            "reason": "drawdown_not_deep",
            "age_min": round(age_min, 2),
            "drawdown_pct": round(drawdown_pct, 3),
        }

    m5 = confirmed(indicators(client.candles(symbol, "5m", 80)))
    ok, details = _adaptive_loss_structure(
        m5,
        drawdown_pct,
        cfg.adaptive_loss_tier1_pct,
        cfg.adaptive_loss_tier2_pct,
        cfg.adaptive_loss_tier3_pct,
    )
    details.update({
        "reason": "HJ_CATASTROPHIC_FAILURE" if ok else "hj_catastrophic_hold",
        "age_min": round(age_min, 2),
    })
    return ok, details


def late_trend_failure_signal(
    client: BybitSwingClient,
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

    # 현재 봇의 실제 Bybit 클라이언트/지표 함수명을 사용한다.
    # 기존 45분 로직은 오래된 이름(add_indicators/klines)과 EMA5/EMA10 컬럼을
    # 참조해 런타임 NameError/컬럼 오류가 발생할 수 있었다.
    df15 = indicators(client.candles(symbol, "15m", 80))
    df15["ema5"] = df15["close"].ewm(span=5, adjust=False).mean()
    df15["ema10"] = df15["close"].ewm(span=10, adjust=False).mean()
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


def stalled_weak_exit_signal(
    client: BybitSwingClient,
    symbol: str,
    base_price: float,
    cfg: DailyConfig,
) -> tuple[bool, dict[str, Any]]:
    """TP1 미도달 장기 정체 포지션이 실제로 약해질 때만 조기종료한다."""
    m15 = confirmed(indicators(client.candles(symbol, "15m", 100)))
    if len(m15) < 25:
        return False, {"reason": "stalled_weak_not_enough_15m"}

    c = m15.iloc[-1]
    b = m15.iloc[-2]

    close = float(c.close)
    ema9 = float(c.ema9)
    ema20 = float(c.ema20)
    rsi = float(c.rsi)
    prev_rsi = float(b.rsi)
    prev_ema9 = float(b.ema9)

    current_pnl_pct = (close / float(base_price) - 1) * 100 if float(base_price) > 0 else -99.0
    progress_weak = bool(current_pnl_pct <= float(cfg.stalled_weak_max_current_pnl_pct))

    close_below_ema9 = bool(close < ema9)
    ema9_falling = bool(ema9 < prev_ema9)
    rsi_falling = bool(rsi < prev_rsi)
    rsi_weak = bool(rsi <= 55.0)
    bearish = bool(close < float(c.open))
    close_below_ema20 = bool(close < ema20)

    weakness_score = (
        int(close_below_ema9)
        + int(ema9_falling)
        + int(rsi_falling)
        + int(rsi_weak)
        + int(bearish)
        + int(close_below_ema20)
    )
    ok = bool(progress_weak and weakness_score >= int(cfg.stalled_weak_min_weakness_score))

    return ok, {
        "reason": "STALL_WEAK_75M" if ok else "stalled_weak_hold",
        "price": close,
        "current_pnl_pct": round(current_pnl_pct, 2),
        "close_below_ema9": close_below_ema9,
        "ema9_falling": ema9_falling,
        "rsi_falling": rsi_falling,
        "rsi_weak": rsi_weak,
        "bearish": bearish,
        "close_below_ema20": close_below_ema20,
        "weakness_score": weakness_score,
        "rsi": round(rsi, 2),
        "ema9": ema9,
        "ema20": ema20,
    }



def pv26_late_failure_signal(
    client: BybitSwingClient,
    symbol: str,
    entry_price: float,
    live_price: float,
    age_min: float,
    mfe_pct: float,
    cfg: DailyConfig,
) -> tuple[bool, dict[str, Any]]:
    """P_V26 TP1 미도달 장기 무진행 실패 감지.

    단순 시간손절/고정손절이 아니다.
    - TP1 이전 거래만 호출한다.
    - MFE가 +1% 미만인 '한 번도 제대로 못 간 거래'만 본다.
    - 80분 이후 -1.5% 이하 또는 120분 이후 -1.0% 이하일 때만 구조를 확인한다.
    - 확정 15분봉에서 EMA9 하락 + higher_lows=False + higher_highs=False가 동시에 성립해야 한다.
    호출부에서 2회 연속 확인해야 실제 LATE_FAILURE_EXIT로 종료한다.
    """
    if entry_price <= 0 or live_price <= 0:
        return False, {"reason": "bad_price"}
    if not cfg.research_pv26_late_failure_enabled:
        return False, {"reason": "disabled"}
    if float(mfe_pct) >= float(cfg.research_pv26_late_mfe_max_pct):
        return False, {
            "reason": "mfe_progressed",
            "mfe_pct": round(float(mfe_pct), 4),
        }

    pnl_pct = (float(live_price) / float(entry_price) - 1.0) * 100.0
    stage1 = bool(
        float(age_min) >= float(cfg.research_pv26_late_stage1_age_minutes)
        and pnl_pct <= float(cfg.research_pv26_late_stage1_pnl_pct)
    )
    stage2 = bool(
        float(age_min) >= float(cfg.research_pv26_late_stage2_age_minutes)
        and pnl_pct <= float(cfg.research_pv26_late_stage2_pnl_pct)
    )
    if not (stage1 or stage2):
        return False, {
            "reason": "loss_age_gate_not_met",
            "age_min": round(float(age_min), 1),
            "pnl_pct": round(pnl_pct, 4),
            "mfe_pct": round(float(mfe_pct), 4),
        }

    m15 = confirmed(indicators(client.candles(symbol, "15m", 80)))
    if len(m15) < 4:
        return False, {"reason": "not_enough_15m"}

    c = m15.iloc[-1]
    b = m15.iloc[-2]
    pp = m15.iloc[-3]

    ema9_falling = bool(float(c.ema9) < float(b.ema9))
    higher_lows = bool(float(c.low) > float(b.low) and float(b.low) >= float(pp.low))
    higher_highs = bool(float(c.high) > float(b.high) and float(b.high) >= float(pp.high))
    structure_failed = bool(ema9_falling and not higher_lows and not higher_highs)

    return structure_failed, {
        "reason": "PV26_LATE_STRUCTURE_FAIL" if structure_failed else "pv26_late_structure_hold",
        "age_min": round(float(age_min), 1),
        "pnl_pct": round(pnl_pct, 4),
        "mfe_pct": round(float(mfe_pct), 4),
        "stage": 2 if stage2 else 1,
        "ema9_falling": ema9_falling,
        "higher_lows": higher_lows,
        "higher_highs": higher_highs,
        "close": float(c.close),
        "ema9": float(c.ema9),
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
        self.raw_client = BybitSwingClient(demo=self.cfg.mode != "live")
        self.client = _BoundedReadClient(self.raw_client, timeout_seconds=8.0)
        init_db()
        ensure_scan_rejected_csv()
        state_set("runtime_version", BOT_RUNTIME_VERSION)
        state_set("runtime_started_at", datetime.now(timezone.utc).isoformat())
        state_set("runtime_bot_file", str(Path(__file__).resolve()))
        # v4.3.70: 시장 telemetry 캐시. 스캔 1회에 BTC/ETH 5분봉을 각 1회만 읽고
        # 모든 종목/Shadow 이벤트가 같은 snapshot을 공유한다. 매매 판정에는 사용하지 않는다.
        self._market_snapshot: dict[str, Any] = {}

    def _build_market_snapshot(self) -> dict[str, Any]:
        """BTC/ETH 단기 시장상태를 기록용으로만 계산한다.

        API 부하를 늘리지 않기 위해 자산별 5분봉 60개를 한 번만 읽어서
        5m/15m/30m/1h/4h 변화율과 최근 1시간 고점 대비 눌림폭을 모두 만든다.
        이 함수의 반환값은 CSV/Shadow telemetry에만 저장하며 진입/손절 판정에는 쓰지 않는다.
        """
        snapshot: dict[str, Any] = {
            "market_snapshot_time_kst": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
            "market_telemetry_status": "OK",
        }
        errors: list[str] = []

        def asset_snapshot(symbol: str, prefix: str) -> None:
            try:
                df = self.client.candles(symbol, "5m", 60)
                if df is None or len(df) < 50 or not {"close", "high"}.issubset(df.columns):
                    raise ValueError(f"{symbol} 5m candles insufficient")
                closes = pd.to_numeric(df["close"], errors="coerce")
                highs = pd.to_numeric(df["high"], errors="coerce")
                if closes.isna().any() or highs.isna().any():
                    raise ValueError(f"{symbol} 5m candles contain NaN")
                current = float(closes.iloc[-1])
                if current <= 0:
                    raise ValueError(f"{symbol} bad current price")

                def change(back_bars: int) -> float:
                    base = float(closes.iloc[-1 - int(back_bars)])
                    if base <= 0:
                        raise ValueError(f"{symbol} bad base price")
                    return round((current / base - 1.0) * 100.0, 4)

                # 마지막 행은 진행 중 5분봉의 현재 close이므로, 이전 close들과 비교해
                # '지금' 기준 단기 변화율을 남긴다.
                snapshot[f"{prefix}_5m_change_pct"] = change(1)
                snapshot[f"{prefix}_15m_change_pct"] = change(3)
                snapshot[f"{prefix}_30m_change_pct"] = change(6)
                snapshot[f"{prefix}_1h_change_pct"] = change(12)
                snapshot[f"{prefix}_4h_change_pct"] = change(48)

                one_hour_high = float(highs.tail(13).max())
                snapshot[f"{prefix}_1h_high_pullback_pct"] = round(
                    (current / one_hour_high - 1.0) * 100.0 if one_hour_high > 0 else 0.0, 4
                )
            except Exception as exc:
                errors.append(f"{prefix}:{type(exc).__name__}:{exc}")
                for suffix in (
                    "5m_change_pct", "15m_change_pct", "30m_change_pct",
                    "1h_change_pct", "4h_change_pct", "1h_high_pullback_pct",
                ):
                    snapshot[f"{prefix}_{suffix}"] = ""

        asset_snapshot("BTCUSDT", "btc")
        asset_snapshot("ETHUSDT", "eth")
        try:
            snapshot["btc_eth_both_down_15m"] = bool(
                float(snapshot.get("btc_15m_change_pct")) < 0
                and float(snapshot.get("eth_15m_change_pct")) < 0
            )
        except (TypeError, ValueError):
            snapshot["btc_eth_both_down_15m"] = ""

        if errors:
            snapshot["market_telemetry_status"] = "PARTIAL:" + " | ".join(errors)[:300]
        return snapshot

    def _refresh_market_snapshot(self) -> dict[str, Any]:
        # dict 전체를 한 번에 교체해 review thread가 읽을 때 반쪽 snapshot을 보지 않게 한다.
        self._market_snapshot = self._build_market_snapshot()
        try:
            state_set("market_snapshot", json.dumps(self._market_snapshot, ensure_ascii=False, default=str))
        except Exception:
            pass
        return dict(self._market_snapshot)

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
        """실제 손절(STOP)만 연속손실로 센다.

        TP1 후 본절보호 종료(BE_EXIT), 정체 종료(FLAT_EXIT), 시간 종료(TIME_EXIT)는
        중립 종료로 간주해 카운트에서 제외한다. 수익 거래가 나오면 연속손실은
        즉시 0으로 초기화된다.
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
            if reason == "STOP" and pnl < 0:
                count += 1
                continue
            if reason == "BE_EXIT" or reason.startswith("FLAT_EXIT") or reason == "TIME_EXIT":
                continue
            # 알 수 없는 음수 종료는 안전하게 손절로 계산한다.
            if pnl < 0:
                count += 1
                continue
            break
        return count

    def loss_cooldown_active(self) -> bool:
        """v4.3.53: 3연속 STOP에 따른 전체 신규진입 쿨다운은 사용하지 않는다.

        config.json에 예전 loss_cooldown_minutes=60 값이 남아 있어도 무시한다.
        실제 STOP/일일손실한도/동일종목 쿨다운 등 다른 안전장치는 그대로 유지한다.
        """
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

    def _private_v5(self, method: str, path: str, params: dict[str, Any]) -> dict[str, Any]:
        """Bybit v5 private REST. Exchange TP pre-orders only."""
        api_key = os.getenv("BYBIT_API_KEY", "").strip()
        api_secret = os.getenv("BYBIT_API_SECRET", "").strip()
        if not api_key or not api_secret:
            raise BybitSwingError("TP 선주문용 BYBIT_API_KEY/BYBIT_API_SECRET이 없습니다.")

        method = method.upper()
        ts = str(int(time.time() * 1000))
        recv_window = "5000"
        base_url = "https://api.bybit.com" if self.cfg.mode == "live" else "https://api-demo.bybit.com"

        if method == "GET":
            query = urllib.parse.urlencode(sorted((k, str(v)) for k, v in params.items()))
            payload = query
            url = base_url + path + (("?" + query) if query else "")
            data = None
        else:
            payload = json.dumps(params, separators=(",", ":"), ensure_ascii=False)
            url = base_url + path
            data = payload.encode("utf-8")

        sign_text = ts + api_key + recv_window + payload
        signature = hmac.new(
            api_secret.encode("utf-8"), sign_text.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        headers = {
            "X-BAPI-API-KEY": api_key,
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-RECV-WINDOW": recv_window,
            "X-BAPI-SIGN": signature,
            "Content-Type": "application/json",
        }
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                result = json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            raise BybitSwingError(f"Bybit TP API 통신 오류: {exc}") from exc
        if int(result.get("retCode", -1)) != 0:
            raise BybitSwingError(f"Bybit TP API 오류 {result.get('retCode')}: {result.get('retMsg')}")
        return result

    def _instrument_steps(self, symbol: str) -> tuple[float, float]:
        """Return (tick_size, qty_step) for linear USDT contract."""
        url = (
            "https://api.bybit.com/v5/market/instruments-info?"
            + urllib.parse.urlencode({"category": "linear", "symbol": symbol})
        )
        try:
            with urllib.request.urlopen(url, timeout=8) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            rows = payload.get("result", {}).get("list", [])
            if rows:
                tick = float(rows[0].get("priceFilter", {}).get("tickSize") or 0)
                step = float(rows[0].get("lotSizeFilter", {}).get("qtyStep") or 0)
                if tick > 0 and step > 0:
                    return tick, step
        except Exception:
            pass
        return 1e-8, 1e-8

    @staticmethod
    def _step_floor(value: float, step: float) -> float:
        if step <= 0:
            return value
        return math.floor((value + 1e-12) / step) * step

    @staticmethod
    def _step_round(value: float, step: float) -> float:
        if step <= 0:
            return value
        return round(value / step) * step

    def _tp_link(self, symbol: str, entry_ts_ms: int, level: int) -> str:
        return f"SWTP{level}{str(entry_ts_ms)[-10:]}{symbol[:10]}"[:36]

    def _cancel_exchange_tp_orders(self, symbol: str, entry_ts_ms: int) -> None:
        if self.cfg.mode == "paper" or not self.cfg.exchange_tp_preorders_enabled:
            return
        for level in (1, 2):
            try:
                self._private_v5("POST", "/v5/order/cancel", {
                    "category": "linear",
                    "symbol": symbol,
                    "orderLinkId": self._tp_link(symbol, entry_ts_ms, level),
                })
            except Exception:
                # 이미 체결/취소된 주문은 취소 실패가 정상일 수 있다.
                pass

    def _place_exchange_tp_orders(
        self, symbol: str, base_price: float, total_qty: float, entry_ts_ms: int
    ) -> None:
        """Place TP1 50% + TP2 remainder as reduce-only limit orders on Bybit."""
        if self.cfg.mode == "paper" or not self.cfg.exchange_tp_preorders_enabled:
            return
        if total_qty <= 0 or base_price <= 0:
            return

        tick, qty_step = self._instrument_steps(symbol)
        total_qty = self._step_floor(total_qty, qty_step)
        if total_qty <= 0:
            return
        qty1 = self._step_floor(total_qty * 0.5, qty_step)
        qty2 = self._step_floor(total_qty - qty1, qty_step)
        if qty1 <= 0 or qty2 <= 0:
            raise BybitSwingError(f"{symbol} TP 수량이 최소 주문단위보다 작습니다.")

        p1 = self._step_round(base_price * (1 + self.cfg.tp1_pct / 100), tick)
        p2 = self._step_round(base_price * (1 + self.cfg.tp2_pct / 100), tick)

        # 수량 변경(추가/회수) 때 호출될 수 있으므로 기존 TP를 먼저 정리한다.
        self._cancel_exchange_tp_orders(symbol, entry_ts_ms)

        for level, qty, target in ((1, qty1, p1), (2, qty2, p2)):
            self._private_v5("POST", "/v5/order/create", {
                "category": "linear",
                "symbol": symbol,
                "side": "Sell",
                "orderType": "Limit",
                "qty": f"{qty:.12f}".rstrip("0").rstrip("."),
                "price": f"{target:.12f}".rstrip("0").rstrip("."),
                "timeInForce": "GTC",
                "positionIdx": 0,
                "reduceOnly": True,
                "closeOnTrigger": False,
                "orderLinkId": self._tp_link(symbol, entry_ts_ms, level),
            })

        log_event(
            symbol, "EXCHANGE_TP_PLACED", base_price, total_qty, self.cfg.mode,
            details=json.dumps({
                "tp1_price": p1, "tp1_qty": qty1,
                "tp2_price": p2, "tp2_qty": qty2,
                "source": "bybit_reduce_only_limit",
            }, ensure_ascii=False)
        )

    def _set_exchange_breakeven_stop(
        self, symbol: str, base_price: float, entry_ts_ms: int
    ) -> float:
        """After TP1, arm an exchange-side full-position market stop above actual entry."""
        if self.cfg.mode != "live":
            return 0.0
        if base_price <= 0:
            raise BybitSwingError(f"{symbol}: 본절보호 기준 평단이 올바르지 않습니다.")

        tick, _ = self._instrument_steps(symbol)
        # 롱 보호스탑은 실제 체결평단보다 breakeven_stop_pct 만큼 위에 둔다.
        stop_price = self._step_round(
            base_price * (1 + float(self.cfg.breakeven_stop_pct) / 100.0),
            tick,
        )
        if stop_price <= 0:
            raise BybitSwingError(f"{symbol}: 본절보호 스탑 가격 계산 실패")

        self._private_v5("POST", "/v5/position/trading-stop", {
            "category": "linear",
            "symbol": symbol,
            "tpslMode": "Full",
            "stopLoss": f"{stop_price:.12f}".rstrip("0").rstrip("."),
            "slTriggerBy": "LastPrice",
            "slOrderType": "Market",
            "positionIdx": 0,
        })

        state_set(f"exchange_be_{symbol}_{entry_ts_ms}", "1")
        log_event(
            symbol, "EXCHANGE_BE_ARMED", stop_price, 0, self.cfg.mode,
            details=json.dumps({
                "base_price": base_price,
                "stop_price": stop_price,
                "buffer_pct": float(self.cfg.breakeven_stop_pct),
                "source": "bybit_trading_stop",
            }, ensure_ascii=False),
        )
        return stop_price

    def _live_position_snapshot(self, symbol: str) -> dict[str, float]:
        """Return the live long position size/average price from Bybit."""
        payload = self._private_v5("GET", "/v5/position/list", {
            "category": "linear",
            "symbol": symbol,
        })
        for item in payload.get("result", {}).get("list", []):
            if str(item.get("side") or "").lower() == "buy" and int(item.get("positionIdx") or 0) in (0, 1):
                return {
                    "qty": float(item.get("size") or 0),
                    "avg_price": float(item.get("avgPrice") or 0),
                    "cur_realized_pnl": float(item.get("curRealisedPnl") or 0),
                }
        return {"qty": 0.0, "avg_price": 0.0, "cur_realized_pnl": 0.0}

    def _live_position_qty(self, symbol: str) -> float:
        return float(self._live_position_snapshot(symbol).get("qty") or 0)

    def _wait_live_position_snapshot(self, symbol: str, min_qty: float = 0.0, tries: int = 20) -> dict[str, float]:
        """Poll briefly after a market order so DB/TP use the actual Bybit fill average."""
        last = {"qty": 0.0, "avg_price": 0.0, "cur_realized_pnl": 0.0}
        for _ in range(max(1, int(tries))):
            last = self._live_position_snapshot(symbol)
            qty = float(last.get("qty") or 0)
            avg = float(last.get("avg_price") or 0)
            if qty > max(0.0, float(min_qty)) and avg > 0:
                return last
            time.sleep(0.20)
        return last

    def _live_closed_pnl_summary(self, symbol: str, entry_ts_ms: int) -> tuple[float, float]:
        """Return Bybit Closed P&L total and latest actual exit price for this trade.

        Bybit Closed P&L is the account-side realised result including trading/funding fees.
        The bot trade duration is well below the API's 7-day query-window limit.
        """
        start_ms = max(0, int(entry_ts_ms) - 5000)
        end_ms = int(time.time() * 1000)
        payload = self._private_v5("GET", "/v5/position/closed-pnl", {
            "category": "linear",
            "symbol": symbol,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": 100,
        })
        rows = payload.get("result", {}).get("list", [])
        total = 0.0
        latest_price = 0.0
        latest_ts = -1
        for item in rows:
            try:
                created = int(item.get("createdTime") or item.get("updatedTime") or 0)
            except Exception:
                created = 0
            if created and created < start_ms:
                continue
            total += float(item.get("closedPnl") or 0)
            if created >= latest_ts:
                latest_ts = created
                latest_price = float(item.get("avgExitPrice") or 0)
        return total, latest_price

    def _wait_live_closed_pnl_summary(
        self, symbol: str, entry_ts_ms: int, previous_total: float, tries: int = 15
    ) -> tuple[float, float]:
        """Poll briefly because Closed P&L can appear just after a market/TP fill."""
        last_total, last_price = float(previous_total), 0.0
        for _ in range(max(1, int(tries))):
            last_total, last_price = self._live_closed_pnl_summary(symbol, entry_ts_ms)
            if abs(last_total - float(previous_total)) > 1e-10:
                return last_total, last_price
            time.sleep(0.20)
        return last_total, last_price

    def _sync_exchange_tp_fill(self, row: sqlite3.Row, price: float) -> sqlite3.Row | None:
        """Reconcile exchange-side TP fills into local DB before local stop/TP logic."""
        if self.cfg.mode != "live" or not self.cfg.exchange_tp_preorders_enabled:
            return row

        now_ts = time.time()
        key = f"tp_sync_{row['symbol']}"
        try:
            last = float(state_get(key, "0") or 0)
        except Exception:
            last = 0.0
        if now_ts - last < max(2.0, float(self.cfg.exchange_tp_sync_seconds)):
            return row
        state_set(key, str(now_ts))

        try:
            actual_qty = self._live_position_qty(row["symbol"])
        except Exception as exc:
            log_event(
                row["symbol"], "TP_SYNC_ERROR", price, 0, self.cfg.mode,
                details=str(exc), strategy=row["strategy"] or "", trade_id=row["trade_id"] or ""
            )
            return row

        db_qty = float(row["total_qty"] or 0)
        base_qty = float(row["base_qty"] or db_qty)
        base_price = float(row["base_entry_price"] or row["avg_price"])
        tp1_price = base_price * (1 + self.cfg.tp1_pct / 100)
        tp2_price = base_price * (1 + self.cfg.tp2_pct / 100)
        tol = max(base_qty * 0.03, 1e-12)

        # 거래소 포지션이 사라졌다면 실제 종료 체결가로 TP2 / 본절보호를 구분한다.
        # TP1 이후에는 거래소 본절보호 스탑도 포지션을 0으로 만들기 때문에
        # tp1_done만 보고 무조건 TP2로 기록하면 안 된다.
        if actual_qty <= tol and db_qty > tol:
            if int(row["tp1_done"] or 0) == 1 or price >= tp2_price * 0.995:
                previous_realized = float(row["realized_pnl"] or 0)
                actual_exit_price = 0.0
                try:
                    total_realized, actual_exit_price = self._wait_live_closed_pnl_summary(
                        row["symbol"], int(row["entry_ts_ms"] or 0), previous_realized
                    )
                    realized_step = total_realized - previous_realized
                except Exception as exc:
                    log_event(row["symbol"], "LIVE_PNL_SYNC_ERROR", price, 0, self.cfg.mode, details=str(exc), strategy=row["strategy"] or "", trade_id=row["trade_id"] or "")
                    total_realized = previous_realized
                    realized_step = 0.0

                be_price = base_price * (1 + self.cfg.breakeven_stop_pct / 100)
                exit_price = actual_exit_price if actual_exit_price > 0 else price

                # 실제 종료가가 TP2와 본절보호 중 어느 가격에 더 가까운지로 종료 사유를 판정한다.
                # Bybit 시장가 스탑은 약간의 슬리피지가 생길 수 있어 정확히 같은 가격일 필요는 없다.
                if int(row["tp1_done"] or 0) == 1:
                    dist_tp2 = abs(exit_price - tp2_price)
                    dist_be = abs(exit_price - be_price)
                    close_reason = "TP2" if dist_tp2 <= dist_be else "BE_EXIT"
                else:
                    close_reason = "TP2"

                # Closed P&L 동기화 실패 시에만 기존 계산값을 안전망으로 사용한다.
                if actual_exit_price <= 0 and abs(total_realized - previous_realized) <= 1e-10:
                    realized_step = (exit_price - float(row["avg_price"])) * db_qty
                    total_realized = previous_realized + realized_step

                with db() as conn:
                    conn.execute(
                        "UPDATE bot_positions SET status='CLOSED',total_qty=0,tp1_done=1,updated_at=?,last_price=?,note=?,realized_pnl=? WHERE symbol=?",
                        (utc_now(), exit_price, close_reason, total_realized, row["symbol"]),
                    )
                log_event(
                    row["symbol"], close_reason, exit_price, db_qty, self.cfg.mode,
                    details=json.dumps({
                        "source": "exchange_position_zero_sync",
                        "actual_exit_price": actual_exit_price,
                        "tp2_price": tp2_price,
                        "be_price": be_price,
                    }, ensure_ascii=False),
                    strategy=row["strategy"] or "", realized_pnl=realized_step,
                    trade_id=row["trade_id"] or ""
                )
                return None
            # TP로 확정할 수 없는 외부 종료는 중복 주문 방지를 위해 로컬도 닫는다.
            with db() as conn:
                conn.execute(
                    "UPDATE bot_positions SET status='CLOSED',total_qty=0,updated_at=?,last_price=?,note=? WHERE symbol=?",
                    (utc_now(), price, "EXTERNAL_CLOSE_SYNC", row["symbol"]),
                )
            return None

        # 거래소 수량이 대략 절반으로 줄었다면 TP1 선주문 체결로 본다.
        if int(row["tp1_done"] or 0) == 0 and actual_qty < db_qty - tol and actual_qty <= base_qty * 0.60 + tol:
            closed_qty = max(0.0, db_qty - actual_qty)
            previous_realized = float(row["realized_pnl"] or 0)
            try:
                total_realized, actual_exit_price = self._wait_live_closed_pnl_summary(
                    row["symbol"], int(row["entry_ts_ms"] or 0), previous_realized
                )
                realized_step = total_realized - previous_realized
                if actual_exit_price > 0:
                    tp1_price = actual_exit_price
            except Exception as exc:
                log_event(row["symbol"], "LIVE_PNL_SYNC_ERROR", price, 0, self.cfg.mode, details=str(exc), strategy=row["strategy"] or "", trade_id=row["trade_id"] or "")
                realized_step = (tp1_price - float(row["avg_price"])) * closed_qty
                total_realized = previous_realized + realized_step
            with db() as conn:
                conn.execute(
                    "UPDATE bot_positions SET total_qty=?,tp1_done=1,updated_at=?,last_price=?,note=?,realized_pnl=? WHERE symbol=?",
                    (actual_qty, utc_now(), tp1_price, "TP1", total_realized, row["symbol"]),
                )

            # TP1 체결을 확인한 즉시 남은 물량의 본절보호를 거래소에 직접 등록한다.
            try:
                snap = self._live_position_snapshot(row["symbol"])
                live_avg = float(snap.get("avg_price") or 0)
                be_base = live_avg if live_avg > 0 else float(row["avg_price"] or base_price)
                self._set_exchange_breakeven_stop(
                    row["symbol"], be_base, int(row["entry_ts_ms"] or 0)
                )
            except Exception as exc:
                # 실패 시 아래 로컬 BE 감시가 안전망으로 남는다.
                log_event(
                    row["symbol"], "EXCHANGE_BE_ERROR", price, 0, self.cfg.mode,
                    details=str(exc), strategy=row["strategy"] or "",
                    trade_id=row["trade_id"] or "",
                )

            log_event(
                row["symbol"], "TP1", tp1_price, closed_qty, self.cfg.mode,
                details=json.dumps({"source": "exchange_preorder_sync"}, ensure_ascii=False),
                strategy=row["strategy"] or "", realized_pnl=realized_step,
                trade_id=row["trade_id"] or ""
            )
            with db() as conn:
                return conn.execute("SELECT * FROM bot_positions WHERE symbol=?", (row["symbol"],)).fetchone()

        return row

    def _execute(self, symbol: str, side: str, qty: float, reduce_only: bool = False) -> float:
        if self.cfg.mode == "paper":
            return float(qty)
        if self.cfg.mode == "live" and not self.client.private_configured:
            raise BybitSwingError("LIVE 모드인데 API 설정이 없습니다.")

        # LIVE 주문 전 종목별 주문 규칙을 조회한다.
        # 신규 BUY는 설정 레버리지(기본 5배)와 거래소 허용 최대 레버리지 중 낮은 값을 사용하고,
        # 모든 주문 수량은 해당 종목의 qtyStep에 맞춰 내림 처리한다.
        url = (
            "https://api.bybit.com/v5/market/instruments-info?"
            + urllib.parse.urlencode({"category": "linear", "symbol": symbol})
        )
        try:
            with urllib.request.urlopen(url, timeout=8) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            rows = payload.get("result", {}).get("list", [])
            if not rows:
                raise BybitSwingError(f"{symbol}: 주문 규칙을 찾지 못했습니다.")

            rule = rows[0]
            lot = rule.get("lotSizeFilter", {})
            lev_filter = rule.get("leverageFilter", {})
            qty_step = float(lot.get("qtyStep") or 0)
            min_qty = float(lot.get("minOrderQty") or qty_step or 0)
            max_leverage = float(lev_filter.get("maxLeverage") or self.cfg.leverage)

            if qty_step <= 0:
                raise BybitSwingError(f"{symbol}: qtyStep 확인 실패")

            effective_leverage = min(float(self.cfg.leverage), max_leverage)
            if effective_leverage <= 0:
                raise BybitSwingError(f"{symbol}: 사용 가능한 레버리지를 확인하지 못했습니다.")

            order_qty = float(qty)
            if side.lower() == "buy" and not reduce_only:
                # qty는 설정 레버리지 기준으로 계산되어 들어오므로,
                # 거래소 최대 레버리지가 더 낮은 종목은 같은 증거금 기준으로 수량을 축소한다.
                order_qty *= effective_leverage / float(self.cfg.leverage)

            order_qty = math.floor((order_qty + 1e-12) / qty_step) * qty_step
            if order_qty <= 0:
                raise BybitSwingError(f"{symbol}: 보정 후 주문수량이 0입니다.")
            if side.lower() == "buy" and not reduce_only and order_qty < min_qty:
                raise BybitSwingError(
                    f"{symbol}: 주문수량 {order_qty}가 최소수량 {min_qty}보다 작습니다."
                )

            if not reduce_only:
                try:
                    self.client.set_leverage(
                        symbol, effective_leverage, self.cfg.margin_mode, "long"
                    )
                except BybitSwingError as exc:
                    msg = str(exc).lower()
                    # 이미 동일한 레버리지면 오류가 아니라 정상 상태로 보고 주문을 계속한다.
                    if "110043" not in msg and "not modified" not in msg:
                        raise

            qty_text = f"{order_qty:.12f}".rstrip("0").rstrip(".")
            self.client.place_market_order(
                symbol, side, qty_text, self.cfg.margin_mode, "long", reduce_only,
                client_order_id=f"HJ{int(time.time())}{symbol[:4]}"
            )
            return float(order_qty)

        except BybitSwingError:
            raise
        except Exception as exc:
            raise BybitSwingError(f"{symbol}: LIVE 주문규칙 처리 오류: {exc}") from exc

    def _open(self, symbol: str, price: float, strategy: str, score: float, signal_details: dict[str, Any] | None = None) -> None:
        entry_margin = self.cfg.hj_position_margin_usdt if strategy == "HJ" else self.cfg.position_margin_usdt
        requested_qty = qty_from_margin(price, entry_margin, self.cfg.leverage)
        entry_ts_ms = int(time.time() * 1000)
        executed_qty = self._execute(symbol, "buy", requested_qty)

        actual_entry_price = float(price)
        actual_qty = float(executed_qty)
        if self.cfg.mode == "live":
            try:
                snap = self._wait_live_position_snapshot(symbol, min_qty=0.0)
                if float(snap.get("avg_price") or 0) > 0:
                    actual_entry_price = float(snap["avg_price"])
                if float(snap.get("qty") or 0) > 0:
                    actual_qty = float(snap["qty"])
            except Exception as exc:
                log_event(symbol, "LIVE_FILL_SYNC_ERROR", price, executed_qty, self.cfg.mode, details=str(exc), strategy=strategy)

        trade_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
        with db() as conn:
            conn.execute("DELETE FROM bot_positions WHERE symbol=?", (symbol,))
            conn.execute(
                """INSERT INTO bot_positions(
                    symbol,status,opened_at,updated_at,avg_price,total_qty,total_margin,dca_count,tp1_done,
                    last_price,unrealized_pct,note,strategy,realized_pnl,entry_date_kst,
                    base_entry_price,base_qty,add_qty,add_price,lowest_price,highest_price,cycle_anchor_price,trade_id,stop_stage1_done,entry_ts_ms
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (symbol, "OPEN", utc_now(), utc_now(), actual_entry_price, actual_qty, entry_margin, 0, 0,
                 actual_entry_price, 0.0, f"{strategy}형 데일리 진입", strategy, 0.0, trading_day(),
                 actual_entry_price, actual_qty, 0.0, 0.0, actual_entry_price, actual_entry_price, actual_entry_price, trade_id, 0, entry_ts_ms),
            )
        signal_details = signal_details or {}
        details = json.dumps({
            "signal_price": price, "entry_price": actual_entry_price, "actual_fill_price": actual_entry_price,
            "margin_usdt": entry_margin, "leverage": self.cfg.leverage, "qty": actual_qty,
            "requested_qty": requested_qty, "executed_qty": executed_qty, "score": round(score, 2),
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
        log_event(symbol, "ENTRY", actual_entry_price, actual_qty, self.cfg.mode, details, strategy, trade_id=trade_id)
        try:
            self._place_exchange_tp_orders(symbol, actual_entry_price, actual_qty, entry_ts_ms)
        except Exception as exc:
            log_event(
                symbol, "EXCHANGE_TP_PLACE_ERROR", actual_entry_price, actual_qty, self.cfg.mode,
                details=str(exc), strategy=strategy, trade_id=trade_id
            )

    def _rebound_add(self, row: sqlite3.Row, price: float) -> None:
        old_avg = float(row["avg_price"])
        # HJ/P 공통 안전장치: 반등이 확인되어도 현재 평단 이상에서는 절대 추가하지 않는다.
        if float(price) >= old_avg:
            return
        add_margin = self.cfg.hj_rebound_add_margin_usdt if str(row["strategy"] or "") == "HJ" else self.cfg.rebound_add_margin_usdt
        add_qty = qty_from_margin(price, add_margin, self.cfg.leverage)
        if add_qty <= 0:
            return
        executed_add_qty = self._execute(row["symbol"], "buy", add_qty)
        old_qty = float(row["total_qty"])
        new_qty = old_qty + executed_add_qty
        new_avg = (old_avg * old_qty + price * executed_add_qty) / new_qty
        actual_add_price = float(price)
        if self.cfg.mode == "live":
            try:
                snap = self._wait_live_position_snapshot(row["symbol"], min_qty=old_qty)
                if float(snap.get("qty") or 0) > old_qty:
                    new_qty = float(snap["qty"])
                if float(snap.get("avg_price") or 0) > 0:
                    new_avg = float(snap["avg_price"])
                    if executed_add_qty > 0:
                        actual_add_price = max(0.0, (new_avg * new_qty - old_avg * old_qty) / executed_add_qty)
            except Exception as exc:
                log_event(row["symbol"], "LIVE_FILL_SYNC_ERROR", price, executed_add_qty, self.cfg.mode, details=str(exc), strategy=row["strategy"] or "", trade_id=row["trade_id"] or "")
        now = datetime.now(timezone.utc)
        add_bucket = str(int(now.timestamp()) // 900)
        with db() as conn:
            conn.execute(
                """UPDATE bot_positions SET avg_price=?,total_qty=?,total_margin=?,dca_count=?,
                   add_qty=?,add_price=?,updated_at=?,last_price=?,note=?,last_add_15m_bucket=? WHERE symbol=?""",
                (new_avg, new_qty, float(row["total_margin"]) + add_margin,
                 int(row["dca_count"] or 0) + 1, executed_add_qty, actual_add_price, utc_now(), actual_add_price,
                 "반등 확인 후 순환 추가진입", add_bucket, row["symbol"]),
            )
        details = json.dumps({"signal_add_price": price, "add_price": actual_add_price, "add_margin_usdt": add_margin,
                              "add_qty": executed_add_qty, "previous_avg": old_avg, "new_avg": new_avg,
                              "cycle_no": int(row["dca_count"] or 0) + 1}, ensure_ascii=False)
        log_event(row["symbol"], "REBOUND_ADD", actual_add_price, executed_add_qty, self.cfg.mode,
                  details=details, strategy=row["strategy"] or "", trade_id=row["trade_id"] or "")
        try:
            self._place_exchange_tp_orders(
                row["symbol"], float(row["base_entry_price"] or old_avg), new_qty, int(row["entry_ts_ms"] or 0)
            )
        except Exception as exc:
            log_event(row["symbol"], "EXCHANGE_TP_REFRESH_ERROR", price, new_qty, self.cfg.mode,
                      details=str(exc), strategy=row["strategy"] or "", trade_id=row["trade_id"] or "")

    def _cycle_reduce(self, row: sqlite3.Row, price: float) -> None:
        add_qty = float(row["add_qty"] or 0)
        if add_qty <= 0:
            return
        executed_qty = self._execute(row["symbol"], "sell", add_qty, reduce_only=True)
        remaining = max(0.0, float(row["total_qty"]) - executed_qty)
        base_price = float(row["base_entry_price"] or row["avg_price"])
        previous_realized = float(row["realized_pnl"] or 0)
        actual_exit_price = float(price)
        if self.cfg.mode == "live":
            try:
                total_realized, live_exit_price = self._wait_live_closed_pnl_summary(
                    row["symbol"], int(row["entry_ts_ms"] or 0), previous_realized
                )
                pnl_usdt = total_realized - previous_realized
                if live_exit_price > 0:
                    actual_exit_price = live_exit_price
                snap = self._live_position_snapshot(row["symbol"])
                remaining = float(snap.get("qty") or remaining)
            except Exception as exc:
                log_event(row["symbol"], "LIVE_PNL_SYNC_ERROR", price, executed_qty, self.cfg.mode, details=str(exc), strategy=row["strategy"] or "", trade_id=row["trade_id"] or "")
                pnl_usdt = (price - float(row["add_price"] or row["avg_price"])) * executed_qty
                total_realized = previous_realized + pnl_usdt
        else:
            pnl_usdt = (price - float(row["add_price"] or row["avg_price"])) * executed_qty
            total_realized = previous_realized + pnl_usdt
        with db() as conn:
            conn.execute(
                """UPDATE bot_positions SET avg_price=?,total_qty=?,total_margin=?,add_qty=0,add_price=0,
                   updated_at=?,last_price=?,note=?,realized_pnl=?,lowest_price=?,highest_price=?,cycle_anchor_price=? WHERE symbol=?""",
                (base_price, remaining, max(0.0, float(row["total_margin"]) - (self.cfg.hj_rebound_add_margin_usdt if str(row["strategy"] or "") == "HJ" else self.cfg.rebound_add_margin_usdt)),
                 utc_now(), price, "순환 추가분 정리 · 최초 물량 유지", total_realized,
                 price, price, price, row["symbol"]),
            )
        details = json.dumps({"add_entry_price": float(row["add_price"] or 0), "reduce_price": actual_exit_price,
                              "reduced_qty": executed_qty, "avg_before_reduce": float(row["avg_price"]),
                              "restored_base_avg": base_price, "remaining_qty": remaining,
                              "cycle_realized_pnl": pnl_usdt}, ensure_ascii=False)
        log_event(row["symbol"], "CYCLE_REDUCE", actual_exit_price, executed_qty, self.cfg.mode,
                  details=details, strategy=row["strategy"] or "", realized_pnl=pnl_usdt,
                  trade_id=row["trade_id"] or "")
        try:
            self._place_exchange_tp_orders(
                row["symbol"], base_price, remaining, int(row["entry_ts_ms"] or 0)
            )
        except Exception as exc:
            log_event(row["symbol"], "EXCHANGE_TP_REFRESH_ERROR", price, remaining, self.cfg.mode,
                      details=str(exc), strategy=row["strategy"] or "", trade_id=row["trade_id"] or "")

    def _close(self, row: sqlite3.Row, price: float, fraction: float, reason: str,
               detected_price: float | None = None, trigger_price: float | None = None) -> None:
        current_qty = float(row["total_qty"])
        requested_qty = current_qty * fraction
        if reason not in {"TP1", "TP2"}:
            self._cancel_exchange_tp_orders(row["symbol"], int(row["entry_ts_ms"] or 0))
        executed_qty = self._execute(row["symbol"], "sell", requested_qty, reduce_only=True)

        previous_realized = float(row["realized_pnl"] or 0)
        actual_exit_price = float(price)
        remaining = max(0.0, current_qty - executed_qty)
        if self.cfg.mode == "live":
            try:
                total_realized, live_exit_price = self._wait_live_closed_pnl_summary(
                    row["symbol"], int(row["entry_ts_ms"] or 0), previous_realized
                )
                pnl_usdt = total_realized - previous_realized
                if live_exit_price > 0:
                    actual_exit_price = live_exit_price
                snap = self._live_position_snapshot(row["symbol"])
                remaining = float(snap.get("qty") or 0)
            except Exception as exc:
                log_event(row["symbol"], "LIVE_PNL_SYNC_ERROR", price, executed_qty, self.cfg.mode, details=str(exc), strategy=row["strategy"] or "", trade_id=row["trade_id"] or "")
                pnl_usdt = (price - float(row["avg_price"])) * executed_qty
                total_realized = previous_realized + pnl_usdt
        else:
            pnl_usdt = (price - float(row["avg_price"])) * executed_qty
            total_realized = previous_realized + pnl_usdt

        with db() as conn:
            if remaining <= 1e-12:
                conn.execute(
                    "UPDATE bot_positions SET status='CLOSED',total_qty=0,updated_at=?,last_price=?,note=?,realized_pnl=? WHERE symbol=?",
                    (utc_now(), actual_exit_price, reason, total_realized, row["symbol"]),
                )
            else:
                if reason == "TP1":
                    conn.execute(
                        "UPDATE bot_positions SET total_qty=?,tp1_done=1,updated_at=?,last_price=?,note=?,realized_pnl=? WHERE symbol=?",
                        (remaining, utc_now(), actual_exit_price, reason, total_realized, row["symbol"]),
                    )
                elif reason == "STOP_HALF":
                    conn.execute(
                        "UPDATE bot_positions SET total_qty=?,stop_stage1_done=1,updated_at=?,last_price=?,note=?,realized_pnl=? WHERE symbol=?",
                        (remaining, utc_now(), actual_exit_price, reason, total_realized, row["symbol"]),
                    )
                else:
                    conn.execute(
                        "UPDATE bot_positions SET total_qty=?,updated_at=?,last_price=?,note=?,realized_pnl=? WHERE symbol=?",
                        (remaining, utc_now(), actual_exit_price, reason, total_realized, row["symbol"]),
                    )
        details = json.dumps({"exit_price": actual_exit_price,
                              "detected_market_price": detected_price if detected_price is not None else price,
                              "configured_trigger_price": trigger_price,
                              "avg_at_exit": float(row["avg_price"]),
                              "base_entry_price": float(row["base_entry_price"] or row["avg_price"]),
                              "closed_qty": executed_qty, "requested_qty": requested_qty,
                              "fraction": fraction, "remaining_qty": remaining,
                              "step_realized_pnl": pnl_usdt, "trade_total_realized_pnl": total_realized,
                              "pnl_source": "bybit_closed_pnl" if self.cfg.mode == "live" else "paper_calculation",
                              "reason": reason}, ensure_ascii=False)
        log_event(row["symbol"], reason, actual_exit_price, executed_qty, self.cfg.mode, details=details,
                  strategy=row["strategy"] or "", realized_pnl=pnl_usdt,
                  trade_id=row["trade_id"] or "")
        self._register_stop_review(row, reason, actual_exit_price)

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

            # v4.3.52: 실제 TP1→BE_EXIT로 끝난 거래만 shadow 추적한다.
            # 실제 주문/포지션에는 전혀 영향 없이, BE가 없었다고 가정했을 때
            # TP2와 TP1 이후 공통 구조손절 중 무엇이 먼저였는지 기록한다.
            if stop_event == "BE_EXIT" and entry_price > 0:
                tp2_price = entry_price * (1 + float(self.cfg.tp2_pct) / 100)
                started_at = utc_now()
                conn.execute(
                    """INSERT OR IGNORE INTO be_shadow_reviews(
                        trade_id,symbol,strategy,opened_at,shadow_started_at,entry_price,
                        be_exit_price,tp2_price,highest_price,lowest_price,last_price,last_checked_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        str(row["trade_id"] or ""),
                        str(row["symbol"]),
                        str(row["strategy"] or ""),
                        str(row["opened_at"]),
                        started_at,
                        entry_price,
                        float(stop_price),
                        tp2_price,
                        float(stop_price),
                        float(stop_price),
                        float(stop_price),
                        started_at,
                    ),
                )

    def update_be_shadow_reviews(self) -> None:
        """TP1→BE_EXIT 이후를 가상 추적한다 (실제 주문 없음).

        가정은 딱 하나다: TP1은 실제처럼 이미 체결됐고, +0.15% BE만 없었다.
        그 상태에서 실제 봇의 TP1 이후 공통 구조손절과 TP2(+3%) 중
        무엇이 먼저 발생하는지, 또는 원래 3시간 TIME_EXIT까지 둘 다 없는지 기록한다.
        """
        with db() as conn:
            pending = conn.execute(
                "SELECT * FROM be_shadow_reviews WHERE completed=0 ORDER BY shadow_started_at"
            ).fetchall()
        if not pending:
            return

        # shadow가 여러 개여도 현재가는 public tickers 한 번으로 묶어 조회해
        # LIVE 주문/스캔 API에 불필요한 부담을 주지 않는다.
        try:
            ticker_map = {
                str(t.get("symbol") or ""): float(t.get("last") or 0)
                for t in self.client.tickers("SWAP")
            }
        except Exception as exc:
            log_event("", "BE_SHADOW_TICKER_ERROR", mode=self.cfg.mode, details=str(exc))
            return

        now = datetime.now(timezone.utc)
        current_bucket = str(int(now.timestamp()) // 900)

        for review in pending:
            symbol = str(review["symbol"] or "")
            trade_id = str(review["trade_id"] or "")
            try:
                price = float(ticker_map.get(symbol) or 0)
                if price <= 0:
                    continue
                entry_price = float(review["entry_price"] or 0)
                tp2_price = float(review["tp2_price"] or 0)
                highest = max(float(review["highest_price"] or price), price)
                lowest = min(float(review["lowest_price"] or price), price)

                opened = datetime.fromisoformat(str(review["opened_at"]))
                if opened.tzinfo is None:
                    opened = opened.replace(tzinfo=timezone.utc)
                age_h = (now - opened).total_seconds() / 3600.0

                # 구조손절은 확정 15분봉 기준이라 15분 버킷이 바뀔 때만 재계산한다.
                # 단, -8% 재난손절은 live_price 기반이므로 즉시 확인한다.
                emergency_now = bool(
                    entry_price > 0
                    and price <= entry_price * (1 - abs(float(self.cfg.structure_emergency_stop_pct)) / 100)
                )
                structure_due = (str(review["last_structure_bucket"] or "") != current_bucket) or emergency_now
                broken = False
                structure = {}
                if self.cfg.hj_structure_stop_enabled and structure_due:
                    broken, structure = hj_structure_broken(
                        self.client, symbol, self.cfg,
                        base_price=entry_price, live_price=price
                    )

                tp2_hit = bool(tp2_price > 0 and price >= tp2_price)

                # 같은 샘플에서 둘 다 처음 보이면 선후를 확정할 수 없으므로 따로 표시한다.
                if tp2_hit and broken:
                    result = "AMBIGUOUS_TP2_OR_STOP"
                    result_price = price
                    details = {"structure": structure, "note": "same shadow sample"}
                elif tp2_hit:
                    result = "TP2_FIRST"
                    result_price = price
                    details = {"tp2_price": tp2_price}
                elif broken:
                    result = "STOP_FIRST"
                    result_price = float(structure.get("price") or price)
                    details = {"structure": structure}
                elif age_h >= float(self.cfg.max_hold_hours):
                    result = "TIME_EXIT_FIRST"
                    result_price = price
                    details = {"max_hold_hours": self.cfg.max_hold_hours}
                else:
                    with db() as conn:
                        conn.execute(
                            """UPDATE be_shadow_reviews SET highest_price=?,lowest_price=?,last_price=?,
                               last_checked_at=?,last_structure_bucket=? WHERE id=?""",
                            (
                                highest, lowest, price, utc_now(),
                                current_bucket if structure_due else str(review["last_structure_bucket"] or ""),
                                int(review["id"]),
                            ),
                        )
                    continue

                result_ts = utc_now()
                with db() as conn:
                    conn.execute(
                        """UPDATE be_shadow_reviews SET highest_price=?,lowest_price=?,last_price=?,
                           last_checked_at=?,last_structure_bucket=?,result=?,result_ts=?,result_price=?,
                           result_details=?,completed=1 WHERE id=?""",
                        (
                            highest, lowest, price, result_ts,
                            current_bucket if structure_due else str(review["last_structure_bucket"] or ""),
                            result, result_ts, float(result_price),
                            json.dumps(details, ensure_ascii=False), int(review["id"]),
                        ),
                    )
                log_event(
                    symbol, f"BE_SHADOW_{result}", float(result_price), 0, self.cfg.mode,
                    details=json.dumps({
                        "source": "tp1_be_shadow_only",
                        "entry_price": entry_price,
                        "be_exit_price": float(review["be_exit_price"] or 0),
                        "tp2_price": tp2_price,
                        "highest_price": highest,
                        "lowest_price": lowest,
                        "age_hours": round(age_h, 3),
                        **details,
                    }, ensure_ascii=False),
                    strategy=str(review["strategy"] or ""), trade_id=trade_id
                )
            except Exception as exc:
                log_event(
                    symbol, "BE_SHADOW_ERROR", mode=self.cfg.mode,
                    details=f"{type(exc).__name__}: {exc}", trade_id=trade_id
                )

    def _register_research_shadow_candidates(self, symbol: str, details: dict[str, Any]) -> None:
        """v4.3.63: 기존 연구군 + P_V22 대조군 + P_V23 dual-filter를 동일 시점에 가상진입해 사후 결과를 비교한다.

        실제 주문/실제 포지션 DB에는 영향을 주지 않는다. 같은 symbol+variant는 진행 중 1개만 유지한다.
        """
        if not self.cfg.research_shadow_enabled:
            return
        raw_variants = str(details.get("research_variants") or "")
        variants = [v.strip() for v in raw_variants.split(",") if v.strip()]
        # v4.3.66: variant가 없는 스캔에서도 활성 P_V24/P_V25 WATCH의 가격경로는 계속 추적해야 한다.
        price = float(details.get("live_price") or details.get("price") or 0)
        if price <= 0:
            return
        now = datetime.now(timezone.utc)
        old_score = float(details.get("p_score") or 0)
        new_score = float(details.get("new_p_score") or 0)
        snapshot = json.dumps(details, ensure_ascii=False, default=str)
        for variant in variants:
            if variant == "P_V24":
                # 기존 위험 setup이 WATCH 중이면 깨끗한 신호가 나와도 즉시 새 진입하지 않고
                # 같은 setup의 회복확인 절차를 끝까지 사용한다.
                if self._pv24_has_active_watch(symbol, now):
                    continue
                allowed, why = self._pv24_can_open_now(now)
                if not allowed:
                    append_entry_record(symbol, "RESEARCH_P_V24_SKIPPED", "P_V24", float(details.get("p_v2_score") or 0), price, why,
                        extra={**details, "p_v24_confirm_state": why, "p_setup_id": str(details.get("p_setup_id") or "")})
                    continue
            if variant == "JUNP_OLD":
                missing = str(details.get("junp_old_missing_condition") or "")
            elif variant == "JUNP_NEW":
                missing = str(details.get("junp_new_missing_condition") or "")
            elif variant == "JUNP_V2":
                missing = str(details.get("junp_v2_missing_components") or "")
            elif variant == "JUNP_V21":
                missing = str(details.get("junp_v21_missing_reason") or "")
            elif variant == "JUNP_V22":
                missing = str(details.get("junp_v22_missing_reason") or "")
            elif variant == "REJECT_NEARMISS":
                missing = str(details.get("p_failed_checks") or "")
            else:
                missing = ""
            variant_score = float(details.get("p_v2_score") or 0) if variant in ("P_V2", "JUNP_V2", "P_V21", "JUNP_V21", "P_V22", "P_V23", "P_V24", "P_V25", "JUNP_V22") else new_score
            with db() as conn:
                pending = conn.execute(
                    "SELECT 1 FROM research_shadow_reviews WHERE symbol=? AND variant=? AND completed=0 LIMIT 1",
                    (symbol, variant),
                ).fetchone()
                if pending:
                    continue
                if variant == "P_V24" and str(details.get("p_setup_id") or ""):
                    same_setup = conn.execute(
                        "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V24' AND setup_id=? LIMIT 1",
                        (str(details.get("p_setup_id") or ""),),
                    ).fetchone()
                    if same_setup:
                        continue
                last = conn.execute(
                    "SELECT result_ts FROM research_shadow_reviews WHERE symbol=? AND variant=? AND completed=1 ORDER BY result_ts DESC LIMIT 1",
                    (symbol, variant),
                ).fetchone()
                if last and last["result_ts"]:
                    try:
                        last_dt = datetime.fromisoformat(str(last["result_ts"]))
                        if last_dt.tzinfo is None:
                            last_dt = last_dt.replace(tzinfo=timezone.utc)
                        if now - last_dt < timedelta(minutes=max(0, int(self.cfg.research_shadow_same_symbol_cooldown_minutes))):
                            continue
                    except Exception:
                        pass
                opened_at = now.isoformat()
                shadow_id = f"RSH-{variant}-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
                tp2_price = price * (1 + float(self.cfg.tp2_pct) / 100)
                be_price = price * (1 + float(self.cfg.breakeven_stop_pct) / 100)
                conn.execute(
                    """INSERT INTO research_shadow_reviews(
                        shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,
                        missing_condition,snapshot_json,tp2_price,be_price,highest_price,lowest_price,last_price,
                        last_checked_at,mfe_pct,mae_pct,setup_id
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        shadow_id, variant, symbol, opened_at, int(now.timestamp()*1000), price,
                        old_score, variant_score, missing, snapshot, tp2_price, be_price,
                        price, price, price, opened_at, 0.0, 0.0, str(details.get("p_setup_id") or ""),
                    ),
                )
            append_entry_record(
                symbol, f"RESEARCH_{variant}_ENTRY", variant,
                variant_score if variant in ("P_V2", "JUNP_V2", "P_V21", "JUNP_V21", "P_V22", "P_V23", "P_V24", "P_V25", "JUNP_V22") else (new_score if "NEW" in variant or "STRENGTH" in variant else old_score),
                price,
                f"old_score={old_score:.2f}; new_score={new_score:.2f}; v2_score={float(details.get('p_v2_score') or 0):.2f}; missing={missing}; actual_order=0",
                extra={**details, "shadow_id": shadow_id, "research_variant": variant, "shadow_entry_time": _kst_stamp(opened_at)},
            )

        # v4.3.65+: P_V24는 코드 보존. V26 검증에서는 V22/V25/V26에 집중하도록 기본 OFF.
        if self.cfg.research_v23_v24_enabled:
            self._handle_pv24_watch(symbol, details)
        # v4.3.66+: P_V25 WATCH를 공통 기준으로 사용하고, 확인 순간 V26도 같은 가격/시점에서 분기한다.
        self._handle_pv25_watch(symbol, details)

    def _pv24_has_active_watch(self, symbol: str, now: datetime) -> bool:
        lock_cut = now - timedelta(minutes=max(1, int(self.cfg.research_pv24_same_setup_lock_minutes)))
        with db() as conn:
            row = conn.execute(
                "SELECT 1 FROM research_pv24_setups WHERE symbol=? AND status='WATCH' AND first_seen_at>=? ORDER BY first_seen_at DESC LIMIT 1",
                (symbol, lock_cut.isoformat()),
            ).fetchone()
        return bool(row)

    def _pv24_can_open_now(self, now: datetime) -> tuple[bool, str]:
        # v4.3.65: 고정 시계 구간이 아니라 직전 15분 rolling window로 제한한다.
        rolling_start = now - timedelta(minutes=15)
        stop_cut = now - timedelta(minutes=max(1, int(self.cfg.research_pv24_stop_pause_window_minutes)))
        with db() as conn:
            cnt = conn.execute(
                "SELECT COUNT(*) AS n FROM research_shadow_reviews WHERE variant='P_V24' AND opened_at>=?",
                (rolling_start.isoformat(),),
            ).fetchone()
            if int(cnt["n"] or 0) >= max(1, int(self.cfg.research_pv24_max_entries_per_15m)):
                return False, "rolling_15m_entry_cap"

            open_cnt = conn.execute(
                "SELECT COUNT(*) AS n FROM research_shadow_reviews WHERE variant='P_V24' AND completed=0"
            ).fetchone()
            if int(open_cnt["n"] or 0) >= max(1, int(self.cfg.research_pv24_max_open_positions)):
                return False, "max_open_positions"

            stops = conn.execute(
                "SELECT COUNT(*) AS n FROM research_shadow_reviews WHERE variant='P_V24' AND completed=1 AND result='STOP' AND result_ts>=?",
                (stop_cut.isoformat(),),
            ).fetchone()
            if int(stops["n"] or 0) >= max(1, int(self.cfg.research_pv24_stop_pause_count)):
                return False, "recent_stop_pause"
        return True, ""

    def _open_pv24_confirmed_shadow(self, symbol: str, details: dict[str, Any], setup_id: str, price: float, note: str) -> bool:
        now = datetime.now(timezone.utc)
        allowed, why = self._pv24_can_open_now(now)
        if not allowed:
            append_entry_record(symbol, "RESEARCH_P_V24_SKIPPED", "P_V24", float(details.get("p_v2_score") or 0), price, why, extra={**details, "p_v24_confirm_state": why, "p_setup_id": setup_id})
            return False
        with db() as conn:
            existing = conn.execute("SELECT 1 FROM research_shadow_reviews WHERE variant='P_V24' AND setup_id=? LIMIT 1", (setup_id,)).fetchone()
            if existing:
                return False
            opened_at = now.isoformat()
            shadow_id = f"RSH-P_V24-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = price * (1 + float(self.cfg.tp2_pct) / 100)
            be_price = price * (1 + float(self.cfg.breakeven_stop_pct) / 100)
            snap = dict(details); snap["p_v24_confirm_state"] = note
            conn.execute("""INSERT INTO research_shadow_reviews(
                shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,missing_condition,snapshot_json,
                tp2_price,be_price,highest_price,lowest_price,last_price,last_checked_at,mfe_pct,mae_pct,setup_id
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (shadow_id,"P_V24",symbol,opened_at,int(now.timestamp()*1000),price,float(details.get("p_score") or 0),float(details.get("p_v2_score") or 0),note,
                 json.dumps(snap,ensure_ascii=False,default=str),tp2_price,be_price,price,price,price,opened_at,0.0,0.0,setup_id))
        append_entry_record(symbol,"RESEARCH_P_V24_ENTRY","P_V24",float(details.get("p_v2_score") or 0),price,f"confirmed={note}; actual_order=0",
            extra={**details,"shadow_id":shadow_id,"research_variant":"P_V24","shadow_entry_time":_kst_stamp(opened_at),"p_v24_confirm_state":note,"p_setup_id":setup_id})
        return True

    def _handle_pv24_watch(self, symbol: str, details: dict[str, Any]) -> None:
        now = datetime.now(timezone.utc); price = float(details.get("live_price") or details.get("price") or 0)
        if price <= 0: return
        base_setup = str(details.get("p_setup_id") or f"P65SET-{symbol}-{int(now.timestamp())//900}")
        lock_cut = now - timedelta(minutes=max(1,int(self.cfg.research_pv24_same_setup_lock_minutes)))
        confirmed_setup = None
        with db() as conn:
            recent = conn.execute("SELECT * FROM research_pv24_setups WHERE symbol=? AND first_seen_at>=? ORDER BY first_seen_at DESC LIMIT 1", (symbol,lock_cut.isoformat())).fetchone()
            if recent:
                status=str(recent["status"] or ""); setup_id=str(recent["setup_id"] or base_setup)
                if status != "WATCH": return
                first=datetime.fromisoformat(str(recent["first_seen_at"]));
                if first.tzinfo is None: first=first.replace(tzinfo=timezone.utc)
                trigger=float(recent["trigger_price"] or price); low=min(float(recent["lowest_price"] or price),price); age=(now-first).total_seconds()/60.0
                adverse=(low/trigger-1)*100 if trigger>0 else 0.0; recovery=(price/low-1)*100 if low>0 else 0.0
                if age > float(self.cfg.research_pv24_confirm_max_minutes) or adverse <= -abs(float(self.cfg.research_pv24_max_adverse_before_confirm_pct)):
                    conn.execute("UPDATE research_pv24_setups SET status='DROPPED',last_seen_at=?,last_price=?,lowest_price=?,note=? WHERE id=?", (now.isoformat(),price,low,"expired_or_adverse",int(recent["id"])))
                    append_entry_record(symbol,"RESEARCH_P_V24_DROP","P_V24",float(details.get("p_v2_score") or 0),price,"expired_or_adverse",extra={**details,"p_setup_id":setup_id,"p_v24_confirm_state":"DROPPED"})
                    return
                normalized=float(details.get("rsi_delta") or 0) < float(self.cfg.research_pv23_rsi_delta_spike_min)
                live_ok=float(details.get("live_candle_gain_pct") or 0) >= float(self.cfg.research_pv24_live_gain_min_pct)
                ok=bool(age >= float(self.cfg.research_pv24_confirm_min_minutes) and price >= trigger and recovery >= float(self.cfg.research_pv24_recovery_from_low_min_pct) and normalized and live_ok)
                conn.execute("UPDATE research_pv24_setups SET last_seen_at=?,last_price=?,lowest_price=?,snapshot_json=? WHERE id=?", (now.isoformat(),price,low,json.dumps(details,ensure_ascii=False,default=str),int(recent["id"])))
                if not ok: return
                conn.execute("UPDATE research_pv24_setups SET status='CONFIRMED',confirmed_at=?,confirmed_price=?,note=? WHERE id=?", (now.isoformat(),price,"price_recovery",int(recent["id"])))
                confirmed_setup=setup_id
            else:
                if not bool(details.get("p_v24_watch_candidate")):
                    return
                expires=now+timedelta(minutes=max(1,int(self.cfg.research_pv24_confirm_max_minutes)))
                conn.execute("""INSERT OR IGNORE INTO research_pv24_setups(setup_id,symbol,first_seen_at,last_seen_at,trigger_price,lowest_price,last_price,block_reason,snapshot_json,status,expires_at) VALUES(?,?,?,?,?,?,?,?,?,'WATCH',?)""",
                    (base_setup,symbol,now.isoformat(),now.isoformat(),price,price,price,str(details.get("p_v23_block_reason") or "all_confirm"),json.dumps(details,ensure_ascii=False,default=str),expires.isoformat()))
                append_entry_record(symbol,"RESEARCH_P_V24_WATCH","P_V24",float(details.get("p_v2_score") or 0),price,str(details.get("p_v23_block_reason") or "all_confirm"),extra={**details,"p_setup_id":base_setup,"p_v24_confirm_state":"WATCH"})
                return
        if confirmed_setup:
            self._open_pv24_confirmed_shadow(symbol, details, confirmed_setup, price, "CONFIRMED_RECOVERY")

    def _pv25_can_open_now(self, now: datetime) -> tuple[bool, str]:
        """P_V25 portfolio guard. LIVE 예정 4슬롯을 반영하되 15분 몰림은 2건으로 제한한다."""
        rolling_start = now - timedelta(minutes=15)
        stop_cut = now - timedelta(minutes=max(1, int(self.cfg.research_pv24_stop_pause_window_minutes)))
        with db() as conn:
            cnt = conn.execute(
                "SELECT COUNT(*) AS n FROM research_shadow_reviews WHERE variant='P_V25' AND opened_at>=?",
                (rolling_start.isoformat(),),
            ).fetchone()
            if int(cnt["n"] or 0) >= max(1, int(self.cfg.research_pv25_max_entries_per_15m)):
                return False, "rolling_15m_entry_cap"
            open_cnt = conn.execute(
                "SELECT COUNT(*) AS n FROM research_shadow_reviews WHERE variant='P_V25' AND completed=0"
            ).fetchone()
            if int(open_cnt["n"] or 0) >= max(1, int(self.cfg.research_pv25_max_open_positions)):
                return False, "max_open_positions"
            stops = conn.execute(
                "SELECT COUNT(*) AS n FROM research_shadow_reviews WHERE variant='P_V25' AND completed=1 AND result='STOP' AND result_ts>=?",
                (stop_cut.isoformat(),),
            ).fetchone()
            if int(stops["n"] or 0) >= max(1, int(self.cfg.research_pv24_stop_pause_count)):
                return False, "recent_stop_pause"
        return True, ""

    def _pv26_a_filter_match(self, details: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
        """V25 표본에서 발견된 A형(확정 5분 재가속 직후 실패) 사전차단 후보."""
        keys = (
            "live_candle_gain_pct",
            "prev2_candle_gain_pct",
            "prev3_candle_gain_pct",
            "ema9_ema20_gap_pct",
        )
        if any(details.get(k) in (None, "") for k in keys):
            return False, {"reason": "a_filter_missing_telemetry"}
        try:
            live_gain = float(details.get("live_candle_gain_pct"))
            prev2 = float(details.get("prev2_candle_gain_pct"))
            prev3 = float(details.get("prev3_candle_gain_pct"))
            ema_gap = float(details.get("ema9_ema20_gap_pct"))
        except (TypeError, ValueError):
            return False, {"reason": "a_filter_bad_telemetry"}

        matched = bool(
            live_gain <= float(self.cfg.research_pv26_a_live_gain_max_pct)
            and prev2 <= float(self.cfg.research_pv26_a_prev2_gain_max_pct)
            and prev3 >= float(self.cfg.research_pv26_a_prev3_gain_min_pct)
            and ema_gap >= float(self.cfg.research_pv26_a_ema_gap_min_pct)
        )
        return matched, {
            "reason": "PV26_A_FILTER" if matched else "a_filter_pass",
            "live_gain_pct": round(live_gain, 4),
            "prev2_gain_pct": round(prev2, 4),
            "prev3_gain_pct": round(prev3, 4),
            "ema9_ema20_gap_pct": round(ema_gap, 4),
            "thresholds": {
                "live_max": float(self.cfg.research_pv26_a_live_gain_max_pct),
                "prev2_max": float(self.cfg.research_pv26_a_prev2_gain_max_pct),
                "prev3_min": float(self.cfg.research_pv26_a_prev3_gain_min_pct),
                "ema_gap_min": float(self.cfg.research_pv26_a_ema_gap_min_pct),
            },
        }

    def _pv26_stop_cooldown_status(self, symbol: str, now: datetime) -> tuple[bool, float, str]:
        """P_V26에서 STOP/LATE_FAILURE_EXIT 난 종목만 3시간 재진입을 막는다."""
        minutes = max(0, int(self.cfg.research_pv26_stop_reentry_cooldown_minutes))
        if minutes <= 0:
            return False, 0.0, ""
        with db() as conn:
            row = conn.execute(
                """SELECT result,result_ts FROM research_shadow_reviews
                   WHERE variant='P_V26' AND symbol=? AND completed=1
                     AND result IN ('STOP','LATE_FAILURE_EXIT')
                   ORDER BY result_ts DESC LIMIT 1""",
                (symbol,),
            ).fetchone()
        if not row or not row["result_ts"]:
            return False, 0.0, ""
        try:
            last_dt = datetime.fromisoformat(str(row["result_ts"]))
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
        except Exception:
            return False, 0.0, ""
        elapsed = (now - last_dt).total_seconds() / 60.0
        remaining = max(0.0, float(minutes) - elapsed)
        return bool(remaining > 0), remaining, str(row["result"] or "")

    def _pv26_can_open_now(self, now: datetime) -> tuple[bool, str]:
        """P_V26 portfolio guard: V25와 같은 4슬롯 + rolling 15분 2건 제한."""
        rolling_start = now - timedelta(minutes=15)
        stop_cut = now - timedelta(minutes=max(1, int(self.cfg.research_pv24_stop_pause_window_minutes)))
        with db() as conn:
            cnt = conn.execute(
                "SELECT COUNT(*) AS n FROM research_shadow_reviews WHERE variant='P_V26' AND opened_at>=?",
                (rolling_start.isoformat(),),
            ).fetchone()
            if int(cnt["n"] or 0) >= max(1, int(self.cfg.research_pv26_max_entries_per_15m)):
                return False, "rolling_15m_entry_cap"

            open_cnt = conn.execute(
                "SELECT COUNT(*) AS n FROM research_shadow_reviews WHERE variant='P_V26' AND completed=0"
            ).fetchone()
            if int(open_cnt["n"] or 0) >= max(1, int(self.cfg.research_pv26_max_open_positions)):
                return False, "max_open_positions"

            # V25의 연구용 recent-stop pause는 그대로 유지한다.
            stops = conn.execute(
                """SELECT COUNT(*) AS n FROM research_shadow_reviews
                   WHERE variant='P_V26' AND completed=1
                     AND result IN ('STOP','LATE_FAILURE_EXIT') AND result_ts>=?""",
                (stop_cut.isoformat(),),
            ).fetchone()
            if int(stops["n"] or 0) >= max(1, int(self.cfg.research_pv24_stop_pause_count)):
                return False, "recent_stop_pause"
        return True, ""

    def _pv27_entry_filter_match(self, details: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
        """V27 통합 진입필터: 둔화형 + 고신뢰 A1만 차단한다."""
        keys = (
            "prev1_candle_gain_pct", "prev2_candle_gain_pct",
            "rsi_change_prev1", "p_v2_score",
        )
        if any(details.get(k) in (None, "") for k in keys):
            return False, {"reason": "v27_filter_missing_telemetry"}
        try:
            prev1 = float(details.get("prev1_candle_gain_pct"))
            prev2 = float(details.get("prev2_candle_gain_pct"))
            rsi_chg1 = float(details.get("rsi_change_prev1"))
            score = float(details.get("p_v2_score"))
        except (TypeError, ValueError):
            return False, {"reason": "v27_filter_bad_telemetry"}

        stall = bool(
            prev2 >= float(self.cfg.research_pv27_stall_prev2_gain_min_pct)
            and rsi_chg1 <= float(self.cfg.research_pv27_stall_rsi_change_prev1_max)
            and score >= float(self.cfg.research_pv27_stall_score_min)
            and prev1 <= float(self.cfg.research_pv27_stall_prev1_gain_max_pct)
        )
        old_a1, old_a1_meta = self._pv26_a_filter_match(details)
        a1_high_conf = bool(
            old_a1
            and score >= float(self.cfg.research_pv27_a1_score_min)
            and prev1 <= float(self.cfg.research_pv27_a1_prev1_gain_max_pct)
        )
        matched = bool(stall or a1_high_conf)
        reasons = []
        if stall:
            reasons.append("STALL")
        if a1_high_conf:
            reasons.append("A1_HIGH_CONF")
        return matched, {
            "reason": "PV27_" + "+".join(reasons) if matched else "v27_filter_pass",
            "p_v27_stall_block": stall,
            "p_v27_a1_high_conf_block": a1_high_conf,
            "prev1_gain_pct": round(prev1, 4),
            "prev2_gain_pct": round(prev2, 4),
            "rsi_change_prev1": round(rsi_chg1, 4),
            "p_v2_score": round(score, 2),
            "a1_meta": old_a1_meta,
        }

    def _pv272_entry_filter_match(self, details: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
        """v4.3.72 V27-2: 두 개의 좁은 반복 손실패턴만 차단한다.

        시장 telemetry는 여기서 사용하지 않는다.
        """
        keys = (
            "p_v2_score", "rsi", "prev2_candle_gain_pct",
            "ema9_ema20_gap_pct", "ema20_slope_pct",
        )
        if any(details.get(k) in (None, "") for k in keys):
            return False, {"reason": "v272_filter_missing_telemetry"}
        try:
            score = float(details.get("p_v2_score"))
            rsi = float(details.get("rsi"))
            prev2 = float(details.get("prev2_candle_gain_pct"))
            ema_gap = float(details.get("ema9_ema20_gap_pct"))
            ema20_slope = float(details.get("ema20_slope_pct"))
        except (TypeError, ValueError):
            return False, {"reason": "v272_filter_bad_telemetry"}

        high_score_rsi_lag = bool(
            score >= float(self.cfg.research_pv272_high_score_min)
            and rsi < float(self.cfg.research_pv272_rsi_max)
            and prev2 >= float(self.cfg.research_pv272_prev2_gain_min_pct)
        )
        weak_structure = bool(
            score < float(self.cfg.research_pv272_weak_score_max)
            and ema_gap < float(self.cfg.research_pv272_weak_ema_gap_max_pct)
            and ema20_slope < float(self.cfg.research_pv272_weak_ema20_slope_max_pct)
        )

        matched = bool(high_score_rsi_lag or weak_structure)
        reasons = []
        if high_score_rsi_lag:
            reasons.append("HIGH_SCORE_RSI_LAG")
        if weak_structure:
            reasons.append("WEAK_STRUCTURE")
        return matched, {
            "reason": "PV272_" + "+".join(reasons) if matched else "v272_filter_pass",
            "p_v272_high_score_rsi_lag_block": high_score_rsi_lag,
            "p_v272_weak_structure_block": weak_structure,
            "p_v2_score": round(score, 2),
            "rsi": round(rsi, 2),
            "prev2_gain_pct": round(prev2, 4),
            "ema9_ema20_gap_pct": round(ema_gap, 4),
            "ema20_slope_pct": round(ema20_slope, 4),
        }

    def _pv273_entry_filter_match(self, details: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
        """v4.3.73 V27-3: 강한 직전 impulse 대비 EMA 분리가 따라오지 못한 진입만 차단한다.

        Forward-test 전용이다. BTC/ETH market telemetry와 STOP/TP/BE 로직은 사용/변경하지 않는다.
        """
        keys = (
            "p_v2_score", "prev2_candle_gain_pct",
            "rsi_change_prev1", "ema9_ema20_gap_pct",
        )
        if any(details.get(k) in (None, "") for k in keys):
            return False, {"reason": "v273_filter_missing_telemetry"}
        try:
            score = float(details.get("p_v2_score"))
            prev2 = float(details.get("prev2_candle_gain_pct"))
            rsi_chg1 = float(details.get("rsi_change_prev1"))
            ema_gap = float(details.get("ema9_ema20_gap_pct"))
        except (TypeError, ValueError):
            return False, {"reason": "v273_filter_bad_telemetry"}

        impulse_separation_lag = bool(
            score < float(self.cfg.research_pv273_score_max)
            and prev2 >= float(self.cfg.research_pv273_prev2_gain_min_pct)
            and rsi_chg1 >= float(self.cfg.research_pv273_rsi_change_prev1_min)
            and ema_gap < float(self.cfg.research_pv273_ema_gap_max_pct)
        )

        return impulse_separation_lag, {
            "reason": "PV273_IMPULSE_SEPARATION_LAG" if impulse_separation_lag else "v273_filter_pass",
            "p_v273_impulse_separation_lag_block": impulse_separation_lag,
            "p_v2_score": round(score, 2),
            "prev2_gain_pct": round(prev2, 4),
            "rsi_change_prev1": round(rsi_chg1, 4),
            "ema9_ema20_gap_pct": round(ema_gap, 4),
        }

    def _pv274_entry_filter_match(self, details: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
        """v4.3.74 V27-4: 네 구조지표가 모두 약할 때만 차단한다.

        V27-3 impulse 조건은 사용하지 않는다. RSI/live volume/BTC/ETH도 hard block에 사용하지 않는다.
        목적은 STOP 자체를 맞히는 것이 아니라, 180분 안에 TP1도 못 가는 약한 구조형을 좁게 줄이는 것이다.
        """
        keys = (
            "ema20_slope_pct", "ema9_ema20_gap_pct",
            "p_v21_persistence_score", "rebound_from_low_pct",
        )
        if any(details.get(k) in (None, "") for k in keys):
            return False, {"reason": "v274_filter_missing_telemetry", "p_v274_weak_count": 0, "p_v274_weak_flags": ""}
        try:
            ema20_slope = float(details.get("ema20_slope_pct"))
            ema_gap = float(details.get("ema9_ema20_gap_pct"))
            persistence = float(details.get("p_v21_persistence_score"))
            rebound = float(details.get("rebound_from_low_pct"))
        except (TypeError, ValueError):
            return False, {"reason": "v274_filter_bad_telemetry", "p_v274_weak_count": 0, "p_v274_weak_flags": ""}

        weak_flags: list[str] = []
        if ema20_slope < float(self.cfg.research_pv274_ema20_slope_max_pct):
            weak_flags.append("EMA20_SLOPE")
        if ema_gap < float(self.cfg.research_pv274_ema_gap_max_pct):
            weak_flags.append("EMA_GAP")
        if persistence < float(self.cfg.research_pv274_persistence_max):
            weak_flags.append("PERSISTENCE")
        if rebound < float(self.cfg.research_pv274_rebound_max_pct):
            weak_flags.append("REBOUND")

        weak_count = len(weak_flags)
        required = max(1, int(self.cfg.research_pv274_required_weak_count))
        weak_structure = bool(weak_count >= required)
        return weak_structure, {
            "reason": "PV274_WEAK_STRUCTURE_4OF4" if weak_structure else "v274_filter_pass",
            "p_v274_weak_structure_block": weak_structure,
            "p_v274_weak_count": weak_count,
            "p_v274_weak_flags": ",".join(weak_flags),
            "ema20_slope_pct": round(ema20_slope, 4),
            "ema9_ema20_gap_pct": round(ema_gap, 4),
            "p_v21_persistence_score": round(persistence, 2),
            "rebound_from_low_pct": round(rebound, 2),
            "v274_thresholds": {
                "ema20_slope_max": float(self.cfg.research_pv274_ema20_slope_max_pct),
                "ema_gap_max": float(self.cfg.research_pv274_ema_gap_max_pct),
                "persistence_max": float(self.cfg.research_pv274_persistence_max),
                "rebound_max": float(self.cfg.research_pv274_rebound_max_pct),
                "required_weak_count": required,
            },
        }

    def _research_live_cooldown_status(
        self, variant: str, symbol: str, now: datetime
    ) -> tuple[bool, float, str]:
        """LIVE와 같은 최소 동일종목 cooldown: 모든 종료 후 90분.

        STOP/LATE의 180분 연구 cooldown은 별도 함수가 추가로 검사한다.
        """
        minutes = max(0, int(self.cfg.research_live_same_symbol_cooldown_minutes))
        if minutes <= 0:
            return False, 0.0, ""
        with db() as conn:
            row = conn.execute(
                """SELECT result,result_ts FROM research_shadow_reviews
                   WHERE variant=? AND symbol=? AND completed=1 AND result_ts IS NOT NULL
                   ORDER BY result_ts DESC LIMIT 1""",
                (variant, symbol),
            ).fetchone()
        if not row or not row["result_ts"]:
            return False, 0.0, ""
        try:
            last_dt = datetime.fromisoformat(str(row["result_ts"]))
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
        except Exception:
            return False, 0.0, ""
        elapsed = (now - last_dt).total_seconds() / 60.0
        remaining = max(0.0, float(minutes) - elapsed)
        return bool(remaining > 0), remaining, str(row["result"] or "")

    def _variant_stop_cooldown_status(self, variant: str, symbol: str, now: datetime) -> tuple[bool, float, str]:
        """STOP/LATE 이후 동일 variant의 같은 종목만 180분 재진입 금지."""
        minutes = max(0, int(self.cfg.research_pv26_stop_reentry_cooldown_minutes))
        if minutes <= 0:
            return False, 0.0, ""
        with db() as conn:
            row = conn.execute(
                """SELECT result,result_ts FROM research_shadow_reviews
                   WHERE variant=? AND symbol=? AND completed=1
                     AND result IN ('STOP','LATE_FAILURE_EXIT')
                   ORDER BY result_ts DESC LIMIT 1""",
                (variant, symbol),
            ).fetchone()
        if not row or not row["result_ts"]:
            return False, 0.0, ""
        try:
            last_dt = datetime.fromisoformat(str(row["result_ts"]))
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
        except Exception:
            return False, 0.0, ""
        elapsed = (now - last_dt).total_seconds() / 60.0
        remaining = max(0.0, float(minutes) - elapsed)
        return bool(remaining > 0), remaining, str(row["result"] or "")

    def _variant_can_open_now(self, variant: str, now: datetime, max_entries_15m: int, max_open: int) -> tuple[bool, str]:
        """연구 variant 공통 4슬롯 + rolling 15분 cap + recent-stop pause."""
        rolling_start = now - timedelta(minutes=15)
        stop_cut = now - timedelta(minutes=max(1, int(self.cfg.research_pv24_stop_pause_window_minutes)))
        with db() as conn:
            cnt = conn.execute(
                "SELECT COUNT(*) AS n FROM research_shadow_reviews WHERE variant=? AND opened_at>=?",
                (variant, rolling_start.isoformat()),
            ).fetchone()
            if int(cnt["n"] or 0) >= max(1, int(max_entries_15m)):
                return False, "rolling_15m_entry_cap"
            open_cnt = conn.execute(
                "SELECT COUNT(*) AS n FROM research_shadow_reviews WHERE variant=? AND completed=0",
                (variant,),
            ).fetchone()
            if int(open_cnt["n"] or 0) >= max(1, int(max_open)):
                return False, "max_open_positions"
            stops = conn.execute(
                """SELECT COUNT(*) AS n FROM research_shadow_reviews
                   WHERE variant=? AND completed=1
                     AND result IN ('STOP','LATE_FAILURE_EXIT') AND result_ts>=?""",
                (variant, stop_cut.isoformat()),
            ).fetchone()
            if int(stops["n"] or 0) >= max(1, int(self.cfg.research_pv24_stop_pause_count)):
                return False, "recent_stop_pause"
        return True, ""

    def _register_pv27_blocked_ghost(
        self, symbol: str, details: dict[str, Any], setup_id: str, entry_price: float,
        reason: str, meta: dict[str, Any], closed_5m_price: float,
    ) -> None:
        if not self.cfg.research_pv27_ghost_tracking_enabled or entry_price <= 0:
            return
        now = datetime.now(timezone.utc)
        with db() as conn:
            existing = conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_BLOCK_GHOST' AND setup_id=? LIMIT 1",
                (setup_id,),
            ).fetchone()
            if existing:
                return
            opened_at = now.isoformat()
            shadow_id = f"RSH-P_V27_BLOCK_GHOST-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = entry_price * (1 + float(self.cfg.tp2_pct) / 100)
            be_price = entry_price * (1 + float(self.cfg.breakeven_stop_pct) / 100)
            snap = dict(details)
            snap.update({
                "p_v27_confirm_state": "BLOCKED_GHOST",
                "p_v27_block_reason": reason,
                "p_v27_ghost_type": "P_V27_BLOCK_GHOST",
                "p_v27_live_entry_price": entry_price,
                "p_v27_closed_5m_price": closed_5m_price,
                **meta,
            })
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,
                    missing_condition,snapshot_json,tp2_price,be_price,highest_price,lowest_price,last_price,
                    last_checked_at,last_5m_bucket,last_15m_bucket,mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    shadow_id,"P_V27_BLOCK_GHOST",symbol,opened_at,int(now.timestamp()*1000),entry_price,
                    float(details.get("p_score") or 0),float(details.get("p_v2_score") or 0),reason,
                    json.dumps(snap,ensure_ascii=False,default=str),tp2_price,be_price,entry_price,entry_price,entry_price,
                    opened_at,str(int(now.timestamp())//300),str(int(now.timestamp())//900),0.0,0.0,setup_id,0,
                ),
            )
        append_entry_record(
            symbol,"RESEARCH_P_V27_BLOCK_GHOST_ENTRY","P_V27_BLOCK_GHOST",float(details.get("p_v2_score") or 0),
            entry_price,f"blocked_by={reason}; ghost_only=1; actual_order=0",
            extra={**details,"shadow_id":shadow_id,"research_variant":"P_V27_BLOCK_GHOST",
                   "p_v27_confirm_state":"BLOCKED_GHOST","p_v27_block_reason":reason,
                   "p_v27_ghost_type":"P_V27_BLOCK_GHOST","p_v27_live_entry_price":entry_price,
                   "p_v27_closed_5m_price":closed_5m_price,**meta},
        )

    def _open_pv27_confirmed_shadow(
        self, symbol: str, details: dict[str, Any], source_setup_id: str, entry_price: float,
        closed_5m_price: float, five_bullish: bool, high_break: bool,
    ) -> bool:
        # v4.3.72부터 기존 P_V27 신규진입은 강제 OFF. 기존 열린 Shadow 관리는 계속한다.
        return False
        now = datetime.now(timezone.utc)
        setup_id = str(source_setup_id).replace("P25SET-", "P27SET-", 1)
        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27' AND symbol=? AND completed=0 LIMIT 1",
                (symbol,),
            ).fetchone():
                return False

        blocked, meta = self._pv27_entry_filter_match(details)
        if blocked:
            reason = str(meta.get("reason") or "V27_ENTRY_FILTER")
            self._register_pv27_blocked_ghost(symbol,details,setup_id,entry_price,reason,meta,closed_5m_price)
            append_entry_record(
                symbol,"RESEARCH_P_V27_BLOCKED","P_V27",float(details.get("p_v2_score") or 0),entry_price,reason,
                extra={**details,"p_v27_confirm_state":"BLOCKED","p_v27_block_reason":reason,
                       "p_v27_stall_block":bool(meta.get("p_v27_stall_block")),
                       "p_v27_a1_high_conf_block":bool(meta.get("p_v27_a1_high_conf_block")),
                       "p_v27_live_entry_price":entry_price,"p_v27_closed_5m_price":closed_5m_price},
            )
            return False

        cooldown, remaining, prior = self._variant_stop_cooldown_status("P_V27",symbol,now)
        if cooldown:
            append_entry_record(
                symbol,"RESEARCH_P_V27_BLOCKED_COOLDOWN","P_V27",float(details.get("p_v2_score") or 0),entry_price,
                f"STOP_COOLDOWN remaining={remaining:.1f}m",
                extra={**details,"p_v27_confirm_state":"BLOCKED_COOLDOWN","p_v27_block_reason":"STOP_COOLDOWN",
                       "p_v27_live_entry_price":entry_price,"p_v27_closed_5m_price":closed_5m_price},
            )
            return False

        allowed, why = self._variant_can_open_now(
            "P_V27",now,self.cfg.research_pv27_max_entries_per_15m,self.cfg.research_pv27_max_open_positions
        )
        if not allowed:
            append_entry_record(symbol,"RESEARCH_P_V27_SKIPPED","P_V27",float(details.get("p_v2_score") or 0),entry_price,why,
                extra={**details,"p_v27_confirm_state":why,"p_v27_block_reason":why,
                       "p_v27_live_entry_price":entry_price,"p_v27_closed_5m_price":closed_5m_price})
            return False

        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27' AND setup_id=? LIMIT 1",(setup_id,)
            ).fetchone():
                return False
            opened_at=now.isoformat()
            shadow_id=f"RSH-P_V27-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price=entry_price*(1+float(self.cfg.tp2_pct)/100)
            be_price=entry_price*(1+float(self.cfg.breakeven_stop_pct)/100)
            snap=dict(details)
            snap.update({
                "p_v27_confirm_state":"CONFIRMED_5M_BREAK","p_v27_block_reason":"",
                "p_v27_live_entry_price":entry_price,"p_v27_closed_5m_price":closed_5m_price,
                "p_v27_stall_block":False,"p_v27_a1_high_conf_block":False,
                "p_v25_setup_id":source_setup_id,"p_v25_5m_bullish":five_bullish,"p_v25_prev_high_break":high_break,
            })
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,missing_condition,snapshot_json,
                    tp2_price,be_price,highest_price,lowest_price,last_price,last_checked_at,last_5m_bucket,last_15m_bucket,
                    mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (shadow_id,"P_V27",symbol,opened_at,int(now.timestamp()*1000),entry_price,float(details.get("p_score") or 0),
                 float(details.get("p_v2_score") or 0),"confirmed_5m_break_v27",json.dumps(snap,ensure_ascii=False,default=str),
                 tp2_price,be_price,entry_price,entry_price,entry_price,opened_at,str(int(now.timestamp())//300),
                 str(int(now.timestamp())//900),0.0,0.0,setup_id,0),
            )
        append_entry_record(
            symbol,"RESEARCH_P_V27_ENTRY","P_V27",float(details.get("p_v2_score") or 0),entry_price,
            "live-price entry + V27 entry filter passed; actual_order=0",
            extra={**details,"shadow_id":shadow_id,"research_variant":"P_V27","shadow_entry_time":_kst_stamp(opened_at),
                   "p_v27_confirm_state":"CONFIRMED_5M_BREAK","p_v27_block_reason":"",
                   "p_v27_live_entry_price":entry_price,"p_v27_closed_5m_price":closed_5m_price},
        )
        return True

    def _clone_pv27_filter_only_from_v26(
        self, symbol: str, details: dict[str, Any], source_shadow_id: str, source_setup_id: str,
        source_opened_at: str, entry_price: float, closed_5m_price: float, five_bullish: bool, high_break: bool,
    ) -> None:
        """v4.3.72부터 기존 V27 FilterOnly 신규복제는 강제 OFF."""
        return
        now = datetime.now(timezone.utc)
        setup_id = str(source_setup_id).replace("P25SET-", "P27FOSET-", 1)
        if setup_id == str(source_setup_id):
            setup_id = f"P27FO-{source_setup_id}"

        blocked, meta = self._pv27_entry_filter_match(details)
        reason = str(meta.get("reason") or ("V27_ENTRY_FILTER" if blocked else "v27_filter_pass"))
        common_extra = {
            **details,
            "research_variant": "P_V27_FILTER_ONLY",
            "p_v27fo_source_shadow_id": source_shadow_id,
            "p_v27fo_filter_pass": not blocked,
            "p_v27fo_stall_block": bool(meta.get("p_v27_stall_block")),
            "p_v27fo_a1_high_conf_block": bool(meta.get("p_v27_a1_high_conf_block")),
            "p_v27_live_entry_price": entry_price,
            "p_v27_closed_5m_price": closed_5m_price,
        }

        if blocked:
            append_entry_record(
                symbol, "RESEARCH_P_V27_FILTER_ONLY_BLOCKED", "P_V27_FILTER_ONLY",
                float(details.get("p_v2_score") or 0), entry_price, reason,
                extra={
                    **common_extra,
                    "p_v27fo_confirm_state": "BLOCKED_BY_V27_FILTER",
                    "p_v27fo_block_reason": reason,
                },
            )
            return

        with db() as conn:
            existing = conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_FILTER_ONLY' AND setup_id=? LIMIT 1",
                (setup_id,),
            ).fetchone()
            if existing:
                return
            # 원본 V26과 같은 종목이 이미 열려 있다면 1:1 불변식 위반이므로 새 거래를 만들지 않고 진단만 남긴다.
            open_same = conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_FILTER_ONLY' AND symbol=? AND completed=0 LIMIT 1",
                (symbol,),
            ).fetchone()
            if open_same:
                append_entry_record(
                    symbol, "RESEARCH_P_V27_FILTER_ONLY_SYNC_ERROR", "P_V27_FILTER_ONLY",
                    float(details.get("p_v2_score") or 0), entry_price, "duplicate_open_symbol",
                    extra={
                        **common_extra,
                        "p_v27fo_confirm_state": "SYNC_ERROR",
                        "p_v27fo_block_reason": "duplicate_open_symbol",
                    },
                )
                return

            opened_at = str(source_opened_at or now.isoformat())
            try:
                opened_dt = datetime.fromisoformat(opened_at)
                if opened_dt.tzinfo is None:
                    opened_dt = opened_dt.replace(tzinfo=timezone.utc)
            except Exception:
                opened_dt = now
                opened_at = now.isoformat()
            shadow_id = f"RSH-P_V27_FILTER_ONLY-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = entry_price * (1 + float(self.cfg.tp2_pct) / 100)
            be_price = entry_price * (1 + float(self.cfg.breakeven_stop_pct) / 100)
            snap = dict(details)
            snap.update({
                "p_v27fo_confirm_state": "CLONED_FROM_V26_FILTER_PASS",
                "p_v27fo_block_reason": "",
                "p_v27fo_source_shadow_id": source_shadow_id,
                "p_v27fo_filter_pass": True,
                "p_v27fo_stall_block": False,
                "p_v27fo_a1_high_conf_block": False,
                "p_v27_live_entry_price": entry_price,
                "p_v27_closed_5m_price": closed_5m_price,
                "p_v25_setup_id": source_setup_id,
                "p_v25_5m_bullish": five_bullish,
                "p_v25_prev_high_break": high_break,
            })
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,missing_condition,snapshot_json,
                    tp2_price,be_price,highest_price,lowest_price,last_price,last_checked_at,last_5m_bucket,last_15m_bucket,
                    mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    shadow_id, "P_V27_FILTER_ONLY", symbol, opened_at, int(opened_dt.timestamp() * 1000), entry_price,
                    float(details.get("p_score") or 0), float(details.get("p_v2_score") or 0),
                    "v26_entry_v27_filter_pass", json.dumps(snap, ensure_ascii=False, default=str),
                    tp2_price, be_price, entry_price, entry_price, entry_price, opened_at,
                    str(int(opened_dt.timestamp()) // 300), str(int(opened_dt.timestamp()) // 900),
                    0.0, 0.0, setup_id, 0,
                ),
            )
        append_entry_record(
            symbol, "RESEARCH_P_V27_FILTER_ONLY_ENTRY", "P_V27_FILTER_ONLY",
            float(details.get("p_v2_score") or 0), entry_price,
            "same V26 entry/time/price; V27 filter passed; no replacement entries; actual_order=0",
            extra={
                **common_extra,
                "shadow_id": shadow_id,
                "shadow_entry_time": _kst_stamp(opened_at),
                "p_v27fo_confirm_state": "CLONED_FROM_V26_FILTER_PASS",
                "p_v27fo_block_reason": "",
            },
        )

    def _register_pv272_blocked_ghost(
        self, symbol: str, details: dict[str, Any], setup_id: str, entry_price: float,
        reason: str, meta: dict[str, Any], closed_5m_price: float,
    ) -> None:
        if not self.cfg.research_pv272_ghost_tracking_enabled or entry_price <= 0:
            return
        now = datetime.now(timezone.utc)
        with db() as conn:
            existing = conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_2_BLOCK_GHOST' AND setup_id=? LIMIT 1",
                (setup_id,),
            ).fetchone()
            if existing:
                return
            opened_at = now.isoformat()
            shadow_id = f"RSH-P_V27_2_BLOCK_GHOST-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = entry_price * (1 + float(self.cfg.tp2_pct) / 100)
            be_price = entry_price * (1 + float(self.cfg.breakeven_stop_pct) / 100)
            snap = dict(details)
            snap.update({
                "p_v272_confirm_state": "BLOCKED_GHOST",
                "p_v272_block_reason": reason,
                "p_v272_ghost_type": "P_V27_2_BLOCK_GHOST",
                "p_v272_live_entry_price": entry_price,
                "p_v272_closed_5m_price": closed_5m_price,
                **meta,
            })
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,
                    missing_condition,snapshot_json,tp2_price,be_price,highest_price,lowest_price,last_price,
                    last_checked_at,last_5m_bucket,last_15m_bucket,mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    shadow_id,"P_V27_2_BLOCK_GHOST",symbol,opened_at,int(now.timestamp()*1000),entry_price,
                    float(details.get("p_score") or 0),float(details.get("p_v2_score") or 0),reason,
                    json.dumps(snap,ensure_ascii=False,default=str),tp2_price,be_price,entry_price,entry_price,entry_price,
                    opened_at,str(int(now.timestamp())//300),str(int(now.timestamp())//900),0.0,0.0,setup_id,0,
                ),
            )
        append_entry_record(
            symbol,"RESEARCH_P_V27_2_BLOCK_GHOST_ENTRY","P_V27_2_BLOCK_GHOST",
            float(details.get("p_v2_score") or 0),entry_price,
            f"blocked_by={reason}; ghost_only=1; actual_order=0",
            extra={**details,"shadow_id":shadow_id,"research_variant":"P_V27_2_BLOCK_GHOST",
                   "p_v272_confirm_state":"BLOCKED_GHOST","p_v272_block_reason":reason,
                   "p_v272_ghost_type":"P_V27_2_BLOCK_GHOST","p_v272_live_entry_price":entry_price,
                   "p_v272_closed_5m_price":closed_5m_price,**meta},
        )

    def _open_pv272_confirmed_shadow(
        self, symbol: str, details: dict[str, Any], source_setup_id: str, entry_price: float,
        closed_5m_price: float, five_bullish: bool, high_break: bool,
    ) -> bool:
        """P_V27_2: V27의 넓은 STALL/A1을 제거하고 두 좁은 반복손실 패턴만 차단."""
        if not self.cfg.research_pv272_enabled or entry_price <= 0:
            return False
        now = datetime.now(timezone.utc)
        setup_id = str(source_setup_id).replace("P25SET-", "P272SET-", 1)
        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_2' AND symbol=? AND completed=0 LIMIT 1",
                (symbol,),
            ).fetchone():
                return False

        blocked, meta = self._pv272_entry_filter_match(details)
        if blocked:
            reason = str(meta.get("reason") or "V272_ENTRY_FILTER")
            self._register_pv272_blocked_ghost(
                symbol, details, setup_id, entry_price, reason, meta, closed_5m_price
            )
            append_entry_record(
                symbol,"RESEARCH_P_V27_2_BLOCKED","P_V27_2",float(details.get("p_v2_score") or 0),entry_price,reason,
                extra={**details,"p_v272_confirm_state":"BLOCKED","p_v272_block_reason":reason,
                       "p_v272_high_score_rsi_lag_block":bool(meta.get("p_v272_high_score_rsi_lag_block")),
                       "p_v272_weak_structure_block":bool(meta.get("p_v272_weak_structure_block")),
                       "p_v272_live_entry_price":entry_price,"p_v272_closed_5m_price":closed_5m_price},
            )
            return False

        # LIVE 기본: 모든 종료 후 90분 동일종목 재진입 금지.
        live_cd, live_remaining, live_prior = self._research_live_cooldown_status("P_V27_2", symbol, now)
        if live_cd:
            reason = "LIVE90_COOLDOWN"
            meta_cd = {"cooldown_remaining_min": round(live_remaining, 1), "prior_result": live_prior}
            self._register_pv272_blocked_ghost(
                symbol, details, setup_id, entry_price, reason, meta_cd, closed_5m_price
            )
            append_entry_record(
                symbol,"RESEARCH_P_V27_2_BLOCKED_LIVE90","P_V27_2",
                float(details.get("p_v2_score") or 0),entry_price,
                f"LIVE90_COOLDOWN remaining={live_remaining:.1f}m",
                extra={**details,"p_v272_confirm_state":"BLOCKED_LIVE90","p_v272_block_reason":reason,
                       "p_v272_live_entry_price":entry_price,"p_v272_closed_5m_price":closed_5m_price,**meta_cd},
            )
            return False

        # STOP/LATE 후에는 V26 연구와 같은 180분 강화 cooldown 유지.
        cooldown, remaining, prior = self._variant_stop_cooldown_status("P_V27_2",symbol,now)
        if cooldown:
            reason = "STOP_COOLDOWN"
            meta_cd = {"cooldown_remaining_min": round(remaining, 1), "prior_failure_result": prior}
            self._register_pv272_blocked_ghost(
                symbol, details, setup_id, entry_price, reason, meta_cd, closed_5m_price
            )
            append_entry_record(
                symbol,"RESEARCH_P_V27_2_BLOCKED_COOLDOWN","P_V27_2",
                float(details.get("p_v2_score") or 0),entry_price,
                f"STOP_COOLDOWN remaining={remaining:.1f}m",
                extra={**details,"p_v272_confirm_state":"BLOCKED_COOLDOWN","p_v272_block_reason":reason,
                       "p_v272_live_entry_price":entry_price,"p_v272_closed_5m_price":closed_5m_price,**meta_cd},
            )
            return False

        allowed, why = self._variant_can_open_now(
            "P_V27_2",now,self.cfg.research_pv272_max_entries_per_15m,self.cfg.research_pv272_max_open_positions
        )
        if not allowed:
            append_entry_record(
                symbol,"RESEARCH_P_V27_2_SKIPPED","P_V27_2",
                float(details.get("p_v2_score") or 0),entry_price,why,
                extra={**details,"p_v272_confirm_state":why,"p_v272_block_reason":why,
                       "p_v272_live_entry_price":entry_price,"p_v272_closed_5m_price":closed_5m_price},
            )
            return False

        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_2' AND setup_id=? LIMIT 1",(setup_id,)
            ).fetchone():
                return False
            opened_at=now.isoformat()
            shadow_id=f"RSH-P_V27_2-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price=entry_price*(1+float(self.cfg.tp2_pct)/100)
            be_price=entry_price*(1+float(self.cfg.breakeven_stop_pct)/100)
            snap=dict(details)
            snap.update({
                "p_v272_confirm_state":"CONFIRMED_5M_BREAK","p_v272_block_reason":"",
                "p_v272_live_entry_price":entry_price,"p_v272_closed_5m_price":closed_5m_price,
                "p_v272_high_score_rsi_lag_block":False,"p_v272_weak_structure_block":False,
                "p_v25_setup_id":source_setup_id,"p_v25_5m_bullish":five_bullish,
                "p_v25_prev_high_break":high_break,
            })
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,missing_condition,snapshot_json,
                    tp2_price,be_price,highest_price,lowest_price,last_price,last_checked_at,last_5m_bucket,last_15m_bucket,
                    mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    shadow_id,"P_V27_2",symbol,opened_at,int(now.timestamp()*1000),entry_price,
                    float(details.get("p_score") or 0),float(details.get("p_v2_score") or 0),
                    "confirmed_5m_break_v272",json.dumps(snap,ensure_ascii=False,default=str),
                    tp2_price,be_price,entry_price,entry_price,entry_price,opened_at,
                    str(int(now.timestamp())//300),str(int(now.timestamp())//900),0.0,0.0,setup_id,0,
                ),
            )
        append_entry_record(
            symbol,"RESEARCH_P_V27_2_ENTRY","P_V27_2",float(details.get("p_v2_score") or 0),entry_price,
            "live-price entry + V27-2 narrow entry filter passed; actual_order=0",
            extra={**details,"shadow_id":shadow_id,"research_variant":"P_V27_2",
                   "shadow_entry_time":_kst_stamp(opened_at),
                   "p_v272_confirm_state":"CONFIRMED_5M_BREAK","p_v272_block_reason":"",
                   "p_v272_live_entry_price":entry_price,"p_v272_closed_5m_price":closed_5m_price},
        )
        return True

    def _clone_pv272_filter_only_from_v26(
        self, symbol: str, details: dict[str, Any], source_shadow_id: str, source_setup_id: str,
        source_opened_at: str, entry_price: float, closed_5m_price: float, five_bullish: bool, high_break: bool,
    ) -> None:
        """V26 실제진입을 기준으로 V27-2 필터 자체만 1:1 비교한다. 대체진입 없음."""
        if not self.cfg.research_pv272_filter_only_enabled or entry_price <= 0:
            return
        now = datetime.now(timezone.utc)
        setup_id = str(source_setup_id).replace("P25SET-", "P272FOSET-", 1)
        if setup_id == str(source_setup_id):
            setup_id = f"P272FO-{source_setup_id}"

        blocked, meta = self._pv272_entry_filter_match(details)
        reason = str(meta.get("reason") or ("V272_ENTRY_FILTER" if blocked else "v272_filter_pass"))
        common_extra = {
            **details,
            "research_variant": "P_V27_2_FILTER_ONLY",
            "p_v272fo_source_shadow_id": source_shadow_id,
            "p_v272fo_filter_pass": not blocked,
            "p_v272fo_high_score_rsi_lag_block": bool(meta.get("p_v272_high_score_rsi_lag_block")),
            "p_v272fo_weak_structure_block": bool(meta.get("p_v272_weak_structure_block")),
            "p_v272_live_entry_price": entry_price,
            "p_v272_closed_5m_price": closed_5m_price,
        }

        if blocked:
            append_entry_record(
                symbol, "RESEARCH_P_V27_2_FILTER_ONLY_BLOCKED", "P_V27_2_FILTER_ONLY",
                float(details.get("p_v2_score") or 0), entry_price, reason,
                extra={
                    **common_extra,
                    "p_v272fo_confirm_state": "BLOCKED_BY_V272_FILTER",
                    "p_v272fo_block_reason": reason,
                },
            )
            return

        with db() as conn:
            existing = conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_2_FILTER_ONLY' AND setup_id=? LIMIT 1",
                (setup_id,),
            ).fetchone()
            if existing:
                return
            open_same = conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_2_FILTER_ONLY' AND symbol=? AND completed=0 LIMIT 1",
                (symbol,),
            ).fetchone()
            if open_same:
                append_entry_record(
                    symbol, "RESEARCH_P_V27_2_FILTER_ONLY_SYNC_ERROR", "P_V27_2_FILTER_ONLY",
                    float(details.get("p_v2_score") or 0), entry_price, "duplicate_open_symbol",
                    extra={
                        **common_extra,
                        "p_v272fo_confirm_state": "SYNC_ERROR",
                        "p_v272fo_block_reason": "duplicate_open_symbol",
                    },
                )
                return

            opened_at = str(source_opened_at or now.isoformat())
            try:
                opened_dt = datetime.fromisoformat(opened_at)
                if opened_dt.tzinfo is None:
                    opened_dt = opened_dt.replace(tzinfo=timezone.utc)
            except Exception:
                opened_dt = now
                opened_at = now.isoformat()
            shadow_id = f"RSH-P_V27_2_FILTER_ONLY-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = entry_price * (1 + float(self.cfg.tp2_pct) / 100)
            be_price = entry_price * (1 + float(self.cfg.breakeven_stop_pct) / 100)
            snap = dict(details)
            snap.update({
                "p_v272fo_confirm_state": "CLONED_FROM_V26_FILTER_PASS",
                "p_v272fo_block_reason": "",
                "p_v272fo_source_shadow_id": source_shadow_id,
                "p_v272fo_filter_pass": True,
                "p_v272fo_high_score_rsi_lag_block": False,
                "p_v272fo_weak_structure_block": False,
                "p_v272_live_entry_price": entry_price,
                "p_v272_closed_5m_price": closed_5m_price,
                "p_v25_setup_id": source_setup_id,
                "p_v25_5m_bullish": five_bullish,
                "p_v25_prev_high_break": high_break,
            })
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,missing_condition,snapshot_json,
                    tp2_price,be_price,highest_price,lowest_price,last_price,last_checked_at,last_5m_bucket,last_15m_bucket,
                    mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    shadow_id, "P_V27_2_FILTER_ONLY", symbol, opened_at, int(opened_dt.timestamp() * 1000), entry_price,
                    float(details.get("p_score") or 0), float(details.get("p_v2_score") or 0),
                    "v26_entry_v272_filter_pass", json.dumps(snap, ensure_ascii=False, default=str),
                    tp2_price, be_price, entry_price, entry_price, entry_price, opened_at,
                    str(int(opened_dt.timestamp()) // 300), str(int(opened_dt.timestamp()) // 900),
                    0.0, 0.0, setup_id, 0,
                ),
            )
        append_entry_record(
            symbol, "RESEARCH_P_V27_2_FILTER_ONLY_ENTRY", "P_V27_2_FILTER_ONLY",
            float(details.get("p_v2_score") or 0), entry_price,
            "same V26 entry/time/price; V27-2 filter passed; no replacement entries; actual_order=0",
            extra={
                **common_extra,
                "shadow_id": shadow_id,
                "shadow_entry_time": _kst_stamp(opened_at),
                "p_v272fo_confirm_state": "CLONED_FROM_V26_FILTER_PASS",
                "p_v272fo_block_reason": "",
            },
        )

    def _register_pv273_blocked_ghost(
        self, symbol: str, details: dict[str, Any], setup_id: str, entry_price: float,
        reason: str, meta: dict[str, Any], closed_5m_price: float,
    ) -> None:
        if not self.cfg.research_pv273_ghost_tracking_enabled or entry_price <= 0:
            return
        now = datetime.now(timezone.utc)
        with db() as conn:
            existing = conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_3_BLOCK_GHOST' AND setup_id=? LIMIT 1",
                (setup_id,),
            ).fetchone()
            if existing:
                return
            opened_at = now.isoformat()
            shadow_id = f"RSH-P_V27_3_BLOCK_GHOST-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = entry_price * (1 + float(self.cfg.tp2_pct) / 100)
            be_price = entry_price * (1 + float(self.cfg.breakeven_stop_pct) / 100)
            snap = dict(details)
            snap.update({
                "p_v273_confirm_state": "BLOCKED_GHOST",
                "p_v273_block_reason": reason,
                "p_v273_ghost_type": "P_V27_3_BLOCK_GHOST",
                "p_v273_live_entry_price": entry_price,
                "p_v273_closed_5m_price": closed_5m_price,
                **meta,
            })
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,
                    missing_condition,snapshot_json,tp2_price,be_price,highest_price,lowest_price,last_price,
                    last_checked_at,last_5m_bucket,last_15m_bucket,mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    shadow_id,"P_V27_3_BLOCK_GHOST",symbol,opened_at,int(now.timestamp()*1000),entry_price,
                    float(details.get("p_score") or 0),float(details.get("p_v2_score") or 0),reason,
                    json.dumps(snap,ensure_ascii=False,default=str),tp2_price,be_price,entry_price,entry_price,entry_price,
                    opened_at,str(int(now.timestamp())//300),str(int(now.timestamp())//900),0.0,0.0,setup_id,0,
                ),
            )
        append_entry_record(
            symbol,"RESEARCH_P_V27_3_BLOCK_GHOST_ENTRY","P_V27_3_BLOCK_GHOST",
            float(details.get("p_v2_score") or 0),entry_price,
            f"blocked_by={reason}; ghost_only=1; actual_order=0",
            extra={**details,"shadow_id":shadow_id,"research_variant":"P_V27_3_BLOCK_GHOST",
                   "p_v273_confirm_state":"BLOCKED_GHOST","p_v273_block_reason":reason,
                   "p_v273_ghost_type":"P_V27_3_BLOCK_GHOST","p_v273_live_entry_price":entry_price,
                   "p_v273_closed_5m_price":closed_5m_price,**meta},
        )

    def _open_pv273_confirmed_shadow(
        self, symbol: str, details: dict[str, Any], source_setup_id: str, entry_price: float,
        closed_5m_price: float, five_bullish: bool, high_break: bool,
    ) -> bool:
        """P_V27_3: impulse+RSI 급등 대비 EMA 분리 부족 패턴만 차단하는 전진검증."""
        if not self.cfg.research_pv273_enabled or entry_price <= 0:
            return False
        now = datetime.now(timezone.utc)
        setup_id = str(source_setup_id).replace("P25SET-", "P273SET-", 1)
        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_3' AND symbol=? AND completed=0 LIMIT 1",
                (symbol,),
            ).fetchone():
                return False

        blocked, meta = self._pv273_entry_filter_match(details)
        if blocked:
            reason = str(meta.get("reason") or "V273_ENTRY_FILTER")
            self._register_pv273_blocked_ghost(
                symbol, details, setup_id, entry_price, reason, meta, closed_5m_price
            )
            append_entry_record(
                symbol,"RESEARCH_P_V27_3_BLOCKED","P_V27_3",float(details.get("p_v2_score") or 0),entry_price,reason,
                extra={**details,"p_v273_confirm_state":"BLOCKED","p_v273_block_reason":reason,
                       "p_v273_impulse_separation_lag_block":bool(meta.get("p_v273_impulse_separation_lag_block")),
                       "p_v273_live_entry_price":entry_price,"p_v273_closed_5m_price":closed_5m_price},
            )
            return False

        # LIVE 기본: 모든 종료 후 90분 동일종목 재진입 금지.
        live_cd, live_remaining, live_prior = self._research_live_cooldown_status("P_V27_3", symbol, now)
        if live_cd:
            reason = "LIVE90_COOLDOWN"
            meta_cd = {"cooldown_remaining_min": round(live_remaining, 1), "prior_result": live_prior}
            self._register_pv273_blocked_ghost(
                symbol, details, setup_id, entry_price, reason, meta_cd, closed_5m_price
            )
            append_entry_record(
                symbol,"RESEARCH_P_V27_3_BLOCKED_LIVE90","P_V27_3",
                float(details.get("p_v2_score") or 0),entry_price,
                f"LIVE90_COOLDOWN remaining={live_remaining:.1f}m",
                extra={**details,"p_v273_confirm_state":"BLOCKED_LIVE90","p_v273_block_reason":reason,
                       "p_v273_live_entry_price":entry_price,"p_v273_closed_5m_price":closed_5m_price,**meta_cd},
            )
            return False

        # STOP/LATE 후에는 V26 연구와 같은 180분 강화 cooldown 유지.
        cooldown, remaining, prior = self._variant_stop_cooldown_status("P_V27_3",symbol,now)
        if cooldown:
            reason = "STOP_COOLDOWN"
            meta_cd = {"cooldown_remaining_min": round(remaining, 1), "prior_failure_result": prior}
            self._register_pv273_blocked_ghost(
                symbol, details, setup_id, entry_price, reason, meta_cd, closed_5m_price
            )
            append_entry_record(
                symbol,"RESEARCH_P_V27_3_BLOCKED_COOLDOWN","P_V27_3",
                float(details.get("p_v2_score") or 0),entry_price,
                f"STOP_COOLDOWN remaining={remaining:.1f}m",
                extra={**details,"p_v273_confirm_state":"BLOCKED_COOLDOWN","p_v273_block_reason":reason,
                       "p_v273_live_entry_price":entry_price,"p_v273_closed_5m_price":closed_5m_price,**meta_cd},
            )
            return False

        allowed, why = self._variant_can_open_now(
            "P_V27_3",now,self.cfg.research_pv273_max_entries_per_15m,self.cfg.research_pv273_max_open_positions
        )
        if not allowed:
            append_entry_record(
                symbol,"RESEARCH_P_V27_3_SKIPPED","P_V27_3",
                float(details.get("p_v2_score") or 0),entry_price,why,
                extra={**details,"p_v273_confirm_state":why,"p_v273_block_reason":why,
                       "p_v273_live_entry_price":entry_price,"p_v273_closed_5m_price":closed_5m_price},
            )
            return False

        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_3' AND setup_id=? LIMIT 1",(setup_id,)
            ).fetchone():
                return False
            opened_at=now.isoformat()
            shadow_id=f"RSH-P_V27_3-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price=entry_price*(1+float(self.cfg.tp2_pct)/100)
            be_price=entry_price*(1+float(self.cfg.breakeven_stop_pct)/100)
            snap=dict(details)
            snap.update({
                "p_v273_confirm_state":"CONFIRMED_5M_BREAK","p_v273_block_reason":"",
                "p_v273_live_entry_price":entry_price,"p_v273_closed_5m_price":closed_5m_price,
                "p_v273_impulse_separation_lag_block":False,
                "p_v25_setup_id":source_setup_id,"p_v25_5m_bullish":five_bullish,
                "p_v25_prev_high_break":high_break,
            })
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,missing_condition,snapshot_json,
                    tp2_price,be_price,highest_price,lowest_price,last_price,last_checked_at,last_5m_bucket,last_15m_bucket,
                    mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    shadow_id,"P_V27_3",symbol,opened_at,int(now.timestamp()*1000),entry_price,
                    float(details.get("p_score") or 0),float(details.get("p_v2_score") or 0),
                    "confirmed_5m_break_v273",json.dumps(snap,ensure_ascii=False,default=str),
                    tp2_price,be_price,entry_price,entry_price,entry_price,opened_at,
                    str(int(now.timestamp())//300),str(int(now.timestamp())//900),0.0,0.0,setup_id,0,
                ),
            )
        append_entry_record(
            symbol,"RESEARCH_P_V27_3_ENTRY","P_V27_3",float(details.get("p_v2_score") or 0),entry_price,
            "live-price entry + V27-3 impulse-separation filter passed; actual_order=0",
            extra={**details,"shadow_id":shadow_id,"research_variant":"P_V27_3",
                   "shadow_entry_time":_kst_stamp(opened_at),
                   "p_v273_confirm_state":"CONFIRMED_5M_BREAK","p_v273_block_reason":"",
                   "p_v273_live_entry_price":entry_price,"p_v273_closed_5m_price":closed_5m_price},
        )
        return True

    def _clone_pv273_filter_only_from_v26(
        self, symbol: str, details: dict[str, Any], source_shadow_id: str, source_setup_id: str,
        source_opened_at: str, entry_price: float, closed_5m_price: float, five_bullish: bool, high_break: bool,
    ) -> None:
        """V26 실제진입을 기준으로 V27-3 필터 자체만 1:1 비교한다. 대체진입 없음."""
        if not self.cfg.research_pv273_filter_only_enabled or entry_price <= 0:
            return
        now = datetime.now(timezone.utc)
        setup_id = str(source_setup_id).replace("P25SET-", "P273FOSET-", 1)
        if setup_id == str(source_setup_id):
            setup_id = f"P273FO-{source_setup_id}"

        blocked, meta = self._pv273_entry_filter_match(details)
        reason = str(meta.get("reason") or ("V273_ENTRY_FILTER" if blocked else "v273_filter_pass"))
        common_extra = {
            **details,
            "research_variant": "P_V27_3_FILTER_ONLY",
            "p_v273fo_source_shadow_id": source_shadow_id,
            "p_v273fo_filter_pass": not blocked,
            "p_v273fo_impulse_separation_lag_block": bool(meta.get("p_v273_impulse_separation_lag_block")),
            "p_v273_live_entry_price": entry_price,
            "p_v273_closed_5m_price": closed_5m_price,
        }

        if blocked:
            append_entry_record(
                symbol, "RESEARCH_P_V27_3_FILTER_ONLY_BLOCKED", "P_V27_3_FILTER_ONLY",
                float(details.get("p_v2_score") or 0), entry_price, reason,
                extra={
                    **common_extra,
                    "p_v273fo_confirm_state": "BLOCKED_BY_V273_FILTER",
                    "p_v273fo_block_reason": reason,
                },
            )
            return

        with db() as conn:
            existing = conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_3_FILTER_ONLY' AND setup_id=? LIMIT 1",
                (setup_id,),
            ).fetchone()
            if existing:
                return
            open_same = conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_3_FILTER_ONLY' AND symbol=? AND completed=0 LIMIT 1",
                (symbol,),
            ).fetchone()
            if open_same:
                append_entry_record(
                    symbol, "RESEARCH_P_V27_3_FILTER_ONLY_SYNC_ERROR", "P_V27_3_FILTER_ONLY",
                    float(details.get("p_v2_score") or 0), entry_price, "duplicate_open_symbol",
                    extra={
                        **common_extra,
                        "p_v273fo_confirm_state": "SYNC_ERROR",
                        "p_v273fo_block_reason": "duplicate_open_symbol",
                    },
                )
                return

            opened_at = str(source_opened_at or now.isoformat())
            try:
                opened_dt = datetime.fromisoformat(opened_at)
                if opened_dt.tzinfo is None:
                    opened_dt = opened_dt.replace(tzinfo=timezone.utc)
            except Exception:
                opened_dt = now
                opened_at = now.isoformat()
            shadow_id = f"RSH-P_V27_3_FILTER_ONLY-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = entry_price * (1 + float(self.cfg.tp2_pct) / 100)
            be_price = entry_price * (1 + float(self.cfg.breakeven_stop_pct) / 100)
            snap = dict(details)
            snap.update({
                "p_v273fo_confirm_state": "CLONED_FROM_V26_FILTER_PASS",
                "p_v273fo_block_reason": "",
                "p_v273fo_source_shadow_id": source_shadow_id,
                "p_v273fo_filter_pass": True,
                "p_v273fo_impulse_separation_lag_block": False,
                "p_v273_live_entry_price": entry_price,
                "p_v273_closed_5m_price": closed_5m_price,
                "p_v25_setup_id": source_setup_id,
                "p_v25_5m_bullish": five_bullish,
                "p_v25_prev_high_break": high_break,
            })
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,missing_condition,snapshot_json,
                    tp2_price,be_price,highest_price,lowest_price,last_price,last_checked_at,last_5m_bucket,last_15m_bucket,
                    mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    shadow_id, "P_V27_3_FILTER_ONLY", symbol, opened_at, int(opened_dt.timestamp() * 1000), entry_price,
                    float(details.get("p_score") or 0), float(details.get("p_v2_score") or 0),
                    "v26_entry_v273_filter_pass", json.dumps(snap, ensure_ascii=False, default=str),
                    tp2_price, be_price, entry_price, entry_price, entry_price, opened_at,
                    str(int(opened_dt.timestamp()) // 300), str(int(opened_dt.timestamp()) // 900),
                    0.0, 0.0, setup_id, 0,
                ),
            )
        append_entry_record(
            symbol, "RESEARCH_P_V27_3_FILTER_ONLY_ENTRY", "P_V27_3_FILTER_ONLY",
            float(details.get("p_v2_score") or 0), entry_price,
            "same V26 entry/time/price; V27-3 filter passed; no replacement entries; actual_order=0",
            extra={
                **common_extra,
                "shadow_id": shadow_id,
                "shadow_entry_time": _kst_stamp(opened_at),
                "p_v273fo_confirm_state": "CLONED_FROM_V26_FILTER_PASS",
                "p_v273fo_block_reason": "",
            },
        )

    def _register_pv274_blocked_ghost(
        self, symbol: str, details: dict[str, Any], setup_id: str, entry_price: float,
        reason: str, meta: dict[str, Any], closed_5m_price: float,
    ) -> None:
        """V27-4가 차단한 후보를 STOP/BE 없이 180분 raw path로 추적한다."""
        if not self.cfg.research_pv274_ghost_tracking_enabled or entry_price <= 0:
            return
        now = datetime.now(timezone.utc)
        with db() as conn:
            existing = conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_4_BLOCK_GHOST' AND setup_id=? LIMIT 1",
                (setup_id,),
            ).fetchone()
            if existing:
                return
            opened_at = now.isoformat()
            shadow_id = f"RSH-P_V27_4_BLOCK_GHOST-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = entry_price * (1 + float(self.cfg.tp2_pct) / 100)
            be_price = entry_price * (1 + float(self.cfg.breakeven_stop_pct) / 100)
            snap = dict(details)
            snap.update({
                "p_v274_confirm_state": "BLOCKED_ENTRY_GHOST",
                "p_v274_block_reason": reason,
                "p_v274_ghost_type": "P_V27_4_BLOCK_GHOST",
                "p_v274_live_entry_price": entry_price,
                "p_v274_closed_5m_price": closed_5m_price,
                "raw_path_milestones": {},
                **meta,
            })
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,
                    missing_condition,snapshot_json,tp2_price,be_price,highest_price,lowest_price,last_price,
                    last_checked_at,last_5m_bucket,last_15m_bucket,mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    shadow_id,"P_V27_4_BLOCK_GHOST",symbol,opened_at,int(now.timestamp()*1000),entry_price,
                    float(details.get("p_score") or 0),float(details.get("p_v2_score") or 0),reason,
                    json.dumps(snap,ensure_ascii=False,default=str),tp2_price,be_price,entry_price,entry_price,entry_price,
                    opened_at,str(int(now.timestamp())//300),str(int(now.timestamp())//900),0.0,0.0,setup_id,0,
                ),
            )
        append_entry_record(
            symbol,"RESEARCH_P_V27_4_BLOCK_GHOST_ENTRY","P_V27_4_BLOCK_GHOST",
            float(details.get("p_v2_score") or 0),entry_price,
            f"blocked_by={reason}; raw_path_180m=1; actual_order=0",
            extra={**details,"shadow_id":shadow_id,"research_variant":"P_V27_4_BLOCK_GHOST",
                   "p_v274_confirm_state":"BLOCKED_ENTRY_GHOST","p_v274_block_reason":reason,
                   "p_v274_ghost_type":"P_V27_4_BLOCK_GHOST","p_v274_live_entry_price":entry_price,
                   "p_v274_closed_5m_price":closed_5m_price,**meta},
        )

    def _open_pv274_confirmed_shadow(
        self, symbol: str, details: dict[str, Any], source_setup_id: str, entry_price: float,
        closed_5m_price: float, five_bullish: bool, high_break: bool,
    ) -> bool:
        """P_V27_4: V25 5m 재가속 + 좁은 4-of-4 weak-structure filter만 전진검증."""
        if not self.cfg.research_pv274_enabled or entry_price <= 0:
            return False
        now = datetime.now(timezone.utc)
        setup_id = str(source_setup_id).replace("P25SET-", "P274SET-", 1)
        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_4' AND symbol=? AND completed=0 LIMIT 1",
                (symbol,),
            ).fetchone():
                return False

        blocked, meta = self._pv274_entry_filter_match(details)
        if blocked:
            reason = str(meta.get("reason") or "V274_ENTRY_FILTER")
            self._register_pv274_blocked_ghost(
                symbol, details, setup_id, entry_price, reason, meta, closed_5m_price
            )
            append_entry_record(
                symbol,"RESEARCH_P_V27_4_BLOCKED","P_V27_4",float(details.get("p_v2_score") or 0),entry_price,reason,
                extra={**details,"p_v274_confirm_state":"BLOCKED","p_v274_block_reason":reason,
                       "p_v274_weak_structure_block":bool(meta.get("p_v274_weak_structure_block")),
                       "p_v274_weak_count":int(meta.get("p_v274_weak_count") or 0),
                       "p_v274_weak_flags":str(meta.get("p_v274_weak_flags") or ""),
                       "p_v274_live_entry_price":entry_price,"p_v274_closed_5m_price":closed_5m_price},
            )
            return False

        # V27-4도 실제 LIVE 운영 가정과 같은 동일종목 cooldown을 유지한다.
        live_cd, live_remaining, live_prior = self._research_live_cooldown_status("P_V27_4", symbol, now)
        if live_cd:
            reason = "LIVE90_COOLDOWN"
            append_entry_record(
                symbol,"RESEARCH_P_V27_4_BLOCKED_LIVE90","P_V27_4",
                float(details.get("p_v2_score") or 0),entry_price,
                f"LIVE90_COOLDOWN remaining={live_remaining:.1f}m",
                extra={**details,"p_v274_confirm_state":"BLOCKED_LIVE90","p_v274_block_reason":reason,
                       "p_v274_live_entry_price":entry_price,"p_v274_closed_5m_price":closed_5m_price,
                       "cooldown_remaining_min":round(live_remaining,1),"prior_result":live_prior},
            )
            return False

        cooldown, remaining, prior = self._variant_stop_cooldown_status("P_V27_4",symbol,now)
        if cooldown:
            reason = "STOP_COOLDOWN"
            append_entry_record(
                symbol,"RESEARCH_P_V27_4_BLOCKED_COOLDOWN","P_V27_4",
                float(details.get("p_v2_score") or 0),entry_price,
                f"STOP_COOLDOWN remaining={remaining:.1f}m",
                extra={**details,"p_v274_confirm_state":"BLOCKED_COOLDOWN","p_v274_block_reason":reason,
                       "p_v274_live_entry_price":entry_price,"p_v274_closed_5m_price":closed_5m_price,
                       "cooldown_remaining_min":round(remaining,1),"prior_failure_result":prior},
            )
            return False

        allowed, why = self._variant_can_open_now(
            "P_V27_4",now,self.cfg.research_pv274_max_entries_per_15m,self.cfg.research_pv274_max_open_positions
        )
        if not allowed:
            append_entry_record(
                symbol,"RESEARCH_P_V27_4_SKIPPED","P_V27_4",
                float(details.get("p_v2_score") or 0),entry_price,why,
                extra={**details,"p_v274_confirm_state":why,"p_v274_block_reason":why,
                       "p_v274_live_entry_price":entry_price,"p_v274_closed_5m_price":closed_5m_price},
            )
            return False

        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_4' AND setup_id=? LIMIT 1",(setup_id,)
            ).fetchone():
                return False
            opened_at = now.isoformat()
            shadow_id = f"RSH-P_V27_4-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = entry_price*(1+float(self.cfg.tp2_pct)/100)
            be_price = entry_price*(1+float(self.cfg.breakeven_stop_pct)/100)
            snap = dict(details)
            snap.update({
                "p_v274_confirm_state":"CONFIRMED_5M_BREAK","p_v274_block_reason":"",
                "p_v274_live_entry_price":entry_price,"p_v274_closed_5m_price":closed_5m_price,
                "p_v274_weak_structure_block":False,
                "p_v274_weak_count":int(meta.get("p_v274_weak_count") or 0),
                "p_v274_weak_flags":str(meta.get("p_v274_weak_flags") or ""),
                "p_v25_setup_id":source_setup_id,"p_v25_5m_bullish":five_bullish,
                "p_v25_prev_high_break":high_break,
            })
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,missing_condition,snapshot_json,
                    tp2_price,be_price,highest_price,lowest_price,last_price,last_checked_at,last_5m_bucket,last_15m_bucket,
                    mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    shadow_id,"P_V27_4",symbol,opened_at,int(now.timestamp()*1000),entry_price,
                    float(details.get("p_score") or 0),float(details.get("p_v2_score") or 0),
                    "confirmed_5m_break_v274",json.dumps(snap,ensure_ascii=False,default=str),
                    tp2_price,be_price,entry_price,entry_price,entry_price,opened_at,
                    str(int(now.timestamp())//300),str(int(now.timestamp())//900),0.0,0.0,setup_id,0,
                ),
            )
        append_entry_record(
            symbol,"RESEARCH_P_V27_4_ENTRY","P_V27_4",float(details.get("p_v2_score") or 0),entry_price,
            "live-price entry + V27-4 weak-structure 4-of-4 filter passed; actual_order=0",
            extra={**details,"shadow_id":shadow_id,"research_variant":"P_V27_4",
                   "shadow_entry_time":_kst_stamp(opened_at),
                   "p_v274_confirm_state":"CONFIRMED_5M_BREAK","p_v274_block_reason":"",
                   "p_v274_weak_structure_block":False,
                   "p_v274_weak_count":int(meta.get("p_v274_weak_count") or 0),
                   "p_v274_weak_flags":str(meta.get("p_v274_weak_flags") or ""),
                   "p_v274_live_entry_price":entry_price,"p_v274_closed_5m_price":closed_5m_price},
        )
        return True

    def _clone_pv274_filter_only_from_v26(
        self, symbol: str, details: dict[str, Any], source_shadow_id: str, source_setup_id: str,
        source_opened_at: str, entry_price: float, closed_5m_price: float, five_bullish: bool, high_break: bool,
    ) -> None:
        """V26 실제진입을 기준으로 V27-4 weak-structure 필터 자체만 1:1 비교한다."""
        if not self.cfg.research_pv274_filter_only_enabled or entry_price <= 0:
            return
        now = datetime.now(timezone.utc)
        setup_id = str(source_setup_id).replace("P25SET-", "P274FOSET-", 1)
        if setup_id == str(source_setup_id):
            setup_id = f"P274FO-{source_setup_id}"

        blocked, meta = self._pv274_entry_filter_match(details)
        reason = str(meta.get("reason") or ("V274_ENTRY_FILTER" if blocked else "v274_filter_pass"))
        common_extra = {
            **details,
            "research_variant": "P_V27_4_FILTER_ONLY",
            "p_v274fo_source_shadow_id": source_shadow_id,
            "p_v274fo_filter_pass": not blocked,
            "p_v274fo_weak_structure_block": bool(meta.get("p_v274_weak_structure_block")),
            "p_v274fo_weak_count": int(meta.get("p_v274_weak_count") or 0),
            "p_v274fo_weak_flags": str(meta.get("p_v274_weak_flags") or ""),
            "p_v274_live_entry_price": entry_price,
            "p_v274_closed_5m_price": closed_5m_price,
        }

        if blocked:
            append_entry_record(
                symbol,"RESEARCH_P_V27_4_FILTER_ONLY_BLOCKED","P_V27_4_FILTER_ONLY",
                float(details.get("p_v2_score") or 0),entry_price,reason,
                extra={**common_extra,"p_v274fo_confirm_state":"BLOCKED_BY_V274_FILTER","p_v274fo_block_reason":reason},
            )
            return

        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_4_FILTER_ONLY' AND setup_id=? LIMIT 1",
                (setup_id,),
            ).fetchone():
                return
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_4_FILTER_ONLY' AND symbol=? AND completed=0 LIMIT 1",
                (symbol,),
            ).fetchone():
                append_entry_record(
                    symbol,"RESEARCH_P_V27_4_FILTER_ONLY_SYNC_ERROR","P_V27_4_FILTER_ONLY",
                    float(details.get("p_v2_score") or 0),entry_price,"duplicate_open_symbol",
                    extra={**common_extra,"p_v274fo_confirm_state":"SYNC_ERROR","p_v274fo_block_reason":"duplicate_open_symbol"},
                )
                return
            opened_at = str(source_opened_at or now.isoformat())
            try:
                opened_dt = datetime.fromisoformat(opened_at)
                if opened_dt.tzinfo is None:
                    opened_dt = opened_dt.replace(tzinfo=timezone.utc)
            except Exception:
                opened_dt = now
                opened_at = now.isoformat()
            shadow_id = f"RSH-P_V27_4_FILTER_ONLY-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = entry_price*(1+float(self.cfg.tp2_pct)/100)
            be_price = entry_price*(1+float(self.cfg.breakeven_stop_pct)/100)
            snap = dict(details)
            snap.update({
                "p_v274fo_confirm_state":"CLONED_FROM_V26_FILTER_PASS","p_v274fo_block_reason":"",
                "p_v274fo_source_shadow_id":source_shadow_id,"p_v274fo_filter_pass":True,
                "p_v274fo_weak_structure_block":False,
                "p_v274fo_weak_count":int(meta.get("p_v274_weak_count") or 0),
                "p_v274fo_weak_flags":str(meta.get("p_v274_weak_flags") or ""),
                "p_v274_live_entry_price":entry_price,"p_v274_closed_5m_price":closed_5m_price,
                "p_v25_setup_id":source_setup_id,"p_v25_5m_bullish":five_bullish,"p_v25_prev_high_break":high_break,
            })
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,missing_condition,snapshot_json,
                    tp2_price,be_price,highest_price,lowest_price,last_price,last_checked_at,last_5m_bucket,last_15m_bucket,
                    mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    shadow_id,"P_V27_4_FILTER_ONLY",symbol,opened_at,int(opened_dt.timestamp()*1000),entry_price,
                    float(details.get("p_score") or 0),float(details.get("p_v2_score") or 0),
                    "v26_entry_v274_filter_pass",json.dumps(snap,ensure_ascii=False,default=str),
                    tp2_price,be_price,entry_price,entry_price,entry_price,opened_at,
                    str(int(opened_dt.timestamp())//300),str(int(opened_dt.timestamp())//900),0.0,0.0,setup_id,0,
                ),
            )
        append_entry_record(
            symbol,"RESEARCH_P_V27_4_FILTER_ONLY_ENTRY","P_V27_4_FILTER_ONLY",
            float(details.get("p_v2_score") or 0),entry_price,
            "same V26 entry/time/price; V27-4 filter passed; no replacement entries; actual_order=0",
            extra={**common_extra,"shadow_id":shadow_id,"shadow_entry_time":_kst_stamp(opened_at),
                   "p_v274fo_confirm_state":"CLONED_FROM_V26_FILTER_PASS","p_v274fo_block_reason":""},
        )

    def _clone_pv271_from_v26(
        self, symbol: str, details: dict[str, Any], source_shadow_id: str, source_setup_id: str,
        entry_price: float, closed_5m_price: float, five_bullish: bool, high_break: bool,
    ) -> None:
        """V26 실제 Shadow 진입을 같은 시각/가격으로 복제해 손절만 비교한다."""
        if not self.cfg.research_pv271_enabled or entry_price <= 0:
            return
        now=datetime.now(timezone.utc)
        setup_id=str(source_setup_id).replace("P25SET-","P271SET-",1)
        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_1' AND setup_id=? LIMIT 1",(setup_id,)
            ).fetchone():
                return
            opened_at=now.isoformat()
            shadow_id=f"RSH-P_V27_1-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price=entry_price*(1+float(self.cfg.tp2_pct)/100)
            be_price=entry_price*(1+float(self.cfg.breakeven_stop_pct)/100)
            snap=dict(details)
            snap.update({
                "p_v271_confirm_state":"CLONED_FROM_V26","p_v271_source_shadow_id":source_shadow_id,
                "p_v271_stop_stage_active":False,"p_v271_stop_signal_price":None,"p_v271_stop_reason":"",
                "p_v27_live_entry_price":entry_price,"p_v27_closed_5m_price":closed_5m_price,
                "p_v25_5m_bullish":five_bullish,"p_v25_prev_high_break":high_break,
            })
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,missing_condition,snapshot_json,
                    tp2_price,be_price,highest_price,lowest_price,last_price,last_checked_at,last_5m_bucket,last_15m_bucket,
                    mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (shadow_id,"P_V27_1",symbol,opened_at,int(now.timestamp()*1000),entry_price,float(details.get("p_score") or 0),
                 float(details.get("p_v2_score") or 0),"v26_entry_clone_stop_only",json.dumps(snap,ensure_ascii=False,default=str),
                 tp2_price,be_price,entry_price,entry_price,entry_price,opened_at,str(int(now.timestamp())//300),
                 str(int(now.timestamp())//900),0.0,0.0,setup_id,0),
            )
        append_entry_record(
            symbol,"RESEARCH_P_V27_1_ENTRY","P_V27_1",float(details.get("p_v2_score") or 0),entry_price,
            "same V26 live-price entry; stop-engine-only comparison; actual_order=0",
            extra={**details,"shadow_id":shadow_id,"research_variant":"P_V27_1",
                   "p_v271_confirm_state":"CLONED_FROM_V26","p_v271_source_shadow_id":source_shadow_id,
                   "p_v271_stop_stage_active":False,"p_v27_live_entry_price":entry_price,
                   "p_v27_closed_5m_price":closed_5m_price},
        )

    def _clone_pv271r_from_v26(
        self, symbol: str, details: dict[str, Any], source_shadow_id: str, source_setup_id: str,
        entry_price: float, closed_5m_price: float, five_bullish: bool, high_break: bool,
    ) -> None:
        """P_V27_1R: V26 진입을 1:1 복제하고 staged-REBOUND recovery만 바꾼다."""
        if not self.cfg.research_pv271r_enabled or entry_price <= 0:
            return
        now = datetime.now(timezone.utc)
        setup_id = str(source_setup_id).replace("P25SET-", "P271RSET-", 1)
        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_1R' AND setup_id=? LIMIT 1", (setup_id,)
            ).fetchone():
                return
            opened_at = now.isoformat()
            shadow_id = f"RSH-P_V27_1R-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = entry_price * (1 + float(self.cfg.tp2_pct) / 100)
            be_price = entry_price * (1 + float(self.cfg.breakeven_stop_pct) / 100)
            snap = dict(details)
            snap.update({
                "p_v271r_confirm_state": "CLONED_FROM_V26",
                "p_v271r_source_shadow_id": source_shadow_id,
                "p_v271_stop_stage_active": False,
                "p_v271_stop_signal_price": None,
                "p_v271_stop_reason": "",
                "p_v271r_recovery_watch_active": False,
                "p_v271r_recovery_confirmed": False,
                "p_v271r_recovery_reason": "",
                "p_v271r_recovery_started_at": "",
                "p_v27_live_entry_price": entry_price,
                "p_v27_closed_5m_price": closed_5m_price,
                "p_v25_5m_bullish": five_bullish,
                "p_v25_prev_high_break": high_break,
            })
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,missing_condition,snapshot_json,
                    tp2_price,be_price,highest_price,lowest_price,last_price,last_checked_at,last_5m_bucket,last_15m_bucket,
                    mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    shadow_id, "P_V27_1R", symbol, opened_at, int(now.timestamp()*1000), entry_price,
                    float(details.get("p_score") or 0), float(details.get("p_v2_score") or 0),
                    "v26_entry_clone_staged_rebound_recovery", json.dumps(snap,ensure_ascii=False,default=str),
                    tp2_price, be_price, entry_price, entry_price, entry_price, opened_at,
                    str(int(now.timestamp())//300), str(int(now.timestamp())//900), 0.0, 0.0, setup_id, 0,
                ),
            )
        append_entry_record(
            symbol, "RESEARCH_P_V27_1R_ENTRY", "P_V27_1R", float(details.get("p_v2_score") or 0), entry_price,
            "same V26 live-price entry; V27-1 control + staged REBOUND recovery only; actual_order=0",
            extra={
                **details, "shadow_id": shadow_id, "research_variant": "P_V27_1R",
                "p_v271r_confirm_state": "CLONED_FROM_V26", "p_v271r_source_shadow_id": source_shadow_id,
                "p_v271r_recovery_watch_active": False, "p_v271r_recovery_confirmed": False,
                "p_v27_live_entry_price": entry_price, "p_v27_closed_5m_price": closed_5m_price,
            },
        )

    def _register_pv26_blocked_ghost(
        self,
        symbol: str,
        details: dict[str, Any],
        setup_id: str,
        entry_price: float,
        ghost_variant: str,
        reason: str,
        ghost_meta: dict[str, Any] | None = None,
    ) -> None:
        """V26이 사전차단한 거래를 슬롯과 무관하게 원래 V25 관리규칙으로 끝까지 추적한다."""
        if not self.cfg.research_pv26_ghost_tracking_enabled or entry_price <= 0:
            return
        now = datetime.now(timezone.utc)
        meta = dict(ghost_meta or {})
        with db() as conn:
            existing = conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant=? AND setup_id=? LIMIT 1",
                (ghost_variant, setup_id),
            ).fetchone()
            if existing:
                return
            opened_at = now.isoformat()
            shadow_id = f"RSH-{ghost_variant}-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = entry_price * (1 + float(self.cfg.tp2_pct) / 100)
            be_price = entry_price * (1 + float(self.cfg.breakeven_stop_pct) / 100)
            snap = dict(details)
            snap.update({
                "p_v26_confirm_state": "BLOCKED_GHOST",
                "p_v26_block_reason": reason,
                "p_v26_ghost_type": ghost_variant,
                **meta,
            })
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,
                    missing_condition,snapshot_json,tp2_price,be_price,highest_price,lowest_price,last_price,
                    last_checked_at,last_5m_bucket,last_15m_bucket,mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    shadow_id, ghost_variant, symbol, opened_at, int(now.timestamp() * 1000), entry_price,
                    float(details.get("p_score") or 0), float(details.get("p_v2_score") or 0), reason,
                    json.dumps(snap, ensure_ascii=False, default=str), tp2_price, be_price,
                    entry_price, entry_price, entry_price, opened_at, str(int(now.timestamp())//300),
                    str(int(now.timestamp())//900), 0.0, 0.0, setup_id, 0,
                ),
            )
        append_entry_record(
            symbol, f"RESEARCH_{ghost_variant}_ENTRY", ghost_variant,
            float(details.get("p_v2_score") or 0), entry_price,
            f"blocked_by={reason}; ghost_only=1; actual_order=0",
            extra={
                **details,
                "shadow_id": shadow_id,
                "research_variant": ghost_variant,
                "shadow_entry_time": _kst_stamp(opened_at),
                "p_v26_confirm_state": "BLOCKED_GHOST",
                "p_v26_block_reason": reason,
                "p_v26_ghost_type": ghost_variant,
                **meta,
            },
        )

    def _clone_pv26_late_ghost(
        self,
        review: sqlite3.Row,
        highest: float,
        lowest: float,
        current_price: float,
        late_meta: dict[str, Any],
    ) -> None:
        """V26 장기실패 종료 뒤 원래 V25라면 어떻게 끝났을지 같은 최초진입부터 계속 추적한다."""
        if not self.cfg.research_pv26_ghost_tracking_enabled:
            return
        symbol = str(review["symbol"] or "")
        opened_at = str(review["opened_at"] or "")
        setup_id = str(review["setup_id"] or "")
        with db() as conn:
            existing = conn.execute(
                """SELECT 1 FROM research_shadow_reviews
                   WHERE variant='P_V26_LATE_GHOST' AND symbol=? AND opened_at=? LIMIT 1""",
                (symbol, opened_at),
            ).fetchone()
            if existing:
                return
            now = datetime.now(timezone.utc)
            shadow_id = f"RSH-P_V26_LATE_GHOST-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            try:
                snap = json.loads(str(review["snapshot_json"] or "{}"))
            except Exception:
                snap = {}
            snap.update({
                "p_v26_confirm_state": "LATE_EXIT_GHOST_CONTINUE",
                "p_v26_block_reason": "LATE_FAILURE_EXIT",
                "p_v26_ghost_type": "P_V26_LATE_GHOST",
                "v26_late_exit_price": current_price,
                "v26_late_meta": late_meta,
            })
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,
                    missing_condition,snapshot_json,tp1_done,tp1_ts,tp1_price,tp2_price,be_price,
                    highest_price,lowest_price,last_price,last_checked_at,last_5m_bucket,last_15m_bucket,
                    mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    shadow_id, "P_V26_LATE_GHOST", symbol, opened_at, int(review["entry_ts_ms"] or 0),
                    float(review["entry_price"] or 0), float(review["old_p_score"] or 0),
                    float(review["new_p_score"] or 0), "continued_after_v26_late_exit",
                    json.dumps(snap, ensure_ascii=False, default=str), int(review["tp1_done"] or 0),
                    review["tp1_ts"], review["tp1_price"], float(review["tp2_price"] or 0),
                    float(review["be_price"] or 0), highest, lowest, current_price, now.isoformat(),
                    str(int(now.timestamp()) // 300), str(int(now.timestamp()) // 900),
                    float(review["mfe_pct"] or 0), float(review["mae_pct"] or 0), setup_id, 0,
                ),
            )
        append_entry_record(
            symbol, "RESEARCH_P_V26_LATE_GHOST_ENTRY", "P_V26_LATE_GHOST",
            float(review["new_p_score"] or 0), float(review["entry_price"] or 0),
            f"continue_after_late_exit_at={current_price}; actual_order=0",
            extra={
                "shadow_id": shadow_id,
                "research_variant": "P_V26_LATE_GHOST",
                "shadow_entry_time": _kst_stamp(opened_at),
                "p_v26_confirm_state": "LATE_EXIT_GHOST_CONTINUE",
                "p_v26_block_reason": "LATE_FAILURE_EXIT",
                "p_v26_ghost_type": "P_V26_LATE_GHOST",
            },
        )

    def _clone_pv26_stop_ghost(
        self,
        review: sqlite3.Row,
        stop_ts: str,
        stop_price: float,
        result_details: dict[str, Any],
        *,
        backfilled: bool = False,
    ) -> None:
        """P_V26 STOP 뒤 180분을 원래 진입가 기준으로 계속 추적한다."""
        if not self.cfg.research_pv26_stop_ghost_enabled:
            return
        source_shadow_id = str(review["shadow_id"] or "")
        symbol = str(review["symbol"] or "")
        entry_price = float(review["entry_price"] or 0)
        if not source_shadow_id or not symbol or entry_price <= 0 or stop_price <= 0:
            return
        with db() as conn:
            existing = conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V26_STOP_GHOST' AND missing_condition=? LIMIT 1",
                (f"source={source_shadow_id}",),
            ).fetchone()
            if existing:
                return
            try:
                stop_dt = datetime.fromisoformat(str(stop_ts))
                if stop_dt.tzinfo is None:
                    stop_dt = stop_dt.replace(tzinfo=timezone.utc)
            except Exception:
                stop_dt = datetime.now(timezone.utc)
                stop_ts = stop_dt.isoformat()
            try:
                source_snap = json.loads(str(review["snapshot_json"] or "{}"))
            except Exception:
                source_snap = {}
            stop_pct = (float(stop_price) / entry_price - 1.0) * 100.0
            stop_type = str(result_details.get("stop_type") or "STOP")
            snap = dict(source_snap)
            snap.update({
                "p_v26_confirm_state": "STOP_GHOST_TRACKING",
                "p_v26_block_reason": stop_type,
                "p_v26_ghost_type": "P_V26_STOP_GHOST",
                "source_shadow_id": source_shadow_id,
                "source_opened_at": str(review["opened_at"] or ""),
                "source_stop_ts": str(stop_ts),
                "source_stop_price": float(stop_price),
                "source_stop_pct": round(stop_pct, 4),
                "source_stop_type": stop_type,
                "stop_ghost_milestones": {},
                "stop_ghost_backfilled": bool(backfilled),
            })
            shadow_id = f"RSH-P_V26_STOP_GHOST-{stop_dt.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = entry_price * (1 + float(self.cfg.tp2_pct) / 100)
            be_price = entry_price * (1 + float(self.cfg.breakeven_stop_pct) / 100)
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,
                    missing_condition,snapshot_json,tp2_price,be_price,highest_price,lowest_price,last_price,
                    last_checked_at,last_5m_bucket,last_15m_bucket,mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    shadow_id, "P_V26_STOP_GHOST", symbol, stop_dt.isoformat(), int(stop_dt.timestamp() * 1000),
                    entry_price, float(review["old_p_score"] or 0), float(review["new_p_score"] or 0),
                    f"source={source_shadow_id}", json.dumps(snap, ensure_ascii=False, default=str),
                    tp2_price, be_price, float(stop_price), float(stop_price), float(stop_price),
                    datetime.now(timezone.utc).isoformat(), str(int(stop_dt.timestamp()) // 300),
                    str(int(stop_dt.timestamp()) // 900), stop_pct, stop_pct, str(review["setup_id"] or ""), 0,
                ),
            )
        append_entry_record(
            symbol, "RESEARCH_P_V26_STOP_GHOST_ENTRY", "P_V26_STOP_GHOST",
            float(review["new_p_score"] or 0), float(stop_price),
            f"source_stop={stop_type}; stop_pct={stop_pct:.3f}%; backfilled={int(backfilled)}; actual_order=0",
            extra={
                "shadow_id": shadow_id,
                "research_variant": "P_V26_STOP_GHOST",
                "shadow_entry_time": _kst_stamp(stop_dt.isoformat()),
                "p_v26_confirm_state": "STOP_GHOST_TRACKING",
                "p_v26_block_reason": stop_type,
                "p_v26_ghost_type": "P_V26_STOP_GHOST",
            },
        )

    def _backfill_pv26_stop_ghosts(self) -> None:
        """패치 전 이미 발생한 P_V26 STOP도 남은 3시간 구간을 소급 추적한다."""
        if not self.cfg.research_pv26_stop_ghost_enabled:
            return
        with db() as conn:
            rows = conn.execute(
                """SELECT * FROM research_shadow_reviews
                   WHERE variant='P_V26' AND completed=1 AND result='STOP'
                   ORDER BY result_ts"""
            ).fetchall()
        for row in rows:
            try:
                details = json.loads(str(row["result_details"] or "{}"))
            except Exception:
                details = {}
            try:
                self._clone_pv26_stop_ghost(
                    row, str(row["result_ts"] or utc_now()), float(row["result_price"] or 0), details, backfilled=True
                )
            except Exception as exc:
                append_entry_record(
                    str(row["symbol"] or ""), "RESEARCH_P_V26_STOP_GHOST_BACKFILL_ERROR",
                    "P_V26_STOP_GHOST", 0, float(row["result_price"] or 0), f"{type(exc).__name__}: {exc}"
                )

    def _pv2741_weak_watch_meta(self, details: dict[str, Any]) -> dict[str, Any]:
        """V27-4.1: hard block 없이 core weak 3-of-3 위험도만 표시한다."""
        vals = {}
        for k in ("ema20_slope_pct", "ema9_ema20_gap_pct", "p_v21_persistence_score"):
            try:
                vals[k] = float(details.get(k))
            except (TypeError, ValueError):
                vals[k] = None
        flags: list[str] = []
        if vals["ema20_slope_pct"] is not None and vals["ema20_slope_pct"] < float(self.cfg.research_pv2741_ema20_slope_max_pct):
            flags.append("EMA20_SLOPE")
        if vals["ema9_ema20_gap_pct"] is not None and vals["ema9_ema20_gap_pct"] < float(self.cfg.research_pv2741_ema_gap_max_pct):
            flags.append("EMA_GAP")
        if vals["p_v21_persistence_score"] is not None and vals["p_v21_persistence_score"] < float(self.cfg.research_pv2741_persistence_max):
            flags.append("PERSISTENCE")
        core_weak = len(flags) == 3
        return {
            "p_v2741_weak_watch": core_weak,
            "p_v2741_weak_count": len(flags),
            "p_v2741_weak_flags": ",".join(flags),
            "p_v2741_watch_logged": [],
            "p_v2741_thresholds": {
                "ema20_slope_max": float(self.cfg.research_pv2741_ema20_slope_max_pct),
                "ema_gap_max": float(self.cfg.research_pv2741_ema_gap_max_pct),
                "persistence_max": float(self.cfg.research_pv2741_persistence_max),
            },
        }

    def _open_pv2741_confirmed_shadow(
        self, symbol: str, details: dict[str, Any], source_setup_id: str, entry_price: float,
        closed_5m_price: float, five_bullish: bool, high_break: bool,
    ) -> bool:
        """P_V27_4_1: V25 재가속 진입을 그대로 쓰고 hard block 없이 weak-watch telemetry만 추가한다."""
        if not self.cfg.research_pv2741_enabled or entry_price <= 0:
            return False
        now = datetime.now(timezone.utc)
        setup_id = str(source_setup_id).replace("P25SET-", "P2741SET-", 1)
        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_4_1' AND symbol=? AND completed=0 LIMIT 1",
                (symbol,),
            ).fetchone():
                return False

        meta = self._pv2741_weak_watch_meta(details)
        live_cd, live_remaining, live_prior = self._research_live_cooldown_status("P_V27_4_1", symbol, now)
        if live_cd:
            append_entry_record(
                symbol,"RESEARCH_P_V27_4_1_BLOCKED_LIVE90","P_V27_4_1",
                float(details.get("p_v2_score") or 0),entry_price,
                f"LIVE90_COOLDOWN remaining={live_remaining:.1f}m",
                extra={**details,**meta,"p_v2741_confirm_state":"BLOCKED_LIVE90",
                       "p_v2741_live_entry_price":entry_price,"p_v2741_closed_5m_price":closed_5m_price,
                       "cooldown_remaining_min":round(live_remaining,1),"prior_result":live_prior},
            )
            return False
        cooldown, remaining, prior = self._variant_stop_cooldown_status("P_V27_4_1",symbol,now)
        if cooldown:
            append_entry_record(
                symbol,"RESEARCH_P_V27_4_1_BLOCKED_COOLDOWN","P_V27_4_1",
                float(details.get("p_v2_score") or 0),entry_price,
                f"STOP_COOLDOWN remaining={remaining:.1f}m",
                extra={**details,**meta,"p_v2741_confirm_state":"BLOCKED_COOLDOWN",
                       "p_v2741_live_entry_price":entry_price,"p_v2741_closed_5m_price":closed_5m_price,
                       "cooldown_remaining_min":round(remaining,1),"prior_failure_result":prior},
            )
            return False
        allowed, why = self._variant_can_open_now(
            "P_V27_4_1",now,self.cfg.research_pv2741_max_entries_per_15m,self.cfg.research_pv2741_max_open_positions
        )
        if not allowed:
            append_entry_record(
                symbol,"RESEARCH_P_V27_4_1_SKIPPED","P_V27_4_1",
                float(details.get("p_v2_score") or 0),entry_price,why,
                extra={**details,**meta,"p_v2741_confirm_state":why,
                       "p_v2741_live_entry_price":entry_price,"p_v2741_closed_5m_price":closed_5m_price},
            )
            return False
        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_4_1' AND setup_id=? LIMIT 1",(setup_id,)
            ).fetchone():
                return False
            opened_at = now.isoformat()
            shadow_id = f"RSH-P_V27_4_1-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = entry_price*(1+float(self.cfg.tp2_pct)/100)
            be_price = entry_price*(1+float(self.cfg.breakeven_stop_pct)/100)
            snap = dict(details)
            snap.update({
                **meta,
                "p_v2741_confirm_state":"CONFIRMED_5M_BREAK",
                "p_v2741_live_entry_price":entry_price,
                "p_v2741_closed_5m_price":closed_5m_price,
                "p_v25_setup_id":source_setup_id,"p_v25_5m_bullish":five_bullish,"p_v25_prev_high_break":high_break,
            })
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,missing_condition,snapshot_json,
                    tp2_price,be_price,highest_price,lowest_price,last_price,last_checked_at,last_5m_bucket,last_15m_bucket,
                    mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    shadow_id,"P_V27_4_1",symbol,opened_at,int(now.timestamp()*1000),entry_price,
                    float(details.get("p_score") or 0),float(details.get("p_v2_score") or 0),
                    "confirmed_5m_break_v2741_no_hard_block",json.dumps(snap,ensure_ascii=False,default=str),
                    tp2_price,be_price,entry_price,entry_price,entry_price,opened_at,
                    str(int(now.timestamp())//300),str(int(now.timestamp())//900),0.0,0.0,setup_id,0,
                ),
            )
        append_entry_record(
            symbol,"RESEARCH_P_V27_4_1_ENTRY","P_V27_4_1",float(details.get("p_v2_score") or 0),entry_price,
            f"V25 live-price entry; no hard block; weak_watch={int(bool(meta.get('p_v2741_weak_watch')))}; actual_order=0",
            extra={**details,**meta,"shadow_id":shadow_id,"research_variant":"P_V27_4_1",
                   "shadow_entry_time":_kst_stamp(opened_at),"p_v2741_confirm_state":"CONFIRMED_5M_BREAK",
                   "p_v2741_live_entry_price":entry_price,"p_v2741_closed_5m_price":closed_5m_price},
        )
        return True

    def _pv2742_adjusted_filter_meta(self, details: dict[str, Any]) -> dict[str, Any]:
        """v4.3.78: Forward 100건에서 범위/측정시점을 보정한 고정 진입필터.

        설계 원칙
        - V25/V27.4.1 진입 골격은 변경하지 않는다.
        - 과거 A9에 포함됐던 9개 규칙(05/08/09/11/12/20/27/32/34)만 V27.4.2 내부에서 직접 계산한다.
        - A9-11 규칙은 live-volume의 봉 시작시점 왜곡을 줄이기 위해 완성 15m volume_ratio>=0.70을 추가한다.
        - A9-27 규칙은 -0.50 경계 턱걸이를 제외하기 위해 prev1<=-0.90으로 좁힌다.
        - B25-v2 / C1-adjusted / C2-v2 / C3-adjusted / C5-adjusted를 OR로 차단한다.
        - C4/C6은 새 Forward에서 정상 TP/BE 오차단만 보여 hard block에서 제외한다.
        - 이 함수의 숫자는 새 Forward 검증 중 절대 자동튜닝하지 않는다.
        """
        def f(key: str) -> float | None:
            try:
                v = details.get(key)
                if v in (None, ""):
                    return None
                return float(v)
            except (TypeError, ValueError):
                return None

        def ge(v: float | None, x: float) -> bool: return v is not None and v >= x
        def le(v: float | None, x: float) -> bool: return v is not None and v <= x
        def between(v: float | None, lo: float, hi: float) -> bool: return v is not None and lo <= v <= hi

        p1=f("prev1_candle_gain_pct"); p2=f("prev2_candle_gain_pct"); p3=f("prev3_candle_gain_pct")
        rd1=f("rsi_change_prev1"); rd2=f("rsi_change_prev2"); rsi=f("rsi"); rsi_delta=f("rsi_delta")
        vol=f("volume_ratio"); lvol=f("live_volume_ratio"); lg=f("live_candle_gain_pct")
        es=f("ema20_slope_pct"); gap9_20=f("ema9_ema20_gap_pct"); gap20_60=f("ema20_ema60_gap_pct")
        pers=f("p_v21_persistence_score"); v2=f("p_v2_score"); reb=f("rebound_from_low_pct")
        pull=f("pullback_from_high_pct"); h1=f("one_hour_signed_move_pct"); ps=f("p_score")
        b15=f("btc_15m_change_pct"); e15=f("eth_15m_change_pct"); low1=f("low_change_prev1_pct")

        # A9 adjusted: 과거 A9의 9개 규칙만 직접 계산하며, 11/27 규칙은 새 Forward에서 확인된 범위/측정문제를 보정.
        a9_r05 = le(vol,0.50) and ge(gap9_20,2.0) and between(reb,8.0,15.0) and le(h1,2.0)
        a9_r08 = le(rd1,-1.0) and ge(reb,10.0) and le(h1,1.5)
        a9_r09 = le(p2,0.30) and ge(lvol,1.0) and le(e15,-0.10)
        a9_r11 = le(lvol,0.10) and le(reb,6.0) and ge(v2,85.0) and ge(vol,0.70)
        a9_r12 = between(p1,-1.5,-1.0) and le(p3,1.0)
        a9_r20 = ge(p2,1.5) and ge(v2,95.0) and le(ps,100.0)
        a9_r27 = le(p1,-0.90) and between(h1,1.0,1.5)
        a9_r32 = le(vol,0.50) and ge(v2,85.0) and le(e15,0.0)
        a9_r34 = ge(es,0.30) and le(pers,50.0) and le(reb,5.0)
        a9_flags = {
            "A05": a9_r05, "A08": a9_r08, "A09": a9_r09, "A11": a9_r11, "A12": a9_r12,
            "A20": a9_r20, "A27": a9_r27, "A32": a9_r32, "A34": a9_r34,
        }
        a9_ids = [k for k,v in a9_flags.items() if v]
        a9 = bool(a9_ids)

        # B25-v2: prev1<=-0.5 + prev3<=0.5 + BTC15>=0.05 + p_v2_score>=70.
        b25 = le(p1,-0.50) and le(p3,0.50) and ge(b15,0.05) and ge(v2,70.0)

        # C1 adjusted: 기존 약한 중기구조에 score/live-volume/RSI 안정성 확인을 추가.
        c1 = (
            le(gap20_60,0.80) and le(reb,4.0) and ge(v2,60.0)
            and between(lvol,0.80,1.60) and ge(rd1,-3.0)
        )

        # C2-v2: 기존 C2의 보수형. C2 정의 혼선을 없애기 위해 p_v2_score>=60을 명시한다.
        c2 = le(vol,0.35) and le(reb,5.0) and ge(v2,60.0)

        # C3 adjusted: 정상적인 0.8~0.9% 돌파를 덜 자르도록 live gain/gap/persistence 범위를 재정의.
        c3 = le(pull,-0.30) and ge(lg,0.90) and le(gap20_60,0.80) and ge(pers,50.0)

        # C5 adjusted: 기존 RSI rollover형에 live gain 0.5~1.2% 범위를 추가.
        c5 = (
            ge(low1,1.50) and le(rd2,-3.30) and between(rsi,62.0,72.0)
            and ge(rsi_delta,0.0) and between(lg,0.50,1.20)
        )

        flags = {
            "A9": a9,
            "B25_V2": b25,
            "C1_ADJ": c1,
            "C2_V2": c2,
            "C3_ADJ": c3,
            "C5_ADJ": c5,
        }
        reasons = [k for k,v in flags.items() if v]
        blocked = bool(reasons)
        return {
            "p_v2742_filter_block": blocked,
            "p_v2742_filter_reasons": ",".join(reasons),
            "p_v2742_a9_match": a9,
            "p_v2742_a9_rule_ids": ",".join(a9_ids),
            "p_v2742_b25_v2": b25,
            "p_v2742_c1_adj": c1,
            "p_v2742_c2_v2": c2,
            "p_v2742_c3_adj": c3,
            "p_v2742_c5_adj": c5,
            "p_v2742_c4_enabled": False,
            "p_v2742_c6_enabled": False,
            "p_v2742_filter_version": "ADJ_A9R11R27_B25_C1_C2V2_C3_C5_V1",
            "p_v2742_filter_frozen": True,
        }

    def _register_pv2742_block_ghost(
        self, symbol: str, details: dict[str, Any], source_setup_id: str, entry_price: float,
        closed_5m_price: float, five_bullish: bool, high_break: bool, meta: dict[str, Any],
    ) -> None:
        """P_V27_4_2가 진입 차단한 거래를 V27.4.1과 같은 기존 공통관리로 끝까지 추적한다."""
        if not self.cfg.research_pv2742_block_ghost_enabled or entry_price <= 0:
            return
        now = datetime.now(timezone.utc)
        setup_id = str(source_setup_id).replace("P25SET-", "P2742BGSET-", 1)
        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_4_2_BLOCK_GHOST' AND setup_id=? LIMIT 1",
                (setup_id,),
            ).fetchone():
                return
            opened_at = now.isoformat()
            shadow_id = f"RSH-P_V27_4_2_BLOCK_GHOST-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = entry_price * (1 + float(self.cfg.tp2_pct) / 100)
            be_price = entry_price * (1 + float(self.cfg.breakeven_stop_pct) / 100)
            snap = dict(details)
            snap.update({
                **meta,
                "p_v2742_confirm_state": "BLOCKED_FILTER_GHOST",
                "p_v2742_live_entry_price": entry_price,
                "p_v2742_closed_5m_price": closed_5m_price,
                "p_v2742_is_block_ghost": True,
                "p_v25_setup_id": source_setup_id,
                "p_v25_5m_bullish": five_bullish,
                "p_v25_prev_high_break": high_break,
            })
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,missing_condition,snapshot_json,
                    tp2_price,be_price,highest_price,lowest_price,last_price,last_checked_at,last_5m_bucket,last_15m_bucket,
                    mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    shadow_id,"P_V27_4_2_BLOCK_GHOST",symbol,opened_at,int(now.timestamp()*1000),entry_price,
                    float(details.get("p_score") or 0),float(details.get("p_v2_score") or 0),
                    f"adjusted_filter_block:{meta.get('p_v2742_filter_reasons') or ''}",json.dumps(snap,ensure_ascii=False,default=str),
                    tp2_price,be_price,entry_price,entry_price,entry_price,opened_at,
                    str(int(now.timestamp())//300),str(int(now.timestamp())//900),0.0,0.0,setup_id,0,
                ),
            )
        append_entry_record(
            symbol,"RESEARCH_P_V27_4_2_BLOCK_GHOST_ENTRY","P_V27_4_2_BLOCK_GHOST",
            float(details.get("p_v2_score") or 0),entry_price,
            f"blocked_by={meta.get('p_v2742_filter_reasons') or 'UNKNOWN'}; baseline-management ghost; actual_order=0",
            extra={**details,**meta,"shadow_id":shadow_id,"research_variant":"P_V27_4_2_BLOCK_GHOST",
                   "shadow_entry_time":_kst_stamp(opened_at),"p_v2742_confirm_state":"BLOCKED_FILTER_GHOST",
                   "p_v2742_live_entry_price":entry_price,"p_v2742_closed_5m_price":closed_5m_price},
        )

    def _open_pv2742_confirmed_shadow(
        self, symbol: str, details: dict[str, Any], source_setup_id: str, entry_price: float,
        closed_5m_price: float, five_bullish: bool, high_break: bool,
    ) -> bool:
        """P_V27_4_2: V25 진입 + adjusted hard filter만 검증. 종료관리는 V27.4.1과 동일."""
        if not self.cfg.research_pv2742_enabled or entry_price <= 0:
            return False
        now = datetime.now(timezone.utc)
        setup_id = str(source_setup_id).replace("P25SET-", "P2742SET-", 1)
        meta = self._pv2742_adjusted_filter_meta(details)

        if bool(meta.get("p_v2742_filter_block")):
            append_entry_record(
                symbol,"RESEARCH_P_V27_4_2_BLOCKED_FILTER","P_V27_4_2",
                float(details.get("p_v2_score") or 0),entry_price,
                f"adjusted_filter={meta.get('p_v2742_filter_reasons') or 'UNKNOWN'}; actual_order=0",
                extra={**details,**meta,"p_v2742_confirm_state":"BLOCKED_FILTER",
                       "p_v2742_live_entry_price":entry_price,"p_v2742_closed_5m_price":closed_5m_price},
            )
            self._register_pv2742_block_ghost(
                symbol, details, source_setup_id, entry_price, closed_5m_price, five_bullish, high_break, meta
            )
            return False

        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_4_2' AND symbol=? AND completed=0 LIMIT 1",
                (symbol,),
            ).fetchone():
                return False

        live_cd, live_remaining, live_prior = self._research_live_cooldown_status("P_V27_4_2", symbol, now)
        if live_cd:
            append_entry_record(
                symbol,"RESEARCH_P_V27_4_2_BLOCKED_LIVE90","P_V27_4_2",
                float(details.get("p_v2_score") or 0),entry_price,
                f"LIVE90_COOLDOWN remaining={live_remaining:.1f}m",
                extra={**details,**meta,"p_v2742_confirm_state":"BLOCKED_LIVE90",
                       "p_v2742_live_entry_price":entry_price,"p_v2742_closed_5m_price":closed_5m_price,
                       "cooldown_remaining_min":round(live_remaining,1),"prior_result":live_prior},
            )
            return False
        cooldown, remaining, prior = self._variant_stop_cooldown_status("P_V27_4_2",symbol,now)
        if cooldown:
            append_entry_record(
                symbol,"RESEARCH_P_V27_4_2_BLOCKED_COOLDOWN","P_V27_4_2",
                float(details.get("p_v2_score") or 0),entry_price,
                f"STOP_COOLDOWN remaining={remaining:.1f}m",
                extra={**details,**meta,"p_v2742_confirm_state":"BLOCKED_COOLDOWN",
                       "p_v2742_live_entry_price":entry_price,"p_v2742_closed_5m_price":closed_5m_price,
                       "cooldown_remaining_min":round(remaining,1),"prior_failure_result":prior},
            )
            return False
        allowed, why = self._variant_can_open_now(
            "P_V27_4_2",now,self.cfg.research_pv2742_max_entries_per_15m,self.cfg.research_pv2742_max_open_positions
        )
        if not allowed:
            append_entry_record(
                symbol,"RESEARCH_P_V27_4_2_SKIPPED","P_V27_4_2",
                float(details.get("p_v2_score") or 0),entry_price,why,
                extra={**details,**meta,"p_v2742_confirm_state":why,
                       "p_v2742_live_entry_price":entry_price,"p_v2742_closed_5m_price":closed_5m_price},
            )
            return False
        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_4_2' AND setup_id=? LIMIT 1",(setup_id,)
            ).fetchone():
                return False
            opened_at = now.isoformat()
            shadow_id = f"RSH-P_V27_4_2-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = entry_price*(1+float(self.cfg.tp2_pct)/100)
            be_price = entry_price*(1+float(self.cfg.breakeven_stop_pct)/100)
            snap = dict(details)
            snap.update({
                **meta,
                "p_v2742_confirm_state":"CONFIRMED_FILTER_PASS",
                "p_v2742_live_entry_price":entry_price,
                "p_v2742_closed_5m_price":closed_5m_price,
                "p_v2742_is_block_ghost":False,
                "p_v25_setup_id":source_setup_id,"p_v25_5m_bullish":five_bullish,"p_v25_prev_high_break":high_break,
            })
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,missing_condition,snapshot_json,
                    tp2_price,be_price,highest_price,lowest_price,last_price,last_checked_at,last_5m_bucket,last_15m_bucket,
                    mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    shadow_id,"P_V27_4_2",symbol,opened_at,int(now.timestamp()*1000),entry_price,
                    float(details.get("p_score") or 0),float(details.get("p_v2_score") or 0),
                    "confirmed_5m_break_adjusted_filter_pass",json.dumps(snap,ensure_ascii=False,default=str),
                    tp2_price,be_price,entry_price,entry_price,entry_price,opened_at,
                    str(int(now.timestamp())//300),str(int(now.timestamp())//900),0.0,0.0,setup_id,0,
                ),
            )
        append_entry_record(
            symbol,"RESEARCH_P_V27_4_2_ENTRY","P_V27_4_2",float(details.get("p_v2_score") or 0),entry_price,
            f"V25 entry + adjusted filter PASS; baseline management; actual_order=0; filter={meta.get('p_v2742_filter_version')}",
            extra={**details,**meta,"shadow_id":shadow_id,"research_variant":"P_V27_4_2",
                   "shadow_entry_time":_kst_stamp(opened_at),"p_v2742_confirm_state":"CONFIRMED_FILTER_PASS",
                   "p_v2742_live_entry_price":entry_price,"p_v2742_closed_5m_price":closed_5m_price},
        )
        return True

    def _clone_pv2742_stop_ghost(
        self, review: sqlite3.Row, stop_ts: str, stop_price: float, result_details: dict[str, Any], *, backfilled: bool = False,
    ) -> None:
        """P_V27_4_2 STOP 뒤 180분 raw path 추적. 진입필터 검증용이며 V27-1과 무관하다."""
        if not self.cfg.research_pv2742_stop_ghost_enabled:
            return
        source_shadow_id = str(review["shadow_id"] or "")
        symbol = str(review["symbol"] or "")
        entry_price = float(review["entry_price"] or 0)
        if not source_shadow_id or not symbol or entry_price <= 0 or stop_price <= 0:
            return
        ghost_variant = "P_V27_4_2_STOP_GHOST"
        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant=? AND missing_condition=? LIMIT 1",
                (ghost_variant, f"source={source_shadow_id}"),
            ).fetchone():
                return
            try:
                stop_dt = datetime.fromisoformat(str(stop_ts))
                if stop_dt.tzinfo is None:
                    stop_dt = stop_dt.replace(tzinfo=timezone.utc)
            except Exception:
                stop_dt = datetime.now(timezone.utc)
            try:
                source_snap = json.loads(str(review["snapshot_json"] or "{}"))
            except Exception:
                source_snap = {}
            stop_pct = (float(stop_price) / entry_price - 1.0) * 100.0
            stop_type = str(result_details.get("stop_type") or "STOP")
            snap = dict(source_snap)
            snap.update({
                "p_v2742_confirm_state": "STOP_GHOST_TRACKING",
                "p_v2742_ghost_type": ghost_variant,
                "p_v2742_source_shadow_id": source_shadow_id,
                "source_stop_ts": str(stop_ts),
                "source_stop_price": float(stop_price),
                "source_stop_pct": round(stop_pct, 4),
                "source_stop_type": stop_type,
                "raw_path_milestones": {},
                "stop_ghost_backfilled": bool(backfilled),
            })
            shadow_id = f"RSH-{ghost_variant}-{stop_dt.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = entry_price * (1 + float(self.cfg.tp2_pct) / 100)
            be_price = entry_price * (1 + float(self.cfg.breakeven_stop_pct) / 100)
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,
                    missing_condition,snapshot_json,tp2_price,be_price,highest_price,lowest_price,last_price,
                    last_checked_at,last_5m_bucket,last_15m_bucket,mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (shadow_id,ghost_variant,symbol,stop_dt.isoformat(),int(stop_dt.timestamp()*1000),entry_price,
                 float(review["old_p_score"] or 0),float(review["new_p_score"] or 0),f"source={source_shadow_id}",
                 json.dumps(snap,ensure_ascii=False,default=str),tp2_price,be_price,float(stop_price),float(stop_price),float(stop_price),
                 datetime.now(timezone.utc).isoformat(),str(int(stop_dt.timestamp())//300),str(int(stop_dt.timestamp())//900),
                 stop_pct,stop_pct,str(review["setup_id"] or ""),0),
            )
        append_entry_record(
            symbol,"RESEARCH_P_V27_4_2_STOP_GHOST_ENTRY",ghost_variant,float(review["new_p_score"] or 0),float(stop_price),
            f"source_stop={stop_type}; stop_pct={stop_pct:.3f}%; actual_order=0",
            extra={"shadow_id":shadow_id,"research_variant":ghost_variant,"p_v2742_ghost_type":ghost_variant,
                   "p_v2742_source_shadow_id":source_shadow_id,
                   "p_v2742_filter_reasons":str(source_snap.get("p_v2742_filter_reasons") or ""),
                   "p_v2742_filter_version":str(source_snap.get("p_v2742_filter_version") or "")},
        )

    def _backfill_pv2742_stop_ghosts(self) -> None:
        if not self.cfg.research_pv2742_stop_ghost_enabled:
            return
        with db() as conn:
            rows = conn.execute(
                "SELECT * FROM research_shadow_reviews WHERE variant='P_V27_4_2' AND completed=1 AND result='STOP' ORDER BY result_ts"
            ).fetchall()
        for row in rows:
            try:
                details = json.loads(str(row["result_details"] or "{}"))
            except Exception:
                details = {}
            try:
                self._clone_pv2742_stop_ghost(
                    row, str(row["result_ts"] or utc_now()), float(row["result_price"] or 0), details, backfilled=True
                )
            except Exception as exc:
                append_entry_record(
                    str(row["symbol"] or ""), "RESEARCH_P_V27_4_2_STOP_GHOST_BACKFILL_ERROR",
                    "P_V27_4_2_STOP_GHOST", 0, float(row["result_price"] or 0), f"{type(exc).__name__}: {exc}"
                )

    def _pv2743_adjusted_filter_meta(self, details: dict[str, Any]) -> dict[str, Any]:
        """v4.3.79: V27.4.2 Forward 오차단을 줄이도록 범위를 좁힌 R2 고정 진입필터.

        설계 원칙
        - V25/V27.4.1 진입 골격은 변경하지 않는다.
        - 과거 A9에 포함됐던 9개 규칙(05/08/09/11/12/20/27/32/34)만 V27.4.3 내부에서 직접 계산한다.
        - A9-11은 기존 조건에 one_hour_signed_move_pct>=4.50을 추가한다.
        - A05/A12/A27, B25-v2, C3 범위를 오차단 감소 방향으로 좁힌다.
        - B25-v2 / C1-adjusted / C2-v2 / C3-adjusted / C5-adjusted를 OR로 차단한다.
        - C4/C6은 새 Forward에서 정상 TP/BE 오차단만 보여 hard block에서 제외한다.
        - 이 함수의 숫자는 새 Forward 검증 중 절대 자동튜닝하지 않는다.
        """
        def f(key: str) -> float | None:
            try:
                v = details.get(key)
                if v in (None, ""):
                    return None
                return float(v)
            except (TypeError, ValueError):
                return None

        def ge(v: float | None, x: float) -> bool: return v is not None and v >= x
        def le(v: float | None, x: float) -> bool: return v is not None and v <= x
        def between(v: float | None, lo: float, hi: float) -> bool: return v is not None and lo <= v <= hi

        p1=f("prev1_candle_gain_pct"); p2=f("prev2_candle_gain_pct"); p3=f("prev3_candle_gain_pct")
        rd1=f("rsi_change_prev1"); rd2=f("rsi_change_prev2"); rsi=f("rsi"); rsi_delta=f("rsi_delta")
        vol=f("volume_ratio"); lvol=f("live_volume_ratio"); lg=f("live_candle_gain_pct")
        es=f("ema20_slope_pct"); gap9_20=f("ema9_ema20_gap_pct"); gap20_60=f("ema20_ema60_gap_pct")
        pers=f("p_v21_persistence_score"); v2=f("p_v2_score"); reb=f("rebound_from_low_pct")
        pull=f("pullback_from_high_pct"); h1=f("one_hour_signed_move_pct"); ps=f("p_score")
        b15=f("btc_15m_change_pct"); e15=f("eth_15m_change_pct"); low1=f("low_change_prev1_pct")

        # A9 adjusted: 과거 A9의 9개 규칙만 직접 계산하며, 11/27 규칙은 새 Forward에서 확인된 범위/측정문제를 보정.
        a9_r05 = le(vol,0.35) and ge(gap9_20,2.0) and between(reb,8.0,15.0) and le(h1,2.0)
        a9_r08 = le(rd1,-1.0) and ge(reb,10.0) and le(h1,1.5)
        a9_r09 = le(p2,0.30) and ge(lvol,1.0) and le(e15,-0.10)
        a9_r11 = le(lvol,0.10) and le(reb,6.0) and ge(v2,85.0) and ge(vol,0.70) and ge(h1,4.50)
        a9_r12 = between(p1,-1.35,-1.0) and le(p3,1.0)
        a9_r20 = ge(p2,1.5) and ge(v2,95.0) and le(ps,100.0)
        a9_r27 = between(p1,-1.40,-0.90) and between(h1,1.0,1.5)
        a9_r32 = le(vol,0.50) and ge(v2,85.0) and le(e15,0.0)
        a9_r34 = ge(es,0.30) and le(pers,50.0) and le(reb,5.0)
        a9_flags = {
            "A05": a9_r05, "A08": a9_r08, "A09": a9_r09, "A11": a9_r11, "A12": a9_r12,
            "A20": a9_r20, "A27": a9_r27, "A32": a9_r32, "A34": a9_r34,
        }
        a9_ids = [k for k,v in a9_flags.items() if v]
        a9 = bool(a9_ids)

        # B25-v2 R2: prev1<=-0.70 + prev3<=0.00 + BTC15>=0.05 + p_v2_score>=70.
        b25 = le(p1,-0.70) and le(p3,0.00) and ge(b15,0.05) and ge(v2,70.0)

        # C1 adjusted: 기존 약한 중기구조에 score/live-volume/RSI 안정성 확인을 추가.
        c1 = (
            le(gap20_60,0.80) and le(reb,4.0) and ge(v2,60.0)
            and between(lvol,0.80,1.60) and ge(rd1,-3.0)
        )

        # C2-v2: 기존 C2의 보수형. C2 정의 혼선을 없애기 위해 p_v2_score>=60을 명시한다.
        c2 = le(vol,0.35) and le(reb,5.0) and ge(v2,60.0)

        # C3 R2: live gain 0.90~1.80%로 상한을 추가해 강한 정상 재가속 오차단을 줄인다.
        c3 = le(pull,-0.30) and between(lg,0.90,1.80) and le(gap20_60,0.80) and ge(pers,50.0)

        # C5 adjusted: 기존 RSI rollover형에 live gain 0.5~1.2% 범위를 추가.
        c5 = (
            ge(low1,1.50) and le(rd2,-3.30) and between(rsi,62.0,72.0)
            and ge(rsi_delta,0.0) and between(lg,0.50,1.20)
        )

        flags = {
            "A9": a9,
            "B25_V2": b25,
            "C1_ADJ": c1,
            "C2_V2": c2,
            "C3_ADJ": c3,
            "C5_ADJ": c5,
        }
        reasons = [k for k,v in flags.items() if v]
        blocked = bool(reasons)
        return {
            "p_v2743_filter_block": blocked,
            "p_v2743_filter_reasons": ",".join(reasons),
            "p_v2743_a9_match": a9,
            "p_v2743_a9_rule_ids": ",".join(a9_ids),
            "p_v2743_b25_v2": b25,
            "p_v2743_c1_adj": c1,
            "p_v2743_c2_v2": c2,
            "p_v2743_c3_adj": c3,
            "p_v2743_c5_adj": c5,
            "p_v2743_c4_enabled": False,
            "p_v2743_c6_enabled": False,
            "p_v2743_filter_version": "R2_A05R11R12R27_B25R2_C1_C2V2_C3R2_C5_V2",
            "p_v2743_filter_frozen": True,
        }


    def _register_pv2743_block_ghost(
        self, symbol: str, details: dict[str, Any], source_setup_id: str, entry_price: float,
        closed_5m_price: float, five_bullish: bool, high_break: bool, meta: dict[str, Any],
    ) -> None:
        """P_V27_4_3가 진입 차단한 거래를 V27.4.1과 같은 기존 공통관리로 끝까지 추적한다."""
        if not self.cfg.research_pv2743_block_ghost_enabled or entry_price <= 0:
            return
        now = datetime.now(timezone.utc)
        setup_id = str(source_setup_id).replace("P25SET-", "P2743BGSET-", 1)
        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_4_3_BLOCK_GHOST' AND setup_id=? LIMIT 1",
                (setup_id,),
            ).fetchone():
                return
            opened_at = now.isoformat()
            shadow_id = f"RSH-P_V27_4_3_BLOCK_GHOST-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = entry_price * (1 + float(self.cfg.tp2_pct) / 100)
            be_price = entry_price * (1 + float(self.cfg.breakeven_stop_pct) / 100)
            snap = dict(details)
            snap.update({
                **meta,
                "p_v2743_confirm_state": "BLOCKED_FILTER_GHOST",
                "p_v2743_live_entry_price": entry_price,
                "p_v2743_closed_5m_price": closed_5m_price,
                "p_v2743_is_block_ghost": True,
                "p_v25_setup_id": source_setup_id,
                "p_v25_5m_bullish": five_bullish,
                "p_v25_prev_high_break": high_break,
            })
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,missing_condition,snapshot_json,
                    tp2_price,be_price,highest_price,lowest_price,last_price,last_checked_at,last_5m_bucket,last_15m_bucket,
                    mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    shadow_id,"P_V27_4_3_BLOCK_GHOST",symbol,opened_at,int(now.timestamp()*1000),entry_price,
                    float(details.get("p_score") or 0),float(details.get("p_v2_score") or 0),
                    f"adjusted_filter_block:{meta.get('p_v2743_filter_reasons') or ''}",json.dumps(snap,ensure_ascii=False,default=str),
                    tp2_price,be_price,entry_price,entry_price,entry_price,opened_at,
                    str(int(now.timestamp())//300),str(int(now.timestamp())//900),0.0,0.0,setup_id,0,
                ),
            )
        append_entry_record(
            symbol,"RESEARCH_P_V27_4_3_BLOCK_GHOST_ENTRY","P_V27_4_3_BLOCK_GHOST",
            float(details.get("p_v2_score") or 0),entry_price,
            f"blocked_by={meta.get('p_v2743_filter_reasons') or 'UNKNOWN'}; baseline-management ghost; actual_order=0",
            extra={**details,**meta,"shadow_id":shadow_id,"research_variant":"P_V27_4_3_BLOCK_GHOST",
                   "shadow_entry_time":_kst_stamp(opened_at),"p_v2743_confirm_state":"BLOCKED_FILTER_GHOST",
                   "p_v2743_live_entry_price":entry_price,"p_v2743_closed_5m_price":closed_5m_price},
        )


    def _open_pv2743_confirmed_shadow(
        self, symbol: str, details: dict[str, Any], source_setup_id: str, entry_price: float,
        closed_5m_price: float, five_bullish: bool, high_break: bool,
    ) -> bool:
        """P_V27_4_3: V25 진입 + adjusted hard filter만 검증. 종료관리는 V27.4.1과 동일."""
        if not self.cfg.research_pv2743_enabled or entry_price <= 0:
            return False
        now = datetime.now(timezone.utc)
        setup_id = str(source_setup_id).replace("P25SET-", "P2743SET-", 1)
        meta = self._pv2743_adjusted_filter_meta(details)

        if bool(meta.get("p_v2743_filter_block")):
            append_entry_record(
                symbol,"RESEARCH_P_V27_4_3_BLOCKED_FILTER","P_V27_4_3",
                float(details.get("p_v2_score") or 0),entry_price,
                f"adjusted_filter={meta.get('p_v2743_filter_reasons') or 'UNKNOWN'}; actual_order=0",
                extra={**details,**meta,"p_v2743_confirm_state":"BLOCKED_FILTER",
                       "p_v2743_live_entry_price":entry_price,"p_v2743_closed_5m_price":closed_5m_price},
            )
            self._register_pv2743_block_ghost(
                symbol, details, source_setup_id, entry_price, closed_5m_price, five_bullish, high_break, meta
            )
            return False

        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_4_3' AND symbol=? AND completed=0 LIMIT 1",
                (symbol,),
            ).fetchone():
                return False

        live_cd, live_remaining, live_prior = self._research_live_cooldown_status("P_V27_4_3", symbol, now)
        if live_cd:
            append_entry_record(
                symbol,"RESEARCH_P_V27_4_3_BLOCKED_LIVE90","P_V27_4_3",
                float(details.get("p_v2_score") or 0),entry_price,
                f"LIVE90_COOLDOWN remaining={live_remaining:.1f}m",
                extra={**details,**meta,"p_v2743_confirm_state":"BLOCKED_LIVE90",
                       "p_v2743_live_entry_price":entry_price,"p_v2743_closed_5m_price":closed_5m_price,
                       "cooldown_remaining_min":round(live_remaining,1),"prior_result":live_prior},
            )
            return False
        cooldown, remaining, prior = self._variant_stop_cooldown_status("P_V27_4_3",symbol,now)
        if cooldown:
            append_entry_record(
                symbol,"RESEARCH_P_V27_4_3_BLOCKED_COOLDOWN","P_V27_4_3",
                float(details.get("p_v2_score") or 0),entry_price,
                f"STOP_COOLDOWN remaining={remaining:.1f}m",
                extra={**details,**meta,"p_v2743_confirm_state":"BLOCKED_COOLDOWN",
                       "p_v2743_live_entry_price":entry_price,"p_v2743_closed_5m_price":closed_5m_price,
                       "cooldown_remaining_min":round(remaining,1),"prior_failure_result":prior},
            )
            return False
        allowed, why = self._variant_can_open_now(
            "P_V27_4_3",now,self.cfg.research_pv2743_max_entries_per_15m,self.cfg.research_pv2743_max_open_positions
        )
        if not allowed:
            append_entry_record(
                symbol,"RESEARCH_P_V27_4_3_SKIPPED","P_V27_4_3",
                float(details.get("p_v2_score") or 0),entry_price,why,
                extra={**details,**meta,"p_v2743_confirm_state":why,
                       "p_v2743_live_entry_price":entry_price,"p_v2743_closed_5m_price":closed_5m_price},
            )
            return False
        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_4_3' AND setup_id=? LIMIT 1",(setup_id,)
            ).fetchone():
                return False
            opened_at = now.isoformat()
            shadow_id = f"RSH-P_V27_4_3-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = entry_price*(1+float(self.cfg.tp2_pct)/100)
            be_price = entry_price*(1+float(self.cfg.breakeven_stop_pct)/100)
            snap = dict(details)
            snap.update({
                **meta,
                "p_v2743_confirm_state":"CONFIRMED_FILTER_PASS",
                "p_v2743_live_entry_price":entry_price,
                "p_v2743_closed_5m_price":closed_5m_price,
                "p_v2743_is_block_ghost":False,
                "p_v25_setup_id":source_setup_id,"p_v25_5m_bullish":five_bullish,"p_v25_prev_high_break":high_break,
            })
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,missing_condition,snapshot_json,
                    tp2_price,be_price,highest_price,lowest_price,last_price,last_checked_at,last_5m_bucket,last_15m_bucket,
                    mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    shadow_id,"P_V27_4_3",symbol,opened_at,int(now.timestamp()*1000),entry_price,
                    float(details.get("p_score") or 0),float(details.get("p_v2_score") or 0),
                    "confirmed_5m_break_adjusted_filter_pass",json.dumps(snap,ensure_ascii=False,default=str),
                    tp2_price,be_price,entry_price,entry_price,entry_price,opened_at,
                    str(int(now.timestamp())//300),str(int(now.timestamp())//900),0.0,0.0,setup_id,0,
                ),
            )
        append_entry_record(
            symbol,"RESEARCH_P_V27_4_3_ENTRY","P_V27_4_3",float(details.get("p_v2_score") or 0),entry_price,
            f"V25 entry + adjusted filter PASS; baseline management; actual_order=0; filter={meta.get('p_v2743_filter_version')}",
            extra={**details,**meta,"shadow_id":shadow_id,"research_variant":"P_V27_4_3",
                   "shadow_entry_time":_kst_stamp(opened_at),"p_v2743_confirm_state":"CONFIRMED_FILTER_PASS",
                   "p_v2743_live_entry_price":entry_price,"p_v2743_closed_5m_price":closed_5m_price},
        )
        return True


    def _clone_pv2743_stop_ghost(
        self, review: sqlite3.Row, stop_ts: str, stop_price: float, result_details: dict[str, Any], *, backfilled: bool = False,
    ) -> None:
        """P_V27_4_3 STOP 뒤 180분 raw path 추적. 진입필터 검증용이며 V27-1과 무관하다."""
        if not self.cfg.research_pv2743_stop_ghost_enabled:
            return
        source_shadow_id = str(review["shadow_id"] or "")
        symbol = str(review["symbol"] or "")
        entry_price = float(review["entry_price"] or 0)
        if not source_shadow_id or not symbol or entry_price <= 0 or stop_price <= 0:
            return
        ghost_variant = "P_V27_4_3_STOP_GHOST"
        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant=? AND missing_condition=? LIMIT 1",
                (ghost_variant, f"source={source_shadow_id}"),
            ).fetchone():
                return
            try:
                stop_dt = datetime.fromisoformat(str(stop_ts))
                if stop_dt.tzinfo is None:
                    stop_dt = stop_dt.replace(tzinfo=timezone.utc)
            except Exception:
                stop_dt = datetime.now(timezone.utc)
            try:
                source_snap = json.loads(str(review["snapshot_json"] or "{}"))
            except Exception:
                source_snap = {}
            stop_pct = (float(stop_price) / entry_price - 1.0) * 100.0
            stop_type = str(result_details.get("stop_type") or "STOP")
            snap = dict(source_snap)
            snap.update({
                "p_v2743_confirm_state": "STOP_GHOST_TRACKING",
                "p_v2743_ghost_type": ghost_variant,
                "p_v2743_source_shadow_id": source_shadow_id,
                "source_stop_ts": str(stop_ts),
                "source_stop_price": float(stop_price),
                "source_stop_pct": round(stop_pct, 4),
                "source_stop_type": stop_type,
                "raw_path_milestones": {},
                "stop_ghost_backfilled": bool(backfilled),
            })
            shadow_id = f"RSH-{ghost_variant}-{stop_dt.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = entry_price * (1 + float(self.cfg.tp2_pct) / 100)
            be_price = entry_price * (1 + float(self.cfg.breakeven_stop_pct) / 100)
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,
                    missing_condition,snapshot_json,tp2_price,be_price,highest_price,lowest_price,last_price,
                    last_checked_at,last_5m_bucket,last_15m_bucket,mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (shadow_id,ghost_variant,symbol,stop_dt.isoformat(),int(stop_dt.timestamp()*1000),entry_price,
                 float(review["old_p_score"] or 0),float(review["new_p_score"] or 0),f"source={source_shadow_id}",
                 json.dumps(snap,ensure_ascii=False,default=str),tp2_price,be_price,float(stop_price),float(stop_price),float(stop_price),
                 datetime.now(timezone.utc).isoformat(),str(int(stop_dt.timestamp())//300),str(int(stop_dt.timestamp())//900),
                 stop_pct,stop_pct,str(review["setup_id"] or ""),0),
            )
        append_entry_record(
            symbol,"RESEARCH_P_V27_4_3_STOP_GHOST_ENTRY",ghost_variant,float(review["new_p_score"] or 0),float(stop_price),
            f"source_stop={stop_type}; stop_pct={stop_pct:.3f}%; actual_order=0",
            extra={"shadow_id":shadow_id,"research_variant":ghost_variant,"p_v2743_ghost_type":ghost_variant,
                   "p_v2743_source_shadow_id":source_shadow_id,
                   "p_v2743_filter_reasons":str(source_snap.get("p_v2743_filter_reasons") or ""),
                   "p_v2743_filter_version":str(source_snap.get("p_v2743_filter_version") or "")},
        )


    def _backfill_pv2743_stop_ghosts(self) -> None:
        if not self.cfg.research_pv2743_stop_ghost_enabled:
            return
        with db() as conn:
            rows = conn.execute(
                "SELECT * FROM research_shadow_reviews WHERE variant='P_V27_4_3' AND completed=1 AND result='STOP' ORDER BY result_ts"
            ).fetchall()
        for row in rows:
            try:
                details = json.loads(str(row["result_details"] or "{}"))
            except Exception:
                details = {}
            try:
                self._clone_pv2743_stop_ghost(
                    row, str(row["result_ts"] or utc_now()), float(row["result_price"] or 0), details, backfilled=True
                )
            except Exception as exc:
                append_entry_record(
                    str(row["symbol"] or ""), "RESEARCH_P_V27_4_3_STOP_GHOST_BACKFILL_ERROR",
                    "P_V27_4_3_STOP_GHOST", 0, float(row["result_price"] or 0), f"{type(exc).__name__}: {exc}"
                )



    def _pv2746_entry_risk_filter_meta(self, details: dict[str, Any]) -> dict[str, Any]:
        """v4.3.82: V25 진입기회를 보존하면서 실패진입만 줄이기 위한 신규 Forward Entry Risk Guard.

        고정 조건(자동튜닝 금지)
        A) ema_gap_delta_pct <= 0.156 AND rsi_delta >= 1.923 AND one_hour_signed_move_pct >= 0
           - 최신 POWR(BE)의 음수 1h 반등형 오차단을 피하기 위한 1h 양수 가드 포함.
        B) ema9_slope_pct <= 0.439 AND eth_15m_change_pct >= 0.104
        C3) 기존 4.3 C3 R2 그대로
        C5) 기존 4.3 C5 adjusted 그대로
        위 4개 중 하나라도 맞으면 4.6만 차단한다. 4.3/LIVE에는 영향 없음.
        """
        def f(key: str) -> float | None:
            try:
                v = details.get(key)
                if v in (None, ""):
                    return None
                return float(v)
            except (TypeError, ValueError):
                return None

        def ge(v: float | None, x: float) -> bool: return v is not None and v >= x
        def le(v: float | None, x: float) -> bool: return v is not None and v <= x
        def between(v: float | None, lo: float, hi: float) -> bool: return v is not None and lo <= v <= hi

        gap_delta = f("ema_gap_delta_pct")
        rsi_delta = f("rsi_delta")
        h1 = f("one_hour_signed_move_pct")
        ema9 = f("ema9_slope_pct")
        e15 = f("eth_15m_change_pct")
        pull = f("pullback_from_high_pct")
        lg = f("live_candle_gain_pct")
        gap20_60 = f("ema20_ema60_gap_pct")
        pers = f("p_v21_persistence_score")
        low1 = f("low_change_prev1_pct")
        rd2 = f("rsi_change_prev2")
        rsi = f("rsi")

        a_gap_rsi_h1 = le(gap_delta, 0.156) and ge(rsi_delta, 1.923) and ge(h1, 0.0)
        b_ema9_eth = le(ema9, 0.439) and ge(e15, 0.104)
        c3 = le(pull, -0.30) and between(lg, 0.90, 1.80) and le(gap20_60, 0.80) and ge(pers, 50.0)
        c5 = (
            ge(low1, 1.50) and le(rd2, -3.30) and between(rsi, 62.0, 72.0)
            and ge(rsi_delta, 0.0) and between(lg, 0.50, 1.20)
        )

        flags = {
            "A_GAP_RSI_H1POS": a_gap_rsi_h1,
            "B_EMA9_ETH15": b_ema9_eth,
            "C3_ADJ": c3,
            "C5_ADJ": c5,
        }
        reasons = [k for k, v in flags.items() if v]
        return {
            "p_v2746_filter_block": bool(reasons),
            "p_v2746_filter_reasons": ",".join(reasons),
            "p_v2746_a_gap_rsi_h1": a_gap_rsi_h1,
            "p_v2746_b_ema9_eth": b_ema9_eth,
            "p_v2746_c3_adj": c3,
            "p_v2746_c5_adj": c5,
            "p_v2746_filter_version": "V1_AGap156_Rsi1923_H1Pos_BEMA9_439_ETH15_104_C3R2_C5",
            "p_v2746_filter_frozen": True,
        }

    def _final_safe_filter_meta(self, details: dict[str, Any]) -> dict[str, Any]:
        """v4.3.86 frozen SAFE entry filter from 9/1~9/17 replay (C OR RN OR RS)."""
        def fv(key: str) -> float | None:
            try:
                v = details.get(key)
                return None if v in (None, "") else float(v)
            except (TypeError, ValueError):
                return None

        rsi = fv("rsi")
        ema20_prev2 = fv("ema20_slope_prev2_pct")
        ema9_prev1 = fv("ema9_slope_prev1_pct")
        pull = fv("pullback_from_high_pct")
        rsi_chg1 = fv("rsi_change_prev1")

        c = bool(rsi is not None and ema20_prev2 is not None and rsi <= 62.46 and ema20_prev2 >= 0.0815)
        rn = bool(ema9_prev1 is not None and pull is not None and ema9_prev1 >= 0.39405 and pull <= -0.472)
        rs = bool(rsi is not None and rsi_chg1 is not None and rsi >= 66.005 and rsi_chg1 <= -1.9521)
        flags = [name for name, ok in (("C", c), ("RN", rn), ("RS", rs)) if ok]
        return {
            "final_safe_block": bool(flags),
            "final_safe_flags": ",".join(flags),
            "final_safe_c": c,
            "final_safe_rn": rn,
            "final_safe_rs": rs,
            "final_safe_version": "SAFE_C_RN_RS_20260918",
        }

    def _fixed_micro_market_meta(self, details: dict[str, Any]) -> dict[str, Any]:
        """v4.3.88: SAFE RELAX에서 절대 풀지 않을 미세시장 위험형.

        연구 고정 기준: BTC/ETH 15m 평균 <= -0.05% AND 종목 EMA9-20 gap <= 1.20%.
        SAFE 자체를 새로 켜는 조건이 아니라, SAFE가 이미 BLOCK한 거래를 RELAX할 때의 금지조건이다.
        """
        try:
            btc15 = float(details.get("btc_15m_change_pct"))
            eth15 = float(details.get("eth_15m_change_pct"))
            gap = float(details.get("ema9_ema20_gap_pct"))
        except (TypeError, ValueError):
            return {
                "fixed_safe_micro_risk": False,
                "fixed_safe_micro_available": False,
                "fixed_safe_micro_reason": "missing_telemetry",
            }
        avg15 = (btc15 + eth15) / 2.0
        active = bool(
            avg15 <= float(self.cfg.research_fixed_micro_market_avg15_max_pct)
            and gap <= float(self.cfg.research_fixed_micro_market_ema_gap_max_pct)
        )
        return {
            "fixed_safe_micro_risk": active,
            "fixed_safe_micro_available": True,
            "fixed_safe_micro_reason": "BTCETH15_WEAK_AND_GAP_SMALL" if active else "NORMAL",
            "fixed_safe_btc15_pct": round(btc15, 4),
            "fixed_safe_eth15_pct": round(eth15, 4),
            "fixed_safe_btceth15_avg_pct": round(avg15, 4),
            "fixed_safe_ema9_20_gap_pct": round(gap, 4),
        }

    def _fixed_safe_relax_regime_meta(self, now: datetime | None = None) -> dict[str, Any]:
        """v4.3.88: 최근 CONTROL 성과로 SAFE 비위험 차단군의 '역전 레짐'만 감지한다.

        14h와 18h 두 창 모두에서 비위험 SAFE 차단군의 양(+) 종료 비율이
        SAFE PASS군보다 +8%p 이상 높고, 양쪽 표본이 최소수 이상일 때만 RELAX ON.
        결과는 research-only P_FWD_CONTROL의 완료거래만 사용하므로 LIVE 주문과 무관하다.
        """
        now = now or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        short_h = float(self.cfg.research_fixed_safe_relax_window_short_hours)
        long_h = float(self.cfg.research_fixed_safe_relax_window_long_hours)
        edge = float(self.cfg.research_fixed_safe_relax_edge_pctp)
        min_safe = max(1, int(self.cfg.research_fixed_safe_relax_min_safe_n))
        min_pass = max(1, int(self.cfg.research_fixed_safe_relax_min_pass_n))
        cutoff = now - timedelta(hours=max(short_h, long_h))

        with db() as conn:
            rows = conn.execute(
                """SELECT result_ts,entry_price,result_price,snapshot_json
                   FROM research_shadow_reviews
                   WHERE variant='P_FWD_CONTROL' AND completed=1 AND result_ts>=?
                   ORDER BY result_ts""",
                (cutoff.isoformat(),),
            ).fetchall()

        obs: list[tuple[datetime, bool, bool, bool]] = []
        for row in rows:
            try:
                ts = datetime.fromisoformat(str(row["result_ts"] or ""))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                snap = json.loads(str(row["snapshot_json"] or "{}"))
                safe_block = bool(snap.get("final_safe_block"))
                micro_meta = self._fixed_micro_market_meta(snap)
                micro = bool(micro_meta.get("fixed_safe_micro_risk"))
                micro_available = bool(micro_meta.get("fixed_safe_micro_available"))
                entry = float(row["entry_price"] or 0)
                result_price = float(row["result_price"] or 0)
                positive = bool(entry > 0 and result_price > entry)
                # SAFE 차단군은 micro telemetry가 확인되는 경우에만 비위험/위험을 분류한다.
                # 과거 telemetry 누락을 비위험으로 오인해 RELAX가 켜지는 일을 막는다.
                if safe_block and not micro_available:
                    continue
                obs.append((ts, safe_block, micro, positive))
            except Exception:
                continue

        def _window(hours: float) -> dict[str, Any]:
            cut = now - timedelta(hours=hours)
            safe_vals: list[bool] = []
            pass_vals: list[bool] = []
            for ts, safe_block, micro, positive in obs:
                if ts < cut:
                    continue
                if safe_block:
                    if not micro:
                        safe_vals.append(positive)
                else:
                    pass_vals.append(positive)
            safe_n = len(safe_vals); pass_n = len(pass_vals)
            safe_rate = (100.0 * sum(safe_vals) / safe_n) if safe_n else None
            pass_rate = (100.0 * sum(pass_vals) / pass_n) if pass_n else None
            diff = (safe_rate - pass_rate) if safe_rate is not None and pass_rate is not None else None
            enough = bool(safe_n >= min_safe and pass_n >= min_pass)
            on = bool(enough and diff is not None and diff >= edge)
            return {
                "safe_n": safe_n, "pass_n": pass_n,
                "safe_rate": safe_rate, "pass_rate": pass_rate,
                "diff": diff, "enough": enough, "on": on,
            }

        w14 = _window(short_h)
        w18 = _window(long_h)
        active = bool(w14["on"] and w18["on"])
        return {
            "fixed_safe_regime_relax_on": active,
            "fixed_safe_regime_edge_pctp": edge,
            "fixed_safe_regime_min_safe_n": min_safe,
            "fixed_safe_regime_min_pass_n": min_pass,
            "fixed_safe_14h_safe_n": w14["safe_n"],
            "fixed_safe_14h_safe_pos_pct": (round(w14["safe_rate"], 3) if w14["safe_rate"] is not None else None),
            "fixed_safe_14h_pass_n": w14["pass_n"],
            "fixed_safe_14h_pass_pos_pct": (round(w14["pass_rate"], 3) if w14["pass_rate"] is not None else None),
            "fixed_safe_14h_diff_pctp": (round(w14["diff"], 3) if w14["diff"] is not None else None),
            "fixed_safe_18h_safe_n": w18["safe_n"],
            "fixed_safe_18h_safe_pos_pct": (round(w18["safe_rate"], 3) if w18["safe_rate"] is not None else None),
            "fixed_safe_18h_pass_n": w18["pass_n"],
            "fixed_safe_18h_pass_pos_pct": (round(w18["pass_rate"], 3) if w18["pass_rate"] is not None else None),
            "fixed_safe_18h_diff_pctp": (round(w18["diff"], 3) if w18["diff"] is not None else None),
            "fixed_safe_regime_version": "CONTROL_POS_14H18H_EDGE8_V1",
        }

    def _final_market_guard_meta(self, details: dict[str, Any]) -> dict[str, Any]:
        """BTC/ETH 4h가 둘 다 ±0.08% 안쪽인 flat regime guard."""
        try:
            btc4 = float(details.get("btc_4h_change_pct"))
            eth4 = float(details.get("eth_4h_change_pct"))
        except (TypeError, ValueError):
            return {
                "final_market_guard": False,
                "final_market_guard_available": False,
                "final_market_guard_reason": "missing_4h_telemetry",
            }
        th = abs(float(self.cfg.research_final_market_flat_abs_4h_pct))
        active = bool(abs(btc4) <= th and abs(eth4) <= th)
        return {
            "final_market_guard": active,
            "final_market_guard_available": True,
            "final_market_guard_reason": "BTC_ETH_4H_FLAT" if active else "NORMAL",
            "final_market_btc4h_pct": round(btc4, 4),
            "final_market_eth4h_pct": round(eth4, 4),
            "final_market_threshold_abs_pct": th,
        }

    def _open_final_forward_variant(
        self, *, variant: str, symbol: str, details: dict[str, Any], source_setup_id: str,
        entry_price: float, closed_5m_price: float, five_bullish: bool, high_break: bool,
        market_mode: str, size_mult: float, safe_meta: dict[str, Any], market_meta: dict[str, Any],
    ) -> bool:
        """Final Forward 한 경로를 독립 portfolio schedule로 연다 (research-only)."""
        if entry_price <= 0:
            return False
        now = datetime.now(timezone.utc)
        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant=? AND symbol=? AND completed=0 LIMIT 1",
                (variant, symbol),
            ).fetchone():
                return False
        live_cd, live_remaining, _ = self._research_live_cooldown_status(variant, symbol, now)
        if live_cd:
            append_entry_record(
                symbol, f"RESEARCH_{variant}_SKIPPED", variant, float(details.get("p_v2_score") or 0), entry_price,
                f"LIVE90_COOLDOWN remaining={live_remaining:.1f}m",
                extra={**details, **safe_meta, **market_meta, "final_fwd_variant": variant,
                       "final_market_mode": market_mode, "final_size_mult": size_mult},
            )
            return False
        stop_cd, stop_remaining, _ = self._variant_stop_cooldown_status(variant, symbol, now)
        if stop_cd:
            append_entry_record(
                symbol, f"RESEARCH_{variant}_SKIPPED", variant, float(details.get("p_v2_score") or 0), entry_price,
                f"STOP180_COOLDOWN remaining={stop_remaining:.1f}m",
                extra={**details, **safe_meta, **market_meta, "final_fwd_variant": variant,
                       "final_market_mode": market_mode, "final_size_mult": size_mult},
            )
            return False
        allowed, why = self._variant_can_open_now(
            variant, now, self.cfg.research_final_max_entries_per_15m, self.cfg.research_final_max_open_positions
        )
        if not allowed:
            append_entry_record(
                symbol, f"RESEARCH_{variant}_SKIPPED", variant, float(details.get("p_v2_score") or 0), entry_price, why,
                extra={**details, **safe_meta, **market_meta, "final_fwd_variant": variant,
                       "final_market_mode": market_mode, "final_size_mult": size_mult},
            )
            return False

        setup_id = f"{source_setup_id}|{variant}"
        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant=? AND setup_id=? LIMIT 1", (variant, setup_id)
            ).fetchone():
                return False
            opened_at = now.isoformat()
            shadow_id = f"RSH-{variant}-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            is_control = variant == "P_FWD_CONTROL"
            target_pct = float(self.cfg.tp2_pct) if is_control else float(self.cfg.research_final_tp_pct)
            tp2_price = entry_price * (1 + target_pct / 100.0)
            be_price = entry_price * (1 + float(self.cfg.breakeven_stop_pct) / 100.0)
            snap = dict(details)
            snap.update({
                **safe_meta, **market_meta,
                "final_fwd_variant": variant,
                "final_market_mode": market_mode,
                "final_size_mult": float(size_mult),
                "final_source_setup_id": source_setup_id,
                "final_tp_pct": (None if is_control else float(self.cfg.research_final_tp_pct)),
                "final_remaining_frac": 1.0,
                "final_realized_weighted_pct": 0.0,
                "final_stop_stage": "NONE",
                "final_checkpoint10_done": False,
                "final_checkpoint15_done": False,
                "final_checkpoint25_done": False,
                "final_checkpoint30_done": False,
                "fixed_pp_armed": False,
                "fixed_pp_arm_mfe_pct": None,
                "fixed_pp_last_close_pct": None,
                "fixed_pp_triggered": False,
                "p_v25_5m_bullish": bool(five_bullish),
                "p_v25_prev_high_break": bool(high_break),
                "p_v25_closed_5m_price": float(closed_5m_price),
            })
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,
                    missing_condition,snapshot_json,tp2_price,be_price,highest_price,lowest_price,last_price,
                    last_checked_at,last_5m_bucket,last_15m_bucket,mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    shadow_id, variant, symbol, opened_at, int(now.timestamp()*1000), entry_price,
                    float(details.get("p_score") or 0), float(details.get("p_v2_score") or 0),
                    "FINAL_FORWARD", json.dumps(snap, ensure_ascii=False, default=str),
                    tp2_price, be_price, entry_price, entry_price, entry_price, opened_at,
                    str(int(now.timestamp())//300), str(int(now.timestamp())//900), 0.0, 0.0, setup_id, 0,
                ),
            )
        append_entry_record(
            symbol, f"RESEARCH_{variant}_ENTRY", variant, float(details.get("p_v2_score") or 0), entry_price,
            f"FinalForward mode={market_mode}; size={size_mult:.2f}; actual_order=0",
            extra={**details, **safe_meta, **market_meta, "shadow_id": shadow_id,
                   "research_variant": variant, "shadow_entry_time": _kst_stamp(opened_at),
                   "final_fwd_variant": variant, "final_market_mode": market_mode,
                   "final_size_mult": float(size_mult), "final_remaining_frac": 1.0,
                   "final_realized_weighted_pct": 0.0, "final_stop_stage": "NONE"},
        )
        return True

    def _open_final_forward_bundle(
        self, symbol: str, details: dict[str, Any], source_setup_id: str, entry_price: float,
        closed_5m_price: float, five_bullish: bool, high_break: bool,
    ) -> None:
        """v4.3.86: CONTROL / CORE / MKT50 / MKT100 final forward bundle."""
        if not self.cfg.research_final_forward_enabled or entry_price <= 0:
            return
        safe_meta = self._final_safe_filter_meta(details)
        market_meta = self._final_market_guard_meta(details)

        # 1) CONTROL: V25 confirmed + 보호장치 + 기존 TP + V27-1 stop.
        self._open_final_forward_variant(
            variant="P_FWD_CONTROL", symbol=symbol, details=details, source_setup_id=source_setup_id,
            entry_price=entry_price, closed_5m_price=closed_5m_price, five_bullish=five_bullish, high_break=high_break,
            market_mode="CONTROL", size_mult=1.0, safe_meta=safe_meta, market_meta=market_meta,
        )

        # v4.3.88 FIXED_SHADOW: 오늘 확정한 수정안을 별도 research-only 경로로 고정한다.
        # 기존 CONTROL/CORE/MKT50/MKT100/LIVE에는 어떤 영향도 주지 않는다.
        if bool(self.cfg.research_fixed_shadow_enabled):
            fixed_micro = self._fixed_micro_market_meta(details)
            fixed_regime = self._fixed_safe_relax_regime_meta(datetime.now(timezone.utc))
            original_safe_block = bool(safe_meta.get("final_safe_block"))
            # 미세시장 위험형은 SAFE 역전레짐이어도 절대 RELAX하지 않는다.
            relaxed = bool(
                original_safe_block
                and bool(fixed_regime.get("fixed_safe_regime_relax_on"))
                and bool(fixed_micro.get("fixed_safe_micro_available"))
                and not bool(fixed_micro.get("fixed_safe_micro_risk"))
            )
            effective_safe_block = bool(original_safe_block and not relaxed)
            fixed_meta = {
                **fixed_micro, **fixed_regime,
                "fixed_safe_relaxed": relaxed,
                "fixed_safe_effective_block": effective_safe_block,
            }
            fixed_safe_meta = {**safe_meta, **fixed_meta}
            guard = bool(market_meta.get("final_market_guard"))
            if effective_safe_block:
                append_entry_record(
                    symbol, "RESEARCH_P_FWD_FIXED_SHADOW_BLOCKED_SAFE", "P_FWD_FIXED_SHADOW",
                    float(details.get("p_v2_score") or 0), entry_price,
                    f"FIXED SAFE block={safe_meta.get('final_safe_flags') or 'UNKNOWN'}; relax={int(relaxed)}; actual_order=0",
                    extra={**details, **fixed_safe_meta, **market_meta,
                           "final_fwd_variant":"P_FWD_FIXED_SHADOW", "final_source_setup_id":source_setup_id,
                           "final_market_mode":"MKT100", "final_size_mult":0.0},
                )
            elif guard:
                append_entry_record(
                    symbol, "RESEARCH_P_FWD_FIXED_SHADOW_BLOCKED_MARKET", "P_FWD_FIXED_SHADOW",
                    float(details.get("p_v2_score") or 0), entry_price,
                    "FIXED MKT100 BTC/ETH 4h flat guard => entry OFF; actual_order=0",
                    extra={**details, **fixed_safe_meta, **market_meta,
                           "final_fwd_variant":"P_FWD_FIXED_SHADOW", "final_source_setup_id":source_setup_id,
                           "final_market_mode":"MKT100", "final_size_mult":0.0},
                )
            else:
                self._open_final_forward_variant(
                    variant="P_FWD_FIXED_SHADOW", symbol=symbol, details=details, source_setup_id=source_setup_id,
                    entry_price=entry_price, closed_5m_price=closed_5m_price,
                    five_bullish=five_bullish, high_break=high_break,
                    market_mode="MKT100", size_mult=1.0,
                    safe_meta=fixed_safe_meta, market_meta=market_meta,
                )

        # 기존 NEW 3계열은 SAFE가 ENTRY를 차단하면 실제 진입하지 않는다.
        if bool(safe_meta.get("final_safe_block")):
            for variant, mode in (("P_FWD_CORE","NONE"),("P_FWD_MKT50","MKT50"),("P_FWD_MKT100","MKT100")):
                append_entry_record(
                    symbol, f"RESEARCH_{variant}_BLOCKED_SAFE", variant, float(details.get("p_v2_score") or 0), entry_price,
                    f"SAFE block={safe_meta.get('final_safe_flags') or 'UNKNOWN'}; actual_order=0",
                    extra={**details, **safe_meta, **market_meta, "final_fwd_variant":variant, "final_source_setup_id":source_setup_id,
                           "final_market_mode":mode, "final_size_mult":0.0},
                )
            return

        # 2) CORE: SAFE + TP1.8 + new stop, market guard 없음.
        self._open_final_forward_variant(
            variant="P_FWD_CORE", symbol=symbol, details=details, source_setup_id=source_setup_id,
            entry_price=entry_price, closed_5m_price=closed_5m_price, five_bullish=five_bullish, high_break=high_break,
            market_mode="NONE", size_mult=1.0, safe_meta=safe_meta, market_meta=market_meta,
        )

        guard = bool(market_meta.get("final_market_guard"))
        # 3) MKT50: guard일 때만 50% size.
        self._open_final_forward_variant(
            variant="P_FWD_MKT50", symbol=symbol, details=details, source_setup_id=source_setup_id,
            entry_price=entry_price, closed_5m_price=closed_5m_price, five_bullish=five_bullish, high_break=high_break,
            market_mode="MKT50", size_mult=(0.5 if guard else 1.0), safe_meta=safe_meta, market_meta=market_meta,
        )

        # 4) MKT100: guard면 진짜 신규진입 OFF. 슬롯/쿨다운도 소비하지 않는다.
        if guard:
            append_entry_record(
                symbol, "RESEARCH_P_FWD_MKT100_BLOCKED_MARKET", "P_FWD_MKT100",
                float(details.get("p_v2_score") or 0), entry_price,
                "BTC/ETH 4h flat guard => entry OFF; actual_order=0",
                extra={**details, **safe_meta, **market_meta, "final_fwd_variant":"P_FWD_MKT100", "final_source_setup_id":source_setup_id,
                       "final_market_mode":"MKT100", "final_size_mult":0.0},
            )
        else:
            self._open_final_forward_variant(
                variant="P_FWD_MKT100", symbol=symbol, details=details, source_setup_id=source_setup_id,
                entry_price=entry_price, closed_5m_price=closed_5m_price, five_bullish=five_bullish, high_break=high_break,
                market_mode="MKT100", size_mult=1.0, safe_meta=safe_meta, market_meta=market_meta,
            )

    def _parallel_insert_shadow(
        self,
        *,
        variant: str,
        group: str,
        rule: str,
        symbol: str,
        details: dict[str, Any],
        source_setup_id: str,
        entry_price: float,
        closed_5m_price: float,
        five_bullish: bool,
        high_break: bool,
        extra_snap: dict[str, Any] | None = None,
    ) -> bool:
        """v4.3.83: V25 confirmed opportunity를 병렬 연구 Shadow 한 경로로 복제한다."""
        if entry_price <= 0:
            return False
        now = datetime.now(timezone.utc)
        setup_id = f"{source_setup_id}|{variant}"
        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant=? AND setup_id=? LIMIT 1",
                (variant, setup_id),
            ).fetchone():
                return False
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant=? AND symbol=? AND completed=0 LIMIT 1",
                (variant, symbol),
            ).fetchone():
                return False

            opened_at = now.isoformat()
            shadow_id = f"RSH-{variant}-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = entry_price * (1 + float(self.cfg.tp2_pct) / 100)
            be_price = entry_price * (1 + float(self.cfg.breakeven_stop_pct) / 100)
            snap = dict(details)
            snap.update({
                "parallel_lab_group": group,
                "parallel_lab_rule": rule,
                "parallel_lab_source_setup_id": source_setup_id,
                "parallel_lab_master_variant": "P_STOP_CONTROL" if group == "STOP" else variant,
                "p_v25_setup_id": source_setup_id,
                "p_v25_5m_bullish": bool(five_bullish),
                "p_v25_prev_high_break": bool(high_break),
                "p_v25_closed_5m_price": closed_5m_price,
                "parallel_live_entry_price": entry_price,
            })
            if extra_snap:
                snap.update(extra_snap)

            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,missing_condition,snapshot_json,
                    tp2_price,be_price,highest_price,lowest_price,last_price,last_checked_at,last_5m_bucket,last_15m_bucket,
                    mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    shadow_id,variant,symbol,opened_at,int(now.timestamp()*1000),entry_price,
                    float(details.get("p_score") or 0),float(details.get("p_v2_score") or 0),
                    f"parallel_{group.lower()}_{rule}",json.dumps(snap,ensure_ascii=False,default=str),
                    tp2_price,be_price,entry_price,entry_price,entry_price,opened_at,
                    str(int(now.timestamp())//300),str(int(now.timestamp())//900),0.0,0.0,setup_id,0,
                ),
            )
        append_entry_record(
            symbol,f"RESEARCH_{variant}_ENTRY",variant,float(details.get("p_v2_score") or 0),entry_price,
            f"parallel {group} rule={rule}; V25 master confirmed; actual_order=0",
            extra={
                **details,
                "shadow_id":shadow_id,"research_variant":variant,"shadow_entry_time":_kst_stamp(opened_at),
                "parallel_lab_group":group,"parallel_lab_rule":rule,
                "parallel_lab_source_setup_id":source_setup_id,
                "parallel_lab_master_variant":"P_STOP_CONTROL" if group=="STOP" else variant,
            },
        )
        return True

    def _open_parallel_stop_bundle(
        self, symbol: str, details: dict[str, Any], source_setup_id: str, entry_price: float,
        closed_5m_price: float, five_bullish: bool, high_break: bool,
    ) -> bool:
        """손절 5경로는 P_STOP_CONTROL의 동일 V25 portfolio schedule을 공유한다."""
        if not self.cfg.research_parallel_lab_enabled or entry_price <= 0:
            return False
        now = datetime.now(timezone.utc)
        master = "P_STOP_CONTROL"
        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant=? AND symbol=? AND completed=0 LIMIT 1",
                (master, symbol),
            ).fetchone():
                return False

        live_cd, live_remaining, live_prior = self._research_live_cooldown_status(master, symbol, now)
        if live_cd:
            append_entry_record(
                symbol,"RESEARCH_P_STOP_BUNDLE_SKIPPED",master,float(details.get("p_v2_score") or 0),entry_price,
                f"LIVE90_COOLDOWN remaining={live_remaining:.1f}m",
                extra={**details,"parallel_lab_group":"STOP","parallel_lab_rule":"BUNDLE",
                       "parallel_lab_source_setup_id":source_setup_id,"parallel_lab_master_variant":master},
            )
            return False
        stop_cd, remaining, prior = self._variant_stop_cooldown_status(master, symbol, now)
        if stop_cd:
            append_entry_record(
                symbol,"RESEARCH_P_STOP_BUNDLE_SKIPPED",master,float(details.get("p_v2_score") or 0),entry_price,
                f"STOP180_COOLDOWN remaining={remaining:.1f}m",
                extra={**details,"parallel_lab_group":"STOP","parallel_lab_rule":"BUNDLE",
                       "parallel_lab_source_setup_id":source_setup_id,"parallel_lab_master_variant":master},
            )
            return False
        allowed, why = self._variant_can_open_now(
            master, now, self.cfg.research_parallel_max_entries_per_15m, self.cfg.research_parallel_max_open_positions
        )
        if not allowed:
            append_entry_record(
                symbol,"RESEARCH_P_STOP_BUNDLE_SKIPPED",master,float(details.get("p_v2_score") or 0),entry_price,why,
                extra={**details,"parallel_lab_group":"STOP","parallel_lab_rule":"BUNDLE",
                       "parallel_lab_source_setup_id":source_setup_id,"parallel_lab_master_variant":master},
            )
            return False

        variants = (
            ("P_STOP_CONTROL","CONTROL"),
            ("P_STOP_EARLY50","EARLY50"),
            ("P_STOP_DISASTER50","DISASTER50"),
            ("P_STOP_LATE50","LATE50"),
            ("P_STOP_COMBO","COMBO"),
        )
        opened_any = False
        for variant, rule in variants:
            opened_any = self._parallel_insert_shadow(
                variant=variant,group="STOP",rule=rule,symbol=symbol,details=details,
                source_setup_id=source_setup_id,entry_price=entry_price,closed_5m_price=closed_5m_price,
                five_bullish=five_bullish,high_break=high_break,
            ) or opened_any
        return opened_any

    def _open_parallel_entry_bundle(
        self, symbol: str, details: dict[str, Any], source_setup_id: str, entry_price: float,
        closed_5m_price: float, five_bullish: bool, high_break: bool,
    ) -> None:
        """진입 7경로: V25 control / A / B / C3 / C5 / B+C3+C5 / current4.6."""
        if not self.cfg.research_parallel_lab_enabled or entry_price <= 0:
            return
        now = datetime.now(timezone.utc)
        meta = self._pv2746_entry_risk_filter_meta(details)
        a = bool(meta.get("p_v2746_a_gap_rsi_h1"))
        b = bool(meta.get("p_v2746_b_ema9_eth"))
        c3 = bool(meta.get("p_v2746_c3_adj"))
        c5 = bool(meta.get("p_v2746_c5_adj"))
        flag_text = ",".join(k for k,v in (
            ("A",a),("B",b),("C3",c3),("C5",c5)
        ) if v)

        specs = (
            ("P_ENTRY_CONTROL","CONTROL",False),
            ("P_ENTRY_A","A",a),
            ("P_ENTRY_B","B",b),
            ("P_ENTRY_C3","C3",c3),
            ("P_ENTRY_C5","C5",c5),
            ("P_ENTRY_COMBO","B+C3+C5",bool(b or c3 or c5)),
            ("P_ENTRY_46","A+B+C3+C5",bool(a or b or c3 or c5)),
        )
        for variant, rule, blocked in specs:
            if blocked:
                append_entry_record(
                    symbol,f"RESEARCH_{variant}_BLOCKED_FILTER",variant,float(details.get("p_v2_score") or 0),entry_price,
                    f"parallel entry block={rule}; matched={flag_text or 'NONE'}; actual_order=0",
                    extra={
                        **details,**meta,"parallel_lab_group":"ENTRY","parallel_lab_rule":rule,
                        "parallel_lab_filter_block":True,"parallel_lab_filter_flags":flag_text,
                        "parallel_lab_source_setup_id":source_setup_id,"parallel_lab_master_variant":variant,
                    },
                )
                continue

            with db() as conn:
                if conn.execute(
                    "SELECT 1 FROM research_shadow_reviews WHERE variant=? AND symbol=? AND completed=0 LIMIT 1",
                    (variant, symbol),
                ).fetchone():
                    continue
            live_cd, live_remaining, live_prior = self._research_live_cooldown_status(variant, symbol, now)
            if live_cd:
                append_entry_record(
                    symbol,f"RESEARCH_{variant}_SKIPPED",variant,float(details.get("p_v2_score") or 0),entry_price,
                    f"LIVE90_COOLDOWN remaining={live_remaining:.1f}m",
                    extra={**details,**meta,"parallel_lab_group":"ENTRY","parallel_lab_rule":rule,
                           "parallel_lab_filter_block":False,"parallel_lab_filter_flags":flag_text,
                           "parallel_lab_source_setup_id":source_setup_id,"parallel_lab_master_variant":variant},
                )
                continue
            stop_cd, remaining, prior = self._variant_stop_cooldown_status(variant, symbol, now)
            if stop_cd:
                append_entry_record(
                    symbol,f"RESEARCH_{variant}_SKIPPED",variant,float(details.get("p_v2_score") or 0),entry_price,
                    f"STOP180_COOLDOWN remaining={remaining:.1f}m",
                    extra={**details,**meta,"parallel_lab_group":"ENTRY","parallel_lab_rule":rule,
                           "parallel_lab_filter_block":False,"parallel_lab_filter_flags":flag_text,
                           "parallel_lab_source_setup_id":source_setup_id,"parallel_lab_master_variant":variant},
                )
                continue
            allowed, why = self._variant_can_open_now(
                variant, now, self.cfg.research_parallel_max_entries_per_15m, self.cfg.research_parallel_max_open_positions
            )
            if not allowed:
                append_entry_record(
                    symbol,f"RESEARCH_{variant}_SKIPPED",variant,float(details.get("p_v2_score") or 0),entry_price,why,
                    extra={**details,**meta,"parallel_lab_group":"ENTRY","parallel_lab_rule":rule,
                           "parallel_lab_filter_block":False,"parallel_lab_filter_flags":flag_text,
                           "parallel_lab_source_setup_id":source_setup_id,"parallel_lab_master_variant":variant},
                )
                continue

            self._parallel_insert_shadow(
                variant=variant,group="ENTRY",rule=rule,symbol=symbol,details=details,
                source_setup_id=source_setup_id,entry_price=entry_price,closed_5m_price=closed_5m_price,
                five_bullish=five_bullish,high_break=high_break,
                extra_snap={
                    **meta,"parallel_lab_filter_block":False,"parallel_lab_filter_flags":flag_text,
                },
            )

    def _register_pv2746_block_ghost(
        self, symbol: str, details: dict[str, Any], source_setup_id: str, entry_price: float,
        closed_5m_price: float, five_bullish: bool, high_break: bool, meta: dict[str, Any],
    ) -> None:
        """P_V27_4_6가 차단한 V25 진입기회를 기존 공통관리로 끝까지 추적한다."""
        if not self.cfg.research_pv2746_block_ghost_enabled or entry_price <= 0:
            return
        now = datetime.now(timezone.utc)
        setup_id = str(source_setup_id).replace("P25SET-", "P2746BGSET-", 1)
        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_4_6_BLOCK_GHOST' AND setup_id=? LIMIT 1",
                (setup_id,),
            ).fetchone():
                return
            opened_at = now.isoformat()
            shadow_id = f"RSH-P_V27_4_6_BLOCK_GHOST-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = entry_price * (1 + float(self.cfg.tp2_pct) / 100)
            be_price = entry_price * (1 + float(self.cfg.breakeven_stop_pct) / 100)
            snap = dict(details)
            snap.update({
                **meta,
                "p_v2746_confirm_state": "BLOCKED_FILTER_GHOST",
                "p_v2746_live_entry_price": entry_price,
                "p_v2746_closed_5m_price": closed_5m_price,
                "p_v2746_is_block_ghost": True,
                "p_v25_setup_id": source_setup_id,
                "p_v25_5m_bullish": five_bullish,
                "p_v25_prev_high_break": high_break,
            })
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,missing_condition,snapshot_json,
                    tp2_price,be_price,highest_price,lowest_price,last_price,last_checked_at,last_5m_bucket,last_15m_bucket,
                    mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    shadow_id,"P_V27_4_6_BLOCK_GHOST",symbol,opened_at,int(now.timestamp()*1000),entry_price,
                    float(details.get("p_score") or 0),float(details.get("p_v2_score") or 0),
                    f"entry_risk_filter_block:{meta.get('p_v2746_filter_reasons') or ''}",json.dumps(snap,ensure_ascii=False,default=str),
                    tp2_price,be_price,entry_price,entry_price,entry_price,opened_at,
                    str(int(now.timestamp())//300),str(int(now.timestamp())//900),0.0,0.0,setup_id,0,
                ),
            )
        append_entry_record(
            symbol,"RESEARCH_P_V27_4_6_BLOCK_GHOST_ENTRY","P_V27_4_6_BLOCK_GHOST",
            float(details.get("p_v2_score") or 0),entry_price,
            f"blocked_by={meta.get('p_v2746_filter_reasons') or 'UNKNOWN'}; baseline-management ghost; actual_order=0",
            extra={**details,**meta,"shadow_id":shadow_id,"research_variant":"P_V27_4_6_BLOCK_GHOST",
                   "shadow_entry_time":_kst_stamp(opened_at),"p_v2746_confirm_state":"BLOCKED_FILTER_GHOST",
                   "p_v2746_live_entry_price":entry_price,"p_v2746_closed_5m_price":closed_5m_price,
                   "p_v2746_is_block_ghost":True},
        )

    def _open_pv2746_confirmed_shadow(
        self, symbol: str, details: dict[str, Any], source_setup_id: str, entry_price: float,
        closed_5m_price: float, five_bullish: bool, high_break: bool,
    ) -> bool:
        """P_V27_4_6: V25 진입 + 신규 Entry Risk Guard만 검증. 종료관리는 4.3과 동일."""
        if not self.cfg.research_pv2746_enabled or entry_price <= 0:
            return False
        now = datetime.now(timezone.utc)
        setup_id = str(source_setup_id).replace("P25SET-", "P2746SET-", 1)
        meta = self._pv2746_entry_risk_filter_meta(details)

        if bool(meta.get("p_v2746_filter_block")):
            append_entry_record(
                symbol,"RESEARCH_P_V27_4_6_BLOCKED_FILTER","P_V27_4_6",
                float(details.get("p_v2_score") or 0),entry_price,
                f"entry_risk_filter={meta.get('p_v2746_filter_reasons') or 'UNKNOWN'}; actual_order=0",
                extra={**details,**meta,"p_v2746_confirm_state":"BLOCKED_FILTER",
                       "p_v2746_live_entry_price":entry_price,"p_v2746_closed_5m_price":closed_5m_price},
            )
            self._register_pv2746_block_ghost(
                symbol, details, source_setup_id, entry_price, closed_5m_price, five_bullish, high_break, meta
            )
            return False

        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_4_6' AND symbol=? AND completed=0 LIMIT 1",
                (symbol,),
            ).fetchone():
                return False

        live_cd, live_remaining, live_prior = self._research_live_cooldown_status("P_V27_4_6", symbol, now)
        if live_cd:
            append_entry_record(
                symbol,"RESEARCH_P_V27_4_6_BLOCKED_LIVE90","P_V27_4_6",
                float(details.get("p_v2_score") or 0),entry_price,
                f"LIVE90_COOLDOWN remaining={live_remaining:.1f}m",
                extra={**details,**meta,"p_v2746_confirm_state":"BLOCKED_LIVE90",
                       "p_v2746_live_entry_price":entry_price,"p_v2746_closed_5m_price":closed_5m_price,
                       "cooldown_remaining_min":round(live_remaining,1),"prior_result":live_prior},
            )
            return False
        cooldown, remaining, prior = self._variant_stop_cooldown_status("P_V27_4_6",symbol,now)
        if cooldown:
            append_entry_record(
                symbol,"RESEARCH_P_V27_4_6_BLOCKED_COOLDOWN","P_V27_4_6",
                float(details.get("p_v2_score") or 0),entry_price,
                f"STOP_COOLDOWN remaining={remaining:.1f}m",
                extra={**details,**meta,"p_v2746_confirm_state":"BLOCKED_COOLDOWN",
                       "p_v2746_live_entry_price":entry_price,"p_v2746_closed_5m_price":closed_5m_price,
                       "cooldown_remaining_min":round(remaining,1),"prior_failure_result":prior},
            )
            return False
        allowed, why = self._variant_can_open_now(
            "P_V27_4_6",now,self.cfg.research_pv2746_max_entries_per_15m,self.cfg.research_pv2746_max_open_positions
        )
        if not allowed:
            append_entry_record(
                symbol,"RESEARCH_P_V27_4_6_SKIPPED","P_V27_4_6",
                float(details.get("p_v2_score") or 0),entry_price,why,
                extra={**details,**meta,"p_v2746_confirm_state":why,
                       "p_v2746_live_entry_price":entry_price,"p_v2746_closed_5m_price":closed_5m_price},
            )
            return False
        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_4_6' AND setup_id=? LIMIT 1",(setup_id,)
            ).fetchone():
                return False
            opened_at = now.isoformat()
            shadow_id = f"RSH-P_V27_4_6-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = entry_price*(1+float(self.cfg.tp2_pct)/100)
            be_price = entry_price*(1+float(self.cfg.breakeven_stop_pct)/100)
            snap = dict(details)
            snap.update({
                **meta,
                "p_v2746_confirm_state":"CONFIRMED_FILTER_PASS",
                "p_v2746_live_entry_price":entry_price,
                "p_v2746_closed_5m_price":closed_5m_price,
                "p_v2746_is_block_ghost":False,
                "p_v25_setup_id":source_setup_id,"p_v25_5m_bullish":five_bullish,"p_v25_prev_high_break":high_break,
            })
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,missing_condition,snapshot_json,
                    tp2_price,be_price,highest_price,lowest_price,last_price,last_checked_at,last_5m_bucket,last_15m_bucket,
                    mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    shadow_id,"P_V27_4_6",symbol,opened_at,int(now.timestamp()*1000),entry_price,
                    float(details.get("p_score") or 0),float(details.get("p_v2_score") or 0),
                    "confirmed_5m_break_entry_risk_filter_pass",json.dumps(snap,ensure_ascii=False,default=str),
                    tp2_price,be_price,entry_price,entry_price,entry_price,opened_at,
                    str(int(now.timestamp())//300),str(int(now.timestamp())//900),0.0,0.0,setup_id,0,
                ),
            )
        append_entry_record(
            symbol,"RESEARCH_P_V27_4_6_ENTRY","P_V27_4_6",float(details.get("p_v2_score") or 0),entry_price,
            f"V25 entry + Entry Risk Guard PASS; baseline management; actual_order=0; filter={meta.get('p_v2746_filter_version')}",
            extra={**details,**meta,"shadow_id":shadow_id,"research_variant":"P_V27_4_6",
                   "shadow_entry_time":_kst_stamp(opened_at),"p_v2746_confirm_state":"CONFIRMED_FILTER_PASS",
                   "p_v2746_live_entry_price":entry_price,"p_v2746_closed_5m_price":closed_5m_price},
        )
        return True

    def _clone_pv2746_stop_ghost(
        self, review: sqlite3.Row, stop_ts: str, stop_price: float, result_details: dict[str, Any], *, backfilled: bool = False,
    ) -> None:
        """P_V27_4_6 또는 4.6 BLOCK_GHOST의 STOP 뒤 180분 raw path를 추적한다."""
        if not self.cfg.research_pv2746_stop_ghost_enabled:
            return
        source_shadow_id = str(review["shadow_id"] or "")
        symbol = str(review["symbol"] or "")
        entry_price = float(review["entry_price"] or 0)
        if not source_shadow_id or not symbol or entry_price <= 0 or stop_price <= 0:
            return
        ghost_variant = "P_V27_4_6_STOP_GHOST"
        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant=? AND missing_condition=? LIMIT 1",
                (ghost_variant, f"source={source_shadow_id}"),
            ).fetchone():
                return
            try:
                stop_dt = datetime.fromisoformat(str(stop_ts))
                if stop_dt.tzinfo is None:
                    stop_dt = stop_dt.replace(tzinfo=timezone.utc)
            except Exception:
                stop_dt = datetime.now(timezone.utc)
            try:
                source_snap = json.loads(str(review["snapshot_json"] or "{}"))
            except Exception:
                source_snap = {}
            stop_pct = (float(stop_price) / entry_price - 1.0) * 100.0
            stop_type = str(result_details.get("stop_type") or "STOP")
            snap = dict(source_snap)
            snap.update({
                "p_v2746_confirm_state":"STOP_GHOST_TRACKING",
                "p_v2746_ghost_type":ghost_variant,
                "p_v2746_source_shadow_id":source_shadow_id,
                "source_variant":str(review["variant"] or ""),
                "source_stop_ts":str(stop_ts),"source_stop_price":float(stop_price),
                "source_stop_pct":round(stop_pct,4),"source_stop_type":stop_type,
                "raw_path_milestones":{},"stop_ghost_backfilled":bool(backfilled),
            })
            shadow_id = f"RSH-{ghost_variant}-{stop_dt.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = entry_price * (1 + float(self.cfg.tp2_pct) / 100)
            be_price = entry_price * (1 + float(self.cfg.breakeven_stop_pct) / 100)
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,
                    missing_condition,snapshot_json,tp2_price,be_price,highest_price,lowest_price,last_price,
                    last_checked_at,last_5m_bucket,last_15m_bucket,mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (shadow_id,ghost_variant,symbol,stop_dt.isoformat(),int(stop_dt.timestamp()*1000),entry_price,
                 float(review["old_p_score"] or 0),float(review["new_p_score"] or 0),f"source={source_shadow_id}",
                 json.dumps(snap,ensure_ascii=False,default=str),tp2_price,be_price,float(stop_price),float(stop_price),float(stop_price),
                 datetime.now(timezone.utc).isoformat(),str(int(stop_dt.timestamp())//300),str(int(stop_dt.timestamp())//900),
                 stop_pct,stop_pct,str(review["setup_id"] or ""),0),
            )
        append_entry_record(
            symbol,"RESEARCH_P_V27_4_6_STOP_GHOST_ENTRY",ghost_variant,float(review["new_p_score"] or 0),float(stop_price),
            f"source_stop={stop_type}; stop_pct={stop_pct:.3f}%; actual_order=0",
            extra={"shadow_id":shadow_id,"research_variant":ghost_variant,"p_v2746_ghost_type":ghost_variant,
                   "p_v2746_source_shadow_id":source_shadow_id,
                   "p_v2746_filter_reasons":str(source_snap.get("p_v2746_filter_reasons") or ""),
                   "p_v2746_filter_version":str(source_snap.get("p_v2746_filter_version") or "")},
        )

    def _backfill_pv2746_stop_ghosts(self) -> None:
        if not self.cfg.research_pv2746_stop_ghost_enabled:
            return
        with db() as conn:
            rows = conn.execute(
                "SELECT * FROM research_shadow_reviews WHERE variant IN ('P_V27_4_6','P_V27_4_6_BLOCK_GHOST') AND completed=1 AND result='STOP' ORDER BY result_ts"
            ).fetchall()
        for row in rows:
            try:
                details = json.loads(str(row["result_details"] or "{}"))
            except Exception:
                details = {}
            try:
                self._clone_pv2746_stop_ghost(
                    row, str(row["result_ts"] or utc_now()), float(row["result_price"] or 0), details, backfilled=True
                )
            except Exception as exc:
                append_entry_record(
                    str(row["symbol"] or ""), "RESEARCH_P_V27_4_6_STOP_GHOST_BACKFILL_ERROR",
                    "P_V27_4_6_STOP_GHOST", 0, float(row["result_price"] or 0), f"{type(exc).__name__}: {exc}"
                )

    def _open_pv2744_confirmed_shadow(
        self, symbol: str, details: dict[str, Any], source_setup_id: str, entry_price: float,
        closed_5m_price: float, five_bullish: bool, high_break: bool,
    ) -> bool:
        """P_V27_4_4: V25 진입 그대로 + TP1 후 BE에서 남은 물량의 절반만 보호청산하는 연구군."""
        if not self.cfg.research_pv2744_enabled or entry_price <= 0:
            return False
        now = datetime.now(timezone.utc)
        setup_id = str(source_setup_id).replace("P25SET-", "P2744SET-", 1)
        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_4_4' AND symbol=? AND completed=0 LIMIT 1",
                (symbol,),
            ).fetchone():
                return False

        live_cd, live_remaining, live_prior = self._research_live_cooldown_status("P_V27_4_4", symbol, now)
        if live_cd:
            append_entry_record(
                symbol,"RESEARCH_P_V27_4_4_BLOCKED_LIVE90","P_V27_4_4",
                float(details.get("p_v2_score") or 0),entry_price,
                f"LIVE90_COOLDOWN remaining={live_remaining:.1f}m",
                extra={**details,"p_v2744_confirm_state":"BLOCKED_LIVE90",
                       "p_v2744_live_entry_price":entry_price,"p_v2744_closed_5m_price":closed_5m_price,
                       "cooldown_remaining_min":round(live_remaining,1),"prior_result":live_prior},
            )
            return False
        cooldown, remaining, prior = self._variant_stop_cooldown_status("P_V27_4_4",symbol,now)
        if cooldown:
            append_entry_record(
                symbol,"RESEARCH_P_V27_4_4_BLOCKED_COOLDOWN","P_V27_4_4",
                float(details.get("p_v2_score") or 0),entry_price,
                f"STOP_COOLDOWN remaining={remaining:.1f}m",
                extra={**details,"p_v2744_confirm_state":"BLOCKED_COOLDOWN",
                       "p_v2744_live_entry_price":entry_price,"p_v2744_closed_5m_price":closed_5m_price,
                       "cooldown_remaining_min":round(remaining,1),"prior_failure_result":prior},
            )
            return False
        allowed, why = self._variant_can_open_now(
            "P_V27_4_4",now,self.cfg.research_pv2744_max_entries_per_15m,self.cfg.research_pv2744_max_open_positions
        )
        if not allowed:
            append_entry_record(
                symbol,"RESEARCH_P_V27_4_4_SKIPPED","P_V27_4_4",
                float(details.get("p_v2_score") or 0),entry_price,why,
                extra={**details,"p_v2744_confirm_state":why,
                       "p_v2744_live_entry_price":entry_price,"p_v2744_closed_5m_price":closed_5m_price},
            )
            return False
        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_4_4' AND setup_id=? LIMIT 1",(setup_id,)
            ).fetchone():
                return False
            opened_at = now.isoformat()
            shadow_id = f"RSH-P_V27_4_4-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = entry_price*(1+float(self.cfg.tp2_pct)/100)
            be_price = entry_price*(1+float(self.cfg.breakeven_stop_pct)/100)
            be_frac_remaining = min(0.95,max(0.05,float(self.cfg.research_pv2744_be_close_fraction_of_remaining)))
            be_frac_original = 0.50 * be_frac_remaining
            final_frac_original = 0.50 * (1.0 - be_frac_remaining)
            snap = dict(details)
            snap.update({
                "p_v2744_confirm_state":"CONFIRMED_5M_BREAK",
                "p_v2744_live_entry_price":entry_price,
                "p_v2744_closed_5m_price":closed_5m_price,
                "p_v2744_be_partial_done":False,
                "p_v2744_be_partial_price":None,
                "p_v2744_be_partial_original_fraction":round(be_frac_original,4),
                "p_v2744_final_original_fraction":round(final_frac_original,4),
                "p_v2744_be_mode":"HALF_OF_REMAINING_AT_BE_THEN_COMMON_MANAGEMENT",
                "p_v25_setup_id":source_setup_id,"p_v25_5m_bullish":five_bullish,"p_v25_prev_high_break":high_break,
            })
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,missing_condition,snapshot_json,
                    tp2_price,be_price,highest_price,lowest_price,last_price,last_checked_at,last_5m_bucket,last_15m_bucket,
                    mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    shadow_id,"P_V27_4_4",symbol,opened_at,int(now.timestamp()*1000),entry_price,
                    float(details.get("p_score") or 0),float(details.get("p_v2_score") or 0),
                    "confirmed_5m_break_partial_be",json.dumps(snap,ensure_ascii=False,default=str),
                    tp2_price,be_price,entry_price,entry_price,entry_price,opened_at,
                    str(int(now.timestamp())//300),str(int(now.timestamp())//900),0.0,0.0,setup_id,0,
                ),
            )
        append_entry_record(
            symbol,"RESEARCH_P_V27_4_4_ENTRY","P_V27_4_4",float(details.get("p_v2_score") or 0),entry_price,
            "V25 live-price entry; TP1 50% + BE half-of-remaining + final quarter common management; actual_order=0",
            extra={**details,"shadow_id":shadow_id,"research_variant":"P_V27_4_4",
                   "shadow_entry_time":_kst_stamp(opened_at),"p_v2744_confirm_state":"CONFIRMED_5M_BREAK",
                   "p_v2744_live_entry_price":entry_price,"p_v2744_closed_5m_price":closed_5m_price,
                   "p_v2744_be_partial_done":False,"p_v2744_be_partial_original_fraction":round(be_frac_original,4),
                   "p_v2744_final_original_fraction":round(final_frac_original,4)},
        )
        return True

    def _open_pv2745_confirmed_shadow(
        self, symbol: str, details: dict[str, Any], source_setup_id: str, entry_price: float,
        closed_5m_price: float, five_bullish: bool, high_break: bool,
    ) -> bool:
        """P_V27_4_5: V25 진입 + V27-1 동일손절 + TP1 후 15분 BE Recovery 연구군."""
        if not self.cfg.research_pv2745_enabled or entry_price <= 0:
            return False
        now = datetime.now(timezone.utc)
        setup_id = str(source_setup_id).replace("P25SET-", "P2745SET-", 1)
        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_4_5' AND symbol=? AND completed=0 LIMIT 1",
                (symbol,),
            ).fetchone():
                return False

        live_cd, live_remaining, live_prior = self._research_live_cooldown_status("P_V27_4_5", symbol, now)
        if live_cd:
            append_entry_record(
                symbol,"RESEARCH_P_V27_4_5_BLOCKED_LIVE90","P_V27_4_5",
                float(details.get("p_v2_score") or 0),entry_price,
                f"LIVE90_COOLDOWN remaining={live_remaining:.1f}m",
                extra={**details,"p_v2745_confirm_state":"BLOCKED_LIVE90",
                       "p_v2745_live_entry_price":entry_price,"p_v2745_closed_5m_price":closed_5m_price,
                       "cooldown_remaining_min":round(live_remaining,1),"prior_result":live_prior},
            )
            return False
        cooldown, remaining, prior = self._variant_stop_cooldown_status("P_V27_4_5",symbol,now)
        if cooldown:
            append_entry_record(
                symbol,"RESEARCH_P_V27_4_5_BLOCKED_COOLDOWN","P_V27_4_5",
                float(details.get("p_v2_score") or 0),entry_price,
                f"STOP_COOLDOWN remaining={remaining:.1f}m",
                extra={**details,"p_v2745_confirm_state":"BLOCKED_COOLDOWN",
                       "p_v2745_live_entry_price":entry_price,"p_v2745_closed_5m_price":closed_5m_price,
                       "cooldown_remaining_min":round(remaining,1),"prior_failure_result":prior},
            )
            return False
        allowed, why = self._variant_can_open_now(
            "P_V27_4_5",now,self.cfg.research_pv2745_max_entries_per_15m,self.cfg.research_pv2745_max_open_positions
        )
        if not allowed:
            append_entry_record(
                symbol,"RESEARCH_P_V27_4_5_SKIPPED","P_V27_4_5",
                float(details.get("p_v2_score") or 0),entry_price,why,
                extra={**details,"p_v2745_confirm_state":why,
                       "p_v2745_live_entry_price":entry_price,"p_v2745_closed_5m_price":closed_5m_price},
            )
            return False
        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_4_5' AND setup_id=? LIMIT 1",(setup_id,)
            ).fetchone():
                return False
            opened_at = now.isoformat()
            shadow_id = f"RSH-P_V27_4_5-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = entry_price*(1+float(self.cfg.tp2_pct)/100)
            be_price = entry_price*(1+float(self.cfg.breakeven_stop_pct)/100)
            be_frac_remaining = min(0.95,max(0.05,float(self.cfg.research_pv2745_be_close_fraction_of_remaining)))
            be_frac_original = 0.50 * be_frac_remaining
            final_frac_original = 0.50 * (1.0 - be_frac_remaining)
            snap = dict(details)
            snap.update({
                "p_v2745_confirm_state":"CONFIRMED_5M_BREAK",
                "p_v2745_live_entry_price":entry_price,
                "p_v2745_closed_5m_price":closed_5m_price,
                "p_v2745_be_partial_done":False,
                "p_v2745_be_partial_price":None,
                "p_v2745_be_partial_original_fraction":round(be_frac_original,4),
                "p_v2745_final_original_fraction":round(final_frac_original,4),
                "p_v2745_be_recovery":{"active":False},
                "p_v2745_be_mode":"V271_STOP_THEN_QUARTER_RECOVERY_15M",
                "p_v25_setup_id":source_setup_id,"p_v25_5m_bullish":five_bullish,"p_v25_prev_high_break":high_break,
            })
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,missing_condition,snapshot_json,
                    tp2_price,be_price,highest_price,lowest_price,last_price,last_checked_at,last_5m_bucket,last_15m_bucket,
                    mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    shadow_id,"P_V27_4_5",symbol,opened_at,int(now.timestamp()*1000),entry_price,
                    float(details.get("p_score") or 0),float(details.get("p_v2_score") or 0),
                    "confirmed_5m_break_v271_stop_be_recovery",json.dumps(snap,ensure_ascii=False,default=str),
                    tp2_price,be_price,entry_price,entry_price,entry_price,opened_at,
                    str(int(now.timestamp())//300),str(int(now.timestamp())//900),0.0,0.0,setup_id,0,
                ),
            )
        append_entry_record(
            symbol,"RESEARCH_P_V27_4_5_ENTRY","P_V27_4_5",float(details.get("p_v2_score") or 0),entry_price,
            "V25 live-price entry; V27-1 pre-TP1 stop; TP1 50% + BE 25% + final 25% recovery(+1/-2.5/15m); actual_order=0",
            extra={**details,"shadow_id":shadow_id,"research_variant":"P_V27_4_5",
                   "shadow_entry_time":_kst_stamp(opened_at),"p_v2745_confirm_state":"CONFIRMED_5M_BREAK",
                   "p_v2745_live_entry_price":entry_price,"p_v2745_closed_5m_price":closed_5m_price,
                   "p_v2745_be_partial_done":False,"p_v2745_be_partial_original_fraction":round(be_frac_original,4),
                   "p_v2745_final_original_fraction":round(final_frac_original,4),
                   "p_v2745_be_recovery_active":False},
        )
        return True

    def _clone_pv2741_stop_ghost(
        self, review: sqlite3.Row, stop_ts: str, stop_price: float, result_details: dict[str, Any], *, backfilled: bool = False,
    ) -> None:
        """P_V27_4_1 STOP 뒤 180분 raw path 추적."""
        if not self.cfg.research_pv2741_stop_ghost_enabled:
            return
        source_shadow_id = str(review["shadow_id"] or "")
        symbol = str(review["symbol"] or "")
        entry_price = float(review["entry_price"] or 0)
        if not source_shadow_id or not symbol or entry_price <= 0 or stop_price <= 0:
            return
        ghost_variant = "P_V27_4_1_STOP_GHOST"
        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant=? AND missing_condition=? LIMIT 1",
                (ghost_variant, f"source={source_shadow_id}"),
            ).fetchone():
                return
            try:
                stop_dt = datetime.fromisoformat(str(stop_ts))
                if stop_dt.tzinfo is None:
                    stop_dt = stop_dt.replace(tzinfo=timezone.utc)
            except Exception:
                stop_dt = datetime.now(timezone.utc)
            try:
                source_snap = json.loads(str(review["snapshot_json"] or "{}"))
            except Exception:
                source_snap = {}
            stop_pct = (float(stop_price)/entry_price-1.0)*100.0
            stop_type = str(result_details.get("stop_type") or "STOP")
            snap = dict(source_snap)
            snap.update({
                "p_v2741_confirm_state":"STOP_GHOST_TRACKING","p_v2741_ghost_type":ghost_variant,
                "p_v2741_source_shadow_id":source_shadow_id,"source_stop_ts":str(stop_ts),
                "source_stop_price":float(stop_price),"source_stop_pct":round(stop_pct,4),"source_stop_type":stop_type,
                "raw_path_milestones":{},"stop_ghost_backfilled":bool(backfilled),
            })
            shadow_id=f"RSH-{ghost_variant}-{stop_dt.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price=entry_price*(1+float(self.cfg.tp2_pct)/100)
            be_price=entry_price*(1+float(self.cfg.breakeven_stop_pct)/100)
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,
                    missing_condition,snapshot_json,tp2_price,be_price,highest_price,lowest_price,last_price,
                    last_checked_at,last_5m_bucket,last_15m_bucket,mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (shadow_id,ghost_variant,symbol,stop_dt.isoformat(),int(stop_dt.timestamp()*1000),entry_price,
                 float(review["old_p_score"] or 0),float(review["new_p_score"] or 0),f"source={source_shadow_id}",
                 json.dumps(snap,ensure_ascii=False,default=str),tp2_price,be_price,float(stop_price),float(stop_price),float(stop_price),
                 datetime.now(timezone.utc).isoformat(),str(int(stop_dt.timestamp())//300),str(int(stop_dt.timestamp())//900),
                 stop_pct,stop_pct,str(review["setup_id"] or ""),0),
            )
        append_entry_record(
            symbol,"RESEARCH_P_V27_4_1_STOP_GHOST_ENTRY",ghost_variant,float(review["new_p_score"] or 0),float(stop_price),
            f"source_stop={stop_type}; stop_pct={stop_pct:.3f}%; actual_order=0",
            extra={"shadow_id":shadow_id,"research_variant":ghost_variant,"p_v2741_ghost_type":ghost_variant,
                   "p_v2741_source_shadow_id":source_shadow_id},
        )

    def _backfill_pv2741_stop_ghosts(self) -> None:
        if not self.cfg.research_pv2741_stop_ghost_enabled:
            return
        with db() as conn:
            rows=conn.execute("SELECT * FROM research_shadow_reviews WHERE variant='P_V27_4_1' AND completed=1 AND result='STOP' ORDER BY result_ts").fetchall()
        for row in rows:
            try:
                details=json.loads(str(row["result_details"] or "{}"))
            except Exception:
                details={}
            try:
                self._clone_pv2741_stop_ghost(row,str(row["result_ts"] or utc_now()),float(row["result_price"] or 0),details,backfilled=True)
            except Exception as exc:
                append_entry_record(str(row["symbol"] or ""),"RESEARCH_P_V27_4_1_STOP_GHOST_BACKFILL_ERROR","P_V27_4_1_STOP_GHOST",0,float(row["result_price"] or 0),f"{type(exc).__name__}: {exc}")

    def _clone_pv274_stop_ghost(
        self,
        review: sqlite3.Row,
        stop_ts: str,
        stop_price: float,
        result_details: dict[str, Any],
        *,
        backfilled: bool = False,
    ) -> None:
        """P_V27_4 STOP 뒤 180분을 원래 진입가 기준으로 추적해 진입문제/STOP문제를 분리한다."""
        if not self.cfg.research_pv274_stop_ghost_enabled:
            return
        source_shadow_id = str(review["shadow_id"] or "")
        symbol = str(review["symbol"] or "")
        entry_price = float(review["entry_price"] or 0)
        if not source_shadow_id or not symbol or entry_price <= 0 or stop_price <= 0:
            return
        with db() as conn:
            if conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V27_4_STOP_GHOST' AND missing_condition=? LIMIT 1",
                (f"source={source_shadow_id}",),
            ).fetchone():
                return
            try:
                stop_dt = datetime.fromisoformat(str(stop_ts))
                if stop_dt.tzinfo is None:
                    stop_dt = stop_dt.replace(tzinfo=timezone.utc)
            except Exception:
                stop_dt = datetime.now(timezone.utc)
                stop_ts = stop_dt.isoformat()
            try:
                source_snap = json.loads(str(review["snapshot_json"] or "{}"))
            except Exception:
                source_snap = {}
            stop_pct = (float(stop_price) / entry_price - 1.0) * 100.0
            stop_type = str(result_details.get("stop_type") or "STOP")
            snap = dict(source_snap)
            snap.update({
                "p_v274_confirm_state":"STOP_GHOST_TRACKING",
                "p_v274_block_reason":stop_type,
                "p_v274_ghost_type":"P_V27_4_STOP_GHOST",
                "p_v274_source_shadow_id":source_shadow_id,
                "source_opened_at":str(review["opened_at"] or ""),
                "source_stop_ts":str(stop_ts),
                "source_stop_price":float(stop_price),
                "source_stop_pct":round(stop_pct,4),
                "source_stop_type":stop_type,
                "raw_path_milestones":{},
                "stop_ghost_backfilled":bool(backfilled),
            })
            shadow_id = f"RSH-P_V27_4_STOP_GHOST-{stop_dt.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = entry_price*(1+float(self.cfg.tp2_pct)/100)
            be_price = entry_price*(1+float(self.cfg.breakeven_stop_pct)/100)
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,
                    missing_condition,snapshot_json,tp2_price,be_price,highest_price,lowest_price,last_price,
                    last_checked_at,last_5m_bucket,last_15m_bucket,mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    shadow_id,"P_V27_4_STOP_GHOST",symbol,stop_dt.isoformat(),int(stop_dt.timestamp()*1000),
                    entry_price,float(review["old_p_score"] or 0),float(review["new_p_score"] or 0),
                    f"source={source_shadow_id}",json.dumps(snap,ensure_ascii=False,default=str),
                    tp2_price,be_price,float(stop_price),float(stop_price),float(stop_price),
                    datetime.now(timezone.utc).isoformat(),str(int(stop_dt.timestamp())//300),
                    str(int(stop_dt.timestamp())//900),stop_pct,stop_pct,str(review["setup_id"] or ""),0,
                ),
            )
        append_entry_record(
            symbol,"RESEARCH_P_V27_4_STOP_GHOST_ENTRY","P_V27_4_STOP_GHOST",
            float(review["new_p_score"] or 0),float(stop_price),
            f"source_stop={stop_type}; stop_pct={stop_pct:.3f}%; backfilled={int(backfilled)}; actual_order=0",
            extra={"shadow_id":shadow_id,"research_variant":"P_V27_4_STOP_GHOST",
                   "shadow_entry_time":_kst_stamp(stop_dt.isoformat()),
                   "p_v274_confirm_state":"STOP_GHOST_TRACKING","p_v274_block_reason":stop_type,
                   "p_v274_ghost_type":"P_V27_4_STOP_GHOST","p_v274_source_shadow_id":source_shadow_id},
        )

    def _backfill_pv274_stop_ghosts(self) -> None:
        """재시작 전 이미 끝난 V27-4 STOP이 있으면 180분 추적 ghost를 보강한다."""
        if not self.cfg.research_pv274_stop_ghost_enabled:
            return
        with db() as conn:
            rows = conn.execute(
                """SELECT * FROM research_shadow_reviews
                   WHERE variant='P_V27_4' AND completed=1 AND result='STOP'
                   ORDER BY result_ts"""
            ).fetchall()
        for row in rows:
            try:
                details = json.loads(str(row["result_details"] or "{}"))
            except Exception:
                details = {}
            try:
                self._clone_pv274_stop_ghost(
                    row,str(row["result_ts"] or utc_now()),float(row["result_price"] or 0),details,backfilled=True
                )
            except Exception as exc:
                append_entry_record(
                    str(row["symbol"] or ""),"RESEARCH_P_V27_4_STOP_GHOST_BACKFILL_ERROR",
                    "P_V27_4_STOP_GHOST",0,float(row["result_price"] or 0),f"{type(exc).__name__}: {exc}"
                )

    def _open_pv26_confirmed_shadow(
        self,
        symbol: str,
        details: dict[str, Any],
        source_setup_id: str,
        entry_price: float,
        closed_5m_price: float,
        five_bullish: bool,
        high_break: bool,
    ) -> bool:
        """P_V26: V25와 같은 확인 진입에서 손실 최소화 3가지만 추가한 독립 Shadow."""
        if not self.cfg.research_pv26_enabled:
            return False
        now = datetime.now(timezone.utc)
        setup_id = str(source_setup_id).replace("P25SET-", "P26SET-", 1)

        # 구현 오류 방지: setup_id가 달라도 같은 종목 P_V26 포지션이 열려 있으면 절대 중복진입하지 않는다.
        with db() as conn:
            open_same = conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V26' AND symbol=? AND completed=0 LIMIT 1",
                (symbol,),
            ).fetchone()
        if open_same:
            append_entry_record(
                symbol, "RESEARCH_P_V26_SKIPPED", "P_V26", float(details.get("p_v2_score") or 0),
                entry_price, "duplicate_open_symbol",
                extra={
                    **details,
                    "p_v26_confirm_state": "duplicate_open_symbol",
                    "p_v26_block_reason": "duplicate_open_symbol",
                },
            )
            return False

        # 1) A형은 진입 전에 차단. 차단된 거래는 ghost로 원래 결과를 끝까지 본다.
        a_block, a_meta = self._pv26_a_filter_match(details)
        if a_block:
            self._register_pv26_blocked_ghost(
                symbol, details, setup_id, entry_price, "P_V26_A_GHOST", "A_FILTER", a_meta
            )
            append_entry_record(
                symbol, "RESEARCH_P_V26_BLOCKED_A", "P_V26", float(details.get("p_v2_score") or 0),
                entry_price, "A_FILTER",
                extra={
                    **details,
                    "p_v26_confirm_state": "BLOCKED_A",
                    "p_v26_block_reason": "A_FILTER",
                    "p_v26_a_filter": True,
                    "p_v26_5m_bullish": five_bullish,
                    "p_v26_prev_high_break": high_break,
                    "p_v26_closed_5m_price": closed_5m_price,
                    **a_meta,
                },
            )
            return False

        # v4.3.72: LIVE 기본과 동일하게 모든 종료 후 90분은 같은 종목 재진입을 금지한다.
        # STOP/LATE는 아래 기존 180분 cooldown이 추가로 더 길게 보호한다.
        live_cd, live_remaining, live_prior = self._research_live_cooldown_status("P_V26", symbol, now)
        if live_cd:
            meta = {"cooldown_remaining_min": round(live_remaining, 1), "prior_result": live_prior}
            self._register_pv26_blocked_ghost(
                symbol, details, setup_id, entry_price, "P_V26_COOLDOWN_GHOST", "LIVE90_COOLDOWN", meta
            )
            append_entry_record(
                symbol, "RESEARCH_P_V26_BLOCKED_LIVE90", "P_V26", float(details.get("p_v2_score") or 0),
                entry_price, f"LIVE90_COOLDOWN remaining={live_remaining:.1f}m",
                extra={
                    **details,
                    "p_v26_confirm_state": "BLOCKED_LIVE90",
                    "p_v26_block_reason": "LIVE90_COOLDOWN",
                    "p_v26_cooldown_active": True,
                    "p_v26_5m_bullish": five_bullish,
                    "p_v26_prev_high_break": high_break,
                    "p_v26_closed_5m_price": closed_5m_price,
                    **meta,
                },
            )
            return False

        # 2) STOP/LATE 실패종목만 180분 재진입 금지. 역시 ghost로 놓친 TP/피한 STOP을 측정한다.
        cooldown, remaining, prior_result = self._pv26_stop_cooldown_status(symbol, now)
        if cooldown:
            meta = {"cooldown_remaining_min": round(remaining, 1), "prior_failure_result": prior_result}
            self._register_pv26_blocked_ghost(
                symbol, details, setup_id, entry_price, "P_V26_COOLDOWN_GHOST", "STOP_COOLDOWN", meta
            )
            append_entry_record(
                symbol, "RESEARCH_P_V26_BLOCKED_COOLDOWN", "P_V26", float(details.get("p_v2_score") or 0),
                entry_price, f"STOP_COOLDOWN remaining={remaining:.1f}m",
                extra={
                    **details,
                    "p_v26_confirm_state": "BLOCKED_COOLDOWN",
                    "p_v26_block_reason": "STOP_COOLDOWN",
                    "p_v26_cooldown_active": True,
                    "p_v26_5m_bullish": five_bullish,
                    "p_v26_prev_high_break": high_break,
                    "p_v26_closed_5m_price": closed_5m_price,
                    **meta,
                },
            )
            return False

        allowed, why = self._pv26_can_open_now(now)
        if not allowed:
            append_entry_record(
                symbol, "RESEARCH_P_V26_SKIPPED", "P_V26", float(details.get("p_v2_score") or 0),
                entry_price, why,
                extra={
                    **details,
                    "p_v26_confirm_state": why,
                    "p_v26_block_reason": why,
                },
            )
            return False

        with db() as conn:
            existing = conn.execute(
                "SELECT 1 FROM research_shadow_reviews WHERE variant='P_V26' AND setup_id=? LIMIT 1",
                (setup_id,),
            ).fetchone()
            if existing:
                return False
            opened_at = now.isoformat()
            shadow_id = f"RSH-P_V26-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = entry_price * (1 + float(self.cfg.tp2_pct) / 100)
            be_price = entry_price * (1 + float(self.cfg.breakeven_stop_pct) / 100)
            snap = dict(details)
            snap.update({
                "p_v26_confirm_state": "CONFIRMED_5M_BREAK",
                "p_v26_block_reason": "",
                "p_v26_a_filter": False,
                "p_v25_setup_id": source_setup_id,
                "p_v25_5m_bullish": five_bullish,
                "p_v25_prev_high_break": high_break,
                "p_v25_closed_5m_price": closed_5m_price,
                "p_v26_5m_bullish": five_bullish,
                "p_v26_prev_high_break": high_break,
                "p_v26_closed_5m_price": closed_5m_price,
            })
            conn.execute(
                """INSERT INTO research_shadow_reviews(
                    shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,
                    missing_condition,snapshot_json,tp2_price,be_price,highest_price,lowest_price,last_price,
                    last_checked_at,last_5m_bucket,last_15m_bucket,mfe_pct,mae_pct,setup_id,v26_late_fail_streak
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    shadow_id, "P_V26", symbol, opened_at, int(now.timestamp() * 1000), entry_price,
                    float(details.get("p_score") or 0), float(details.get("p_v2_score") or 0),
                    "confirmed_5m_break_v26", json.dumps(snap, ensure_ascii=False, default=str),
                    tp2_price, be_price, entry_price, entry_price, entry_price, opened_at,
                    str(int(now.timestamp()) // 300), str(int(now.timestamp()) // 900),
                    0.0, 0.0, setup_id, 0,
                ),
            )
        append_entry_record(
            symbol, "RESEARCH_P_V26_ENTRY", "P_V26", float(details.get("p_v2_score") or 0),
            entry_price, "V25 confirmed 5m break + V26 loss guards passed; actual_order=0",
            extra={
                **details,
                "shadow_id": shadow_id,
                "research_variant": "P_V26",
                "shadow_entry_time": _kst_stamp(opened_at),
                "p_v26_confirm_state": "CONFIRMED_5M_BREAK",
                "p_v26_block_reason": "",
                "p_v26_a_filter": False,
                "p_v26_cooldown_active": False,
                "p_v26_late_fail_streak": 0,
                "p_v25_setup_id": source_setup_id,
                "p_v25_5m_bullish": five_bullish,
                "p_v25_prev_high_break": high_break,
                "p_v25_closed_5m_price": closed_5m_price,
                "p_v26_5m_bullish": five_bullish,
                "p_v26_prev_high_break": high_break,
                "p_v26_closed_5m_price": closed_5m_price,
            },
        )
        self._clone_pv27_filter_only_from_v26(
            symbol, details, shadow_id, source_setup_id, opened_at, entry_price, closed_5m_price, five_bullish, high_break
        )
        self._clone_pv272_filter_only_from_v26(
            symbol, details, shadow_id, source_setup_id, opened_at, entry_price, closed_5m_price, five_bullish, high_break
        )
        self._clone_pv273_filter_only_from_v26(
            symbol, details, shadow_id, source_setup_id, opened_at, entry_price, closed_5m_price, five_bullish, high_break
        )
        self._clone_pv274_filter_only_from_v26(
            symbol, details, shadow_id, source_setup_id, opened_at, entry_price, closed_5m_price, five_bullish, high_break
        )
        self._clone_pv271_from_v26(
            symbol, details, shadow_id, source_setup_id, entry_price, closed_5m_price, five_bullish, high_break
        )
        self._clone_pv271r_from_v26(
            symbol, details, shadow_id, source_setup_id, entry_price, closed_5m_price, five_bullish, high_break
        )
        return True

    def _open_pv25_confirmed_shadow(self, symbol: str, details: dict[str, Any], setup_id: str,
                                     entry_price: float, five_bullish: bool, high_break: bool) -> bool:
        now = datetime.now(timezone.utc)
        allowed, why = self._pv25_can_open_now(now)
        if not allowed:
            with db() as conn:
                conn.execute("UPDATE research_pv25_setups SET status='DROPPED',last_seen_at=?,note=? WHERE setup_id=?",
                             (now.isoformat(), why, setup_id))
            append_entry_record(symbol, "RESEARCH_P_V25_SKIPPED", "P_V25", float(details.get("p_v2_score") or 0), entry_price, why,
                extra={**details, "p_v25_setup_id": setup_id, "p_v25_confirm_state": why,
                       "p_v25_5m_bullish": five_bullish, "p_v25_prev_high_break": high_break,
                       "p_v25_closed_5m_price": entry_price})
            return False
        with db() as conn:
            existing = conn.execute("SELECT 1 FROM research_shadow_reviews WHERE variant='P_V25' AND setup_id=? LIMIT 1", (setup_id,)).fetchone()
            if existing:
                return False
            opened_at = now.isoformat()
            shadow_id = f"RSH-P_V25-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = entry_price * (1 + float(self.cfg.tp2_pct) / 100)
            be_price = entry_price * (1 + float(self.cfg.breakeven_stop_pct) / 100)
            snap = dict(details)
            snap.update({"p_v25_confirm_state": "CONFIRMED_5M_BREAK", "p_v25_5m_bullish": five_bullish,
                         "p_v25_prev_high_break": high_break, "p_v25_closed_5m_price": entry_price,
                         "p_v25_setup_id": setup_id})
            conn.execute("""INSERT INTO research_shadow_reviews(
                shadow_id,variant,symbol,opened_at,entry_ts_ms,entry_price,old_p_score,new_p_score,missing_condition,snapshot_json,
                tp2_price,be_price,highest_price,lowest_price,last_price,last_checked_at,mfe_pct,mae_pct,setup_id
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (shadow_id,"P_V25",symbol,opened_at,int(now.timestamp()*1000),entry_price,float(details.get("p_score") or 0),
                 float(details.get("p_v2_score") or 0),"confirmed_5m_break",json.dumps(snap,ensure_ascii=False,default=str),
                 tp2_price,be_price,entry_price,entry_price,entry_price,opened_at,0.0,0.0,setup_id))
            conn.execute("UPDATE research_pv25_setups SET status='CONFIRMED',confirmed_at=?,confirmed_price=?,last_seen_at=?,note=? WHERE setup_id=?",
                         (now.isoformat(),entry_price,now.isoformat(),"confirmed_5m_break",setup_id))
        append_entry_record(symbol,"RESEARCH_P_V25_ENTRY","P_V25",float(details.get("p_v2_score") or 0),entry_price,
            "confirmed closed 5m bullish + previous high break; actual_order=0",
            extra={**details,"shadow_id":shadow_id,"research_variant":"P_V25","shadow_entry_time":_kst_stamp(opened_at),
                   "p_v25_setup_id":setup_id,"p_v25_confirm_state":"CONFIRMED_5M_BREAK",
                   "p_v25_5m_bullish":five_bullish,"p_v25_prev_high_break":high_break,"p_v25_closed_5m_price":entry_price})
        return True

    def _handle_pv25_watch(self, symbol: str, details: dict[str, Any]) -> None:
        """P_V25: 15분 P_V22 후보 발생 후 확정 5분봉에서 재가속이 확인될 때만 Shadow 진입."""
        now = datetime.now(timezone.utc)
        live_price = float(details.get("live_price") or details.get("price") or 0)
        if live_price <= 0:
            return
        source_setup = str(details.get("p_setup_id") or f"P65SET-{symbol}-{int(now.timestamp())//900}")
        bucket_suffix = source_setup.rsplit("-", 1)[-1] if "-" in source_setup else str(int(now.timestamp())//900)
        setup_id = f"P25SET-{symbol}-{bucket_suffix}"
        lock_cut = now - timedelta(minutes=max(1, int(self.cfg.research_pv25_same_setup_lock_minutes)))
        current_5m_bucket = str(int(now.timestamp()) // 300)

        with db() as conn:
            recent = conn.execute(
                "SELECT * FROM research_pv25_setups WHERE symbol=? AND first_seen_at>=? ORDER BY first_seen_at DESC LIMIT 1",
                (symbol, lock_cut.isoformat()),
            ).fetchone()
            if not recent:
                if not bool(details.get("p_v22_candidate")):
                    return
                expires = now + timedelta(minutes=max(1, int(self.cfg.research_pv25_confirm_max_minutes)))
                conn.execute("""INSERT OR IGNORE INTO research_pv25_setups(
                    setup_id,symbol,first_seen_at,last_seen_at,trigger_price,lowest_price,last_price,snapshot_json,status,last_5m_bucket,expires_at,note
                ) VALUES(?,?,?,?,?,?,?,?,'WATCH','',?,?)""",
                    (setup_id,symbol,now.isoformat(),now.isoformat(),live_price,live_price,live_price,
                     json.dumps(details,ensure_ascii=False,default=str),expires.isoformat(),"await_closed_5m"))
                append_entry_record(symbol,"RESEARCH_P_CONFIRM_WATCH","P_CONFIRM",float(details.get("p_v2_score") or 0),live_price,"await_closed_5m",
                    extra={**details,"p_v25_setup_id":setup_id,"p_v25_confirm_state":"WATCH"})
                return

            status = str(recent["status"] or "")
            if status != "WATCH":
                return
            setup_id = str(recent["setup_id"] or setup_id)
            first = datetime.fromisoformat(str(recent["first_seen_at"]))
            if first.tzinfo is None:
                first = first.replace(tzinfo=timezone.utc)
            trigger = float(recent["trigger_price"] or live_price)
            low = min(float(recent["lowest_price"] or live_price), live_price)
            age = (now - first).total_seconds() / 60.0
            adverse = (low / trigger - 1) * 100 if trigger > 0 else 0.0

            if age > float(self.cfg.research_pv25_confirm_max_minutes) or adverse <= -abs(float(self.cfg.research_pv25_max_adverse_before_confirm_pct)):
                conn.execute("UPDATE research_pv25_setups SET status='DROPPED',last_seen_at=?,last_price=?,lowest_price=?,note=? WHERE id=?",
                             (now.isoformat(),live_price,low,"expired_or_adverse",int(recent["id"])))
                append_entry_record(symbol,"RESEARCH_P_CONFIRM_DROP","P_CONFIRM",float(details.get("p_v2_score") or 0),live_price,"expired_or_adverse",
                    extra={**details,"p_v25_setup_id":setup_id,"p_v25_confirm_state":"DROPPED"})
                return

            # 가격/저점은 매 스캔 갱신하되, 확정 5분봉 평가는 5분 bucket당 한 번만 한다.
            if str(recent["last_5m_bucket"] or "") == current_5m_bucket:
                conn.execute("UPDATE research_pv25_setups SET last_seen_at=?,last_price=?,lowest_price=?,snapshot_json=? WHERE id=?",
                             (now.isoformat(),live_price,low,json.dumps(details,ensure_ascii=False,default=str),int(recent["id"])))
                return
            conn.execute("UPDATE research_pv25_setups SET last_seen_at=?,last_price=?,lowest_price=?,last_5m_bucket=?,snapshot_json=? WHERE id=?",
                         (now.isoformat(),live_price,low,current_5m_bucket,json.dumps(details,ensure_ascii=False,default=str),int(recent["id"])))

        if age < float(self.cfg.research_pv25_confirm_min_minutes):
            return
        try:
            m5 = confirmed(indicators(self.client.candles(symbol, "5m", 8)))
            if len(m5) < 2:
                return
            row5, prev5 = m5.iloc[-1], m5.iloc[-2]
            five_bullish = bool(float(row5.close) > float(row5.open))
            high_break = bool(float(row5.close) > float(prev5.high))
            closed_price = float(row5.close)
        except Exception as exc:
            append_entry_record(symbol,"RESEARCH_P_CONFIRM_CHECK_ERROR","P_CONFIRM",float(details.get("p_v2_score") or 0),live_price,str(exc),
                extra={**details,"p_v25_setup_id":setup_id,"p_v25_confirm_state":"CHECK_ERROR"})
            return

        if not (five_bullish and high_break):
            return
        # v4.3.69: 확정 5분봉은 신호 판정에만 사용하고, Shadow 평단은 확인 순간 live_price를 사용한다.
        # V25 신규 control은 OFF. v4.3.81부터 V27.4.1(V25 control), 4.3(entry filter), 4.4(partial-BE), 4.5(V25+V27-1 stop+BE recovery), V27-1/1R을 분리 비교한다.
        live_entry_price = float(details.get("live_price") or live_price or 0)
        if live_entry_price <= 0:
            return
        if self.cfg.research_pv25_control_enabled:
            self._open_pv25_confirmed_shadow(symbol, details, setup_id, live_entry_price, five_bullish, high_break)

        # v4.3.86: 최종 Forward 4경로. 기존 LIVE 주문과 완전히 분리된 Shadow다.
        self._open_final_forward_bundle(
            symbol, details, setup_id, live_entry_price, closed_price, five_bullish, high_break
        )

        # v4.3.83: 과거 병렬 Lab은 config에서 신규 OFF. 열린 Shadow 관리만 유지한다.
        # 손절은 P_STOP_CONTROL schedule을 공통으로 사용하고, 진입은 각 경로별 독립 portfolio schedule을 사용한다.
        self._open_parallel_stop_bundle(
            symbol, details, setup_id, live_entry_price, closed_price, five_bullish, high_break
        )
        self._open_parallel_entry_bundle(
            symbol, details, setup_id, live_entry_price, closed_price, five_bullish, high_break
        )

        # 구형 연구 신규진입 함수는 config에서 OFF이며, 기존 열린 Shadow 관리만 유지한다.
        self._open_pv26_confirmed_shadow(
            symbol, details, setup_id, live_entry_price, closed_price, five_bullish, high_break
        )
        self._open_pv27_confirmed_shadow(
            symbol, details, setup_id, live_entry_price, closed_price, five_bullish, high_break
        )
        self._open_pv272_confirmed_shadow(
            symbol, details, setup_id, live_entry_price, closed_price, five_bullish, high_break
        )
        self._open_pv273_confirmed_shadow(
            symbol, details, setup_id, live_entry_price, closed_price, five_bullish, high_break
        )
        self._open_pv274_confirmed_shadow(
            symbol, details, setup_id, live_entry_price, closed_price, five_bullish, high_break
        )
        self._open_pv2741_confirmed_shadow(
            symbol, details, setup_id, live_entry_price, closed_price, five_bullish, high_break
        )
        # 4.2 신규는 동결. 4.3/4.6(entry filter), 4.4/4.5(BE)를 같은 V25 기회에서 병렬 관찰한다.
        self._open_pv2743_confirmed_shadow(
            symbol, details, setup_id, live_entry_price, closed_price, five_bullish, high_break
        )
        self._open_pv2746_confirmed_shadow(
            symbol, details, setup_id, live_entry_price, closed_price, five_bullish, high_break
        )
        self._open_pv2744_confirmed_shadow(
            symbol, details, setup_id, live_entry_price, closed_price, five_bullish, high_break
        )
        self._open_pv2745_confirmed_shadow(
            symbol, details, setup_id, live_entry_price, closed_price, five_bullish, high_break
        )
        if not self.cfg.research_pv25_control_enabled:
            with db() as conn:
                conn.execute(
                    "UPDATE research_pv25_setups SET status='CONFIRMED',confirmed_at=?,confirmed_price=?,last_seen_at=?,note=? WHERE setup_id=?",
                    (now.isoformat(),live_entry_price,now.isoformat(),"confirmed_5m_break_live_entry",setup_id),
                )

    def update_research_shadow_reviews(self) -> None:
        """v4.3.58: 연구용 P/준P/near-miss Shadow를 현재 P 관리규칙으로 가상 추적한다.

        TP1/TP2/BE/STOP/TIME/FLAT 결과와 MFE/MAE를 남긴다. 여러 variant가 같은 symbol이면
        ticker는 공유해 API 호출량을 줄인다.
        """
        self._backfill_pv26_stop_ghosts()
        self._backfill_pv274_stop_ghosts()
        self._backfill_pv2741_stop_ghosts()
        self._backfill_pv2742_stop_ghosts()
        self._backfill_pv2743_stop_ghosts()
        self._backfill_pv2746_stop_ghosts()
        with db() as conn:
            if self.cfg.research_junp_variants_enabled:
                pending = conn.execute(
                    "SELECT * FROM research_shadow_reviews WHERE completed=0 ORDER BY opened_at"
                ).fetchall()
            else:
                pending = conn.execute(
                    "SELECT * FROM research_shadow_reviews WHERE completed=0 AND variant NOT LIKE 'JUNP_%' ORDER BY opened_at"
                ).fetchall()
        if not pending:
            return

        symbols = sorted({str(r["symbol"] or "") for r in pending if str(r["symbol"] or "")})
        ticker_map: dict[str, float] = {}
        try:
            for tk in self.client.tickers("SWAP"):
                sk = str(tk.get("symbol") or "")
                if sk not in symbols:
                    continue
                lv = tk.get("lastPrice")
                if lv in (None, ""):
                    lv = tk.get("last")
                try:
                    ticker_map[sk] = float(lv or 0)
                except (TypeError, ValueError):
                    ticker_map[sk] = 0.0
        except Exception:
            pass
        for symbol in symbols:
            if float(ticker_map.get(symbol) or 0) > 0:
                continue
            try:
                one = self.client.ticker(symbol)
                lv = one.get("last")
                if lv in (None, ""):
                    lv = one.get("lastPrice")
                ticker_map[symbol] = float(lv or 0)
            except Exception as exc:
                append_entry_record(symbol, "RESEARCH_TICKER_ERROR", "RESEARCH", 0, 0, f"{type(exc).__name__}: {exc}")

        now = datetime.now(timezone.utc)
        # 가장 최근 1분 스캔에서 만든 시장 snapshot을 exit/TP telemetry에도 재사용한다.
        # 여기서는 BTC/ETH API를 추가 호출하지 않는다.
        market_snapshot = dict(getattr(self, "_market_snapshot", {}) or {})
        bucket_5m = str(int(now.timestamp()) // 300)
        bucket_15m = str(int(now.timestamp()) // 900)
        # v4.3.69: 5분 버킷마다 최근 5개 확정 1분봉을 한 번만 받아 모든 variant가 공유한다.
        # high만 보던 이전 방식은 짧은 TP2 터치를 놓쳤으므로 high/low와 분봉 순서를 함께 보존한다.
        due_symbols = {str(r["symbol"] or "") for r in pending if str(r["last_5m_bucket"] or "") != bucket_5m}
        observed_high_map: dict[str, float] = {}
        observed_low_map: dict[str, float] = {}
        minute_bars_map: dict[str, list[dict[str, float]]] = {}
        for symbol in sorted(due_symbols):
            try:
                m1 = self.client.candles(symbol, "1m", 7)
                cm1 = confirmed(m1) if m1 is not None and len(m1) > 0 else m1
                if cm1 is not None and len(cm1) > 0 and {"open","high","low","close"}.issubset(cm1.columns):
                    tail = cm1.tail(5)
                    bars: list[dict[str, float]] = []
                    for _, br in tail.iterrows():
                        br_ts = ""
                        try:
                            br_ts = br.name.isoformat() if hasattr(br.name, "isoformat") else str(br.name)
                        except Exception:
                            br_ts = ""
                        bars.append({
                            "open": float(br.open), "high": float(br.high),
                            "low": float(br.low), "close": float(br.close), "ts": br_ts,
                        })
                    minute_bars_map[symbol] = bars
                    if bars:
                        observed_high_map[symbol] = max(x["high"] for x in bars)
                        observed_low_map[symbol] = min(x["low"] for x in bars)
            except Exception:
                pass

        # 동일 스캔에서 여러 variant가 동시에 열린 경우 stop/structure API 결과를 공유해 rate-limit을 줄인다.
        stop_cache: dict[tuple[Any, ...], tuple[bool, str, dict[str, Any]]] = {}
        structure_cache: dict[tuple[Any, ...], tuple[bool, dict[str, Any]]] = {}
        flat_cache: dict[tuple[Any, ...], tuple[bool, dict[str, Any]]] = {}
        late26_cache: dict[tuple[Any, ...], tuple[bool, dict[str, Any]]] = {}

        # v4.3.84: 병렬 경로가 같은 symbol/timeframe 데이터를 중복 호출하지 않도록 공유한다.
        parallel_m5_cache: dict[str, pd.DataFrame | None] = {}
        parallel_m15_cache: dict[str, pd.DataFrame | None] = {}
        recent_1m_cache: dict[str, pd.DataFrame | None] = {}

        def _parallel_m5(symbol_: str) -> pd.DataFrame | None:
            if symbol_ not in parallel_m5_cache:
                try:
                    raw = self.client.candles(symbol_, "5m", 80)
                    parallel_m5_cache[symbol_] = confirmed(indicators(raw)) if raw is not None and len(raw) else raw
                except Exception:
                    parallel_m5_cache[symbol_] = None
            return parallel_m5_cache[symbol_]

        def _parallel_m15(symbol_: str) -> pd.DataFrame | None:
            if symbol_ not in parallel_m15_cache:
                try:
                    raw = self.client.candles(symbol_, "15m", 80)
                    parallel_m15_cache[symbol_] = confirmed(indicators(raw)) if raw is not None and len(raw) else raw
                except Exception:
                    parallel_m15_cache[symbol_] = None
            return parallel_m15_cache[symbol_]

        def _recent_1m(symbol_: str, limit_: int) -> pd.DataFrame | None:
            # 병렬 variant가 5/7개 분봉을 각각 재요청하지 않도록 symbol당 7개를 한 번만 공유한다.
            if symbol_ not in recent_1m_cache:
                try:
                    raw = self.client.candles(symbol_, "1m", max(7, int(limit_)))
                    recent_1m_cache[symbol_] = confirmed(raw) if raw is not None and len(raw) else raw
                except Exception:
                    recent_1m_cache[symbol_] = None
            df = recent_1m_cache[symbol_]
            if df is None:
                return None
            try:
                return df.tail(max(1, int(limit_)))
            except Exception:
                return df

        # v4.3.84: 진입이 03:10:22라면 03:10 1분봉에는 진입 전 22초의 high/low도 섞여 있다.
        # 따라서 MFE/MAE/TP/STOP 재생에는 진입분 전체를 쓰지 않고, 03:11:00 이후의
        # '완전히 진입 뒤에 형성된 확정 1분봉'만 사용한다. 현재 ticker 가격은 계속 즉시 반영한다.
        def _bar_open_dt(bar_: dict[str, Any]) -> datetime | None:
            try:
                raw_ts = str(bar_.get("ts") or "")
                if not raw_ts:
                    return None
                dt = datetime.fromisoformat(raw_ts)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except Exception:
                return None

        def _full_post_entry_minute_bars(symbol_: str, opened_: datetime, five_due_: bool) -> list[dict[str, float]]:
            if not five_due_:
                return []
            opened_utc = opened_.astimezone(timezone.utc) if opened_.tzinfo is not None else opened_.replace(tzinfo=timezone.utc)
            first_full_minute = opened_utc.replace(second=0, microsecond=0) + timedelta(minutes=1)
            out: list[dict[str, float]] = []
            for bar in list(minute_bars_map.get(symbol_) or []):
                bar_dt = _bar_open_dt(bar)
                # timestamp를 확인할 수 없는 봉은 보수적으로 제외한다.
                # 그래야 진입 전 high/low가 손절/TP 판정에 섞이지 않는다.
                if bar_dt is not None and bar_dt >= first_full_minute:
                    out.append(bar)
            return out

        for review in pending:
            symbol = str(review["symbol"] or "")
            variant = str(review["variant"] or "")
            is_pv26_stop_ghost = variant == "P_V26_STOP_GHOST"
            is_pv274_stop_ghost = variant == "P_V27_4_STOP_GHOST"
            is_pv2741_stop_ghost = variant == "P_V27_4_1_STOP_GHOST"
            is_pv2742_stop_ghost = variant == "P_V27_4_2_STOP_GHOST"
            is_pv2743_stop_ghost = variant == "P_V27_4_3_STOP_GHOST"
            is_pv2746_stop_ghost = variant == "P_V27_4_6_STOP_GHOST"
            is_pv274_block_ghost = variant == "P_V27_4_BLOCK_GHOST"
            is_raw_path_ghost = bool(is_pv26_stop_ghost or is_pv274_stop_ghost or is_pv2741_stop_ghost or is_pv2742_stop_ghost or is_pv2743_stop_ghost or is_pv2746_stop_ghost or is_pv274_block_ghost)
            # V27-1 계열 + v4.3.83 병렬 연구경로는 같은 staged/disaster + 1R recovery 골격을 공유한다.
            # 새 병렬 경로는 LIVE 주문과 완전히 분리된 research-only Shadow다.
            parallel_stop_variants = {
                "P_STOP_CONTROL","P_STOP_EARLY50","P_STOP_DISASTER50","P_STOP_LATE50","P_STOP_COMBO",
            }
            parallel_entry_variants = {
                "P_ENTRY_CONTROL","P_ENTRY_A","P_ENTRY_B","P_ENTRY_C3","P_ENTRY_C5","P_ENTRY_COMBO","P_ENTRY_46",
            }
            is_parallel_stop = variant in parallel_stop_variants
            is_parallel_entry = variant in parallel_entry_variants
            is_parallel_recovery = bool(is_parallel_stop or is_parallel_entry)
            final_new_variants = {"P_FWD_CORE", "P_FWD_MKT50", "P_FWD_MKT100", "P_FWD_FIXED_SHADOW", "P_MANUAL_SHADOW"}
            is_final_new = variant in final_new_variants
            is_final_control = variant == "P_FWD_CONTROL"
            # Final Forward CONTROL/New 모두 V27-1 fallback stop 골격을 공유한다.
            # New 3경로는 아래에서 TP1.8 full + 4단 손절 overlay만 별도로 적용한다.
            is_v271 = bool(variant == "P_V27_1" or is_final_control or is_final_new)
            is_v271r = bool(variant == "P_V27_1R" or is_parallel_recovery)
            is_pv2745 = variant == "P_V27_4_5"
            is_v271_like = bool(is_v271 or is_v271r or is_pv2745)
            is_pv2744 = variant == "P_V27_4_4"
            try:
                try:
                    review_snap = json.loads(str(review["snapshot_json"] or "{}"))
                except Exception:
                    review_snap = {}
                v271_stage = dict(review_snap.get("v271_stop_stage") or {}) if is_v271_like else {}
                v271_stage_active = bool(v271_stage.get("active"))
                v271r_recovery = dict(review_snap.get("v271r_recovery") or {}) if is_v271r else {}
                v271r_recovery_active = bool(v271r_recovery.get("active"))
                v271r_recovery_confirmed = bool(v271r_recovery.get("confirmed"))
                v2744_be_partial_done = bool(review_snap.get("p_v2744_be_partial_done")) if is_pv2744 else False
                v2745_be_partial_done = bool(review_snap.get("p_v2745_be_partial_done")) if is_pv2745 else False
                v2745_be_recovery = dict(review_snap.get("p_v2745_be_recovery") or {}) if is_pv2745 else {}
                v2745_be_recovery_active = bool(v2745_be_recovery.get("active"))
                price = float(ticker_map.get(symbol) or 0)
                if price <= 0:
                    continue
                entry_price = float(review["entry_price"] or 0)
                opened_at = str(review["opened_at"])
                tp1_done = bool(int(review["tp1_done"] or 0))
                v26_late_fail_streak = int(review["v26_late_fail_streak"] or 0)
                if is_final_new:
                    # 각 Final Shadow가 진입 snapshot에 저장한 TP를 우선 사용한다.
                    # 기존 자동 Final Forward는 +1.8%, P_MANUAL_SHADOW는 +2.0%로 독립 검증한다.
                    final_tp_pct = float(
                        review_snap.get("final_tp_pct")
                        if review_snap.get("final_tp_pct") not in (None, "")
                        else self.cfg.research_final_tp_pct
                    )
                    tp1_price = entry_price * (1 + final_tp_pct / 100)
                    tp2_price = tp1_price  # Final New 계열은 전량익절 단일 이벤트로 종료한다.
                    tp_full_result = "TP20_FULL" if variant == "P_MANUAL_SHADOW" else "TP18_FULL"
                    tp_full_price_key = "tp20_price" if variant == "P_MANUAL_SHADOW" else "tp18_price"
                else:
                    tp1_price = entry_price * (1 + float(self.cfg.tp1_pct) / 100)
                    tp2_price = float(review["tp2_price"] or entry_price * (1 + float(self.cfg.tp2_pct) / 100))
                be_price = float(review["be_price"] or entry_price * (1 + float(self.cfg.breakeven_stop_pct) / 100))
                opened = datetime.fromisoformat(opened_at)
                if opened.tzinfo is None:
                    opened = opened.replace(tzinfo=timezone.utc)
                age_min = (now - opened).total_seconds() / 60.0
                five_due = str(review["last_5m_bucket"] or "") != bucket_5m
                fifteen_due = str(review["last_15m_bucket"] or "") != bucket_15m

                # v4.3.84 intraminute guard:
                # 같은 1분봉의 진입 전 high/low가 경로에 섞이지 않도록 진입분을 통째로 제외한다.
                # 현재 ticker와 DB에 이미 저장된 진입 이후 관측값은 그대로 반영한다.
                minute_bars = _full_post_entry_minute_bars(symbol, opened, five_due)
                post_entry_bar_high = max((float(x["high"]) for x in minute_bars), default=price)
                post_entry_bar_low = min((float(x["low"]) for x in minute_bars), default=price)
                highest = max(float(review["highest_price"] or entry_price or price), price, post_entry_bar_high)
                lowest = min(float(review["lowest_price"] or entry_price or price), price, post_entry_bar_low)
                mfe_pct = (highest / entry_price - 1) * 100 if entry_price > 0 else 0.0
                mae_pct = (lowest / entry_price - 1) * 100 if entry_price > 0 else 0.0
                observed_high = highest

                result = ""
                result_price = price
                result_details: dict[str, Any] = {}

                # V27-4.1 weak-watch: 추가 API 호출 없이 현재 ticker/MFE/MAE만 5/10/15분 체크포인트로 남긴다.
                if variant == "P_V27_4_1" and bool(review_snap.get("p_v2741_weak_watch")):
                    logged = list(review_snap.get("p_v2741_watch_logged") or [])
                    added = False
                    for minute in tuple(self.cfg.research_pv2741_watch_checkpoints_min):
                        key = int(minute)
                        if age_min >= key and key not in logged:
                            logged.append(key)
                            added = True
                            append_entry_record(
                                symbol,f"RESEARCH_P_V27_4_1_WEAK_WATCH_{key}M","P_V27_4_1",
                                float(review["new_p_score"] or 0),price,
                                f"pct_from_entry={(price/entry_price-1)*100:.3f}%; actual_order=0",
                                extra={"shadow_id":str(review["shadow_id"] or ""),"research_variant":"P_V27_4_1",
                                       "p_v2741_weak_watch":True,"p_v2741_weak_count":int(review_snap.get("p_v2741_weak_count") or 0),
                                       "p_v2741_weak_flags":str(review_snap.get("p_v2741_weak_flags") or ""),
                                       "shadow_age_min":round(age_min,1),"mfe_pct":round(mfe_pct,4),"mae_pct":round(mae_pct,4),
                                       "pct_from_entry":round((price/entry_price-1)*100,4)},
                            )
                    if added:
                        review_snap["p_v2741_watch_logged"] = logged
                        with db() as conn:
                            conn.execute("UPDATE research_shadow_reviews SET snapshot_json=? WHERE id=?",
                                         (json.dumps(review_snap,ensure_ascii=False,default=str),int(review["id"])))

                if is_raw_path_ghost:
                    try:
                        ghost_snap = json.loads(str(review["snapshot_json"] or "{}"))
                    except Exception:
                        ghost_snap = {}
                    milestones = dict(ghost_snap.get("raw_path_milestones") or ghost_snap.get("stop_ghost_milestones") or {})
                    backfilled_ghost = bool(ghost_snap.get("stop_ghost_backfilled"))
                    track_minutes = float(
                        self.cfg.research_pv26_stop_ghost_minutes
                        if is_pv26_stop_ghost
                        else (
                            self.cfg.research_pv2741_post_track_minutes
                            if is_pv2741_stop_ghost
                            else (
                                self.cfg.research_pv2742_post_track_minutes if is_pv2742_stop_ghost
                                else (self.cfg.research_pv2743_post_track_minutes if is_pv2743_stop_ghost
                                      else (self.cfg.research_pv2746_post_track_minutes if is_pv2746_stop_ghost else self.cfg.research_pv274_post_track_minutes))
                            )
                        )
                    )
                    ghost_type_field = (
                        "p_v26_ghost_type" if is_pv26_stop_ghost
                        else ("p_v2741_ghost_type" if is_pv2741_stop_ghost
                              else ("p_v2742_ghost_type" if is_pv2742_stop_ghost
                                    else ("p_v2743_ghost_type" if is_pv2743_stop_ghost
                                          else ("p_v2746_ghost_type" if is_pv2746_stop_ghost else "p_v274_ghost_type"))))
                    )
                    ghost_extra = {ghost_type_field: variant}
                    if is_pv2741_stop_ghost:
                        ghost_extra["p_v2741_source_shadow_id"] = str(ghost_snap.get("p_v2741_source_shadow_id") or "")
                    elif is_pv2742_stop_ghost:
                        ghost_extra["p_v2742_source_shadow_id"] = str(ghost_snap.get("p_v2742_source_shadow_id") or "")
                    elif is_pv2743_stop_ghost:
                        ghost_extra["p_v2743_source_shadow_id"] = str(ghost_snap.get("p_v2743_source_shadow_id") or "")
                    elif is_pv2746_stop_ghost:
                        ghost_extra["p_v2746_source_shadow_id"] = str(ghost_snap.get("p_v2746_source_shadow_id") or "")
                    elif not is_pv26_stop_ghost:
                        ghost_extra["p_v274_source_shadow_id"] = str(ghost_snap.get("p_v274_source_shadow_id") or "")
                    milestone_added = False
                    for minute in (15, 30, 60, 120, 180):
                        if float(minute) > track_minutes:
                            continue
                        key = str(minute)
                        if age_min >= minute and key not in milestones:
                            # backfill ghost가 이미 오래 지난 경우 현재가를 과거 시점 가격처럼 쓰지 않는다.
                            if backfilled_ghost and age_min > minute + 2:
                                milestones[key] = {
                                    "price": None, "pct_from_entry": None,
                                    "recorded_at": utc_now(), "note": "milestone_passed_before_stoptrace_patch",
                                }
                                milestone_added = True
                                continue
                            milestones[key] = {
                                "price": round(price, 12),
                                "pct_from_entry": round((price / entry_price - 1.0) * 100.0, 4),
                                "recorded_at": utc_now(),
                            }
                            milestone_added = True
                            append_entry_record(
                                symbol,f"RESEARCH_{variant}_{minute}M",variant,
                                float(review["new_p_score"] or 0),price,
                                f"pct_from_entry={milestones[key]['pct_from_entry']:.3f}%; actual_order=0",
                                extra={"shadow_id":str(review["shadow_id"] or ""),"research_variant":variant,
                                       "shadow_age_min":round(age_min,1),"mfe_pct":round(mfe_pct,4),"mae_pct":round(mae_pct,4),
                                       **ghost_extra},
                            )
                    if milestone_added:
                        ghost_snap["raw_path_milestones"] = milestones
                        with db() as conn:
                            conn.execute(
                                "UPDATE research_shadow_reviews SET snapshot_json=? WHERE id=?",
                                (json.dumps(ghost_snap,ensure_ascii=False,default=str),int(review["id"])),
                            )

                    if not tp1_done and observed_high >= tp1_price:
                        tp1_done = True
                        with db() as conn:
                            conn.execute(
                                "UPDATE research_shadow_reviews SET tp1_done=1,tp1_ts=?,tp1_price=? WHERE id=?",
                                (utc_now(),tp1_price,int(review["id"])),
                            )
                        append_entry_record(
                            symbol,f"RESEARCH_{variant}_TP1_REACHED",variant,
                            float(review["new_p_score"] or 0),tp1_price,"actual_order=0",
                            extra={"shadow_id":str(review["shadow_id"] or ""),"research_variant":variant,
                                   "mfe_pct":round(mfe_pct,4),"mae_pct":round(mae_pct,4),"shadow_age_min":round(age_min,1),
                                   **ghost_extra},
                        )

                    if observed_high >= tp2_price:
                        result_price = tp2_price
                        if is_pv274_block_ghost:
                            result = "ENTRY_GHOST_TP2"
                            result_details = {
                                "classification":"BLOCKED_TP2_CAPABLE","tp1_reached":True,"tp2_reached":True,
                                "milestones":milestones,
                            }
                        else:
                            result = "STOP_GHOST_TP2_RECOVERED"
                            result_details = {
                                "classification":"FALSE_STOP_TP2","recovered_entry":True,
                                "tp1_recovered":True,"tp2_recovered":True,"milestones":milestones,
                            }
                    elif age_min >= track_minutes:
                        if is_pv274_block_ghost:
                            if tp1_done:
                                result = "ENTRY_GHOST_TP1_180M"
                                classification = "BLOCKED_TP1_CAPABLE"
                            else:
                                result = "ENTRY_GHOST_NO_TP1_180M"
                                classification = "BLOCKED_TRUE_FAILURE"
                            result_details = {
                                "classification":classification,"tp1_reached":bool(tp1_done),"tp2_reached":False,
                                "milestones":milestones,
                            }
                        else:
                            recovered_entry = bool(highest >= entry_price)
                            if tp1_done:
                                classification = "FALSE_STOP_TP1"
                                result = "STOP_GHOST_FALSE_STOP"
                            elif recovered_entry:
                                classification = "RECOVERED_ENTRY_ONLY"
                                result = "STOP_GHOST_RECOVERED_ENTRY"
                            else:
                                classification = "VALID_STOP"
                                result = "STOP_GHOST_VALID_STOP"
                            result_details = {
                                "classification":classification,"recovered_entry":recovered_entry,
                                "tp1_recovered":bool(tp1_done),"tp2_recovered":False,"milestones":milestones,
                            }
                    if result and not is_pv26_stop_ghost:
                        result_details["p_v274_ghost_type"] = variant
                        result_details["p_v274_ghost_classification"] = str(result_details.get("classification") or "")
                else:
                    tp1_was_done = tp1_done
                    # minute_bars는 위에서 v4.3.84 intraminute guard를 적용한
                    # '진입 다음 완성 1분봉부터'의 경로만 재사용한다.

                    # v4.3.89 Final Forward + Manual P Shadow 공통 4-stage stop overlay.
                    # 10m DEAD: pnl<=-1.0 & MFE<=+0.10 => remaining의 50% 축소
                    # 15m: pnl<=-1.5 => remaining 전량 종료
                    # 25m GIVEBACK: 0.3<=MFE<=1.2 & pnl<=-1.2 => remaining의 50% 축소
                    # 30m DETERIORATION: pnl<=-0.9 & 25m대비 5분 추가하락<=-0.3p => remaining 전량 종료
                    # 그 외에는 아래 기존 V27-1 stop 엔진이 fallback으로 계속 관리한다.
                    if is_final_new and not result and not tp1_done and observed_high < tp1_price:
                        pnl_now = (price / entry_price - 1.0) * 100.0 if entry_price > 0 else 0.0
                        final_remaining = float(review_snap.get("final_remaining_frac") if review_snap.get("final_remaining_frac") not in (None, "") else 1.0)
                        final_realized = float(review_snap.get("final_realized_weighted_pct") or 0.0)

                        def _final_save_snapshot() -> None:
                            with db() as conn:
                                conn.execute(
                                    "UPDATE research_shadow_reviews SET snapshot_json=? WHERE id=?",
                                    (json.dumps(review_snap, ensure_ascii=False, default=str), int(review["id"])),
                                )

                        def _final_partial(stage_name: str, fraction_of_remaining: float) -> None:
                            nonlocal final_remaining, final_realized
                            frac_of_rem = min(1.0, max(0.0, float(fraction_of_remaining)))
                            close_frac = final_remaining * frac_of_rem
                            if close_frac <= 0:
                                return
                            final_realized += close_frac * pnl_now
                            final_remaining = max(0.0, final_remaining - close_frac)
                            review_snap["final_remaining_frac"] = round(final_remaining, 6)
                            review_snap["final_realized_weighted_pct"] = round(final_realized, 6)
                            review_snap["final_stop_stage"] = stage_name
                            _final_save_snapshot()
                            append_entry_record(
                                symbol, f"RESEARCH_{variant}_{stage_name}", variant,
                                float(review["new_p_score"] or 0), price,
                                f"partial close={close_frac:.3f} of original; pnl={pnl_now:.3f}%; actual_order=0",
                                extra={
                                    "shadow_id": str(review["shadow_id"] or ""), "research_variant": variant,
                                    "final_fwd_variant": variant, "final_market_mode": str(review_snap.get("final_market_mode") or ""),
                                    "final_size_mult": float(review_snap.get("final_size_mult") or 1.0),
                                    "final_stop_stage": stage_name, "final_remaining_frac": round(final_remaining, 6),
                                    "final_realized_weighted_pct": round(final_realized, 6),
                                    "mfe_pct": round(mfe_pct, 4), "mae_pct": round(mae_pct, 4), "shadow_age_min": round(age_min, 2),
                                    **market_snapshot,
                                },
                            )

                        # v4.3.88 FIXED_SHADOW PROFIT_PROTECT.
                        # +1.2% MFE를 한 번이라도 확인하면 ARM. 그 뒤 확정 5분봉 종가가
                        # 진입가 대비 -1.15% 이하이면 남은 물량 전량 보호종료한다.
                        # FINAL NEW 공통이 아니라 P_FWD_FIXED_SHADOW에만 적용한다.
                        if variant == "P_FWD_FIXED_SHADOW" and not result:
                            pp_armed = bool(review_snap.get("fixed_pp_armed"))
                            if (not pp_armed) and mfe_pct >= float(self.cfg.research_fixed_pp_arm_mfe_pct):
                                pp_armed = True
                                review_snap["fixed_pp_armed"] = True
                                review_snap["fixed_pp_arm_mfe_pct"] = round(mfe_pct, 4)
                                review_snap["fixed_pp_arm_ts"] = utc_now()
                                _final_save_snapshot()
                                append_entry_record(
                                    symbol, "RESEARCH_P_FWD_FIXED_SHADOW_PP12_ARM", "P_FWD_FIXED_SHADOW",
                                    float(review["new_p_score"] or 0), price,
                                    f"MFE={mfe_pct:.3f}% >= {self.cfg.research_fixed_pp_arm_mfe_pct:.2f}%; actual_order=0",
                                    extra={
                                        "shadow_id":str(review["shadow_id"] or ""),
                                        "research_variant":"P_FWD_FIXED_SHADOW",
                                        "final_fwd_variant":"P_FWD_FIXED_SHADOW",
                                        "fixed_pp_armed":True,
                                        "fixed_pp_arm_mfe_pct":round(mfe_pct,4),
                                        "mfe_pct":round(mfe_pct,4), "mae_pct":round(mae_pct,4),
                                        "shadow_age_min":round(age_min,2), **market_snapshot,
                                    },
                                )

                            if pp_armed and five_due and not result:
                                m5_pp = _parallel_m5(symbol)
                                if m5_pp is not None and len(m5_pp) > 0:
                                    close5 = float(m5_pp.iloc[-1].close)
                                    high5 = float(m5_pp.iloc[-1].high)
                                    close5_pct = (close5 / entry_price - 1.0) * 100.0 if entry_price > 0 else 0.0
                                    review_snap["fixed_pp_last_close_pct"] = round(close5_pct, 4)
                                    review_snap["fixed_pp_last_close_price"] = close5
                                    review_snap["fixed_pp_last_high_price"] = high5
                                    # 같은 확정 5분봉 안에서 +1.8% TP가 한 번이라도 닿았다면
                                    # 거래소 TP가 먼저 체결됐어야 하므로 PP가 그 TP를 덮어쓰지 않는다.
                                    pp_bar_hit_tp = bool(high5 >= tp1_price)
                                    if (not pp_bar_hit_tp) and close5_pct <= float(self.cfg.research_fixed_pp_close_pct):
                                        result, result_price = "PROFIT_PROTECT_EXIT", close5
                                        review_snap["fixed_pp_triggered"] = True
                                        review_snap["final_stop_stage"] = "PP12_CLOSE_M115_FULL"
                                        result_details = {
                                            "stop_type":"PP12_CLOSE_M115_FULL",
                                            "final_stop_stage":"PP12_CLOSE_M115_FULL",
                                            "fixed_pp_armed":True,
                                            "fixed_pp_arm_mfe_pct":review_snap.get("fixed_pp_arm_mfe_pct"),
                                            "fixed_pp_last_close_pct":round(close5_pct,4),
                                            "fixed_pp_triggered":True,
                                        }
                                    _final_save_snapshot()

                        # 10분 체크포인트는 최초 1회만 판정한다.
                        if not result and age_min >= 10.0 and not bool(review_snap.get("final_checkpoint10_done")):
                            review_snap["final_checkpoint10_done"] = True
                            review_snap["final_checkpoint10_pnl_pct"] = round(pnl_now, 4)
                            review_snap["final_checkpoint10_mfe_pct"] = round(mfe_pct, 4)
                            if pnl_now <= float(self.cfg.research_final_10m_pnl_pct) and mfe_pct <= float(self.cfg.research_final_10m_mfe_max_pct):
                                _final_partial("STOP10_DEAD50", 0.50)
                            else:
                                _final_save_snapshot()

                        # 15분까지 -1.5% 아래면 남은 물량 전량 종료.
                        if not result and age_min >= 15.0 and not bool(review_snap.get("final_checkpoint15_done")):
                            review_snap["final_checkpoint15_done"] = True
                            review_snap["final_checkpoint15_pnl_pct"] = round(pnl_now, 4)
                            if pnl_now <= float(self.cfg.research_final_15m_pnl_pct):
                                result, result_price = "STOP", price
                                review_snap["final_stop_stage"] = "STOP15_FULL"
                                result_details = {"stop_type":"FINAL_STOP15_FULL", "final_stop_stage":"STOP15_FULL"}
                            _final_save_snapshot()

                        # 25분: 초반에는 조금 갔지만 되밀린 GIVEBACK만 remaining 50% 축소.
                        if not result and age_min >= 25.0 and not bool(review_snap.get("final_checkpoint25_done")):
                            review_snap["final_checkpoint25_done"] = True
                            review_snap["final_checkpoint25_pnl_pct"] = round(pnl_now, 4)
                            review_snap["final_checkpoint25_mfe_pct"] = round(mfe_pct, 4)
                            if (
                                float(self.cfg.research_final_25m_mfe_min_pct) <= mfe_pct <= float(self.cfg.research_final_25m_mfe_max_pct)
                                and pnl_now <= float(self.cfg.research_final_25m_pnl_pct)
                            ):
                                _final_partial("STOP25_GIVEBACK50", 0.50)
                            else:
                                _final_save_snapshot()

                        # 30분: 25분 체크 이후 5분간 추가로 -0.3%p 이상 밀리면 remaining 종료.
                        if not result and age_min >= 30.0 and not bool(review_snap.get("final_checkpoint30_done")):
                            review_snap["final_checkpoint30_done"] = True
                            review_snap["final_checkpoint30_pnl_pct"] = round(pnl_now, 4)
                            p25 = review_snap.get("final_checkpoint25_pnl_pct")
                            try:
                                delta5 = pnl_now - float(p25)
                            except (TypeError, ValueError):
                                delta5 = None
                            review_snap["final_checkpoint30_delta5_pct"] = (round(delta5, 4) if delta5 is not None else None)
                            if (
                                pnl_now <= float(self.cfg.research_final_30m_pnl_pct)
                                and delta5 is not None
                                and delta5 <= float(self.cfg.research_final_30m_delta5_pct)
                            ):
                                result, result_price = "STOP", price
                                review_snap["final_stop_stage"] = "STOP30_DETERIORATION"
                                result_details = {
                                    "stop_type":"FINAL_STOP30_DETERIORATION", "final_stop_stage":"STOP30_DETERIORATION",
                                    "final_delta5_pct": round(delta5, 4),
                                }
                            _final_save_snapshot()

                    # P_V27_4_5 BE Recovery 관리.
                    # TP1 50% + BE에서 원포지션 25%는 이미 확보된 상태이며 마지막 25%만 관리한다.
                    if is_pv2745 and v2745_be_recovery_active:
                        recovery_target = entry_price * (1 + float(self.cfg.research_pv2745_recovery_target_pct) / 100)
                        recovery_hard = entry_price * (1 - abs(float(self.cfg.research_pv2745_recovery_hard_stop_pct)) / 100)
                        try:
                            recovery_started = datetime.fromisoformat(str(v2745_be_recovery.get("started_at") or utc_now()))
                            if recovery_started.tzinfo is None:
                                recovery_started = recovery_started.replace(tzinfo=timezone.utc)
                        except Exception:
                            recovery_started = now
                        recovery_age = (now - recovery_started).total_seconds() / 60.0

                        def _finish_pv2745_recovery(final_price: float, reason: str) -> None:
                            nonlocal result, result_price, result_details, v2745_be_recovery_active
                            final_pct = (float(final_price) / entry_price - 1.0) * 100.0
                            partial_frac = float(review_snap.get("p_v2745_be_partial_original_fraction") or 0.25)
                            final_frac = float(review_snap.get("p_v2745_final_original_fraction") or 0.25)
                            strategy_gross = (
                                0.50 * float(self.cfg.tp1_pct)
                                + partial_frac * float(self.cfg.breakeven_stop_pct)
                                + final_frac * final_pct
                            )
                            result = "BE_RECOVERY_EXIT"
                            result_price = float(final_price)
                            result_details = {
                                "p_v2745_finish_reason": reason,
                                "p_v2745_be_partial_done": True,
                                "p_v2745_be_partial_price": review_snap.get("p_v2745_be_partial_price"),
                                "p_v2745_be_partial_original_fraction": partial_frac,
                                "p_v2745_final_original_fraction": final_frac,
                                "p_v2745_be_recovery_exit_pct": round(final_pct, 4),
                                "p_v2745_strategy_gross_pct": round(strategy_gross, 4),
                                "p_v2745_recovery_target_pct": float(self.cfg.research_pv2745_recovery_target_pct),
                                "p_v2745_recovery_hard_stop_pct": float(self.cfg.research_pv2745_recovery_hard_stop_pct),
                                "p_v2745_recovery_timeout_minutes": float(self.cfg.research_pv2745_recovery_timeout_minutes),
                                "p_v2745_be_recovery_started_at": str(v2745_be_recovery.get("started_at") or ""),
                            }
                            v2745_be_recovery["active"] = False
                            v2745_be_recovery["finished_at"] = utc_now()
                            v2745_be_recovery["finish_reason"] = reason
                            v2745_be_recovery["exit_price"] = float(final_price)
                            review_snap["p_v2745_be_recovery"] = v2745_be_recovery
                            review_snap["p_v2745_be_recovery_active"] = False
                            review_snap["p_v2745_be_recovery_finish_reason"] = reason
                            with db() as conn:
                                conn.execute(
                                    "UPDATE research_shadow_reviews SET snapshot_json=? WHERE id=?",
                                    (json.dumps(review_snap,ensure_ascii=False,default=str),int(review["id"])),
                                )
                            v2745_be_recovery_active = False

                        recovery_decision = ""
                        recovery_decision_price = 0.0
                        # 5분 단위로 가져온 완성 1분봉을 BE 발생 다음 봉부터 시간순으로 확인한다.
                        for rb in minute_bars:
                            rb_dt = None
                            try:
                                rb_dt = datetime.fromisoformat(str(rb.get("ts") or ""))
                                if rb_dt.tzinfo is None:
                                    rb_dt = rb_dt.replace(tzinfo=timezone.utc)
                            except Exception:
                                rb_dt = None
                            if rb_dt is not None and rb_dt <= recovery_started:
                                continue
                            hit_hard = float(rb["low"]) <= recovery_hard
                            hit_target = float(rb["high"]) >= recovery_target
                            if hit_hard and hit_target:
                                recovery_decision, recovery_decision_price = "HARD_STOP_AMBIGUOUS_1M", recovery_hard
                                break
                            if hit_hard:
                                recovery_decision, recovery_decision_price = "HARD_STOP_1M", recovery_hard
                                break
                            if hit_target:
                                recovery_decision, recovery_decision_price = "RECOVERY_TARGET_1M", recovery_target
                                break

                        if not recovery_decision:
                            if price <= recovery_hard:
                                recovery_decision, recovery_decision_price = "HARD_STOP_TICKER", recovery_hard
                            elif price >= recovery_target:
                                recovery_decision, recovery_decision_price = "RECOVERY_TARGET_TICKER", recovery_target
                            elif recovery_age >= float(self.cfg.research_pv2745_recovery_timeout_minutes):
                                recovery_decision, recovery_decision_price = "RECOVERY_TIMEOUT", price

                        if recovery_decision:
                            _finish_pv2745_recovery(recovery_decision_price, recovery_decision)

                    # P_V27_1R Recovery Watch / Recovery Confirm 관리.
                    # 첫 50% staged signal은 기존 V27-1과 동일하게 이미 signal_price에서 손절된 것으로 계산한다.
                    elif is_v271r and v271r_recovery_active:
                        first_frac = min(0.95, max(0.05, float(self.cfg.research_pv271_stage_fraction)))
                        first_pct = float(v271r_recovery.get("first_pct") or 0.0)
                        recovery_hard = entry_price * (1 - abs(float(self.cfg.research_pv271r_hard_stop_pct)) / 100)
                        confirm_price = entry_price * (1 - abs(float(self.cfg.research_pv271r_confirm_drawdown_pct)) / 100)
                        recovery_tp1 = entry_price * (1 + float(self.cfg.tp1_pct) / 100)
                        try:
                            recovery_started = datetime.fromisoformat(str(v271r_recovery.get("started_at") or utc_now()))
                            if recovery_started.tzinfo is None:
                                recovery_started = recovery_started.replace(tzinfo=timezone.utc)
                        except Exception:
                            recovery_started = now
                        watch_age = (now - recovery_started).total_seconds() / 60.0
                        confirm_at_raw = str(v271r_recovery.get("confirmed_at") or "")
                        try:
                            confirm_at = datetime.fromisoformat(confirm_at_raw) if confirm_at_raw else None
                            if confirm_at is not None and confirm_at.tzinfo is None:
                                confirm_at = confirm_at.replace(tzinfo=timezone.utc)
                        except Exception:
                            confirm_at = None

                        def _finish_v271r_recovery(second_price: float, reason: str, success: bool = False) -> None:
                            nonlocal result, result_price, result_details, v271r_recovery_active, v271r_recovery_confirmed
                            second_pct = (float(second_price) / entry_price - 1) * 100
                            gross_pct = first_frac * first_pct + (1 - first_frac) * second_pct
                            result_price = entry_price * (1 + gross_pct / 100)
                            result = "RECOVERY_TP1_EXIT" if success else "STOP"
                            result_details = {
                                "stop_type": "V27_1R_STAGED_RECOVERY",
                                "v271r_finish_reason": reason,
                                "v271r_first_fraction": first_frac,
                                "v271r_first_pct": round(first_pct, 4),
                                "v271r_second_pct": round(second_pct, 4),
                                "v271r_total_gross_pct": round(gross_pct, 4),
                                "v271r_recovery_confirmed": bool(v271r_recovery.get("confirmed")),
                                "v271r_recovery_started_at": str(v271r_recovery.get("started_at") or ""),
                                "v271r_recovery_confirmed_at": str(v271r_recovery.get("confirmed_at") or ""),
                                "v271r_watch_minutes": float(self.cfg.research_pv271r_watch_minutes),
                                "v271r_confirm_drawdown_pct": float(self.cfg.research_pv271r_confirm_drawdown_pct),
                                "v271r_hard_stop_pct": float(self.cfg.research_pv271r_hard_stop_pct),
                            }
                            v271r_recovery["active"] = False
                            v271r_recovery["finished_at"] = utc_now()
                            v271r_recovery["finish_reason"] = reason
                            review_snap["v271r_recovery"] = v271r_recovery
                            review_snap["p_v271r_recovery_watch_active"] = False
                            review_snap["p_v271r_recovery_reason"] = reason
                            with db() as conn:
                                conn.execute(
                                    "UPDATE research_shadow_reviews SET snapshot_json=? WHERE id=?",
                                    (json.dumps(review_snap,ensure_ascii=False,default=str),int(review["id"])),
                                )
                            v271r_recovery_active = False

                        if not v271r_recovery_confirmed:
                            recovery_decision = ""
                            recovery_decision_price = 0.0
                            if price <= recovery_hard:
                                recovery_decision, recovery_decision_price = "WATCH_HARD_STOP", recovery_hard
                            elif price >= confirm_price:
                                recovery_decision, recovery_decision_price = "CONFIRMED", confirm_price
                            elif watch_age >= float(self.cfg.research_pv271r_watch_minutes):
                                # 5분 watch 동안 hard/confirm touch 순서를 1분봉으로 소급 확인한다.
                                try:
                                    hist = _recent_1m(symbol, 7)
                                    need = max(1, int(math.ceil(float(self.cfg.research_pv271r_watch_minutes))))
                                    hist = hist.tail(need) if hist is not None else hist
                                    if hist is not None and len(hist) > 0:
                                        for _, hb in hist.iterrows():
                                            hit_hard = float(hb.low) <= recovery_hard
                                            hit_confirm = float(hb.high) >= confirm_price
                                            if hit_hard and hit_confirm:
                                                recovery_decision, recovery_decision_price = "WATCH_HARD_AMBIGUOUS", recovery_hard
                                                break
                                            if hit_hard:
                                                recovery_decision, recovery_decision_price = "WATCH_HARD_STOP_1M", recovery_hard
                                                break
                                            if hit_confirm:
                                                if float(hb.high) >= recovery_tp1:
                                                    recovery_decision, recovery_decision_price = "CONFIRMED_TP1_1M", recovery_tp1
                                                else:
                                                    recovery_decision, recovery_decision_price = "CONFIRMED_1M", confirm_price
                                                break
                                except Exception:
                                    pass
                                if not recovery_decision:
                                    recovery_decision, recovery_decision_price = "WATCH_TIMEOUT", price

                            if recovery_decision.startswith("CONFIRMED"):
                                v271r_recovery["confirmed"] = True
                                v271r_recovery["confirmed_at"] = now.isoformat()
                                v271r_recovery["confirm_price"] = confirm_price
                                review_snap["v271r_recovery"] = v271r_recovery
                                review_snap["p_v271r_recovery_confirmed"] = True
                                review_snap["p_v271r_recovery_reason"] = recovery_decision
                                with db() as conn:
                                    conn.execute(
                                        "UPDATE research_shadow_reviews SET snapshot_json=? WHERE id=?",
                                        (json.dumps(review_snap,ensure_ascii=False,default=str),int(review["id"])),
                                    )
                                append_entry_record(
                                    symbol,f"RESEARCH_{variant}_RECOVERY_CONFIRMED",variant,
                                    float(review["new_p_score"] or 0),confirm_price,
                                    "staged REBOUND recovery confirmed; remaining 50% targets TP1",
                                    extra={"shadow_id":str(review["shadow_id"] or ""),"research_variant":variant,
                                           "parallel_lab_group":str(review_snap.get("parallel_lab_group") or ""),
                                           "parallel_lab_rule":str(review_snap.get("parallel_lab_rule") or ""),
                                           "p_v271r_recovery_watch_active":True,"p_v271r_recovery_confirmed":True,
                                           "p_v271r_recovery_reason":recovery_decision,**market_snapshot},
                                )
                                v271r_recovery_confirmed = True
                                confirm_at = now
                                if recovery_decision == "CONFIRMED_TP1_1M" or price >= recovery_tp1:
                                    _finish_v271r_recovery(recovery_tp1, "RECOVERY_TP1", success=True)
                            elif recovery_decision:
                                _finish_v271r_recovery(recovery_decision_price, recovery_decision, success=False)
                        else:
                            confirm_age = (now - (confirm_at or recovery_started)).total_seconds() / 60.0
                            finish_reason = ""
                            finish_price = 0.0
                            success = False
                            # confirmed 이후에도 -2.60% 재난컷은 계속 유지한다.
                            # recovery confirm 이전 분봉을 뒤섞지 않기 위해 이후 구간은 ticker 관측만 사용한다.
                            if price <= recovery_hard:
                                finish_reason, finish_price = "POST_CONFIRM_HARD_STOP", recovery_hard
                            elif price >= recovery_tp1:
                                finish_reason, finish_price, success = "RECOVERY_TP1", recovery_tp1, True
                            elif confirm_age >= float(self.cfg.research_pv271r_max_hold_minutes):
                                finish_reason, finish_price = "RECOVERY_MAX_HOLD", price
                            if finish_reason:
                                _finish_v271r_recovery(finish_price, finish_reason, success=success)

                    # V27-1 / V27-1R staged stop이 이미 시작됐다면 남은 절반의 반등/재난/3분 타임아웃을 본다.
                    elif is_v271_like and v271_stage_active:
                        signal_price = float(v271_stage.get("signal_price") or entry_price)
                        rebound_target = signal_price * (1 + float(self.cfg.research_pv271_rebound_from_signal_pct) / 100)
                        hard_price = entry_price * (1 - abs(float(self.cfg.research_pv271_disaster_stop_pct)) / 100)
                        try:
                            stage_started = datetime.fromisoformat(str(v271_stage.get("started_at") or utc_now()))
                            if stage_started.tzinfo is None:
                                stage_started = stage_started.replace(tzinfo=timezone.utc)
                        except Exception:
                            stage_started = now
                        stage_age = (now - stage_started).total_seconds() / 60.0
                        second_price = 0.0
                        stage_reason = ""
                        if price <= hard_price:
                            second_price, stage_reason = hard_price, "DISASTER"
                        elif price >= rebound_target:
                            second_price, stage_reason = rebound_target, "REBOUND"
                        elif stage_age >= float(self.cfg.research_pv271_wait_minutes):
                            try:
                                hist = _recent_1m(symbol, 5)
                                need = max(1, int(math.ceil(float(self.cfg.research_pv271_wait_minutes))))
                                hist = hist.tail(need) if hist is not None else hist
                                if hist is not None and len(hist) > 0:
                                    for _, hb in hist.iterrows():
                                        hit_hard = float(hb.low) <= hard_price
                                        hit_rebound = float(hb.high) >= rebound_target
                                        if hit_hard and hit_rebound:
                                            second_price, stage_reason = hard_price, "DISASTER_AMBIGUOUS"
                                            break
                                        if hit_hard:
                                            second_price, stage_reason = hard_price, "DISASTER_1M"
                                            break
                                        if hit_rebound:
                                            second_price, stage_reason = rebound_target, "REBOUND_1M"
                                            break
                            except Exception:
                                pass
                            if second_price <= 0:
                                second_price, stage_reason = price, "TIMEOUT"
                        if second_price > 0:
                            frac = min(0.95, max(0.05, float(self.cfg.research_pv271_stage_fraction)))
                            first_pct = (signal_price / entry_price - 1) * 100
                            # P_V27_1R만 REBOUND에서 남은 50%를 닫지 않고 Recovery Watch로 넘긴다.
                            if is_v271r and stage_reason.startswith("REBOUND"):
                                v271_stage["active"] = False
                                v271_stage["finished_at"] = utc_now()
                                v271_stage["finish_reason"] = stage_reason
                                v271r_recovery = {
                                    "active": True, "confirmed": False, "started_at": now.isoformat(),
                                    "stage_signal_price": signal_price, "rebound_price": second_price,
                                    "stage_reason": stage_reason, "first_fraction": frac,
                                    "first_pct": first_pct,
                                }
                                review_snap["v271_stop_stage"] = v271_stage
                                review_snap["p_v271_stop_stage_active"] = False
                                review_snap["v271r_recovery"] = v271r_recovery
                                review_snap["p_v271r_recovery_watch_active"] = True
                                review_snap["p_v271r_recovery_confirmed"] = False
                                review_snap["p_v271r_recovery_reason"] = stage_reason
                                review_snap["p_v271r_recovery_started_at"] = str(v271r_recovery["started_at"])
                                with db() as conn:
                                    conn.execute(
                                        "UPDATE research_shadow_reviews SET snapshot_json=? WHERE id=?",
                                        (json.dumps(review_snap,ensure_ascii=False,default=str),int(review["id"])),
                                    )
                                append_entry_record(
                                    symbol,f"RESEARCH_{variant}_RECOVERY_WATCH",variant,
                                    float(review["new_p_score"] or 0),second_price,
                                    f"staged {stage_reason}; keep remaining 50%; watch={self.cfg.research_pv271r_watch_minutes}m; confirm=-{self.cfg.research_pv271r_confirm_drawdown_pct}%; hard=-{self.cfg.research_pv271r_hard_stop_pct}%",
                                    extra={"shadow_id":str(review["shadow_id"] or ""),"research_variant":variant,
                                           "parallel_lab_group":str(review_snap.get("parallel_lab_group") or ""),
                                           "parallel_lab_rule":str(review_snap.get("parallel_lab_rule") or ""),
                                           "p_v271r_recovery_watch_active":True,"p_v271r_recovery_confirmed":False,
                                           "p_v271r_recovery_reason":stage_reason,"p_v271r_recovery_started_at":str(v271r_recovery["started_at"]),
                                           **market_snapshot},
                                )
                                v271_stage_active = False
                                v271r_recovery_active = True
                            else:
                                v271_stage["active"] = False
                                v271_stage["finished_at"] = utc_now()
                                v271_stage["finish_reason"] = stage_reason
                                review_snap["v271_stop_stage"] = v271_stage
                                review_snap["p_v271_stop_stage_active"] = False
                                with db() as conn:
                                    conn.execute("UPDATE research_shadow_reviews SET snapshot_json=? WHERE id=?",
                                        (json.dumps(review_snap,ensure_ascii=False,default=str),int(review["id"])))
                                second_pct = (second_price / entry_price - 1) * 100
                                if is_final_new:
                                    rem = float(review_snap.get("final_remaining_frac") if review_snap.get("final_remaining_frac") not in (None, "") else 1.0)
                                    realized = float(review_snap.get("final_realized_weighted_pct") or 0.0)
                                    gross_pct = realized + rem * (frac * first_pct + (1 - frac) * second_pct)
                                else:
                                    gross_pct = frac * first_pct + (1 - frac) * second_pct
                                result, result_price = "STOP", entry_price * (1 + gross_pct / 100)
                                result_details = {
                                    "stop_type": ("V27_1_STAGED_STOP" if is_v271 else ("V27_4_5_V271_STAGED_STOP" if is_pv2745 else "V27_1R_STAGED_STOP")),
                                    "v271_stage_reason": stage_reason,
                                    "v271_first_fraction": frac,
                                    "v271_signal_price": signal_price,
                                    "v271_second_exit_price": second_price,
                                    "v271_first_pct": round(first_pct, 4),
                                    "v271_second_pct": round(second_pct, 4),
                                    "v271_total_gross_pct": round(gross_pct, 4),
                                    "v271_wait_minutes": float(self.cfg.research_pv271_wait_minutes),
                                    "v271_rebound_pct": float(self.cfg.research_pv271_rebound_from_signal_pct),
                                    "v271_engine_variant": variant,
                                    "v271_signal_started_at": str(v271_stage.get("started_at") or ""),
                                    "final_trade_gross_override": (round(gross_pct,4) if is_final_new else None),
                                    "final_stop_stage": ("V271_FALLBACK_STAGE" if is_final_new else ""),
                                }
                    else:
                        # v4.3.83 Parallel STOP Lab.
                        # CONTROL은 기존 V27-1R 그대로. EARLY/DISASTER/LATE/COMBO만 첫 50% Stage1 시점을 달리한다.
                        # COMBO에서는 한 번 Stage1이 시작되면 다른 병렬 손절이 중복 발동하지 않는다.
                        if is_parallel_stop and not tp1_done and not result:
                            pnl_pct_now = (price / entry_price - 1.0) * 100.0 if entry_price > 0 else 0.0
                            parallel_trigger = ""
                            parallel_signal_price = 0.0
                            parallel_details: dict[str, Any] = {}
                            trade_low_candidates = [price]
                            opened_floor = opened.replace(second=0, microsecond=0)
                            for _mb in minute_bars:
                                try:
                                    _ts = datetime.fromisoformat(str(_mb.get("ts") or ""))
                                    if _ts.tzinfo is None:
                                        _ts = _ts.replace(tzinfo=timezone.utc)
                                    if _ts >= opened_floor:
                                        trade_low_candidates.append(float(_mb.get("low") or price))
                                except Exception:
                                    if age_min >= 5.0:
                                        trade_low_candidates.append(float(_mb.get("low") or price))
                            observed_low_now = min(trade_low_candidates)
                            full_hard_price = entry_price * (1 - abs(float(self.cfg.research_pv271_disaster_stop_pct)) / 100)

                            # DISASTER50: -2.5%를 먼저 통과하면 첫 50%를 정확히 -2.5%로 축소.
                            # 같은 관측 구간에서 -3%까지 함께 통과했다면 보수적으로 나머지 50%도 -3% 처리한다.
                            if variant in ("P_STOP_DISASTER50","P_STOP_COMBO"):
                                disaster_stage_price = entry_price * (1 - abs(float(self.cfg.research_parallel_disaster_stage_pct)) / 100)
                                if observed_low_now <= disaster_stage_price:
                                    if observed_low_now <= full_hard_price:
                                        frac = min(0.95, max(0.05, float(self.cfg.research_pv271_stage_fraction)))
                                        first_pct = -abs(float(self.cfg.research_parallel_disaster_stage_pct))
                                        second_pct = -abs(float(self.cfg.research_pv271_disaster_stop_pct))
                                        gross_pct = frac * first_pct + (1 - frac) * second_pct
                                        result = "STOP"
                                        result_price = entry_price * (1 + gross_pct / 100)
                                        result_details = {
                                            "stop_type":"PARALLEL_DISASTER50_DIRECT",
                                            "parallel_lab_stop_trigger":"DISASTER50",
                                            "parallel_first_fraction":frac,
                                            "parallel_first_pct":round(first_pct,4),
                                            "parallel_second_pct":round(second_pct,4),
                                            "parallel_total_gross_pct":round(gross_pct,4),
                                            "source":"shared_1m_low_or_ticker",
                                        }
                                    else:
                                        parallel_trigger = "DISASTER50"
                                        parallel_signal_price = disaster_stage_price
                                        parallel_details = {
                                            "reason":"PARALLEL_DISASTER50",
                                            "drawdown_pct":round(-abs(float(self.cfg.research_parallel_disaster_stage_pct)),3),
                                            "observed_low_pct":round((observed_low_now/entry_price-1)*100,3),
                                        }

                            # EARLY50: 15~45m, -1.5% 이하, MFE<=0.5%, 현재 TIER2형 5m 구조약화.
                            if (
                                not result and not parallel_trigger
                                and variant in ("P_STOP_EARLY50","P_STOP_COMBO")
                                and five_due
                                and float(self.cfg.research_parallel_early_min_age_minutes) <= age_min <= float(self.cfg.research_parallel_early_max_age_minutes)
                                and pnl_pct_now <= -abs(float(self.cfg.research_parallel_early_drawdown_pct))
                                and mfe_pct <= float(self.cfg.research_parallel_early_mfe_max_pct)
                            ):
                                m5p = _parallel_m5(symbol)
                                if m5p is not None and len(m5p) >= 3:
                                    aa, bb, cc = m5p.iloc[-3], m5p.iloc[-2], m5p.iloc[-1]
                                    two_below = bool(float(bb.close) < float(bb.ema9) and float(cc.close) < float(cc.ema9))
                                    ema9_fall = bool(float(cc.ema9) < float(bb.ema9) < float(aa.ema9))
                                    below20 = bool(float(cc.close) < float(cc.ema20) * 0.998)
                                    lower_lows = bool(float(cc.low) < float(bb.low) <= float(aa.low))
                                    rsi_weak = bool(float(cc.rsi) < float(bb.rsi) and float(cc.rsi) <= 50.0)
                                    sscore = int(below20) + int(lower_lows) + int(rsi_weak)
                                    if two_below and ema9_fall and sscore >= 1:
                                        parallel_trigger = "EARLY50"
                                        parallel_signal_price = price
                                        parallel_details = {
                                            "reason":"PARALLEL_EARLY50","age_min":round(age_min,2),
                                            "drawdown_pct":round(pnl_pct_now,3),"mfe_pct":round(mfe_pct,3),
                                            "two_below_ema9":two_below,"ema9_falling":ema9_fall,
                                            "close_below_ema20":below20,"lower_lows":lower_lows,
                                            "rsi_weakening":rsi_weak,"structure_score":sscore,
                                        }

                            # LATE50: 60m+, TP1 미도달, 저-MFE/손실상태 + 15m EMA9/HL/HH 동시 붕괴를 2회 확인.
                            if (
                                not result and not parallel_trigger
                                and variant in ("P_STOP_LATE50","P_STOP_COMBO")
                                and five_due
                                and age_min >= float(self.cfg.research_parallel_late_min_age_minutes)
                                and pnl_pct_now <= float(self.cfg.research_parallel_late_pnl_pct)
                                and mfe_pct <= float(self.cfg.research_parallel_late_mfe_max_pct)
                            ):
                                m15p = _parallel_m15(symbol)
                                late_ok = False
                                late_meta: dict[str, Any] = {}
                                if m15p is not None and len(m15p) >= 3:
                                    aa, bb, cc = m15p.iloc[-3], m15p.iloc[-2], m15p.iloc[-1]
                                    ema9_fall15 = bool(float(cc.ema9) < float(bb.ema9) < float(aa.ema9))
                                    hl_fail = bool(float(cc.low) <= float(bb.low))
                                    hh_fail = bool(float(cc.high) <= float(bb.high))
                                    late_ok = bool(ema9_fall15 and hl_fail and hh_fail)
                                    late_meta = {
                                        "ema9_falling":ema9_fall15,"higher_low_failed":hl_fail,
                                        "higher_high_failed":hh_fail,"pnl_pct":round(pnl_pct_now,3),
                                        "mfe_pct":round(mfe_pct,3),"age_min":round(age_min,2),
                                    }
                                streak = int(review_snap.get("parallel_late50_streak") or 0)
                                streak = streak + 1 if late_ok else 0
                                review_snap["parallel_late50_streak"] = streak
                                review_snap["parallel_lab_late_streak"] = streak
                                with db() as conn:
                                    conn.execute(
                                        "UPDATE research_shadow_reviews SET snapshot_json=? WHERE id=?",
                                        (json.dumps(review_snap,ensure_ascii=False,default=str),int(review["id"])),
                                    )
                                if late_ok and streak >= max(1,int(self.cfg.research_parallel_late_confirmations)):
                                    parallel_trigger = "LATE50"
                                    parallel_signal_price = price
                                    parallel_details = {"reason":"PARALLEL_LATE50","streak":streak,**late_meta}

                            if parallel_trigger and not result:
                                stage = {
                                    "active":True,"started_at":now.isoformat(),"signal_price":parallel_signal_price,
                                    "stop_type":f"PARALLEL_{parallel_trigger}","stop_details":parallel_details,
                                }
                                review_snap["v271_stop_stage"] = stage
                                review_snap["p_v271_stop_stage_active"] = True
                                review_snap["p_v271_stop_signal_price"] = parallel_signal_price
                                review_snap["p_v271_stop_reason"] = f"PARALLEL_{parallel_trigger}"
                                review_snap["parallel_lab_stop_trigger"] = parallel_trigger
                                review_snap["parallel_lab_stop_signal_pct"] = round((parallel_signal_price/entry_price-1)*100,4)
                                with db() as conn:
                                    conn.execute(
                                        "UPDATE research_shadow_reviews SET snapshot_json=? WHERE id=?",
                                        (json.dumps(review_snap,ensure_ascii=False,default=str),int(review["id"])),
                                    )
                                append_entry_record(
                                    symbol,f"RESEARCH_{variant}_STAGE1",variant,float(review["new_p_score"] or 0),parallel_signal_price,
                                    f"parallel {parallel_trigger}; 50% Stage1; wait={self.cfg.research_pv271_wait_minutes}m; 1R enabled",
                                    extra={
                                        "shadow_id":str(review["shadow_id"] or ""),"research_variant":variant,
                                        "parallel_lab_group":"STOP","parallel_lab_rule":str(review_snap.get("parallel_lab_rule") or ""),
                                        "parallel_lab_stop_trigger":parallel_trigger,
                                        "parallel_lab_stop_signal_pct":round((parallel_signal_price/entry_price-1)*100,4),
                                        "parallel_lab_late_streak":int(review_snap.get("parallel_late50_streak") or 0),
                                        **market_snapshot,
                                    },
                                )
                                v271_stage = stage
                                v271_stage_active = True

                        if not result and not v271_stage_active and age_min >= float(self.cfg.max_hold_hours) * 60.0:
                            result = "TIME_EXIT"
                            result_details = {"age_min": round(age_min, 1)}

                        # 최근 5개 확정 1분봉을 시간순으로 읽어 TP2/BE 순서를 최대한 보존한다.
                        hard_price = entry_price * (1 - abs(float(self.cfg.research_pv271_disaster_stop_pct)) / 100) if is_v271_like else 0.0
                        for mb in ([] if (is_v271_like and v271_stage_active) else minute_bars):
                            if result:
                                break
                            if not tp1_done:
                                hit_tp1 = mb["high"] >= tp1_price
                                hit_hard = bool(is_v271_like and mb["low"] <= hard_price)
                                if hit_tp1 and hit_hard:
                                    # 같은 1분봉 안의 순서는 알 수 없어 V27-1에서는 보수적으로 재난선 우선.
                                    result, result_price = "STOP", hard_price
                                    result_details = {"stop_type":("V27_1R_DISASTER" if is_v271r else ("V27_4_5_V271_DISASTER" if is_pv2745 else "V27_1_DISASTER")),"minute_order_ambiguous":True}
                                    break
                                if hit_hard:
                                    result, result_price = "STOP", hard_price
                                    result_details = {"stop_type":("V27_1R_DISASTER" if is_v271r else ("V27_4_5_V271_DISASTER" if is_pv2745 else "V27_1_DISASTER")),"source":"confirmed_1m_low"}
                                    break
                                if hit_tp1:
                                    tp1_done = True
                                    if is_final_new:
                                        # v4.3.87: NEW 3경로는 +1.8% 전량익절을 TP18_FULL 한 줄로만 기록한다.
                                        result, result_price = tp_full_result, tp1_price
                                        result_details = {tp_full_price_key:tp1_price,"tp_full_pct":final_tp_pct,"source":"confirmed_1m_high","final_full_tp":True}
                                        break
                                    if mb["high"] >= tp2_price:
                                        result, result_price = "TP2", tp2_price
                                        result_details = {"tp2_price":tp2_price,"source":"confirmed_1m_high"}
                                        break
                                    # TP1이 처음 체결된 같은 1분봉의 low는 TP1 이전일 수 있으므로 BE 판정에 쓰지 않는다.
                                    continue
                            else:
                                hit_tp2 = mb["high"] >= tp2_price
                                hit_be = mb["low"] <= be_price
                                if is_final_new:
                                    # 이전 v4.3.86에서 tp1_done=1 상태로 남아 있던 미완료 row도 단일 TP18로 정리한다.
                                    if hit_tp2:
                                        result, result_price = tp_full_result, tp2_price
                                        result_details = {tp_full_price_key:tp2_price,"tp_full_pct":final_tp_pct,"source":"confirmed_1m_high","final_full_tp":True}
                                    continue
                                if is_pv2744:
                                    if hit_tp2 and hit_be and not v2744_be_partial_done:
                                        # 같은 1분봉 안 순서는 불명확하므로 보수적으로 BE 부분청산을 먼저 기록하고
                                        # 이 봉에서는 TP2 완주를 인정하지 않는다. 마지막 25%는 다음 관측부터 계속 추적.
                                        v2744_be_partial_done = True
                                        frac_rem = min(0.95,max(0.05,float(self.cfg.research_pv2744_be_close_fraction_of_remaining)))
                                        orig_frac = 0.50 * frac_rem
                                        final_frac = 0.50 * (1.0 - frac_rem)
                                        review_snap.update({
                                            "p_v2744_be_partial_done":True,"p_v2744_be_partial_ts":utc_now(),
                                            "p_v2744_be_partial_price":be_price,"p_v2744_be_partial_original_fraction":round(orig_frac,4),
                                            "p_v2744_final_original_fraction":round(final_frac,4),"p_v2744_be_partial_source":"confirmed_1m_ambiguous",
                                        })
                                        with db() as conn:
                                            conn.execute("UPDATE research_shadow_reviews SET snapshot_json=? WHERE id=?",
                                                (json.dumps(review_snap,ensure_ascii=False,default=str),int(review["id"])))
                                        append_entry_record(symbol,"RESEARCH_P_V27_4_4_BE_PARTIAL","P_V27_4_4",float(review["new_p_score"] or 0),be_price,
                                            "TP1 후 BE: 남은 50%의 절반만 보호청산; final quarter continues; ambiguous 1m",
                                            extra={"shadow_id":str(review["shadow_id"] or ""),"research_variant":"P_V27_4_4",
                                                   "p_v2744_be_partial_done":True,"p_v2744_be_partial_price":be_price,
                                                   "p_v2744_be_partial_original_fraction":round(orig_frac,4),
                                                   "p_v2744_final_original_fraction":round(final_frac,4),**market_snapshot})
                                    elif hit_tp2:
                                        result, result_price = "TP2", tp2_price
                                        result_details = {"tp2_price":tp2_price,"source":"confirmed_1m_high"}
                                    elif hit_be and not v2744_be_partial_done:
                                        v2744_be_partial_done = True
                                        frac_rem = min(0.95,max(0.05,float(self.cfg.research_pv2744_be_close_fraction_of_remaining)))
                                        orig_frac = 0.50 * frac_rem
                                        final_frac = 0.50 * (1.0 - frac_rem)
                                        review_snap.update({
                                            "p_v2744_be_partial_done":True,"p_v2744_be_partial_ts":utc_now(),
                                            "p_v2744_be_partial_price":be_price,"p_v2744_be_partial_original_fraction":round(orig_frac,4),
                                            "p_v2744_final_original_fraction":round(final_frac,4),"p_v2744_be_partial_source":"confirmed_1m_low",
                                        })
                                        with db() as conn:
                                            conn.execute("UPDATE research_shadow_reviews SET snapshot_json=? WHERE id=?",
                                                (json.dumps(review_snap,ensure_ascii=False,default=str),int(review["id"])))
                                        append_entry_record(symbol,"RESEARCH_P_V27_4_4_BE_PARTIAL","P_V27_4_4",float(review["new_p_score"] or 0),be_price,
                                            "TP1 후 BE: 남은 50%의 절반만 보호청산; final quarter continues",
                                            extra={"shadow_id":str(review["shadow_id"] or ""),"research_variant":"P_V27_4_4",
                                                   "p_v2744_be_partial_done":True,"p_v2744_be_partial_price":be_price,
                                                   "p_v2744_be_partial_original_fraction":round(orig_frac,4),
                                                   "p_v2744_final_original_fraction":round(final_frac,4),**market_snapshot})
                                elif is_pv2745:
                                    if hit_tp2 and not hit_be:
                                        result, result_price = "TP2", tp2_price
                                        result_details = {"tp2_price":tp2_price,"source":"confirmed_1m_high"}
                                    elif hit_be and not v2745_be_partial_done:
                                        v2745_be_partial_done = True
                                        frac_rem = min(0.95,max(0.05,float(self.cfg.research_pv2745_be_close_fraction_of_remaining)))
                                        orig_frac = 0.50 * frac_rem
                                        final_frac = 0.50 * (1.0 - frac_rem)
                                        be_started_at = str(mb.get("ts") or utc_now())
                                        try:
                                            be_dt = datetime.fromisoformat(be_started_at)
                                            if be_dt.tzinfo is None:
                                                be_dt = be_dt.replace(tzinfo=timezone.utc)
                                            be_started_at = be_dt.isoformat()
                                        except Exception:
                                            be_started_at = utc_now()
                                        v2745_be_recovery = {
                                            "active":True,"started_at":be_started_at,"source":"confirmed_1m_low",
                                            "target_pct":float(self.cfg.research_pv2745_recovery_target_pct),
                                            "hard_stop_pct":float(self.cfg.research_pv2745_recovery_hard_stop_pct),
                                            "timeout_minutes":float(self.cfg.research_pv2745_recovery_timeout_minutes),
                                        }
                                        review_snap.update({
                                            "p_v2745_be_partial_done":True,"p_v2745_be_partial_ts":be_started_at,
                                            "p_v2745_be_partial_price":be_price,"p_v2745_be_partial_original_fraction":round(orig_frac,4),
                                            "p_v2745_final_original_fraction":round(final_frac,4),
                                            "p_v2745_be_recovery":v2745_be_recovery,"p_v2745_be_recovery_active":True,
                                            "p_v2745_be_partial_source":("confirmed_1m_ambiguous" if hit_tp2 else "confirmed_1m_low"),
                                        })
                                        with db() as conn:
                                            conn.execute("UPDATE research_shadow_reviews SET snapshot_json=? WHERE id=?",
                                                (json.dumps(review_snap,ensure_ascii=False,default=str),int(review["id"])))
                                        append_entry_record(symbol,"RESEARCH_P_V27_4_5_BE_PARTIAL","P_V27_4_5",float(review["new_p_score"] or 0),be_price,
                                            "TP1 후 BE: 원포지션 25% 보호청산; final 25% recovery +1/-2.5/15m",
                                            extra={"shadow_id":str(review["shadow_id"] or ""),"research_variant":"P_V27_4_5",
                                                   "p_v2745_be_partial_done":True,"p_v2745_be_partial_price":be_price,
                                                   "p_v2745_be_partial_original_fraction":round(orig_frac,4),
                                                   "p_v2745_final_original_fraction":round(final_frac,4),
                                                   "p_v2745_be_recovery_active":True,
                                                   "p_v2745_be_recovery_started_at":be_started_at,**market_snapshot})
                                        # BE가 발생한 같은 1분봉은 recovery target/stop 판정에 다시 쓰지 않는다.
                                        v2745_be_recovery_active = True
                                        break
                                else:
                                    if hit_tp2 and hit_be:
                                        result, result_price = "BE_EXIT", be_price
                                        result_details = {"be_price":be_price,"minute_order_ambiguous":True,"conservative":"BE"}
                                    elif hit_tp2:
                                        result, result_price = "TP2", tp2_price
                                        result_details = {"tp2_price":tp2_price,"source":"confirmed_1m_high"}
                                    elif hit_be:
                                        result, result_price = "BE_EXIT", be_price
                                        result_details = {"be_price":be_price,"source":"confirmed_1m_low"}

                        # 분봉 사이의 현재가도 보조 확인한다.
                        if not result and not tp1_done and is_v271_like and price <= hard_price:
                            result, result_price = "STOP", hard_price
                            result_details = {"stop_type":("V27_1R_DISASTER" if is_v271r else ("V27_4_5_V271_DISASTER" if is_pv2745 else "V27_1_DISASTER")),"source":"ticker"}
                        if not result and not tp1_done and price >= tp1_price:
                            tp1_done = True
                            if is_final_new:
                                result, result_price = tp_full_result, tp1_price
                                result_details = {tp_full_price_key:tp1_price,"tp_full_pct":final_tp_pct,"source":"ticker","final_full_tp":True}
                        if not result and tp1_done and not (is_pv2745 and v2745_be_recovery_active) and price >= tp2_price:
                            if is_final_new:
                                result, result_price = tp_full_result, tp2_price
                                result_details = {tp_full_price_key:tp2_price,"tp_full_pct":final_tp_pct,"source":"ticker","final_full_tp":True}
                            else:
                                result, result_price = "TP2", tp2_price
                                result_details = {"tp2_price":tp2_price,"source":"ticker"}
                        if not result and not is_final_new and tp1_done and not (is_pv2745 and v2745_be_recovery_active) and price <= be_price:
                            if is_pv2744:
                                if not v2744_be_partial_done:
                                    v2744_be_partial_done = True
                                    frac_rem = min(0.95,max(0.05,float(self.cfg.research_pv2744_be_close_fraction_of_remaining)))
                                    orig_frac = 0.50 * frac_rem
                                    final_frac = 0.50 * (1.0 - frac_rem)
                                    review_snap.update({
                                        "p_v2744_be_partial_done":True,"p_v2744_be_partial_ts":utc_now(),
                                        "p_v2744_be_partial_price":be_price,"p_v2744_be_partial_original_fraction":round(orig_frac,4),
                                        "p_v2744_final_original_fraction":round(final_frac,4),"p_v2744_be_partial_source":"ticker",
                                    })
                                    with db() as conn:
                                        conn.execute("UPDATE research_shadow_reviews SET snapshot_json=? WHERE id=?",
                                            (json.dumps(review_snap,ensure_ascii=False,default=str),int(review["id"])))
                                    append_entry_record(symbol,"RESEARCH_P_V27_4_4_BE_PARTIAL","P_V27_4_4",float(review["new_p_score"] or 0),be_price,
                                        "TP1 후 BE: 남은 50%의 절반만 보호청산; final quarter continues",
                                        extra={"shadow_id":str(review["shadow_id"] or ""),"research_variant":"P_V27_4_4",
                                               "p_v2744_be_partial_done":True,"p_v2744_be_partial_price":be_price,
                                               "p_v2744_be_partial_original_fraction":round(orig_frac,4),
                                               "p_v2744_final_original_fraction":round(final_frac,4),**market_snapshot})
                            elif is_pv2745:
                                if not v2745_be_partial_done:
                                    v2745_be_partial_done = True
                                    frac_rem = min(0.95,max(0.05,float(self.cfg.research_pv2745_be_close_fraction_of_remaining)))
                                    orig_frac = 0.50 * frac_rem
                                    final_frac = 0.50 * (1.0 - frac_rem)
                                    be_started_at = utc_now()
                                    v2745_be_recovery = {
                                        "active":True,"started_at":be_started_at,"source":"ticker",
                                        "target_pct":float(self.cfg.research_pv2745_recovery_target_pct),
                                        "hard_stop_pct":float(self.cfg.research_pv2745_recovery_hard_stop_pct),
                                        "timeout_minutes":float(self.cfg.research_pv2745_recovery_timeout_minutes),
                                    }
                                    review_snap.update({
                                        "p_v2745_be_partial_done":True,"p_v2745_be_partial_ts":be_started_at,
                                        "p_v2745_be_partial_price":be_price,"p_v2745_be_partial_original_fraction":round(orig_frac,4),
                                        "p_v2745_final_original_fraction":round(final_frac,4),
                                        "p_v2745_be_recovery":v2745_be_recovery,"p_v2745_be_recovery_active":True,
                                        "p_v2745_be_partial_source":"ticker",
                                    })
                                    with db() as conn:
                                        conn.execute("UPDATE research_shadow_reviews SET snapshot_json=? WHERE id=?",
                                            (json.dumps(review_snap,ensure_ascii=False,default=str),int(review["id"])))
                                    append_entry_record(symbol,"RESEARCH_P_V27_4_5_BE_PARTIAL","P_V27_4_5",float(review["new_p_score"] or 0),be_price,
                                        "TP1 후 BE: 원포지션 25% 보호청산; final 25% recovery +1/-2.5/15m",
                                        extra={"shadow_id":str(review["shadow_id"] or ""),"research_variant":"P_V27_4_5",
                                               "p_v2745_be_partial_done":True,"p_v2745_be_partial_price":be_price,
                                               "p_v2745_be_partial_original_fraction":round(orig_frac,4),
                                               "p_v2745_final_original_fraction":round(final_frac,4),
                                               "p_v2745_be_recovery_active":True,
                                               "p_v2745_be_recovery_started_at":be_started_at,**market_snapshot})
                                    v2745_be_recovery_active = True
                            else:
                                result, result_price = "BE_EXIT", be_price
                                result_details = {"be_price":be_price,"source":"ticker"}

                        if tp1_done and not tp1_was_done and not is_final_new:
                            with db() as conn:
                                conn.execute(
                                    "UPDATE research_shadow_reviews SET tp1_done=1,tp1_ts=?,tp1_price=? WHERE id=?",
                                    (utc_now(), tp1_price, int(review["id"])),
                                )
                            append_entry_record(
                                symbol,f"RESEARCH_{variant}_TP1",variant,float(review["new_p_score"] or 0),tp1_price,"actual_order=0",
                                extra={"shadow_id":str(review["shadow_id"] or ""),"research_variant":variant,
                                       "mfe_pct":round(mfe_pct,4),"mae_pct":round(mae_pct,4),"shadow_age_min":round(age_min,1),
                                       **market_snapshot},
                            )

                # V26/V27/V27-1/V27-4 공통 장기 무진행 실패관리. raw-path ghost는 제외한다.
                # V25의 공통 STOP 로직은 그대로 남기되, 확정 15분 구조실패 + 손실상태가
                # 서로 다른 5분 확인시점에서 2회 연속 유지될 때 먼저 작은 손실로 종료한다.
                if (
                    not result
                    and (
                        variant in ("P_V26", "P_V27", "P_V27_BLOCK_GHOST", "P_V27_FILTER_ONLY", "P_V27_1", "P_V27_1R", "P_V27_2", "P_V27_2_BLOCK_GHOST", "P_V27_2_FILTER_ONLY", "P_V27_3", "P_V27_3_BLOCK_GHOST", "P_V27_3_FILTER_ONLY", "P_V27_4", "P_V27_4_FILTER_ONLY", "P_V27_4_1", "P_V27_4_2", "P_V27_4_2_BLOCK_GHOST", "P_V27_4_3", "P_V27_4_3_BLOCK_GHOST", "P_V27_4_4", "P_V27_4_5", "P_V27_4_6", "P_V27_4_6_BLOCK_GHOST")
                        or is_parallel_recovery or is_final_control or is_final_new
                    )
                    and not tp1_done
                    and not (is_v271_like and v271_stage_active)
                    and not v271r_recovery_active
                    and five_due
                    and self.cfg.research_pv26_late_failure_enabled
                ):
                    late26_key = (symbol, round(entry_price, 12), bucket_5m)
                    if late26_key in late26_cache:
                        late26, late26_details = late26_cache[late26_key]
                    else:
                        try:
                            late26, late26_details = pv26_late_failure_signal(
                                self.client, symbol, entry_price, price, age_min, mfe_pct, self.cfg
                            )
                        except Exception as exc:
                            late26, late26_details = False, {"error": str(exc), "reason": "pv26_late_check_error"}
                        late26_cache[late26_key] = (late26, late26_details)

                    v26_late_fail_streak = (v26_late_fail_streak + 1) if late26 else 0
                    required_confirms = max(1, int(self.cfg.research_pv26_late_confirmations))
                    if late26 and v26_late_fail_streak >= required_confirms:
                        result, result_price = "LATE_FAILURE_EXIT", price
                        result_details = {
                            "stop_type": "V26_LATE_FAILURE",
                            "late": late26_details,
                            "late_fail_streak": v26_late_fail_streak,
                            "required_confirmations": required_confirms,
                        }
                        # V26 control만 기존 late-ghost를 계속 남긴다. V27/V27-1은 동일 LATE 판정값만 비교한다.
                        if variant == "P_V26":
                            self._clone_pv26_late_ghost(review, highest, lowest, price, result_details)

                if not result and not is_raw_path_ghost and not tp1_done and not (is_v271_like and v271_stage_active) and not v271r_recovery_active and five_due:
                    stop_key = (symbol, round(entry_price, 12), bucket_5m)
                    if stop_key in stop_cache:
                        stop_hit, stop_type, stop_details = stop_cache[stop_key]
                    else:
                        stop_hit = False
                        stop_type = ""
                        stop_details: dict[str, Any] = {}
                        try:
                            crash, d = early_crash_failure_signal(self.client, symbol, opened_at, entry_price, price, self.cfg)
                            if crash:
                                stop_hit, stop_type, stop_details = True, "EARLY_CRASH", d
                        except Exception as exc:
                            stop_details = {"early_crash_error": str(exc)}
                        if not stop_hit:
                            try:
                                cat, d = p_catastrophic_failure_signal(self.client, symbol, opened_at, entry_price, price, self.cfg)
                                if cat:
                                    stop_hit, stop_type, stop_details = True, "P_CATASTROPHIC", d
                            except Exception as exc:
                                stop_details = {**stop_details, "p_cat_error": str(exc)}
                        if not stop_hit and self.cfg.early_failure_enabled:
                            try:
                                early, d = early_failure_signal(self.client, symbol, opened_at, self.cfg)
                                if early:
                                    stop_hit, stop_type, stop_details = True, str(d.get("failure_type") or "EARLY_FAILURE"), d
                            except Exception as exc:
                                stop_details = {**stop_details, "early_failure_error": str(exc)}
                        if not stop_hit and age_min >= 45.0:
                            try:
                                late, d = late_trend_failure_signal(self.client, symbol, int(review["entry_ts_ms"] or 0), False)
                                if late:
                                    stop_hit, stop_type, stop_details = True, "LATE_TREND_FAILURE", d
                            except Exception as exc:
                                stop_details = {**stop_details, "late_failure_error": str(exc)}
                        stop_cache[stop_key] = (stop_hit, stop_type, stop_details)
                    if stop_hit:
                        if is_v271_like:
                            signal_price = price
                            stage = {
                                "active": True, "started_at": now.isoformat(), "signal_price": signal_price,
                                "stop_type": stop_type, "stop_details": stop_details,
                            }
                            review_snap["v271_stop_stage"] = stage
                            review_snap["p_v271_stop_stage_active"] = True
                            review_snap["p_v271_stop_signal_price"] = signal_price
                            review_snap["p_v271_stop_reason"] = stop_type
                            with db() as conn:
                                conn.execute("UPDATE research_shadow_reviews SET snapshot_json=? WHERE id=?",
                                    (json.dumps(review_snap,ensure_ascii=False,default=str),int(review["id"])))
                            append_entry_record(
                                symbol,f"RESEARCH_{variant}_STAGE1",variant,float(review["new_p_score"] or 0),signal_price,
                                f"50% stop signal={stop_type}; wait={self.cfg.research_pv271_wait_minutes}m; rebound={self.cfg.research_pv271_rebound_from_signal_pct}%",
                                extra={"shadow_id":str(review["shadow_id"] or ""),"research_variant":variant,
                                       "p_v271_stop_stage_active":True,"p_v271_stop_signal_price":signal_price,"p_v271_stop_reason":stop_type,
                                       **market_snapshot},
                            )
                            v271_stage_active = True
                            v271_stage = stage
                        else:
                            result, result_price = "STOP", price
                            result_details = {"stop_type": stop_type, "stop": stop_details}

                emergency_now = bool(entry_price > 0 and price <= entry_price * (1 - abs(float(self.cfg.structure_emergency_stop_pct)) / 100))
                if not result and not is_raw_path_ghost and not (is_v271_like and v271_stage_active) and not v271r_recovery_active and (fifteen_due or emergency_now):
                    structure_key = (symbol, round(entry_price, 12), bucket_15m, bool(emergency_now))
                    if structure_key in structure_cache:
                        broken, structure = structure_cache[structure_key]
                    else:
                        try:
                            broken, structure = hj_structure_broken(self.client, symbol, self.cfg, base_price=entry_price, live_price=price)
                        except Exception as exc:
                            broken, structure = False, {"error": str(exc)}
                        structure_cache[structure_key] = (broken, structure)
                    if broken:
                        signal_price = float(structure.get("price") or price)
                        if is_v271_like:
                            stage = {
                                "active": True, "started_at": now.isoformat(), "signal_price": signal_price,
                                "stop_type": "STRUCTURE", "stop_details": structure,
                            }
                            review_snap["v271_stop_stage"] = stage
                            review_snap["p_v271_stop_stage_active"] = True
                            review_snap["p_v271_stop_signal_price"] = signal_price
                            review_snap["p_v271_stop_reason"] = "STRUCTURE"
                            with db() as conn:
                                conn.execute("UPDATE research_shadow_reviews SET snapshot_json=? WHERE id=?",
                                    (json.dumps(review_snap,ensure_ascii=False,default=str),int(review["id"])))
                            append_entry_record(
                                symbol,f"RESEARCH_{variant}_STAGE1",variant,float(review["new_p_score"] or 0),signal_price,
                                f"50% stop signal=STRUCTURE; wait={self.cfg.research_pv271_wait_minutes}m; rebound={self.cfg.research_pv271_rebound_from_signal_pct}%",
                                extra={"shadow_id":str(review["shadow_id"] or ""),"research_variant":variant,
                                       "p_v271_stop_stage_active":True,"p_v271_stop_signal_price":signal_price,"p_v271_stop_reason":"STRUCTURE",
                                       **market_snapshot},
                            )
                            v271_stage_active = True
                            v271_stage = stage
                        else:
                            result = "STOP"
                            result_price = signal_price
                            result_details = {"stop_type": "STRUCTURE", "stop": structure}
                if not result and not is_raw_path_ghost and not (is_v271_like and v271_stage_active) and not v271r_recovery_active and fifteen_due:
                    flat_due = bool(age_min >= float(self.cfg.flat_exit_minutes) and mfe_pct < float(self.cfg.flat_min_favorable_pct))
                    if flat_due:
                        flat_key = (symbol, round(entry_price, 12), bucket_15m)
                        if flat_key in flat_cache:
                            flat_ok, flat_details = flat_cache[flat_key]
                        else:
                            try:
                                flat_ok, flat_details = flat_exit_signal(self.client, symbol, entry_price, self.cfg)
                            except Exception as exc:
                                flat_ok, flat_details = False, {"error": str(exc)}
                            flat_cache[flat_key] = (flat_ok, flat_details)
                        if flat_ok:
                            result, result_price = "FLAT_EXIT_75M", price
                            result_details = {"flat": flat_details}

                if result:
                    result_ts = utc_now()
                    # v4.3.86 Final New: 부분축소(10m/25m) + 마지막 exit를 원포지션 기준으로 합산하고,
                    # MKT50은 guard 발동 시 전체 전략손익을 0.5배로 반영한다.
                    if is_final_new:
                        size_mult = float(review_snap.get("final_size_mult") if review_snap.get("final_size_mult") not in (None, "") else 1.0)
                        rem = float(review_snap.get("final_remaining_frac") if review_snap.get("final_remaining_frac") not in (None, "") else 1.0)
                        realized = float(review_snap.get("final_realized_weighted_pct") or 0.0)
                        override = result_details.get("final_trade_gross_override")
                        if override not in (None, ""):
                            trade_gross = float(override)
                        else:
                            exit_pct_raw = (float(result_price) / entry_price - 1.0) * 100.0 if entry_price > 0 else 0.0
                            trade_gross = realized + rem * exit_pct_raw
                        strategy_gross = trade_gross * size_mult
                        result_price = entry_price * (1.0 + strategy_gross / 100.0)
                        review_snap["final_remaining_frac"] = 0.0
                        review_snap["final_strategy_gross_pct"] = round(strategy_gross, 6)
                        result_details.update({
                            "final_trade_gross_pct": round(trade_gross, 6),
                            "final_strategy_gross_pct": round(strategy_gross, 6),
                            "final_size_mult": size_mult,
                            "final_realized_weighted_pct": round(realized, 6),
                            "final_remaining_frac_before_exit": round(rem, 6),
                            "final_stop_stage": str(review_snap.get("final_stop_stage") or result_details.get("final_stop_stage") or ""),
                        })
                    elif is_final_control:
                        # CONTROL은 기존 TP1(+1.5%) 50% + TP2(+3%)/BE(+0.15%) 구조의
                        # 실제 원포지션 가중 손익을 기록한다. 단순 최종가격 수익률(3%/0.15%)로
                        # 기록하면 NEW와 비교가 왜곡되므로 TP1 실현분을 반드시 합산한다.
                        exit_pct_raw = (float(result_price) / entry_price - 1.0) * 100.0 if entry_price > 0 else 0.0
                        if tp1_done:
                            strategy_gross = 0.50 * float(self.cfg.tp1_pct) + 0.50 * exit_pct_raw
                        else:
                            strategy_gross = exit_pct_raw
                        result_price = entry_price * (1.0 + strategy_gross / 100.0)
                        review_snap["final_strategy_gross_pct"] = round(strategy_gross, 6)
                        result_details.update({
                            "final_trade_gross_pct": round(strategy_gross, 6),
                            "final_strategy_gross_pct": round(strategy_gross, 6),
                            "final_size_mult": 1.0,
                            "final_control_tp1_weighted": bool(tp1_done),
                        })
                    net_pct = (float(result_price) / entry_price - 1) * 100 if entry_price > 0 else 0.0
                    if variant == "P_V26" and result == "STOP":
                        try:
                            self._clone_pv26_stop_ghost(review, result_ts, float(result_price), result_details, backfilled=False)
                        except Exception as exc:
                            append_entry_record(
                                symbol, "RESEARCH_P_V26_STOP_GHOST_REGISTER_ERROR", "P_V26_STOP_GHOST",
                                float(review["new_p_score"] or 0), float(result_price), f"{type(exc).__name__}: {exc}"
                            )
                    if variant == "P_V27_4" and result == "STOP":
                        try:
                            self._clone_pv274_stop_ghost(review, result_ts, float(result_price), result_details, backfilled=False)
                        except Exception as exc:
                            append_entry_record(
                                symbol, "RESEARCH_P_V27_4_STOP_GHOST_REGISTER_ERROR", "P_V27_4_STOP_GHOST",
                                float(review["new_p_score"] or 0), float(result_price), f"{type(exc).__name__}: {exc}"
                            )
                    if variant == "P_V27_4_1" and result == "STOP":
                        try:
                            self._clone_pv2741_stop_ghost(review, result_ts, float(result_price), result_details, backfilled=False)
                        except Exception as exc:
                            append_entry_record(
                                symbol, "RESEARCH_P_V27_4_1_STOP_GHOST_REGISTER_ERROR", "P_V27_4_1_STOP_GHOST",
                                float(review["new_p_score"] or 0), float(result_price), f"{type(exc).__name__}: {exc}"
                            )
                    if variant == "P_V27_4_2" and result == "STOP":
                        try:
                            self._clone_pv2742_stop_ghost(review, result_ts, float(result_price), result_details, backfilled=False)
                        except Exception as exc:
                            append_entry_record(
                                symbol, "RESEARCH_P_V27_4_2_STOP_GHOST_REGISTER_ERROR", "P_V27_4_2_STOP_GHOST",
                                float(review["new_p_score"] or 0), float(result_price), f"{type(exc).__name__}: {exc}"
                            )
                    if variant == "P_V27_4_3" and result == "STOP":
                        try:
                            self._clone_pv2743_stop_ghost(review, result_ts, float(result_price), result_details, backfilled=False)
                        except Exception as exc:
                            append_entry_record(
                                symbol, "RESEARCH_P_V27_4_3_STOP_GHOST_REGISTER_ERROR", "P_V27_4_3_STOP_GHOST",
                                float(review["new_p_score"] or 0), float(result_price), f"{type(exc).__name__}: {exc}"
                            )
                    if variant in ("P_V27_4_6", "P_V27_4_6_BLOCK_GHOST") and result == "STOP":
                        try:
                            self._clone_pv2746_stop_ghost(review, result_ts, float(result_price), result_details, backfilled=False)
                        except Exception as exc:
                            append_entry_record(
                                symbol, "RESEARCH_P_V27_4_6_STOP_GHOST_REGISTER_ERROR", "P_V27_4_6_STOP_GHOST",
                                float(review["new_p_score"] or 0), float(result_price), f"{type(exc).__name__}: {exc}"
                            )
                    if result == "BE_EXIT":
                        with db() as conn:
                            conn.execute("""INSERT OR IGNORE INTO research_be_shadow_reviews(
                                shadow_id,variant,symbol,opened_at,shadow_started_at,entry_price,be_exit_price,tp2_price,highest_price,lowest_price,last_price,last_checked_at
                            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                                (str(review["shadow_id"] or ""),variant,symbol,opened_at,result_ts,entry_price,float(result_price),tp2_price,float(result_price),float(result_price),float(result_price),result_ts))
                    final_exit_pct = (float(result_price) / entry_price - 1.0) * 100.0 if entry_price > 0 else 0.0
                    if is_pv2744:
                        partial_done = bool(review_snap.get("p_v2744_be_partial_done"))
                        if tp1_done:
                            if partial_done:
                                partial_frac = float(review_snap.get("p_v2744_be_partial_original_fraction") or 0.25)
                                final_frac = float(review_snap.get("p_v2744_final_original_fraction") or 0.25)
                                strategy_gross = 0.50 * float(self.cfg.tp1_pct) + partial_frac * float(self.cfg.breakeven_stop_pct) + final_frac * final_exit_pct
                            else:
                                strategy_gross = 0.50 * float(self.cfg.tp1_pct) + 0.50 * final_exit_pct
                        else:
                            strategy_gross = final_exit_pct
                        result_details.update({
                            "p_v2744_be_partial_done":partial_done,
                            "p_v2744_be_partial_price":review_snap.get("p_v2744_be_partial_price"),
                            "p_v2744_be_partial_original_fraction":review_snap.get("p_v2744_be_partial_original_fraction"),
                            "p_v2744_final_original_fraction":review_snap.get("p_v2744_final_original_fraction"),
                            "p_v2744_strategy_gross_pct":round(strategy_gross,4),
                        })
                    if is_pv2745:
                        partial_done = bool(review_snap.get("p_v2745_be_partial_done"))
                        if "p_v2745_strategy_gross_pct" in result_details:
                            strategy_gross = float(result_details.get("p_v2745_strategy_gross_pct") or 0.0)
                        elif tp1_done:
                            if partial_done:
                                partial_frac = float(review_snap.get("p_v2745_be_partial_original_fraction") or 0.25)
                                final_frac = float(review_snap.get("p_v2745_final_original_fraction") or 0.25)
                                strategy_gross = 0.50 * float(self.cfg.tp1_pct) + partial_frac * float(self.cfg.breakeven_stop_pct) + final_frac * final_exit_pct
                            else:
                                strategy_gross = 0.50 * float(self.cfg.tp1_pct) + 0.50 * final_exit_pct
                        else:
                            strategy_gross = final_exit_pct
                        result_details.update({
                            "p_v2745_be_partial_done":partial_done,
                            "p_v2745_be_partial_price":review_snap.get("p_v2745_be_partial_price"),
                            "p_v2745_be_partial_original_fraction":review_snap.get("p_v2745_be_partial_original_fraction"),
                            "p_v2745_final_original_fraction":review_snap.get("p_v2745_final_original_fraction"),
                            "p_v2745_be_recovery_active":bool((review_snap.get("p_v2745_be_recovery") or {}).get("active")),
                            "p_v2745_be_recovery_finish_reason":str((review_snap.get("p_v2745_be_recovery") or {}).get("finish_reason") or result_details.get("p_v2745_finish_reason") or ""),
                            "p_v2745_strategy_gross_pct":round(strategy_gross,4),
                        })
                    details_json = json.dumps({
                        "shadow_id": str(review["shadow_id"] or ""), "variant": variant,
                        "entry_price": entry_price, "old_p_score": float(review["old_p_score"] or 0),
                        "new_p_score": float(review["new_p_score"] or 0), "missing_condition": str(review["missing_condition"] or ""),
                        "highest_price": highest, "lowest_price": lowest, "mfe_pct": round(mfe_pct, 4),
                        "mae_pct": round(mae_pct, 4), "age_min": round(age_min, 1), "net_pct_at_exit": round(net_pct, 4),
                        "snapshot": review_snap,
                        "market_at_exit": market_snapshot, **result_details,
                    }, ensure_ascii=False, default=str)
                    with db() as conn:
                        conn.execute(
                            """UPDATE research_shadow_reviews SET tp1_done=?,highest_price=?,lowest_price=?,last_price=?,
                               last_checked_at=?,last_5m_bucket=?,last_15m_bucket=?,mfe_pct=?,mae_pct=?,v26_late_fail_streak=?,
                               result=?,result_ts=?,result_price=?,result_details=?,completed=1 WHERE id=?""",
                            (1 if tp1_done else 0, highest, lowest, price, result_ts, bucket_5m if five_due else str(review["last_5m_bucket"] or ""),
                             bucket_15m if (fifteen_due or emergency_now) else str(review["last_15m_bucket"] or ""), mfe_pct, mae_pct,
                             v26_late_fail_streak, result, result_ts, float(result_price), details_json, int(review["id"])),
                        )
                    append_entry_record(symbol, f"RESEARCH_{variant}_{result}", variant, float(review["new_p_score"] or 0), float(result_price), f"mfe={mfe_pct:.3f}%; mae={mae_pct:.3f}%; actual_order=0",
                        extra={"shadow_id": str(review["shadow_id"] or ""), "research_variant": variant, "mfe_pct": round(mfe_pct,4), "mae_pct": round(mae_pct,4),
                               "shadow_age_min": round(age_min,1), "shadow_exit_pct": round(net_pct,4), "shadow_stop_type": str(result_details.get("stop_type") or ""),
                               "parallel_lab_group": str(review_snap.get("parallel_lab_group") or ""),
                               "parallel_lab_rule": str(review_snap.get("parallel_lab_rule") or ""),
                               "parallel_lab_filter_block": review_snap.get("parallel_lab_filter_block",""),
                               "parallel_lab_filter_flags": str(review_snap.get("parallel_lab_filter_flags") or ""),
                               "parallel_lab_source_setup_id": str(review_snap.get("parallel_lab_source_setup_id") or ""),
                               "parallel_lab_master_variant": str(review_snap.get("parallel_lab_master_variant") or ""),
                               "parallel_lab_stop_trigger": str(review_snap.get("parallel_lab_stop_trigger") or result_details.get("parallel_lab_stop_trigger") or ""),
                               "parallel_lab_stop_signal_pct": review_snap.get("parallel_lab_stop_signal_pct",""),
                               "parallel_lab_late_streak": review_snap.get("parallel_lab_late_streak",""),
                               "final_fwd_variant": str(review_snap.get("final_fwd_variant") or ""),
                               "final_source_setup_id": str(review_snap.get("final_source_setup_id") or ""),
                               "final_safe_block": bool(review_snap.get("final_safe_block")),
                               "final_safe_flags": str(review_snap.get("final_safe_flags") or ""),
                               "final_market_guard": bool(review_snap.get("final_market_guard")),
                               "final_market_mode": str(review_snap.get("final_market_mode") or ""),
                               "final_size_mult": review_snap.get("final_size_mult",""),
                               "final_stop_stage": str(review_snap.get("final_stop_stage") or result_details.get("final_stop_stage") or ""),
                               "final_remaining_frac": review_snap.get("final_remaining_frac",""),
                               "final_realized_weighted_pct": review_snap.get("final_realized_weighted_pct",""),
                               "final_strategy_gross_pct": result_details.get("final_strategy_gross_pct", review_snap.get("final_strategy_gross_pct","")),
                               "fixed_safe_micro_risk": review_snap.get("fixed_safe_micro_risk",""),
                               "fixed_safe_regime_relax_on": review_snap.get("fixed_safe_regime_relax_on",""),
                               "fixed_safe_relaxed": review_snap.get("fixed_safe_relaxed",""),
                               "fixed_safe_effective_block": review_snap.get("fixed_safe_effective_block",""),
                               "fixed_safe_regime_edge_pctp": review_snap.get("fixed_safe_regime_edge_pctp",""),
                               "fixed_safe_14h_safe_n": review_snap.get("fixed_safe_14h_safe_n",""),
                               "fixed_safe_14h_safe_pos_pct": review_snap.get("fixed_safe_14h_safe_pos_pct",""),
                               "fixed_safe_14h_pass_n": review_snap.get("fixed_safe_14h_pass_n",""),
                               "fixed_safe_14h_pass_pos_pct": review_snap.get("fixed_safe_14h_pass_pos_pct",""),
                               "fixed_safe_14h_diff_pctp": review_snap.get("fixed_safe_14h_diff_pctp",""),
                               "fixed_safe_18h_safe_n": review_snap.get("fixed_safe_18h_safe_n",""),
                               "fixed_safe_18h_safe_pos_pct": review_snap.get("fixed_safe_18h_safe_pos_pct",""),
                               "fixed_safe_18h_pass_n": review_snap.get("fixed_safe_18h_pass_n",""),
                               "fixed_safe_18h_pass_pos_pct": review_snap.get("fixed_safe_18h_pass_pos_pct",""),
                               "fixed_safe_18h_diff_pctp": review_snap.get("fixed_safe_18h_diff_pctp",""),
                               "fixed_pp_armed": review_snap.get("fixed_pp_armed",""),
                               "fixed_pp_arm_mfe_pct": review_snap.get("fixed_pp_arm_mfe_pct",""),
                               "fixed_pp_last_close_pct": review_snap.get("fixed_pp_last_close_pct",""),
                               "fixed_pp_triggered": review_snap.get("fixed_pp_triggered",""),
                               "p_v274_ghost_type": variant if variant in ("P_V27_4_BLOCK_GHOST","P_V27_4_STOP_GHOST") else "",
                               "p_v274_source_shadow_id": str(review_snap.get("p_v274_source_shadow_id") or "") if variant in ("P_V27_4_BLOCK_GHOST","P_V27_4_STOP_GHOST") else "",
                               "p_v274_ghost_classification": str(result_details.get("classification") or "") if variant in ("P_V27_4_BLOCK_GHOST","P_V27_4_STOP_GHOST") else "",
                               "p_v2742_filter_block": bool(review_snap.get("p_v2742_filter_block")),
                               "p_v2742_filter_reasons": str(review_snap.get("p_v2742_filter_reasons") or ""),
                               "p_v2742_a9_rule_ids": str(review_snap.get("p_v2742_a9_rule_ids") or ""),
                               "p_v2742_filter_version": str(review_snap.get("p_v2742_filter_version") or ""),
                               "p_v2742_ghost_type": variant if variant == "P_V27_4_2_STOP_GHOST" else "",
                               "p_v2742_source_shadow_id": str(review_snap.get("p_v2742_source_shadow_id") or "") if variant == "P_V27_4_2_STOP_GHOST" else "",
                               "p_v2743_filter_block": bool(review_snap.get("p_v2743_filter_block")),
                               "p_v2743_filter_reasons": str(review_snap.get("p_v2743_filter_reasons") or ""),
                               "p_v2743_a9_rule_ids": str(review_snap.get("p_v2743_a9_rule_ids") or ""),
                               "p_v2743_filter_version": str(review_snap.get("p_v2743_filter_version") or ""),
                               "p_v2743_ghost_type": variant if variant == "P_V27_4_3_STOP_GHOST" else "",
                               "p_v2743_source_shadow_id": str(review_snap.get("p_v2743_source_shadow_id") or "") if variant == "P_V27_4_3_STOP_GHOST" else "",
                               "p_v2743_ghost_classification": str(result_details.get("classification") or "") if variant == "P_V27_4_3_STOP_GHOST" else "",
                               "p_v2746_filter_block": bool(review_snap.get("p_v2746_filter_block")),
                               "p_v2746_filter_reasons": str(review_snap.get("p_v2746_filter_reasons") or ""),
                               "p_v2746_filter_version": str(review_snap.get("p_v2746_filter_version") or ""),
                               "p_v2746_ghost_type": variant if variant == "P_V27_4_6_STOP_GHOST" else "",
                               "p_v2746_source_shadow_id": str(review_snap.get("p_v2746_source_shadow_id") or "") if variant == "P_V27_4_6_STOP_GHOST" else "",
                               "p_v2746_ghost_classification": str(result_details.get("classification") or "") if variant == "P_V27_4_6_STOP_GHOST" else "",
                               "p_v2744_be_partial_done": bool(review_snap.get("p_v2744_be_partial_done")) if variant == "P_V27_4_4" else "",
                               "p_v2744_be_partial_price": review_snap.get("p_v2744_be_partial_price") if variant == "P_V27_4_4" else "",
                               "p_v2744_be_partial_original_fraction": review_snap.get("p_v2744_be_partial_original_fraction") if variant == "P_V27_4_4" else "",
                               "p_v2744_final_original_fraction": review_snap.get("p_v2744_final_original_fraction") if variant == "P_V27_4_4" else "",
                               "p_v2744_strategy_gross_pct": result_details.get("p_v2744_strategy_gross_pct","") if variant == "P_V27_4_4" else "",
                               "p_v2745_be_partial_done": bool(review_snap.get("p_v2745_be_partial_done")) if variant == "P_V27_4_5" else "",
                               "p_v2745_be_partial_price": review_snap.get("p_v2745_be_partial_price") if variant == "P_V27_4_5" else "",
                               "p_v2745_be_partial_original_fraction": review_snap.get("p_v2745_be_partial_original_fraction") if variant == "P_V27_4_5" else "",
                               "p_v2745_final_original_fraction": review_snap.get("p_v2745_final_original_fraction") if variant == "P_V27_4_5" else "",
                               "p_v2745_be_recovery_active": bool((review_snap.get("p_v2745_be_recovery") or {}).get("active")) if variant == "P_V27_4_5" else "",
                               "p_v2745_be_recovery_started_at": (review_snap.get("p_v2745_be_recovery") or {}).get("started_at") if variant == "P_V27_4_5" else "",
                               "p_v2745_be_recovery_finish_reason": result_details.get("p_v2745_be_recovery_finish_reason","") if variant == "P_V27_4_5" else "",
                               "p_v2745_be_recovery_exit_pct": result_details.get("p_v2745_be_recovery_exit_pct","") if variant == "P_V27_4_5" else "",
                               "p_v2745_strategy_gross_pct": result_details.get("p_v2745_strategy_gross_pct","") if variant == "P_V27_4_5" else "",
                               **market_snapshot})
                else:
                    with db() as conn:
                        conn.execute(
                            """UPDATE research_shadow_reviews SET tp1_done=?,highest_price=?,lowest_price=?,last_price=?,last_checked_at=?,
                               last_5m_bucket=?,last_15m_bucket=?,mfe_pct=?,mae_pct=?,v26_late_fail_streak=? WHERE id=?""",
                            (1 if tp1_done else 0, highest, lowest, price, utc_now(), bucket_5m if five_due else str(review["last_5m_bucket"] or ""),
                             bucket_15m if (fifteen_due or emergency_now) else str(review["last_15m_bucket"] or ""), mfe_pct, mae_pct,
                             v26_late_fail_streak, int(review["id"])),
                        )
            except Exception as exc:
                append_entry_record(symbol, "RESEARCH_SHADOW_ERROR", variant, float(review["new_p_score"] or 0), float(review["last_price"] or 0), f"{type(exc).__name__}: {exc}")

    def update_research_be_shadow_reviews(self) -> None:
        """P_V22/V23/V24 BE 이후를 원래 보유시간까지 계속 가상추적한다."""
        with db() as conn:
            pending=conn.execute("SELECT * FROM research_be_shadow_reviews WHERE completed=0 ORDER BY shadow_started_at").fetchall()
        if not pending: return
        try:
            tickers={str(t.get("symbol") or ""):float(t.get("last") or 0) for t in self.client.tickers("SWAP")}
        except Exception as exc:
            append_entry_record("","RESEARCH_BE_TRACE_ERROR","",0,0,str(exc)); return
        now=datetime.now(timezone.utc)
        for r in pending:
            symbol=str(r["symbol"] or ""); price=float(tickers.get(symbol) or 0)
            if price<=0: continue
            entry=float(r["entry_price"] or 0); tp2=float(r["tp2_price"] or 0); hi=max(float(r["highest_price"] or price),price); lo=min(float(r["lowest_price"] or price),price)
            opened=datetime.fromisoformat(str(r["opened_at"]));
            if opened.tzinfo is None: opened=opened.replace(tzinfo=timezone.utc)
            age_h=(now-opened).total_seconds()/3600.0; result=""; details={}
            if tp2>0 and price>=tp2: result="TP2_AFTER_BE"; details={"tp2_price":tp2}
            elif entry>0 and price<=entry*(1-abs(float(self.cfg.structure_emergency_stop_pct))/100): result="EMERGENCY_STOP_AFTER_BE"
            elif age_h>=float(self.cfg.max_hold_hours): result="TIME_AFTER_BE"
            if result:
                ts=utc_now(); mfe=(hi/entry-1)*100 if entry>0 else 0; mae=(lo/entry-1)*100 if entry>0 else 0
                payload={**details,"mfe_pct":round(mfe,4),"mae_pct":round(mae,4),"age_h":round(age_h,3)}
                with db() as conn:
                    conn.execute("UPDATE research_be_shadow_reviews SET highest_price=?,lowest_price=?,last_price=?,last_checked_at=?,result=?,result_ts=?,result_price=?,result_details=?,completed=1 WHERE id=?",(hi,lo,price,ts,result,ts,price,json.dumps(payload,ensure_ascii=False),int(r["id"])))
                append_entry_record(symbol,f"RESEARCH_{str(r['variant'] or '')}_BE_TRACE_{result}",str(r["variant"] or ""),0,price,f"BE follow-up; mfe={mfe:.3f}%; mae={mae:.3f}%",extra={"shadow_id":str(r["shadow_id"] or ""),"research_variant":str(r["variant"] or ""),"mfe_pct":round(mfe,4),"mae_pct":round(mae,4)})
            else:
                with db() as conn:
                    conn.execute("UPDATE research_be_shadow_reviews SET highest_price=?,lowest_price=?,last_price=?,last_checked_at=? WHERE id=?",(hi,lo,price,utc_now(),int(r["id"])))

    def _register_junp_shadow_candidate(self, symbol: str, details: dict[str, Any]) -> None:
        """준P형 후보를 실제 주문 없이 별도 DB/SCAN 기록으로만 등록한다."""
        if not self.cfg.junp_shadow_enabled or not bool(details.get("junp_shadow_candidate")):
            return
        price = float(details.get("live_price") or details.get("price") or 0)
        p_score = float(details.get("p_score") or 0)
        missing = str(details.get("junp_missing_condition") or "")
        if price <= 0 or p_score < float(self.cfg.junp_shadow_min_score) or not missing:
            return

        now = datetime.now(timezone.utc)
        with db() as conn:
            pending = conn.execute(
                "SELECT 1 FROM junp_shadow_reviews WHERE symbol=? AND completed=0 LIMIT 1",
                (symbol,),
            ).fetchone()
            if pending:
                return
            last = conn.execute(
                "SELECT result_ts FROM junp_shadow_reviews WHERE symbol=? AND completed=1 ORDER BY result_ts DESC LIMIT 1",
                (symbol,),
            ).fetchone()
            if last and last["result_ts"]:
                try:
                    last_dt = datetime.fromisoformat(str(last["result_ts"]))
                    if last_dt.tzinfo is None:
                        last_dt = last_dt.replace(tzinfo=timezone.utc)
                    if now - last_dt < timedelta(minutes=max(0, int(self.cfg.junp_shadow_same_symbol_cooldown_minutes))):
                        return
                except Exception:
                    pass

            opened_at = now.isoformat()
            shadow_id = f"JUNP-{now.strftime('%Y%m%dT%H%M%S%f')}-{symbol}"
            tp2_price = price * (1 + float(self.cfg.tp2_pct) / 100)
            be_price = price * (1 + float(self.cfg.breakeven_stop_pct) / 100)
            conn.execute(
                """INSERT INTO junp_shadow_reviews(
                    shadow_id,symbol,opened_at,entry_ts_ms,entry_price,p_score,missing_condition,
                    tp2_price,be_price,highest_price,lowest_price,last_price,last_checked_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    shadow_id, symbol, opened_at, int(now.timestamp() * 1000), price, p_score, missing,
                    tp2_price, be_price, price, price, price, opened_at,
                ),
            )

        append_entry_record(
            symbol, "JUNP_SHADOW_ENTRY", "JUNP_SHADOW", p_score, price,
            f"missing={missing}; actual_order=0"
        )

    def update_junp_shadow_reviews(self) -> None:
        """준P형 Shadow를 실제 주문 없이 P형 관리규칙으로 가상 추적한다.

        실제 P형 LIVE/DB 손익에는 전혀 반영하지 않는다. 결과는 SCAN CSV의
        JUNP_SHADOW_* 이벤트와 junp_shadow_reviews 테이블에만 남긴다.
        DCA는 가상 체결 오차를 키울 수 있어 적용하지 않고 최초 진입가 기준으로 비교한다.
        """
        if not self.cfg.junp_shadow_enabled:
            return
        with db() as conn:
            pending = conn.execute(
                "SELECT * FROM junp_shadow_reviews WHERE completed=0 ORDER BY opened_at"
            ).fetchall()
        if not pending:
            return

        # 준P Shadow는 실제 주문과 완전히 분리된 리뷰 기능이다.
        # bulk tickers 호출이 메인 스캔과 겹쳐 skip/timeout 될 수 있으므로,
        # bulk 조회가 실패하면 pending 심볼만 개별 ticker로 fallback 한다.
        ticker_map: dict[str, float] = {}
        bulk_error = ""
        try:
            for t in self.client.tickers("SWAP"):
                symbol_key = str(t.get("symbol") or "")
                if not symbol_key:
                    continue
                # Bybit bulk ticker는 lastPrice, wrapper에 따라 last를 쓸 수 있어 둘 다 지원.
                last_value = t.get("lastPrice")
                if last_value in (None, ""):
                    last_value = t.get("last")
                try:
                    ticker_map[symbol_key] = float(last_value or 0)
                except (TypeError, ValueError):
                    ticker_map[symbol_key] = 0.0
        except Exception as exc:
            bulk_error = str(exc)

        # bulk 결과가 없거나 해당 심볼 가격이 0이면 개별 ticker로 복구한다.
        # Shadow 개수만큼만 호출하므로 실제 LIVE 주문/관리 로직에는 관여하지 않는다.
        for review in pending:
            symbol_key = str(review["symbol"] or "")
            if not symbol_key or float(ticker_map.get(symbol_key) or 0) > 0:
                continue
            try:
                one = self.client.ticker(symbol_key)
                last_value = one.get("last")
                if last_value in (None, ""):
                    last_value = one.get("lastPrice")
                ticker_map[symbol_key] = float(last_value or 0)
            except Exception as exc:
                if bulk_error:
                    err_text = f"bulk={bulk_error}; fallback={type(exc).__name__}: {exc}"
                else:
                    err_text = f"fallback={type(exc).__name__}: {exc}"
                append_entry_record(
                    symbol_key, "JUNP_SHADOW_TICKER_ERROR", "JUNP_SHADOW", 0, 0, err_text
                )

        now = datetime.now(timezone.utc)
        bucket_5m = str(int(now.timestamp()) // 300)
        bucket_15m = str(int(now.timestamp()) // 900)

        for review in pending:
            symbol = str(review["symbol"] or "")
            shadow_id = str(review["shadow_id"] or "")
            try:
                price = float(ticker_map.get(symbol) or 0)
                if price <= 0:
                    continue
                entry_price = float(review["entry_price"] or 0)
                opened_at = str(review["opened_at"])
                tp1_done = bool(int(review["tp1_done"] or 0))
                tp1_price = entry_price * (1 + float(self.cfg.tp1_pct) / 100)
                tp2_price = float(review["tp2_price"] or entry_price * (1 + float(self.cfg.tp2_pct) / 100))
                be_price = float(review["be_price"] or entry_price * (1 + float(self.cfg.breakeven_stop_pct) / 100))
                highest = max(float(review["highest_price"] or price), price)
                lowest = min(float(review["lowest_price"] or price), price)

                opened = datetime.fromisoformat(opened_at)
                if opened.tzinfo is None:
                    opened = opened.replace(tzinfo=timezone.utc)
                age_min = (now - opened).total_seconds() / 60.0

                # 1분봉 high로 짧은 TP 터치를 보강한다. API 부담을 줄이기 위해 5분 버킷당 1회만 조회.
                observed_high = max(price, highest)
                five_due = str(review["last_5m_bucket"] or "") != bucket_5m
                if five_due:
                    try:
                        m1 = self.client.candles(symbol, "1m", 3)
                        if m1 is not None and len(m1) > 0 and "high" in m1.columns:
                            observed_high = max(
                                observed_high,
                                float(pd.to_numeric(m1["high"], errors="coerce").dropna().tail(2).max())
                            )
                            highest = max(highest, observed_high)
                    except Exception:
                        pass

                result = ""
                result_price = price
                result_details: dict[str, Any] = {}

                # 실제 봇과 동일하게 시간종료가 최우선.
                if age_min >= float(self.cfg.max_hold_hours) * 60.0:
                    result = "TIME_EXIT"
                    result_details = {"age_min": round(age_min, 1)}

                # TP는 손절보다 먼저 처리한다.
                if not result and not tp1_done and observed_high >= tp1_price:
                    tp1_done = True
                    with db() as conn:
                        conn.execute(
                            """UPDATE junp_shadow_reviews SET tp1_done=1,tp1_ts=?,tp1_price=?,highest_price=?,
                               last_price=?,last_checked_at=?,last_5m_bucket=? WHERE id=?""",
                            (utc_now(), tp1_price, highest, price, utc_now(), bucket_5m, int(review["id"])),
                        )
                    append_entry_record(
                        symbol, "JUNP_SHADOW_TP1", "JUNP_SHADOW", float(review["p_score"] or 0),
                        tp1_price, f"missing={review['missing_condition']}"
                    )

                if not result and tp1_done and observed_high >= tp2_price:
                    result = "TP2"
                    result_price = tp2_price
                    result_details = {"tp2_price": tp2_price}

                # TP1 이후에는 현재 LIVE P형과 동일하게 +0.15% BE를 적용.
                if not result and tp1_done and price <= be_price:
                    result = "BE_EXIT"
                    result_price = be_price
                    result_details = {"be_price": be_price}

                # TP1 이전 조기/단계형 손절은 확정 5분봉이 바뀔 때만 재평가.
                if not result and not tp1_done and five_due:
                    stop_hit = False
                    stop_type = ""
                    stop_details: dict[str, Any] = {}
                    try:
                        crash, d = early_crash_failure_signal(
                            self.client, symbol, opened_at, entry_price, price, self.cfg
                        )
                        if crash:
                            stop_hit, stop_type, stop_details = True, "EARLY_CRASH", d
                    except Exception as exc:
                        stop_details = {"early_crash_error": str(exc)}

                    if not stop_hit:
                        try:
                            cat, d = p_catastrophic_failure_signal(
                                self.client, symbol, opened_at, entry_price, price, self.cfg
                            )
                            if cat:
                                stop_hit, stop_type, stop_details = True, "P_CATASTROPHIC", d
                        except Exception as exc:
                            stop_details = {**stop_details, "p_cat_error": str(exc)}

                    if not stop_hit and self.cfg.early_failure_enabled:
                        try:
                            early, d = early_failure_signal(self.client, symbol, opened_at, self.cfg)
                            if early:
                                stop_hit, stop_type, stop_details = True, str(d.get("failure_type") or "EARLY_FAILURE"), d
                        except Exception as exc:
                            stop_details = {**stop_details, "early_failure_error": str(exc)}

                    if not stop_hit and age_min >= 45.0:
                        try:
                            late, d = late_trend_failure_signal(
                                self.client, symbol, int(review["entry_ts_ms"] or 0), False
                            )
                            if late:
                                stop_hit, stop_type, stop_details = True, "LATE_TREND_FAILURE", d
                        except Exception as exc:
                            stop_details = {**stop_details, "late_failure_error": str(exc)}

                    if stop_hit:
                        result = "STOP"
                        result_price = price
                        result_details = {"stop_type": stop_type, "stop": stop_details}

                # HJ/P 공통 15분 구조손절 + 정체종료는 확정 15분봉 변경 시에만 재평가.
                fifteen_due = str(review["last_15m_bucket"] or "") != bucket_15m
                emergency_now = bool(
                    entry_price > 0
                    and price <= entry_price * (1 - abs(float(self.cfg.structure_emergency_stop_pct)) / 100)
                )
                if not result and (fifteen_due or emergency_now):
                    try:
                        broken, structure = hj_structure_broken(
                            self.client, symbol, self.cfg, base_price=entry_price, live_price=price
                        )
                    except Exception as exc:
                        broken, structure = False, {"error": str(exc)}
                    if broken:
                        result = "STOP"
                        result_price = float(structure.get("price") or price)
                        result_details = {"stop_type": "STRUCTURE", "stop": structure}

                if not result and fifteen_due:
                    flat_due = bool(
                        age_min >= float(self.cfg.flat_exit_minutes)
                        and (highest / entry_price - 1) * 100 < float(self.cfg.flat_min_favorable_pct)
                    )
                    if flat_due:
                        try:
                            flat_ok, flat_details = flat_exit_signal(self.client, symbol, entry_price, self.cfg)
                        except Exception as exc:
                            flat_ok, flat_details = False, {"error": str(exc)}
                        if flat_ok:
                            result = "FLAT_EXIT_75M"
                            result_price = price
                            result_details = {"flat": flat_details}

                if result:
                    result_ts = utc_now()
                    with db() as conn:
                        conn.execute(
                            """UPDATE junp_shadow_reviews SET tp1_done=?,highest_price=?,lowest_price=?,last_price=?,
                               last_checked_at=?,last_5m_bucket=?,last_15m_bucket=?,result=?,result_ts=?,result_price=?,
                               result_details=?,completed=1 WHERE id=?""",
                            (
                                1 if tp1_done else 0, highest, lowest, price, result_ts,
                                bucket_5m if five_due else str(review["last_5m_bucket"] or ""),
                                bucket_15m if (fifteen_due or emergency_now) else str(review["last_15m_bucket"] or ""),
                                result, result_ts, float(result_price),
                                json.dumps({
                                    "shadow_id": shadow_id,
                                    "missing_condition": str(review["missing_condition"] or ""),
                                    "entry_price": entry_price,
                                    "p_score": float(review["p_score"] or 0),
                                    "highest_price": highest,
                                    "lowest_price": lowest,
                                    "age_min": round(age_min, 1),
                                    "dca_simulated": False,
                                    **result_details,
                                }, ensure_ascii=False),
                                int(review["id"]),
                            ),
                        )
                    append_entry_record(
                        symbol, f"JUNP_SHADOW_{result}", "JUNP_SHADOW",
                        float(review["p_score"] or 0), float(result_price),
                        f"missing={review['missing_condition']}; high={highest:.10g}; low={lowest:.10g}; actual_order=0"
                    )
                else:
                    with db() as conn:
                        conn.execute(
                            """UPDATE junp_shadow_reviews SET tp1_done=?,highest_price=?,lowest_price=?,last_price=?,
                               last_checked_at=?,last_5m_bucket=?,last_15m_bucket=? WHERE id=?""",
                            (
                                1 if tp1_done else 0, highest, lowest, price, utc_now(),
                                bucket_5m if five_due else str(review["last_5m_bucket"] or ""),
                                bucket_15m if (fifteen_due or emergency_now) else str(review["last_15m_bucket"] or ""),
                                int(review["id"]),
                            ),
                        )
            except Exception as exc:
                append_entry_record(
                    symbol, "JUNP_SHADOW_ERROR", "JUNP_SHADOW", float(review["p_score"] or 0),
                    float(review["last_price"] or 0), f"{type(exc).__name__}: {exc}"
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
            # 최우선 시간종료 판정은 어떤 신규 API 호출보다 먼저 한다.
            # 3시간이 지난 PAPER 포지션은 DB에 저장된 마지막 가격으로 즉시 종료하여
            # ticker/API 지연 때문에 TIME_EXIT 자체가 밀리지 않게 한다.
            age_h = (datetime.now(timezone.utc) - datetime.fromisoformat(row["opened_at"])).total_seconds() / 3600
            if age_h >= self.cfg.max_hold_hours and self.cfg.mode == "paper":
                fallback_price = float(row["last_price"] or row["avg_price"] or row["base_entry_price"] or 0)
                if fallback_price > 0:
                    log_event(
                        row["symbol"], "TIME_EXIT_DUE",
                        price=fallback_price, mode=self.cfg.mode,
                        details=json.dumps({
                            "age_h": round(age_h, 4),
                            "max_hold_hours": self.cfg.max_hold_hours,
                            "price_source": "stored_last_price"
                        }, ensure_ascii=False),
                        strategy=row["strategy"] or "", trade_id=row["trade_id"] or ""
                    )
                    self._close(row, fallback_price, 1.0, "TIME_EXIT", fallback_price, None)
                    continue

            # 3시간 미만 PAPER 또는 DEMO/LIVE 관리부터는 최신 시세가 필요하므로 ticker 조회.
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

            # DEMO/LIVE는 실제 최신가가 확보된 뒤 TIME_EXIT 처리.
            if age_h >= self.cfg.max_hold_hours:
                log_event(
                    row["symbol"], "TIME_EXIT_DUE",
                    price=price, mode=self.cfg.mode,
                    details=json.dumps({
                        "age_h": round(age_h, 4),
                        "max_hold_hours": self.cfg.max_hold_hours,
                        "price_source": "ticker"
                    }, ensure_ascii=False),
                    strategy=row["strategy"] or "", trade_id=row["trade_id"] or ""
                )
                self._close(row, price, 1.0, "TIME_EXIT", price, None)
                continue

            avg = float(row["avg_price"])
            base_price = float(row["base_entry_price"] or avg)
            pnl_pct = (price / avg - 1) * 100
            base_pnl_pct = (price / base_price - 1) * 100
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

            # 실전: 거래소에 미리 걸어둔 TP 주문의 실제 체결을 먼저 동기화한다.
            synced_row = self._sync_exchange_tp_fill(row, price)
            if synced_row is None:
                continue
            row = synced_row
            avg = float(row["avg_price"])
            base_price = float(row["base_entry_price"] or avg)
            highest = float(row["highest_price"] or price)

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

            if not (self.cfg.mode == "live" and self.cfg.exchange_tp_preorders_enabled):
                if int(row["tp1_done"]) == 0 and tp_observed_high >= tp1_trigger:
                    fill = paper_fill(tp1_trigger) if self.cfg.mode == "paper" else price
                    self._close(row, fill, 0.5, "TP1", price, tp1_trigger)
                    continue

                if int(row["tp1_done"]) == 1 and tp_observed_high >= tp2_trigger:
                    fill = paper_fill(tp2_trigger) if self.cfg.mode == "paper" else price
                    self._close(row, fill, 1.0, "TP2", price, tp2_trigger)
                    continue

            # TP가 터치되지 않았을 때만 BE/구조손절/시간종료를 확인한다.

            # v4.3.41 진입 직후 3~15분 급락 전용 Early Crash Guard.
            # -2.5% 이상 급락 + 확정 5분 구조 약화가 함께 있을 때만 종료한다.
            if int(row["tp1_done"] or 0) == 0:
                try:
                    early_crash, early_crash_details = early_crash_failure_signal(
                        self.client, row["symbol"], row["opened_at"], base_price, price, self.cfg
                    )
                except Exception as exc:
                    early_crash, early_crash_details = False, {"error": str(exc)}
                    log_event(
                        row["symbol"], "EARLY_CRASH_CHECK_ERROR",
                        mode=self.cfg.mode, details=str(exc),
                        strategy=strategy, trade_id=row["trade_id"] or ""
                    )
                if early_crash:
                    log_event(
                        row["symbol"], "EARLY_CRASH_3_15_TRIGGER",
                        price=price, mode=self.cfg.mode,
                        details=json.dumps(early_crash_details, ensure_ascii=False),
                        strategy=strategy, trade_id=row["trade_id"] or ""
                    )
                    reason = "HJ_STRUCTURE_STOP" if strategy == "HJ" else "STOP"
                    self._close(row, price, 1.0, reason, price, price)
                    continue

            # v4.3.39 P형 단계형 대손실 가드.
            # TP1 전 P 포지션만 대상으로 하며, 손실이 깊어질수록 5분 구조 확인 강도를 완화한다.
            if strategy == "P" and int(row["tp1_done"] or 0) == 0:
                try:
                    p_cat_fail, p_cat_details = p_catastrophic_failure_signal(
                        self.client, row["symbol"], row["opened_at"], base_price, price, self.cfg
                    )
                except Exception as exc:
                    p_cat_fail, p_cat_details = False, {"error": str(exc)}
                    log_event(
                        row["symbol"], "P_CATASTROPHIC_CHECK_ERROR",
                        mode=self.cfg.mode, details=str(exc),
                        strategy=strategy, trade_id=row["trade_id"] or ""
                    )
                if p_cat_fail:
                    log_event(
                        row["symbol"], "P_CATASTROPHIC_FAILURE_TRIGGER",
                        price=price, mode=self.cfg.mode,
                        details=json.dumps(p_cat_details, ensure_ascii=False),
                        strategy=strategy, trade_id=row["trade_id"] or ""
                    )
                    self._close(row, price, 1.0, "STOP", price, price)
                    continue

            # v4.3.39 HJ형 단계형 대손실 가드.
            # 단순 고정손절이 아니라 -1.5/-2.0/-2.5% 구간별로 확정 5분 구조 확인 강도를 조절한다.
            if strategy == "HJ" and int(row["tp1_done"] or 0) == 0:
                try:
                    hj_cat_fail, hj_cat_details = hj_catastrophic_failure_signal(
                        self.client, row["symbol"], row["opened_at"], base_price, price, self.cfg
                    )
                except Exception as exc:
                    hj_cat_fail, hj_cat_details = False, {"error": str(exc)}
                    log_event(
                        row["symbol"], "HJ_CATASTROPHIC_CHECK_ERROR",
                        mode=self.cfg.mode, details=str(exc),
                        strategy=strategy, trade_id=row["trade_id"] or ""
                    )
                if hj_cat_fail:
                    log_event(
                        row["symbol"], "HJ_CATASTROPHIC_FAILURE_TRIGGER",
                        price=price, mode=self.cfg.mode,
                        details=json.dumps(hj_cat_details, ensure_ascii=False),
                        strategy=strategy, trade_id=row["trade_id"] or ""
                    )
                    self._close(row, price, 1.0, "HJ_STRUCTURE_STOP", price, price)
                    continue

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
                entry_ts_ms = int(row["entry_ts_ms"] or 0)
                if entry_ts_ms <= 0:
                    # 마이그레이션 전에 열려 있던 포지션도 안전하게 처리.
                    opened_dt = datetime.fromisoformat(row["opened_at"])
                    if opened_dt.tzinfo is None:
                        opened_dt = opened_dt.replace(tzinfo=timezone.utc)
                    entry_ts_ms = int(opened_dt.timestamp() * 1000)

                late_fail, late_details = late_trend_failure_signal(
                    self.client,
                    row["symbol"],
                    entry_ts_ms,
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

            if int(row["tp1_done"]) == 1:
                be_trigger = base_price * (1 + self.cfg.breakeven_stop_pct / 100)
                exchange_be_armed = (
                    self.cfg.mode == "live"
                    and state_get(
                        f"exchange_be_{row['symbol']}_{int(row['entry_ts_ms'] or 0)}",
                        "0",
                    ) == "1"
                )

                # LIVE에서 거래소 스탑이 정상 등록됐다면 Bybit가 직접 보호한다.
                # 등록 실패/비LIVE일 때만 로컬 시장가 종료를 안전망으로 사용한다.
                if not exchange_be_armed and price <= be_trigger:
                    self._close(
                        row, paper_fill(be_trigger), 1.0,
                        "BE_EXIT", price, be_trigger
                    )
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

            # v4.3.55 P형 장기 정체 + 모멘텀 약화 조기종료:
            # TP1을 아직 못 찍은 상태에서 75분 이상 지났고, 현재 진행이 거의 없으면서
            # 확정 15분봉 약화가 3개 이상 겹칠 때만 정체종료로 처리한다.
            # 기존 FLAT_EXIT보다 넓게 "지지부진 후 약화"를 잡되 단순 시간 종료는 하지 않는다.
            if (
                strategy == "P"
                and self.cfg.stalled_weak_exit_enabled
                and int(row["tp1_done"] or 0) == 0
                and age_h * 60 >= float(self.cfg.stalled_weak_exit_minutes)
            ):
                try:
                    stalled_ok, stalled_details = stalled_weak_exit_signal(
                        self.client, row["symbol"], base_price, self.cfg
                    )
                except Exception as exc:
                    stalled_ok, stalled_details = False, {"error": str(exc)}
                    log_event(
                        row["symbol"], "STALL_WEAK_EXIT_CHECK_ERROR", mode=self.cfg.mode,
                        details=str(exc), strategy=strategy, trade_id=row["trade_id"] or ""
                    )

                if stalled_ok:
                    log_event(
                        row["symbol"], "STALL_WEAK_EXIT_75M_TRIGGER",
                        price=price, mode=self.cfg.mode,
                        details=json.dumps(stalled_details, ensure_ascii=False),
                        strategy=strategy, trade_id=row["trade_id"] or ""
                    )
                    self._close(row, price, 1.0, "FLAT_EXIT_75M", price, None)
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


    def scan_entries(self) -> None:
        """긴급복구: SCAN_OK가 나오면 같은 스캔 안에서 바로 진입한다.

        기존처럼 모든 후보를 모은 뒤 별도 진입 루프로 넘기지 않아
        SCAN_OK 이후 발생하던 공통 오류를 우회한다.
        """
        # v4.3.70 telemetry-only: 스캔 1회에 BTC/ETH 각각 5분봉 1회만 조회.
        # 아래 market_snapshot은 기록에만 붙이며 진입/STOP 조건에는 사용하지 않는다.
        market_snapshot = self._refresh_market_snapshot()

        live_entry_paused = state_flag("pause_new_entries", False)
        if live_entry_paused:
            # v4.3.57: LIVE 신규진입만 막고 SCAN/JUNP Shadow는 계속 수행한다.
            # 실제 주문은 아래 strategy 처리 직전에 차단한다.
            append_entry_record("", "ENTRY_PAUSED_SCAN_CONTINUES", "", 0, 0, "pause_new_entries=1; scan=1; junp_shadow=0; live_order=0")
        if self.loss_cooldown_active():
            losses = self.consecutive_losses()
            state_set("loss_cooldown_active", "1")
            append_entry_record("", "ENTRY_BLOCKED", "", 0, 0, f"loss_cooldown=1; consecutive_losses={losses}")
            return
        state_set("loss_cooldown_active", "0")

        open_rows = self.open_rows()
        open_symbols = {str(r["symbol"]) for r in open_rows}
        slots = max(0, int(self.cfg.max_positions) - len(open_symbols))
        if slots <= 0 and not live_entry_paused:
            append_entry_record("", "ENTRY_BLOCKED", "", 0, 0, "slots=0")
            return

        for symbol in self.active_symbols():
            # LIVE 진입이 허용된 경우에만 실제 슬롯 제한으로 스캔을 멈춘다.
            # pause 상태에서는 준P Shadow 후보 수집을 위해 전체 스캔을 계속한다.
            if slots <= 0 and not live_entry_paused:
                break
            if symbol in open_symbols:
                continue
            if self.symbol_in_cooldown(symbol):
                continue

            try:
                strategy, score, details = candidate_signal(
                    self.client, symbol, self.cfg
                )
                # 동일 스캔의 모든 종목이 정확히 같은 BTC/ETH snapshot을 공유한다.
                details.update(market_snapshot)
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

                # v4.3.58: LIVE pause 여부와 무관하게 연구용 P/준P/near-miss Shadow를 등록한다.
                self._register_research_shadow_candidates(symbol, details)

                # v4.3.53: 준P형은 실제 진입과 완전히 분리된 Shadow로만 등록한다.
                if self.cfg.junp_shadow_enabled and bool(details.get("junp_shadow_candidate")):
                    self._register_junp_shadow_candidate(symbol, details)

                if not strategy:
                    continue

                # v4.3.57: pause_new_entries는 실제 LIVE 주문만 차단한다.
                # SCAN/정식 P 신호 기록/준P Shadow 등록은 위에서 정상 수행된다.
                if live_entry_paused:
                    append_entry_record(
                        symbol, "ENTRY_BLOCKED", strategy, score, price,
                        "pause_new_entries=1; scan_continued=1; junp_shadow=0"
                    )
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
        self.update_be_shadow_reviews()
        self.update_junp_shadow_reviews()
        self.scan_entries()

    def _scan_entries_background(self) -> None:
        """신규진입 스캔을 관리루프와 분리한다.

        스캔/API 호출이 오래 걸려도 보유 포지션 manage()는 계속 돌 수 있게 한다.
        동시에 두 개의 스캔이 겹치지 않도록 run_forever에서 단일 스레드만 허용한다.
        """
        try:
            state_set("scan_worker_status", "RUNNING")
            state_set("scan_worker_started_at", datetime.now(timezone.utc).isoformat())
            self.scan_entries()
            state_set("scan_worker_status", "IDLE")
            state_set("scan_worker_finished_at", datetime.now(timezone.utc).isoformat())
        except Exception as exc:
            state_set("scan_worker_status", "ERROR")
            state_set("scan_worker_error", f"{type(exc).__name__}: {exc}")
            log_event("", "SCAN_WORKER_ERROR", mode=self.cfg.mode,
                      details=f"{type(exc).__name__}: {exc}")

    def _stop_reviews_background(self) -> None:
        """손절 리뷰를 메인 포지션 관리루프와 분리한다.

        리뷰용 ticker/API가 느려져도 manage() / TIME_EXIT / TP·SL 관리는 계속 돌 수 있게 한다.
        """
        try:
            state_set("stop_review_worker_status", "RUNNING")
            state_set("stop_review_worker_started_at", datetime.now(timezone.utc).isoformat())
            self.update_stop_reviews()
            self.update_be_shadow_reviews()
            self.update_junp_shadow_reviews()
            self.update_research_shadow_reviews()
            self.update_research_be_shadow_reviews()
            state_set("stop_review_worker_status", "IDLE")
            state_set("stop_review_worker_finished_at", datetime.now(timezone.utc).isoformat())
        except Exception as exc:
            state_set("stop_review_worker_status", "ERROR")
            state_set("stop_review_worker_error", f"{type(exc).__name__}: {exc}")
            log_event("", "STOP_REVIEW_WORKER_ERROR", mode=self.cfg.mode,
                      details=f"{type(exc).__name__}: {exc}")

    def run_forever(self) -> None:
        state_set("bot_process_status", "RUNNING")
        state_set("runtime_version", BOT_RUNTIME_VERSION)
        state_set("runtime_started_at", datetime.now(timezone.utc).isoformat())
        log_event("", "BOT_START", mode=self.cfg.mode, details=json.dumps(asdict(self.cfg), ensure_ascii=False))

        next_scan_at = 0.0
        next_review_at = 0.0
        scan_thread: threading.Thread | None = None
        review_thread: threading.Thread | None = None

        while True:
            loop_started = time.monotonic()
            try:
                state_set("main_loop_heartbeat", datetime.now(timezone.utc).isoformat())
                # 보유 포지션 관리는 항상 메인 루프 최우선.
                self.manage()
                state_set("manage_last_ok_at", datetime.now(timezone.utc).isoformat())

                if state_flag("shutdown_when_flat", False) and not self.open_rows():
                    state_set("bot_process_status", "STOPPED")
                    state_set("shutdown_when_flat", "0")
                    log_event("", "BOT_SAFE_STOP", mode=self.cfg.mode, details="포지션 0 확인 후 안전 종료")
                    break

                now_mono = time.monotonic()

                # 손절 리뷰도 별도 daemon thread에서 실행.
                # 리뷰 API가 느려져도 manage()는 계속 1초 주기로 돈다.
                if now_mono >= next_review_at:
                    if review_thread is None or not review_thread.is_alive():
                        review_thread = threading.Thread(
                            target=self._stop_reviews_background,
                            name="bybit-stop-review",
                            daemon=True,
                        )
                        review_thread.start()
                        next_review_at = now_mono + 30.0
                    else:
                        # 기존 리뷰가 아직 끝나지 않았다면 중첩 실행하지 않는다.
                        next_review_at = now_mono + 5.0

                # 신규진입 스캔은 별도 daemon thread에서 실행.
                # 스캔이 느려져도 manage() / TIME_EXIT은 계속 돈다.
                if now_mono >= next_scan_at:
                    if scan_thread is None or not scan_thread.is_alive():
                        scan_thread = threading.Thread(
                            target=self._scan_entries_background,
                            name="bybit-entry-scan",
                            daemon=True,
                        )
                        scan_thread.start()
                        next_scan_at = now_mono + max(30.0, float(self.cfg.scan_seconds))
                    else:
                        # 기존 스캔이 아직 끝나지 않았다면 겹쳐 실행하지 않는다.
                        next_scan_at = now_mono + 5.0

            except Exception as exc:
                log_event("", "ERROR", mode=self.cfg.mode,
                          details=f"{type(exc).__name__}: {exc}")

            elapsed = time.monotonic() - loop_started
            time.sleep(max(0.1, float(self.cfg.manage_seconds) - elapsed))


# 기존 실행 파일(run_okx_swing_bot.py)과 호환
SwingBot = DailyBot
SwingConfig = DailyConfig
