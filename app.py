from __future__ import annotations

import hashlib
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import extra_streamlit_components as stx
import pandas as pd
import streamlit as st

from bybit import get_klines, get_ticker, list_usdt_perpetual_symbols
from strategy import StrategySettings, analyze_symbol, evaluate_live_entry

st.set_page_config(page_title="HJ Trader", page_icon="📈", layout="centered", initial_sidebar_state="collapsed")
DB_PATH = Path(__file__).with_name("hyejin_trader.db")
DEFAULT_WATCHLIST: list[str] = []


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=20, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, candle_time_utc TEXT,
            scanned_at_utc TEXT, signal TEXT, signal_now INTEGER, buy_price REAL,
            current_score REAL, pullback_score REAL, rsi REAL, stop_price REAL,
            stop_pct REAL, tp_price REAL, entry_status TEXT, reasons TEXT,
            fail_reasons TEXT, UNIQUE(symbol, candle_time_utc)
        );
        CREATE TABLE IF NOT EXISTS positions (
            symbol TEXT PRIMARY KEY, entry_price REAL NOT NULL, entry_time_utc TEXT NOT NULL,
            tp_pct REAL NOT NULL, stop_price REAL NOT NULL, status TEXT NOT NULL DEFAULT 'OPEN'
        );
        CREATE TABLE IF NOT EXISTS position_live (
            symbol TEXT PRIMARY KEY, last_price REAL, pnl_pct REAL, tp_price REAL, stop_price REAL,
            live_action TEXT, trend_action TEXT, hold_score REAL, bars_elapsed INTEGER,
            updated_at_utc TEXT, latest_closed_start_utc TEXT
        );
        CREATE TABLE IF NOT EXISTS position_ticks (
            id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, price REAL, pnl_pct REAL, checked_at_utc TEXT, action TEXT
        );
        CREATE TABLE IF NOT EXISTS position_checks (
            id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, candle_time_utc TEXT,
            checked_at_utc TEXT, close_price REAL, high_price REAL, low_price REAL,
            pnl_pct REAL, hold_score REAL, action TEXT, bars_elapsed INTEGER,
            UNIQUE(symbol, candle_time_utc)
        );
        """)


def get_setting(key: str, default):
    with db() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    if not row:
        return default
    try:
        return json.loads(row["value"])
    except Exception:
        return default


def set_setting(key: str, value) -> None:
    with db() as conn:
        conn.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, json.dumps(value, ensure_ascii=False)),
        )


def auth_token(password: str) -> str:
    return hashlib.sha256(("HJ-TRADER-2026|" + password).encode()).hexdigest()


cookie_manager = stx.CookieManager(key="hj_cookie_manager")


def require_password() -> None:
    expected = st.secrets.get("APP_PASSWORD", "")
    if not expected:
        st.error("APP_PASSWORD가 설정되지 않았습니다.")
        st.stop()
    token = auth_token(expected)
    if st.session_state.get("authenticated") or cookie_manager.get("hj_auth") == token:
        st.session_state.authenticated = True
        return
    st.title("🔒 HJ Trader")
    password = st.text_input("비밀번호", type="password")
    keep = st.checkbox("이 기기에서 7일간 로그인 유지", value=True)
    if st.button("로그인", use_container_width=True):
        if password == expected:
            st.session_state.authenticated = True
            if keep:
                cookie_manager.set("hj_auth", token, max_age=7 * 24 * 60 * 60, key="set_auth")
            st.rerun()
        else:
            st.error("비밀번호가 맞지 않습니다.")
    st.stop()


@st.cache_data(ttl=3600)
def cached_symbols() -> list[str]:
    return list_usdt_perpetual_symbols()


def open_position_symbols() -> set[str]:
    with db() as conn:
        rows = conn.execute(
            "SELECT symbol FROM positions WHERE status='OPEN'"
        ).fetchall()
    return {str(row["symbol"]) for row in rows}


def all_scan_symbols() -> list[str]:
    """전체 USDT 무기한 종목에서 현재 보유 중인 종목만 자동 제외."""
    symbols = cached_symbols()
    excluded = open_position_symbols()
    return [symbol for symbol in symbols if symbol not in excluded]


def scan_one(symbol: str, settings: StrategySettings) -> dict:
    return analyze_symbol(symbol, get_klines(symbol, "15", 240), settings).to_dict()


def save_signal(r: dict) -> None:
    with db() as conn:
        conn.execute(
            """INSERT INTO signals(
                symbol,candle_time_utc,scanned_at_utc,signal,signal_now,buy_price,
                current_score,pullback_score,rsi,stop_price,stop_pct,tp_price,
                entry_status,reasons,fail_reasons
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(symbol,candle_time_utc) DO UPDATE SET
                scanned_at_utc=excluded.scanned_at_utc,
                signal=excluded.signal,
                signal_now=excluded.signal_now,
                buy_price=excluded.buy_price,
                current_score=excluded.current_score,
                pullback_score=excluded.pullback_score,
                rsi=excluded.rsi,
                stop_price=excluded.stop_price,
                stop_pct=excluded.stop_pct,
                tp_price=excluded.tp_price,
                entry_status=excluded.entry_status,
                reasons=excluded.reasons,
                fail_reasons=excluded.fail_reasons""",
            (
                r["symbol"],
                r["candle_time_utc"],
                datetime.now(timezone.utc).isoformat(),
                r["signal"],
                int(r["signal_now"]),
                r["buy_price"],
                r["current_score_10"],
                r["pullback_score_10"],
                r["rsi"],
                r["stop_price"],
                r["stop_pct"],
                r["tp_price"],
                r["entry_status"],
                r["reasons"],
                r["fail_reasons"],
            ),
        )


def do_scan(
    watchlist: list[str], settings: StrategySettings
) -> tuple[list[dict], list[str]]:
    results, errors = [], []
    if not watchlist:
        return results, errors
    with ThreadPoolExecutor(max_workers=min(8, len(watchlist))) as pool:
        futures = {pool.submit(scan_one, s, settings): s for s in watchlist}
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
                save_signal(result)
            except Exception as exc:
                errors.append(f"{futures[future]}: {exc}")
    return results, errors


def save_position(
    symbol: str, entry_price: float, stop_price: float, tp_pct: float
) -> None:
    with db() as conn:
        conn.execute(
            """INSERT INTO positions(
                symbol,entry_price,entry_time_utc,tp_pct,stop_price,status
            )
            VALUES(?,?,?,?,?,'OPEN')
            ON CONFLICT(symbol) DO UPDATE SET
                entry_price=excluded.entry_price,
                entry_time_utc=excluded.entry_time_utc,
                tp_pct=excluded.tp_pct,
                stop_price=excluded.stop_price,
                status='OPEN'""",
            (
                symbol,
                entry_price,
                datetime.now(timezone.utc).isoformat(),
                tp_pct,
                stop_price,
            ),
        )


def monitor_positions() -> list[dict]:
    with db() as conn:
        rows = conn.execute(
            """SELECT
                p.*,
                l.last_price,
                l.pnl_pct,
                l.tp_price,
                l.live_action,
                l.trend_action,
                l.hold_score,
                l.bars_elapsed,
                l.updated_at_utc,
                l.latest_closed_start_utc
            FROM positions p
            LEFT JOIN position_live l ON l.symbol=p.symbol
            WHERE p.status='OPEN'"""
        ).fetchall()
    return [dict(row) for row in rows]


def close_position(symbol: str, status: str) -> None:
    with db() as conn:
        conn.execute(
            "UPDATE positions SET status=? WHERE symbol=?",
            (status, symbol),
        )


init_db()
require_password()

st.title("📈 HJ Trader")
st.caption("v2.5.1 · 전체 USDT 자동 스캔 · 보유 종목 자동 제외 · 진입 후 5초 추적")

min_score = float(get_setting("min_score", 5.0))
show_only_buy = bool(get_setting("show_only_buy", False))
tp_pct = float(get_setting("tp_pct", 1.2))
max_stop_pct = float(get_setting("max_stop_pct", 5.0))

with st.expander("⚙️ 전체 스캔 기준", expanded=False):
    try:
        total_symbols = cached_symbols()
        held_symbols = open_position_symbols()
        scan_symbols = [
            symbol for symbol in total_symbols
            if symbol not in held_symbols
        ]
        st.info(
            f"전체 USDT 무기한 {len(total_symbols)}개 · "
            f"보유 중 자동 제외 {len(held_symbols)}개 · "
            f"현재 스캔 대상 {len(scan_symbols)}개"
        )
        if held_symbols:
            st.caption(
                "자동 제외 중: " + ", ".join(sorted(held_symbols))
            )
    except Exception as exc:
        st.warning(f"종목 목록 조회 오류: {exc}")

    new_min = st.slider(
        "표시할 최소점수(10점 만점)",
        0.0,
        10.0,
        min_score,
        0.5,
    )
    new_only = st.toggle("BUY 신호만 보기", value=show_only_buy)
    new_tp = st.number_input("TP (%)", 0.1, 10.0, tp_pct, 0.1)
    new_stop = st.number_input(
        "구조 손절폭 참고 상한 (%)",
        0.5,
        10.0,
        max_stop_pct,
        0.1,
    )

    if st.button("설정 영구 저장", use_container_width=True):
        set_setting("min_score", new_min)
        set_setting("show_only_buy", new_only)
        set_setting("tp_pct", new_tp)
        set_setting("max_stop_pct", new_stop)
        st.success("전체 종목 스캔 기준을 저장했어요.")
        st.rerun()

col_scan, col_logout = st.columns(2)
manual = col_scan.button(
    "🔍 지금 스캔",
    type="primary",
    use_container_width=True,
)
if col_logout.button("로그아웃", use_container_width=True):
    cookie_manager.delete("hj_auth", key="delete_auth")
    st.session_state.authenticated = False
    st.rerun()

settings = StrategySettings(
    take_profit_pct=tp_pct,
    max_stop_pct=max_stop_pct,
)


@st.fragment(run_every=5)
def auto_scan_panel():
    current_watchlist = all_scan_symbols()
    now_utc = datetime.now(timezone.utc)
    last_bucket = now_utc.strftime("%Y-%m-%dT%H:%M")[:15]
    stored_bucket = get_setting("last_auto_bucket", "")
    minute = now_utc.minute

    should_auto = minute % 15 in (0, 1, 2) and stored_bucket != last_bucket

    if manual or should_auto or "last_results" not in st.session_state:
        with st.spinner("마감된 15분봉을 스캔하고 있습니다..."):
            results, errors = do_scan(current_watchlist, settings)

        st.session_state.last_results = results
        st.session_state.last_errors = errors
        st.session_state.last_scan = datetime.now(timezone.utc).isoformat()

        if should_auto:
            set_setting("last_auto_bucket", last_bucket)

    results = st.session_state.get("last_results", [])
    if not results:
        st.info("아직 표시할 결과가 없습니다.")
        return

    frame = pd.DataFrame(results)
    filtered = frame[
        frame["current_score_10"] >= get_setting("min_score", 5.0)
    ].copy()

    if get_setting("show_only_buy", False):
        filtered = filtered[filtered["signal_now"]]

    filtered = filtered.sort_values(
        ["signal_now", "current_score_10", "pullback_score_10"],
        ascending=[False, False, False],
    )

    st.subheader(f"스캔 결과 {len(filtered)}개")
    st.caption(f"현재 전체 스캔 대상 {len(current_watchlist)}개")
    scan_time = st.session_state.get("last_scan", "")
    st.caption(
        f"마지막 검사 UTC {scan_time[:19].replace('T', ' ')} · "
        "앱을 열어둔 동안 15분봉 마감 직후 자동 검사"
    )

    if filtered.empty:
        st.info(
            "BUY 조건 통과 종목은 없어요. "
            "'BUY 신호만 보기'를 끄면 대기 종목과 탈락 사유를 볼 수 있어요."
        )

    st.caption("LIVE ENTRY v2.4 · 현재 진행봉을 5초마다 다시 확인")

    for _, row in filtered.iterrows():
        try:
            live_df = get_klines(row["symbol"], "15", 3).sort_values("start_time")
            live_candle = live_df.iloc[-1]
            live_price = float(live_candle["close"])
            live_open = float(live_candle["open"])

            live_status, can_enter, diff = evaluate_live_entry(
                signal_now=bool(row["signal_now"]),
                signal_price=float(row["buy_price"]),
                max_entry_price=float(row["max_entry_price"]),
                live_open=live_open,
                live_price=live_price,
            )
            candle_text = "양봉" if live_price > live_open else "음봉"
        except Exception:
            live_price = float(row["buy_price"])
            live_open = live_price
            diff = 0.0
            can_enter = False
            candle_text = "조회 지연"
            live_status = "현재 봉 조회 지연"

        if not bool(row["signal_now"]):
            icon = "⚪️"
            header_status = "대기"
        elif can_enter:
            icon = "🟢"
            header_status = f"{row['signal']} · 지금 진입 가능"
        else:
            icon = "🟡"
            header_status = f"{row['signal']} 후보 · 진입 보류"

        with st.container(border=True):
            st.markdown(f"### {icon} {row['symbol']} · {header_status}")

            metric_score, metric_pullback, metric_rsi = st.columns(3)
            metric_score.metric(
                "현재점수",
                f"{row['current_score_10']:.1f}/10",
            )
            metric_pullback.metric(
                "눌림점수",
                f"{row['pullback_score_10']:.1f}/10",
            )
            metric_rsi.metric("RSI", f"{row['rsi']:.1f}")

            close_kst = (
                pd.Timestamp(row["candle_time_utc"])
                + pd.Timedelta(minutes=15)
            ).tz_convert("Asia/Seoul")

            st.write(
                f"**마감 신호:** "
                f"{row['signal'] if row['signal_now'] else '대기'} · "
                f"**최종 진입 판단:** {live_status}  \n"
                f"**현재 진행봉:** {candle_text} · 시가 {live_open:.10g}  \n"
                f"**신호가:** {row['buy_price']:.10g} · "
                f"**현재가:** {live_price:.10g} ({diff:+.2f}%)  \n"
                f"**TP:** {row['tp_price']:.10g} · "
                f"**비상 STOP 참고:** {row['stop_price']:.10g} "
                f"(-{row['stop_pct']:.2f}%)  \n"
                f"**봉 마감(KST):** {close_kst.strftime('%m-%d %H:%M')}  \n"
                f"**통과:** {row['reasons']}  \n"
                f"**주의:** {row['fail_reasons']}"
            )

            if can_enter:
                entered = st.checkbox(
                    "이 BUY에 실제 진입했어요",
                    key=f"entered_{row['symbol']}_{row['candle_time_utc']}",
                )

                if entered:
                    entry_price = st.number_input(
                        "내 평단가",
                        min_value=0.0,
                        value=float(row["buy_price"]),
                        format="%.10f",
                        key=f"ep_{row['symbol']}",
                    )

                    if st.button(
                        "평단 저장 및 TP/STOP 관리 시작",
                        key=f"save_{row['symbol']}",
                        use_container_width=True,
                    ):
                        save_position(
                            row["symbol"],
                            entry_price,
                            float(row["stop_price"]),
                            tp_pct,
                        )
                        st.success("포지션 관리에 저장했어요.")
            else:
                st.caption(
                    "진입 보류 상태에서는 진입 체크박스를 표시하지 않습니다."
                )

            st.link_button(
                "Bybit 차트 열기",
                "https://www.bybit.com/trade/usdt/"
                + row["symbol"].replace("USDT", "/USDT"),
                use_container_width=True,
            )

    for error in st.session_state.get("last_errors", []):
        st.warning(error)


auto_scan_panel()

st.divider()
st.subheader("📌 실시간 포지션 관리")
st.caption(
    "tracker.py가 서버에서 5초마다 현재가를 기록합니다. "
    "보유 중인 종목은 전체 스캔에서 자동 제외되며, 포지션 종료 후 다음 스캔부터 자동 재포함됩니다."
)


@st.fragment(run_every=5)
def live_positions_panel():
    positions = monitor_positions()

    if not positions:
        st.info("관리 중인 포지션이 없습니다.")

    for position in positions:
        with st.container(border=True):
            last_price = position.get("last_price")
            pnl_pct = position.get("pnl_pct")
            bars_elapsed = position.get("bars_elapsed") or 0
            live_action = (
                position.get("live_action")
                or "실시간 추적 시작 대기"
            )
            trend_action = (
                position.get("trend_action")
                or "마감봉 판단 대기"
            )

            st.markdown(
                f"### {position['symbol']} · {live_action}"
            )

            metric_pnl, metric_trend, metric_bars = st.columns(3)
            metric_pnl.metric(
                "실시간 손익",
                "조회 중" if pnl_pct is None else f"{pnl_pct:+.2f}%",
            )
            metric_trend.metric("마감봉 추세", trend_action)
            metric_bars.metric("경과", f"{bars_elapsed}/6봉")

            current_text = (
                "조회 중"
                if last_price is None
                else format(last_price, ".10g")
            )
            tp_text = (
                "조회 중"
                if position.get("tp_price") is None
                else format(position["tp_price"], ".10g")
            )
            updated_text = str(
                position.get("updated_at_utc") or "tracker 대기"
            )[:19].replace("T", " ")

            st.write(
                f"평단 {position['entry_price']:.10g} · "
                f"현재 {current_text}  \n"
                f"TP {tp_text} · "
                f"비상 STOP {position['stop_price']:.10g}  \n"
                f"업데이트 UTC {updated_text}"
            )

            button_tp, button_stop, button_close, button_cancel = st.columns(4)

            if button_tp.button(
                "익절 완료",
                key=f"tpdone_{position['symbol']}",
            ):
                close_position(position["symbol"], "TP_MANUAL")
                st.rerun()

            if button_stop.button(
                "손절 완료",
                key=f"stdone_{position['symbol']}",
            ):
                close_position(position["symbol"], "STOP_MANUAL")
                st.rerun()

            if button_close.button(
                "수동 종료",
                key=f"closed_{position['symbol']}",
            ):
                close_position(position["symbol"], "CLOSED")
                st.rerun()

            if button_cancel.button(
                "잘못 저장",
                key=f"cancel_{position['symbol']}",
            ):
                close_position(position["symbol"], "CANCELLED")
                st.rerun()


live_positions_panel()

with st.expander("📊 축적 데이터 요약"):
    with db() as conn:
        total = conn.execute(
            "SELECT COUNT(*) c FROM signals"
        ).fetchone()["c"]
        buys = conn.execute(
            "SELECT COUNT(*) c FROM signals WHERE signal_now=1"
        ).fetchone()["c"]
        checks = conn.execute(
            "SELECT COUNT(*) c FROM position_checks"
        ).fetchone()["c"]
        recent = pd.read_sql_query(
            """SELECT
                symbol,
                candle_time_utc,
                signal,
                current_score,
                pullback_score,
                rsi,
                entry_status
            FROM signals
            ORDER BY id DESC
            LIMIT 30""",
            conn,
        )

    st.write(
        f"저장된 스캔 {total}건 · "
        f"BUY 후보 {buys}건 · "
        f"포지션 추적 {checks}봉"
    )

    if not recent.empty:
        st.dataframe(
            recent,
            use_container_width=True,
            hide_index=True,
        )

st.caption("자동 주문은 하지 않습니다. 거래소 보호주문은 별도로 설정하세요.")
