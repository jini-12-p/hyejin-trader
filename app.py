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
from strategy import StrategySettings, analyze_symbol
from trade_records import STRATEGY_VERSION, complete_trade, make_trade_id, migrate_trade_schema, write_log

st.set_page_config(page_title="HJ Trader", page_icon="📈", layout="centered", initial_sidebar_state="collapsed")
DB_PATH = Path(__file__).with_name("hyejin_trader.db")
DEFAULT_WATCHLIST = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]


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
        conn.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, json.dumps(value, ensure_ascii=False)))


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


def scan_one(symbol: str, settings: StrategySettings) -> dict:
    return analyze_symbol(symbol, get_klines(symbol, "15", 240), settings).to_dict()


def save_signal(r: dict) -> None:
    with db() as conn:
        conn.execute("""INSERT INTO signals(symbol,candle_time_utc,scanned_at_utc,signal,signal_now,buy_price,current_score,pullback_score,rsi,stop_price,stop_pct,tp_price,entry_status,reasons,fail_reasons)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(symbol,candle_time_utc) DO UPDATE SET
        scanned_at_utc=excluded.scanned_at_utc, signal=excluded.signal, signal_now=excluded.signal_now,
        buy_price=excluded.buy_price,current_score=excluded.current_score,pullback_score=excluded.pullback_score,
        rsi=excluded.rsi,stop_price=excluded.stop_price,stop_pct=excluded.stop_pct,tp_price=excluded.tp_price,
        entry_status=excluded.entry_status,reasons=excluded.reasons,fail_reasons=excluded.fail_reasons""",
        (r["symbol"],r["candle_time_utc"],datetime.now(timezone.utc).isoformat(),r["signal"],int(r["signal_now"]),r["buy_price"],r["current_score_10"],r["pullback_score_10"],r["rsi"],r["stop_price"],r["stop_pct"],r["tp_price"],r["entry_status"],r["reasons"],r["fail_reasons"]))


def do_scan(watchlist: list[str], settings: StrategySettings) -> tuple[list[dict], list[str]]:
    results, errors = [], []
    if not watchlist:
        return results, errors
    with ThreadPoolExecutor(max_workers=min(8, len(watchlist))) as pool:
        futures = {pool.submit(scan_one, s, settings): s for s in watchlist}
        for f in as_completed(futures):
            try:
                r = f.result(); results.append(r); save_signal(r)
            except Exception as exc:
                errors.append(f"{futures[f]}: {exc}")
    return results, errors


def save_position(symbol: str, entry_price: float, stop_price: float, tp_pct: float,
                  recommendation_rank: int | None = None, recommendation_score: float | None = None,
                  pullback_score: float | None = None, rsi: float | None = None,
                  signal: str | None = None, signal_candle_time_utc: str | None = None) -> str:
    trade_id = make_trade_id(symbol)
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        conn.execute("""INSERT INTO positions(
            symbol,entry_price,entry_time_utc,tp_pct,stop_price,status,trade_id,
            recommendation_rank,recommendation_score,pullback_score,rsi,signal,
            signal_candle_time_utc,strategy_version
        ) VALUES(?,?,?,?,?,'OPEN',?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol) DO UPDATE SET
            entry_price=excluded.entry_price,entry_time_utc=excluded.entry_time_utc,
            tp_pct=excluded.tp_pct,stop_price=excluded.stop_price,status='OPEN',
            trade_id=excluded.trade_id,recommendation_rank=excluded.recommendation_rank,
            recommendation_score=excluded.recommendation_score,pullback_score=excluded.pullback_score,
            rsi=excluded.rsi,signal=excluded.signal,signal_candle_time_utc=excluded.signal_candle_time_utc,
            strategy_version=excluded.strategy_version""",
        (symbol,entry_price,now,tp_pct,stop_price,trade_id,recommendation_rank,recommendation_score,
         pullback_score,rsi,signal,signal_candle_time_utc,STRATEGY_VERSION))
    write_log(f"OPEN {trade_id} {symbol} entry={entry_price} stop={stop_price} tp_pct={tp_pct}")
    return trade_id


def monitor_positions() -> list[dict]:
    with db() as conn:
        rows=conn.execute("""SELECT p.*,l.last_price,l.pnl_pct,l.tp_price,l.live_action,l.trend_action,
        l.hold_score,l.bars_elapsed,l.updated_at_utc,l.latest_closed_start_utc
        FROM positions p LEFT JOIN position_live l ON l.symbol=p.symbol WHERE p.status='OPEN'""").fetchall()
    return [dict(r) for r in rows]


def close_position(symbol: str, status: str) -> None:
    with db() as conn:
        conn.execute("UPDATE positions SET status=? WHERE symbol=?",(status,symbol))


init_db(); migrate_trade_schema(); require_password()
st.title("📈 HJ Trader")
st.caption("마감봉 신호 + Bybit 실시간 현재가 · 진입 후 5초 추적")

watchlist = get_setting("watchlist", DEFAULT_WATCHLIST)
min_score = float(get_setting("min_score", 5.0))
show_only_buy = bool(get_setting("show_only_buy", False))
tp_pct = float(get_setting("tp_pct", 1.2))
max_stop_pct = float(get_setting("max_stop_pct", 5.0))

with st.expander("⚙️ 감시종목 및 기준", expanded=False):
    try: symbols = cached_symbols()
    except Exception as exc:
        symbols = watchlist; st.warning(f"종목 목록 오류: {exc}")
    selected = st.multiselect("감시종목", options=symbols, default=[s for s in watchlist if s in symbols], placeholder="예: AERGOUSDT")
    new_min = st.slider("표시할 최소점수(10점 만점)", 0.0, 10.0, min_score, 0.5)
    new_only = st.toggle("BUY 신호만 보기", value=show_only_buy)
    new_tp = st.number_input("TP (%)", 0.1, 10.0, tp_pct, 0.1)
    new_stop = st.number_input("구조 손절폭 참고 상한 (%)", 0.5, 10.0, max_stop_pct, 0.1)
    if st.button("설정 영구 저장", use_container_width=True):
        set_setting("watchlist",selected); set_setting("min_score",new_min); set_setting("show_only_buy",new_only); set_setting("tp_pct",new_tp); set_setting("max_stop_pct",new_stop)
        st.success("저장했어요. 다시 로그인하거나 서버가 재시작돼도 유지됩니다."); st.rerun()

c1,c2=st.columns(2)
manual = c1.button("🔍 지금 스캔", type="primary", use_container_width=True)
if c2.button("로그아웃", use_container_width=True):
    cookie_manager.delete("hj_auth", key="delete_auth"); st.session_state.authenticated=False; st.rerun()

settings=StrategySettings(take_profit_pct=tp_pct,max_stop_pct=max_stop_pct)

@st.fragment(run_every=60)
def auto_scan_panel():
    current_watchlist=get_setting("watchlist",DEFAULT_WATCHLIST)
    last_bucket=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")[:15]
    stored_bucket=get_setting("last_auto_bucket","")
    minute=datetime.now(timezone.utc).minute
    should_auto = minute % 15 in (0,1,2) and stored_bucket != last_bucket
    if manual or should_auto or "last_results" not in st.session_state:
        with st.spinner("마감된 15분봉을 스캔하고 있습니다..."):
            results,errors=do_scan(current_watchlist,settings)
        st.session_state.last_results=results; st.session_state.last_errors=errors
        st.session_state.last_scan=datetime.now(timezone.utc).isoformat()
        if should_auto: set_setting("last_auto_bucket",last_bucket)
    results=st.session_state.get("last_results",[])
    if not results:
        st.info("아직 표시할 결과가 없습니다."); return
    frame=pd.DataFrame(results)
    filtered=frame[frame["current_score_10"]>=get_setting("min_score",5.0)].copy()
    if get_setting("show_only_buy",False): filtered=filtered[filtered["signal_now"]]
    filtered=filtered.sort_values(["signal_now","current_score_10","pullback_score_10"],ascending=[False,False,False]).reset_index(drop=True)
    filtered["recommendation_rank"] = range(1, len(filtered) + 1)
    st.subheader(f"스캔 결과 {len(filtered)}개")
    scan_time=st.session_state.get("last_scan","")
    st.caption(f"마지막 검사 UTC {scan_time[:19].replace('T',' ')} · 앱을 열어둔 동안 15분봉 마감 직후 자동 검사")
    if filtered.empty:
        st.info("BUY 조건 통과 종목은 없어요. 'BUY 신호만 보기'를 끄면 대기 종목과 탈락 사유를 볼 수 있어요.")
    for _,r in filtered.iterrows():
        icon="🟢" if r["signal_now"] else "⚪️"
        with st.container(border=True):
            st.markdown(f"### {icon} 추천 {int(r['recommendation_rank'])}위 · {r['symbol']} · {r['signal'] if r['signal']!='-' else '대기'}")
            a,b,c=st.columns(3); a.metric("현재점수",f"{r['current_score_10']:.1f}/10"); b.metric("눌림점수",f"{r['pullback_score_10']:.1f}/10"); c.metric("RSI",f"{r['rsi']:.1f}")
            try:
                live_price=get_ticker(r['symbol']); diff=(live_price-float(r['buy_price']))/float(r['buy_price'])*100
                if diff>0.6: live_entry='추격 대기'
                elif diff<-1.5: live_entry='신호 약화 확인'
                else: live_entry='실시간 진입 가능 범위'
            except Exception:
                live_price=float(r['buy_price']); diff=0.0; live_entry='현재가 조회 지연'
            close_kst=(pd.Timestamp(r['candle_time_utc'])+pd.Timedelta(minutes=15)).tz_convert('Asia/Seoul')
            st.write(f"**마감 신호:** {r['entry_status']} · **실시간:** {live_entry}  \n**신호가:** {r['buy_price']:.10g} · **현재가:** {live_price:.10g} ({diff:+.2f}%)  \n**TP:** {r['tp_price']:.10g} · **비상 STOP 참고:** {r['stop_price']:.10g} (-{r['stop_pct']:.2f}%)  \n**봉 마감(KST):** {close_kst.strftime('%m-%d %H:%M')}  \n**통과:** {r['reasons']}  \n**주의:** {r['fail_reasons']}")
            entered=st.checkbox("이 BUY에 실제 진입했어요",key=f"entered_{r['symbol']}_{r['candle_time_utc']}")
            if entered:
                ep=st.number_input("내 평단가",min_value=0.0,value=float(r['buy_price']),format="%.10f",key=f"ep_{r['symbol']}")
                if st.button("평단 저장 및 TP/STOP 관리 시작",key=f"save_{r['symbol']}",use_container_width=True):
                    trade_id=save_position(r['symbol'],ep,float(r['stop_price']),tp_pct,int(r['recommendation_rank']),float(r['current_score_10']),float(r['pullback_score_10']),float(r['rsi']),str(r['signal']),str(r['candle_time_utc'])); st.success(f"포지션 관리에 저장했어요. Trade ID: {trade_id}")
            st.link_button("Bybit 차트 열기","https://www.bybit.com/trade/usdt/"+r["symbol"].replace("USDT","/USDT"),use_container_width=True)
    for e in st.session_state.get("last_errors",[]): st.warning(e)

auto_scan_panel()

st.divider(); st.subheader("📌 실시간 포지션 관리")
st.caption("tracker.py가 서버에서 5초마다 현재가를 기록합니다. TP/비상 STOP은 진입 이후 실시간 가격, 추세 판단은 마감봉 기준입니다.")
@st.fragment(run_every=5)
def live_positions_panel():
    positions=monitor_positions()
    if not positions:
        st.info("관리 중인 포지션이 없습니다.")
    for p in positions:
        with st.container(border=True):
            lp=p.get('last_price'); pnl=p.get('pnl_pct'); bars=p.get('bars_elapsed') or 0
            live=p.get('live_action') or '실시간 추적 시작 대기'; trend=p.get('trend_action') or '마감봉 판단 대기'
            st.markdown(f"### {p['symbol']} · {live}")
            a,b,c=st.columns(3)
            a.metric("실시간 손익", "조회 중" if pnl is None else f"{pnl:+.2f}%")
            b.metric("마감봉 추세", trend)
            c.metric("경과", f"{bars}/6봉")
            st.write(f"평단 {p['entry_price']:.10g} · 현재 {'조회 중' if lp is None else format(lp,'.10g')}  \nTP {'조회 중' if p.get('tp_price') is None else format(p['tp_price'],'.10g')} · 비상 STOP {p['stop_price']:.10g}  \n업데이트 UTC {str(p.get('updated_at_utc') or 'tracker 대기')[:19].replace('T',' ')}")
            with st.expander("거래 종료 기록", expanded=False):
                default_exit = float(lp) if lp is not None else float(p['entry_price'])
                exit_type = st.selectbox(
                    "종료 유형",
                    ["TP", "1.2% STOP", "Emergency STOP", "Trend STOP", "Manual exit"],
                    key=f"exit_type_{p['symbol']}"
                )
                exit_price = st.number_input(
                    "실제 종료가", min_value=0.0, value=default_exit, format="%.10f",
                    key=f"exit_price_{p['symbol']}"
                )
                memo = st.text_area("메모(선택)", key=f"exit_memo_{p['symbol']}")
                if st.button("종료 확정 및 기록 저장", type="primary", use_container_width=True, key=f"finish_{p['symbol']}"):
                    try:
                        result = complete_trade(p['symbol'], exit_type, exit_price, memo)
                        st.success(f"저장 완료 · 손익 {result['pnl_pct']:+.2f}% · Trade ID {result['trade_id']}")
                        st.rerun()
                    except Exception as exc:
                        st.error(f"종료 기록 저장 실패: {exc}")
            if st.button("잘못 저장한 포지션 취소", key=f"cancel_{p['symbol']}", use_container_width=True):
                close_position(p['symbol'],'CANCELLED'); st.rerun()
live_positions_panel()

with st.expander("📊 축적 데이터 요약"):
    with db() as conn:
        total=conn.execute("SELECT COUNT(*) c FROM signals").fetchone()["c"]
        buys=conn.execute("SELECT COUNT(*) c FROM signals WHERE signal_now=1").fetchone()["c"]
        checks=conn.execute("SELECT COUNT(*) c FROM position_checks").fetchone()["c"]
        recent=pd.read_sql_query("SELECT symbol,candle_time_utc,signal,current_score,pullback_score,rsi,entry_status FROM signals ORDER BY id DESC LIMIT 30",conn)
    st.write(f"저장된 스캔 {total}건 · BUY 후보 {buys}건 · 포지션 추적 {checks}봉")
    if not recent.empty: st.dataframe(recent,use_container_width=True,hide_index=True)

st.caption("자동 주문은 하지 않습니다. 거래소 보호주문은 별도로 설정하세요.")
