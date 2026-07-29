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

from bybit import get_klines, list_usdt_perpetual_symbols
from strategy import StrategySettings, analyze_symbol

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


def save_position(symbol: str, entry_price: float, stop_price: float, tp_pct: float) -> None:
    with db() as conn:
        conn.execute("INSERT INTO positions(symbol,entry_price,entry_time_utc,tp_pct,stop_price,status) VALUES(?,?,?,?,?,'OPEN') ON CONFLICT(symbol) DO UPDATE SET entry_price=excluded.entry_price,entry_time_utc=excluded.entry_time_utc,tp_pct=excluded.tp_pct,stop_price=excluded.stop_price,status='OPEN'", (symbol,entry_price,datetime.now(timezone.utc).isoformat(),tp_pct,stop_price))


def monitor_positions() -> list[dict]:
    with db() as conn:
        positions = conn.execute("SELECT * FROM positions WHERE status='OPEN'").fetchall()
    output = []
    for p in positions:
        try:
            raw = get_klines(p["symbol"], "15", 120)
            result = analyze_symbol(p["symbol"], raw, StrategySettings()).to_dict()
            last = raw.iloc[-2] if len(raw) >= 2 else raw.iloc[-1]
            entry = float(p["entry_price"]); current = float(last.close)
            pnl = (current-entry)/entry*100; tp = entry*(1+float(p["tp_pct"])/100); stop=float(p["stop_price"])
            bars = max(0, int((pd.Timestamp(last.start_time)-pd.Timestamp(p["entry_time_utc"]))/pd.Timedelta(minutes=15)))
            if float(last.high) >= tp: action="TP 도달"
            elif float(last.low) <= stop: action="STOP 도달"
            elif bars >= 6: action="6봉 종료"
            elif result["current_score_10"] >= 8: action="HOLD"
            elif result["current_score_10"] >= 6: action="주의"
            else: action="STOP 권고"
            with db() as conn:
                conn.execute("INSERT OR IGNORE INTO position_checks(symbol,candle_time_utc,checked_at_utc,close_price,high_price,low_price,pnl_pct,hold_score,action,bars_elapsed) VALUES(?,?,?,?,?,?,?,?,?,?)", (p["symbol"],pd.Timestamp(last.start_time).isoformat(),datetime.now(timezone.utc).isoformat(),current,float(last.high),float(last.low),pnl,result["current_score_10"],action,bars))
            output.append({"symbol":p["symbol"],"entry":entry,"current":current,"pnl":pnl,"tp":tp,"stop":stop,"score":result["current_score_10"],"action":action,"bars":bars})
        except Exception as exc:
            output.append({"symbol":p["symbol"],"error":str(exc)})
    return output


init_db(); require_password()
st.title("📈 HJ Trader")
st.caption("Bybit USDT 무기한 · 마감된 15분봉 기준 · 설정/신호/포지션 자동 저장")

watchlist = get_setting("watchlist", DEFAULT_WATCHLIST)
min_score = float(get_setting("min_score", 5.0))
show_only_buy = bool(get_setting("show_only_buy", False))
tp_pct = float(get_setting("tp_pct", 1.2))
max_stop_pct = float(get_setting("max_stop_pct", 1.0))

with st.expander("⚙️ 감시종목 및 기준", expanded=False):
    try: symbols = cached_symbols()
    except Exception as exc:
        symbols = watchlist; st.warning(f"종목 목록 오류: {exc}")
    selected = st.multiselect("감시종목", options=symbols, default=[s for s in watchlist if s in symbols], placeholder="예: AERGOUSDT")
    new_min = st.slider("표시할 최소점수(10점 만점)", 0.0, 10.0, min_score, 0.5)
    new_only = st.toggle("BUY 신호만 보기", value=show_only_buy)
    new_tp = st.number_input("TP (%)", 0.1, 10.0, tp_pct, 0.1)
    new_stop = st.number_input("최대 허용 손절폭 (%)", 0.2, 5.0, max_stop_pct, 0.1)
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
    filtered=filtered.sort_values(["signal_now","current_score_10","pullback_score_10"],ascending=[False,False,False])
    st.subheader(f"스캔 결과 {len(filtered)}개")
    scan_time=st.session_state.get("last_scan","")
    st.caption(f"마지막 검사 UTC {scan_time[:19].replace('T',' ')} · 앱을 열어둔 동안 15분봉 마감 직후 자동 검사")
    if filtered.empty:
        st.info("BUY 조건 통과 종목은 없어요. 'BUY 신호만 보기'를 끄면 대기 종목과 탈락 사유를 볼 수 있어요.")
    for _,r in filtered.iterrows():
        icon="🟢" if r["signal_now"] else "⚪️"
        with st.container(border=True):
            st.markdown(f"### {icon} {r['symbol']} · {r['signal'] if r['signal']!='-' else '대기'}")
            a,b,c=st.columns(3); a.metric("현재점수",f"{r['current_score_10']:.1f}/10"); b.metric("눌림점수",f"{r['pullback_score_10']:.1f}/10"); c.metric("RSI",f"{r['rsi']:.1f}")
            st.write(f"**상태:** {r['entry_status']}  \n**신호가:** {r['buy_price']:.10g}  \n**TP:** {r['tp_price']:.10g}  \n**STOP:** {r['stop_price']:.10g} (-{r['stop_pct']:.2f}%)  \n**마감봉:** {r['candle_time_utc'][:16].replace('T',' ')} UTC  \n**통과:** {r['reasons']}  \n**탈락/주의:** {r['fail_reasons']}")
            entered=st.checkbox("이 BUY에 실제 진입했어요",key=f"entered_{r['symbol']}_{r['candle_time_utc']}")
            if entered:
                ep=st.number_input("내 평단가",min_value=0.0,value=float(r['buy_price']),format="%.10f",key=f"ep_{r['symbol']}")
                if st.button("평단 저장 및 TP/STOP 관리 시작",key=f"save_{r['symbol']}",use_container_width=True):
                    save_position(r['symbol'],ep,float(r['stop_price']),tp_pct); st.success("포지션 관리에 저장했어요.")
            st.link_button("Bybit 차트 열기","https://www.bybit.com/trade/usdt/"+r["symbol"].replace("USDT","/USDT"),use_container_width=True)
    for e in st.session_state.get("last_errors",[]): st.warning(e)

auto_scan_panel()

st.divider(); st.subheader("📌 진입 포지션 6봉 관리")
for p in monitor_positions():
    with st.container(border=True):
        if "error" in p: st.error(f"{p['symbol']}: {p['error']}"); continue
        st.markdown(f"### {p['symbol']} · {p['action']}")
        a,b,c=st.columns(3); a.metric("손익률",f"{p['pnl']:+.2f}%"); b.metric("HOLD 점수",f"{p['score']:.1f}/10"); c.metric("경과",f"{p['bars']}/6봉")
        st.write(f"평단 {p['entry']:.10g} · 현재 {p['current']:.10g}  \nTP {p['tp']:.10g} · STOP {p['stop']:.10g}")

with st.expander("📊 축적 데이터 요약"):
    with db() as conn:
        total=conn.execute("SELECT COUNT(*) c FROM signals").fetchone()["c"]
        buys=conn.execute("SELECT COUNT(*) c FROM signals WHERE signal_now=1").fetchone()["c"]
        checks=conn.execute("SELECT COUNT(*) c FROM position_checks").fetchone()["c"]
        recent=pd.read_sql_query("SELECT symbol,candle_time_utc,signal,current_score,pullback_score,rsi,entry_status FROM signals ORDER BY id DESC LIMIT 30",conn)
    st.write(f"저장된 스캔 {total}건 · BUY 후보 {buys}건 · 포지션 추적 {checks}봉")
    if not recent.empty: st.dataframe(recent,use_container_width=True,hide_index=True)

st.caption("자동 주문은 하지 않습니다. HOLD/STOP은 데이터 축적용 보조 판단이며 실제 진입 전 검증이 필요합니다.")
