from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

from bybit import BybitError, get_klines, list_usdt_perpetual_symbols
from strategy import StrategySettings, analyze_symbol

st.set_page_config(
    page_title="HJ Trader",
    page_icon="📈",
    layout="centered",
    initial_sidebar_state="collapsed",
)


def require_password() -> None:
    expected = st.secrets.get("APP_PASSWORD", "")
    if not expected:
        st.error("앱 비밀번호가 아직 설정되지 않았습니다.")
        st.caption("Streamlit Cloud → App settings → Secrets에 APP_PASSWORD를 설정하세요.")
        st.stop()

    if st.session_state.get("authenticated"):
        return

    st.title("🔒 HJ Trader")
    password = st.text_input("비밀번호", type="password")
    if st.button("로그인", use_container_width=True):
        if password == expected:
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("비밀번호가 맞지 않습니다.")
    st.stop()


@st.cache_data(ttl=3600)
def cached_symbols() -> list[str]:
    return list_usdt_perpetual_symbols()


@st.cache_data(ttl=30, show_spinner=False)
def cached_kline(symbol: str) -> pd.DataFrame:
    return get_klines(symbol, interval="15", limit=240)


def scan_one(symbol: str, settings: StrategySettings) -> dict:
    df = cached_kline(symbol)
    return analyze_symbol(symbol, df, settings).to_dict()


require_password()

st.title("📈 HJ Trader")
st.caption("Bybit USDT 무기한 · 15분봉 · 개인 테스트판")

if "watchlist" not in st.session_state:
    st.session_state.watchlist = [
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
        "XRPUSDT",
    ]

with st.expander("⚙️ 감시종목 관리", expanded=False):
    try:
        symbols = cached_symbols()
    except Exception as exc:
        symbols = st.session_state.watchlist
        st.warning(f"종목 목록을 새로 받지 못했습니다: {exc}")

    selected = st.multiselect(
        "검색해서 감시할 종목 선택",
        options=symbols,
        default=[s for s in st.session_state.watchlist if s in symbols],
        placeholder="예: AERGOUSDT",
    )
    if st.button("감시종목 저장", use_container_width=True):
        st.session_state.watchlist = selected
        st.success(f"{len(selected)}개 종목을 저장했습니다.")

    min_score = st.slider("표시할 최소 현재 점수", 1, 5, 3)
    show_only_buy = st.toggle("BUY 신호만 보기", value=True)

col1, col2 = st.columns(2)
with col1:
    scan_clicked = st.button("🔍 지금 스캔", type="primary", use_container_width=True)
with col2:
    if st.button("로그아웃", use_container_width=True):
        st.session_state.authenticated = False
        st.rerun()

if scan_clicked:
    watchlist = st.session_state.watchlist
    if not watchlist:
        st.warning("먼저 감시종목을 추가하세요.")
        st.stop()

    settings = StrategySettings()
    results: list[dict] = []
    errors: list[str] = []

    progress = st.progress(0, text="Bybit 15분봉을 확인하고 있습니다.")
    with ThreadPoolExecutor(max_workers=min(8, len(watchlist))) as pool:
        futures = {
            pool.submit(scan_one, symbol, settings): symbol for symbol in watchlist
        }
        completed = 0
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                results.append(future.result())
            except Exception as exc:
                errors.append(f"{symbol}: {exc}")
            completed += 1
            progress.progress(
                completed / len(watchlist),
                text=f"{completed}/{len(watchlist)} 스캔 완료",
            )
    progress.empty()

    st.session_state.last_results = results
    st.session_state.last_scan = datetime.now(timezone.utc).isoformat()
    st.session_state.last_errors = errors

results = st.session_state.get("last_results", [])
if results:
    df = pd.DataFrame(results)
    filtered = df[df["current_score"] >= min_score].copy()
    if show_only_buy:
        filtered = filtered[filtered["signal_now"]]

    filtered = filtered.sort_values(
        ["signal_now", "current_score", "pullback_score"],
        ascending=[False, False, False],
    )

    scan_time = st.session_state.get("last_scan", "")
    st.subheader(f"BUY 후보 {len(filtered)}개")
    st.caption(f"마지막 스캔(UTC): {scan_time[:19].replace('T', ' ')}")

    if filtered.empty:
        st.info("현재 조건에 맞는 BUY 신호가 없습니다.")
    else:
        for _, row in filtered.iterrows():
            signal_icon = "🔵" if row["signal"] == "BUY-R" else "🟢"
            with st.container(border=True):
                st.markdown(
                    f"### {signal_icon} {row['symbol']} · {row['signal']}"
                )
                a, b, c = st.columns(3)
                a.metric("현재 점수", f"{int(row['current_score'])}/5")
                b.metric("Pullback", f"{int(row['pullback_score'])}/7")
                c.metric("RSI", f"{row['rsi']:.1f}")

                st.write(
                    f"**상태:** {row['entry_status']}  \n"
                    f"**BUY:** {row['buy_price']:.10g}  \n"
                    f"**MAX:** {row['max_entry_price']:.10g}  \n"
                    f"**근거:** {row['reasons']}"
                )
                chart_url = (
                    "https://www.bybit.com/trade/usdt/"
                    + row["symbol"].replace("USDT", "/USDT")
                )
                st.link_button(
                    "Bybit 차트 열기",
                    chart_url,
                    use_container_width=True,
                )

errors = st.session_state.get("last_errors", [])
if errors:
    with st.expander(f"⚠️ 스캔 오류 {len(errors)}개"):
        for message in errors:
            st.write(message)

st.divider()
st.caption(
    "V1 검증판입니다. 주문을 실행하지 않으며 Bybit 공개 시세만 읽습니다. "
    "TradingView와 BUY 발생 시점이 맞는지 먼저 비교하세요."
)
