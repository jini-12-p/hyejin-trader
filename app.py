from __future__ import annotations

import hashlib
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import extra_streamlit_components as stx
import pandas as pd
import requests
import streamlit as st

from bybit import (
    BybitError,
    get_klines,
    get_linear_tickers,
    get_ticker,
    list_usdt_perpetual_symbols,
    private_api_configured,
    get_unified_wallet_balance,
    get_open_linear_positions,
    place_long_market_with_risk,
)
from strategy import StrategySettings, analyze_symbol, evaluate_live_entry, analyze_position_health

st.set_page_config(page_title="HJ Trader", page_icon="📈", layout="centered", initial_sidebar_state="collapsed")
DB_PATH = Path(__file__).with_name("hyejin_trader.db")
APP_VERSION = "v3.6.0"
TOP_GAINER_LIMIT = 30
STOCK_SCAN_LIMIT = 10
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


@st.cache_data(ttl=3600)
def cached_symbol_types() -> dict[str, str]:
    """Bybit 상품 분류를 가져옵니다. 실패하면 안전하게 코인형으로 처리합니다."""
    type_map: dict[str, str] = {}
    cursor = ""
    try:
        while True:
            params = {"category": "linear", "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            response = requests.get(
                "https://api.bybit.com/v5/market/instruments-info",
                params=params,
                timeout=15,
            )
            response.raise_for_status()
            payload = response.json()
            if payload.get("retCode") != 0:
                break
            result = payload.get("result", {})
            for row in result.get("list", []):
                symbol = str(row.get("symbol", ""))
                raw_type = str(row.get("symbolType", "")).lower()
                if symbol:
                    if raw_type == "stock":
                        type_map[symbol] = "stock"
                    elif raw_type == "forex":
                        type_map[symbol] = "forex"
                    else:
                        type_map[symbol] = "crypto"
            cursor = result.get("nextPageCursor", "")
            if not cursor:
                break
    except Exception:
        return {}
    return type_map


def enrich_rotation_metrics(result: dict, klines: pd.DataFrame, symbol_type: str) -> dict:
    """이미 받은 15분봉으로 회전력 지표를 계산해 API 호출을 늘리지 않습니다."""
    out = dict(result)
    df = klines.sort_values("start_time").copy()
    if len(df) > 1:
        df = df.iloc[:-1].copy()  # 진행 중 봉 제외

    turnover = pd.to_numeric(df.get("turnover"), errors="coerce").fillna(0.0)
    close = pd.to_numeric(df.get("close"), errors="coerce")
    high = pd.to_numeric(df.get("high"), errors="coerce")
    low = pd.to_numeric(df.get("low"), errors="coerce")

    recent4 = float(turnover.tail(4).mean()) if len(turnover) else 0.0
    base20 = float(turnover.iloc[-24:-4].mean()) if len(turnover) >= 24 else float(turnover.mean() or 0.0)
    accel = recent4 / base20 if base20 > 0 else 0.0
    turnover_24h = float(turnover.tail(96).sum())

    range_pct = ((high - low) / close.replace(0, pd.NA) * 100).dropna()
    volatility = float(range_pct.tail(4).mean()) if len(range_pct) else 0.0

    momentum = 0.0
    if len(close.dropna()) >= 5 and float(close.dropna().iloc[-5]) > 0:
        momentum = (float(close.dropna().iloc[-1]) / float(close.dropna().iloc[-5]) - 1) * 100

    out.update({
        "symbol_type": symbol_type if symbol_type in {"stock", "forex"} else "crypto",
        "turnover_24h_est": turnover_24h,
        "turnover_accel": max(0.0, accel),
        "short_volatility": max(0.0, volatility),
        "momentum_1h": momentum,
    })
    return out


def add_rotation_scores(frame: pd.DataFrame) -> pd.DataFrame:
    """유형 안에서 거래대금을 비교한 뒤 모든 유형을 하나의 회전 순위로 합칩니다."""
    if frame.empty:
        return frame
    out = frame.copy()
    for col in ["turnover_24h_est", "turnover_accel", "short_volatility", "momentum_1h"]:
        if col not in out:
            out[col] = 0.0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    if "symbol_type" not in out:
        out["symbol_type"] = "crypto"

    group = out.groupby("symbol_type", dropna=False)
    out["liquidity_pct"] = group["turnover_24h_est"].rank(pct=True, method="average") * 100
    out["accel_pct"] = group["turnover_accel"].rank(pct=True, method="average") * 100
    out["volatility_pct"] = group["short_volatility"].rank(pct=True, method="average") * 100
    out["momentum_pct"] = group["momentum_1h"].rank(pct=True, method="average") * 100

    out["rotation_score"] = (
        out["accel_pct"] * 0.35
        + out["volatility_pct"] * 0.30
        + out["momentum_pct"] * 0.20
        + out["liquidity_pct"] * 0.15
    ).round(1)
    for col in ["change_24h_pct", "turnover24h"]:
        if col not in out:
            out[col] = 0.0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    out["chase_score"] = (
        out["accel_pct"] * 0.40
        + out["momentum_pct"] * 0.30
        + out["liquidity_pct"] * 0.20
        + out["volatility_pct"] * 0.10
    ).round(1)
    out["entry_style"] = out.apply(
        lambda row: "돌파·추격형" if float(row.get("momentum_1h", 0.0)) > 0.4 and float(row.get("turnover_accel", 0.0)) >= 1.15 else "눌림·반등형",
        axis=1,
    )
    out["liquidity_warning"] = out["turnover_24h_est"] < 1_000_000
    return out


def product_badge(symbol_type: str) -> str:
    if symbol_type == "stock":
        return "🟣 주식형"
    if symbol_type == "forex":
        return "🟠 외환형"
    return "🔵 코인"


def open_position_symbols() -> set[str]:
    """로컬 관리목록과 Bybit 실제 보유종목을 합쳐 스캔 제외에 사용합니다."""
    with db() as conn:
        rows = conn.execute(
            "SELECT symbol FROM positions WHERE status='OPEN'"
        ).fetchall()
    symbols = {str(row["symbol"]) for row in rows}
    if private_api_configured():
        try:
            symbols.update(
                str(position["symbol"])
                for position in get_open_linear_positions()
                if position.get("symbol")
            )
        except Exception:
            # 조회 오류가 스캔 전체를 막지 않도록 로컬 목록으로 계속 진행합니다.
            pass
    return symbols


def favorite_symbols() -> list[str]:
    saved = get_setting("favorite_symbols", DEFAULT_WATCHLIST)
    if not isinstance(saved, list):
        return []
    valid = set(cached_symbols())
    return sorted({str(symbol).upper().strip() for symbol in saved if str(symbol).upper().strip() in valid})


def build_scan_universe() -> tuple[list[str], dict[str, dict]]:
    """코인 상승률 TOP30, 주식형 TOP10, 즐겨찾기를 서로 분리해 스캔합니다."""
    tickers = get_linear_tickers()
    valid_symbols = set(cached_symbols())
    symbol_types = cached_symbol_types()
    held = open_position_symbols()

    eligible = tickers[tickers["symbol"].isin(valid_symbols)].copy()
    eligible["symbol_type"] = eligible["symbol"].map(
        lambda symbol: symbol_types.get(str(symbol), "crypto")
    )

    crypto_top = eligible[eligible["symbol_type"] == "crypto"].head(TOP_GAINER_LIMIT).copy()
    crypto_top["gainer_rank"] = range(1, len(crypto_top) + 1)

    stock_top = eligible[eligible["symbol_type"] == "stock"].head(STOCK_SCAN_LIMIT).copy()
    stock_top["stock_rank"] = range(1, len(stock_top) + 1)

    selected = pd.concat([crypto_top, stock_top], ignore_index=True)
    selected_records = selected.to_dict("records")
    ticker_meta = {str(row["symbol"]): row for row in selected_records}

    favorites = favorite_symbols()
    requested = [str(row["symbol"]) for row in selected_records] + favorites
    universe: list[str] = []
    seen: set[str] = set()
    for symbol in requested:
        if symbol in held or symbol in seen:
            continue
        seen.add(symbol)
        universe.append(symbol)

    # TOP 목록 밖의 즐겨찾기에도 24시간 정보와 상품 유형을 붙입니다.
    all_meta = tickers.set_index("symbol").to_dict("index") if not tickers.empty else {}
    crypto_top_symbols = set(crypto_top["symbol"].astype(str))
    stock_top_symbols = set(stock_top["symbol"].astype(str))
    for symbol in universe:
        meta = dict(all_meta.get(symbol, {}))
        meta["symbol_type"] = symbol_types.get(symbol, "crypto")
        meta["is_favorite"] = symbol in favorites
        meta["is_top30"] = symbol in crypto_top_symbols
        meta["is_stock_top"] = symbol in stock_top_symbols
        ticker_meta[symbol] = meta
    return universe, ticker_meta


def scan_one(
    symbol: str,
    settings: StrategySettings,
    symbol_types: dict[str, str],
    ticker_meta: dict[str, dict],
) -> dict:
    klines = get_klines(symbol, "15", 240)
    result = analyze_symbol(symbol, klines, settings).to_dict()
    symbol_type = symbol_types.get(symbol, "crypto")
    result = enrich_rotation_metrics(result, klines, symbol_type)
    meta = ticker_meta.get(symbol, {})
    result.update({
        "change_24h_pct": float(meta.get("change_24h_pct", 0.0) or 0.0),
        "turnover24h": float(meta.get("turnover24h", 0.0) or 0.0),
        "gainer_rank": int(meta.get("gainer_rank", 0) or 0),
        "is_favorite": bool(meta.get("is_favorite", False)),
        "is_top30": bool(meta.get("is_top30", False)),
    })
    return result


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
    watchlist: list[str], settings: StrategySettings, ticker_meta: dict[str, dict]
) -> tuple[list[dict], list[str]]:
    results, errors = [], []
    if not watchlist:
        return results, errors
    symbol_types = cached_symbol_types()
    with ThreadPoolExecutor(max_workers=min(8, len(watchlist))) as pool:
        futures = {
            pool.submit(scan_one, s, settings, symbol_types, ticker_meta): s
            for s in watchlist
        }
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


def latest_signal_for_symbol(symbol: str) -> dict | None:
    with db() as conn:
        row = conn.execute(
            """SELECT *
            FROM signals
            WHERE symbol=?
            ORDER BY id DESC
            LIMIT 1""",
            (symbol,),
        ).fetchone()
    return dict(row) if row else None


init_db()
require_password()

st.title("📈 HJ Trader")
st.caption(f"{APP_VERSION} · BUY TOP10 · Bybit 실제 포지션 자동 동기화")

min_score = float(get_setting("min_score", 5.0))
show_only_buy = bool(get_setting("show_only_buy", False))
tp_pct = float(get_setting("tp_pct", 1.2))
max_stop_pct = float(get_setting("max_stop_pct", 5.0))
default_margin_usdt = float(get_setting("order_margin_usdt", 12.0))
default_leverage = int(get_setting("order_leverage", 5))
hedge_mode = bool(get_setting("bybit_hedge_mode", True))

with st.expander("⚙️ 감시종목 및 기준", expanded=False):
    try:
        total_symbols = cached_symbols()
        held_symbols = open_position_symbols()
        current_favorites = favorite_symbols()
        st.info(
            f"상승률 TOP{TOP_GAINER_LIMIT} + 즐겨찾기 {len(current_favorites)}개를 분석합니다. "
            f"보유 중 {len(held_symbols)}개는 자동 제외됩니다."
        )
        selected_favorites = st.multiselect(
            "⭐ 즐겨찾기 종목",
            options=total_symbols,
            default=current_favorites,
            placeholder="순위가 낮아도 계속 감시할 종목을 선택",
        )
        if held_symbols:
            st.caption("보유 중 자동 제외: " + ", ".join(sorted(held_symbols)))
    except Exception as exc:
        selected_favorites = favorite_symbols()
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
        set_setting("favorite_symbols", selected_favorites)
        st.success("TOP30 스캔 기준과 즐겨찾기를 저장했어요.")
        st.rerun()

with st.expander("🔐 Bybit API 주문 설정", expanded=True):
    if private_api_configured():
        try:
            api_balance = get_unified_wallet_balance()
            st.success(f"Bybit API 연결됨 · USDT 지갑잔액 {api_balance:,.2f}")
        except Exception as exc:
            st.error(f"API 키는 있으나 연결 실패: {exc}")
    else:
        st.error(".env의 BYBIT_API_KEY / BYBIT_API_SECRET을 확인하세요.")

    order_margin = st.number_input(
        "1회 증거금 (USDT)", min_value=1.0, max_value=10000.0,
        value=default_margin_usdt, step=1.0,
        help="실제 주문 명목금액은 증거금 × 레버리지입니다.",
    )
    order_leverage = st.number_input(
        "레버리지", min_value=1, max_value=50,
        value=default_leverage, step=1,
    )
    order_hedge = st.toggle(
        "헤지모드 사용 (LONG positionIdx=1)", value=hedge_mode,
        help="같은 종목 LONG과 SHORT를 동시에 보유하는 계정이면 켜세요.",
    )
    st.caption(f"예상 명목금액: {float(order_margin) * int(order_leverage):,.2f} USDT")
    if st.button("주문 설정 저장", use_container_width=True):
        set_setting("order_margin_usdt", float(order_margin))
        set_setting("order_leverage", int(order_leverage))
        set_setting("bybit_hedge_mode", bool(order_hedge))
        st.success("주문 설정을 저장했습니다.")
        st.rerun()

with st.expander("➕ 수동 등록(비상용)", expanded=False):
    st.caption(
        "Bybit 자동 동기화가 실패했을 때만 사용하세요. 입력값은 로컬 관리용이며 실제 Bybit TP/SL을 변경하지 않습니다."
    )

    try:
        manual_symbols = cached_symbols()
    except Exception:
        manual_symbols = []

    manual_symbol = st.selectbox(
        "매수한 종목",
        options=[""] + manual_symbols,
        index=0,
        placeholder="종목 선택",
        key="manual_position_symbol",
    )

    latest_signal = (
        latest_signal_for_symbol(manual_symbol)
        if manual_symbol
        else None
    )

    default_entry = 0.0
    default_stop = 0.0
    signal_note = "종목을 선택하면 최근 신호의 비상 손절가를 불러옵니다."

    if latest_signal:
        default_entry = float(latest_signal.get("buy_price") or 0.0)
        default_stop = float(latest_signal.get("stop_price") or 0.0)
        signal_time = str(
            latest_signal.get("candle_time_utc") or ""
        )[:19].replace("T", " ")
        signal_note = (
            f"최근 신호 UTC {signal_time} · "
            f"신호 {latest_signal.get('signal') or '대기'} · "
            f"비상 STOP {default_stop:.10g}"
        )

    st.caption(signal_note)

    manual_entry_price = st.number_input(
        "실제 내 평단가",
        min_value=0.0,
        value=default_entry,
        format="%.10f",
        key=f"manual_entry_{manual_symbol}",
    )

    manual_stop_price = st.number_input(
        "비상 손절가",
        min_value=0.0,
        value=default_stop,
        format="%.10f",
        key=f"manual_stop_{manual_symbol}",
    )

    if st.button(
        "이 종목 포지션 관리에 등록",
        type="primary",
        use_container_width=True,
        disabled=(
            not manual_symbol
            or manual_entry_price <= 0
            or manual_stop_price <= 0
        ),
    ):
        save_position(
            manual_symbol,
            float(manual_entry_price),
            float(manual_stop_price),
            tp_pct,
        )
        st.success(
            f"{manual_symbol} 등록 완료 · "
            "이제 전체 스캔에서 자동 제외되고 실시간 관리에 표시됩니다."
        )
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


def render_pending_order_confirmation() -> None:
    pending = st.session_state.get("pending_long_order")
    if not pending:
        return
    symbol = str(pending["symbol"])
    st.error("⚠️ 실제 Bybit 시장가 주문 최종 확인")
    with st.container(border=True):
        st.markdown(f"### {symbol} LONG")
        st.write(
            f"증거금 **{pending['margin_usdt']:.2f} USDT** · "
            f"레버리지 **{pending['leverage']}x** · "
            f"예상 명목금액 **{pending['margin_usdt'] * pending['leverage']:.2f} USDT**"
        )
        st.write(
            f"구조 손절가 **{pending['stop_price']:.10g}** · "
            f"실제 체결 평단 기준 TP **+{pending['tp_pct']:.2f}%**"
        )
        confirm_text = st.text_input(
            "주문하려면 LONG 입력",
            key=f"confirm_text_{symbol}",
            placeholder="LONG",
        )
        c1, c2 = st.columns(2)
        if c1.button("취소", use_container_width=True, key=f"cancel_{symbol}"):
            st.session_state.pop("pending_long_order", None)
            st.rerun()
        if c2.button(
            "실제 주문 실행",
            type="primary",
            use_container_width=True,
            disabled=confirm_text.strip().upper() != "LONG",
            key=f"execute_{symbol}",
        ):
            try:
                with st.spinner(f"{symbol} 주문·체결·TP/SL 등록 중..."):
                    result = place_long_market_with_risk(
                        symbol=symbol,
                        margin_usdt=float(pending["margin_usdt"]),
                        leverage=int(pending["leverage"]),
                        stop_price=float(pending["stop_price"]),
                        tp_pct=float(pending["tp_pct"]),
                        position_idx=1 if bool(pending["hedge_mode"]) else 0,
                    )
                    save_position(
                        symbol,
                        float(result["avg_price"]),
                        float(result["stop_price"]),
                        float(pending["tp_pct"]),
                    )
                st.session_state.pop("pending_long_order", None)
                st.success(
                    f"✅ {symbol} 진입완료 · 평단 {result['avg_price']:.10g} · "
                    f"수량 {result['qty']:.10g} · TP {result['tp_price']:.10g} · "
                    f"STOP {result['stop_price']:.10g}"
                )
            except Exception as exc:
                st.error(f"주문 실패: {exc}")


render_pending_order_confirmation()

@st.fragment(run_every=10)
def auto_scan_panel():
    try:
        current_watchlist, ticker_meta = build_scan_universe()
    except Exception as exc:
        st.error(f"TOP30 종목 조회 오류: {exc}")
        return
    now_utc = datetime.now(timezone.utc)
    last_bucket = now_utc.strftime("%Y-%m-%dT%H:%M")[:15]
    stored_bucket = get_setting("last_auto_bucket", "")
    minute = now_utc.minute

    should_auto = minute % 15 in (0, 1, 2) and stored_bucket != last_bucket

    if manual or should_auto or "last_results" not in st.session_state:
        with st.spinner("마감된 15분봉을 스캔하고 있습니다..."):
            results, errors = do_scan(current_watchlist, settings, ticker_meta)

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

    # 회전/추격 순위는 점수 기준을 통과하지 못한 종목도 포함해 전체 스캔 결과에서 계산합니다.
    # 진입 가능 여부는 각 행의 signal_now와 점수로 별도 표시합니다.
    ranked_all = add_rotation_scores(frame.copy())
    ranked_all = ranked_all.sort_values(
        ["signal_now", "rotation_score", "current_score_10", "pullback_score_10"],
        ascending=[False, False, False, False],
    )

    filtered = ranked_all[
        ranked_all["current_score_10"] >= get_setting("min_score", 5.0)
    ].copy()
    if get_setting("show_only_buy", False):
        filtered = filtered[filtered["signal_now"]]

    st.subheader(f"스캔 결과 {len(filtered)}개 · 전체 분석 {len(ranked_all)}개")
    favorite_count = sum(1 for r in results if bool(r.get("is_favorite", False)))
    st.caption(
        f"현재 분석 대상 {len(current_watchlist)}개 · 코인 TOP30 + "
        f"주식형 TOP{STOCK_SCAN_LIMIT} + 즐겨찾기 {favorite_count}개"
    )
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

    # BUY가 발생한 종목만 선호 차트 점수 순으로 한 번에 표시합니다.
    buy_ranked = ranked_all[ranked_all["signal_now"]].copy()
    if not buy_ranked.empty:
        for col, default in [
            ("preference_score_9", 0),
            ("volume_ratio", 0.0),
            ("signal_diff_pct", 0.0),
        ]:
            if col not in buy_ranked:
                buy_ranked[col] = default

        buy_ranked["preference_score_9"] = pd.to_numeric(
            buy_ranked["preference_score_9"], errors="coerce"
        ).fillna(0)
        buy_ranked["volume_ratio"] = pd.to_numeric(
            buy_ranked["volume_ratio"], errors="coerce"
        ).fillna(0.0)
        buy_ranked["signal_diff_pct"] = pd.to_numeric(
            buy_ranked["signal_diff_pct"], errors="coerce"
        ).fillna(0.0)

        buy_ranked = buy_ranked.sort_values(
            ["preference_score_9", "volume_ratio", "candle_time_utc"],
            ascending=[False, False, False],
        )

    st.markdown("### 🟢 BUY 선호순위 TOP10")
    st.caption(
        "BUY-P·BUY-R이 실제 발생한 종목만 표시합니다. "
        "선호점수 → 거래량 강도 → 최신 신호 순으로 정렬됩니다."
    )

    if buy_ranked.empty:
        st.info("현재 마감된 15분봉 기준 BUY 신호가 없습니다.")
    else:
        for rank, (_, row) in enumerate(buy_ranked.head(10).iterrows(), start=1):
            diff = float(row.get("signal_diff_pct", 0.0) or 0.0)
            if diff < -0.30:
                price_state = "눌림"
            elif diff <= 0.25:
                price_state = "진입권"
            elif diff <= 0.50:
                price_state = "확인 필요"
            else:
                price_state = "추격주의"

            favorite_mark = " ⭐" if bool(row.get("is_favorite", False)) else ""
            signal_time = str(row.get("candle_time_utc") or "")[11:16]
            with st.container(border=True):
                st.markdown(
                    f"### {rank}. {row['symbol']}{favorite_mark} · "
                    f"{row['signal']} · 선호 {int(row.get('preference_score_9', 0))}/9"
                )
                c1, c2, c3 = st.columns(3)
                c1.metric("현재가 차이", f"{diff:+.2f}%")
                c2.metric("거래량", f"{float(row.get('volume_ratio', 0.0)):.2f}배")
                c3.metric("상태", price_state)
                st.write(
                    f"신호가 {float(row.get('buy_price', 0.0)):.10g} · "
                    f"현재가 {float(row.get('current_price', 0.0)):.10g} · "
                    f"신호 UTC {signal_time}  \n"
                    f"선호조건: {row.get('preference_reasons') or '-'}"
                )
                already_open = str(row["symbol"]) in open_position_symbols()
                stop_pct_value = float(row.get("stop_pct", 0.0) or 0.0)
                stop_price_value = float(row.get("stop_price", 0.0) or 0.0)
                unsafe_stop = (
                    stop_price_value <= 0
                    or stop_pct_value <= 0
                    or stop_pct_value > max_stop_pct
                )
                if already_open:
                    st.success("✅ 진입 중 · 포지션 관리에서 확인")
                else:
                    if unsafe_stop:
                        st.warning(
                            f"주문 잠금: 구조 손절폭 {stop_pct_value:.2f}%가 "
                            f"설정 상한 {max_stop_pct:.2f}%를 벗어났습니다."
                        )
                    if st.button(
                        f"LONG 준비 · 증거금 {default_margin_usdt:.0f} USDT · {default_leverage}x",
                        use_container_width=True,
                        disabled=(not private_api_configured()) or unsafe_stop,
                        key=f"prepare_long_{row['symbol']}_{row['candle_time_utc']}",
                    ):
                        st.session_state["pending_long_order"] = {
                            "symbol": str(row["symbol"]),
                            "margin_usdt": float(default_margin_usdt),
                            "leverage": int(default_leverage),
                            "stop_price": stop_price_value,
                            "tp_pct": float(tp_pct),
                            "hedge_mode": bool(hedge_mode),
                        }
                        st.rerun()

    st.caption(
        f"FAST ROTATION {APP_VERSION} · BUY TOP10 아래에는 별도로 실시간 움직임 레이더가 표시됩니다."
    )

    crypto_ranked = ranked_all[ranked_all["symbol_type"] == "crypto"].copy()
    crypto_ranked = crypto_ranked.sort_values(
        ["rotation_score", "chase_score", "current_score_10", "pullback_score_10"],
        ascending=[False, False, False, False],
    )

    # 실시간 레이더: 코인 TOP30 + 즐겨찾기의 1분봉을 10초마다 다시 읽습니다.
    # 15분봉 점수는 방향 필터로만 사용하고, TOP10 순위는 최근 1·3·5분 움직임과 거래량으로 결정합니다.
    scanner_source = crypto_ranked.copy()

    def fetch_live_motion(row_dict: dict) -> dict:
        symbol = str(row_dict["symbol"])
        row_data = dict(row_dict)
        try:
            minute_df = get_klines(symbol, "1", 32).sort_values("start_time").copy()
            if len(minute_df) < 8:
                raise ValueError("1분봉 부족")

            for col in ["open", "high", "low", "close", "turnover"]:
                minute_df[col] = pd.to_numeric(minute_df[col], errors="coerce")
            minute_df = minute_df.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)
            if len(minute_df) < 8:
                raise ValueError("유효 1분봉 부족")

            current = minute_df.iloc[-1]       # 진행 중 1분봉
            closed = minute_df.iloc[:-1].copy()
            live_price = float(current["close"])
            one_open = float(current["open"])
            move_1m = (live_price / one_open - 1) * 100 if one_open > 0 else 0.0

            def pct_from_close(bars_back: int) -> float:
                if len(closed) < bars_back or float(closed.iloc[-bars_back]["close"]) <= 0:
                    return 0.0
                return (live_price / float(closed.iloc[-bars_back]["close"]) - 1) * 100

            move_3m = pct_from_close(3)
            move_5m = pct_from_close(5)

            current_turnover = float(current.get("turnover", 0.0) or 0.0)
            elapsed_ms = max(
                1.0,
                datetime.now(timezone.utc).timestamp() * 1000 - pd.Timestamp(current["start_time"]).timestamp() * 1000,
            )
            elapsed_ratio = min(max(elapsed_ms / 60000.0, 0.05), 1.0)
            projected_turnover = current_turnover / elapsed_ratio
            baseline = float(pd.to_numeric(closed["turnover"], errors="coerce").tail(20).median() or 0.0)
            volume_accel_1m = projected_turnover / baseline if baseline > 0 else 0.0

            recent_high = float(closed["high"].tail(5).max()) if len(closed) else live_price
            breakout_pct = (live_price / recent_high - 1) * 100 if recent_high > 0 else 0.0
            distance_to_high = (live_price / recent_high - 1) * 100 if recent_high > 0 else 0.0

            prev3 = closed.tail(3)
            green_count = int((prev3["close"] > prev3["open"]).sum())
            rising_closes = int(closed["close"].tail(4).is_monotonic_increasing)

            # 지나친 급등은 후보에는 남기되 추격 금지로 분리합니다.
            overheated = move_1m >= 1.30 or move_3m >= 2.40 or float(row_data.get("rsi", 50.0)) >= 78
            just_breakout = breakout_pct >= 0.03 and volume_accel_1m >= 1.20 and move_1m > 0
            motion_start = move_1m >= 0.10 and move_3m >= 0.18 and volume_accel_1m >= 1.15
            pullback_rebound = (
                float(row_data.get("pullback_score_10", 0.0)) >= 5.0
                and move_1m > 0.05
                and move_3m > 0
                and volume_accel_1m >= 0.90
            )
            volume_surge = volume_accel_1m >= 1.35 and move_1m > -0.10

            if overheated:
                state = "🔴 과열"
                action = "추격 금지"
                state_rank = 4
            elif just_breakout:
                state = "🚀 방금 돌파"
                action = "즉시 차트 확인"
                state_rank = 0
            elif motion_start:
                state = "⚡ 움직임 시작"
                action = "우선 확인"
                state_rank = 1
            elif pullback_rebound:
                state = "🌊 눌림 반등"
                action = "진입 관찰"
                state_rank = 2
            elif volume_surge:
                state = "🔥 거래량 급증"
                action = "방향 확인"
                state_rank = 3
            else:
                state = "⚪ 대기"
                action = "관찰"
                state_rank = 5

            realtime_score = (
                max(min(move_1m, 1.5), -0.8) * 22
                + max(min(move_3m, 3.0), -1.5) * 11
                + max(min(move_5m, 5.0), -2.0) * 5
                + min(max(volume_accel_1m, 0.0), 4.0) * 9
                + max(min(breakout_pct, 0.8), -1.0) * 15
                + green_count * 2
                + rising_closes * 4
                + float(row_data.get("rotation_score", 0.0)) * 0.08
            )
            if state_rank == 0:
                realtime_score += 18
            elif state_rank == 1:
                realtime_score += 12
            elif state_rank == 2:
                realtime_score += 7
            elif state_rank == 4:
                realtime_score -= 20

            row_data.update({
                "_live_ok": True,
                "_live_price": live_price,
                "_move_1m": move_1m,
                "_move_3m": move_3m,
                "_move_5m": move_5m,
                "_volume_accel_1m": volume_accel_1m,
                "_breakout_pct": breakout_pct,
                "_distance_to_high": distance_to_high,
                "_state": state,
                "_action": action,
                "_state_rank": state_rank,
                "_realtime_score": round(realtime_score, 1),
            })
        except Exception as exc:
            row_data.update({
                "_live_ok": False,
                "_live_price": float(row_data.get("buy_price", 0.0) or 0.0),
                "_move_1m": 0.0,
                "_move_3m": 0.0,
                "_move_5m": 0.0,
                "_volume_accel_1m": 0.0,
                "_breakout_pct": 0.0,
                "_distance_to_high": 0.0,
                "_state": "⚪ 조회 지연",
                "_action": "잠시 후 재확인",
                "_state_rank": 6,
                "_realtime_score": -999.0,
                "_live_error": str(exc),
            })
        return row_data

    source_records = scanner_source.to_dict("records")
    live_rows = []
    if source_records:
        with ThreadPoolExecutor(max_workers=min(10, len(source_records))) as pool:
            futures = [pool.submit(fetch_live_motion, row) for row in source_records]
            for future in as_completed(futures):
                live_rows.append(future.result())

    # 상태보다 실제 움직임 점수를 우선해 정렬하되, 과열·조회지연은 아래로 내립니다.
    live_rows = sorted(
        live_rows,
        key=lambda item: (
            int(item["_state_rank"] >= 4),
            -float(item["_realtime_score"]),
            int(item["_state_rank"]),
        ),
    )

    previous_states = st.session_state.get("radar_previous_states", {})
    current_states = {str(row["symbol"]): str(row["_state"]) for row in live_rows}
    fresh_signals = [
        row for row in live_rows
        if int(row["_state_rank"]) <= 2
        and previous_states.get(str(row["symbol"])) != str(row["_state"])
    ]
    st.session_state.radar_previous_states = current_states

    radar_time = datetime.now(timezone.utc).strftime("%H:%M:%S")
    st.markdown("### 📡 실시간 움직임 TOP10")
    st.caption(
        f"UTC {radar_time} · 10초마다 갱신 · 최근 1·3·5분 가격과 1분 거래량 가속으로 순위가 바뀝니다. "
        "15분봉 점수는 방향 참고용일 뿐 TOP10의 주 기준이 아닙니다."
    )

    if fresh_signals:
        names = " · ".join(f"{row['_state']} {row['symbol']}" for row in fresh_signals[:5])
        st.success(f"방금 새 움직임 감지: {names}")

    priority_rows = live_rows[:10]
    for rank, row in enumerate(priority_rows, start=1):
        favorite_mark = " ⭐" if bool(row.get("is_favorite", False)) else ""
        low_liq = " · ⚠️ 저유동성" if bool(row.get("liquidity_warning", False)) else ""
        with st.container(border=True):
            st.markdown(f"### {rank}. {row['_state']} {row['symbol']}{favorite_mark}{low_liq}")
            m1, m2, m3 = st.columns(3)
            m1.metric("1분", f"{float(row['_move_1m']):+.2f}%")
            m2.metric("3분", f"{float(row['_move_3m']):+.2f}%")
            m3.metric("1분 거래량", f"{float(row['_volume_accel_1m']):.2f}배")
            st.write(
                f"**행동:** {row['_action']} · 실시간점수 {float(row['_realtime_score']):.1f}  \n"
                f"현재가 {float(row['_live_price']):.10g} · 5분 {float(row['_move_5m']):+.2f}% · "
                f"최근 5분 고점 대비 {float(row['_distance_to_high']):+.2f}%  \n"
                f"15분 방향참고: 회전 {float(row.get('rotation_score', 0.0)):.1f} · "
                f"눌림 {float(row.get('pullback_score_10', 0.0)):.1f} · RSI {float(row.get('rsi', 0.0)):.1f}"
            )

    with st.expander(f"📋 전체 실시간 레이더 {len(live_rows)}개", expanded=False):
        for rank, row in enumerate(live_rows, start=1):
            st.markdown(
                f"**{rank}. {row['_state']} {row['symbol']} · {row['_action']}**  \n"
                f"1분 {float(row['_move_1m']):+.2f}% · 3분 {float(row['_move_3m']):+.2f}% · "
                f"5분 {float(row['_move_5m']):+.2f}% · 거래량 {float(row['_volume_accel_1m']):.2f}배 · "
                f"점수 {float(row['_realtime_score']):.1f}"
            )
            st.divider()

    st.markdown("### 📈 주식형 별도 관찰 TOP3")
    stock_top = ranked_all[ranked_all["symbol_type"] == "stock"].head(3).copy()
    if stock_top.empty:
        st.info("현재 주식형 관찰 종목이 없습니다.")
    else:
        for rank, (_, row) in enumerate(stock_top.iterrows(), start=1):
            st.markdown(
                f"**{rank}. 🟣 {row['symbol']} · 회전 {float(row.get('rotation_score', 0.0)):.1f}**  \n"
                f"거래가속 {float(row.get('turnover_accel', 0.0)):.2f}배 · "
                f"1시간 {float(row.get('momentum_1h', 0.0)):+.2f}%"
            )

    st.caption(
        "상태 기준: 🚀 방금 돌파 → ⚡ 움직임 시작 → 🌊 눌림 반등 → 🔥 거래량 급증 → ⚪ 대기 → 🔴 과열. "
        "TOP10은 최근 1·3·5분 움직임 기준이며 자동 매수 신호가 아닙니다."
    )

    for error in st.session_state.get("last_errors", []):
        st.warning(error)


auto_scan_panel()

st.divider()
st.subheader("🔄 Bybit 실제 보유 포지션")
st.caption(
    "Bybit 앱·PC·이 프로그램 어디서 진입해도 실제 계정 포지션을 10초마다 자동 조회합니다. "
    "조회만 하며, 외부에서 진입한 포지션에 TP·SL을 자동으로 걸지 않습니다."
)


@st.fragment(run_every=10)
def bybit_positions_panel():
    if not private_api_configured():
        st.warning("Bybit API가 설정되지 않아 실제 포지션을 불러올 수 없습니다.")
        return
    try:
        positions = get_open_linear_positions()
    except Exception as exc:
        st.error(f"Bybit 포지션 동기화 실패: {exc}")
        return

    if not positions:
        st.info("현재 Bybit에 열려 있는 USDT 선물 포지션이 없습니다.")
        return

    hidden = set(get_setting("hidden_bybit_positions", []))
    favorites = set(get_setting("favorite_bybit_positions", []))

    def pos_key(p: dict) -> str:
        return f"{p['symbol']}|{p['side']}|{p.get('position_idx', 0)}"

    def inspect(p: dict) -> dict:
        out = dict(p)
        try:
            candles = get_klines(p["symbol"], interval="15", limit=120)
            out["analysis"] = analyze_position_health(candles, p["side"])
        except Exception as exc:
            out["analysis"] = {
                "health": 0, "status": "분석 대기", "icon": "⚪", "priority": 3,
                "recommendation": "차트 데이터 재확인", "reasons": [str(exc)], "danger": False,
            }
        return out

    analyzed = []
    with ThreadPoolExecutor(max_workers=min(8, max(1, len(positions)))) as executor:
        futures = [executor.submit(inspect, p) for p in positions]
        for future in as_completed(futures):
            analyzed.append(future.result())

    analyzed.sort(key=lambda p: (
        0 if pos_key(p) in favorites else 1,
        p["analysis"].get("priority", 3),
        p["unrealised_pnl"],
    ))
    visible = [p for p in analyzed if pos_key(p) not in hidden]
    hidden_rows = [p for p in analyzed if pos_key(p) in hidden]
    hidden_dangers = [p for p in hidden_rows if p["analysis"].get("danger")]

    st.success(f"Bybit 실제 포지션 {len(positions)}개 동기화됨 · 관리 목록 {len(visible)}개")
    if hidden_dangers:
        st.error(f"⚠️ 숨김 종목 위험 신호 {len(hidden_dangers)}개: " + ", ".join(p["symbol"] for p in hidden_dangers))

    urgent = [p for p in visible if p["analysis"].get("priority", 3) <= 1]
    if urgent:
        st.markdown("#### 🚨 관리 우선순위")
        st.write(" · ".join(
            f"{p['analysis']['icon']} {p['symbol']}({p['analysis']['status']})" for p in urgent[:6]
        ))

    def render_position(position: dict, is_hidden: bool = False) -> None:
        key = pos_key(position)
        a = position["analysis"]
        side_icon = "🟢" if position["side"] == "LONG" else "🔴"
        fav_icon = "⭐" if key in favorites else "☆"
        with st.container(border=True):
            st.markdown(
                f"### {fav_icon} {side_icon} {position['symbol']} · {position['side']}  "
                f"{a['icon']} {a['status']} · 건강도 {a['health']}점"
            )
            c1, c2, c3 = st.columns(3)
            c1.metric("평단", format(position["avg_price"], ".10g"))
            c2.metric("현재가", format(position["mark_price"], ".10g"))
            c3.metric("가격변동", f"{position['price_pnl_pct']:+.2f}%")
            d1, d2, d3 = st.columns(3)
            d1.metric("미실현손익", f"{position['unrealised_pnl']:+.2f} USDT")
            d2.metric("증거금(추정)", f"{position['margin_estimate']:.2f} USDT")
            d3.metric("포지션", f"{position['position_value']:.2f} USDT")

            st.markdown(f"**추천: {a['recommendation']}**")
            st.caption(" · ".join(a.get("reasons", [])))

            tp_text = format(position["take_profit"], ".10g") if position["take_profit"] > 0 else "미설정"
            sl_text = format(position["stop_loss"], ".10g") if position["stop_loss"] > 0 else "미설정"
            liq_text = format(position["liq_price"], ".10g") if position["liq_price"] > 0 else "-"
            st.write(
                f"수량 {position['size']:.10g} · 레버리지 {position['leverage']:.10g}배  \n"
                f"Bybit TP {tp_text} · Bybit SL {sl_text} · 청산가 {liq_text}"
            )
            b1, b2 = st.columns(2)
            if b1.button("☆ 즐겨찾기 해제" if key in favorites else "⭐ 즐겨찾기", key=f"fav_{key}", use_container_width=True):
                new_favorites = set(favorites)
                new_favorites.discard(key) if key in new_favorites else new_favorites.add(key)
                set_setting("favorite_bybit_positions", sorted(new_favorites))
                st.rerun()
            label = "↩️ 관리 목록으로 복원" if is_hidden else "👁 숨기기"
            if b2.button(label, key=f"hide_{key}", use_container_width=True):
                new_hidden = set(hidden)
                new_hidden.discard(key) if is_hidden else new_hidden.add(key)
                set_setting("hidden_bybit_positions", sorted(new_hidden))
                st.rerun()
            if position["take_profit"] <= 0 or position["stop_loss"] <= 0:
                st.caption("⚠️ 보호주문 미설정 항목이 있습니다. 프로그램은 임의로 등록하지 않습니다.")

    if visible:
        for position in visible:
            render_position(position)
    else:
        st.info("관리 목록에 표시할 포지션이 없습니다.")

    if hidden_rows:
        with st.expander(f"🙈 숨김 포지션 ({len(hidden_rows)})", expanded=False):
            for position in hidden_rows:
                render_position(position, is_hidden=True)


bybit_positions_panel()

st.divider()
st.subheader("📌 프로그램 관리 포지션")
st.caption(
    "이 아래는 프로그램에서 주문했거나 수동 등록한 로컬 관리목록입니다. "
    "보유 중인 종목은 TOP30·즐겨찾기 스캔에서 자동 제외되며, 포지션 종료 후 다음 스캔부터 자동 재포함됩니다."
)


@st.fragment(run_every=10)
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
        recent = conn.execute(
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
            LIMIT 30"""
        ).fetchall()

    st.write(
        f"저장된 스캔 {total}건 · "
        f"BUY 후보 {buys}건 · "
        f"포지션 추적 {checks}봉"
    )

    if recent:
        for row in recent:
            row = dict(row)
            candle_time = str(row.get("candle_time_utc") or "")[:16].replace("T", " ")
            score = row.get("current_score")
            pullback = row.get("pullback_score")
            rsi = row.get("rsi")
            score_text = "-" if score is None else f"{float(score):.1f}"
            pullback_text = "-" if pullback is None else f"{float(pullback):.1f}"
            rsi_text = "-" if rsi is None else f"{float(rsi):.1f}"
            st.markdown(
                f"**{row.get('symbol', '-')} · {row.get('signal', '-')}**  \n"
                f"{candle_time} UTC · 점수 {score_text} · "
                f"눌림 {pullback_text} · RSI {rsi_text}  \n"
                f"{row.get('entry_status') or '-'}"
            )
            st.divider()



st.divider()
st.subheader("🤖 OKX 데일리 3회 PAPER 봇")
st.caption("하루 최대 3회, 한 번에 1종목만 진입하는 2~3시간 단기 추세매매 테스트입니다. 실제 주문은 발생하지 않습니다.")
OKX_BOT_DB = Path(__file__).with_name("Okx_swing") / "okx_swing_bot.db"
OKX_BOT_CONFIG = Path(__file__).with_name("Okx_swing") / "config.json"
bot_cfg = {}
if OKX_BOT_CONFIG.exists():
    try:
        bot_cfg = json.loads(OKX_BOT_CONFIG.read_text(encoding="utf-8"))
        mode = str(bot_cfg.get("mode", "paper")).upper()
        symbols = bot_cfg.get("symbols", [])
        st.info(
            f"모드 {mode} · 격리 {bot_cfg.get('leverage', 5)}배 · 하루 최대 {bot_cfg.get('max_daily_entries', 3)}회 · "
            f"동시 {bot_cfg.get('max_positions', 1)}종목 · 1회 증거금 {bot_cfg.get('position_margin_usdt', 54)} USDT"
        )
        st.write(
            f"TP1 +{bot_cfg.get('tp1_pct', 1.5)}% 절반 · TP2 +{bot_cfg.get('tp2_pct', 3.0)}% 나머지 · "
            f"손절 -{bot_cfg.get('hard_stop_pct', 1.5)}% · 최대 보유 {bot_cfg.get('max_hold_hours', 3)}시간"
        )
        st.caption("감시: " + ", ".join(str(x).replace("-USDT-SWAP", "") for x in symbols))
        if mode == "LIVE":
            st.error("⚠️ LIVE 모드입니다. API 출금 권한이 꺼져 있는지 반드시 확인하세요.")
        elif mode == "PAPER":
            st.success("현재 PAPER 모드: 모의기록만 하며 실제 주문은 발생하지 않습니다.")
    except Exception as exc:
        st.warning(f"데일리봇 설정 확인 실패: {exc}")

if OKX_BOT_DB.exists():
    try:
        with sqlite3.connect(OKX_BOT_DB) as bot_conn:
            bot_conn.row_factory = sqlite3.Row
            bot_positions = bot_conn.execute(
                "SELECT * FROM bot_positions WHERE status='OPEN' ORDER BY opened_at"
            ).fetchall()
            bot_events = bot_conn.execute(
                "SELECT * FROM bot_events ORDER BY id DESC LIMIT 20"
            ).fetchall()
            today_stats = bot_conn.execute(
                """SELECT
                    SUM(CASE WHEN event='ENTRY' THEN 1 ELSE 0 END) AS entries,
                    COALESCE(SUM(realized_pnl),0) AS pnl
                    FROM bot_events
                    WHERE substr(datetime(ts,'+9 hours'),1,10)=date('now','+9 hours')"""
            ).fetchone()
        c1, c2, c3 = st.columns(3)
        c1.metric("오늘 진입", f"{int(today_stats['entries'] or 0)} / {int(bot_cfg.get('max_daily_entries', 3))}")
        c2.metric("오늘 확정손익", f"{float(today_stats['pnl'] or 0):+.2f} USDT")
        c3.metric("현재 포지션", str(len(bot_positions)))

        if bot_positions:
            for row in bot_positions:
                with st.container(border=True):
                    strategy = row['strategy'] if 'strategy' in row.keys() else '-'
                    st.markdown(f"### {row['symbol']} · {strategy}형 · {row['note'] or '관리 중'}")
                    a, b, c = st.columns(3)
                    a.metric("평단", format(float(row['avg_price']), '.10g'))
                    b.metric("현재가", '-' if row['last_price'] is None else format(float(row['last_price']), '.10g'))
                    c.metric("가격 변동", '-' if row['unrealized_pct'] is None else f"{float(row['unrealized_pct']):+.2f}%")
                    st.write(
                        f"증거금 {float(row['total_margin']):.2f} USDT · 레버리지 {bot_cfg.get('leverage', 5)}배 · "
                        f"TP1 {'완료' if int(row['tp1_done']) else '대기'} · 물타기 없음"
                    )
        else:
            st.info("현재 데일리봇 보유 포지션이 없습니다.")

        with st.expander("최근 데일리봇 기록", expanded=False):
            for e in bot_events:
                strategy = e['strategy'] if 'strategy' in e.keys() and e['strategy'] else '-'
                pnl = float(e['realized_pnl'] or 0) if 'realized_pnl' in e.keys() else 0.0
                pnl_txt = f" · 확정 {pnl:+.2f} USDT" if abs(pnl) > 1e-12 else ""
                st.write(f"{str(e['ts'])[:19]} · {e['symbol'] or '-'} · {strategy}형 · {e['event']}{pnl_txt} · {e['details'] or ''}")
    except Exception as exc:
        st.warning(f"데일리봇 상태 읽기 실패: {exc}")
else:
    st.info("데일리봇을 한 번 실행하면 상태 데이터가 여기에 표시됩니다.")

st.caption("Bybit 실제 포지션이 원본입니다. 외부 진입 포지션에는 프로그램이 TP·SL을 임의 설정하지 않습니다.")
