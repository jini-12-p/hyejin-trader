from __future__ import annotations
import sqlite3, time
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
from bybit import get_klines, get_ticker
from strategy import StrategySettings, analyze_symbol

DB_PATH=Path(__file__).with_name('hyejin_trader.db')

def conn():
    c=sqlite3.connect(DB_PATH,timeout=20); c.row_factory=sqlite3.Row; return c

def migrate():
    with conn() as c:
        c.executescript('''
        CREATE TABLE IF NOT EXISTS position_live (
          symbol TEXT PRIMARY KEY, last_price REAL, pnl_pct REAL, tp_price REAL, stop_price REAL,
          live_action TEXT, trend_action TEXT, hold_score REAL, bars_elapsed INTEGER,
          updated_at_utc TEXT, latest_closed_start_utc TEXT
        );
        CREATE TABLE IF NOT EXISTS position_ticks (
          id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, price REAL, pnl_pct REAL,
          checked_at_utc TEXT, action TEXT
        );
        ''')

def closed(df):
    now=pd.Timestamp(datetime.now(timezone.utc)); return df[df.start_time+pd.Timedelta(minutes=15)<=now].copy()

def track_one(p):
    symbol=p['symbol']; entry=float(p['entry_price']); tp=entry*(1+float(p['tp_pct'])/100); stop=float(p['stop_price'])
    price=get_ticker(symbol); pnl=(price-entry)/entry*100
    if price>=tp: live='TP 도달'
    elif price<=stop: live='비상 STOP 도달'
    elif price<=stop+(entry-stop)*0.25: live='비상 STOP 근접'
    else: live='실시간 HOLD'
    raw=get_klines(symbol,'15',120); cdf=closed(raw)
    trend='대기'; score=0.0; bars=0; latest=''
    if not cdf.empty:
        latest_start=pd.Timestamp(cdf.iloc[-1].start_time); latest=latest_start.isoformat()
        result=analyze_symbol(symbol,raw,StrategySettings()).to_dict(); score=float(result['current_score_10'])
        if score>=8: trend='추세 HOLD'
        elif score>=6: trend='추세 주의'
        else: trend='추세 STOP'
        et=pd.Timestamp(p['entry_time_utc']); bucket=et.floor('15min')
        if latest_start+pd.Timedelta(minutes=15)>et:
            bars=max(0,int((latest_start-bucket)/pd.Timedelta(minutes=15))+1)
    now=datetime.now(timezone.utc).isoformat()
    with conn() as c:
        c.execute('''INSERT INTO position_live(symbol,last_price,pnl_pct,tp_price,stop_price,live_action,trend_action,hold_score,bars_elapsed,updated_at_utc,latest_closed_start_utc)
        VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(symbol) DO UPDATE SET last_price=excluded.last_price,pnl_pct=excluded.pnl_pct,tp_price=excluded.tp_price,stop_price=excluded.stop_price,live_action=excluded.live_action,trend_action=excluded.trend_action,hold_score=excluded.hold_score,bars_elapsed=excluded.bars_elapsed,updated_at_utc=excluded.updated_at_utc,latest_closed_start_utc=excluded.latest_closed_start_utc''',
        (symbol,price,pnl,tp,stop,live,trend,score,bars,now,latest))
        c.execute('INSERT INTO position_ticks(symbol,price,pnl_pct,checked_at_utc,action) VALUES(?,?,?,?,?)',(symbol,price,pnl,now,live))

def main():
    migrate()
    while True:
        try:
            with conn() as c: positions=c.execute("SELECT * FROM positions WHERE status='OPEN'").fetchall()
            for p in positions:
                try: track_one(p)
                except Exception as e: print(datetime.now(),p['symbol'],e,flush=True)
        except Exception as e: print(datetime.now(),'tracker',e,flush=True)
        time.sleep(5)
if __name__=='__main__': main()
