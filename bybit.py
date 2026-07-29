from __future__ import annotations
import time
from typing import Any
import pandas as pd
import requests

BASE_URL='https://api.bybit.com'
_SESSION=requests.Session(); _SESSION.headers.update({'User-Agent':'HJ-Trader/0.3','Accept':'application/json'})
class BybitError(RuntimeError): pass

def _get(path:str, params:dict[str,Any]|None=None, retries:int=3)->dict[str,Any]:
    last=None
    for attempt in range(retries):
        try:
            r=_SESSION.get(BASE_URL+path,params=params or {},timeout=12); r.raise_for_status(); p=r.json()
            if p.get('retCode')!=0: raise BybitError(p.get('retMsg','Bybit API 오류'))
            return p
        except (requests.RequestException,ValueError,BybitError) as e:
            last=e
            if attempt<retries-1: time.sleep(attempt+1)
    raise BybitError(f'Bybit 데이터를 가져오지 못했습니다: {last}')

def list_usdt_perpetual_symbols()->list[str]:
    out=[]; cursor=''
    while True:
        params={'category':'linear','limit':1000}
        if cursor: params['cursor']=cursor
        result=_get('/v5/market/instruments-info',params)['result']
        out += [x['symbol'] for x in result.get('list',[]) if x.get('quoteCoin')=='USDT' and x.get('contractType')=='LinearPerpetual' and x.get('status')=='Trading']
        cursor=result.get('nextPageCursor','')
        if not cursor: break
    return sorted(set(out))

def get_ticker(symbol:str)->float:
    rows=_get('/v5/market/tickers',{'category':'linear','symbol':symbol.upper()})['result'].get('list',[])
    if not rows: raise BybitError(f'{symbol}: 현재가가 없습니다.')
    return float(rows[0]['lastPrice'])

def get_klines(symbol:str, interval:str='15', limit:int=240)->pd.DataFrame:
    rows=_get('/v5/market/kline',{'category':'linear','symbol':symbol.upper(),'interval':interval,'limit':limit})['result'].get('list',[])
    if not rows: raise BybitError(f'{symbol}: 캔들 데이터가 없습니다.')
    df=pd.DataFrame(rows,columns=['start_time','open','high','low','close','volume','turnover'])
    for c in ['open','high','low','close','volume','turnover']: df[c]=pd.to_numeric(df[c],errors='coerce')
    df['start_time']=pd.to_datetime(df['start_time'].astype('int64'),unit='ms',utc=True)
    return df.sort_values('start_time').reset_index(drop=True)
