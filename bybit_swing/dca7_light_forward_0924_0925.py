#!/usr/bin/env python3
import csv,json,math,time,urllib.parse,urllib.request,zipfile
from datetime import datetime,timedelta,timezone
from pathlib import Path
R=Path("/root/hyejin-trader/bybit_swing")
SRC=R/"FORWARD_0924_0925_4WAY_V25_PLUS_MARKET_PLUS_V22Q_TRADES.csv"
D=R/"FORWARD_0924_0925_DCA7_DETAIL.csv"; S=R/"FORWARD_0924_0925_DCA7_SUMMARY.txt"; Z=R/"FORWARD_0924_0925_DCA7_RESULTS.zip"
KST=timezone(timedelta(hours=9)); UTC=timezone.utc
VOL=2.61; SLOPE=.00061; LOWMIN=-2.74314; RSIMIN=43.89902; REB=1.5; FEE=.055
def dt(s): return datetime.strptime(str(s)[:19],"%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)
def pct(a,b): return (a/b-1)*100 if b else None
def avg(a): return sum(a)/len(a) if a else None
def ema(v,n):
 a=2/(n+1); e=float(v[0])
 for x in v[1:]: e=a*float(x)+(1-a)*e
 return e
def rsi(v,n=14):
 if len(v)<n+1:return None
 ds=[b-a for a,b in zip(v[-n-1:-1],v[-n:])]; g=sum(max(x,0) for x in ds)/n; l=sum(max(-x,0) for x in ds)/n
 return 100 if l==0 else 100-100/(1+g/l)
def bars(sym,a,b):
 p={"category":"linear","symbol":sym,"interval":"1","start":int(a.timestamp()*1000),"end":int(b.timestamp()*1000),"limit":1000}
 u="https://api.bybit.com/v5/market/kline?"+urllib.parse.urlencode(p); err=None
 for k in range(8):
  try:
   q=urllib.request.Request(u,headers={"User-Agent":"HJ-DCA7/1.0"})
   with urllib.request.urlopen(q,timeout=25) as f:j=json.loads(f.read().decode())
   if int(j.get("retCode",-1))!=0:raise RuntimeError(j)
   z=[{"t":datetime.fromtimestamp(int(x[0])/1000,tz=UTC),"o":float(x[1]),"h":float(x[2]),"l":float(x[3]),"c":float(x[4]),"v":float(x[5])} for x in j["result"]["list"]]
   return sorted(z,key=lambda x:x["t"])
  except Exception as e:err=e;time.sleep(.5*(k+1))
 raise err
def f5(one):
 d={}
 for b in one:
  t=b["t"].replace(minute=b["t"].minute//5*5,second=0,microsecond=0)
  if t not in d:d[t]={"t":t,"o":b["o"],"h":b["h"],"l":b["l"],"c":b["c"],"v":b["v"]}
  else:
   x=d[t];x["h"]=max(x["h"],b["h"]);x["l"]=min(x["l"],b["l"]);x["c"]=b["c"];x["v"]+=b["v"]
 return [d[k] for k in sorted(d)]
def feat(one,cp):
 cp=cp.replace(second=0,microsecond=0); p=[x for x in one if x["t"]<cp]; q=[x for x in f5(one) if x["t"]+timedelta(minutes=5)<=cp]
 o={"vr":None,"p3":None,"sl":None,"rsi":None}
 if p:
  vs=[x["v"] for x in p[-11:-1]]; m=avg(vs);o["vr"]=p[-1]["v"]/m if m else None
  z=p[-3:];o["p3"]=pct(z[-1]["c"],z[0]["o"])
 if q:
  c=[x["c"] for x in q];e=ema(c[-80:],20);ep=ema(c[-81:-1],20) if len(c)>=2 else None;o["sl"]=(e/ep-1)*100 if e and ep else None;o["rsi"]=rsi(c)
 return o
def one_net(e,x):return pct(x,e)-(FEE+FEE*x/e)
def avg_net(e,a):
 m=(e+a)/2;return -(FEE+FEE*a/e+FEE*2*m/e)
def fail_net(e,a,x):return ((x-e)+(x-a))/e*100-(FEE+FEE*a/e+FEE*2*x/e)
with SRC.open(encoding="utf-8-sig") as f: rr=list(csv.DictReader(f))
st=[x for x in rr if x.get("result")=="STOP"]; out=[]
for i,x in enumerate(st,1):
 sym=x["symbol"];e=float(x["entry_price"]);cur=float(x["net_pct"]);stop=dt(x["exit_time_kst"]).astimezone(UTC)
 one=bars(sym,stop-timedelta(hours=3),stop+timedelta(hours=6,minutes=5));sf=feat(one,stop)
 ob=sf["vr"] is not None and sf["sl"] is not None and sf["vr"]<=VOL and sf["sl"]>=SLOPE
 cl="NOT_OBSERVER";new=cur;tr="";lp="";ri="";p3="";mins="";dc=0;ok="";br=""
 if ob:
  fl=stop.replace(second=0,microsecond=0);post=[b for b in one if fl+timedelta(minutes=1)<=b["t"]<=fl+timedelta(hours=6)]
  lo=lt=tt=ap=None
  for b in post:
   if lo is None or b["l"]<lo:lo=b["l"];lt=b["t"]
   if b["t"]>lt and b["h"]>=lo*(1+REB/100):tt=b["t"];ap=lo*(1+REB/100);break
  if tt is None:cl="OBSERVER_NO_TRIGGER_6H"
  else:
   ff=feat(one,tt);lp=pct(lo,e);mins=(tt-lt).total_seconds()/60;ri=ff["rsi"];p3=ff["p3"];tr=tt.astimezone(KST).strftime("%F %T")
   green=lp>=LOWMIN and ri is not None and ri>=RSIMIN
   safe=(not green and mins<12 and p3 is not None and p3>-1)
   if green or safe:
    dc=1;cl="GREEN_DCA" if green else "SAFEGRAY_DCA";av=(e+ap)/2;rec=breaks=False
    for b in post:
     if b["t"]<=tt:continue
     if b["l"]<=lo:breaks=True;break
     if b["h"]>=av:rec=True;break
    ok=int(rec);br=int(breaks);new=avg_net(e,ap) if rec else fail_net(e,ap,lo) if breaks else cur
   else:cl="RISK_NO_DCA_EXIT_REBOUND";new=one_net(e,ap)
 out.append({"symbol":sym,"entry_time_kst":x["entry_time_kst"],"stop_time_kst":x["exit_time_kst"],"current_stop_net":round(cur,6),"stop_vol_ratio":sf["vr"],"stop_ema20_slope":sf["sl"],"observer":int(ob),"class":cl,"trigger_time_kst":tr,"swing_low_pct":lp,"rebound_rsi5":ri,"rebound_prev3m_ret":p3,"low_to_trigger_min":mins,"dca100":dc,"dca_success":ok,"dca_false_break":br,"policy_net":round(new,6),"delta":round(new-cur,6)})
 print(f"[{i}/{len(st)}] {sym} observer={int(ob)} {cl} {cur:+.4f}->{new:+.4f}",flush=True)
with D.open("w",encoding="utf-8-sig",newline="") as f:w=csv.DictWriter(f,fieldnames=out[0].keys());w.writeheader();w.writerows(out)
a=sum(x["current_stop_net"] for x in out);b=sum(x["policy_net"] for x in out)
lines=["LIGHTWEIGHT DCA7 VALIDATION",f"STOPs={len(out)}",f"observers={sum(x['observer'] for x in out)}",f"GREEN={sum(x['class']=='GREEN_DCA' for x in out)}",f"SAFE_GRAY={sum(x['class']=='SAFEGRAY_DCA' for x in out)}",f"RISK_NO_DCA={sum(x['class']=='RISK_NO_DCA_EXIT_REBOUND' for x in out)}",f"DCA100={sum(x['dca100'] for x in out)}",f"DCA_success={sum(str(x['dca_success'])=='1' for x in out)}",f"DCA_false_break={sum(str(x['dca_false_break'])=='1' for x in out)}",f"current_stop_net={a:+.6f}",f"policy_stop_net={b:+.6f}",f"delta={b-a:+.6f}"]
S.write_text("\n".join(lines)+"\n",encoding="utf-8")
with zipfile.ZipFile(Z,"w",zipfile.ZIP_DEFLATED) as z:z.write(D,arcname=D.name);z.write(S,arcname=S.name)
print("\n"+"\n".join(lines));print("DONE:",Z)
