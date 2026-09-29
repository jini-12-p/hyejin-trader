#!/usr/bin/env python3
import csv,gzip,zipfile,io,json,hashlib,sqlite3
from pathlib import Path
from datetime import datetime
R=Path("/root/hyejin-trader/bybit_swing");DAY="2026-09-28";OUT=R/"SCAN_FULL_20260928_0000_2359_KST.csv";DB=R/".scan28.sqlite";ZOUT=R/"FINAL_FULLDAY_20260928_RESULTS.zip"
src=[]
for b in (R,R/"scan_archive"):
 if b.exists():
  for p in b.rglob("*"):
   n=p.name.lower()
   if p.is_file() and "scan" in n and (p.suffix.lower() in (".csv",".zip") or n.endswith(".csv.gz")) and p.resolve()!=OUT.resolve():src.append(p)
src=list(dict.fromkeys(src));DB.unlink(missing_ok=True);c=sqlite3.connect(DB);c.execute("create table x(h text primary key,ts text,p text)");fields=[];fs=set()
def eat(fh):
 rd=csv.DictReader(fh)
 if not rd.fieldnames:return
 for k in rd.fieldnames:
  if k not in fs:fs.add(k);fields.append(k)
 tk=next((k for k in ("time_kst","timestamp_kst","datetime_kst","time","timestamp") if k in rd.fieldnames),None)
 if not tk:return
 a=[]
 for r in rd:
  ts=str(r.get(tk,"")).strip().strip('"')
  if not ts.startswith(DAY):continue
  s=json.dumps(r,sort_keys=True,ensure_ascii=False,separators=(",",":"));a.append((hashlib.sha1(s.encode()).hexdigest(),ts,s))
  if len(a)>=3000:c.executemany("insert or ignore into x values(?,?,?)",a);a=[]
 if a:c.executemany("insert or ignore into x values(?,?,?)",a)
for i,p in enumerate(src,1):
 try:
  if p.name.lower().endswith(".csv.gz"):
   with gzip.open(p,"rt",encoding="utf-8-sig",errors="replace",newline="") as h:eat(h)
  elif p.suffix.lower()==".zip":
   with zipfile.ZipFile(p) as z:
    for n in z.namelist():
     if n.lower().endswith(".csv"):
      with z.open(n) as q:
       with io.TextIOWrapper(q,encoding="utf-8-sig",errors="replace",newline="") as h:eat(h)
  else:
   with p.open("r",encoding="utf-8-sig",errors="replace",newline="") as h:eat(h)
  if i%20==0 or i==len(src):c.commit();print("[SCAN]",i,"/",len(src),c.execute("select count(*) from x").fetchone()[0],flush=True)
 except Exception as e:print("[WARN]",p.name,e)
rr=list(c.execute("select ts,p from x order by ts"));c.close();DB.unlink(missing_ok=True)
if not rr:raise SystemExit("NO 9/28 DATA")
with OUT.open("w",encoding="utf-8-sig",newline="") as h:
 w=csv.DictWriter(h,fieldnames=fields,extrasaction="ignore");w.writeheader()
 for ts,s in rr:w.writerow(json.loads(s))
tt=[]
for ts,_ in rr:
 try:tt.append(datetime.strptime(ts[:19],"%Y-%m-%d %H:%M:%S"))
 except:pass
tt=sorted(set(tt));mg=(0,None,None)
for a,b in zip(tt,tt[1:]):
 g=(b-a).total_seconds()/60
 if g>mg[0]:mg=(g,a,b)
note=R/"FINAL_FULLDAY_20260928_COVERAGE.txt";note.write_text(f"FIRST={tt[0]}\nLAST={tt[-1]}\nUNIQUE_TS={len(tt)}\nMAX_GAP_MIN={mg[0]}\nMAX_GAP_FROM={mg[1]}\nMAX_GAP_TO={mg[2]}\n",encoding="utf-8")
with zipfile.ZipFile(ZOUT,"w",zipfile.ZIP_DEFLATED) as z:z.write(OUT,arcname=OUT.name);z.write(note,arcname=note.name)
print("FIRST",tt[0],"LAST",tt[-1],"MAX_GAP_MIN",mg[0]);print("SAVED",OUT);print("DONE",ZOUT)
