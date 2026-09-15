#!/usr/bin/env python3
import sqlite3, json, csv, zipfile, os
from pathlib import Path
from collections import defaultdict, Counter
from datetime import datetime, timezone, timedelta

DB = Path("bybit_swing/bybit_swing_bot.db")
OUTDIR = Path("bybit_swing")
KST = timezone(timedelta(hours=9))

# 분석 목적:
# 1) V25 순수진입 원본(P_V25 및 P_V27_4_1)을 우선 anchor로 수집
# 2) V26~V27.4 계열 중 동일 symbol/진입가/근접시각으로 복제된 연구행을 cluster
# 3) 각 cluster에서 TP1/BE/STOP 결과와 snapshot을 보존
# 4) 기존 variant 합산이 아니라 "독립 원진입" 단위 MASTER 생성
#
# 주의: 이 exporter는 데이터를 바꾸지 않는다. SELECT만 수행한다.
# BE 최종 재생은 이 MASTER에서 동일진입/동일손절 여부를 검증한 뒤 수행한다.

VARS = (
    "P_V25","P_V26","P_V27","P_V27_1","P_V27_2","P_V27_3","P_V27_4",
    "P_V27_4_1","P_V27_4_2","P_V27_4_3","P_V27_4_4"
)
GHOST_WORDS = ("GHOST","FILTER_ONLY","BLOCK_GHOST","STOP_GHOST","LATE_GHOST")

def parse_dt(s):
    if not s: return None
    try:
        d=datetime.fromisoformat(str(s).replace("Z","+00:00"))
        if d.tzinfo is None: d=d.replace(tzinfo=timezone.utc)
        return d.astimezone(timezone.utc)
    except: return None

def fnum(x):
    try: return float(x)
    except: return None

def clean_variant(v):
    return str(v or "").strip()

con=sqlite3.connect(DB)
con.row_factory=sqlite3.Row
cols=[r[1] for r in con.execute("PRAGMA table_info(research_shadow_reviews)").fetchall()]
need=["id","shadow_id","symbol","variant","opened_at","entry_ts_ms","entry_price",
      "tp1_done","tp1_ts","tp1_price","result","result_ts","result_price",
      "result_details","snapshot_json","completed"]
sel=[x for x in need if x in cols]

q="SELECT "+",".join(sel)+" FROM research_shadow_reviews ORDER BY opened_at,id"
raw=[dict(r) for r in con.execute(q).fetchall()]
con.close()

rows=[]
for r in raw:
    v=clean_variant(r.get("variant"))
    if v not in VARS: continue
    if any(w in v for w in GHOST_WORDS): continue
    d=parse_dt(r.get("opened_at"))
    p=fnum(r.get("entry_price"))
    if not d or not p or p<=0: continue
    r["_dt"]=d; r["_p"]=p
    rows.append(r)

# 독립 거래 cluster: 같은 symbol, 진입가격 0.05% 이내, 시작시각 10초 이내.
# 여러 variant가 같은 source에서 거의 동시에 복제되는 구조를 합친다.
clusters=[]
used=[False]*len(rows)
by_symbol=defaultdict(list)
for i,r in enumerate(rows): by_symbol[r["symbol"]].append((i,r))

for sym, arr in by_symbol.items():
    arr.sort(key=lambda z:z[1]["_dt"])
    for pos,(i,r) in enumerate(arr):
        if used[i]: continue
        members=[r]; used[i]=True
        for j,s in arr[pos+1:]:
            if used[j]: continue
            dt=abs((s["_dt"]-r["_dt"]).total_seconds())
            if dt>10:
                if s["_dt"]>r["_dt"] and dt>10: break
                continue
            pd=abs(s["_p"]/r["_p"]-1)*100
            if pd<=0.05:
                members.append(s); used[j]=True
        clusters.append(members)

def jload(x):
    try: return json.loads(x or "{}")
    except: return {}

def member_summary(m):
    return {
        "id":m.get("id"),"shadow_id":m.get("shadow_id"),"variant":m.get("variant"),
        "opened_at":m.get("opened_at"),"entry_price":m.get("entry_price"),
        "tp1_done":m.get("tp1_done"),"tp1_ts":m.get("tp1_ts"),
        "result":m.get("result"),"result_ts":m.get("result_ts"),
        "result_price":m.get("result_price"),
    }

master=[]
members_out=[]
for n,mem in enumerate(sorted(clusters,key=lambda x:x[0]["_dt"]),1):
    variants=sorted(set(x["variant"] for x in mem))
    # V25 anchor 우선순위
    anchor=None
    for av in ("P_V25","P_V27_4_1","P_V26","P_V27","P_V27_1","P_V27_2","P_V27_3","P_V27_4"):
        anchor=next((x for x in mem if x["variant"]==av),None)
        if anchor: break
    has_v25_anchor=any(x["variant"] in ("P_V25","P_V27_4_1") for x in mem)
    v271=next((x for x in mem if x["variant"]=="P_V27_1"),None)
    # 동일 cluster에서 V27-1이 있으면 동일손절 결과를 직접 사용 가능.
    v271_tp1=bool(v271 and int(v271.get("tp1_done") or 0))
    v271_be=bool(v271 and str(v271.get("result") or "")=="BE_EXIT")
    # V25 anchor 자체 BE도 별도 표시
    anchor_tp1=bool(int(anchor.get("tp1_done") or 0))
    anchor_be=str(anchor.get("result") or "")=="BE_EXIT"

    cid=f"U{n:05d}"
    snap=jload(anchor.get("snapshot_json"))
    master.append({
        "cluster_id":cid,
        "symbol":anchor["symbol"],
        "opened_at":anchor["opened_at"],
        "entry_price":anchor["entry_price"],
        "anchor_variant":anchor["variant"],
        "has_v25_anchor":int(has_v25_anchor),
        "has_v271_exact_pair":int(v271 is not None),
        "variants":"|".join(variants),
        "member_count":len(mem),
        "anchor_tp1":int(anchor_tp1),
        "anchor_result":anchor.get("result"),
        "anchor_result_ts":anchor.get("result_ts"),
        "v271_tp1":int(v271_tp1),
        "v271_result":v271.get("result") if v271 else "",
        "v271_result_ts":v271.get("result_ts") if v271 else "",
        "v271_result_price":v271.get("result_price") if v271 else "",
        "v25_entry_score":snap.get("new_p_score",snap.get("p_score","")),
        "v25_score_structure":snap.get("new_score_structure",""),
        "v25_score_trend":snap.get("new_score_trend",""),
        "v25_score_live":snap.get("new_score_live",""),
        "snapshot_json":anchor.get("snapshot_json") or "",
    })
    for x in mem:
        z=member_summary(x); z["cluster_id"]=cid
        z["result_details"]=x.get("result_details") or ""
        z["snapshot_json"]=x.get("snapshot_json") or ""
        members_out.append(z)

stamp=datetime.now(KST).strftime("%Y%m%d_%H%M_KST")
master_csv=OUTDIR/f"V25_V271_UNIFIED_ENTRY_MASTER_{stamp}.csv"
members_csv=OUTDIR/f"V25_V271_UNIFIED_ENTRY_MEMBERS_{stamp}.csv"
summary_txt=OUTDIR/f"V25_V271_UNIFIED_ENTRY_SUMMARY_{stamp}.txt"
zip_path=OUTDIR/f"V25_V271_BE_SOURCE_{stamp}.zip"

def write_csv(path,data):
    fields=list(data[0].keys()) if data else ["empty"]
    with path.open("w",encoding="utf-8-sig",newline="") as f:
        w=csv.DictWriter(f,fieldnames=fields); w.writeheader()
        if data: w.writerows(data)

write_csv(master_csv,master)
write_csv(members_csv,members_out)

v25=[x for x in master if x["has_v25_anchor"]]
paired=[x for x in v25 if x["has_v271_exact_pair"]]
paired_be=[x for x in paired if x["v271_result"]=="BE_EXIT"]
anchor_be=[x for x in v25 if x["anchor_result"]=="BE_EXIT"]

lines=[]
lines.append("=== V25 -> V27-1 UNIFIED SOURCE CHECK ===")
lines.append(f"RAW NON-GHOST ROWS = {len(rows)}")
lines.append(f"INDEPENDENT ENTRY CLUSTERS = {len(master)}")
lines.append(f"V25-ANCHOR CLUSTERS = {len(v25)}")
lines.append(f"V25 + EXACT V27-1 PAIRS = {len(paired)}")
lines.append(f"EXACT-PAIR V27-1 BE = {len(paired_be)}")
lines.append(f"V25-ANCHOR OWN BE = {len(anchor_be)}")
lines.append("")
lines.append("=== ANCHOR VARIANTS ===")
for k,v in Counter(x["anchor_variant"] for x in master).most_common():
    lines.append(f"{k} {v}")
lines.append("")
lines.append("=== V25 ANCHOR RESULT ===")
for k,v in Counter(str(x["anchor_result"]) for x in v25).most_common():
    lines.append(f"{k} {v}")
lines.append("")
lines.append("=== EXACT PAIRED V27-1 RESULT ===")
for k,v in Counter(str(x["v271_result"]) for x in paired).most_common():
    lines.append(f"{k} {v}")
lines.append("")
lines.append("NOTE: exact-pair BE rows are already V25-anchor entries with the same V27-1 stop result.")
lines.append("Unpaired historical V25 anchors are preserved for the next replay step; do not fabricate V27-1 results.")

summary_txt.write_text("\n".join(lines),encoding="utf-8")

with zipfile.ZipFile(zip_path,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(master_csv,master_csv.name)
    z.write(members_csv,members_csv.name)
    z.write(summary_txt,summary_txt.name)

print("\n".join(lines))
print("\nMASTER =",master_csv)
print("ZIP =",zip_path)
