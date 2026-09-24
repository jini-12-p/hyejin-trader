#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V22 QUALITY FILTER — exact portfolio reschedule validation

Current V25 market guard remains ON:
  V25 rolling2h >= 8
  AND mean(abs(BTC4h), abs(ETH4h)) >= 0.40%

Candidate V22 quality failures, using candidate-time telemetry ONLY:

A) OVEREXT
   p_v2_score >= 90
   AND ema9_ema20_gap_pct >= 1.20

B) WEAK_REACCEL
   rebound_from_low_pct <= 5.00
   AND rsi_delta <= 7.00
   AND btc_15m_change_pct >= -0.08

Scenarios:
  CUR_V25_ONLY
  OVEREXT
  WEAK_REACCEL
  V22_QUALITY_OR = A OR B

Important:
- Missing exact candidate-time telemetry is NEVER blocked.
- 9/1~17 therefore acts as a conservative partial-history validation.
- 9/18~22 and 9/23 have much higher candidate-time coverage.
- Full portfolio is rescheduled after each block:
  4 slots / rolling15m 2 entries / 90m / STOP-LATE180 / STOP pause.
- Exit engine remains current BASE: TP2.0 + PP12 + Final4 + V27-1.
- No DB writes. No orders.

Embedded RECOVERED_PRE contains exact WATCH rows recovered from prior Library scan files
during this chat, supplementing V22_ROOT_AUDIT_SETUPS.csv.
"""
from __future__ import annotations

import copy
import csv
import importlib.util
import math
import sqlite3
import sys
import zipfile
from collections import deque
from datetime import datetime as RealDateTime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path("/root/hyejin-trader/bybit_swing")
U_PATH = ROOT / "unified_current_0901_0922.py"
AUDIT_PATH = ROOT / "V22_ROOT_AUDIT_SETUPS.csv"
DB = ROOT / "bybit_swing_bot.db"

KST = timezone(timedelta(hours=9))
UTC = timezone.utc

HIST_START = RealDateTime(2026,9,1,0,0,0,tzinfo=KST)
HIST_END   = RealDateTime(2026,9,23,0,0,0,tzinfo=KST)

FRESH_WARM  = RealDateTime(2026,9,22,8,0,0,tzinfo=KST)
FRESH_START = RealDateTime(2026,9,22,14,18,34,tzinfo=KST)
FRESH_END   = RealDateTime(2026,9,23,19,10,43,tzinfo=KST)

SCENARIOS = ["CUR_V25_ONLY","OVEREXT","WEAK_REACCEL","V22_QUALITY_OR"]

RECOVERED_PRE = {"P25SET-EDGEUSDT-1987129":{"time_kst":"2026-09-03 15:15:17","p_v2_score":83.46,"ema9_ema20_gap_pct":2.2016,"rebound_from_low_pct":10.92,"rsi_delta":-8.3,"btc_15m_change_pct":null},"P25SET-CHIPUSDT-1987129":{"time_kst":"2026-09-03 15:15:18","p_v2_score":80.67,"ema9_ema20_gap_pct":1.0883,"rebound_from_low_pct":5.44,"rsi_delta":4.366,"btc_15m_change_pct":null},"P25SET-EGLDUSDT-1987129":{"time_kst":"2026-09-03 15:19:15","p_v2_score":89.98,"ema9_ema20_gap_pct":1.5401,"rebound_from_low_pct":11.7,"rsi_delta":2.443,"btc_15m_change_pct":null},"P25SET-ARUSDT-1987129":{"time_kst":"2026-09-03 15:19:16","p_v2_score":94.88,"ema9_ema20_gap_pct":2.3325,"rebound_from_low_pct":8.91,"rsi_delta":3.597,"btc_15m_change_pct":null},"P25SET-PONSUSDT-1987154":{"time_kst":"2026-09-03 21:30:24","p_v2_score":76.92,"ema9_ema20_gap_pct":1.961,"rebound_from_low_pct":10.55,"rsi_delta":4.355,"btc_15m_change_pct":null},"P25SET-CHIPUSDT-1987154":{"time_kst":"2026-09-03 21:30:24","p_v2_score":93.43,"ema9_ema20_gap_pct":1.4528,"rebound_from_low_pct":6.15,"rsi_delta":0.838,"btc_15m_change_pct":null},"P25SET-UBUSDT-1987154":{"time_kst":"2026-09-03 21:31:26","p_v2_score":70.82,"ema9_ema20_gap_pct":1.7831,"rebound_from_low_pct":7.95,"rsi_delta":1.88,"btc_15m_change_pct":null},"P25SET-ZESTUSDT-1987155":{"time_kst":"2026-09-03 21:45:26","p_v2_score":71.5,"ema9_ema20_gap_pct":2.3454,"rebound_from_low_pct":16.37,"rsi_delta":2.761,"btc_15m_change_pct":null},"P25SET-TWTUSDT-1987155":{"time_kst":"2026-09-03 21:45:27","p_v2_score":74.77,"ema9_ema20_gap_pct":2.0876,"rebound_from_low_pct":8.95,"rsi_delta":1.195,"btc_15m_change_pct":null},"P25SET-USELESSUSDT-1987156":{"time_kst":"2026-09-03 22:00:25","p_v2_score":75.56,"ema9_ema20_gap_pct":2.3793,"rebound_from_low_pct":9.38,"rsi_delta":0.063,"btc_15m_change_pct":null},"P25SET-SPKUSDT-1987156":{"time_kst":"2026-09-03 22:00:27","p_v2_score":74.2,"ema9_ema20_gap_pct":2.0983,"rebound_from_low_pct":10.7,"rsi_delta":2.391,"btc_15m_change_pct":null},"P25SET-FWDIUSDT-1987156":{"time_kst":"2026-09-03 22:00:28","p_v2_score":74.46,"ema9_ema20_gap_pct":0.7378,"rebound_from_low_pct":4.77,"rsi_delta":3.764,"btc_15m_change_pct":null},"P25SET-UNIUSDT-1987156":{"time_kst":"2026-09-03 22:01:28","p_v2_score":75.6,"ema9_ema20_gap_pct":1.742,"rebound_from_low_pct":7.89,"rsi_delta":2.547,"btc_15m_change_pct":null},"P25SET-PONSUSDT-1987157":{"time_kst":"2026-09-03 22:15:26","p_v2_score":74.89,"ema9_ema20_gap_pct":2.1122,"rebound_from_low_pct":8.33,"rsi_delta":-3.382,"btc_15m_change_pct":null},"P25SET-CHIPUSDT-1987157":{"time_kst":"2026-09-03 22:16:27","p_v2_score":89.0,"ema9_ema20_gap_pct":2.3052,"rebound_from_low_pct":10.86,"rsi_delta":-3.733,"btc_15m_change_pct":null},"P25SET-UBUSDT-1987157":{"time_kst":"2026-09-03 22:16:28","p_v2_score":69.74,"ema9_ema20_gap_pct":1.7754,"rebound_from_low_pct":6.18,"rsi_delta":2.951,"btc_15m_change_pct":null},"P25SET-XPLUSDT-1987158":{"time_kst":"2026-09-03 22:30:27","p_v2_score":73.18,"ema9_ema20_gap_pct":0.8916,"rebound_from_low_pct":4.64,"rsi_delta":0.556,"btc_15m_change_pct":null},"P25SET-TWTUSDT-1987158":{"time_kst":"2026-09-03 22:30:27","p_v2_score":76.09,"ema9_ema20_gap_pct":2.3222,"rebound_from_low_pct":9.03,"rsi_delta":0.879,"btc_15m_change_pct":null},"P25SET-REDUSDT-1987158":{"time_kst":"2026-09-03 22:31:29","p_v2_score":73.97,"ema9_ema20_gap_pct":0.2306,"rebound_from_low_pct":4.97,"rsi_delta":7.029,"btc_15m_change_pct":null},"P25SET-FLOCKUSDT-1987159":{"time_kst":"2026-09-03 22:45:27","p_v2_score":89.99,"ema9_ema20_gap_pct":1.4978,"rebound_from_low_pct":9.35,"rsi_delta":9.022,"btc_15m_change_pct":null},"P25SET-SAHARAUSDT-1987159":{"time_kst":"2026-09-03 22:45:28","p_v2_score":83.36,"ema9_ema20_gap_pct":0.6106,"rebound_from_low_pct":3.8,"rsi_delta":2.941,"btc_15m_change_pct":null},"P25SET-APRUSDT-1987160":{"time_kst":"2026-09-03 23:00:27","p_v2_score":69.98,"ema9_ema20_gap_pct":3.5682,"rebound_from_low_pct":12.43,"rsi_delta":0.587,"btc_15m_change_pct":null},"P25SET-CHIPUSDT-1987160":{"time_kst":"2026-09-03 23:01:28","p_v2_score":73.48,"ema9_ema20_gap_pct":2.2458,"rebound_from_low_pct":10.6,"rsi_delta":0.318,"btc_15m_change_pct":null},"P25SET-EDGEUSDT-1987161":{"time_kst":"2026-09-03 23:15:27","p_v2_score":70.86,"ema9_ema20_gap_pct":1.8875,"rebound_from_low_pct":9.8,"rsi_delta":0.087,"btc_15m_change_pct":null},"P25SET-TWTUSDT-1987161":{"time_kst":"2026-09-03 23:15:28","p_v2_score":73.78,"ema9_ema20_gap_pct":2.5202,"rebound_from_low_pct":10.62,"rsi_delta":0.353,"btc_15m_change_pct":null},"P25SET-XPLUSDT-1987161":{"time_kst":"2026-09-03 23:15:28","p_v2_score":90.49,"ema9_ema20_gap_pct":1.3219,"rebound_from_low_pct":6.72,"rsi_delta":1.631,"btc_15m_change_pct":null},"P25SET-DELLUSDT-1987161":{"time_kst":"2026-09-03 23:15:28","p_v2_score":73.03,"ema9_ema20_gap_pct":0.7739,"rebound_from_low_pct":4.36,"rsi_delta":-15.742,"btc_15m_change_pct":null},"P25SET-SOXSUSDT-1987161":{"time_kst":"2026-09-03 23:15:29","p_v2_score":79.16,"ema9_ema20_gap_pct":0.8067,"rebound_from_low_pct":4.57,"rsi_delta":4.828,"btc_15m_change_pct":null},"P25SET-FWDIUSDT-1987162":{"time_kst":"2026-09-03 23:30:28","p_v2_score":83.49,"ema9_ema20_gap_pct":1.2787,"rebound_from_low_pct":6.21,"rsi_delta":6.074,"btc_15m_change_pct":null},"P25SET-PONSUSDT-1987162":{"time_kst":"2026-09-03 23:31:29","p_v2_score":89.16,"ema9_ema20_gap_pct":1.829,"rebound_from_low_pct":7.6,"rsi_delta":4.527,"btc_15m_change_pct":null},"P25SET-APRUSDT-1987187":{"time_kst":"2026-09-04 05:45:32","p_v2_score":68.21,"ema9_ema20_gap_pct":4.1003,"rebound_from_low_pct":24.31,"rsi_delta":0.81,"btc_15m_change_pct":null},"P25SET-UNIUSDT-1987187":{"time_kst":"2026-09-04 05:45:33","p_v2_score":70.06,"ema9_ema20_gap_pct":0.7112,"rebound_from_low_pct":4.31,"rsi_delta":0.445,"btc_15m_change_pct":null},"P25SET-NIULAIUSDT-1987188":{"time_kst":"2026-09-04 06:00:33","p_v2_score":74.76,"ema9_ema20_gap_pct":0.5231,"rebound_from_low_pct":7.84,"rsi_delta":3.665,"btc_15m_change_pct":null},"P25SET-ARBUSDT-1987188":{"time_kst":"2026-09-04 06:01:34","p_v2_score":70.94,"ema9_ema20_gap_pct":1.2076,"rebound_from_low_pct":7.49,"rsi_delta":-0.822,"btc_15m_change_pct":null},"P25SET-USELESSUSDT-1987189":{"time_kst":"2026-09-04 06:15:32","p_v2_score":78.82,"ema9_ema20_gap_pct":1.028,"rebound_from_low_pct":5.04,"rsi_delta":2.861,"btc_15m_change_pct":null},"P25SET-ARUSDT-1987189":{"time_kst":"2026-09-04 06:16:34","p_v2_score":72.66,"ema9_ema20_gap_pct":1.0693,"rebound_from_low_pct":4.95,"rsi_delta":-4.568,"btc_15m_change_pct":null},"P25SET-EDGEUSDT-1987190":{"time_kst":"2026-09-04 06:30:33","p_v2_score":70.38,"ema9_ema20_gap_pct":0.7373,"rebound_from_low_pct":5.29,"rsi_delta":2.829,"btc_15m_change_pct":null},"P25SET-APRUSDT-1987190":{"time_kst":"2026-09-04 06:30:33","p_v2_score":96.02,"ema9_ema20_gap_pct":3.8759,"rebound_from_low_pct":25.04,"rsi_delta":-0.226,"btc_15m_change_pct":null},"P25SET-UNIUSDT-1987190":{"time_kst":"2026-09-04 06:31:35","p_v2_score":78.15,"ema9_ema20_gap_pct":0.9582,"rebound_from_low_pct":6.23,"rsi_delta":9.842,"btc_15m_change_pct":null},"P25SET-PONSUSDT-1987191":{"time_kst":"2026-09-04 06:45:33","p_v2_score":80.0,"ema9_ema20_gap_pct":1.6323,"rebound_from_low_pct":8.29,"rsi_delta":-4.051,"btc_15m_change_pct":null},"P25SET-NIULAIUSDT-1987191":{"time_kst":"2026-09-04 06:45:33","p_v2_score":79.71,"ema9_ema20_gap_pct":1.1086,"rebound_from_low_pct":6.7,"rsi_delta":-1.948,"btc_15m_change_pct":null},"P25SET-ARBUSDT-1987191":{"time_kst":"2026-09-04 06:46:35","p_v2_score":86.18,"ema9_ema20_gap_pct":1.3302,"rebound_from_low_pct":7.97,"rsi_delta":2.212,"btc_15m_change_pct":null},"P25SET-USELESSUSDT-1987192":{"time_kst":"2026-09-04 07:00:33","p_v2_score":85.75,"ema9_ema20_gap_pct":1.2329,"rebound_from_low_pct":6.64,"rsi_delta":0.445,"btc_15m_change_pct":null},"P25SET-CHIPUSDT-1987192":{"time_kst":"2026-09-04 07:00:33","p_v2_score":83.31,"ema9_ema20_gap_pct":0.8672,"rebound_from_low_pct":7.29,"rsi_delta":5.572,"btc_15m_change_pct":null},"P25SET-LITUSDT-1987192":{"time_kst":"2026-09-04 07:00:34","p_v2_score":79.28,"ema9_ema20_gap_pct":0.7218,"rebound_from_low_pct":3.76,"rsi_delta":7.336,"btc_15m_change_pct":null},"P25SET-HEMIUSDT-1987193":{"time_kst":"2026-09-04 07:29:34","p_v2_score":68.93,"ema9_ema20_gap_pct":0.3007,"rebound_from_low_pct":5.83,"rsi_delta":1.026,"btc_15m_change_pct":null},"P25SET-NIULAIUSDT-1987194":{"time_kst":"2026-09-04 07:30:33","p_v2_score":91.0,"ema9_ema20_gap_pct":1.8664,"rebound_from_low_pct":12.17,"rsi_delta":4.813,"btc_15m_change_pct":null},"P25SET-PONSUSDT-1987194":{"time_kst":"2026-09-04 07:30:34","p_v2_score":87.26,"ema9_ema20_gap_pct":1.9719,"rebound_from_low_pct":9.92,"rsi_delta":2.036,"btc_15m_change_pct":null},"P25SET-USELESSUSDT-1987195":{"time_kst":"2026-09-04 07:45:33","p_v2_score":73.58,"ema9_ema20_gap_pct":1.491,"rebound_from_low_pct":7.52,"rsi_delta":-0.704,"btc_15m_change_pct":null},"P25SET-LITUSDT-1987195":{"time_kst":"2026-09-04 07:45:34","p_v2_score":84.94,"ema9_ema20_gap_pct":1.1718,"rebound_from_low_pct":5.36,"rsi_delta":1.765,"btc_15m_change_pct":null},"P25SET-HNTUSDT-1987196":{"time_kst":"2026-09-04 08:00:34","p_v2_score":78.25,"ema9_ema20_gap_pct":4.2537,"rebound_from_low_pct":27.77,"rsi_delta":-0.384,"btc_15m_change_pct":null},"P25SET-HEMIUSDT-1987197":{"time_kst":"2026-09-04 08:15:34","p_v2_score":90.15,"ema9_ema20_gap_pct":1.1309,"rebound_from_low_pct":8.85,"rsi_delta":1.071,"btc_15m_change_pct":null},"P25SET-USELESSUSDT-1987223":{"time_kst":"2026-09-04 14:48:55","p_v2_score":76.13,"ema9_ema20_gap_pct":2.5727,"rebound_from_low_pct":13.39,"rsi_delta":-1.459,"btc_15m_change_pct":-0.1055},"P25SET-XCNUSDT-1987224":{"time_kst":"2026-09-04 15:06:58","p_v2_score":70.28,"ema9_ema20_gap_pct":2.493,"rebound_from_low_pct":11.68,"rsi_delta":2.188,"btc_15m_change_pct":0.0908},"P25SET-DASHUSDT-1987226":{"time_kst":"2026-09-04 15:30:57","p_v2_score":72.49,"ema9_ema20_gap_pct":0.9762,"rebound_from_low_pct":5.27,"rsi_delta":3.305,"btc_15m_change_pct":-0.3141},"P25SET-EDGEUSDT-1987226":{"time_kst":"2026-09-04 15:30:57","p_v2_score":77.97,"ema9_ema20_gap_pct":0.4521,"rebound_from_low_pct":10.8,"rsi_delta":3.538,"btc_15m_change_pct":-0.3141},"P25SET-QUSDT-1987226":{"time_kst":"2026-09-04 15:31:59","p_v2_score":77.75,"ema9_ema20_gap_pct":4.6796,"rebound_from_low_pct":23.04,"rsi_delta":2.499,"btc_15m_change_pct":-0.3027},"P25SET-LPTUSDT-1987227":{"time_kst":"2026-09-04 15:45:59","p_v2_score":68.44,"ema9_ema20_gap_pct":0.2215,"rebound_from_low_pct":3.15,"rsi_delta":6.868,"btc_15m_change_pct":0.199},"P25SET-ZENUSDT-1987229":{"time_kst":"2026-09-04 16:15:58","p_v2_score":82.82,"ema9_ema20_gap_pct":0.797,"rebound_from_low_pct":4.47,"rsi_delta":10.834,"btc_15m_change_pct":-0.0235},"P25SET-DASHUSDT-1987229":{"time_kst":"2026-09-04 16:16:59","p_v2_score":89.56,"ema9_ema20_gap_pct":1.1859,"rebound_from_low_pct":6.32,"rsi_delta":7.011,"btc_15m_change_pct":-0.0205},"P25SET-QUSDT-1987230":{"time_kst":"2026-09-04 16:30:58","p_v2_score":97.11,"ema9_ema20_gap_pct":4.2961,"rebound_from_low_pct":22.3,"rsi_delta":0.61,"btc_15m_change_pct":-0.0541},"P25SET-ZECUSDT-1987230":{"time_kst":"2026-09-04 16:30:58","p_v2_score":77.55,"ema9_ema20_gap_pct":0.5823,"rebound_from_low_pct":3.16,"rsi_delta":1.826,"btc_15m_change_pct":-0.0541},"P25SET-LITUSDT-1987230":{"time_kst":"2026-09-04 16:30:58","p_v2_score":70.71,"ema9_ema20_gap_pct":0.1712,"rebound_from_low_pct":4.28,"rsi_delta":4.592,"btc_15m_change_pct":-0.0541},"P25SET-PIPPINUSDT-1987230":{"time_kst":"2026-09-04 16:30:59","p_v2_score":83.68,"ema9_ema20_gap_pct":0.6928,"rebound_from_low_pct":4.92,"rsi_delta":2.387,"btc_15m_change_pct":-0.0541},"P25SET-EDGEUSDT-1987230":{"time_kst":"2026-09-04 16:34:58","p_v2_score":76.08,"ema9_ema20_gap_pct":1.1683,"rebound_from_low_pct":8.38,"rsi_delta":0.706,"btc_15m_change_pct":-0.0873},"P25SET-ZORAUSDT-1987261":{"time_kst":"2026-09-05 00:15:23","p_v2_score":90.88,"ema9_ema20_gap_pct":1.5123,"rebound_from_low_pct":10.65,"rsi_delta":5.417,"btc_15m_change_pct":0.2589},"P25SET-TACUSDT-1987261":{"time_kst":"2026-09-05 00:15:23","p_v2_score":97.12,"ema9_ema20_gap_pct":2.1053,"rebound_from_low_pct":8.49,"rsi_delta":0.077,"btc_15m_change_pct":0.2589},"P25SET-PORTALUSDT-1987262":{"time_kst":"2026-09-05 00:30:23","p_v2_score":83.78,"ema9_ema20_gap_pct":2.0354,"rebound_from_low_pct":10.36,"rsi_delta":-0.812,"btc_15m_change_pct":0.2266},"P25SET-LITUSDT-1987262":{"time_kst":"2026-09-05 00:30:23","p_v2_score":79.48,"ema9_ema20_gap_pct":0.7458,"rebound_from_low_pct":5.05,"rsi_delta":4.854,"btc_15m_change_pct":0.2266},"P25SET-DASHUSDT-1987263":{"time_kst":"2026-09-05 00:45:23","p_v2_score":71.57,"ema9_ema20_gap_pct":-0.2807,"rebound_from_low_pct":5.85,"rsi_delta":8.587,"btc_15m_change_pct":0.2001},"P25SET-SMCIUSDT-1987263":{"time_kst":"2026-09-05 00:45:24","p_v2_score":68.69,"ema9_ema20_gap_pct":0.9385,"rebound_from_low_pct":5.48,"rsi_delta":1.864,"btc_15m_change_pct":0.2001},"P25SET-TSEMUSDT-1987263":{"time_kst":"2026-09-05 00:45:24","p_v2_score":69.82,"ema9_ema20_gap_pct":1.466,"rebound_from_low_pct":7.6,"rsi_delta":2.07,"btc_15m_change_pct":0.2001},"P25SET-BICOUSDT-1987263":{"time_kst":"2026-09-05 00:45:24","p_v2_score":90.52,"ema9_ema20_gap_pct":0.8454,"rebound_from_low_pct":7.44,"rsi_delta":8.909,"btc_15m_change_pct":0.2001},"P25SET-SYRUPUSDT-1987263":{"time_kst":"2026-09-05 00:45:24","p_v2_score":91.39,"ema9_ema20_gap_pct":0.7262,"rebound_from_low_pct":4.69,"rsi_delta":4.824,"btc_15m_change_pct":0.2001},"P25SET-QUSDT-1987263":{"time_kst":"2026-09-05 00:49:23","p_v2_score":84.46,"ema9_ema20_gap_pct":1.6331,"rebound_from_low_pct":9.85,"rsi_delta":0.309,"btc_15m_change_pct":0.0705},"P25SET-PONSUSDT-1987264":{"time_kst":"2026-09-05 01:00:22","p_v2_score":85.92,"ema9_ema20_gap_pct":1.9234,"rebound_from_low_pct":16.17,"rsi_delta":-1.386,"btc_15m_change_pct":0.0058},"P25SET-TACUSDT-1987264":{"time_kst":"2026-09-05 01:00:23","p_v2_score":91.12,"ema9_ema20_gap_pct":2.0763,"rebound_from_low_pct":9.54,"rsi_delta":2.036,"btc_15m_change_pct":0.0058},"P25SET-TRIAUSDT-1987264":{"time_kst":"2026-09-05 01:02:24","p_v2_score":86.68,"ema9_ema20_gap_pct":1.0581,"rebound_from_low_pct":14.61,"rsi_delta":1.751,"btc_15m_change_pct":0.0804},"P25SET-LPTUSDT-1987264":{"time_kst":"2026-09-05 01:02:25","p_v2_score":69.7,"ema9_ema20_gap_pct":0.1275,"rebound_from_low_pct":8.51,"rsi_delta":1.709,"btc_15m_change_pct":0.0804},"P25SET-LAUSDT-1987264":{"time_kst":"2026-09-05 01:02:26","p_v2_score":89.58,"ema9_ema20_gap_pct":0.484,"rebound_from_low_pct":6.33,"rsi_delta":1.052,"btc_15m_change_pct":0.0804},"P25SET-EGLDUSDT-1987264":{"time_kst":"2026-09-05 01:02:26","p_v2_score":83.88,"ema9_ema20_gap_pct":1.1598,"rebound_from_low_pct":5.55,"rsi_delta":-0.108,"btc_15m_change_pct":0.0804},"P25SET-ZENUSDT-1987265":{"time_kst":"2026-09-05 01:15:24","p_v2_score":73.06,"ema9_ema20_gap_pct":-0.0169,"rebound_from_low_pct":5.01,"rsi_delta":5.262,"btc_15m_change_pct":0.0917},"P25SET-LITUSDT-1987266":{"time_kst":"2026-09-05 01:30:23","p_v2_score":86.73,"ema9_ema20_gap_pct":1.1223,"rebound_from_low_pct":7.55,"rsi_delta":7.15,"btc_15m_change_pct":0.109},"P25SET-CBRSUSDT-1987266":{"time_kst":"2026-09-05 01:30:24","p_v2_score":74.78,"ema9_ema20_gap_pct":2.5671,"rebound_from_low_pct":13.09,"rsi_delta":1.104,"btc_15m_change_pct":0.109},"P25SET-SMCIUSDT-1987266":{"time_kst":"2026-09-05 01:31:24","p_v2_score":77.43,"ema9_ema20_gap_pct":1.2608,"rebound_from_low_pct":7.12,"rsi_delta":0.59,"btc_15m_change_pct":0.1299},"P25SET-ZORAUSDT-1987267":{"time_kst":"2026-09-05 01:45:23","p_v2_score":89.28,"ema9_ema20_gap_pct":3.7415,"rebound_from_low_pct":15.96,"rsi_delta":0.19,"btc_15m_change_pct":0.0763},"P25SET-SYRUPUSDT-1987267":{"time_kst":"2026-09-05 01:45:24","p_v2_score":74.39,"ema9_ema20_gap_pct":1.1904,"rebound_from_low_pct":6.38,"rsi_delta":3.976,"btc_15m_change_pct":0.0763},"P25SET-BICOUSDT-1987267":{"time_kst":"2026-09-05 01:45:24","p_v2_score":82.77,"ema9_ema20_gap_pct":1.9364,"rebound_from_low_pct":11.43,"rsi_delta":1.848,"btc_15m_change_pct":0.0763},"P25SET-ZECUSDT-1987267":{"time_kst":"2026-09-05 01:45:24","p_v2_score":87.95,"ema9_ema20_gap_pct":0.4304,"rebound_from_low_pct":6.25,"rsi_delta":7.09,"btc_15m_change_pct":0.0763},"P25SET-LPTUSDT-1987267":{"time_kst":"2026-09-05 01:47:26","p_v2_score":82.6,"ema9_ema20_gap_pct":0.7396,"rebound_from_low_pct":6.08,"rsi_delta":0.931,"btc_15m_change_pct":-0.0059},"P25SET-TACUSDT-1987268":{"time_kst":"2026-09-05 02:00:24","p_v2_score":84.68,"ema9_ema20_gap_pct":2.3878,"rebound_from_low_pct":10.51,"rsi_delta":-5.439,"btc_15m_change_pct":-0.1026},"P25SET-DASHUSDT-1987268":{"time_kst":"2026-09-05 02:00:24","p_v2_score":93.0,"ema9_ema20_gap_pct":1.5583,"rebound_from_low_pct":10.93,"rsi_delta":3.377,"btc_15m_change_pct":-0.1026},"P25SET-ZENUSDT-1987268":{"time_kst":"2026-09-05 02:00:24","p_v2_score":93.77,"ema9_ema20_gap_pct":1.1035,"rebound_from_low_pct":8.94,"rsi_delta":0.94,"btc_15m_change_pct":-0.1026},"P25SET-0GUSDT-1987268":{"time_kst":"2026-09-05 02:00:25","p_v2_score":69.24,"ema9_ema20_gap_pct":2.1276,"rebound_from_low_pct":9.44,"rsi_delta":-8.02,"btc_15m_change_pct":-0.1026},"P25SET-MERLUSDT-1987268":{"time_kst":"2026-09-05 02:00:25","p_v2_score":71.64,"ema9_ema20_gap_pct":0.3836,"rebound_from_low_pct":8.61,"rsi_delta":-1.635,"btc_15m_change_pct":-0.1026},"P25SET-SUSDT-1987268":{"time_kst":"2026-09-05 02:02:26","p_v2_score":77.21,"ema9_ema20_gap_pct":0.2558,"rebound_from_low_pct":5.67,"rsi_delta":2.538,"btc_15m_change_pct":-0.1466},"P25SET-PEAQUSDT-1987268":{"time_kst":"2026-09-05 02:04:24","p_v2_score":72.22,"ema9_ema20_gap_pct":0.3813,"rebound_from_low_pct":6.57,"rsi_delta":2.009,"btc_15m_change_pct":-0.1741},"P25SET-SNXXUSDT-1987269":{"time_kst":"2026-09-05 02:15:24","p_v2_score":74.87,"ema9_ema20_gap_pct":3.2723,"rebound_from_low_pct":18.01,"rsi_delta":-11.982,"btc_15m_change_pct":-0.1819},"P25SET-COTIUSDT-1987269":{"time_kst":"2026-09-05 02:15:25","p_v2_score":87.75,"ema9_ema20_gap_pct":0.488,"rebound_from_low_pct":4.75,"rsi_delta":8.619,"btc_15m_change_pct":-0.1819},"P25SET-GIGGLEUSDT-1987269":{"time_kst":"2026-09-05 02:17:26","p_v2_score":87.24,"ema9_ema20_gap_pct":0.9913,"rebound_from_low_pct":6.49,"rsi_delta":1.769,"btc_15m_change_pct":-0.1485},"P25SET-QUSDT-1987270":{"time_kst":"2026-09-05 02:30:23","p_v2_score":89.25,"ema9_ema20_gap_pct":2.0779,"rebound_from_low_pct":8.35,"rsi_delta":0.213,"btc_15m_change_pct":-0.0533},"P25SET-NILUSDT-1987317":{"time_kst":"2026-09-05 14:19:35","p_v2_score":77.03,"ema9_ema20_gap_pct":0.8119,"rebound_from_low_pct":3.94,"rsi_delta":8.511,"btc_15m_change_pct":0.0938},"P25SET-BROCCOLIUSDT-1987317":{"time_kst":"2026-09-05 14:22:35","p_v2_score":77.99,"ema9_ema20_gap_pct":2.3652,"rebound_from_low_pct":10.89,"rsi_delta":2.45,"btc_15m_change_pct":0.0536},"P25SET-TRIAUSDT-1987318":{"time_kst":"2026-09-05 14:30:34","p_v2_score":93.79,"ema9_ema20_gap_pct":1.1989,"rebound_from_low_pct":7.99,"rsi_delta":3.333,"btc_15m_change_pct":0.0571},"P25SET-NIULAIUSDT-1987318":{"time_kst":"2026-09-05 14:30:35","p_v2_score":81.77,"ema9_ema20_gap_pct":3.2803,"rebound_from_low_pct":14.4,"rsi_delta":-0.154,"btc_15m_change_pct":0.0571},"P25SET-ZENUSDT-1987319":{"time_kst":"2026-09-05 14:45:34","p_v2_score":100.0,"ema9_ema20_gap_pct":2.2287,"rebound_from_low_pct":10.26,"rsi_delta":6.652,"btc_15m_change_pct":-0.0965},"P25SET-ZKUSDT-1987319":{"time_kst":"2026-09-05 14:45:35","p_v2_score":68.08,"ema9_ema20_gap_pct":1.4059,"rebound_from_low_pct":5.35,"rsi_delta":4.515,"btc_15m_change_pct":-0.0965},"P25SET-TWTUSDT-1987319":{"time_kst":"2026-09-05 14:45:35","p_v2_score":80.58,"ema9_ema20_gap_pct":0.5261,"rebound_from_low_pct":3.99,"rsi_delta":9.72,"btc_15m_change_pct":-0.0965},"P25SET-ROSEUSDT-1987319":{"time_kst":"2026-09-05 14:47:37","p_v2_score":83.4,"ema9_ema20_gap_pct":0.94,"rebound_from_low_pct":4.58,"rsi_delta":7.783,"btc_15m_change_pct":-0.0833},"P25SET-DASHUSDT-1987320":{"time_kst":"2026-09-05 15:00:35","p_v2_score":70.0,"ema9_ema20_gap_pct":3.1415,"rebound_from_low_pct":11.67,"rsi_delta":-2.63,"btc_15m_change_pct":0.092},"P25SET-STRKUSDT-1987320":{"time_kst":"2026-09-05 15:00:36","p_v2_score":68.8,"ema9_ema20_gap_pct":2.1745,"rebound_from_low_pct":7.3,"rsi_delta":-0.353,"btc_15m_change_pct":0.092},"P25SET-PONSUSDT-1987320":{"time_kst":"2026-09-05 15:00:36","p_v2_score":72.01,"ema9_ema20_gap_pct":0.6403,"rebound_from_low_pct":7.47,"rsi_delta":6.248,"btc_15m_change_pct":0.092},"P25SET-NILUSDT-1987320":{"time_kst":"2026-09-05 15:04:36","p_v2_score":78.02,"ema9_ema20_gap_pct":1.4115,"rebound_from_low_pct":5.75,"rsi_delta":2.736,"btc_15m_change_pct":0.1383},"P25SET-NIULAIUSDT-1987321":{"time_kst":"2026-09-05 15:15:35","p_v2_score":79.04,"ema9_ema20_gap_pct":3.4036,"rebound_from_low_pct":15.27,"rsi_delta":-10.663,"btc_15m_change_pct":0.0214},"P25SET-COTIUSDT-1987321":{"time_kst":"2026-09-05 15:15:36","p_v2_score":82.62,"ema9_ema20_gap_pct":0.7269,"rebound_from_low_pct":6.28,"rsi_delta":12.615,"btc_15m_change_pct":0.0214},"P25SET-ICPUSDT-1987321":{"time_kst":"2026-09-05 15:17:37","p_v2_score":75.26,"ema9_ema20_gap_pct":1.4125,"rebound_from_low_pct":6.85,"rsi_delta":3.801,"btc_15m_change_pct":-0.0028},"P25SET-ZENUSDT-1987322":{"time_kst":"2026-09-05 15:30:35","p_v2_score":80.01,"ema9_ema20_gap_pct":2.5123,"rebound_from_low_pct":9.38,"rsi_delta":-4.166,"btc_15m_change_pct":0.0285},"P25SET-PTBUSDT-1987323":{"time_kst":"2026-09-05 15:45:36","p_v2_score":86.25,"ema9_ema20_gap_pct":0.4517,"rebound_from_low_pct":4.49,"rsi_delta":5.995,"btc_15m_change_pct":0.0476},"P25SET-STRKUSDT-1987323":{"time_kst":"2026-09-05 15:48:36","p_v2_score":68.85,"ema9_ema20_gap_pct":2.3602,"rebound_from_low_pct":6.71,"rsi_delta":0.08,"btc_15m_change_pct":0.0597},"P25SET-NEARUSDT-1987324":{"time_kst":"2026-09-05 16:00:41","p_v2_score":84.04,"ema9_ema20_gap_pct":0.6386,"rebound_from_low_pct":4.08,"rsi_delta":3.254,"btc_15m_change_pct":-0.0389},"P25SET-COTIUSDT-1987324":{"time_kst":"2026-09-05 16:00:41","p_v2_score":95.31,"ema9_ema20_gap_pct":1.2191,"rebound_from_low_pct":7.45,"rsi_delta":2.853,"btc_15m_change_pct":-0.0389},"P25SET-PONSUSDT-1987324":{"time_kst":"2026-09-05 16:02:42","p_v2_score":85.61,"ema9_ema20_gap_pct":2.1256,"rebound_from_low_pct":10.72,"rsi_delta":-0.929,"btc_15m_change_pct":0.022},"P25SET-GALAUSDT-1987324":{"time_kst":"2026-09-05 16:02:44","p_v2_score":72.94,"ema9_ema20_gap_pct":1.2806,"rebound_from_low_pct":4.8,"rsi_delta":4.172,"btc_15m_change_pct":0.022},"P25SET-ICPUSDT-1987324":{"time_kst":"2026-09-05 16:02:44","p_v2_score":81.22,"ema9_ema20_gap_pct":1.7112,"rebound_from_low_pct":6.51,"rsi_delta":2.135,"btc_15m_change_pct":0.022},"P25SET-CHIPUSDT-1987325":{"time_kst":"2026-09-05 16:15:42","p_v2_score":94.47,"ema9_ema20_gap_pct":1.2695,"rebound_from_low_pct":8.93,"rsi_delta":8.139,"btc_15m_change_pct":-0.0108},"P25SET-ASTERUSDT-1987343":{"time_kst":"2026-09-05 20:45:49","p_v2_score":78.22,"ema9_ema20_gap_pct":1.4014,"rebound_from_low_pct":6.77,"rsi_delta":2.838,"btc_15m_change_pct":-0.0492},"P25SET-UAIUSDT-1987343":{"time_kst":"2026-09-05 20:48:50","p_v2_score":79.67,"ema9_ema20_gap_pct":0.9157,"rebound_from_low_pct":5.3,"rsi_delta":5.925,"btc_15m_change_pct":-0.0381},"P25SET-PONSUSDT-1987343":{"time_kst":"2026-09-05 20:48:50","p_v2_score":75.57,"ema9_ema20_gap_pct":3.0431,"rebound_from_low_pct":12.58,"rsi_delta":-5.023,"btc_15m_change_pct":-0.0381},"P25SET-ZORAUSDT-1987343":{"time_kst":"2026-09-05 20:50:48","p_v2_score":87.83,"ema9_ema20_gap_pct":1.8218,"rebound_from_low_pct":8.76,"rsi_delta":-1.448,"btc_15m_change_pct":0.0273},"P25SET-NIULAIUSDT-1987344":{"time_kst":"2026-09-05 21:00:48","p_v2_score":74.73,"ema9_ema20_gap_pct":3.4042,"rebound_from_low_pct":13.67,"rsi_delta":1.321,"btc_15m_change_pct":-0.0284},"P25SET-BNCUSDT-1987344":{"time_kst":"2026-09-05 21:06:49","p_v2_score":73.89,"ema9_ema20_gap_pct":1.2827,"rebound_from_low_pct":6.26,"rsi_delta":5.902,"btc_15m_change_pct":0.0292},"P25SET-PTBUSDT-1987345":{"time_kst":"2026-09-05 21:15:48","p_v2_score":90.33,"ema9_ema20_gap_pct":1.3914,"rebound_from_low_pct":6.3,"rsi_delta":1.639,"btc_15m_change_pct":0.0087},"P25SET-ARUSDT-1987345":{"time_kst":"2026-09-05 21:15:50","p_v2_score":82.06,"ema9_ema20_gap_pct":0.5977,"rebound_from_low_pct":4.77,"rsi_delta":5.536,"btc_15m_change_pct":0.0087},"P25SET-SKRUSDT-1987345":{"time_kst":"2026-09-05 21:16:50","p_v2_score":68.81,"ema9_ema20_gap_pct":0.6344,"rebound_from_low_pct":4.62,"rsi_delta":0.901,"btc_15m_change_pct":0.0087},"P25SET-CAKEUSDT-1987345":{"time_kst":"2026-09-05 21:18:51","p_v2_score":72.79,"ema9_ema20_gap_pct":1.478,"rebound_from_low_pct":7.22,"rsi_delta":8.904,"btc_15m_change_pct":0.0106},"P25SET-UAIUSDT-1987346":{"time_kst":"2026-09-05 21:33:50","p_v2_score":69.16,"ema9_ema20_gap_pct":1.0012,"rebound_from_low_pct":5.18,"rsi_delta":4.973,"btc_15m_change_pct":0.0486},"P25SET-PONSUSDT-1987346":{"time_kst":"2026-09-05 21:33:50","p_v2_score":93.7,"ema9_ema20_gap_pct":3.1468,"rebound_from_low_pct":8.91,"rsi_delta":-0.2,"btc_15m_change_pct":0.0486},"P25SET-FORMUSDT-1987346":{"time_kst":"2026-09-05 21:33:51","p_v2_score":84.45,"ema9_ema20_gap_pct":0.9304,"rebound_from_low_pct":5.62,"rsi_delta":10.43,"btc_15m_change_pct":0.0486},"P25SET-DASHUSDT-1987347":{"time_kst":"2026-09-05 21:45:49","p_v2_score":85.52,"ema9_ema20_gap_pct":0.3735,"rebound_from_low_pct":6.59,"rsi_delta":6.67,"btc_15m_change_pct":0.0635},"P25SET-TUTUSDT-1987348":{"time_kst":"2026-09-05 22:00:50","p_v2_score":88.71,"ema9_ema20_gap_pct":2.4091,"rebound_from_low_pct":11.85,"rsi_delta":2.475,"btc_15m_change_pct":-0.0218},"P25SET-UBUSDT-1987348":{"time_kst":"2026-09-05 22:00:50","p_v2_score":69.36,"ema9_ema20_gap_pct":-0.6842,"rebound_from_low_pct":7.84,"rsi_delta":5.338,"btc_15m_change_pct":-0.0218},"P25SET-CAKEUSDT-1987348":{"time_kst":"2026-09-05 22:03:52","p_v2_score":83.18,"ema9_ema20_gap_pct":1.682,"rebound_from_low_pct":5.09,"rsi_delta":3.51,"btc_15m_change_pct":-0.0486},"P25SET-NAORISUSDT-1987348":{"time_kst":"2026-09-05 22:08:51","p_v2_score":74.91,"ema9_ema20_gap_pct":1.687,"rebound_from_low_pct":5.78,"rsi_delta":0.37,"btc_15m_change_pct":-0.0604},"P25SET-WIFUSDT-1987348":{"time_kst":"2026-09-05 22:13:52","p_v2_score":71.27,"ema9_ema20_gap_pct":0.8077,"rebound_from_low_pct":3.43,"rsi_delta":5.844,"btc_15m_change_pct":-0.0079},"P25SET-TRIAUSDT-1987349":{"time_kst":"2026-09-05 22:15:50","p_v2_score":83.15,"ema9_ema20_gap_pct":0.8577,"rebound_from_low_pct":6.51,"rsi_delta":6.142,"btc_15m_change_pct":0.0195},"P25SET-BROCCOLIUSDT-1987349":{"time_kst":"2026-09-05 22:15:50","p_v2_score":72.72,"ema9_ema20_gap_pct":-0.2113,"rebound_from_low_pct":6.11,"rsi_delta":2.541,"btc_15m_change_pct":0.0195},"P25SET-BNCUSDT-1987349":{"time_kst":"2026-09-05 22:23:52","p_v2_score":75.12,"ema9_ema20_gap_pct":1.5654,"rebound_from_low_pct":6.56,"rsi_delta":6.002,"btc_15m_change_pct":0.1059},"P25SET-UAIUSDT-1987350":{"time_kst":"2026-09-05 22:30:49","p_v2_score":91.57,"ema9_ema20_gap_pct":1.2605,"rebound_from_low_pct":6.78,"rsi_delta":3.401,"btc_15m_change_pct":0.0567},"P25SET-BLESSUSDT-1987350":{"time_kst":"2026-09-05 22:30:50","p_v2_score":78.12,"ema9_ema20_gap_pct":1.1049,"rebound_from_low_pct":5.25,"rsi_delta":11.454,"btc_15m_change_pct":0.0567},"P25SET-FFUSDT-1987350":{"time_kst":"2026-09-05 22:30:50","p_v2_score":83.35,"ema9_ema20_gap_pct":1.8966,"rebound_from_low_pct":6.87,"rsi_delta":2.41,"btc_15m_change_pct":0.0567},"P25SET-ENAUSDT-1987383":{"time_kst":"2026-09-06 06:55:56","p_v2_score":90.18,"ema9_ema20_gap_pct":0.9449,"rebound_from_low_pct":5.17,"rsi_delta":1.027,"btc_15m_change_pct":-0.0341},"P25SET-BUSDT-1987384":{"time_kst":"2026-09-06 07:11:55","p_v2_score":70.77,"ema9_ema20_gap_pct":-0.0831,"rebound_from_low_pct":4.25,"rsi_delta":0.168,"btc_15m_change_pct":-0.0264},"P25SET-TIAUSDT-1987384":{"time_kst":"2026-09-06 07:13:58","p_v2_score":71.72,"ema9_ema20_gap_pct":0.7271,"rebound_from_low_pct":3.26,"rsi_delta":5.537,"btc_15m_change_pct":0.0045},"P25SET-LDOUSDT-1987385":{"time_kst":"2026-09-06 07:18:57","p_v2_score":78.59,"ema9_ema20_gap_pct":1.4207,"rebound_from_low_pct":6.06,"rsi_delta":-11.054,"btc_15m_change_pct":-0.0145},"P25SET-STRKUSDT-1987386":{"time_kst":"2026-09-06 07:30:55","p_v2_score":88.92,"ema9_ema20_gap_pct":1.2951,"rebound_from_low_pct":6.67,"rsi_delta":4.805,"btc_15m_change_pct":-0.0469},"P25SET-OPUSDT-1987386":{"time_kst":"2026-09-06 07:30:56","p_v2_score":88.86,"ema9_ema20_gap_pct":2.2707,"rebound_from_low_pct":8.97,"rsi_delta":0.874,"btc_15m_change_pct":-0.0469},"P25SET-NOMUSDT-1987387":{"time_kst":"2026-09-06 07:45:55","p_v2_score":90.28,"ema9_ema20_gap_pct":0.791,"rebound_from_low_pct":8.53,"rsi_delta":6.269,"btc_15m_change_pct":-0.0668},"P25SET-FLOCKUSDT-1987388":{"time_kst":"2026-09-06 08:00:56","p_v2_score":71.9,"ema9_ema20_gap_pct":2.547,"rebound_from_low_pct":6.74,"rsi_delta":-1.388,"btc_15m_change_pct":-0.0073},"P25SET-NIULAIUSDT-1987388":{"time_kst":"2026-09-06 08:10:55","p_v2_score":69.01,"ema9_ema20_gap_pct":-0.0169,"rebound_from_low_pct":9.06,"rsi_delta":1.102,"btc_15m_change_pct":-0.0547},"P25SET-EPICUSDT-1987389":{"time_kst":"2026-09-06 08:15:57","p_v2_score":75.53,"ema9_ema20_gap_pct":0.6388,"rebound_from_low_pct":4.31,"rsi_delta":7.762,"btc_15m_change_pct":0.0128},"P25SET-APRUSDT-1987390":{"time_kst":"2026-09-06 08:30:56","p_v2_score":88.97,"ema9_ema20_gap_pct":0.7032,"rebound_from_low_pct":5.34,"rsi_delta":6.982,"btc_15m_change_pct":-0.0034},"P25SET-FLOCKUSDT-1987412":{"time_kst":"2026-09-06 14:00:05","p_v2_score":72.66,"ema9_ema20_gap_pct":2.5788,"rebound_from_low_pct":12.8,"rsi_delta":2.596,"btc_15m_change_pct":-0.0654},"P25SET-WOOUSDT-1987412":{"time_kst":"2026-09-06 14:00:06","p_v2_score":82.65,"ema9_ema20_gap_pct":1.4762,"rebound_from_low_pct":47.32,"rsi_delta":0.0,"btc_15m_change_pct":-0.0654},"P25SET-IOSTUSDT-1987413":{"time_kst":"2026-09-06 14:15:05","p_v2_score":78.85,"ema9_ema20_gap_pct":2.7246,"rebound_from_low_pct":10.66,"rsi_delta":-0.768,"btc_15m_change_pct":-0.0464},"P25SET-JTOUSDT-1987413":{"time_kst":"2026-09-06 14:15:06","p_v2_score":75.34,"ema9_ema20_gap_pct":1.1562,"rebound_from_low_pct":5.85,"rsi_delta":7.945,"btc_15m_change_pct":-0.0464},"P25SET-ASTERUSDT-1987413":{"time_kst":"2026-09-06 14:15:06","p_v2_score":72.04,"ema9_ema20_gap_pct":0.3146,"rebound_from_low_pct":3.09,"rsi_delta":1.906,"btc_15m_change_pct":-0.0464},"P25SET-ICXUSDT-1987414":{"time_kst":"2026-09-06 14:30:06","p_v2_score":92.67,"ema9_ema20_gap_pct":1.4502,"rebound_from_low_pct":11.51,"rsi_delta":5.915,"btc_15m_change_pct":0.0627},"P25SET-BOMEUSDT-1987414":{"time_kst":"2026-09-06 14:30:06","p_v2_score":84.0,"ema9_ema20_gap_pct":2.9573,"rebound_from_low_pct":13.87,"rsi_delta":3.601,"btc_15m_change_pct":0.0627},"P25SET-COTIUSDT-1987415":{"time_kst":"2026-09-06 14:45:06","p_v2_score":77.34,"ema9_ema20_gap_pct":1.1958,"rebound_from_low_pct":5.15,"rsi_delta":6.808,"btc_15m_change_pct":0.1379},"P25SET-ORCAUSDT-1987415":{"time_kst":"2026-09-06 14:45:06","p_v2_score":83.03,"ema9_ema20_gap_pct":1.2475,"rebound_from_low_pct":5.56,"rsi_delta":3.617,"btc_15m_change_pct":0.1379},"P25SET-FLOCKUSDT-1987416":{"time_kst":"2026-09-06 15:00:05","p_v2_score":83.05,"ema9_ema20_gap_pct":2.8093,"rebound_from_low_pct":12.59,"rsi_delta":-1.941,"btc_15m_change_pct":-0.0354},"P25SET-RAYDIUMUSDT-1987416":{"time_kst":"2026-09-06 15:00:05","p_v2_score":78.37,"ema9_ema20_gap_pct":5.1727,"rebound_from_low_pct":22.71,"rsi_delta":1.868,"btc_15m_change_pct":-0.0354},"P25SET-ZECUSDT-1987416":{"time_kst":"2026-09-06 15:00:06","p_v2_score":76.95,"ema9_ema20_gap_pct":3.0294,"rebound_from_low_pct":10.0,"rsi_delta":-9.221,"btc_15m_change_pct":-0.0354},"P25SET-PENDLEUSDT-1987416":{"time_kst":"2026-09-06 15:00:07","p_v2_score":78.28,"ema9_ema20_gap_pct":0.7084,"rebound_from_low_pct":5.78,"rsi_delta":9.185,"btc_15m_change_pct":-0.0354},"P25SET-METUSDT-1987417":{"time_kst":"2026-09-06 15:15:06","p_v2_score":82.0,"ema9_ema20_gap_pct":1.5059,"rebound_from_low_pct":8.45,"rsi_delta":-8.888,"btc_15m_change_pct":-0.0355},"P25SET-WOOUSDT-1987417":{"time_kst":"2026-09-06 15:20:08","p_v2_score":71.21,"ema9_ema20_gap_pct":1.2478,"rebound_from_low_pct":5.16,"rsi_delta":0.58,"btc_15m_change_pct":-0.0324},"P25SET-COTIUSDT-1987418":{"time_kst":"2026-09-06 15:30:07","p_v2_score":82.68,"ema9_ema20_gap_pct":1.6403,"rebound_from_low_pct":5.92,"rsi_delta":1.231,"btc_15m_change_pct":-0.1253},"P25SET-GRTUSDT-1987418":{"time_kst":"2026-09-06 15:40:07","p_v2_score":80.52,"ema9_ema20_gap_pct":1.811,"rebound_from_low_pct":7.08,"rsi_delta":-0.435,"btc_15m_change_pct":-0.0144},"P25SET-ZECUSDT-1987419":{"time_kst":"2026-09-06 15:45:07","p_v2_score":69.1,"ema9_ema20_gap_pct":2.9927,"rebound_from_low_pct":10.59,"rsi_delta":0.733,"btc_15m_change_pct":-0.0098},"P25SET-PENDLEUSDT-1987419":{"time_kst":"2026-09-06 15:45:08","p_v2_score":75.04,"ema9_ema20_gap_pct":1.1708,"rebound_from_low_pct":5.53,"rsi_delta":1.631,"btc_15m_change_pct":-0.0098},"P25SET-JUPUSDT-1987419":{"time_kst":"2026-09-06 15:45:08","p_v2_score":72.03,"ema9_ema20_gap_pct":0.8818,"rebound_from_low_pct":5.95,"rsi_delta":11.29,"btc_15m_change_pct":-0.0098},"P25SET-NEARUSDT-1987447":{"time_kst":"2026-09-06 22:45:12","p_v2_score":80.79,"ema9_ema20_gap_pct":0.8197,"rebound_from_low_pct":4.25,"rsi_delta":8.669,"btc_15m_change_pct":0.0071},"P25SET-XANUSDT-1987448":{"time_kst":"2026-09-06 23:01:12","p_v2_score":73.02,"ema9_ema20_gap_pct":1.3321,"rebound_from_low_pct":8.06,"rsi_delta":-3.415,"btc_15m_change_pct":0.0321},"P25SET-METISUSDT-1987448":{"time_kst":"2026-09-06 23:11:17","p_v2_score":82.42,"ema9_ema20_gap_pct":2.0158,"rebound_from_low_pct":9.75,"rsi_delta":2.183,"btc_15m_change_pct":-0.0504},"P25SET-NAORISUSDT-1987449":{"time_kst":"2026-09-06 23:15:11","p_v2_score":89.34,"ema9_ema20_gap_pct":4.3481,"rebound_from_low_pct":13.61,"rsi_delta":0.985,"btc_15m_change_pct":-0.0899},"P25SET-CHILLGUYUSDT-1987449":{"time_kst":"2026-09-06 23:15:12","p_v2_score":86.34,"ema9_ema20_gap_pct":2.7985,"rebound_from_low_pct":9.65,"rsi_delta":-0.592,"btc_15m_change_pct":-0.0899},"P25SET-DOODUSDT-1987449":{"time_kst":"2026-09-06 23:15:12","p_v2_score":98.77,"ema9_ema20_gap_pct":2.0599,"rebound_from_low_pct":9.94,"rsi_delta":1.181,"btc_15m_change_pct":-0.0899},"P25SET-EDGEUSDT-1987449":{"time_kst":"2026-09-06 23:15:13","p_v2_score":77.59,"ema9_ema20_gap_pct":0.749,"rebound_from_low_pct":4.65,"rsi_delta":4.146,"btc_15m_change_pct":-0.0899},"P25SET-BSBUSDT-1987449":{"time_kst":"2026-09-06 23:15:13","p_v2_score":78.96,"ema9_ema20_gap_pct":1.1772,"rebound_from_low_pct":6.06,"rsi_delta":7.929,"btc_15m_change_pct":-0.0899},"P25SET-1000BONKUSDT-1987449":{"time_kst":"2026-09-06 23:16:14","p_v2_score":87.56,"ema9_ema20_gap_pct":0.9207,"rebound_from_low_pct":5.05,"rsi_delta":0.883,"btc_15m_change_pct":-0.1169},"P25SET-UAIUSDT-1987451":{"time_kst":"2026-09-06 23:46:13","p_v2_score":68.51,"ema9_ema20_gap_pct":0.2877,"rebound_from_low_pct":5.0,"rsi_delta":3.425,"btc_15m_change_pct":0.076},"P25SET-COTIUSDT-1987452":{"time_kst":"2026-09-07 00:00:12","p_v2_score":74.8,"ema9_ema20_gap_pct":2.0115,"rebound_from_low_pct":8.81,"rsi_delta":0.69,"btc_15m_change_pct":-0.1736},"P25SET-XANUSDT-1987452":{"time_kst":"2026-09-07 00:00:12","p_v2_score":69.74,"ema9_ema20_gap_pct":1.7152,"rebound_from_low_pct":9.7,"rsi_delta":-1.794,"btc_15m_change_pct":-0.1736},"P25SET-RAYDIUMUSDT-1987452":{"time_kst":"2026-09-07 00:00:12","p_v2_score":72.59,"ema9_ema20_gap_pct":3.9412,"rebound_from_low_pct":16.84,"rsi_delta":1.209,"btc_15m_change_pct":-0.1736},"P25SET-BSBUSDT-1987452":{"time_kst":"2026-09-07 00:01:14","p_v2_score":81.33,"ema9_ema20_gap_pct":1.4964,"rebound_from_low_pct":5.99,"rsi_delta":0.305,"btc_15m_change_pct":-0.3251},"P25SET-1000BONKUSDT-1987452":{"time_kst":"2026-09-07 00:01:15","p_v2_score":78.43,"ema9_ema20_gap_pct":1.2787,"rebound_from_low_pct":5.75,"rsi_delta":0.572,"btc_15m_change_pct":-0.3251},"P25SET-USELESSUSDT-1987611":{"time_kst":"2026-09-08 15:45:48","p_v2_score":78.75,"ema9_ema20_gap_pct":2.3536,"rebound_from_low_pct":12.3,"rsi_delta":-5.599,"btc_15m_change_pct":0.0636},"P25SET-BLASTUSDT-1987611":{"time_kst":"2026-09-08 15:45:49","p_v2_score":78.35,"ema9_ema20_gap_pct":1.0711,"rebound_from_low_pct":4.14,"rsi_delta":2.239,"btc_15m_change_pct":0.0636},"P25SET-KAITOUSDT-1987611":{"time_kst":"2026-09-08 15:46:49","p_v2_score":68.18,"ema9_ema20_gap_pct":1.3584,"rebound_from_low_pct":5.56,"rsi_delta":-0.3,"btc_15m_change_pct":0.0777},"P25SET-FORMUSDT-1987612":{"time_kst":"2026-09-08 16:00:51","p_v2_score":92.77,"ema9_ema20_gap_pct":5.2848,"rebound_from_low_pct":27.74,"rsi_delta":1.805,"btc_15m_change_pct":-0.0337},"P25SET-AKEUSDT-1987613":{"time_kst":"2026-09-08 16:15:51","p_v2_score":97.36,"ema9_ema20_gap_pct":2.4969,"rebound_from_low_pct":9.58,"rsi_delta":2.241,"btc_15m_change_pct":0.0871},"P25SET-MEGAUSDT-1987613":{"time_kst":"2026-09-08 16:15:52","p_v2_score":88.59,"ema9_ema20_gap_pct":1.1715,"rebound_from_low_pct":5.34,"rsi_delta":3.993,"btc_15m_change_pct":0.0871},"P25SET-CAKEUSDT-1987615":{"time_kst":"2026-09-08 16:45:52","p_v2_score":89.29,"ema9_ema20_gap_pct":1.1238,"rebound_from_low_pct":7.4,"rsi_delta":10.364,"btc_15m_change_pct":-0.1425},"P25SET-KAITOUSDT-1987615":{"time_kst":"2026-09-08 16:46:53","p_v2_score":75.72,"ema9_ema20_gap_pct":1.3951,"rebound_from_low_pct":6.56,"rsi_delta":4.299,"btc_15m_change_pct":-0.1782},"P25SET-MEGAUSDT-1987616":{"time_kst":"2026-09-08 17:00:53","p_v2_score":71.61,"ema9_ema20_gap_pct":1.473,"rebound_from_low_pct":6.26,"rsi_delta":2.847,"btc_15m_change_pct":0.1178},"P25SET-WLDUSDT-1987616":{"time_kst":"2026-09-08 17:00:54","p_v2_score":70.51,"ema9_ema20_gap_pct":-0.1608,"rebound_from_low_pct":4.27,"rsi_delta":4.632,"btc_15m_change_pct":0.1178},"P25SET-CAKEUSDT-1987618":{"time_kst":"2026-09-08 17:30:54","p_v2_score":87.12,"ema9_ema20_gap_pct":1.6469,"rebound_from_low_pct":6.56,"rsi_delta":-3.744,"btc_15m_change_pct":0.1517},"P25SET-SKRUSDT-1987620":{"time_kst":"2026-09-08 18:00:54","p_v2_score":84.01,"ema9_ema20_gap_pct":0.1485,"rebound_from_low_pct":6.33,"rsi_delta":5.414,"btc_15m_change_pct":-0.1815},"P25SET-USELESSUSDT-1987620":{"time_kst":"2026-09-08 18:00:54","p_v2_score":81.41,"ema9_ema20_gap_pct":0.597,"rebound_from_low_pct":8.54,"rsi_delta":4.062,"btc_15m_change_pct":-0.1815},"P25SET-NAORISUSDT-1987620":{"time_kst":"2026-09-08 18:00:55","p_v2_score":84.29,"ema9_ema20_gap_pct":0.3339,"rebound_from_low_pct":5.53,"rsi_delta":6.476,"btc_15m_change_pct":-0.1815},"P25SET-XPLUSDT-1987620":{"time_kst":"2026-09-08 18:00:55","p_v2_score":73.41,"ema9_ema20_gap_pct":0.2802,"rebound_from_low_pct":5.18,"rsi_delta":6.893,"btc_15m_change_pct":-0.1815},"P25SET-KATUSDT-1987705":{"time_kst":"2026-09-09 15:15:18","p_v2_score":78.37,"ema9_ema20_gap_pct":3.4025,"rebound_from_low_pct":19.57,"rsi_delta":1.058,"btc_15m_change_pct":0.2933},"P25SET-PONSUSDT-1987705":{"time_kst":"2026-09-09 15:25:21","p_v2_score":85.02,"ema9_ema20_gap_pct":1.497,"rebound_from_low_pct":11.68,"rsi_delta":2.689,"btc_15m_change_pct":0.1261},"P25SET-NEARUSDT-1987706":{"time_kst":"2026-09-09 15:30:47","p_v2_score":83.67,"ema9_ema20_gap_pct":0.635,"rebound_from_low_pct":5.78,"rsi_delta":5.139,"btc_15m_change_pct":0.0633},"P25SET-PUMPFUNUSDT-1987706":{"time_kst":"2026-09-09 15:31:47","p_v2_score":79.35,"ema9_ema20_gap_pct":0.7249,"rebound_from_low_pct":4.97,"rsi_delta":8.843,"btc_15m_change_pct":0.0239},"P25SET-SKRUSDT-1987706":{"time_kst":"2026-09-09 15:31:47","p_v2_score":71.4,"ema9_ema20_gap_pct":1.4879,"rebound_from_low_pct":7.12,"rsi_delta":0.773,"btc_15m_change_pct":0.0239},"P25SET-JTOUSDT-1987706":{"time_kst":"2026-09-09 15:31:48","p_v2_score":75.66,"ema9_ema20_gap_pct":0.6288,"rebound_from_low_pct":4.35,"rsi_delta":8.962,"btc_15m_change_pct":0.0239},"P25SET-BTRUSDT-1987707":{"time_kst":"2026-09-09 15:45:46","p_v2_score":83.84,"ema9_ema20_gap_pct":0.6315,"rebound_from_low_pct":4.85,"rsi_delta":5.442,"btc_15m_change_pct":-0.0387},"P25SET-ETHFIUSDT-1987707":{"time_kst":"2026-09-09 15:45:47","p_v2_score":78.31,"ema9_ema20_gap_pct":1.0894,"rebound_from_low_pct":6.31,"rsi_delta":3.403,"btc_15m_change_pct":-0.0387},"P25SET-STRKUSDT-1987707":{"time_kst":"2026-09-09 15:45:47","p_v2_score":73.58,"ema9_ema20_gap_pct":0.9815,"rebound_from_low_pct":4.9,"rsi_delta":0.937,"btc_15m_change_pct":-0.0387},"P25SET-LITUSDT-1987707":{"time_kst":"2026-09-09 15:46:47","p_v2_score":76.85,"ema9_ema20_gap_pct":1.7334,"rebound_from_low_pct":8.1,"rsi_delta":2.858,"btc_15m_change_pct":-0.0347},"P25SET-NAORISUSDT-1987708":{"time_kst":"2026-09-09 16:00:47","p_v2_score":81.87,"ema9_ema20_gap_pct":0.832,"rebound_from_low_pct":5.0,"rsi_delta":1.172,"btc_15m_change_pct":0.0149},"P25SET-ZENUSDT-1987708":{"time_kst":"2026-09-09 16:00:48","p_v2_score":80.79,"ema9_ema20_gap_pct":0.7648,"rebound_from_low_pct":4.91,"rsi_delta":2.114,"btc_15m_change_pct":0.0149},"P25SET-NEARUSDT-1987709":{"time_kst":"2026-09-09 16:15:47","p_v2_score":80.59,"ema9_ema20_gap_pct":0.9702,"rebound_from_low_pct":6.44,"rsi_delta":0.382,"btc_15m_change_pct":0.087},"P25SET-BTRUSDT-1987710":{"time_kst":"2026-09-09 16:30:46","p_v2_score":80.09,"ema9_ema20_gap_pct":1.2551,"rebound_from_low_pct":6.0,"rsi_delta":1.475,"btc_15m_change_pct":0.0276},"P25SET-GRASSUSDT-1987710":{"time_kst":"2026-09-09 16:30:47","p_v2_score":72.0,"ema9_ema20_gap_pct":2.3678,"rebound_from_low_pct":10.0,"rsi_delta":-6.08,"btc_15m_change_pct":0.0276},"P25SET-LITUSDT-1987710":{"time_kst":"2026-09-09 16:37:46","p_v2_score":72.33,"ema9_ema20_gap_pct":1.746,"rebound_from_low_pct":8.8,"rsi_delta":0.792,"btc_15m_change_pct":-0.0852},"P25SET-MINAUSDT-1987711":{"time_kst":"2026-09-09 16:45:47","p_v2_score":78.55,"ema9_ema20_gap_pct":2.0129,"rebound_from_low_pct":8.6,"rsi_delta":-1.632,"btc_15m_change_pct":-0.0232},"P25SET-GPSUSDT-1987712":{"time_kst":"2026-09-09 17:00:47","p_v2_score":87.19,"ema9_ema20_gap_pct":1.1425,"rebound_from_low_pct":5.65,"rsi_delta":2.24,"btc_15m_change_pct":0.1291},"P25SET-NEARUSDT-1987712":{"time_kst":"2026-09-09 17:00:47","p_v2_score":87.2,"ema9_ema20_gap_pct":1.3437,"rebound_from_low_pct":7.17,"rsi_delta":0.15,"btc_15m_change_pct":0.1291},"P25SET-NEARUSDT-1987730":{"time_kst":"2026-09-09 21:30:20","p_v2_score":82.0,"ema9_ema20_gap_pct":2.0865,"rebound_from_low_pct":7.52,"rsi_delta":-2.46,"btc_15m_change_pct":0.0551},"P25SET-BRUSDT-1987730":{"time_kst":"2026-09-09 21:34:21","p_v2_score":69.08,"ema9_ema20_gap_pct":2.3722,"rebound_from_low_pct":8.24,"rsi_delta":2.504,"btc_15m_change_pct":0.1532},"P25SET-LRCUSDT-1987730":{"time_kst":"2026-09-09 21:41:20","p_v2_score":82.07,"ema9_ema20_gap_pct":1.951,"rebound_from_low_pct":10.2,"rsi_delta":5.479,"btc_15m_change_pct":0.1241},"P25SET-IOSTUSDT-1987731":{"time_kst":"2026-09-09 21:45:19","p_v2_score":84.09,"ema9_ema20_gap_pct":2.5852,"rebound_from_low_pct":9.1,"rsi_delta":-1.275,"btc_15m_change_pct":0.0233},"P25SET-RAYDIUMUSDT-1987731":{"time_kst":"2026-09-09 21:45:20","p_v2_score":91.14,"ema9_ema20_gap_pct":0.7828,"rebound_from_low_pct":7.79,"rsi_delta":5.311,"btc_15m_change_pct":0.0233},"P25SET-COTIUSDT-1987731":{"time_kst":"2026-09-09 21:45:21","p_v2_score":82.43,"ema9_ema20_gap_pct":1.5051,"rebound_from_low_pct":6.45,"rsi_delta":0.352,"btc_15m_change_pct":0.0233},"P25SET-PUMPFUNUSDT-1987731":{"time_kst":"2026-09-09 21:46:20","p_v2_score":94.56,"ema9_ema20_gap_pct":0.9838,"rebound_from_low_pct":8.7,"rsi_delta":4.854,"btc_15m_change_pct":0.0599},"P25SET-STRKUSDT-1987732":{"time_kst":"2026-09-09 22:00:24","p_v2_score":76.51,"ema9_ema20_gap_pct":0.5515,"rebound_from_low_pct":4.73,"rsi_delta":9.667,"btc_15m_change_pct":-0.0716},"P25SET-KATUSDT-1987732":{"time_kst":"2026-09-09 22:04:21","p_v2_score":69.95,"ema9_ema20_gap_pct":1.3756,"rebound_from_low_pct":8.5,"rsi_delta":2.371,"btc_15m_change_pct":-0.0706},"P25SET-UAIUSDT-1987732":{"time_kst":"2026-09-09 22:10:25","p_v2_score":93.0,"ema9_ema20_gap_pct":1.8479,"rebound_from_low_pct":9.87,"rsi_delta":4.906,"btc_15m_change_pct":-0.1047},"P25SET-APTUSDT-1987733":{"time_kst":"2026-09-09 22:15:25","p_v2_score":74.74,"ema9_ema20_gap_pct":0.5777,"rebound_from_low_pct":5.46,"rsi_delta":7.721,"btc_15m_change_pct":-0.1157},"P25SET-MINAUSDT-1987734":{"time_kst":"2026-09-09 22:30:24","p_v2_score":77.83,"ema9_ema20_gap_pct":1.4231,"rebound_from_low_pct":7.59,"rsi_delta":3.029,"btc_15m_change_pct":0.0457},"P25SET-GPSUSDT-1987734":{"time_kst":"2026-09-09 22:30:24","p_v2_score":72.58,"ema9_ema20_gap_pct":0.0819,"rebound_from_low_pct":3.71,"rsi_delta":4.22,"btc_15m_change_pct":0.0457},"P25SET-RAYDIUMUSDT-1987734":{"time_kst":"2026-09-09 22:30:24","p_v2_score":70.36,"ema9_ema20_gap_pct":0.9972,"rebound_from_low_pct":7.47,"rsi_delta":1.924,"btc_15m_change_pct":0.0457},"P25SET-COTIUSDT-1987734":{"time_kst":"2026-09-09 22:30:25","p_v2_score":83.92,"ema9_ema20_gap_pct":1.8305,"rebound_from_low_pct":7.98,"rsi_delta":2.042,"btc_15m_change_pct":0.0457},"P25SET-PHAUSDT-1987734":{"time_kst":"2026-09-09 22:35:24","p_v2_score":77.04,"ema9_ema20_gap_pct":0.7224,"rebound_from_low_pct":4.11,"rsi_delta":2.046,"btc_15m_change_pct":0.0555},"P25SET-NIULAIUSDT-1987735":{"time_kst":"2026-09-09 22:54:23","p_v2_score":73.33,"ema9_ema20_gap_pct":3.3575,"rebound_from_low_pct":27.99,"rsi_delta":0.643,"btc_15m_change_pct":-0.1291},"P25SET-AIOZUSDT-1987736":{"time_kst":"2026-09-09 23:00:25","p_v2_score":90.14,"ema9_ema20_gap_pct":0.5408,"rebound_from_low_pct":6.76,"rsi_delta":3.624,"btc_15m_change_pct":-0.2117},"P25SET-LITEUSDT-1987736":{"time_kst":"2026-09-09 23:00:26","p_v2_score":77.03,"ema9_ema20_gap_pct":0.5914,"rebound_from_low_pct":5.04,"rsi_delta":3.66,"btc_15m_change_pct":-0.2117},"P25SET-APTUSDT-1987736":{"time_kst":"2026-09-09 23:00:26","p_v2_score":76.99,"ema9_ema20_gap_pct":0.9825,"rebound_from_low_pct":5.77,"rsi_delta":5.795,"btc_15m_change_pct":-0.2117},"P25SET-GPSUSDT-1987737":{"time_kst":"2026-09-09 23:15:26","p_v2_score":73.23,"ema9_ema20_gap_pct":0.6316,"rebound_from_low_pct":4.99,"rsi_delta":1.39,"btc_15m_change_pct":-0.0067},"P25SET-COTIUSDT-1987737":{"time_kst":"2026-09-09 23:16:25","p_v2_score":90.67,"ema9_ema20_gap_pct":2.1991,"rebound_from_low_pct":9.73,"rsi_delta":0.162,"btc_15m_change_pct":0.0271},"P25SET-RAYDIUMUSDT-1987738":{"time_kst":"2026-09-09 23:30:26","p_v2_score":92.75,"ema9_ema20_gap_pct":0.9837,"rebound_from_low_pct":7.86,"rsi_delta":9.216,"btc_15m_change_pct":0.3754},"P25SET-PUMPFUNUSDT-1987738":{"time_kst":"2026-09-09 23:30:26","p_v2_score":69.5,"ema9_ema20_gap_pct":0.9304,"rebound_from_low_pct":7.94,"rsi_delta":5.765,"btc_15m_change_pct":0.3754},"P25SET-NEARUSDT-1987738":{"time_kst":"2026-09-09 23:30:26","p_v2_score":70.97,"ema9_ema20_gap_pct":1.3748,"rebound_from_low_pct":5.45,"rsi_delta":8.159,"btc_15m_change_pct":0.3754},"P25SET-GRASSUSDT-1987739":{"time_kst":"2026-09-09 23:45:27","p_v2_score":81.78,"ema9_ema20_gap_pct":0.406,"rebound_from_low_pct":4.59,"rsi_delta":3.752,"btc_15m_change_pct":-0.4551},"P25SET-APTUSDT-1987739":{"time_kst":"2026-09-09 23:45:28","p_v2_score":78.51,"ema9_ema20_gap_pct":1.1855,"rebound_from_low_pct":6.15,"rsi_delta":-1.077,"btc_15m_change_pct":-0.4551},"P25SET-ZENUSDT-1987739":{"time_kst":"2026-09-09 23:45:28","p_v2_score":83.96,"ema9_ema20_gap_pct":0.6065,"rebound_from_low_pct":4.98,"rsi_delta":6.439,"btc_15m_change_pct":-0.4551},"P25SET-UAIUSDT-1987739":{"time_kst":"2026-09-09 23:45:28","p_v2_score":81.49,"ema9_ema20_gap_pct":2.5451,"rebound_from_low_pct":9.55,"rsi_delta":-0.552,"btc_15m_change_pct":-0.4551},"P25SET-PHAUSDT-1987739":{"time_kst":"2026-09-09 23:50:49","p_v2_score":72.99,"ema9_ema20_gap_pct":1.0974,"rebound_from_low_pct":5.48,"rsi_delta":2.72,"btc_15m_change_pct":-0.1955},"P25SET-UAIUSDT-1987761":{"time_kst":"2026-09-10 05:15:51","p_v2_score":81.77,"ema9_ema20_gap_pct":0.5421,"rebound_from_low_pct":4.85,"rsi_delta":1.801,"btc_15m_change_pct":0.0773},"P25SET-MINAUSDT-1987762":{"time_kst":"2026-09-10 05:30:51","p_v2_score":91.39,"ema9_ema20_gap_pct":0.0061,"rebound_from_low_pct":5.32,"rsi_delta":4.729,"btc_15m_change_pct":-0.0911},"P25SET-SAHARAUSDT-1987764":{"time_kst":"2026-09-10 06:00:52","p_v2_score":76.72,"ema9_ema20_gap_pct":0.2258,"rebound_from_low_pct":3.4,"rsi_delta":3.585,"btc_15m_change_pct":0.2406},"P25SET-BTRUSDT-1987764":{"time_kst":"2026-09-10 06:00:52","p_v2_score":89.79,"ema9_ema20_gap_pct":1.2754,"rebound_from_low_pct":7.2,"rsi_delta":4.428,"btc_15m_change_pct":0.2406},"P25SET-MINAUSDT-1987765":{"time_kst":"2026-09-10 06:15:52","p_v2_score":79.31,"ema9_ema20_gap_pct":0.9724,"rebound_from_low_pct":7.41,"rsi_delta":3.886,"btc_15m_change_pct":-0.1032},"P25SET-ARXUSDT-1987765":{"time_kst":"2026-09-10 06:15:53","p_v2_score":74.4,"ema9_ema20_gap_pct":0.7361,"rebound_from_low_pct":4.66,"rsi_delta":5.174,"btc_15m_change_pct":-0.1032},"P25SET-KASUSDT-1987765":{"time_kst":"2026-09-10 06:15:53","p_v2_score":85.16,"ema9_ema20_gap_pct":0.5118,"rebound_from_low_pct":4.45,"rsi_delta":13.051,"btc_15m_change_pct":-0.1032},"P25SET-SAHARAUSDT-1987768":{"time_kst":"2026-09-10 07:00:52","p_v2_score":79.49,"ema9_ema20_gap_pct":1.6835,"rebound_from_low_pct":8.79,"rsi_delta":-0.342,"btc_15m_change_pct":0.0756},"P25SET-KATUSDT-1987768":{"time_kst":"2026-09-10 07:00:52","p_v2_score":78.33,"ema9_ema20_gap_pct":1.4862,"rebound_from_low_pct":6.0,"rsi_delta":3.311,"btc_15m_change_pct":0.0756},"P25SET-KASUSDT-1987768":{"time_kst":"2026-09-10 07:01:52","p_v2_score":73.74,"ema9_ema20_gap_pct":0.8037,"rebound_from_low_pct":4.76,"rsi_delta":7.625,"btc_15m_change_pct":0.091},"P25SET-NEARUSDT-1987791":{"time_kst":"2026-09-10 12:45:39","p_v2_score":72.28,"ema9_ema20_gap_pct":0.3084,"rebound_from_low_pct":4.25,"rsi_delta":0.812,"btc_15m_change_pct":-0.0331},"P25SET-PORTALUSDT-1987791":{"time_kst":"2026-09-10 12:50:39","p_v2_score":73.98,"ema9_ema20_gap_pct":0.3487,"rebound_from_low_pct":3.01,"rsi_delta":1.906,"btc_15m_change_pct":0.1067},"P25SET-RAYDIUMUSDT-1987792":{"time_kst":"2026-09-10 13:01:39","p_v2_score":91.06,"ema9_ema20_gap_pct":0.4847,"rebound_from_low_pct":8.05,"rsi_delta":3.217,"btc_15m_change_pct":-0.1538},"P25SET-SAHARAUSDT-1987793":{"time_kst":"2026-09-10 13:15:39","p_v2_score":86.22,"ema9_ema20_gap_pct":1.4016,"rebound_from_low_pct":10.16,"rsi_delta":2.758,"btc_15m_change_pct":0.0079},"P25SET-BLASTUSDT-1987793":{"time_kst":"2026-09-10 13:16:40","p_v2_score":85.14,"ema9_ema20_gap_pct":1.7922,"rebound_from_low_pct":7.51,"rsi_delta":0.664,"btc_15m_change_pct":0.0126},"P25SET-ARXUSDT-1987795":{"time_kst":"2026-09-10 13:46:40","p_v2_score":75.59,"ema9_ema20_gap_pct":1.3415,"rebound_from_low_pct":7.06,"rsi_delta":3.529,"btc_15m_change_pct":-0.0499},"P25SET-METUSDT-1987796":{"time_kst":"2026-09-10 14:00:40","p_v2_score":78.08,"ema9_ema20_gap_pct":0.5832,"rebound_from_low_pct":4.34,"rsi_delta":7.777,"btc_15m_change_pct":0.0815},"P25SET-KASUSDT-1987798":{"time_kst":"2026-09-10 14:30:42","p_v2_score":75.17,"ema9_ema20_gap_pct":0.194,"rebound_from_low_pct":3.29,"rsi_delta":5.517,"btc_15m_change_pct":0.1394},"P25SET-ARXUSDT-1987798":{"time_kst":"2026-09-10 14:43:40","p_v2_score":95.0,"ema9_ema20_gap_pct":1.5967,"rebound_from_low_pct":9.13,"rsi_delta":6.507,"btc_15m_change_pct":-0.071},"P25SET-COTIUSDT-1987800":{"time_kst":"2026-09-10 15:00:41","p_v2_score":84.66,"ema9_ema20_gap_pct":0.271,"rebound_from_low_pct":8.86,"rsi_delta":4.651,"btc_15m_change_pct":0.1375},"P25SET-EGLDUSDT-1987801":{"time_kst":"2026-09-10 15:15:42","p_v2_score":71.53,"ema9_ema20_gap_pct":-0.0119,"rebound_from_low_pct":3.48,"rsi_delta":3.71,"btc_15m_change_pct":-0.1916},"P25SET-ARXUSDT-1987802":{"time_kst":"2026-09-10 15:30:41","p_v2_score":79.27,"ema9_ema20_gap_pct":2.6482,"rebound_from_low_pct":8.68,"rsi_delta":-1.592,"btc_15m_change_pct":0.0159},"P25SET-REZUSDT-1987802":{"time_kst":"2026-09-10 15:31:42","p_v2_score":73.0,"ema9_ema20_gap_pct":3.285,"rebound_from_low_pct":15.86,"rsi_delta":4.599,"btc_15m_change_pct":-0.0226},"P25SET-KASUSDT-1987804":{"time_kst":"2026-09-10 16:00:42","p_v2_score":81.45,"ema9_ema20_gap_pct":0.9494,"rebound_from_low_pct":5.73,"rsi_delta":8.204,"btc_15m_change_pct":-0.091},"P25SET-EGLDUSDT-1987804":{"time_kst":"2026-09-10 16:00:42","p_v2_score":86.48,"ema9_ema20_gap_pct":0.6203,"rebound_from_low_pct":5.25,"rsi_delta":3.487,"btc_15m_change_pct":-0.091},"P25SET-THETAUSDT-1987924":{"time_kst":"2026-09-11 22:00:27","p_v2_score":87.14,"ema9_ema20_gap_pct":1.4362,"rebound_from_low_pct":5.65,"rsi_delta":0.126,"btc_15m_change_pct":0.1038},"P25SET-NEARUSDT-1987924":{"time_kst":"2026-09-11 22:00:28","p_v2_score":89.92,"ema9_ema20_gap_pct":0.588,"rebound_from_low_pct":7.51,"rsi_delta":5.43,"btc_15m_change_pct":0.1038},"P25SET-MINAUSDT-1987924":{"time_kst":"2026-09-11 22:00:28","p_v2_score":85.99,"ema9_ema20_gap_pct":0.4116,"rebound_from_low_pct":5.12,"rsi_delta":12.401,"btc_15m_change_pct":0.1038},"P25SET-NEARUSDT-1987927":{"time_kst":"2026-09-11 22:45:35","p_v2_score":78.35,"ema9_ema20_gap_pct":1.5577,"rebound_from_low_pct":9.56,"rsi_delta":2.897,"btc_15m_change_pct":0.3174},"P25SET-THETAUSDT-1987927":{"time_kst":"2026-09-11 22:45:35","p_v2_score":81.4,"ema9_ema20_gap_pct":1.6145,"rebound_from_low_pct":7.31,"rsi_delta":4.228,"btc_15m_change_pct":0.3174},"P25SET-BNCUSDT-1987928":{"time_kst":"2026-09-11 23:00:02","p_v2_score":96.61,"ema9_ema20_gap_pct":1.0768,"rebound_from_low_pct":7.21,"rsi_delta":3.925,"btc_15m_change_pct":0.9018},"P25SET-RAYDIUMUSDT-1987929":{"time_kst":"2026-09-11 23:15:02","p_v2_score":94.24,"ema9_ema20_gap_pct":0.935,"rebound_from_low_pct":12.28,"rsi_delta":5.287,"btc_15m_change_pct":-0.6054},"P25SET-METUSDT-1987929":{"time_kst":"2026-09-11 23:15:02","p_v2_score":70.99,"ema9_ema20_gap_pct":1.4239,"rebound_from_low_pct":6.81,"rsi_delta":1.431,"btc_15m_change_pct":-0.6054},"P25SET-EIGENUSDT-1987929":{"time_kst":"2026-09-11 23:15:03","p_v2_score":92.61,"ema9_ema20_gap_pct":0.9611,"rebound_from_low_pct":7.94,"rsi_delta":0.134,"btc_15m_change_pct":-0.6054},"P25SET-THETAUSDT-1987930":{"time_kst":"2026-09-11 23:30:37","p_v2_score":84.92,"ema9_ema20_gap_pct":2.0892,"rebound_from_low_pct":9.09,"rsi_delta":-1.384,"btc_15m_change_pct":-0.2926},"P25SET-NEARUSDT-1987931":{"time_kst":"2026-09-11 23:45:38","p_v2_score":79.29,"ema9_ema20_gap_pct":2.2884,"rebound_from_low_pct":11.44,"rsi_delta":-6.273,"btc_15m_change_pct":-0.0917},"P25SET-RAYDIUMUSDT-1987932":{"time_kst":"2026-09-12 00:00:37","p_v2_score":77.12,"ema9_ema20_gap_pct":1.3054,"rebound_from_low_pct":12.05,"rsi_delta":5.01,"btc_15m_change_pct":0.0867},"P25SET-EIGENUSDT-1987932":{"time_kst":"2026-09-12 00:00:38","p_v2_score":69.89,"ema9_ema20_gap_pct":1.5462,"rebound_from_low_pct":8.77,"rsi_delta":-1.302,"btc_15m_change_pct":0.0867},"P25SET-METUSDT-1987932":{"time_kst":"2026-09-12 00:01:39","p_v2_score":72.09,"ema9_ema20_gap_pct":1.1046,"rebound_from_low_pct":5.76,"rsi_delta":4.332,"btc_15m_change_pct":-0.1317},"P25SET-PUFFERUSDT-1987932":{"time_kst":"2026-09-12 00:01:39","p_v2_score":85.36,"ema9_ema20_gap_pct":0.9532,"rebound_from_low_pct":7.98,"rsi_delta":-4.838,"btc_15m_change_pct":-0.1317},"P25SET-1000RATSUSDT-1987932":{"time_kst":"2026-09-12 00:01:41","p_v2_score":75.86,"ema9_ema20_gap_pct":0.5051,"rebound_from_low_pct":4.28,"rsi_delta":4.11,"btc_15m_change_pct":-0.1317},"P25SET-MINAUSDT-1987933":{"time_kst":"2026-09-12 00:16:39","p_v2_score":74.3,"ema9_ema20_gap_pct":3.7788,"rebound_from_low_pct":16.31,"rsi_delta":0.906,"btc_15m_change_pct":-0.1183},"P25SET-XCNUSDT-1987957":{"time_kst":"2026-09-12 06:16:04","p_v2_score":85.06,"ema9_ema20_gap_pct":1.3468,"rebound_from_low_pct":7.18,"rsi_delta":6.719,"btc_15m_change_pct":0.0414},"P25SET-RAYDIUMUSDT-1987957":{"time_kst":"2026-09-12 06:18:06","p_v2_score":68.23,"ema9_ema20_gap_pct":0.2588,"rebound_from_low_pct":6.12,"rsi_delta":2.725,"btc_15m_change_pct":0.0829},"P25SET-4USDT-1987957":{"time_kst":"2026-09-12 06:18:06","p_v2_score":69.2,"ema9_ema20_gap_pct":0.8541,"rebound_from_low_pct":5.48,"rsi_delta":1.826,"btc_15m_change_pct":0.0829},"P25SET-LSKUSDT-1987959":{"time_kst":"2026-09-12 06:45:04","p_v2_score":86.0,"ema9_ema20_gap_pct":2.1297,"rebound_from_low_pct":11.34,"rsi_delta":-3.657,"btc_15m_change_pct":-0.0381},"P25SET-RAYDIUMUSDT-1987961":{"time_kst":"2026-09-12 07:17:30","p_v2_score":73.0,"ema9_ema20_gap_pct":1.0387,"rebound_from_low_pct":7.93,"rsi_delta":1.489,"btc_15m_change_pct":-0.0017},"P25SET-LSKUSDT-1987962":{"time_kst":"2026-09-12 07:30:30","p_v2_score":87.35,"ema9_ema20_gap_pct":2.5957,"rebound_from_low_pct":13.69,"rsi_delta":-7.156,"btc_15m_change_pct":-0.1216},"P25SET-XCNUSDT-1987962":{"time_kst":"2026-09-12 07:34:30","p_v2_score":78.66,"ema9_ema20_gap_pct":2.5297,"rebound_from_low_pct":10.3,"rsi_delta":-12.615,"btc_15m_change_pct":-0.2196},"P25SET-XCNUSDT-1988278":{"time_kst":"2026-09-15 14:30:09","p_v2_score":88.62,"ema9_ema20_gap_pct":0.7922,"rebound_from_low_pct":4.73,"rsi_delta":4.153,"btc_15m_change_pct":0.1731},"P25SET-ZILUSDT-1988280":{"time_kst":"2026-09-15 15:00:09","p_v2_score":80.35,"ema9_ema20_gap_pct":1.107,"rebound_from_low_pct":6.12,"rsi_delta":3.738,"btc_15m_change_pct":0.2116},"P25SET-MIRAUSDT-1988282":{"time_kst":"2026-09-15 15:30:10","p_v2_score":79.83,"ema9_ema20_gap_pct":0.5159,"rebound_from_low_pct":3.98,"rsi_delta":5.164,"btc_15m_change_pct":-0.186},"P25SET-PUFFERUSDT-1988283":{"time_kst":"2026-09-15 15:57:10","p_v2_score":68.03,"ema9_ema20_gap_pct":6.2188,"rebound_from_low_pct":27.57,"rsi_delta":-0.645,"btc_15m_change_pct":-0.2557},"P25SET-AKEUSDT-1988286":{"time_kst":"2026-09-15 16:30:10","p_v2_score":83.05,"ema9_ema20_gap_pct":3.6032,"rebound_from_low_pct":15.14,"rsi_delta":-5.048,"btc_15m_change_pct":-0.0026},"P25SET-SAGAUSDT-1988313":{"time_kst":"2026-09-15 23:19:06","p_v2_score":73.0,"ema9_ema20_gap_pct":0.9976,"rebound_from_low_pct":6.83,"rsi_delta":2.914,"btc_15m_change_pct":-0.097},"P25SET-POWERUSDT-1988319":{"time_kst":"2026-09-16 00:45:06","p_v2_score":70.16,"ema9_ema20_gap_pct":0.7236,"rebound_from_low_pct":12.37,"rsi_delta":3.391,"btc_15m_change_pct":0.0329},"P25SET-SAGAUSDT-1988319":{"time_kst":"2026-09-16 00:45:06","p_v2_score":87.98,"ema9_ema20_gap_pct":1.4435,"rebound_from_low_pct":9.27,"rsi_delta":-1.097,"btc_15m_change_pct":0.0329},"P25SET-PONSUSDT-1988319":{"time_kst":"2026-09-16 00:50:07","p_v2_score":69.86,"ema9_ema20_gap_pct":0.2624,"rebound_from_low_pct":6.74,"rsi_delta":2.616,"btc_15m_change_pct":0.0872},"P25SET-ARCUSDT-1988320":{"time_kst":"2026-09-16 01:00:07","p_v2_score":69.31,"ema9_ema20_gap_pct":1.0649,"rebound_from_low_pct":3.87,"rsi_delta":1.419,"btc_15m_change_pct":0.0494},"P25SET-PONSUSDT-1988322":{"time_kst":"2026-09-16 01:35:07","p_v2_score":70.76,"ema9_ema20_gap_pct":0.3844,"rebound_from_low_pct":8.43,"rsi_delta":9.692,"btc_15m_change_pct":-0.1605},"P25SET-POWERUSDT-1988323":{"time_kst":"2026-09-16 01:45:07","p_v2_score":95.89,"ema9_ema20_gap_pct":1.5128,"rebound_from_low_pct":13.12,"rsi_delta":1.257,"btc_15m_change_pct":-0.017},"P25SET-TUTUSDT-1988323":{"time_kst":"2026-09-16 01:49:08","p_v2_score":75.64,"ema9_ema20_gap_pct":2.5204,"rebound_from_low_pct":10.32,"rsi_delta":2.439,"btc_15m_change_pct":0.0296},"P25SET-RKLBUSDT-1988324":{"time_kst":"2026-09-16 02:00:08","p_v2_score":80.51,"ema9_ema20_gap_pct":0.2481,"rebound_from_low_pct":3.88,"rsi_delta":4.066,"btc_15m_change_pct":-0.0212},"P25SET-ARBUSDT-1988325":{"time_kst":"2026-09-16 02:19:08","p_v2_score":85.81,"ema9_ema20_gap_pct":2.6816,"rebound_from_low_pct":11.73,"rsi_delta":2.801,"btc_15m_change_pct":-0.0594},"P25SET-HEMIUSDT-1988373":{"time_kst":"2026-09-16 14:15:17","p_v2_score":87.65,"ema9_ema20_gap_pct":1.4028,"rebound_from_low_pct":7.29,"rsi_delta":7.269,"btc_15m_change_pct":0.096},"P25SET-LRCUSDT-1988374":{"time_kst":"2026-09-16 14:30:18","p_v2_score":75.91,"ema9_ema20_gap_pct":0.5743,"rebound_from_low_pct":3.73,"rsi_delta":3.136,"btc_15m_change_pct":0.0134},"P25SET-HEMIUSDT-1988376":{"time_kst":"2026-09-16 15:00:18","p_v2_score":91.59,"ema9_ema20_gap_pct":1.5025,"rebound_from_low_pct":7.48,"rsi_delta":0.179,"btc_15m_change_pct":0.0755},"P25SET-4USDT-1988376":{"time_kst":"2026-09-16 15:10:19","p_v2_score":73.57,"ema9_ema20_gap_pct":-0.1762,"rebound_from_low_pct":4.72,"rsi_delta":3.868,"btc_15m_change_pct":0.0593},"P25SET-USELESSUSDT-1988379":{"time_kst":"2026-09-16 15:45:18","p_v2_score":84.3,"ema9_ema20_gap_pct":0.9858,"rebound_from_low_pct":5.2,"rsi_delta":1.877,"btc_15m_change_pct":0.0084},"P25SET-AKEUSDT-1988380":{"time_kst":"2026-09-16 16:00:18","p_v2_score":68.76,"ema9_ema20_gap_pct":0.5374,"rebound_from_low_pct":13.08,"rsi_delta":3.83,"btc_15m_change_pct":0.0626},"P25SET-ZECUSDT-1988380":{"time_kst":"2026-09-16 16:00:19","p_v2_score":81.67,"ema9_ema20_gap_pct":1.3047,"rebound_from_low_pct":6.16,"rsi_delta":2.43,"btc_15m_change_pct":0.0626},"P25SET-HUSDT-1988381":{"time_kst":"2026-09-16 16:15:19","p_v2_score":76.57,"ema9_ema20_gap_pct":0.6842,"rebound_from_low_pct":3.7,"rsi_delta":3.221,"btc_15m_change_pct":-0.2409},"P25SET-USELESSUSDT-1988382":{"time_kst":"2026-09-16 16:30:19","p_v2_score":82.77,"ema9_ema20_gap_pct":1.7455,"rebound_from_low_pct":8.06,"rsi_delta":-0.82,"btc_15m_change_pct":0.0053},"P25SET-CROSSUSDT-1988382":{"time_kst":"2026-09-16 16:30:19","p_v2_score":74.42,"ema9_ema20_gap_pct":1.2697,"rebound_from_low_pct":19.52,"rsi_delta":3.299,"btc_15m_change_pct":0.0053},"P25SET-CROSSUSDT-1988385":{"time_kst":"2026-09-16 17:15:19","p_v2_score":93.9,"ema9_ema20_gap_pct":1.7878,"rebound_from_low_pct":14.48,"rsi_delta":1.935,"btc_15m_change_pct":-0.194},"P25SET-BTRUSDT-1988385":{"time_kst":"2026-09-16 17:15:19","p_v2_score":87.18,"ema9_ema20_gap_pct":1.3681,"rebound_from_low_pct":7.03,"rsi_delta":8.621,"btc_15m_change_pct":-0.194},"P25SET-BTWUSDT-1988385":{"time_kst":"2026-09-16 17:15:20","p_v2_score":78.74,"ema9_ema20_gap_pct":1.7949,"rebound_from_low_pct":10.15,"rsi_delta":0.903,"btc_15m_change_pct":-0.194},"P25SET-USELESSUSDT-1988387":{"time_kst":"2026-09-16 17:45:20","p_v2_score":72.91,"ema9_ema20_gap_pct":1.9615,"rebound_from_low_pct":9.82,"rsi_delta":3.18,"btc_15m_change_pct":0.0692},"P25SET-SKYAI1USDT-1988416":{"time_kst":"2026-09-17 01:00:25","p_v2_score":95.33,"ema9_ema20_gap_pct":1.6181,"rebound_from_low_pct":9.08,"rsi_delta":2.886,"btc_15m_change_pct":-0.0276},"P25SET-POWRUSDT-1988416":{"time_kst":"2026-09-17 01:00:26","p_v2_score":81.77,"ema9_ema20_gap_pct":0.7284,"rebound_from_low_pct":4.04,"rsi_delta":0.73,"btc_15m_change_pct":-0.0276},"P25SET-ZECUSDT-1988418":{"time_kst":"2026-09-17 01:30:26","p_v2_score":83.05,"ema9_ema20_gap_pct":1.0308,"rebound_from_low_pct":5.24,"rsi_delta":5.04,"btc_15m_change_pct":-0.0718},"P25SET-HEIUSDT-1988419":{"time_kst":"2026-09-17 01:50:26","p_v2_score":79.46,"ema9_ema20_gap_pct":3.4884,"rebound_from_low_pct":15.2,"rsi_delta":-0.357,"btc_15m_change_pct":0.0217},"P25SET-SKYAI1USDT-1988419":{"time_kst":"2026-09-17 01:57:25","p_v2_score":84.64,"ema9_ema20_gap_pct":1.822,"rebound_from_low_pct":9.85,"rsi_delta":1.828,"btc_15m_change_pct":0.1286},"P25SET-TEMUSDT-1988420":{"time_kst":"2026-09-17 02:00:28","p_v2_score":79.22,"ema9_ema20_gap_pct":0.5256,"rebound_from_low_pct":3.91,"rsi_delta":10.028,"btc_15m_change_pct":-0.0033},"P25SET-SKYAI1USDT-1988422":{"time_kst":"2026-09-17 02:42:25","p_v2_score":76.46,"ema9_ema20_gap_pct":1.7976,"rebound_from_low_pct":7.16,"rsi_delta":0.492,"btc_15m_change_pct":-0.2944},"P25SET-DGAIUSDT-1988426":{"time_kst":"2026-09-17 03:30:22","p_v2_score":76.65,"ema9_ema20_gap_pct":1.6687,"rebound_from_low_pct":4.36,"rsi_delta":1.533,"btc_15m_change_pct":0.022},"P25SET-NEARUSDT-1988426":{"time_kst":"2026-09-17 03:30:24","p_v2_score":89.1,"ema9_ema20_gap_pct":0.6239,"rebound_from_low_pct":4.67,"rsi_delta":3.551,"btc_15m_change_pct":0.022},"P25SET-ENAUSDT-1988426":{"time_kst":"2026-09-17 03:30:24","p_v2_score":86.21,"ema9_ema20_gap_pct":0.391,"rebound_from_low_pct":5.01,"rsi_delta":5.73,"btc_15m_change_pct":0.022},"P25SET-HEIUSDT-1988434":{"time_kst":"2026-09-17 05:30:47","p_v2_score":90.26,"ema9_ema20_gap_pct":0.855,"rebound_from_low_pct":9.4,"rsi_delta":4.602,"btc_15m_change_pct":-0.1469},"P25SET-ARBUSDT-1988434":{"time_kst":"2026-09-17 05:30:48","p_v2_score":70.08,"ema9_ema20_gap_pct":0.3298,"rebound_from_low_pct":5.79,"rsi_delta":3.591,"btc_15m_change_pct":-0.1469},"P25SET-LITUSDT-1988434":{"time_kst":"2026-09-17 05:30:48","p_v2_score":84.21,"ema9_ema20_gap_pct":1.318,"rebound_from_low_pct":6.97,"rsi_delta":6.005,"btc_15m_change_pct":-0.1469},"P25SET-ZECUSDT-1988435":{"time_kst":"2026-09-17 05:45:48","p_v2_score":88.36,"ema9_ema20_gap_pct":1.0857,"rebound_from_low_pct":7.22,"rsi_delta":2.753,"btc_15m_change_pct":-0.1814},"P25SET-HEIUSDT-1988437":{"time_kst":"2026-09-17 06:15:48","p_v2_score":86.6,"ema9_ema20_gap_pct":1.5542,"rebound_from_low_pct":11.53,"rsi_delta":1.07,"btc_15m_change_pct":-0.0445},"P25SET-ARBUSDT-1988437":{"time_kst":"2026-09-17 06:15:48","p_v2_score":90.33,"ema9_ema20_gap_pct":0.6901,"rebound_from_low_pct":7.32,"rsi_delta":3.614,"btc_15m_change_pct":-0.0445},"P25SET-LITUSDT-1988437":{"time_kst":"2026-09-17 06:15:48","p_v2_score":73.2,"ema9_ema20_gap_pct":1.4092,"rebound_from_low_pct":6.35,"rsi_delta":-2.79,"btc_15m_change_pct":-0.0445},"P25SET-NEARUSDT-1988437":{"time_kst":"2026-09-17 06:15:49","p_v2_score":82.75,"ema9_ema20_gap_pct":1.1715,"rebound_from_low_pct":6.11,"rsi_delta":1.158,"btc_15m_change_pct":-0.0445},"P25SET-NBISUSDT-1988438":{"time_kst":"2026-09-17 06:30:51","p_v2_score":80.68,"ema9_ema20_gap_pct":-0.0491,"rebound_from_low_pct":4.34,"rsi_delta":7.2,"btc_15m_change_pct":-0.0351},"P25SET-DASHUSDT-1988440":{"time_kst":"2026-09-17 07:00:49","p_v2_score":84.1,"ema9_ema20_gap_pct":0.6859,"rebound_from_low_pct":6.94,"rsi_delta":4.893,"btc_15m_change_pct":-0.042},"P25SET-HEIUSDT-1988440":{"time_kst":"2026-09-17 07:00:49","p_v2_score":69.02,"ema9_ema20_gap_pct":1.7955,"rebound_from_low_pct":12.25,"rsi_delta":-2.148,"btc_15m_change_pct":-0.042},"P25SET-ARXUSDT-1988441":{"time_kst":"2026-09-17 07:15:50","p_v2_score":80.3,"ema9_ema20_gap_pct":0.8044,"rebound_from_low_pct":5.09,"rsi_delta":0.603,"btc_15m_change_pct":0.2029},"P25SET-NBISUSDT-1988441":{"time_kst":"2026-09-17 07:17:50","p_v2_score":91.73,"ema9_ema20_gap_pct":0.6271,"rebound_from_low_pct":6.35,"rsi_delta":5.914,"btc_15m_change_pct":0.1709}}

OUT_SUM = ROOT / "V22_QUALITY_RESCHEDULE_SUMMARY.csv"
OUT_DAY = ROOT / "V22_QUALITY_RESCHEDULE_DAILY.csv"
OUT_INC = ROOT / "V22_QUALITY_RESCHEDULE_INCREMENTAL.csv"
OUT_TRADES = ROOT / "V22_QUALITY_RESCHEDULE_TRADES.csv"
OUT_TXT = ROOT / "V22_QUALITY_RESCHEDULE_SUMMARY.txt"
OUT_ZIP = ROOT / "V22_QUALITY_RESCHEDULE_RESULTS.zip"

SCRIPT_VERSION = "V22_QUALITY_RESCHEDULE_v1_20260924"

def load_module(name,path):
    spec=importlib.util.spec_from_file_location(name,str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    m=importlib.util.module_from_spec(spec)
    sys.modules[name]=m
    spec.loader.exec_module(m)
    return m

U=load_module("U_V22_QUALITY",U_PATH)

def fv(v,default=None):
    try:
        if v is None or str(v).strip()=="":
            return default
        x=float(v)
        if math.isnan(x): return default
        return x
    except Exception:
        return default

def dt(v):
    if v in (None,""): return None
    try:
        d=RealDateTime.fromisoformat(str(v).replace("Z","+00:00"))
        if d.tzinfo is None: d=d.replace(tzinfo=UTC)
        return d.astimezone(UTC)
    except Exception:
        return None

def kst(d):
    return d.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S") if d else ""

def write_csv(path,rows):
    if not rows:
        path.write_text("",encoding="utf-8-sig"); return
    keys=[]; seen=set()
    for r in rows:
        for k in r:
            if k not in seen:
                seen.add(k); keys.append(k)
    with path.open("w",newline="",encoding="utf-8-sig") as f:
        w=csv.DictWriter(f,fieldnames=keys,extrasaction="ignore")
        w.writeheader(); w.writerows(rows)

def configure_range(start_kst,end_kst,cache_name):
    U.START_KST=start_kst
    U.END_KST=end_kst
    U.START_UTC=start_kst.astimezone(UTC)
    U.END_UTC=end_kst.astimezone(UTC)
    U.EXPECTED_V25=-1
    U.CACHE_DIR=ROOT/cache_name
    U.CACHE_DIR.mkdir(parents=True,exist_ok=True)
    U.KC=U.KlineCache()
    U._market_1m={}
    U._market_recompute_count=0
    U.load_market_series()

def load_exact_features():
    out={}
    if AUDIT_PATH.exists():
        df=pd.read_csv(AUDIT_PATH)
        for r in df.to_dict("records"):
            if str(r.get("feature_source") or "") != "WATCH_SCAN":
                continue
            sid=str(r.get("setup_id") or "")
            if not sid: continue
            out[sid]={
                "source":"AUDIT_WATCH",
                "p_v2_score":fv(r.get("p_v2_score")),
                "ema9_ema20_gap_pct":fv(r.get("ema9_ema20_gap_pct")),
                "rebound_from_low_pct":fv(r.get("rebound_from_low_pct")),
                "rsi_delta":fv(r.get("rsi_delta")),
                "btc_15m_change_pct":fv(r.get("btc_15m_change_pct")),
            }
    # Library-recovered exact first WATCH rows override / supplement.
    for sid,r in RECOVERED_PRE.items():
        out[sid]={
            "source":"RECOVERED_LIBRARY_WATCH",
            "p_v2_score":fv(r.get("p_v2_score")),
            "ema9_ema20_gap_pct":fv(r.get("ema9_ema20_gap_pct")),
            "rebound_from_low_pct":fv(r.get("rebound_from_low_pct")),
            "rsi_delta":fv(r.get("rsi_delta")),
            "btc_15m_change_pct":fv(r.get("btc_15m_change_pct")),
        }
    return out

def quality_flags(feat):
    if not feat:
        return False,False,False
    score=fv(feat.get("p_v2_score"))
    gap=fv(feat.get("ema9_ema20_gap_pct"))
    reb=fv(feat.get("rebound_from_low_pct"))
    rd=fv(feat.get("rsi_delta"))
    b15=fv(feat.get("btc_15m_change_pct"))

    over=bool(score is not None and gap is not None and score>=90.0 and gap>=1.20)
    weak=bool(
        reb is not None and rd is not None and b15 is not None
        and reb<=5.0 and rd<=7.0 and b15>=-0.08
    )
    return over,weak,(over or weak)

def build_v25_guard_meta(setups):
    q=deque()
    out={}
    for st in setups:
        t=st["entry"]
        while q and q[0] < t-timedelta(hours=2):
            q.popleft()
        q.append(t)
        d=st["details"]
        U.fill_missing_market(d,t)
        b4=U.first_f(d,"btc_4h_change_pct","btc_4h")
        e4=U.first_f(d,"eth_4h_change_pct","eth_4h")
        avg=None if b4 is None or e4 is None else (abs(b4)+abs(e4))/2.0
        out[st["setup_id"]]=bool(len(q)>=8 and avg is not None and avg>=0.40)
    return out

def load_cache(path_name,eval_start=None):
    p=ROOT/path_name
    out={}
    if not p.exists(): return out
    df=pd.read_csv(p)
    for r in df.to_dict("records"):
        if int(fv(r.get("accepted"),0) or 0)!=1: continue
        if eval_start is not None:
            t=str(r.get("entry_time_kst") or "")
            if t and t < eval_start.strftime("%Y-%m-%d %H:%M:%S"):
                continue
        et=dt(r.get("exit_ts_utc"))
        if et is None: continue
        ep=fv(r.get("entry_price"),0) or 0
        terminal=fv(r.get("terminal_price"),ep) or ep
        derr=str(r.get("data_error") or "")
        if derr.lower()=="nan": derr=""
        out[str(r["setup_id"])]=U.SimResult(
            result=str(r.get("result") or ""),
            exit_time=et, terminal_price=terminal, fills=[],
            gross_pct=fv(r.get("gross_pct"),0) or 0,
            fee_pct=fv(r.get("fee_pct"),0) or 0,
            net_pct=fv(r.get("net_pct"),0) or 0,
            mfe_pct=fv(r.get("mfe_pct"),0) or 0,
            mae_pct=fv(r.get("mae_pct"),0) or 0,
            stop_stage=str(r.get("stop_stage") or ""),
            detail="CACHED", data_error=derr,
        )
    return out

def getsim(st,cache):
    sid=st["setup_id"]
    if sid not in cache:
        cache[sid]=U.simulate_base(st)
    return cache[sid]

def warm_base(setups,controls,until_utc,cache):
    sched=U.Scheduler()
    for st in setups:
        if st["entry"]>=until_utc: break
        ef=U.entry_filter(st,controls)
        if not ef["pass"]: continue
        ok,_=sched.can_open(st["entry"],st["symbol"])
        if not ok: continue
        sim=getsim(st,cache)
        sched.add(st["entry"],st["symbol"],sim,sim.result in ("STOP","LATE_FAILURE_EXIT"))
    return sched

def scenario_block(name,feat):
    over,weak,either=quality_flags(feat)
    if name=="OVEREXT": return over
    if name=="WEAK_REACCEL": return weak
    if name=="V22_QUALITY_OR": return either
    return False

def run(name,setups,controls,v25guard,features,cache,start_utc=None,end_utc=None,warm=None):
    sched=copy.deepcopy(warm) if warm is not None else U.Scheduler()
    rows=[]
    for st in setups:
        if start_utc and st["entry"]<start_utc: continue
        if end_utc and st["entry"]>end_utc: continue
        sid=st["setup_id"]
        ef=U.entry_filter(st,controls)
        feat=features.get(sid)
        over,weak,either=quality_flags(feat)

        row={
            "scenario":name,"setup_id":sid,"symbol":st["symbol"],
            "entry_time_kst":kst(st["entry"]),
            "exact_v22_feature":int(bool(feat)),
            "feature_source":"" if not feat else feat.get("source",""),
            "overext":int(over),"weak_reaccel":int(weak),
            "accepted":0,"block_reason":ef["reason"],
            "result":"","net_pct":"","exit_time_kst":"","data_error":"",
        }

        if not ef["pass"]:
            rows.append(row); continue

        # Current V25 market guard is frozen ON in every scenario.
        if bool(v25guard.get(sid)):
            row["block_reason"]="CUR_V25_GUARD"
            rows.append(row); continue

        # Missing exact V22 candidate-time data => never block.
        if feat and scenario_block(name,feat):
            row["block_reason"]=name
            rows.append(row); continue

        ok,why=sched.can_open(st["entry"],st["symbol"])
        if not ok:
            row["block_reason"]=why
            rows.append(row); continue

        sim=getsim(st,cache)
        row.update({
            "accepted":1,"block_reason":"","result":sim.result,
            "net_pct":round(sim.net_pct,6),"exit_time_kst":kst(sim.exit_time),
            "data_error":sim.data_error,
        })
        sched.add(st["entry"],st["symbol"],sim,sim.result in ("STOP","LATE_FAILURE_EXIT"))
        rows.append(row)
    return rows

def acc(rows,lo=None,hi=None):
    z=[]
    for r in rows:
        if int(r.get("accepted") or 0)!=1: continue
        d=str(r["entry_time_kst"])[:10]
        if lo and d<lo: continue
        if hi and d>hi: continue
        z.append(r)
    return z

def stats(dataset,period,name,rows,cur_rows,lo=None,hi=None):
    a=acc(rows,lo,hi); b=acc(cur_rows,lo,hi)
    an=sum(float(r["net_pct"]) for r in a); bn=sum(float(r["net_pct"]) for r in b)
    aids={r["setup_id"] for r in a}; bids={r["setup_id"] for r in b}
    amap={r["setup_id"]:r for r in a}; bmap={r["setup_id"]:r for r in b}
    new=[amap[x] for x in aids-bids]
    rem=[bmap[x] for x in bids-aids]
    known=sum(int(r.get("exact_v22_feature") or 0) for r in b)
    return {
        "dataset":dataset,"period":period,"scenario":name,
        "entries":len(a),"net_pct":round(an,6),
        "cur_entries":len(b),"cur_net":round(bn,6),
        "delta_vs_cur":round(an-bn,6),
        "cur_exact_feature_entries":known,
        "cur_exact_feature_coverage_pct":round(100*known/len(b),3) if b else 0,
        "tp":sum(r["result"]=="TP20_FULL" for r in a),
        "stop":sum(r["result"]=="STOP" for r in a),
        "pp12":sum(r["result"]=="PROFIT_PROTECT_EXIT" for r in a),
        "late":sum(r["result"]=="LATE_FAILURE_EXIT" for r in a),
        "new_entries":len(new),"new_net":round(sum(float(r["net_pct"]) for r in new),6),
        "removed_cur":len(rem),"removed_cur_net":round(sum(float(r["net_pct"]) for r in rem),6),
    }

def daily(dataset,name,rows,cur_rows):
    dates=sorted(set(str(r["entry_time_kst"])[:10] for r in rows+cur_rows))
    out=[]
    for d in dates:
        a=acc(rows,d,d); b=acc(cur_rows,d,d)
        an=sum(float(r["net_pct"]) for r in a); bn=sum(float(r["net_pct"]) for r in b)
        out.append({
            "dataset":dataset,"date":d,"scenario":name,
            "entries":len(a),"net_pct":round(an,6),
            "cur_entries":len(b),"cur_net":round(bn,6),
            "delta_vs_cur":round(an-bn,6),
        })
    return out

def incremental(dataset,name,rows,cur_rows):
    a=acc(rows); b=acc(cur_rows)
    aids={r["setup_id"] for r in a}; bids={r["setup_id"] for r in b}
    amap={r["setup_id"]:r for r in a}; bmap={r["setup_id"]:r for r in b}
    new=[amap[x] for x in aids-bids]
    rem=[bmap[x] for x in bids-aids]
    direct_ids={str(r["setup_id"]) for r in rows if str(r.get("block_reason"))==name}
    direct=[bmap[x] for x in (bids-aids) if x in direct_ids]
    displaced=[bmap[x] for x in (bids-aids) if x not in direct_ids]
    return {
        "dataset":dataset,"scenario":name,
        "cur_net":round(sum(float(r["net_pct"]) for r in b),6),
        "scenario_net":round(sum(float(r["net_pct"]) for r in a),6),
        "delta_vs_cur":round(sum(float(r["net_pct"]) for r in a)-sum(float(r["net_pct"]) for r in b),6),
        "direct_removed":len(direct),"direct_removed_net":round(sum(float(r["net_pct"]) for r in direct),6),
        "displaced_cur":len(displaced),"displaced_cur_net":round(sum(float(r["net_pct"]) for r in displaced),6),
        "new_after_block":len(new),"new_after_block_net":round(sum(float(r["net_pct"]) for r in new),6),
    }

features=load_exact_features()
all_sum=[]; all_day=[]; all_inc=[]; all_trades=[]

# HIST
print("=== HIST ===",flush=True)
configure_range(HIST_START,HIST_END,".v22_quality_hist_cache")
hs=U.load_setups(); hc=U.load_control_proxy()
hv25=build_v25_guard_meta(hs)
hcache=load_cache("UNIFIED_CURRENT_0901_0922_TRADES.csv")
hruns={}
for name in SCENARIOS:
    print("HIST",name,flush=True)
    hruns[name]=run(name,hs,hc,hv25,features,hcache)
    for r in hruns[name]:
        rr=dict(r); rr["dataset"]="HIST"; all_trades.append(rr)
hcur=hruns["CUR_V25_ONLY"]
for name in SCENARIOS:
    all_sum.append(stats("HIST","PRE_0901_0917",name,hruns[name],hcur,"2026-09-01","2026-09-17"))
    all_sum.append(stats("HIST","POST_0918_0922",name,hruns[name],hcur,"2026-09-18","2026-09-22"))
    all_sum.append(stats("HIST","ALL_0901_0922",name,hruns[name],hcur))
    if name!="CUR_V25_ONLY":
        all_day += daily("HIST",name,hruns[name],hcur)
        all_inc.append(incremental("HIST_ALL",name,hruns[name],hcur))

# FRESH
print("=== FRESH ===",flush=True)
configure_range(FRESH_WARM,FRESH_END+timedelta(seconds=1),".v22_quality_fresh_cache")
fs=U.load_setups(); fc=U.load_control_proxy()
fv25=build_v25_guard_meta(fs)
fcache=load_cache("FRESH_BASE_TRADES.csv",FRESH_START)
warm=warm_base(fs,fc,FRESH_START.astimezone(UTC),fcache)
fruns={}
for name in SCENARIOS:
    print("FRESH",name,flush=True)
    fruns[name]=run(name,fs,fc,fv25,features,fcache,FRESH_START.astimezone(UTC),FRESH_END.astimezone(UTC),warm)
    for r in fruns[name]:
        rr=dict(r); rr["dataset"]="FRESH"; all_trades.append(rr)
fcur=fruns["CUR_V25_ONLY"]
for name in SCENARIOS:
    all_sum.append(stats("FRESH","ALL",name,fruns[name],fcur))
    if name!="CUR_V25_ONLY":
        all_day += daily("FRESH",name,fruns[name],fcur)
        all_inc.append(incremental("FRESH",name,fruns[name],fcur))

write_csv(OUT_SUM,all_sum)
write_csv(OUT_DAY,all_day)
write_csv(OUT_INC,all_inc)
write_csv(OUT_TRADES,all_trades)

def pick(dataset,period,scenario):
    return next(r for r in all_sum if r["dataset"]==dataset and r["period"]==period and r["scenario"]==scenario)

lines=[
    "V22 QUALITY FILTER — EXACT RESCHEDULE",
    f"script={SCRIPT_VERSION}",
    "",
    "[RULES]",
    "OVEREXT = p_v2_score>=90 AND ema9_ema20_gap_pct>=1.20",
    "WEAK_REACCEL = rebound_from_low<=5 AND rsi_delta<=7 AND BTC15>=-0.08",
    "V22_QUALITY_OR = either",
    "Current V25 market guard remains ON.",
    "Missing exact candidate-time telemetry is never blocked.",
    "",
    "[RESULTS]",
]
for r in all_sum:
    lines.append(
        f"{r['dataset']} {r['period']} {r['scenario']}: "
        f"entries={r['entries']} NET={r['net_pct']:.6f} delta_vs_CUR={r['delta_vs_cur']:+.6f} "
        f"coverage={r['cur_exact_feature_entries']}/{r['cur_entries']} ({r['cur_exact_feature_coverage_pct']}%) "
        f"new={r['new_entries']}({r['new_net']:+.6f}) removed={r['removed_cur']}({r['removed_cur_net']:+.6f})"
    )

lines += ["","[ROBUSTNESS]"]
for name in ["OVEREXT","WEAK_REACCEL","V22_QUALITY_OR"]:
    pre=pick("HIST","PRE_0901_0917",name)
    post=pick("HIST","POST_0918_0922",name)
    fresh=pick("FRESH","ALL",name)
    vals=(pre["delta_vs_cur"],post["delta_vs_cur"],fresh["delta_vs_cur"])
    lines.append(
        f"{name}: PRE={vals[0]:+.6f}, POST={vals[1]:+.6f}, FRESH={vals[2]:+.6f}, "
        f"all_positive={all(v>0 for v in vals)}"
    )

OUT_TXT.write_text("\n".join(lines)+"\n",encoding="utf-8")
print("\n".join(lines),flush=True)

with zipfile.ZipFile(OUT_ZIP,"w",zipfile.ZIP_DEFLATED) as z:
    for p in [OUT_SUM,OUT_DAY,OUT_INC,OUT_TRADES,OUT_TXT]:
        z.write(p,arcname=p.name)
print("DONE:",OUT_ZIP,flush=True)
