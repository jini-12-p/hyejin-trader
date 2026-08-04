#!/usr/bin/env bash
set -e
cd /root/hyejin-trader
git pull
pkill -f run_bybit_swing_bot.py || true
nohup ./venv/bin/python run_bybit_swing_bot.py >/tmp/bybit_swing.log 2>&1 &
pkill -f "streamlit run app.py" || true
nohup ./venv/bin/streamlit run app.py >/tmp/streamlit.log 2>&1 &
echo "v4.2.0 restarted"
