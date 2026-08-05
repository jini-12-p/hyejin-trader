#!/usr/bin/env bash
set -e
cd /root/hyejin-trader
git pull
pkill -f run_bybit_swing_bot.py || true
nohup ./venv/bin/python run_bybit_swing_bot.py >/tmp/bybit_swing.log 2>&1 &
sleep 2
echo "EMERGENCY-v4.2.3-entry-debug restarted"
