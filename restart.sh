#!/usr/bin/env bash
set -e
cd /root/hyejin-trader
git pull
pkill -f run_bybit_swing_bot.py || true
nohup ./venv/bin/python run_bybit_swing_bot.py >/tmp/bybit_swing.log 2>&1 &
sleep 3

if pgrep -f "run_bybit_swing_bot.py" >/dev/null; then
  echo "EMERGENCY-v4.2.4-runtime-fix RUNNING"
else
  echo "EMERGENCY-v4.2.4-runtime-fix FAILED"
  tail -n 30 /tmp/bybit_swing.log || true
  exit 1
fi
