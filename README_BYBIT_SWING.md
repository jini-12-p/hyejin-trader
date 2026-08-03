# Bybit Swing v4.0.6

- 봇 시작 즉시 `bybit_swing/scan_rejected.csv` 생성
- 첫 스캔 전에도 CSV 헤더 확인 가능
- 각 스캔 결과를 append 후 즉시 flush/fsync
- 단계형 손절(STOP_HALF / RECOVERY_EXIT / FINAL_STOP) 유지

# Bybit Swing PAPER v4.0.4

## 핵심 변경
- 진입가 대비 -1.5% 도달 시 50%만 1차 손절
- 남은 50%는 -2.3%에서 최종 손절
- 1차 손절 후 가격이 진입가 대비 -0.3% 이상으로 회복하면 남은 물량 전량 회복 종료
- 1차 손절 후에는 신규 물타기 금지
- STOP_HALF를 TP1로 잘못 처리해 BE_EXIT가 즉시 발동할 수 있던 부분 방지
- PAPER 모드 유지

## 주의
이 방식은 손절 후 반등 종목을 살릴 수 있지만, 계속 하락하는 거래에서는 기존 전량 -1.5% 손절보다 총손실이 커질 수 있습니다. 반드시 PAPER 결과를 먼저 비교하세요.


## v4.0.5
- `bybit_swing/scan_rejected.csv` 자동 생성
- 매 스캔의 종목, 점수, 탈락 사유와 핵심 지표를 한국시간으로 누적 기록
- 기존 v4.0.4 단계형 손절 로직은 그대로 유지
