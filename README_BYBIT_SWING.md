# Bybit Swing v4.0.2

- 1초 TP/SL 감시 및 PAPER 트리거가 체결
- 매매기록 CSV/JSON 다운로드(KST)
- 진입 당시 RSI/EMA/거래량비/24h/최근 변동성 저장
- 텔레그램 진입·순환추가·회수·TP·손절·오류 알림

## 텔레그램 설정
`bybit_swing/config.json`의 `telegram_enabled`를 true로 변경하고 서버 환경변수 사용 권장:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

토큰/채팅 ID를 GitHub에 직접 올리지 마세요.
