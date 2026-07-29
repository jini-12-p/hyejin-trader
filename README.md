# HJ Trader MVP

혜진 전용 Bybit 15분봉 BUY 스캐너의 첫 검증판입니다.

## 현재 기능

- 비밀번호 로그인
- Bybit USDT 무기한 종목 검색
- 감시종목 선택
- BUY-P / BUY-R 스캔
- BUY, MAX 진입가
- ENTRY OK / WAIT / PASS
- 현재 5점 HOLD 점수
- Bybit 차트 바로가기
- 주문 권한/API Key 없음

## GitHub에 올리는 파일

ZIP을 푼 뒤 폴더 안의 파일을 전부 업로드하세요.

- `app.py`
- `bybit.py`
- `strategy.py`
- `requirements.txt`
- `.gitignore`
- `.streamlit/config.toml`
- `secrets.example.toml`
- `README.md`

## Streamlit 배포

1. Streamlit Community Cloud에 GitHub로 로그인
2. `Create app` 선택
3. 비공개 저장소 `hyejin-trader` 선택
4. Main file path는 `app.py`
5. App settings → Secrets에 아래 형식으로 입력

```toml
APP_PASSWORD = "본인만 아는 긴 비밀번호"
```

6. Deploy
7. 생성된 주소를 휴대폰에서 열고 로그인

> 실제 비밀번호는 GitHub 파일에 적지 마세요.

## 중요한 검증 사항

이 버전은 Pine Script v7.4의 로직을 Python으로 옮긴 첫 검증판입니다.
TradingView와 데이터 공급원, VWAP 세션, 아직 마감되지 않은 현재 봉 때문에
신호 시점이 다를 수 있습니다. 실매매 판단 전에 동일 종목·동일 15분봉에서
BUY-P / BUY-R, RSI, EMA, 거래량 조건을 반드시 비교하세요.

## 다음 버전

- 진입 버튼
- 실제 진입가 저장
- TP / STOP
- 6봉 HOLD/STOP
- 외부 데이터베이스 저장
- 모바일 알림
