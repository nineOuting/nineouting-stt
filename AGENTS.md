# nineouting-stt 작업 규칙

## 구조
- prototype/: 회의 실시간 전사 웹 프로토타입 (app/engine.py, app/server.py, app/static/index.html)
- 실행: cd prototype && .venv/bin/python app/server.py --device <번호>  → http://localhost:8765
- 설치: python3 -m venv .venv && .venv/bin/pip install --only-binary=:all: -r requirements.txt

## 올리면 안 되는 것
- 모델 파일(*.onnx, *.pt, models/), 회의 전사문·녹음(transcripts/, *.wav, *.m4a, *.jsonl)
- AI Hub 데이터는 국외 반출 제한: 레포·외부 서비스에 올리지 않음
- 커밋 전에 git status로 위 파일이 섞이지 않았는지 확인

## 작업 방식
- 셸 명령·패키지 설치·다운로드는 실행 전에 내용과 목적을 알리고 확인받음
- 기능 확인은 wav 입력(--wav, 실제 속도)으로 함. 마이크 확인은 사람이 직접 함
- 사용자가 실행 중인 서버(8765)는 끄거나 요청을 보내지 않음. 시험 서버는 다른 포트로 띄우고, 띄운 프로세스만 종료
- 서버 코드(engine.py, server.py)를 바꾸면 서버를 다시 켜야 적용됨을 알림. 화면(index.html)만 바꾸면 새로고침으로 적용

## 커밋
- 커밋 메시지는 한국어, 공동 작성자(Co-Authored-By) 줄을 넣지 않음
- push는 내용을 보여주고 확인받은 뒤에 함

## 평가·설정 근거
- STT 설정과 평가 결과는 별도 평가 저장소(nineouting-ref)의 문서를 따름: chunk-64, 후보 4, VAD 보완 기다림 2.0초
- 성능 수치를 적을 때는 측정값인지 추정값인지 구분함
