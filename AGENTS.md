# nineouting-stt 작업 규칙

사람과 AI 도구가 함께 따르는 레포 규칙. 개인 작업 방식은 각자 로컬 설정에 둠 (CLAUDE.md 등은 깃 제외).

## 구조
- prototype/app/: STT 서버. engine.py(인식 엔진), server.py(입력·화면 서버), backend_sender.py(백엔드 전송), summary_sender.py(요약 쪽 전송, 이전 설계), envfile.py(.env 읽기), static/index.html(개발 화면)
- prototype/tools/: download_models.py(모델 받기), mock_backend.py(가짜 백엔드), mic_sender.html(시험용 보내는 화면)
- prototype/docs/notes.md: 설정 근거와 시험 기록
- 설치·모델 받기·실행 순서는 prototype/README.md "빠른 시작" (윈도우·맥). 맥: .venv/bin/python, 윈도우: .venv\Scripts\python
- 주소·포트는 prototype/.env (.env.example 복사, 깃 제외). 명령 옵션 > 환경 변수 > .env > 기본값

## 프로토타입의 범위
- prototype/의 화면(index.html, mic_sender.html)과 웹 서버(server.py의 http.server)는 기능을 시험·확인하는 임시용임. 정식 화면은 FE에서 따로 만듦
- 정식 서비스로 가져갈 부분에 집중함: 인식 엔진(engine.py), 브라우저 입력 규약(WebSocket), 백엔드 전송 형식(backend_sender.py)
- 임시 화면은 시험에 꼭 필요한 만큼만 고치고 다듬는 데 시간을 쓰지 않음

## 올리면 안 되는 것
- 모델 파일(*.onnx, *.pt, models/), 회의 전사문·녹음(transcripts/, *.wav, *.m4a, *.jsonl), .env
- AI Hub 데이터는 국외 반출 제한: 레포·외부 서비스에 올리지 않음
- 커밋 전에 git status로 위 파일이 섞이지 않았는지 확인

## 시험
- 기능 확인은 wav 입력(--wav, 실제 속도)으로 함. 마이크 확인은 사람이 직접 함
- 녹음 파일로 정확도를 볼 때는 스피커로 틀지 말고 --wav로 넣음 (스피커→마이크를 거치면 확신도 낮은 글자가 2~3배, 2026-10-14 측정)
- 이미 실행 중인 서버는 끄거나 건드리지 않음. 시험 서버는 다른 포트로 띄우고, 띄운 프로세스만 종료
- 서버 코드(*.py)를 바꾸면 서버를 다시 켜야 적용됨. 화면(html)만 바꾸면 새로고침으로 적용

## 커밋
- 커밋 메시지는 한국어
- 작업은 develop에서 기능 브랜치를 따서 하고, main에는 합치지 않음

## 평가·설정 근거
- STT 설정과 근거 수치는 prototype/docs/notes.md: chunk-64, 후보 4, VAD 보완 기다림 2.0초, 줄 최대 10초 + 강제 분할 때 1초 다시 넣기,
  blank penalty 0.5, 핫워드 31개. 측정 스크립트와 회의 녹음은 내부 평가 저장소(비공개)에 있음
- 평가 스크립트는 jjinsword 전용 평가 도구로, 그 환경에서만 동작함 (이 레포의 engine.py를 가져다 씀). 다른 사람은 실행하지 않아도 됨
- 성능 수치를 적을 때는 측정값인지 추정값인지 구분함
