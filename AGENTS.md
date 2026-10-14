# nineouting-stt 작업 규칙

## 구조
- prototype/: 회의 실시간 전사 웹 프로토타입 (app/engine.py, app/summary_sender.py, app/server.py, app/static/index.html)
- 실행: cd prototype && .venv/bin/python app/server.py --device <번호>  → http://localhost:8765
  (마이크 입력은 sounddevice. 장치 번호는 --list-devices로 확인. ffmpeg 입력 때의 번호와 다름)
- 설치: python3 -m venv .venv && .venv/bin/pip install --only-binary=:all: -r requirements.txt

## 프로토타입의 범위
- prototype/의 화면(index.html)과 웹 서버(server.py의 http.server)는 기능을 시험·확인하는 임시용임. 정식 화면은 앱 쪽에서 따로 만듦
- 정식 서비스로 가져갈 부분에 집중함: 인식 엔진(engine.py), 요약 쪽 전송 형식(summary_sender.py), 세션·녹음·수정·삭제 규칙
- 임시 화면은 시험에 꼭 필요한 만큼만 고치고 다듬는 데 시간을 쓰지 않음 (상태창, 보기 설정, 위치 보정 등은 확인용)
- STT 서버는 백엔드와 따로 구성될 예정. 정식 서버로 옮길 때 엔진과 전송 부분을 그대로 가져감

## 올리면 안 되는 것
- 모델 파일(*.onnx, *.pt, models/), 회의 전사문·녹음(transcripts/, *.wav, *.m4a, *.jsonl)
- AI Hub 데이터는 국외 반출 제한: 레포·외부 서비스에 올리지 않음
- 커밋 전에 git status로 위 파일이 섞이지 않았는지 확인

## 작업 방식
- 셸 명령·패키지 설치·다운로드는 실행 전에 내용과 목적을 알리고 확인받음
- 기능 확인은 wav 입력(--wav, 실제 속도)으로 함. 마이크 확인은 사람이 직접 함
- 녹음 파일로 정확도를 볼 때는 스피커로 틀지 말고 --wav로 넣음 (스피커→마이크를 거치면 확신도 낮은 글자가 2~3배, 2026-10-14 측정)
- 사용자가 실행 중인 서버(8765)는 끄거나 요청을 보내지 않음. 시험 서버는 다른 포트로 띄우고, 띄운 프로세스만 종료
- 서버 코드(engine.py, server.py)를 바꾸면 서버를 다시 켜야 적용됨을 알림. 화면(index.html)만 바꾸면 새로고침으로 적용

## 커밋
- 커밋 메시지는 한국어, 공동 작성자(Co-Authored-By) 줄을 넣지 않음
- push는 내용을 보여주고 확인받은 뒤에 함
- PR은 초안을 먼저 보여주고 검토받은 뒤 올림. 본문에 생성 표시(Generated with ...)를 넣지 않음
- 작업은 develop에서 기능 브랜치를 따서 하고, main에는 합치지 않음

## 평가·설정 근거
- STT 설정과 평가 결과는 별도 평가 저장소(nineouting-ref)의 문서를 따름: chunk-64, 후보 4, VAD 보완 기다림 2.0초,
  줄 최대 10초 + 강제 분할 때 1초 다시 넣기 (nineouting-ref review/endpoint_check.py·endpoint_cut_check.py)
- 성능 수치를 적을 때는 측정값인지 추정값인지 구분함
