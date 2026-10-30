# nineouting-stt

회의 음성을 실시간으로 받아 적는 STT 서버 (한국어 스트리밍 Zipformer, sherpa-onnx).

**처음 실행한다면 [prototype/README.md](prototype/README.md)의 "빠른 시작"부터** (윈도우·맥, 설치 → 모델 받기 → 실행).

| 폴더 | 내용 |
|---|---|
| [prototype/](prototype/) | STT 서버 프로토타입 (로컬 실행). 브라우저 소리 입력(WebSocket), 백엔드 전송(0.5초), 확인용 화면 |
| [prototype/docs/](prototype/docs/) | 설정 근거와 시험 기록 |

## 올리지 않는 것

`.gitignore`로 제외함.

- 모델 파일 (`models/`, `*.onnx`, `*.pt`): 용량이 커서 배포처에서 받음. `python prototype/tools/download_models.py`로 받음 (출처는 prototype/README.md)
- 회의 전사문·녹음 (`transcripts/`, `*.wav`, `*.m4a`, `*.jsonl`): 개인정보
- 가상환경 (`.venv`), 주소·포트 설정 (`prototype/.env`, 예시는 `.env.example`), 이 Mac 전용 실행 설정 (`.claude/launch.json`)
- 개인 AI 도구 설정 (`CLAUDE.md`, `CLAUDE.local.md`): 레포 공통 규칙은 [AGENTS.md](AGENTS.md)
