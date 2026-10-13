# nineouting-stt

회의 음성 인식(STT) 관련 코드.

| 폴더 | 내용 |
|---|---|
| [prototype/](prototype/) | 회의 실시간 전사 웹 프로토타입 (로컬 실행). 스트리밍 STT + VAD 보완 + 화자 번호 + 고치기 화면 |

## 올리지 않는 것

`.gitignore`로 제외함.

- 모델 파일 (`models/`, `*.onnx`, `*.pt`): 용량이 커서 배포처에서 받음. 출처와 체크섬은 prototype/README.md 참고
- 회의 전사문·녹음 (`transcripts/`, `*.wav`, `*.m4a`, `*.jsonl`): 개인정보
- 가상환경 (`.venv`), 이 Mac 전용 실행 설정 (`.claude/launch.json`)
