# prototype — 회의 실시간 전사 STT 서버 (로컬 실행)

회의 소리를 받아 한국어 전사문을 실시간으로 만들고, 백엔드로 0.5초마다 보내는 STT 서버의 프로토타입.
정식 서비스 흐름은 아래와 같고, 이 폴더에는 STT 서버와 그것을 시험하는 화면들이 들어 있음.

```text
작성자 브라우저 → STT 서버 : 소리 (WebSocket)
STT 서버 → 백엔드         : 전사 (0.5초마다)
백엔드 → AI 서버          : 요약용 전사
백엔드 → 브라우저         : 자막
```

## 빠른 시작

파이썬 3.12 이상 (numpy 2.5가 요구, 3.13에서 확인함). 인터넷 연결 필요 (패키지와 모델 약 160MB를 받음).

### 1. 설치

윈도우 (PowerShell)

```powershell
cd nineouting-stt\prototype
py -m venv .venv
.venv\Scripts\pip install --only-binary=:all: -r requirements.txt
```

`py`가 없다고 나오면 `python -m venv .venv` (Microsoft Store에서 설치한 파이썬).

맥

```bash
cd nineouting-stt/prototype
python3 -m venv .venv
.venv/bin/pip install --only-binary=:all: -r requirements.txt
```

### 2. 모델 받기

```bash
python tools/download_models.py          # 필수 모델만 (약 160MB) → prototype/models/
python tools/download_models.py --all    # 선택 기능용 모델까지 (약 360MB)
```

- 윈도우는 `python` 대신 `.venv\Scripts\python`, 맥은 `.venv/bin/python` (아래 명령도 같음)
- 공식 배포처에서 받아 체크섬(파일 지문)을 맞춰 보고, 맞을 때만 저장함. 이미 받은 파일은 건너뜀
- 선택 모델이 없으면 서버가 해당 기능(다른 인식 결과, 화자 번호)만 끄고 켜짐
- 모델을 다른 폴더에 두려면 `.env`의 `NINEOUTING_STT_MODELS`에 적음 (받기·서버 모두 그 폴더를 씀)

### 3. 마이크로 바로 확인

```bash
python app/server.py
```

- `http://localhost:8765` 를 열고 말하면 글자가 나타남. 끄기는 Ctrl+C
- 마이크가 여러 개면 `--list-devices`로 번호를 보고 `--device N`
- 처음 실행할 때 운영체제가 마이크 권한을 물을 수 있음 (윈도우: 설정 → 개인정보 → 마이크, 맥: 시스템 설정 → 개인정보 보호 및 보안 → 마이크)

### 4. 정식 서비스 흐름으로 확인 (브라우저 → STT → 백엔드)

터미널 두 개를 씀.

```bash
python tools/mock_backend.py                                         # 터미널 1: 가짜 백엔드
python app/server.py --input ws --backend-url http://127.0.0.1:8767  # 터미널 2: STT 서버
```

`http://localhost:8765/sender` 를 열고 "시작"을 누른 뒤 말함. 아래 화면 표 참고.

### 주소·포트 바꾸기 (필요할 때만)

기본값(8765·8770·8767, 이 컴퓨터에서만 접속)으로 위 순서가 그대로 됨. 포트가 겹치거나 백엔드 주소를 넣을 때만 설정 파일을 만듦.

```bash
copy .env.example .env      # 윈도우 (맥: cp .env.example .env)
```

`.env`에서 값만 고치고 서버를 다시 켬 (`BACKEND_URL`을 넣으면 `--backend-url` 없이 실행 가능). 항목 설명은 `.env.example` 안에 있음.
같은 값을 명령 옵션으로 주면 옵션이 우선함. `.env`는 깃에 올라가지 않음.

### 잘 안 될 때

| 증상 | 해결 |
|---|---|
| 설치에서 `No matching distribution` | 파이썬이 3.11 이하. 3.12 이상으로 `.venv`를 다시 만듦 |
| 모델 받기에서 `CERTIFICATE_VERIFY_FAILED` (맥) | python.org 파이썬이면 `/Applications/Python 3.x/Install Certificates.command`를 한 번 실행 |
| 서버 시작에서 `WinError 10013` 또는 `Address already in use` | 그 포트를 다른 프로그램이 쓰거나 윈도우가 예약함. `.env`에서 `STT_PORT`·`WS_PORT`·`MOCK_PORT`를 바꿈 |
| 모델 파일이 있는데 열지 못함 (윈도우) | 경로에 한글이 있으면 생길 수 있음 (미확인). `.env`의 `NINEOUTING_STT_MODELS`를 `C:\nineouting\models` 같은 영문 경로로 두고 모델을 다시 받음 |
| 마이크 장치를 열지 못함 | `--list-devices`로 목록을 보고 `--device 번호`로 다른 장치를 고름 |
| 끄는 중에 멈춘 것 같음 | 저장에 최대 약 10초 걸림. 그래도 안 꺼지면 맥은 Ctrl+\, 윈도우는 터미널 창 닫기 |

## 확인용 화면

모두 시험용이고 실제 FE·백엔드가 아님. STT 서버를 켜면 화면 2개(8765), 가짜 백엔드를 켜면 화면 1개(8767)가 열림.

| 켜는 서버 | 주소 | 화면 | 보는 것 |
|---|---|---|---|
| STT 서버 (`app/server.py`) | `localhost:8765/sender` | 보내기 + 자막 + 지연 | 작성자 브라우저 흉내. 마이크 소리를 STT로 보내고(시작·멈춤·재개·종료, 끊고 다시 붙기), 가짜 백엔드가 받은 자막과 지연(말한 때 → 글자가 뜬 때)을 보여줌 |
| STT 서버 (`app/server.py`) | `localhost:8765` | STT 개발 화면 | STT가 인식한 결과를 바로 봄. 직접 고치기, 다른 인식 결과, 확신도 낮은 글자 표시. 시각은 STT 내부 기준이라 멈출 때마다 2.5초씩 앞섬 |
| 가짜 백엔드 (`tools/mock_backend.py`) | `localhost:8767` | 가짜 백엔드 | 받은 요청 기록과 원문 JSON, 합친 자막, 규칙 위반 수. "10초 동안 실패(503)" 버튼으로 장애 뒤 재전송 시험 |

## 자주 쓰는 옵션

전체 목록은 `python app/server.py --help`.

| 옵션 | 내용 |
|---|---|
| `--input ws` | 마이크 대신 브라우저가 WebSocket(기본 포트 8770)으로 보내는 소리를 받음 |
| `--backend-url 주소` | 전사를 보낼 백엔드. 없으면 파일에만 기록 |
| `--device N` / `--list-devices` | 마이크 선택 / 마이크 목록 |
| `--wav 파일` | 마이크 대신 wav(16kHz 모노)를 실제 속도로 넣음 (같은 녹음으로 반복 시험) |
| `--hotwords 파일` | 잘 틀리는 용어 목록 (기본 `app/hotwords.txt`, 끄려면 `--hotwords ''`) |
| `--models 폴더` | 모델 폴더 (기본 `prototype/models`) |
| `--no-alt`, `--no-diar` | 다른 인식 결과 끄기, 화자 번호 끄기 (메모리 절약) |
| `--port N`, `--ws-port N` | 화면 포트 (기본 8765), WebSocket 포트 (기본 8770) |
| `--host`, `--ws-host` | 접속을 받는 범위. 기본 `127.0.0.1`(이 컴퓨터만), 서버에 올릴 때 `0.0.0.0` |

`--backend-url`, `--port`, `--ws-port`, `--host`, `--ws-host`, `--models`는 `.env`(`BACKEND_URL`, `STT_PORT`, `WS_PORT`, `STT_HOST`, `WS_HOST`, `NINEOUTING_STT_MODELS`)로도 정할 수 있음.

## 내보내는 형식

- 백엔드: `POST {backend-url}/internal/meetings/{meetingId}/transcripts`, 본문 `{"segments": [...]}`. 0.5초마다 바뀐 줄만 보내고, 실패하면 다음 주기에 다시 보냄. 필드 설명은 `app/backend_sender.py` 첫머리
- 브라우저 → STT (WebSocket, `ws://localhost:8770`)
  - 상태 확인: `GET /health` → `{"ok": true, "input": "ws", "state": "run", "meeting": "12", "audioSec": 134.2}`. 엔진이 죽었으면 `503`. **음성 포트와 개발 화면 포트 양쪽에 있음** — 운영에서는 개발 화면 포트를 닫으므로 바깥에서는 음성 포트로 확인
  - 연결 주소: `/ws/stt/meetings/{meetingId}?ticket=…` (Gateway를 거쳐도 경로가 그대로 옴)
  - 입장권 확인: 연결을 받으면 STT가 `POST {backend-url}/internal/stt-tickets/{ticket}/consume`을 한 번 부름. 401이거나 백엔드가 3초 안에 답하지 못하면 `1008`로 거절. 연결 주소·`start` 메시지·확인 응답의 `meetingId`가 **모두 같아야** 받아들임
    - 입장권은 30초 유효하고 한 번 쓰면 지워짐. 다시 연결할 때는 브라우저가 새로 받아 와야 함
    - `--backend-url`이 없거나 `--no-ticket`이면 확인하지 않음 (백엔드 없이 `/sender`로 시험할 때)
  - 소리: 바이너리로 16kHz 모노 16비트 PCM(little-endian), 100ms(3,200바이트)씩
  - 제어(JSON): `{"type":"start","meetingId":"…","offsetMs":0}`, `{"type":"pause"}`, `{"type":"resume"}`, `{"type":"end"}`
  - 응답(JSON): `ready`(서버가 받은 그 회의 소리 길이 `meetingMs`), `ended`, `error`
  - 시각은 모두 회의 시작 기준 음성 밀리초. 실제 시각(벽시계)은 브라우저가 보내지 않음 (백엔드가 기록)
  - 끊겼다 다시 붙을 때는 `start`의 `offsetMs`에 이어 보낼 위치를 넣음. 겹친 부분은 버리고 빈 시간은 무음으로 채움
  - `meetingId`는 백엔드 회의 번호(숫자). 숫자가 아니면 연결을 끊음 (틀린 번호로 전사가 나가지 않게)
  - 연결은 한 번에 하나. 새 연결이 오면 이전 연결을 닫고 이어받음 (서버가 끊김을 아직 모르는 사이에 다시 붙어도 막히지 않게). 자리를 내준 연결은 코드 `4001`로 닫히므로, 브라우저는 이 코드를 받으면 다시 붙지 않아야 함
- 백엔드로 보내는 것 (`app/backend_sender.py`)
  - 전사 줄: `POST /internal/meetings/{meetingId}/transcripts`, 0.5초마다 바뀐 줄만
  - 회의 알림: `POST /internal/meetings/{meetingId}/events`, 시작·멈춤·재개·끝을 생긴 순서대로 하나씩. 앞의 알림이 성공해야 다음을 보내고, 못 보낸 것은 복구 뒤 순서대로 나감
  - AI 서비스로는 백엔드가 넘김. STT에서 직접 보내던 경로는 없앰 (2026-10-20)
- 파일: 세션마다 `transcripts/날짜_시각.jsonl`(전사), `.backend.jsonl`(백엔드로 보낸 것), 녹음 버튼을 누르면 `.wav`. 깃에 올라가지 않음

## 이미지

`develop`에 들어갈 때마다 GitHub Actions가 만들어 GHCR에 올립니다.

```bash
docker pull ghcr.io/nineouting/nineouting-stt:develop
docker run -p 8770:8770 -e BACKEND_URL=http://meeting-service:8083 ghcr.io/nineouting/nineouting-stt:develop
```

- **플랫폼: `linux/amd64`와 `linux/arm64`.** 하나의 태그에 둘 다 들어 있어 받는 쪽 CPU에 맞는 것이 자동으로 내려옵니다. 일부러 고르려면 `docker pull --platform linux/amd64 …`
- 태그: 브랜치 이름(`:develop`)과 커밋 SHA. 다른 서비스 레포와 같은 모양입니다
- 모델은 이미지 안에 들어 있어(약 160MB) 실행할 때 받지 않습니다
- 기본값으로 음성 포트만 열립니다 (`WS_HOST=0.0.0.0`, `STT_HOST=127.0.0.1`). 개발 화면은 로그인이 없으므로 운영에서 열지 마세요
- 상태 확인: `GET :8770/health`

직접 만들려면 `docker build -t nineouting-stt prototype/` 입니다. 빌드 컨텍스트가 `prototype/`입니다.

## 용어

| 용어 | 뜻 |
|---|---|
| 말하는 중 / 확정 | 말하는 동안 계속 바뀌는 글자 / 발화가 끝나 더는 안 바뀌는 줄 |
| 보완 줄 | 스트리밍 인식이 놓친 말을 VAD(말소리 감지)로 찾아 다시 인식한 줄. 2초쯤 늦게 앞 시각 자리에 끼워짐 |
| `seq` / `rev` | 줄 번호 / 그 줄의 몇 번째 버전인지. 같은 `seq`는 `rev`가 큰 것으로 덮어씀 |
| 핫워드 | 인식기가 우선해서 고르게 하는 용어 목록 |
| 확신도 낮음 (`lowConfidence`) | 줄 글자 중 모델 확신도 0.3 미만이 20% 이상 |

## 알려진 제한

- 윈도우에서는 아직 처음부터 따라 해 보지 않음 (패키지의 윈도우용 설치 파일이 있는 것만 확인)
- 아주 짧은 대답이나 말이 겹치는 부분은 빠지거나 틀리기 쉬움 (보완 줄로 일부만 되살림)
- 보완 줄과 이웃 줄의 경계에서 한 단어가 겹칠 수 있음
- 화면에서 고친 내용은 백엔드로 보내지 않음 (정식 서비스에서는 백엔드가 처리)
- 화자 번호는 시험 기능이고, 제품에서는 발화자 구분을 하지 않기로 함
- 정식 서비스의 Gateway 경유 연결(로그인 정보, 경로)은 아직 정하지 않음

## 모델 출처

| 모델 | 출처 | 라이선스 |
|---|---|---|
| STT (Zipformer 174M, 스트리밍) | [kangkyu/icefall-asr-ko-streaming-zipformer-174m](https://huggingface.co/kangkyu/icefall-asr-ko-streaming-zipformer-174m) | Apache 2.0 |
| VAD (Silero) | [sherpa-onnx 배포](https://github.com/k2-fsa/sherpa-onnx/releases/tag/asr-models) | MIT |
| 화자 (TitaNet small, 선택) | [sherpa-onnx 배포](https://github.com/k2-fsa/sherpa-onnx/releases/tag/speaker-recongition-models) | NVIDIA 모델 (원 배포처 조건 확인 필요) |

설정을 고른 근거와 시험 기록은 [docs/notes.md](docs/notes.md).
