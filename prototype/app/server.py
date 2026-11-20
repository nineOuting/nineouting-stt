r"""회의 실시간 전사 웹 화면 (로컬 전용).

마이크 또는 wav를 엔진에 넣고, 이벤트를 SSE로 브라우저에 보냄.
http://localhost:8765 에서 열림. 녹음은 화면의 "녹음 시작"을 눌렀을 때만 이 컴퓨터에 저장함.
--backend-url·--summary-url을 주면 그 주소로 전사를 보냄 (--input ws면 브라우저가 WebSocket으로 보내는 소리를 받음).
마이크 입력: sounddevice (윈도우·맥 공통). 예전 ffmpeg 입력은 틱 잡음·소리 빠짐 문제로 지움 (docs/notes.md)

실행 (nineouting-stt/prototype 기준, 윈도우는 python 대신 .venv\Scripts\python, 맥은 .venv/bin/python)
  마이크 장치 목록:  python app/server.py --list-devices
  마이크로 전사:     python app/server.py            (시스템 기본 입력. 장치를 고르려면 --device 번호)
  입력 상태 진단:    python app/server.py --diag     (30초마다 받은 소리·읽기 대기·처리 시간을 터미널에 출력)
  wav로 시험:       python app/server.py --wav 녹음.wav     (16kHz 모노 16비트, 실제 속도로 넣음)
  전사문 저장:       자동. 세션마다 transcripts/날짜_시각.jsonl 에 줄이 바뀔 때마다 저장 (--save-dir로 폴더, --out으로 첫 세션 파일 지정)
종료: Ctrl+C
주소·포트 설정: prototype/.env (.env.example을 복사). 값을 정하는 순서는 명령 옵션 > 환경 변수 > .env > 기본값

세션 (화면 버튼)
  서버를 켜면 세션 1이 시작되고 바로 전사함. 녹음은 하지 않음
  녹음 시작: 지금 세션에 녹음을 붙임 (직전 최대 2분, 세션 시작 이후분 포함). 같은 이름의 .wav
  일시정지/계속: 전사와 녹음을 같이 멈춤. 멈출 때 무음 2.5초를 넣어 말하던 줄과 보완 판단을 마무리함 (녹음에도 들어감)
  완료: 세션을 닫고 대기. 닫은 세션의 줄을 고치면 그 파일에 반영됨. 대기에서 녹음 시작 → 화면을 비우고 새 세션 파일
  시각: 줄의 start·end는 서버 시작 뒤 엔진에 넣은 소리 기준 초 (멈춘 동안은 늘지 않음). wav 안의 시각 = start - wav_t0 (파일 첫 줄 _meta)

회의 알림 (backend_sender.py, CN팀 S2 2026-10-20)
  세션 = 회의. 시작·멈춤·재개·끝을 백엔드로 바로 보냄. AI 서비스로는 백엔드가 넘김 (STT에서 직접 보내던 경로는 없앰)

브라우저 입력 (--input ws, CN팀 2장 2.1 검토 확정)
  회의록 작성자 브라우저가 16kHz 모노 16비트 PCM 100ms 조각과 start·pause·resume·end(JSON)를 WebSocket으로 보냄 (규약은 ws_handler 위 설명)
  서버를 켜면 대기하다가 start를 받으면 그 meetingId로 회의(Room)를 엶. 시험용 보내는 화면: http://localhost:8765/sender
  실행: python app/server.py --input ws --backend-url http://127.0.0.1:8767

여러 회의 (--input ws, SCRUM-288)
  모델(Shared)은 한 번만 올리고, 회의마다 Room(엔진 상태·세션 파일·백엔드 전송·소리 대기열·처리 스레드)을 따로 둠
  접속 포트는 하나. 연결마다 받는 스레드가 meetingId에 맞는 Room 대기열에 소리를 넣고, Room의 처리 스레드가 인식함
  동시에 여는 회의는 --max-meetings개까지. 다 차면 새 회의 연결은 1013으로 닫음
  /ready는 다 차도 200으로 둠: DEV는 파드가 하나라 /ready가 503이 되면 ALB 대상에서 빠져, 진행 중인 회의의 다시 연결까지 막힘
  연결 없이 --idle-release초가 지난 회의는 남은 줄을 보내고 end 없이 정리함 (나중에 붙으면 offsetMs로 이어 감)
  개발 화면은 주소 뒤 ?m=회의번호로 회의를 고름 (없으면 가장 최근에 시작한 회의)
  마이크·wav 입력은 지금처럼 Room 하나

백엔드 전송 (backend_sender.py, CN팀 2장 2.2 + AI팀 추가 필드, 2026-10-15 확정)
  말하는 중·확정·보완 줄을 --backend-period초(기본 0.5)마다 바뀐 것만 POST {--backend-url}/internal/meetings/{meeting_id}/transcripts
  화면에서 고치거나 지운 것은 보내지 않음 (정식 서비스에서는 백엔드가 처리). 발화자 구분은 하지 않아 화자 번호는 보내지 않음
  --backend-url이 없으면 세션 파일 옆 <이름>.backend.jsonl 에만 기록
"""
import argparse, collections, datetime, json, os, queue, re, signal, sys, threading, time, urllib.parse, wave
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from envfile import load_env  # noqa: E402
ENV_LOADED = load_env()   # engine의 모델 폴더 기본값(NINEOUTING_STT_MODELS)도 .env를 따르도록 engine보다 먼저
from engine import Engine, Shared, SR, DEFAULT_MODELS, DEFAULTS, line_kind  # noqa: E402
from backend_sender import BackendSender, consume_ticket  # noqa: E402
from drain import draining  # noqa: E402

STATIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')
p = argparse.ArgumentParser()
p.add_argument('--input', default='sd', choices=['sd', 'ws'],
               help='입력 방식 (sd=이 컴퓨터 마이크, ws=브라우저가 WebSocket으로 보내는 소리)')
p.add_argument('--host', default=os.environ.get('STT_HOST', '127.0.0.1'),
               help='화면 서버가 접속을 받는 범위 (.env STT_HOST). 127.0.0.1=이 컴퓨터만, 0.0.0.0=네트워크 전체')
p.add_argument('--ws-host', default=os.environ.get('WS_HOST', '127.0.0.1'), help='--input ws: WebSocket이 접속을 받는 범위 (.env WS_HOST)')
p.add_argument('--ws-port', type=int, default=os.environ.get('WS_PORT', 8770), help='--input ws: WebSocket 포트 (.env WS_PORT)')
p.add_argument('--device', default='', help='마이크 장치 번호 (--list-devices). 없으면 시스템 기본 입력')
p.add_argument('--list-devices', action='store_true')
p.add_argument('--diag', action='store_true', help='30초마다 입력 상태(실제 시간 대비 받은 소리, 읽기 대기, 처리 시간)를 터미널에 출력. '
               '없어도 마이크 소리가 빠지면(최근 30초 1%% 이상, 입력 넘침) 경고 한 줄은 나옴')
p.add_argument('--wav', default=''); p.add_argument('--port', type=int, default=os.environ.get('STT_PORT', 8765), help='화면 포트 (.env STT_PORT)')
p.add_argument('--models', default=DEFAULT_MODELS, help='모델 폴더 (기본: prototype/models, .env NINEOUTING_STT_MODELS). 받기: python tools/download_models.py')
p.add_argument('--chunk', default=DEFAULTS['chunk'], choices=['16', '64'])
p.add_argument('--hotwords', default=DEFAULTS['hotwords'],
               help="핫워드(회의 용어) 파일. 기본 app/hotwords.txt 31개. 끄려면 --hotwords ''")
p.add_argument('--hotwords-score', type=float, default=DEFAULTS['hotwords_score'])
p.add_argument('--blank-penalty', type=float, default=DEFAULTS['blank_penalty'], help='글자 안 냄(blank)에 주는 벌점. 빠지는 말이 줄고 틀린·낀 글자가 조금 늚. 0이면 끔 (1.0부터 삽입 급증)')
p.add_argument('--no-recover', action='store_true'); p.add_argument('--out', default='', help='첫 세션 전사 파일 경로 (기본: 자동 이름)')
p.add_argument('--save-dir', default=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'transcripts'),
               help='세션 전사·녹음 파일 폴더 (기본: prototype/transcripts, 깃 제외)')
p.add_argument('--no-diar', action='store_true', help='화자 번호 표시 끄기')
p.add_argument('--no-alt', action='store_true', help='다른 인식 결과 끄기 (메모리 약 300MB 절약, 추정)')
p.add_argument('--max-speakers', type=int, default=0, help='참석자 수 (0이면 자동). 화면에서도 바꿀 수 있음')
p.add_argument('--max-len', type=float, default=DEFAULTS['max_len'], help='한 줄 최대 길이(초). 쉼 없이 길게 말하면 이 길이에서 끊음 (이전 20)')
p.add_argument('--overlap', type=float, default=DEFAULTS['overlap'], help='최대 길이로 끊을 때 직전 소리를 새 줄에 다시 넣는 길이(초). 걸친 단어를 살리는 대신 앞뒤 줄에 겹칠 수 있음. 0이면 끔')
p.add_argument('--backend-url', default=os.environ.get('BACKEND_URL', ''),
               help='백엔드 주소 (.env BACKEND_URL, 예: http://meeting-service:8083). 없으면 backend.jsonl 파일에만 기록')
p.add_argument('--backend-period', type=float, default=0.5, help='백엔드 전송 주기(초). 그사이 바뀐 줄만 보냄')
p.add_argument('--meeting-id', default='', help='첫 회의의 회의 번호 (백엔드가 쓰는 숫자). 없으면 자동 이름을 붙이고 백엔드로는 보내지 않음')
p.add_argument('--max-meetings', type=int, default=int(os.environ.get('MAX_MEETINGS') or 4),
               help='--input ws: 동시에 여는 회의 수 상한 (.env MAX_MEETINGS). 다 차면 새 회의는 1013으로 닫음 (/ready는 그대로 200). '
                    '0이면 상한 없음 (시험용: CPU가 모자라면 모든 회의의 자막이 같이 늦어짐). '
                    '기본 4: DEV 파드의 보장 CPU 0.5코어로 버티는 수 (회의당 약 0.1코어, 2026-11-18 DEV 측정)')
p.add_argument('--backlog-warn', type=float, default=float(os.environ.get('BACKLOG_WARN_SEC') or 5),
               help='--input ws: 회의의 밀린 소리가 이 초를 넘으면 로그에 남김 (.env BACKLOG_WARN_SEC, 0이면 끔). 1초 밑으로 내려오면 풀림도 남김')
p.add_argument('--idle-release', type=float, default=float(os.environ.get('MEETING_IDLE_SEC') or 600),
               help='--input ws: 연결이 끊긴 회의를 이 초만큼 기다린 뒤 정리함 (.env MEETING_IDLE_SEC). end는 보내지 않음')
p.add_argument('--replace-max', type=int, default=int(os.environ.get('REPLACE_MAX') or 5),
               help='--input ws: 한 회의의 연결이 --replace-window초 안에 이 횟수만큼 바뀌었으면 새 연결을 4002로 거절하고 지금 연결을 지킴 '
                    '(.env REPLACE_MAX, 0이면 끔). 같은 회의를 연 브라우저 둘이 서로 밀어내기를 반복하지 않게 함. 기본 5는 추정값')
p.add_argument('--replace-window', type=float, default=float(os.environ.get('REPLACE_WINDOW_SEC') or 60),
               help='--input ws: --replace-max를 세는 시간(초) (.env REPLACE_WINDOW_SEC)')
p.add_argument('--no-ticket', action='store_true',
               help='--input ws: 음성 연결 입장권을 확인하지 않음 (백엔드 없이 /sender로 시험할 때). '
                    '--backend-url이 없으면 어차피 확인하지 않음')
A = p.parse_args()

if A.list_devices:
    import sounddevice as sd
    for i, d in enumerate(sd.query_devices()):
        if d['max_input_channels'] > 0:
            print(f'[{i}] {d["name"]} (입력 {d["max_input_channels"]}채널, {d["default_samplerate"]:.0f}Hz)' + (' ← 기본' if i == sd.default.device[0] else ''))
    sys.exit(0)
if A.input == 'sd' and not A.wav and A.device:
    # 스피커 등 마이크가 아닌 장치를 고르기 쉬움 → 열기 전에 확인하고 마이크 목록을 알려 줌
    import sounddevice as sd
    devs = sd.query_devices()
    mics = ', '.join(f'[{i}] {d["name"]}' for i, d in enumerate(devs) if d['max_input_channels'] > 0)
    try:
        info = sd.query_devices(int(A.device) if A.device.isdigit() else A.device)
    except (ValueError, sd.PortAudioError):
        sys.exit(f'장치 {A.device!r}를 찾지 못함. 마이크: {mics} (--list-devices)')
    if info['max_input_channels'] < 1:
        sys.exit(f'장치 {A.device}번 {info["name"]!r}는 마이크가 아님. 마이크: {mics} (--list-devices)')

# ---- 모델 확인: 필수가 없으면 받는 방법을 알려 주고 끝냄, 선택이 없으면 그 기능만 끔 ----
_st = 'epoch-99-avg-1-chunk-{}-left-128.int8.onnx'
_need = [f'zipformer-174m/tokens.txt', f'zipformer-174m/encoder-{_st.format(A.chunk)}', f'zipformer-174m/decoder-{_st.format(A.chunk)}',
         f'zipformer-174m/joiner-{_st.format(A.chunk)}'] + ([] if A.no_recover else ['speaker/silero_vad.onnx'])
_missing = [f for f in _need if not os.path.exists(os.path.join(A.models, f))]
if _missing:
    sys.exit(f'모델 파일이 없음 ({A.models}):\n  ' + '\n  '.join(_missing) +
             '\n받기: python tools/download_models.py  (prototype 폴더에서 실행. 다른 폴더의 모델을 쓰려면 --models 경로)')
if not A.no_alt and not os.path.exists(os.path.join(A.models, f'zipformer-174m/encoder-{_st.format(16)}')):
    A.no_alt = True
    print('다른 인식 결과용 모델(chunk-16)이 없어 이 기능을 끔. 쓰려면 python tools/download_models.py --alt', file=sys.stderr, flush=True)
if not A.no_diar and not os.path.exists(os.path.join(A.models, 'speaker/nemo_en_titanet_small.onnx')):
    A.no_diar = True
    print('화자 번호용 모델(TitaNet)이 없어 이 기능을 끔. 쓰려면 python tools/download_models.py --diar', file=sys.stderr, flush=True)

# ---- 회의방 (Room) ----
# 회의 하나의 상태를 모두 담음: 엔진 상태, 줄, 세션 파일, 백엔드 전송, 소리·제어 대기열, 처리 스레드, 지금 연결.
# 마이크·wav 입력은 Room 하나('local')를 서버가 켜질 때 만들고, --input ws는 start가 올 때 meetingId마다 만듦
SHARED = None          # 여러 회의가 같이 쓰는 모델 (engine.Shared). main에서 한 번 만듦
LOCAL = None           # 마이크·wav 입력의 Room
MLOCK = threading.Lock()
MEETINGS = {}          # --input ws: meetingId → 열린 Room. end·정리·오류 때 바로 빠짐 (자리 계산은 이 수로 함)
LIVE = set()           # 처리 스레드나 전송이 아직 끝나지 않은 Room (끌 때 남은 줄을 보내야 하는 것)
RECENT = collections.OrderedDict()   # 개발 화면용 최근 Room (끝난 것 포함 5개). key → Room
CONNS = set()          # 지금 붙어 있는 WebSocket 연결 전부 (start 전 포함). drain이 이 수가 0이 되길 기다림
PROC = {'running': True, 'error': None}   # 프로세스 단위 상태. 입력 서버·로컬 입력 스레드가 죽으면 False → /health 503
ROOM_ERR = {'count': 0, 'last': None}     # 회의 하나의 처리 스레드가 죽은 횟수 (그 회의만 닫고 서버는 계속)
stop = threading.Event()
MS = SR * 2 // 1000   # 1ms = 32바이트
FRESH_MS = 2000       # 이만큼 받은 회의에 offsetMs 0으로 다시 붙으면 새로 연 브라우저로 봄 (시작 직후 끊긴 정상 다시 연결과 구분)
WS_MODE = A.input == 'ws' and not A.wav


def log(m):
    print(m, file=sys.stderr, flush=True)


def at_cap(n):
    """열린 회의 n개가 상한에 닿았는지. 상한 0은 상한 없음"""
    return A.max_meetings > 0 and n >= A.max_meetings


def full():
    """--input ws: 열린 회의가 상한에 닿음"""
    with MLOCK: return WS_MODE and at_cap(len(MEETINGS))


class Room:
    def __init__(self, key, source):
        self.key = key
        self.tag = f'[회의 {key}] ' if WS_MODE else ''
        self.lock = threading.RLock()   # 요약 전송 알림이 publish 안에서 다시 publish를 부를 수 있어 재진입 허용
        self.lines = {}          # id → 확정·보완 줄 (새로 접속한 화면에 보냄)
        self.partial = None      # 지금 말하는 중인 줄
        self.clients = []        # 화면별 큐
        self.status = {'source': source, 'audio_sec': 0.0, 'running': True, 'max_speakers': A.max_speakers,
                       'state': 'run', 'rec': False, 'rec_sec': 0.0, 'session': ''}   # state: run / pause / idle(완료 후 대기)
        self.engine = None
        self.manual_seq = 0      # 직접 추가한 줄 번호
        self.clear_at = -1.0     # 마지막으로 지운 오디오 시각. 이보다 앞에서 시작한 보완 줄은 나중에 도착해도 버림
        self.backlog_warned = False   # 밀림 로그를 남긴 뒤 아직 풀리지 않음
        self.clear_id = 0        # 지울 때의 엔진 줄 번호. 이 번호 이하의 말하던·확정 줄은 지우기 전 줄이라 버림
        # (인식기는 글자로 안 바꾼 소리를 약 1.2초 들고 있어서, 지운 뒤 새 줄의 시작 시각이 지운 시각보다 조금 앞설 수 있음 → 시각으로 거르면 지운 뒤 말까지 버려짐)
        self.sess = None         # 지금 세션: file, wav_path, wav(쓰는 중인 wave 객체), wav_t0, t0, started, complete
        self.ctrl = queue.Queue()   # 녹음·일시정지·완료 요청. 처리 스레드가 소리 조각 사이에 처리함
        self.q = queue.Queue()      # --input ws: 소리와 제어 메시지를 받은 순서대로 (받는 스레드 → 처리 스레드)
        self.catchup = False     # 밀린 소리가 1초 넘게 쌓임 (다시 연결 뒤 몰아 보내는 중) → 말하는 중 줄은 백엔드로 보내지 않음
        # --input ws 연결 상태(conn·gone_at·meeting·ms·skip·shift·replaced)는 clock으로 지킴. 받는 스레드가 0.1초마다 잡으므로,
        # 처리 스레드가 줄을 내보내며 파일까지 쓰는 lock과 나눔
        self.clock = threading.Lock()
        self.replaced = collections.deque()   # 연결이 바뀐 시각 (monotonic, 최근 --replace-window초만)
        self.conn = None         # --input ws: 지금 붙어 있는 연결 (이 연결이 보낸 것만 회의에 넣음)
        self.meeting = None      # --input ws: start를 받은 meetingId
        self.ms = 0.0            # 그 회의에서 받은 소리(ms)
        self.skip = 0            # 다시 연결 뒤 겹쳐서 버릴 바이트
        self.shift = 0.0         # 서버 시계 - 브라우저 시계(ms). 새로 연 브라우저가 0부터 다시 셀 때 생김
        self.gone_at = None      # 연결이 끊긴 시각 (monotonic). 붙어 있으면 None
        self.finished = False    # 처리 스레드가 빠져나갈 때 (end·정리)
        self.thread = None
        self.backend = BackendSender(A.backend_url, A.backend_period, now=lambda: self.engine.fed if self.engine else 0.0,
                                     log=lambda m: log(self.tag + m), on_update=lambda ev: self.publish(ev))

    # ---- 이벤트 저장·배포 ----
    def publish(self, ev):
        with self.lock:
            if (ev['type'] in ('partial', 'final', 'drop') and ev['id'] <= self.clear_id) or (ev['type'] == 'recovered' and ev['start'] < self.clear_at):
                return   # 지우기 전 줄 (말하던 중이던 줄, 판단 대기 중이던 보완)
            if ev['type'] == 'partial':
                self.partial = ev
                if not self.catchup: self.backend.partial(ev)   # 몰아 보내는 동안은 확정 줄만 보냄
            elif ev['type'] == 'drop':   # 말하는 중이던 줄을 엔진이 버림: 화면에서 빼고 백엔드에 삭제로 알림 (세션 파일에는 원래 없음)
                if self.partial and self.partial['id'] == ev['id']: self.partial = None
                self.backend.drop(ev)
                for q in self.clients: q.put(ev)
                return
            elif ev['type'] == 'send':   # 백엔드 전송 상태창 (줄에는 표시하지 않음)
                for q in self.clients: q.put(ev)
                return
            elif ev['type'] == 'status':
                pass
            elif ev['type'] == 'alt':  # 이미 있는 줄에 다른 인식 결과를 붙임 (새로 접속한 화면에도 보내기 위해)
                if ev['id'] not in self.lines: return   # 지운 줄에 늦게 온 결과는 버림
                self.lines[ev['id']]['alts'] = ev['alts']
            elif ev['type'] == 'clear':  # 전사 기록 지우기 (모든 화면에 알림). 이미 보낸 줄은 백엔드에서 지우지 않음
                eng = self.engine
                self.lines.clear(); self.partial = None; self.clear_at = self.status['audio_sec']; self.clear_id = eng.next_id if eng else 0
                if eng: eng.restart = True   # 지운 뒤에 한 말이 지우기 전 줄에 이어 붙어 버려지지 않게
            elif ev['type'] == 'merge':  # 화자 src를 dst로 합침: 지난 줄 번호를 모두 바꿈
                for ln in self.lines.values():
                    if ln.get('spk') == ev['src']:
                        ln['spk'] = ev['dst']; ln['merged_from'] = ev['src']
                if self.partial and self.partial.get('spk') == ev['src']: self.partial['spk'] = ev['dst']
            elif ev['type'] == 'edit':  # 사용자가 후보를 고르거나 직접 고침. 처음 텍스트는 orig로 남겨 되돌릴 수 있게 함
                ln = self.lines.get(ev['id'])
                if not ln or ln.get('deleted'): return
                if 'orig' not in ln: ln['orig'], ln['orig_conf'] = ln['text'], ln.get('conf')
                if ev['text'] == ln['orig']:   # 원래대로 되돌림
                    ln['text'], ln['conf'] = ln['orig'], ln['orig_conf']; ln.pop('edited', None)
                else:
                    ln['text'], ln['conf'], ln['edited'] = ev['text'], None, ev['source']
                ln['kind'] = line_kind(ln['text'])   # 고친 텍스트로 맞장구·간투사 다시 판별
                ln['rev'] = ln.get('rev', 0) + 1
                ev = {'type': 'edit', 'id': ev['id'], 'text': ln['text'], 'conf': ln['conf'],
                      'edited': ln.get('edited'), 'orig': ln['orig'], 'kind': ln['kind']}
            elif ev['type'] == 'delete':  # 사용자가 줄을 지움. 파일에는 deleted로 남기고 화면에서는 뺌
                ln = self.lines.get(ev['id'])
                if not ln or ln.get('deleted'): return
                ln['deleted'] = True; ln['rev'] = ln.get('rev', 0) + 1
            else:
                self.lines[ev['id']] = ev
                if self.partial and self.partial['id'] == ev['id']: self.partial = None
                if ev['type'] in ('final', 'recovered'): self.backend.line(ev)   # 직접 추가한 줄은 백엔드에서 생기므로 보내지 않음
            for q in self.clients: q.put(ev)
            if ev['type'] not in ('partial', 'status'): self.save_session()

    def publish_status(self):
        st = self.status
        self.publish({'type': 'status', **{k: st[k] for k in ('state', 'rec', 'rec_sec', 'session', 'audio_sec')}})

    # ---- 세션 (자동 저장·녹음) ----
    def new_session(self, t0, path=None, mid=None, offset=0.0):
        """offset: --input ws에서 start의 offsetMs(초). 백엔드 seq 시작값을 정함 (STT가 다시 켜져도 앞서 보낸 줄과 겹치지 않게)"""
        os.makedirs(A.save_dir, exist_ok=True)
        ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        # 여러 회의가 같은 초에 시작하면 파일 이름이 겹치므로 회의 번호를 붙임
        base = path or os.path.join(A.save_dir, ts + (f'_m{mid}' if WS_MODE and mid else '') + '.jsonl')
        os.makedirs(os.path.dirname(os.path.abspath(base)), exist_ok=True)
        mid = mid or (A.meeting_id if A.meeting_id and self.sess is None else '') or 'm_' + ts
        self.sess = {'file': base, 'wav_path': None, 'wav': None, 'wav_t0': None, 't0': round(t0, 2), 'meeting_id': mid,
                     'started': datetime.datetime.now().isoformat(timespec='seconds'), 'complete': False}
        eng = self.engine
        self.backend.start(mid, t0, os.path.splitext(base)[0] + '.backend.jsonl',
                           first_id=eng.next_id if eng else 0, offset_ms=round(offset * 1000))
        self.status['session'] = os.path.basename(base); self.status['rec'] = False; self.status['rec_sec'] = 0.0

    def save_session(self):
        """지금 세션의 줄을 파일에 통째로 다시 씀 (lock 안에서 호출). 첫 줄은 _meta"""
        S = self.sess
        if not S: return
        meta = {k: S[k] for k in ('meeting_id', 'started', 't0', 'wav_t0', 'complete')}
        if S.get('gaps'): meta['gaps'] = S['gaps']   # --input ws: 멈춤 때 넣은 무음 [엔진 시각, 길이]. 회의 시각 = start - t0 - (그 앞 gaps 합)
        meta.update(source=self.status['source'], wav=S['wav_path'] and os.path.basename(S['wav_path']),
                    note='start·end는 회의(Room)를 연 뒤 엔진에 넣은 소리 기준 초. wav 안의 시각 = start - wav_t0')
        tmp = S['file'] + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            f.write(json.dumps({'_meta': meta}, ensure_ascii=False) + '\n')
            for ev in sorted(self.lines.values(), key=lambda e: e['start']):
                f.write(json.dumps({k: ev.get(k) for k in ('id', 'start', 'end', 'spk', 'spk_info', 'text', 'type', 'kind', 'conf',
                                                           'alts', 'edited', 'orig', 'orig_conf', 'merged_from', 'rev', 'deleted')},
                                   ensure_ascii=False) + '\n')
        os.replace(tmp, S['file'])   # 쓰는 도중 꺼져도 이전 파일은 남음

    def rec_write(self, c):
        S = self.sess
        if S and S['wav']:
            S['wav'].writeframes((np.clip(c, -1, 1) * 32767).astype(np.int16).tobytes())
            self.status['rec_sec'] = round(self.status['rec_sec'] + len(c) / SR, 1)

    def rec_close(self):
        if self.sess and self.sess['wav']: self.sess['wav'].close(); self.sess['wav'] = None

    def control(self, eng, act):
        """처리 스레드에서만 호출. 상태에 맞지 않는 요청은 무시"""
        st, status = self.status['state'], self.status

        def flush():   # 말하던 줄 확정·보완 판단을 위해 무음을 넣음 (녹음에도 같이 넣어 시각을 맞춤)
            z = np.zeros(1600, np.float32)
            for _ in range(25): eng.feed(z); self.rec_write(z)
            status['audio_sec'] = round(eng.fed, 1)

        def mark_gap(at, to):   # 위 무음은 브라우저가 세는 회의 시간에 없음 → 보내는 시각에서 뺌
            if WS_MODE:
                self.backend.gap(at, to - at)
                self.sess.setdefault('gaps', []).append([round(at, 2), round(to - at, 2)])
        if isinstance(act, tuple) and act[0] == 'start':   # --input ws: 브라우저가 회의 시작을 알림 (meetingId, 회의 시작 기준 음성 초)
            _, mid, off = act
            if st in ('run', 'pause'): self.control(eng, 'complete')   # Room마다 start는 한 번이라 보통 오지 않음
            with self.lock: self.new_session(eng.fed - off, mid=mid, offset=off)   # 줄 시각 = 회의 시작 기준 음성 시간이 되도록 기준점을 맞춤
            self.publish({'type': 'clear', 'session_switch': True})
            status['state'] = 'run'
            with self.lock: self.save_session()
            self.publish_status()
            return
        if act == 'release':   # --input ws: 연결 없이 오래 지난 회의를 정리. 회의가 끝난 것이 아니라 end는 보내지 않음
            if st == 'run':
                at = eng.fed; flush(); mark_gap(at, eng.fed)
            self.rec_close()
            self.backend.drain(wait=2.0)
            with self.lock: self.save_session()
            self.finished = True
            log(f'{self.tag}연결 없이 {A.idle_release:.0f}초가 지나 정리함 (end 알림은 보내지 않음)')
            return
        if act == 'record' and st == 'idle':      # 새 세션: 화면을 비우고 새 파일로
            with self.lock: self.new_session(eng.fed)   # 먼저 새 세션으로 바꿔야 아래 clear가 닫은 세션 파일을 비우지 않음
            self.publish({'type': 'clear', 'session_switch': True})
            st = status['state'] = 'run'
        S = self.sess
        if act == 'record' and st in ('run', 'pause') and not S['wav']:
            a0 = max(S['t0'], eng.fed - 120); pre = eng._audio(a0, eng.fed) if eng.fed > a0 else np.zeros(0, np.float32)
            S['wav_path'] = os.path.splitext(S['file'])[0] + '.wav'
            w = wave.open(S['wav_path'], 'wb'); w.setnchannels(1); w.setsampwidth(2); w.setframerate(SR)
            S['wav'] = w; S['wav_t0'] = round(eng.fed - len(pre) / SR, 2); status['rec'] = True; status['rec_sec'] = 0.0
            self.rec_write(pre)
        elif act == 'pause' and st == 'run':
            at = eng.fed; flush(); mark_gap(at, eng.fed)
            status['state'] = 'pause'; self.backend.event('pause')
        elif act == 'resume' and st == 'pause':
            status['state'] = 'run'; self.backend.event('resume')
        elif act == 'complete' and st in ('run', 'pause'):
            if st == 'run':
                at = eng.fed; flush(); mark_gap(at, eng.fed)   # 마무리용 무음은 회의 시간에 넣지 않음 (end의 atMs가 커지지 않게)
            self.rec_close(); status['state'] = 'idle'; status['rec'] = False
            self.backend.event('end', wait_lines=3.0)   # 남은 줄을 먼저 보낸 뒤 end (CN팀 S4). 3초 안에 못 비우면 그냥 보냄
            with self.lock: S['complete'] = True; self.save_session()
            if WS_MODE: self.finished = True   # 회의 하나 = Room 하나. 끝나면 처리 스레드를 닫음
        else:
            return
        with self.lock: self.save_session()
        self.publish_status()

    # ---- 소리 처리 ----
    def ws_read(self):
        """소리면 bytes, 제어 메시지면 제어 대기열로 넘기고 None, 0.1초 동안 없으면 None"""
        try: item = self.q.get(timeout=0.1)
        except queue.Empty: return None
        if not isinstance(item, bytes):
            self.ctrl.put(item); return None
        self.catchup = self.q.qsize() > 10
        return item

    def loop(self, read, D=None):
        """소리를 읽어 엔진에 넣음. stop·finished가 되거나 입력이 끝나면(빈 bytes) 돌아옴"""
        eng = self.engine = Engine(self.publish, shared=SHARED, max_speakers=A.max_speakers, recover=not A.no_recover,
                                   overlap=A.overlap)
        if D:   # 엔진이 부르는 화면 알림·파일 저장 시간을 따로 셈 (다른 결과 스레드의 알림은 세지 않음)
            def timed(ev, _p=self.publish):
                t = time.perf_counter(); _p(ev); D.pub += time.perf_counter() - t
            eng.on_event = timed
        status = self.status
        if WS_MODE:   # 브라우저가 보낸 start(대기열 첫 항목)로 세션을 엶
            status['state'] = 'idle'
        else:
            with self.lock: self.new_session(0.0, A.out or None); self.save_session()
        t0 = time.perf_counter(); fed = 0.0
        while not stop.is_set() and not self.finished:
            if D: D.tick(status['state'] == 'run')
            while not self.ctrl.empty() and not self.finished:
                was = status['state']; self.control(eng, self.ctrl.get())
                if was != 'run' and status['state'] == 'run': t0 = time.perf_counter() - fed   # wav 입력: 실제 속도 기준을 다시 맞춤
            if self.finished: break
            if A.wav and status['state'] != 'run':   # 파일 입력은 멈춘 동안 읽지 않음 (위치 유지)
                time.sleep(0.1); continue
            tr = time.perf_counter(); b = read()
            if b is None: continue   # WebSocket: 소리가 아직 없거나 제어 메시지를 넘김
            if not b: break
            c = np.frombuffer(b, dtype=np.int16).astype(np.float32) / 32768
            if A.wav:  # 실제 속도로
                d = fed + len(c) / SR - (time.perf_counter() - t0)
                if d > 0: time.sleep(d)
            if status['state'] != 'run':   # 마이크 입력은 멈춘 동안 읽어서 버림
                continue
            if D:
                dt = time.perf_counter() - tr; D.read += dt; D.n += 1; D.instant += dt < 0.001
                tf = time.perf_counter(); eng.feed(c); D.feed += time.perf_counter() - tf
                tw = time.perf_counter(); self.rec_write(c); D.rec += time.perf_counter() - tw; D.got += len(c) / SR
            else:
                eng.feed(c); self.rec_write(c)
            fed += len(c) / SR; status['audio_sec'] = round(eng.fed, 1)
        # 끌 때·입력이 끝났을 때는 말하던 줄을 확정함. end·정리 때는 이미 무음을 넣어 마무리했고,
        # end 알림 뒤에 줄이 나가지 않도록 여기서 더 넣지 않음
        if stop.is_set() or not WS_MODE: eng.finish()

    def ws_main(self):
        """--input ws Room의 처리 스레드"""
        try:
            self.loop(self.ws_read, Diag(tag=self.tag, verbose=True) if A.diag else None)
        except Exception as e:   # 이 회의만 닫고 서버는 계속 (브라우저가 다시 붙으면 새 Room이 offsetMs로 이어 감)
            log(f'{self.tag}입력/엔진 오류로 이 회의를 닫음: {e!r}')
            self.status['running'] = False; self.status['error'] = str(e)
            ROOM_ERR['count'] += 1; ROOM_ERR['last'] = str(e)
            with MLOCK:
                if MEETINGS.get(self.key) is self: del MEETINGS[self.key]
            c = self.conn
            if c is not None: threading.Thread(target=ws_close, args=(c, 1011, 'stt error'), daemon=True).start()
            if not stop.is_set():
                try: self.backend.drain(wait=2.0)
                except Exception: pass
        finally:
            if stop.is_set(): return   # 끌 때는 main이 남은 줄을 모아서 보냄
            self.status['running'] = False
            self.backend.stop(); self.engine = None   # 엔진(최근 2분 소리·VAD)을 놓아 메모리를 돌려줌. 줄은 개발 화면용으로 남김
            with MLOCK:
                if MEETINGS.get(self.key) is self: del MEETINGS[self.key]
                LIVE.discard(self)
            self.publish({'type': 'status', 'running': False})


def remember(room):
    with MLOCK:
        RECENT[room.key] = room; RECENT.move_to_end(room.key)
        while len(RECENT) > 5: RECENT.popitem(last=False)


def latest_room():
    with MLOCK:
        return next(reversed(RECENT.values()), None)


# ---- WebSocket 입력 (--input ws) ----
# 브라우저(회의록 작성자)가 보내는 소리를 받음. 규약은 CN팀 2장 2.1 검토 (2026-10-15 확정, 메시지 이름은 AI팀이 정함)
#   바이너리: 16kHz 모노 16비트 PCM(little-endian), 100ms = 3,200바이트
#   텍스트(JSON): {"type":"start","meetingId":"…","offsetMs":0} / {"type":"pause"} / {"type":"resume"} / {"type":"end"}
#     start의 offsetMs = 이어서 보낼 소리가 회의 시작 기준 몇 ms부터인지. 같은 회의로 다시 연결하면 겹친 만큼 버리고 빈 만큼 무음으로 채움
#   서버 → 브라우저: {"type":"ready","meetingId","meetingMs"} (meetingMs = 서버가 받은 그 회의 소리 길이), {"type":"ended"}, {"type":"error","message"}
# 여러 회의를 동시에 받음 (SCRUM-288). 접속 포트는 하나이고, 연결마다 받는 스레드가 하나씩 돎 (websockets 동기 서버)
#   같은 meetingId로 새 연결이 오면 그 회의의 이전 연결만 닫고 이어받음 (CN팀 S6을 회의별로).
#   끊긴 것을 서버가 아직 모르는 사이에 브라우저가 다시 붙어도 막히지 않게 함. 자리를 내준 연결은 4001로 닫아
#   브라우저가 "내가 밀려남"과 "그냥 끊김"을 구분할 수 있게 함. 다른 회의의 연결은 건드리지 않음
#   밀려난 연결이 닫히기 전까지 보낸 소리·제어 메시지는 버림 (SCRUM-335). 이전 연결이 살아 있으면(창 둘) 닫히는 사이에도
#   소리가 계속 오고, 라이브러리는 닫히는 중에도 받은 것을 넘겨주므로, 버리지 않으면 같은 구간이 두 번 들어가 시각이 밀림
#   한 회의의 연결이 --replace-window초 안에 --replace-max번 바뀌었으면 새 연결을 4002로 닫고 지금 연결을 지킴.
#   창 둘이 4001을 받고도 다시 붙으면 서로 밀어내기를 끝없이 반복하므로 막음. 4002도 다시 붙지 않아야 함
#   열린 회의가 --max-meetings에 닿으면 새 회의 연결은 1013(잠시 뒤 다시)으로 닫음. 입장권 주소로 회의를 알 수 있으면 입장권을 쓰기 전에 거절함
#   작성자 확인은 백엔드 몫이라 하지 않음
# 받는 스레드는 소리와 제어 메시지를 그 회의 Room의 대기열에 순서대로 넣고, Room의 처리 스레드가 받은 순서대로 처리함


def health():
    """상태 확인용 내용과 HTTP 코드. 입력 서버나 로컬 입력 스레드가 죽었으면 503을 내서,
    프로세스는 떠 있는데 전사가 멈춘 상태를 밖에서 알 수 있게 함. 회의 하나의 오류는 그 회의만 닫으므로 503을 내지 않음"""
    ok = bool(PROC['running'])
    body = {'ok': ok, 'input': A.input, 'connected': bool(CONNS), 'connections': len(CONNS), 'draining': draining()}
    if WS_MODE:
        with MLOCK: ms = sorted(MEETINGS); rs = [MEETINGS[k] for k in ms]
        # backlog: 처리 스레드가 아직 못 넣은 소리(초). 계속 늘면 이 서버가 회의 수를 감당하지 못하는 것
        body.update(meetings=ms, maxMeetings=A.max_meetings, full=at_cap(len(ms)),
                    backlog={k: round(r.q.qsize() * 0.1, 1) for k, r in zip(ms, rs)})
        if ROOM_ERR['count']: body.update(roomErrors=ROOM_ERR['count'], lastRoomError=ROOM_ERR['last'])
    elif LOCAL:
        body.update(state=LOCAL.status['state'], meeting=LOCAL.sess and LOCAL.sess['meeting_id'], audioSec=LOCAL.status['audio_sec'])
    if not ok and PROC.get('error'): body['error'] = PROC['error']
    return (200 if ok else 503), json.dumps(body, ensure_ascii=False).encode() + b'\n'


def ws_process_request(conn, request):
    """음성 포트로 오는 평범한 HTTP GET. 상태 확인만 받고, 나머지는 WebSocket 손잡기로 넘김.
    이 포트는 밖으로 열려 있어서(WS_HOST=0.0.0.0) 컨테이너 밖에서도 닿음. 개발 화면 포트는 닫아 둠
    /ready: drain 중이면 503 → ALB가 새 연결을 다른 파드로 보냄 (이미 붙은 연결은 그대로).
    회의가 다 차도 503을 내지 않음: 파드가 하나면 ALB 대상에서 빠져 진행 중인 회의의 다시 연결까지 막힘.
    새 회의는 ws_handler가 1013으로 거절함"""
    path = request.path.split('?')[0]
    if path not in ('/health', '/ready') and not draining(): return None
    from http import HTTPStatus
    from websockets.datastructures import Headers
    from websockets.http11 import Response
    code, body = health()
    if path == '/ready' and draining():
        code, body = 503, b'{"ready":false,"draining":true}\n'
    elif path not in ('/health', '/ready'):
        # Do not consume a one-use ticket on a retiring process.
        code, body = 503, b'{"error":"draining"}\n'
    st = HTTPStatus(code)
    return Response(st.value, st.phrase,
                    Headers([('Content-Type', 'application/json; charset=utf-8'),
                             ('Content-Length', str(len(body)))]), body)


def ws_close(conn, code, reason):
    """연결을 닫음. 닫기는 상대 응답을 기다릴 수 있어 다른 연결을 막지 않게 따로 스레드에서 부름"""
    try: conn.close(code, reason)
    except Exception: pass


WSPATH = re.compile(r'^/ws/stt/meetings/([^/]+)$')


def ws_path_meeting(conn):
    u = urllib.parse.urlsplit(getattr(getattr(conn, 'request', None), 'path', '') or '')
    m = WSPATH.match(u.path)
    return u, (m.group(1) if m else None)


def ws_auth(conn):
    """음성 연결 입장권을 확인함 (CN팀 S1). 이전 연결을 밀어내기 전에 하므로, 틀린 입장권은 열린 회의를 건드리지 못함.
    → 확인한 정보(dict) / 확인하지 않음(None) / 막음(False, 연결을 닫은 뒤)"""
    if A.no_ticket or not A.backend_url: return None
    u, mid = ws_path_meeting(conn)
    tk = urllib.parse.parse_qs(u.query).get('ticket', [''])[0]
    if not mid or not tk:
        log(f'입장권 없는 연결을 거절함 ({u.path or "경로 없음"})')
        conn.close(1008, 'ticket required'); return False
    data, err = consume_ticket(A.backend_url, tk, 3.0)   # 백엔드가 응답하지 못해도 거절 (CN팀 확인)
    if data is None:
        log(f'입장권 확인 실패로 연결을 거절함: {err}')
        conn.close(1008, 'invalid ticket'); return False
    data['pathMeetingId'] = mid
    return data


def reject_full(conn, say, mid):
    log(f'회의가 다 차서({A.max_meetings}개) 회의 {mid} 연결을 거절함')
    try: say(type='error', message=f'STT 서버의 회의 자리가 다 참 ({A.max_meetings}개). 잠시 뒤 다시 연결')
    except Exception: pass
    conn.close(1013, 'meeting capacity full')


def ws_handler(conn):
    from websockets.exceptions import ConnectionClosed
    say = lambda **m: conn.send(json.dumps(m, ensure_ascii=False))
    _, pmid = ws_path_meeting(conn)
    if pmid and full():   # 입장권을 쓰기 전에 거절 (브라우저는 다시 연결할 때 새 입장권을 받아 옴)
        with MLOCK: known = pmid in MEETINGS
        if not known: reject_full(conn, say, pmid); return
    auth = ws_auth(conn)
    if auth is False: return
    room = None
    dropped = [0, 0]   # 밀려난 뒤 버린 소리(바이트)·메시지 수
    with MLOCK: CONNS.add(conn)
    try:   # 등록한 뒤부터는 어떤 오류가 나도 아래 finally에서 정리되어야 함
        try: peer = conn.remote_address   # 상대가 벌써 끊었으면 소켓 주소를 못 읽음
        except OSError: peer = '?'
        log(f'WebSocket 연결: {peer}' + (f' (회의 {pmid})' if pmid else ''))
        for msg in conn:
            if isinstance(msg, bytes):
                if room is None: continue   # start 전 소리는 버림
                with room.clock:   # 주인 확인과 넣기를 한 번에: 사이에 새 연결이 ms·skip을 계산하면 틀어짐
                    if room.conn is not conn or room.meeting is None:
                        if room.conn is not conn: dropped[0] += len(msg)
                        continue
                    if room.skip:                                # 다시 연결 뒤 이미 받은 부분
                        k = min(room.skip, len(msg)); msg = msg[k:]; room.skip -= k
                    if len(msg) % 2: msg = msg[:-1]
                    if msg: room.q.put(msg); room.ms += len(msg) / MS
                continue
            try: m = json.loads(msg)
            except ValueError:
                say(type='error', message='JSON이 아님'); continue
            t = m.get('type')
            if room is not None and room.conn is not conn:   # 밀려난 연결: start로 되찾지도, pause·end로 회의를 건드리지도 못함
                dropped[1] += 1; continue
            if t == 'start':
                mid = str(m.get('meetingId') or '').strip(); off = float(m.get('offsetMs') or 0)
                if not mid.isdigit():   # 백엔드 회의 번호는 숫자뿐 (CN팀 S5). 틀린 번호로 전사가 나가지 않게 연결을 끊음
                    say(type='error', message='meetingId 없음' if not mid else 'meetingId는 숫자여야 함')
                    conn.close(1008, 'meetingId must be numeric'); return
                if auth and not (mid == str(auth['meetingId']) == str(auth['pathMeetingId'])):
                    # 연결 주소·start 메시지·입장권 확인 응답의 meetingId가 모두 같아야 함 (CN팀 S1)
                    log(f'meetingId가 어긋나 연결을 거절함: 주소 {auth["pathMeetingId"]} · start {mid} · 입장권 {auth["meetingId"]}')
                    say(type='error', message='meetingId가 연결 주소·입장권과 다름')
                    conn.close(1008, 'meetingId mismatch'); return
                with MLOCK:
                    r = MEETINGS.get(mid)
                    if r is None:
                        if at_cap(len(MEETINGS)): r = False
                        else:
                            r = MEETINGS[mid] = Room(mid, f'WebSocket ws://{A.ws_host}:{A.ws_port}'); LIVE.add(r)
                            r.thread = threading.Thread(target=r.ws_main, daemon=True); r.thread.start()
                if r is False: reject_full(conn, say, mid); return
                remember(r)
                with r.clock:   # 주인 바꾸기부터 ms·skip 계산까지 한 번에 (밀려난 연결의 받는 스레드가 사이에 끼지 못하게)
                    old, rejected = r.conn, False
                    if old is not None and old is not conn:
                        now = time.monotonic()
                        while r.replaced and now - r.replaced[0] > A.replace_window: r.replaced.popleft()
                        if A.replace_max > 0 and len(r.replaced) >= A.replace_max:
                            rejected = True   # 거절: 지금 연결을 지킴 (거절은 세지 않으므로 조용히 --replace-window초가 지나면 다시 받음)
                        else:
                            r.replaced.append(now)
                    if not rejected:
                        r.conn, r.gone_at = conn, None
                        if mid == r.meeting and off == 0 and r.ms - r.shift >= FRESH_MS:
                            # 새로 연 브라우저(새로고침 등)는 0부터 셈. 겹침으로 보고 버리면 앞서 받은 길이만큼 새 말이 사라지므로,
                            # 버리지 않고 받은 위치 뒤에 이어 붙임. 줄 시각은 서버가 계산하므로 틀어지지 않음
                            # 이때부터 브라우저 시계가 서버보다 r.ms만큼 늦으므로 그 차이(shift)를 기억해, 뒤의 다시 연결에서 offsetMs에 더함
                            # (회의가 이미 정리됐거나 STT가 다시 켜진 뒤의 새로고침은 여기서 막지 못함: 브라우저가 이어서 셀 위치를 보내야 함)
                            fresh = r.ms; gap = 0
                            r.skip = 0; r.shift = r.ms
                        elif mid == r.meeting:                   # 같은 회의로 다시 연결
                            fresh = None
                            gap = off + r.shift - r.ms         # 브라우저 시계를 서버 시계로 바꿔 비교
                            if gap > 0:                        # 빈 시간은 무음으로 채워 시각을 맞춤
                                z = bytes(int(gap) * MS)
                                for i in range(0, len(z), 3200): r.q.put(z[i:i + 3200])
                                r.ms += int(gap)
                            elif gap < 0: r.skip = int(-gap) * MS
                        else:
                            fresh = None; gap = 0
                            r.meeting, r.ms, r.skip, r.shift = mid, off, 0, 0.0
                            r.q.put(('start', mid, off / 1000))
                        ready_ms = round(r.ms - r.shift)   # 브라우저 시계 기준
                if rejected:
                    log(f'{r.tag}{A.replace_window:.0f}초 동안 연결이 {A.replace_max}번 바뀌어 새 연결을 거절함 '
                        f'(같은 회의를 연 창이 둘 이상인지 확인)')
                    try: say(type='error', message='같은 회의의 연결이 너무 자주 바뀜. 다른 창에서 이 회의를 쓰는 중인지 확인')
                    except Exception: pass
                    conn.close(4002, 'replaced too often'); return
                if room is not None and room is not r:   # 같은 연결이 다른 회의로 start: 앞 회의에서 떼기만 함 (정리 시간이 지나면 정리)
                    with room.clock:
                        if room.conn is conn: room.conn = None; room.gone_at = time.monotonic()
                room = r
                if old is not None and old is not conn:
                    log(f'{room.tag}WebSocket 새 연결이 이전 연결을 대신함')
                    threading.Thread(target=ws_close, args=(old, 4001, 'replaced by a new connection'), daemon=True).start()
                if fresh is not None:
                    log(f'{room.tag}offsetMs 0으로 다시 연결 (받은 소리 {fresh / 1000:.1f}초): 새로 연 브라우저로 보고 이어 붙임')
                elif gap > 10000: log(f'{room.tag}다시 연결: {gap / 1000:.1f}초가 비어 무음으로 채움')
                say(type='ready', meetingId=mid, meetingMs=ready_ms)
            elif t in ('pause', 'resume'):
                if room is not None: room.q.put(t)
            elif t == 'end':
                if room is not None:
                    with MLOCK:   # 바로 자리를 비움: 같은 회의로 다시 start하면 새 Room을 엶
                        if MEETINGS.get(room.key) is room: del MEETINGS[room.key]
                    with room.clock:
                        room.q.put('complete'); room.meeting = None
                        if room.conn is conn: room.conn = None
                    room = None
                say(type='ended')
            else:
                say(type='error', message=f'모르는 type: {t}')
    except ConnectionClosed:
        pass
    finally:
        with MLOCK: CONNS.discard(conn)
        if room is not None:
            with room.clock:
                mine = room.conn is conn   # 이미 다른 연결이 들어왔으면 그쪽을 지우지 않음
                if mine: room.conn = None; room.gone_at = time.monotonic()
            if mine: log(f'{room.tag}WebSocket 연결 끊김 (회의는 열린 채 다시 연결을 기다림, 최대 {A.idle_release:.0f}초)')
            if dropped[0] or dropped[1]:
                log(f'{room.tag}밀려난 연결에서 온 소리 {dropped[0] / MS / 1000:.1f}초·메시지 {dropped[1]}개를 버림')


def reaper():
    """5초마다: 연결 없이 --idle-release초가 지난 회의를 정리함 (자리를 바로 비우고, 처리 스레드가 남은 줄을 보냄).
    회의의 밀린 소리가 --backlog-warn초를 넘거나 다시 1초 밑으로 내려오면 로그에 남김 (몇 개부터 밀리는지 로그로 보기 위함)"""
    while not stop.wait(5.0):
        now = time.monotonic()
        with MLOCK:
            for k, r in list(MEETINGS.items()):
                if r.conn is None and r.gone_at is not None and now - r.gone_at >= A.idle_release:
                    del MEETINGS[k]; r.q.put('release')
            rooms, n = list(MEETINGS.values()), len(MEETINGS)
        if A.backlog_warn <= 0: continue
        for r in rooms:
            b = r.q.qsize() * 0.1
            if not r.backlog_warned and b >= A.backlog_warn:
                r.backlog_warned = True
                log(f'{r.tag}밀림: 처리하지 못한 소리 {b:.1f}초 (열린 회의 {n}개). CPU가 회의 수를 따라가지 못함')
            elif r.backlog_warned and b < 1.0:
                r.backlog_warned = False
                log(f'{r.tag}밀림 풀림: 처리하지 못한 소리 {b:.1f}초 (열린 회의 {n}개)')


# ---- 마이크·wav 입력 ----
class Diag:
    """처리 스레드가 시간을 어디에 쓰는지 셈 (멈춘 동안은 세지 않음).
    --diag: 30초마다 출력. 없으면 최근 30초에 빠진 소리가 1% 이상이거나 입력 넘침이 새로 생겼을 때만 경고 한 줄"""
    def __init__(self, q=None, flags=None, verbose=False, tag=''):
        self.q, self.flags, self.verbose, self.tag = q, flags or {}, verbose, tag
        self.last = self.t_rep = time.perf_counter()
        self.wall = self.got = self.read = self.feed = self.pub = self.rec = 0.0; self.n = self.instant = 0; self.maxq = 0
        self.prev = (0.0, 0.0, 0)   # 지난 출력 때의 (실행 시간, 받은 소리, 입력 넘침)

    def tick(self, running):
        now = time.perf_counter()
        if running: self.wall += now - self.last
        self.last = now
        if self.q is not None: self.maxq = max(self.maxq, self.q.qsize())
        if now - self.t_rep >= 30 and self.wall > 0:
            self.t_rep = now
            lost = (1 - self.got / self.wall) * 100
            over = sum(self.flags.values())
            dw, dg = self.wall - self.prev[0], self.got - self.prev[1]
            recent = (1 - dg / dw) * 100 if dw > 1 else 0.0
            bad = recent >= 1.0 or over > self.prev[2]
            self.prev = (self.wall, self.got, over)
            if not self.verbose:
                if bad:
                    log(f'{self.tag}[경고] 마이크 소리가 빠지고 있음: 최근 {dw:.0f}초 중 {recent:.1f}% 빠짐, 입력 넘침 누적 {over}번 '
                        f'(전체 {lost:.1f}%). 자세히 보려면 --diag')
                return
            msg = (f'{self.tag}[진단] 실행 {self.wall:.0f}초 · 받은 소리 {self.got:.0f}초 (빠짐 {lost:.1f}%) · 읽기 대기 {self.read:.1f}초 '
                   f'(바로 읽힘 {self.instant / max(self.n, 1) * 100:.0f}%) · 엔진 {self.feed - self.pub:.1f}초 · 알림·저장 {self.pub:.1f}초 · 녹음 쓰기 {self.rec:.1f}초')
            if self.q is not None: msg += f' · 대기열 최대 {self.maxq * 0.1:.1f}초'
            if self.flags: msg += ' · ' + ', '.join(f'{k} {v}' for k, v in self.flags.items())
            log(msg)


def local_main(room):
    """마이크·wav 입력의 처리 스레드. 이 스레드가 죽으면 서버 전체가 전사를 못 하므로 /health 503"""
    stream = None
    try:
        sdq = None; flags = {}
        if A.wav:
            w = wave.open(A.wav); assert w.getframerate() == SR and w.getnchannels() == 1 and w.getsampwidth() == 2
            read = lambda: w.readframes(1600)
        else:
            # 운영체제 오디오(PortAudio)가 자기 스레드에서 0.1초 조각을 대기열에 넣음 → 처리가 잠깐 늦어도 입력이 버려지지 않음
            import sounddevice as sd
            dev = int(A.device) if A.device.isdigit() else (A.device or None)
            sdq = queue.Queue(); flags = {'입력 넘침': 0}
            def cb(data, frames, t, st):
                if st.input_overflow: flags['입력 넘침'] += 1
                sdq.put(bytes(data))
            stream = sd.RawInputStream(samplerate=SR, channels=1, dtype='int16', blocksize=1600, device=dev, callback=cb)
            room.status['source'] = f'마이크 {sd.query_devices(stream.device)["name"]}'
            stream.start()
            def read():
                while not stop.is_set():
                    try: return sdq.get(timeout=0.5)
                    except queue.Empty:
                        if not stream.active: raise RuntimeError('마이크 입력이 멈춤 (장치 연결을 확인)')
                return b''
        # 마이크 입력은 항상 지켜봄 (문제 있을 때만 경고). wav는 --diag일 때만
        D = Diag(sdq, flags, verbose=A.diag) if (A.diag or not A.wav) else None
        room.loop(read, D)
        room.status['running'] = False
        room.publish({'type': 'status', 'running': False})
    except Exception as e:  # 스레드 안 오류는 화면과 터미널에 알림
        log(f'입력/엔진 오류: {e}')
        PROC['running'] = False; PROC['error'] = str(e)
        room.status['running'] = False; room.status['error'] = str(e)
        room.publish({'type': 'status', 'running': False, 'error': str(e)})
    finally:
        if stream: stream.stop(); stream.close()


# ---- 웹 (개발 화면) ----
# 어느 회의를 볼지는 주소 뒤 ?m=회의번호 (--input ws). 없으면 가장 최근에 시작한 회의. 마이크·wav 입력은 회의가 하나뿐
class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def _room(self):
        u = urllib.parse.urlsplit(self.path)
        m = urllib.parse.parse_qs(u.query).get('m', [''])[0]
        if LOCAL: return LOCAL
        with MLOCK:
            if m: return MEETINGS.get(m) or RECENT.get(m)
        return latest_room()

    def _send(self, code, body, ctype='application/json; charset=utf-8'):
        self.send_response(code); self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', str(len(body))); self.end_headers(); self.wfile.write(body)

    def do_GET(self):
        path = urllib.parse.urlsplit(self.path).path
        if path == '/sender':   # 시험용 보내는 화면 (마이크 → WebSocket, --input ws와 함께 씀)
            body = open(os.path.join(os.path.dirname(STATIC), '..', 'tools', 'mic_sender.html'), 'rb').read()
            cfg = json.dumps({'wsPort': A.ws_port, 'backendUrl': A.backend_url.rstrip('/')}).encode()
            body = re.sub(rb'/\*CFG\*/.*?/\*CFG\*/', lambda m: cfg, body, count=1)   # 화면이 실제 포트·자막 주소를 쓰게
            self._send(200, body, 'text/html; charset=utf-8')
        elif path == '/health':   # 음성 포트에도 같은 내용이 있음 (컨테이너 밖에서는 그쪽으로)
            self._send(*health())
        elif path in ('/', '/index.html'):
            self._send(200, open(os.path.join(STATIC, 'index.html'), 'rb').read(), 'text/html; charset=utf-8')
        elif path == '/events':
            self.send_response(200); self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Cache-Control', 'no-cache'); self.end_headers()
            q = queue.Queue(); room = None
            idle = {'source': f'WebSocket ws://{A.ws_host}:{A.ws_port} (회의 없음)', 'audio_sec': 0.0, 'running': True,
                    'state': 'idle', 'rec': False, 'rec_sec': 0.0, 'session': ''}
            try:
                while room is None:   # 회의가 아직 없으면 기다렸다가 처음 열린 회의를 보여 줌
                    room = self._room()
                    if room is None:
                        self.wfile.write(f"event: tick\ndata: {json.dumps(idle, ensure_ascii=False)}\n\n".encode()); self.wfile.flush()
                        time.sleep(1.0)
                        continue
                    with room.lock:
                        hist = sorted((e for e in room.lines.values() if not e.get('deleted')), key=lambda e: e['start']) + ([room.partial] if room.partial else [])
                        hist.append({'type': 'send_init', 'history': list(room.backend.history), 'snap': room.backend.snapshot()})
                        room.clients.append(q)
                    self.wfile.write(f"event: hello\ndata: {json.dumps(room.status, ensure_ascii=False)}\n\n".encode())
                    for ev in hist: self.wfile.write(f"data: {json.dumps(ev, ensure_ascii=False)}\n\n".encode())
                    self.wfile.flush()
                while True:
                    try: ev = q.get(timeout=1.0)
                    except queue.Empty:
                        self.wfile.write(f"event: tick\ndata: {json.dumps(room.status)}\n\n".encode()); self.wfile.flush(); continue
                    self.wfile.write(f"data: {json.dumps(ev, ensure_ascii=False)}\n\n".encode()); self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                if room is not None:
                    with room.lock:
                        if q in room.clients: room.clients.remove(q)
        else:
            self.send_response(404); self.end_headers()

    def _json(self):
        try:
            n = int(self.headers.get('Content-Length') or 0)
            return json.loads(self.rfile.read(n)) if 0 < n <= 20000 else None
        except (ValueError, json.JSONDecodeError):
            return None

    def _code(self, code):
        self.send_response(code); self.end_headers()

    def do_POST(self):
        path = urllib.parse.urlsplit(self.path).path
        # 사용자 지정 헤더를 요구해, 다른 웹사이트가 몰래 보내는 요청(브라우저가 사전 확인으로 막음)은 받지 않음
        if self.headers.get('X-Meetingnote') != path[1:] or path not in ('/clear', '/speakers', '/merge', '/control', '/delete', '/add', '/edit'):
            return self._code(403)
        room = self._room()
        if room is None: return self._code(404)
        if path == '/clear':
            room.publish({'type': 'clear'})
            self._code(204)
        elif path in ('/speakers', '/merge'):
            d = self._json()
            eng = room.engine; diar = eng.diar if eng else None
            if d is None or not diar: return self._code(400 if d is None else 409)
            try:
                if path == '/speakers':   # 참석자 수 (0이면 자동)
                    n = int(d['max'])
                    if not 0 <= n <= 50: raise ValueError
                    with diar.lock: diar.max = n
                    room.status['max_speakers'] = n
                    room.publish({'type': 'status', 'max_speakers': n})
                else:                          # 화자 합치기
                    src, dst = int(d['src']), int(d['dst'])
                    if not diar.merge(src, dst): raise ValueError
                    room.publish({'type': 'merge', 'src': src, 'dst': dst})
            except (ValueError, KeyError, TypeError):
                return self._code(400)
            self._code(204)
        elif path == '/control':
            d = self._json()
            if not d or d.get('action') not in ('record', 'pause', 'resume', 'complete'): return self._code(400)
            room.ctrl.put(d['action']); self._code(204)
        elif path == '/delete':
            d = self._json()
            try: i = int(d['id'])
            except (TypeError, KeyError, ValueError): return self._code(400)
            with room.lock: ok = i in room.lines and not room.lines[i].get('deleted')
            if not ok: return self._code(404)
            room.publish({'type': 'delete', 'id': i})
            self._code(204)
        elif path == '/add':
            # STT가 아예 못 잡은 말을 사용자가 직접 추가. 시각은 화면이 정한 위치(앞뒤 줄 사이)의 근사치
            d = self._json()
            try:
                start, text = float(d['start']), str(d['text']).strip()
                spk = None if d.get('spk') in (None, '') else int(d['spk'])
                if not text or len(text) > 2000 or start < 0: raise ValueError
            except (ValueError, KeyError, TypeError):
                return self._code(400)
            with room.lock: room.manual_seq += 1; i = 1_000_000 + room.manual_seq   # 엔진 번호와 겹치지 않게
            room.publish({'type': 'manual', 'id': i, 'start': round(start, 2), 'end': round(start, 2), 'text': text,
                          'conf': None, 'spk': spk, 'spk_info': None, 'kind': line_kind(text)})
            self._code(204)
        elif path == '/edit':
            try:
                n = int(self.headers.get('Content-Length') or 0)
                if n > 20000: raise ValueError('too large')
                d = json.loads(self.rfile.read(n))
                i, text, src = int(d['id']), str(d['text']).strip(), d.get('source', 'user')
                if not text or len(text) > 2000 or src not in ('user', 'alt'): raise ValueError('bad text')
            except (ValueError, KeyError, TypeError, json.JSONDecodeError):
                return self._code(400)
            with room.lock: ok = i in room.lines
            if not ok: return self._code(404)
            room.publish({'type': 'edit', 'id': i, 'text': text, 'source': src})
            self._code(204)


STOP_BY = ['']   # 끈 신호: 'SIGTERM'이면 회의 end 알림을 보내지 않음


def on_sigterm(signum, frame):
    """docker stop·쿠버네티스가 파드를 내릴 때 오는 SIGTERM. Ctrl+C와 같은 정리 경로를 탐.
    처리 함수를 걸어야 컨테이너 1번 프로세스로 돌 때도 SIGTERM을 받음 (걸지 않으면 무시되어 SIGKILL까지 기다림)"""
    STOP_BY[0] = 'SIGTERM'
    raise KeyboardInterrupt


def finish_room(room, term):
    """끌 때 Room 하나 정리: 녹음 닫기, 남은 줄·알림 보내기, 파일 저장"""
    with room.lock: room.rec_close(); ending = bool(room.sess and not room.sess['complete'])
    # 백엔드 전송은 lock 밖에서 함. 전송 스레드가 sending을 잡은 채 화면 알림(publish)으로 lock을 기다리므로,
    # lock을 잡고 보내면 서로 기다려 멈춤 (백엔드가 응답하지 못할 때 드러남)
    if ending and not term:
        room.backend.event('end', wait_lines=2.0)   # Ctrl+C: 완료를 누르지 않고 끈 경우 회의를 끝냄
    else:
        # SIGTERM: 파드를 내리는 것이지 회의가 끝난 것이 아님. 새 STT에 브라우저가 offsetMs로 이어 붙으므로 end를 보내지 않음
        room.backend.drain(wait=2.0)
        if ending: print(f'{room.tag}회의는 끝내지 않음 (end 알림을 보내지 않음)', flush=True)
    with room.lock:
        if room.sess:
            room.save_session()
            print(f'{room.tag}전사문 저장: {room.sess["file"]}' + (f'  녹음: {room.sess["wav_path"]}' if room.sess['wav_path'] else ''), flush=True)


def main():
    global SHARED, LOCAL
    signal.signal(signal.SIGTERM, on_sigterm)
    SHARED = Shared(model_dir=f'{A.models}/zipformer-174m', vad_model=f'{A.models}/speaker/silero_vad.onnx', chunk=A.chunk,
                    hotwords=A.hotwords, hotwords_score=A.hotwords_score, spk_model=f'{A.models}/speaker/nemo_en_titanet_small.onnx',
                    diar=not A.no_diar, alt=not A.no_alt, max_len=A.max_len, blank_penalty=A.blank_penalty)
    wss = th = None
    if WS_MODE:
        from websockets.sync.server import serve
        wss = serve(ws_handler, A.ws_host, A.ws_port, max_size=2 ** 20, ping_interval=5, ping_timeout=10,
                    process_request=ws_process_request)   # 같은 포트로 GET /health·/ready도 받음
        # ping 5초 간격·10초 대기 → 끊김을 최대 15초 안에 알아차림 (websockets 기본값은 20초·20초)

        def serve_ws():
            try: wss.serve_forever()
            except Exception as e:
                if not stop.is_set(): log(f'WebSocket 서버 오류: {e}'); PROC['running'] = False; PROC['error'] = str(e)
        threading.Thread(target=serve_ws, daemon=True).start()
        threading.Thread(target=reaper, daemon=True).start()
        source = f'WebSocket ws://{A.ws_host}:{A.ws_port}'
        log(f'WebSocket 입력 대기: {source}  동시 회의 {f"최대 {A.max_meetings}개" if A.max_meetings > 0 else "상한 없음"}  시험용 보내는 화면: http://localhost:{A.port}/sender')
    else:
        source = A.wav or f'마이크 {A.device or "기본"} ({A.input})'
        LOCAL = Room('local', source); remember(LOCAL)
        th = threading.Thread(target=local_main, args=(LOCAL,), daemon=True); th.start()
    srv = ThreadingHTTPServer((A.host, A.port), H); srv.daemon_threads = True
    if ENV_LOADED: print(f'설정 파일: {ENV_LOADED}', flush=True)
    print(f'http://localhost:{A.port}  입력: {source}  (Ctrl+C로 종료)'
          + (f'  접속 범위: {A.host}' if A.host != '127.0.0.1' else ''), flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        # 정리(마이크 끄기·저장)는 최대 약 10초. 그 사이 Ctrl+C·SIGTERM이 또 오면 저장이 끊기므로 무시함 (강제 종료는 맥 Ctrl+\, 윈도우는 터미널 창 닫기)
        signal.signal(signal.SIGINT, signal.SIG_IGN); signal.signal(signal.SIGTERM, signal.SIG_IGN)
        term = STOP_BY[0] == 'SIGTERM'
        t_stop = time.monotonic()
        print(f'\n끄는 중{" (SIGTERM)" if term else ""}… 녹음·전사 파일을 저장하고 있으니 잠시 기다려 주세요 (최대 약 10초)', flush=True)
        stop.set()
        if wss: wss.shutdown()   # 새 연결을 더 받지 않음
        with MLOCK: rooms = [LOCAL] if LOCAL else list(LIVE)
        for r in rooms:   # 소리 처리를 멈추고 말하던 줄을 확정함 (eng.finish). 모두 합쳐 5초까지
            t = th if r is LOCAL else r.thread
            if t: t.join(timeout=max(0.1, 5 - (time.monotonic() - t_stop)))
        jobs = [threading.Thread(target=finish_room, args=(r, term)) for r in rooms]   # 회의마다 동시에 남은 줄을 보냄
        for j in jobs: j.start()
        for j in jobs: j.join(timeout=4)
        srv.server_close()
        print(f'정리 {time.monotonic() - t_stop:.1f}초', flush=True)


if __name__ == '__main__':
    main()
