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

요약 쪽 전송 (summary_sender.py, 2026-10-14 회의에서 정한 형식)
  세션 = 회의. 세션마다 meeting_id를 붙이고, 확정 줄을 --send-period초(기본 10)마다 묶어 보냄. 시작·멈춤·재개·끝 알림은 바로 보냄
  --summary-url이 없으면 세션 파일 옆 <이름>.outbox.jsonl 에만 기록

브라우저 입력 (--input ws, CN팀 2장 2.1 검토 확정)
  회의록 작성자 브라우저가 16kHz 모노 16비트 PCM 100ms 조각과 start·pause·resume·end(JSON)를 WebSocket으로 보냄 (규약은 ws_handler 위 설명)
  서버를 켜면 대기하다가 start를 받으면 그 meetingId로 세션을 엶. 시험용 보내는 화면: http://localhost:8765/sender
  실행: python app/server.py --input ws --backend-url http://127.0.0.1:8767

백엔드 전송 (backend_sender.py, CN팀 2장 2.2 + AI팀 추가 필드, 2026-10-15 확정)
  말하는 중·확정·보완 줄을 --backend-period초(기본 0.5)마다 바뀐 것만 POST {--backend-url}/internal/meetings/{meeting_id}/transcripts
  화면에서 고치거나 지운 것은 보내지 않음 (정식 서비스에서는 백엔드가 처리). 발화자 구분은 하지 않아 화자 번호는 보내지 않음
  --backend-url이 없으면 세션 파일 옆 <이름>.backend.jsonl 에만 기록
"""
import argparse, datetime, json, os, queue, re, signal, sys, threading, time, wave
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from envfile import load_env  # noqa: E402
ENV_LOADED = load_env()   # engine의 모델 폴더 기본값(NINEOUTING_STT_MODELS)도 .env를 따르도록 engine보다 먼저
from engine import Engine, SR, DEFAULT_MODELS, DEFAULTS, line_kind  # noqa: E402
from summary_sender import Sender  # noqa: E402
from backend_sender import BackendSender  # noqa: E402

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
p.add_argument('--summary-url', default='', help='요약 쪽 받는 주소 (예: http://localhost:8000/stt). 없으면 outbox 파일에만 기록')
p.add_argument('--send-period', type=float, default=10.0, help='요약 쪽 전송 주기(초)')
p.add_argument('--backend-url', default=os.environ.get('BACKEND_URL', ''),
               help='백엔드 주소 (.env BACKEND_URL, 예: http://meeting-service:8080). 없으면 backend.jsonl 파일에만 기록')
p.add_argument('--backend-period', type=float, default=0.5, help='백엔드 전송 주기(초). 그사이 바뀐 줄만 보냄')
p.add_argument('--meeting-id', default='', help='첫 회의의 meeting_id (앱이 줄 값. 없으면 자동)')
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

# ---- 이벤트 저장·배포 ----
lock = threading.RLock()   # 요약 전송 알림이 publish 안에서 다시 publish를 부를 수 있어 재진입 허용
lines = {}          # id → 확정·보완 줄 (새로 접속한 화면에 보냄)
partial = None      # 지금 말하는 중인 줄
clients = []        # 화면별 큐
status = {'source': A.wav or (f'WebSocket ws://{A.ws_host}:{A.ws_port}' if A.input == 'ws' else f'마이크 {A.device or "기본"} ({A.input})'), 'audio_sec': 0.0, 'running': True, 'max_speakers': A.max_speakers,
          'state': 'run', 'rec': False, 'rec_sec': 0.0, 'session': ''}   # state: run(전사 중) / pause(일시정지) / idle(완료 후 대기)
ENGINE = None       # 화면에서 화자 합치기·참석자 수를 바꿀 때 씀
manual_seq = 0      # 직접 추가한 줄 번호
SENDER = Sender(A.summary_url, A.send_period, now=lambda: ENGINE.fed if ENGINE else 0.0,
                log=lambda m: print(m, file=sys.stderr, flush=True), on_update=lambda ev: publish(ev))
BACKEND = BackendSender(A.backend_url, A.backend_period, log=lambda m: print(m, file=sys.stderr, flush=True))


clear_at = -1.0     # 마지막으로 지운 오디오 시각. 이보다 앞에서 시작한 보완 줄은 나중에 도착해도 버림
clear_id = 0        # 지울 때의 엔진 줄 번호. 이 번호 이하의 말하던·확정 줄은 지우기 전 줄이라 버림
                    # (인식기는 글자로 안 바꾼 소리를 약 1.2초 들고 있어서, 지운 뒤 새 줄의 시작 시각이 지운 시각보다 조금 앞설 수 있음 → 시각으로 거르면 지운 뒤 말까지 버려짐)


def publish(ev):
    global partial, clear_at, clear_id
    with lock:
        if (ev['type'] in ('partial', 'final') and ev['id'] <= clear_id) or (ev['type'] == 'recovered' and ev['start'] < clear_at):
            return   # 지우기 전 줄 (말하던 중이던 줄, 판단 대기 중이던 보완)
        if ev['type'] == 'partial':
            partial = ev
            if not CATCHUP[0]: BACKEND.partial(ev)   # 몰아 보내는 동안은 확정 줄만 보냄
        elif ev['type'] in ('send', 'send_pending'):   # 요약 전송 상태창. 보낸 줄·수정에 묶음 번호를 붙여 둠 (새로 접속한 화면용)
            m = ev.get('entry', {}).get('msg', {})
            for x in m.get('lines', []):
                if x['id'] in lines: lines[x['id']]['sent_seq'] = m['seq']
            for x in m.get('edits', []):
                if x['id'] in lines: lines[x['id']]['edit_sent_seq'] = m['seq']
            for q in clients: q.put(ev)
            return
        elif ev['type'] == 'status':
            pass
        elif ev['type'] == 'alt':  # 이미 있는 줄에 다른 인식 결과를 붙임 (새로 접속한 화면에도 보내기 위해)
            if ev['id'] not in lines: return   # 지운 줄에 늦게 온 결과는 버림
            lines[ev['id']]['alts'] = ev['alts']
        elif ev['type'] == 'clear':  # 전사 기록 지우기 (모든 화면에 알림)
            if not ev.get('session_switch'):   # 같은 회의 안에서 지우면 요약 쪽에도 삭제로 알림 (새 회의로 넘어갈 때는 알리지 않음)
                for ln in lines.values():
                    if not ln.get('deleted'): SENDER.edit(ln['id'], ln.get('rev', 0) + 1, deleted=True)
            lines.clear(); partial = None; clear_at = status['audio_sec']; clear_id = ENGINE.next_id if ENGINE else 0
            if ENGINE: ENGINE.restart = True   # 지운 뒤에 한 말이 지우기 전 줄에 이어 붙어 버려지지 않게
        elif ev['type'] == 'merge':  # 화자 src를 dst로 합침: 지난 줄 번호를 모두 바꿈
            for ln in lines.values():
                if ln.get('spk') == ev['src']:
                    ln['spk'] = ev['dst']; ln['merged_from'] = ev['src']
            if partial and partial.get('spk') == ev['src']: partial['spk'] = ev['dst']
        elif ev['type'] == 'edit':  # 사용자가 후보를 고르거나 직접 고침. 처음 텍스트는 orig로 남겨 되돌릴 수 있게 함
            ln = lines.get(ev['id'])
            if not ln or ln.get('deleted'): return
            if 'orig' not in ln: ln['orig'], ln['orig_conf'] = ln['text'], ln.get('conf')
            if ev['text'] == ln['orig']:   # 원래대로 되돌림
                ln['text'], ln['conf'] = ln['orig'], ln['orig_conf']; ln.pop('edited', None)
            else:
                ln['text'], ln['conf'], ln['edited'] = ev['text'], None, ev['source']
            ln['kind'] = line_kind(ln['text'])   # 고친 텍스트로 맞장구·간투사 다시 판별
            ln['rev'] = ln.get('rev', 0) + 1
            SENDER.edit(ln['id'], ln['rev'], text=ln['text'], kind=ln['kind'])
            ev = {'type': 'edit', 'id': ev['id'], 'text': ln['text'], 'conf': ln['conf'],
                  'edited': ln.get('edited'), 'orig': ln['orig'], 'kind': ln['kind']}
        elif ev['type'] == 'delete':  # 사용자가 줄을 지움. 파일에는 deleted로 남기고 화면·요약 쪽에서는 뺌
            ln = lines.get(ev['id'])
            if not ln or ln.get('deleted'): return
            ln['deleted'] = True; ln['rev'] = ln.get('rev', 0) + 1
            SENDER.edit(ln['id'], ln['rev'], deleted=True)
        else:
            lines[ev['id']] = ev
            if partial and partial['id'] == ev['id']: partial = None
            if ev['type'] in ('final', 'recovered', 'manual'): SENDER.line(ev)
            if ev['type'] in ('final', 'recovered'): BACKEND.line(ev)   # 직접 추가한 줄은 백엔드에서 생기므로 보내지 않음
        for q in clients: q.put(ev)
        if ev['type'] not in ('partial', 'status'): save_session()


# ---- 세션 (자동 저장·녹음) ----
SESS = None        # 지금 세션: file, wav_path, wav(쓰는 중인 wave 객체), wav_t0, t0, started, complete
CTRL = queue.Queue()   # 화면의 녹음·일시정지·완료 요청. 소리 입력 스레드가 소리 조각 사이에 처리함


def new_session(t0, path=None, mid=None):
    global SESS
    os.makedirs(A.save_dir, exist_ok=True)
    base = path or os.path.join(A.save_dir, datetime.datetime.now().strftime('%Y%m%d_%H%M%S') + '.jsonl')
    os.makedirs(os.path.dirname(os.path.abspath(base)), exist_ok=True)
    mid = mid or (A.meeting_id if A.meeting_id and SESS is None else '') or 'm_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    SESS = {'file': base, 'wav_path': None, 'wav': None, 'wav_t0': None, 't0': round(t0, 2), 'meeting_id': mid,
            'started': datetime.datetime.now().isoformat(timespec='seconds'), 'complete': False}
    SENDER.start(mid, t0, os.path.splitext(base)[0] + '.outbox.jsonl')
    BACKEND.start(mid, t0, os.path.splitext(base)[0] + '.backend.jsonl')
    status['session'] = os.path.basename(base); status['rec'] = False; status['rec_sec'] = 0.0


def save_session():
    """지금 세션의 줄을 파일에 통째로 다시 씀 (lock 안에서 호출). 첫 줄은 _meta"""
    if not SESS: return
    meta = {k: SESS[k] for k in ('meeting_id', 'started', 't0', 'wav_t0', 'complete')}
    if SESS.get('gaps'): meta['gaps'] = SESS['gaps']   # --input ws: 멈춤 때 넣은 무음 [엔진 시각, 길이]. 회의 시각 = start - t0 - (그 앞 gaps 합)
    meta.update(source=status['source'], wav=SESS['wav_path'] and os.path.basename(SESS['wav_path']),
                note='start·end는 서버 시작 뒤 엔진에 넣은 소리 기준 초. wav 안의 시각 = start - wav_t0')
    tmp = SESS['file'] + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        f.write(json.dumps({'_meta': meta}, ensure_ascii=False) + '\n')
        for ev in sorted(lines.values(), key=lambda e: e['start']):
            f.write(json.dumps({k: ev.get(k) for k in ('id', 'start', 'end', 'spk', 'spk_info', 'text', 'type', 'kind', 'conf',
                                                       'alts', 'edited', 'orig', 'orig_conf', 'merged_from', 'rev', 'deleted')},
                               ensure_ascii=False) + '\n')
    os.replace(tmp, SESS['file'])   # 쓰는 도중 꺼져도 이전 파일은 남음


def rec_write(c):
    if SESS and SESS['wav']:
        SESS['wav'].writeframes((np.clip(c, -1, 1) * 32767).astype(np.int16).tobytes())
        status['rec_sec'] = round(status['rec_sec'] + len(c) / SR, 1)


def rec_close():
    if SESS and SESS['wav']: SESS['wav'].close(); SESS['wav'] = None


def control(eng, act):
    """소리 입력 스레드에서만 호출. 상태에 맞지 않는 요청은 무시"""
    st = status['state']
    def flush():   # 말하던 줄 확정·보완 판단을 위해 무음을 넣음 (녹음에도 같이 넣어 시각을 맞춤)
        z = np.zeros(1600, np.float32)
        for _ in range(25): eng.feed(z); rec_write(z)
        status['audio_sec'] = round(eng.fed, 1)
    if isinstance(act, tuple) and act[0] == 'start':   # --input ws: 브라우저가 회의 시작을 알림 (meetingId, 회의 시작 기준 음성 초)
        _, mid, off = act
        if st in ('run', 'pause'): control(eng, 'complete')   # 다른 회의가 열려 있으면 닫음
        with lock: new_session(eng.fed - off, mid=mid)          # 줄 시각 = 회의 시작 기준 음성 시간이 되도록 기준점을 맞춤
        publish({'type': 'clear', 'session_switch': True})
        status['state'] = 'run'
        with lock: save_session()
        publish({'type': 'status', **{k: status[k] for k in ('state', 'rec', 'rec_sec', 'session', 'audio_sec')}})
        return
    if act == 'record' and st == 'idle':      # 새 세션: 화면을 비우고 새 파일로
        with lock: new_session(eng.fed)   # 먼저 새 세션으로 바꿔야 아래 clear가 닫은 세션 파일을 비우지 않음
        publish({'type': 'clear', 'session_switch': True})
        st = status['state'] = 'run'
    if act == 'record' and st in ('run', 'pause') and not SESS['wav']:
        a0 = max(SESS['t0'], eng.fed - 120); pre = eng._audio(a0, eng.fed) if eng.fed > a0 else np.zeros(0, np.float32)
        SESS['wav_path'] = os.path.splitext(SESS['file'])[0] + '.wav'
        w = wave.open(SESS['wav_path'], 'wb'); w.setnchannels(1); w.setsampwidth(2); w.setframerate(SR)
        SESS['wav'] = w; SESS['wav_t0'] = round(eng.fed - len(pre) / SR, 2); status['rec'] = True; status['rec_sec'] = 0.0
        rec_write(pre)
    elif act == 'pause' and st == 'run':
        at = eng.fed; flush()
        if A.input == 'ws' and not A.wav:   # 브라우저가 세는 회의 시간에는 이 무음이 없음 → 전송 시각에서 뺌
            for s in (SENDER, BACKEND): s.gap(at, eng.fed - at)
            SESS.setdefault('gaps', []).append([round(at, 2), round(eng.fed - at, 2)])
        status['state'] = 'pause'; SENDER.event('pause'); BACKEND.flush()
    elif act == 'resume' and st == 'pause':
        status['state'] = 'run'; SENDER.event('resume')
    elif act == 'complete' and st in ('run', 'pause'):
        if st == 'run': flush()
        rec_close(); status['state'] = 'idle'; status['rec'] = False; SENDER.event('end'); BACKEND.flush()
        with lock: SESS['complete'] = True; save_session()
    else:
        return
    with lock: save_session()
    publish({'type': 'status', **{k: status[k] for k in ('state', 'rec', 'rec_sec', 'session', 'audio_sec')}})


# ---- WebSocket 입력 (--input ws) ----
# 브라우저(회의록 작성자)가 보내는 소리를 받음. 규약은 CN팀 2장 2.1 검토 (2026-10-15 확정, 메시지 이름은 AI팀이 정함)
#   바이너리: 16kHz 모노 16비트 PCM(little-endian), 100ms = 3,200바이트
#   텍스트(JSON): {"type":"start","meetingId":"…","offsetMs":0} / {"type":"pause"} / {"type":"resume"} / {"type":"end"}
#     start의 offsetMs = 이어서 보낼 소리가 회의 시작 기준 몇 ms부터인지. 같은 회의로 다시 연결하면 겹친 만큼 버리고 빈 만큼 무음으로 채움
#   서버 → 브라우저: {"type":"ready","meetingId","meetingMs"} (meetingMs = 서버가 받은 그 회의 소리 길이), {"type":"ended"}, {"type":"error","message"}
# 연결은 한 번에 하나. 새 연결이 오면 이전 연결을 닫고 이어받음 (CN팀 S6, 2026-10-20)
#   끊긴 것을 서버가 아직 모르는 사이에 브라우저가 다시 붙어도 막히지 않게 함. 자리를 내준 연결은 4001로 닫아
#   브라우저가 "내가 밀려남"과 "그냥 끊김"을 구분할 수 있게 함
#   연결 시점에는 어느 회의인지 알 수 없어(meetingId는 뒤이어 오는 start에 들어 있음) 회의를 가리지 않고 이어받음.
#   이어받은 연결이 다른 meetingId로 start하면 이전 회의를 닫고 새 회의를 엶 (control의 start 분기)
#   작성자 확인은 백엔드 몫이라 하지 않음 (입장권 확인이 들어오면 연결 주소의 meetingId로 회의를 가릴 수 있음)
# 소리와 제어 메시지를 한 대기열에 순서대로 넣어, 소리 처리 루프가 받은 순서대로 처리함
WSQ = queue.Queue()
WS = {'conn': None, 'meeting': None, 'ms': 0.0, 'skip': 0}   # 지금 연결, 회의, 그 회의에서 받은 소리(ms), 겹쳐서 버릴 바이트
CATCHUP = [False]   # 밀린 소리가 1초 넘게 쌓임 (다시 연결 뒤 몰아 보내는 중) → 말하는 중 줄은 백엔드로 보내지 않음
MS = SR * 2 // 1000   # 1ms = 32바이트


def ws_close_old(conn):
    """자리를 내준 연결을 닫음. 닫기는 상대 응답을 기다릴 수 있어 새 연결을 막지 않게 따로 스레드에서 부름"""
    try: conn.close(4001, 'replaced by a new connection')
    except Exception: pass


def ws_handler(conn):
    from websockets.exceptions import ConnectionClosed
    say = lambda **m: conn.send(json.dumps(m, ensure_ascii=False))
    with lock:
        old, WS['conn'] = WS['conn'], conn
    if old is not None:
        print('WebSocket 새 연결이 이전 연결을 대신함', file=sys.stderr, flush=True)
        threading.Thread(target=ws_close_old, args=(old,), daemon=True).start()
    print(f'WebSocket 연결: {conn.remote_address}', file=sys.stderr, flush=True)
    try:
        for msg in conn:
            if isinstance(msg, bytes):
                if WS['meeting'] is None: continue          # start 전 소리는 버림
                if WS['skip']:                               # 다시 연결 뒤 이미 받은 부분
                    k = min(WS['skip'], len(msg)); msg = msg[k:]; WS['skip'] -= k
                if len(msg) % 2: msg = msg[:-1]
                if msg: WSQ.put(msg); WS['ms'] += len(msg) / MS
                continue
            try: m = json.loads(msg)
            except ValueError:
                say(type='error', message='JSON이 아님'); continue
            t = m.get('type')
            if t == 'start':
                mid = str(m.get('meetingId') or '').strip(); off = float(m.get('offsetMs') or 0)
                if not mid:
                    say(type='error', message='meetingId 없음'); continue
                if mid == WS['meeting']:                     # 같은 회의로 다시 연결
                    gap = off - WS['ms']
                    if gap > 0:                              # 빈 시간은 무음으로 채워 시각을 맞춤
                        if gap > 10000: print(f'다시 연결: {gap / 1000:.1f}초가 비어 무음으로 채움', file=sys.stderr, flush=True)
                        z = bytes(int(gap) * MS)
                        for i in range(0, len(z), 3200): WSQ.put(z[i:i + 3200])
                        WS['ms'] += int(gap)
                    elif gap < 0: WS['skip'] = int(-gap) * MS
                else:
                    WS.update(meeting=mid, ms=off, skip=0)
                    WSQ.put(('start', mid, off / 1000))
                say(type='ready', meetingId=mid, meetingMs=round(WS['ms']))
            elif t in ('pause', 'resume'):
                WSQ.put(t)
            elif t == 'end':
                WSQ.put('complete'); WS['meeting'] = None; say(type='ended')
            else:
                say(type='error', message=f'모르는 type: {t}')
    except ConnectionClosed:
        pass
    finally:
        with lock:
            mine = WS['conn'] is conn   # 이미 다른 연결이 들어왔으면 그쪽을 지우지 않음
            if mine: WS['conn'] = None
        if mine:
            print('WebSocket 연결 끊김' + (f' (회의 {WS["meeting"]}는 열린 채 다시 연결을 기다림)' if WS['meeting'] else ''), file=sys.stderr, flush=True)


def ws_read():
    """소리면 bytes, 제어 메시지면 제어 대기열로 넘기고 None, 0.1초 동안 없으면 None"""
    try: item = WSQ.get(timeout=0.1)
    except queue.Empty: return None
    if not isinstance(item, bytes):
        CTRL.put(item); return None
    CATCHUP[0] = WSQ.qsize() > 10
    return item


# ---- 소리 입력 스레드 ----
stop = threading.Event()


def audio_loop():
    try:
        _audio_loop()
    except Exception as e:  # 스레드 안 오류는 화면과 터미널에 알림
        print(f'입력/엔진 오류: {e}', file=sys.stderr, flush=True)
        status['running'] = False; status['error'] = str(e)
        publish({'type': 'status', 'running': False, 'error': str(e)})


class Diag:
    """소리 입력 스레드가 시간을 어디에 쓰는지 셈 (멈춘 동안은 세지 않음).
    --diag: 30초마다 출력. 없으면 최근 30초에 빠진 소리가 1% 이상이거나 입력 넘침이 새로 생겼을 때만 경고 한 줄"""
    def __init__(self, q=None, flags=None, verbose=False):
        self.q, self.flags, self.verbose = q, flags or {}, verbose
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
                    print(f'[경고] 마이크 소리가 빠지고 있음: 최근 {dw:.0f}초 중 {recent:.1f}% 빠짐, 입력 넘침 누적 {over}번 '
                          f'(전체 {lost:.1f}%). 자세히 보려면 --diag', file=sys.stderr, flush=True)
                return
            msg = (f'[진단] 실행 {self.wall:.0f}초 · 받은 소리 {self.got:.0f}초 (빠짐 {lost:.1f}%) · 읽기 대기 {self.read:.1f}초 '
                   f'(바로 읽힘 {self.instant / max(self.n, 1) * 100:.0f}%) · 엔진 {self.feed - self.pub:.1f}초 · 알림·저장 {self.pub:.1f}초 · 녹음 쓰기 {self.rec:.1f}초')
            if self.q is not None: msg += f' · 대기열 최대 {self.maxq * 0.1:.1f}초'
            if self.flags: msg += ' · ' + ', '.join(f'{k} {v}' for k, v in self.flags.items())
            print(msg, file=sys.stderr, flush=True)


def _audio_loop():
    global ENGINE
    eng = ENGINE = Engine(publish, max_speakers=A.max_speakers, model_dir=f'{A.models}/zipformer-174m', vad_model=f'{A.models}/speaker/silero_vad.onnx',
                 chunk=A.chunk, hotwords=A.hotwords, hotwords_score=A.hotwords_score, recover=not A.no_recover,
                 spk_model=f'{A.models}/speaker/nemo_en_titanet_small.onnx', diar=not A.no_diar, alt=not A.no_alt,
                 max_len=A.max_len, overlap=A.overlap, blank_penalty=A.blank_penalty)
    stream = None; sdq = None; flags = {}; wss = None
    if A.wav:
        w = wave.open(A.wav); assert w.getframerate() == SR and w.getnchannels() == 1 and w.getsampwidth() == 2
        read = lambda: w.readframes(1600)
    elif A.input == 'ws':
        from websockets.sync.server import serve
        wss = serve(ws_handler, A.ws_host, A.ws_port, max_size=2 ** 20, ping_interval=5, ping_timeout=10)
        # ping 5초 간격·10초 대기 → 끊김을 최대 15초 안에 알아차림 (websockets 기본값은 20초·20초)
        threading.Thread(target=wss.serve_forever, daemon=True).start()
        status['source'] = f'WebSocket ws://{A.ws_host}:{A.ws_port}'
        print(f'WebSocket 입력 대기: ws://{A.ws_host}:{A.ws_port}  시험용 보내는 화면: http://localhost:{A.port}/sender', file=sys.stderr, flush=True)
        read = ws_read
    else:
        # 운영체제 오디오(PortAudio)가 자기 스레드에서 0.1초 조각을 대기열에 넣음 → 처리가 잠깐 늦어도 입력이 버려지지 않음
        import sounddevice as sd
        dev = int(A.device) if A.device.isdigit() else (A.device or None)
        sdq = queue.Queue(); flags = {'입력 넘침': 0}
        def cb(data, frames, t, st):
            if st.input_overflow: flags['입력 넘침'] += 1
            sdq.put(bytes(data))
        stream = sd.RawInputStream(samplerate=SR, channels=1, dtype='int16', blocksize=1600, device=dev, callback=cb)
        status['source'] = f'마이크 {sd.query_devices(stream.device)["name"]}'
        stream.start()
        def read():
            while not stop.is_set():
                try: return sdq.get(timeout=0.5)
                except queue.Empty:
                    if not stream.active: raise RuntimeError('마이크 입력이 멈춤 (장치 연결을 확인)')
            return b''
    # 마이크 입력은 항상 지켜봄 (문제 있을 때만 경고). WebSocket 입력은 브라우저가 멈추거나 끊긴 동안도 시간이 흘러 빠짐으로 잘못 세므로 --diag일 때만
    D = Diag(sdq, flags, verbose=A.diag) if (A.diag or (not A.wav and A.input != 'ws')) else None
    if D:   # 엔진이 부르는 화면 알림·파일 저장 시간을 따로 셈 (다른 결과 스레드의 알림은 세지 않음)
        def timed(ev, _p=publish):
            t = time.perf_counter(); _p(ev); D.pub += time.perf_counter() - t
        eng.on_event = timed
    if A.input == 'ws' and not A.wav:   # 브라우저가 start를 보낼 때 세션을 엶
        status['state'] = 'idle'
    else:
        with lock: new_session(0.0, A.out or None); save_session()
    t0 = time.perf_counter(); fed = 0.0
    try:
        while not stop.is_set():
            if D: D.tick(status['state'] == 'run')
            while not CTRL.empty():
                was = status['state']; control(eng, CTRL.get())
                if was != 'run' and status['state'] == 'run': t0 = time.perf_counter() - fed   # wav 입력: 실제 속도 기준을 다시 맞춤
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
                tw = time.perf_counter(); rec_write(c); D.rec += time.perf_counter() - tw; D.got += len(c) / SR
            else:
                eng.feed(c); rec_write(c)
            fed += len(c) / SR; status['audio_sec'] = round(eng.fed, 1)
    finally:
        if stream: stream.stop(); stream.close()
        if wss: wss.shutdown()
    eng.finish(); status['running'] = False
    publish({'type': 'status', 'running': False})


# ---- 웹 ----
class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def do_GET(self):
        if self.path == '/sender':   # 시험용 보내는 화면 (마이크 → WebSocket, --input ws와 함께 씀)
            body = open(os.path.join(os.path.dirname(STATIC), '..', 'tools', 'mic_sender.html'), 'rb').read()
            cfg = json.dumps({'wsPort': A.ws_port, 'backendUrl': A.backend_url.rstrip('/')}).encode()
            body = re.sub(rb'/\*CFG\*/.*?/\*CFG\*/', lambda m: cfg, body, count=1)   # 화면이 실제 포트·자막 주소를 쓰게
            self.send_response(200); self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(body))); self.end_headers(); self.wfile.write(body)
        elif self.path in ('/', '/index.html'):
            body = open(os.path.join(STATIC, 'index.html'), 'rb').read()
            self.send_response(200); self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(body))); self.end_headers(); self.wfile.write(body)
        elif self.path == '/events':
            self.send_response(200); self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Cache-Control', 'no-cache'); self.end_headers()
            q = queue.Queue()
            with lock:
                hist = sorted((e for e in lines.values() if not e.get('deleted')), key=lambda e: e['start']) + ([partial] if partial else [])
                hist.append({'type': 'send_init', 'history': list(SENDER.history), 'snap': SENDER.snapshot()})
                clients.append(q)
            try:
                self.wfile.write(f"event: hello\ndata: {json.dumps(status, ensure_ascii=False)}\n\n".encode())
                for ev in hist: self.wfile.write(f"data: {json.dumps(ev, ensure_ascii=False)}\n\n".encode())
                self.wfile.flush()
                while True:
                    try: ev = q.get(timeout=1.0)
                    except queue.Empty:
                        self.wfile.write(f"event: tick\ndata: {json.dumps(status)}\n\n".encode()); self.wfile.flush(); continue
                    self.wfile.write(f"data: {json.dumps(ev, ensure_ascii=False)}\n\n".encode()); self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                with lock:
                    if q in clients: clients.remove(q)
        else:
            self.send_response(404); self.end_headers()

    def _json(self):
        try:
            n = int(self.headers.get('Content-Length') or 0)
            return json.loads(self.rfile.read(n)) if 0 < n <= 20000 else None
        except (ValueError, json.JSONDecodeError):
            return None

    def do_POST(self):
        # 사용자 지정 헤더를 요구해, 다른 웹사이트가 몰래 보내는 요청(브라우저가 사전 확인으로 막음)은 받지 않음
        if self.path == '/clear' and self.headers.get('X-Meetingnote') == 'clear':
            publish({'type': 'clear'})
            self.send_response(204); self.end_headers()
        elif self.path in ('/speakers', '/merge') and self.headers.get('X-Meetingnote') == self.path[1:]:
            d = self._json()
            diar = ENGINE.diar if ENGINE else None
            if d is None or not diar:
                self.send_response(400 if d is None else 409); self.end_headers(); return
            try:
                if self.path == '/speakers':   # 참석자 수 (0이면 자동)
                    n = int(d['max'])
                    if not 0 <= n <= 50: raise ValueError
                    with diar.lock: diar.max = n
                    status['max_speakers'] = n
                    publish({'type': 'status', 'max_speakers': n})
                else:                          # 화자 합치기
                    src, dst = int(d['src']), int(d['dst'])
                    if not diar.merge(src, dst): raise ValueError
                    publish({'type': 'merge', 'src': src, 'dst': dst})
            except (ValueError, KeyError, TypeError):
                self.send_response(400); self.end_headers(); return
            self.send_response(204); self.end_headers()
        elif self.path == '/control' and self.headers.get('X-Meetingnote') == 'control':
            d = self._json()
            if not d or d.get('action') not in ('record', 'pause', 'resume', 'complete'):
                self.send_response(400); self.end_headers(); return
            CTRL.put(d['action']); self.send_response(204); self.end_headers()
        elif self.path == '/delete' and self.headers.get('X-Meetingnote') == 'delete':
            d = self._json()
            try: i = int(d['id'])
            except (TypeError, KeyError, ValueError):
                self.send_response(400); self.end_headers(); return
            with lock: ok = i in lines and not lines[i].get('deleted')
            if not ok:
                self.send_response(404); self.end_headers(); return
            publish({'type': 'delete', 'id': i})
            self.send_response(204); self.end_headers()
        elif self.path == '/add' and self.headers.get('X-Meetingnote') == 'add':
            # STT가 아예 못 잡은 말을 사용자가 직접 추가. 시각은 화면이 정한 위치(앞뒤 줄 사이)의 근사치
            global manual_seq
            d = self._json()
            try:
                start, text = float(d['start']), str(d['text']).strip()
                spk = None if d.get('spk') in (None, '') else int(d['spk'])
                if not text or len(text) > 2000 or start < 0: raise ValueError
            except (ValueError, KeyError, TypeError):
                self.send_response(400); self.end_headers(); return
            with lock: manual_seq += 1; i = 1_000_000 + manual_seq   # 엔진 번호와 겹치지 않게
            publish({'type': 'manual', 'id': i, 'start': round(start, 2), 'end': round(start, 2), 'text': text,
                     'conf': None, 'spk': spk, 'spk_info': None, 'kind': line_kind(text)})
            self.send_response(204); self.end_headers()
        elif self.path == '/edit' and self.headers.get('X-Meetingnote') == 'edit':
            try:
                n = int(self.headers.get('Content-Length') or 0)
                if n > 20000: raise ValueError('too large')
                d = json.loads(self.rfile.read(n))
                i, text, src = int(d['id']), str(d['text']).strip(), d.get('source', 'user')
                if not text or len(text) > 2000 or src not in ('user', 'alt'): raise ValueError('bad text')
            except (ValueError, KeyError, TypeError, json.JSONDecodeError):
                self.send_response(400); self.end_headers(); return
            with lock: ok = i in lines
            if not ok:
                self.send_response(404); self.end_headers(); return
            publish({'type': 'edit', 'id': i, 'text': text, 'source': src})
            self.send_response(204); self.end_headers()
        else:
            self.send_response(403); self.end_headers()


def main():
    th = threading.Thread(target=audio_loop, daemon=True); th.start()
    srv = ThreadingHTTPServer((A.host, A.port), H); srv.daemon_threads = True
    if ENV_LOADED: print(f'설정 파일: {ENV_LOADED}', flush=True)
    print(f'http://localhost:{A.port}  입력: {status["source"]}  (Ctrl+C로 종료)'
          + (f'  접속 범위: {A.host}' if A.host != '127.0.0.1' else ''), flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        # 정리(마이크 끄기·저장)는 최대 약 10초. 그 사이 Ctrl+C를 또 누르면 저장이 끊기므로 무시함 (강제 종료는 맥 Ctrl+\, 윈도우는 터미널 창 닫기)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        print('\n끄는 중… 녹음·전사 파일을 저장하고 있으니 잠시 기다려 주세요 (최대 약 10초)', flush=True)
        stop.set()
        th.join(timeout=5)
        with lock:
            rec_close()
            if SESS and not SESS['complete']: SENDER.event('end')   # 완료를 누르지 않고 끈 경우
            BACKEND.flush()
            if SESS:
                save_session(); print(f'전사문 저장: {SESS["file"]}' + (f'  녹음: {SESS["wav_path"]}' if SESS['wav_path'] else ''))
        srv.server_close()


if __name__ == '__main__':
    main()
