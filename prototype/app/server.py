"""회의 실시간 전사 웹 화면 (로컬 전용).

마이크 또는 wav를 엔진에 넣고, 이벤트를 SSE로 브라우저에 보냄.
http://localhost:8765 에서 열림. 외부로 보내지 않음. 녹음은 저장하지 않음.
마이크 입력: 기본은 sounddevice(CoreAudio). ffmpeg avfoundation 입력은 말할 때 틱 잡음이 생겨(맥 내장 마이크, 2026-10-14
  진 청취) --input ffmpeg 일 때만 씀. 같은 날 서버 세션들에서 실제 시간 대비 소리가 약 11% 빠졌음 (원인 확인용 --diag)

실행 (nineouting-stt/prototype 기준)
  마이크 장치 목록:  .venv/bin/python app/server.py --list-devices
  마이크로 전사:     .venv/bin/python app/server.py            (시스템 기본 입력. 장치를 고르려면 --device 번호)
  입력 상태 진단:    .venv/bin/python app/server.py --diag     (30초마다 받은 소리·읽기 대기·처리 시간을 터미널에 출력)
  wav로 시험:       .venv/bin/python app/server.py --wav ~/Dev/nineouting-ref/stt_eval/own/eval_own_01.wav
  전사문 저장:       자동. 세션마다 transcripts/날짜_시각.jsonl 에 줄이 바뀔 때마다 저장 (--save-dir로 폴더, --out으로 첫 세션 파일 지정)
종료: Ctrl+C (이 프로그램이 띄운 ffmpeg만 종료)

세션 (화면 버튼)
  서버를 켜면 세션 1이 시작되고 바로 전사함. 녹음은 하지 않음
  녹음 시작: 지금 세션에 녹음을 붙임 (직전 최대 2분, 세션 시작 이후분 포함). 같은 이름의 .wav
  일시정지/계속: 전사와 녹음을 같이 멈춤. 멈출 때 무음 2.5초를 넣어 말하던 줄과 보완 판단을 마무리함 (녹음에도 들어감)
  완료: 세션을 닫고 대기. 닫은 세션의 줄을 고치면 그 파일에 반영됨. 대기에서 녹음 시작 → 화면을 비우고 새 세션 파일
  시각: 줄의 start·end는 서버 시작 뒤 엔진에 넣은 소리 기준 초 (멈춘 동안은 늘지 않음). wav 안의 시각 = start - wav_t0 (파일 첫 줄 _meta)

요약 쪽 전송 (summary_sender.py, 2026-10-14 회의에서 정한 형식)
  세션 = 회의. 세션마다 meeting_id를 붙이고, 확정 줄을 --send-period초(기본 10)마다 묶어 보냄. 시작·멈춤·재개·끝 알림은 바로 보냄
  --summary-url이 없으면 세션 파일 옆 <이름>.outbox.jsonl 에만 기록
"""
import argparse, datetime, json, os, queue, shutil, signal, subprocess, sys, threading, time, wave
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from engine import Engine, SR, DEFAULT_MODELS, line_kind  # noqa: E402
from summary_sender import Sender  # noqa: E402

STATIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')
p = argparse.ArgumentParser()
p.add_argument('--input', default='sd', choices=['sd', 'ffmpeg'], help='마이크 입력 방식 (sd=sounddevice/CoreAudio, ffmpeg=avfoundation)')
p.add_argument('--device', default='', help='마이크 장치 번호 (--list-devices). 없으면 sd는 시스템 기본 입력, ffmpeg는 0')
p.add_argument('--list-devices', action='store_true')
p.add_argument('--diag', action='store_true', help='30초마다 입력 상태(실제 시간 대비 받은 소리, 읽기 대기, 처리 시간)를 터미널에 출력')
p.add_argument('--wav', default=''); p.add_argument('--port', type=int, default=8765)
p.add_argument('--models', default=DEFAULT_MODELS, help='stt_models 폴더 (기본: nineouting-ref/stt_models)')
p.add_argument('--chunk', default='64', choices=['16', '64'])
p.add_argument('--hotwords', default=''); p.add_argument('--hotwords-score', type=float, default=1.0)
p.add_argument('--blank-penalty', type=float, default=0.5, help='글자 안 냄(blank)에 주는 벌점. 빠지는 말이 줄고 틀린·낀 글자가 조금 늚. 0이면 끔 (1.0부터 삽입 급증)')
p.add_argument('--no-recover', action='store_true'); p.add_argument('--out', default='', help='첫 세션 전사 파일 경로 (기본: 자동 이름)')
p.add_argument('--save-dir', default=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'transcripts'),
               help='세션 전사·녹음 파일 폴더 (기본: prototype/transcripts, 깃 제외)')
p.add_argument('--no-diar', action='store_true', help='화자 번호 표시 끄기')
p.add_argument('--no-alt', action='store_true', help='다른 인식 결과 끄기 (메모리 약 300MB 절약, 추정)')
p.add_argument('--max-speakers', type=int, default=0, help='참석자 수 (0이면 자동). 화면에서도 바꿀 수 있음')
p.add_argument('--max-len', type=float, default=10.0, help='한 줄 최대 길이(초). 쉼 없이 길게 말하면 이 길이에서 끊음 (이전 20)')
p.add_argument('--overlap', type=float, default=1.0, help='최대 길이로 끊을 때 직전 소리를 새 줄에 다시 넣는 길이(초). 걸친 단어를 살리는 대신 앞뒤 줄에 겹칠 수 있음. 0이면 끔')
p.add_argument('--summary-url', default='', help='요약 쪽 받는 주소 (예: http://localhost:8000/stt). 없으면 outbox 파일에만 기록')
p.add_argument('--send-period', type=float, default=10.0, help='요약 쪽 전송 주기(초)')
p.add_argument('--meeting-id', default='', help='첫 회의의 meeting_id (앱이 줄 값. 없으면 자동)')
A = p.parse_args()

FFMPEG = shutil.which('ffmpeg')
if A.list_devices and A.input == 'sd':
    import sounddevice as sd
    for i, d in enumerate(sd.query_devices()):
        if d['max_input_channels'] > 0:
            print(f'[{i}] {d["name"]} (입력 {d["max_input_channels"]}채널, {d["default_samplerate"]:.0f}Hz)' + (' ← 기본' if i == sd.default.device[0] else ''))
    sys.exit(0)
if A.list_devices:
    if not FFMPEG: sys.exit('ffmpeg를 찾지 못함')
    r = subprocess.run([FFMPEG, '-hide_banner', '-f', 'avfoundation', '-list_devices', 'true', '-i', ''], capture_output=True, text=True)
    print('\n'.join(l for l in r.stderr.splitlines() if 'AVFoundation' in l)); sys.exit(0)

# ---- 이벤트 저장·배포 ----
lock = threading.RLock()   # 요약 전송 알림이 publish 안에서 다시 publish를 부를 수 있어 재진입 허용
lines = {}          # id → 확정·보완 줄 (새로 접속한 화면에 보냄)
partial = None      # 지금 말하는 중인 줄
clients = []        # 화면별 큐
status = {'source': A.wav or f'마이크 {A.device or "기본"} ({A.input})', 'audio_sec': 0.0, 'running': True, 'max_speakers': A.max_speakers,
          'state': 'run', 'rec': False, 'rec_sec': 0.0, 'session': ''}   # state: run(전사 중) / pause(일시정지) / idle(완료 후 대기)
ENGINE = None       # 화면에서 화자 합치기·참석자 수를 바꿀 때 씀
manual_seq = 0      # 직접 추가한 줄 번호
SENDER = Sender(A.summary_url, A.send_period, now=lambda: ENGINE.fed if ENGINE else 0.0,
                log=lambda m: print(m, file=sys.stderr, flush=True), on_update=lambda ev: publish(ev))


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
        for q in clients: q.put(ev)
        if ev['type'] not in ('partial', 'status'): save_session()


# ---- 세션 (자동 저장·녹음) ----
SESS = None        # 지금 세션: file, wav_path, wav(쓰는 중인 wave 객체), wav_t0, t0, started, complete
CTRL = queue.Queue()   # 화면의 녹음·일시정지·완료 요청. 소리 입력 스레드가 소리 조각 사이에 처리함


def new_session(t0, path=None):
    global SESS
    os.makedirs(A.save_dir, exist_ok=True)
    base = path or os.path.join(A.save_dir, datetime.datetime.now().strftime('%Y%m%d_%H%M%S') + '.jsonl')
    os.makedirs(os.path.dirname(os.path.abspath(base)), exist_ok=True)
    mid = (A.meeting_id if A.meeting_id and SESS is None else '') or 'm_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    SESS = {'file': base, 'wav_path': None, 'wav': None, 'wav_t0': None, 't0': round(t0, 2), 'meeting_id': mid,
            'started': datetime.datetime.now().isoformat(timespec='seconds'), 'complete': False}
    SENDER.start(mid, t0, os.path.splitext(base)[0] + '.outbox.jsonl')
    status['session'] = os.path.basename(base); status['rec'] = False; status['rec_sec'] = 0.0


def save_session():
    """지금 세션의 줄을 파일에 통째로 다시 씀 (lock 안에서 호출). 첫 줄은 _meta"""
    if not SESS: return
    meta = {k: SESS[k] for k in ('meeting_id', 'started', 't0', 'wav_t0', 'complete')}
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
        flush(); status['state'] = 'pause'; SENDER.event('pause')
    elif act == 'resume' and st == 'pause':
        status['state'] = 'run'; SENDER.event('resume')
    elif act == 'complete' and st in ('run', 'pause'):
        if st == 'run': flush()
        rec_close(); status['state'] = 'idle'; status['rec'] = False; SENDER.event('end')
        with lock: SESS['complete'] = True; save_session()
    else:
        return
    with lock: save_session()
    publish({'type': 'status', **{k: status[k] for k in ('state', 'rec', 'rec_sec', 'session', 'audio_sec')}})


# ---- 소리 입력 스레드 ----
stop = threading.Event()
proc = None
ffmpeg_err = []     # ffmpeg 메시지 (평소엔 숨기고, 예기치 않게 멈췄을 때만 보여줌)


def audio_loop():
    try:
        _audio_loop()
    except Exception as e:  # 스레드 안 오류는 화면과 터미널에 알림
        print(f'입력/엔진 오류: {e}', file=sys.stderr, flush=True)
        status['running'] = False; status['error'] = str(e)
        publish({'type': 'status', 'running': False, 'error': str(e)})


class Diag:
    """--diag: 소리 입력 스레드가 시간을 어디에 쓰는지 30초마다 출력 (멈춘 동안은 세지 않음)"""
    def __init__(self, q=None, flags=None):
        self.q, self.flags = q, flags or {}
        self.last = self.t_rep = time.perf_counter(); self.n_err = 0
        self.wall = self.got = self.read = self.feed = self.pub = self.rec = 0.0; self.n = self.instant = 0; self.maxq = 0

    def tick(self, running):
        now = time.perf_counter()
        if running: self.wall += now - self.last
        self.last = now
        if self.q is not None: self.maxq = max(self.maxq, self.q.qsize())
        if now - self.t_rep >= 30 and self.wall > 0:
            self.t_rep = now
            lost = (1 - self.got / self.wall) * 100
            msg = (f'[진단] 실행 {self.wall:.0f}초 · 받은 소리 {self.got:.0f}초 (빠짐 {lost:.1f}%) · 읽기 대기 {self.read:.1f}초 '
                   f'(바로 읽힘 {self.instant / max(self.n, 1) * 100:.0f}%) · 엔진 {self.feed - self.pub:.1f}초 · 알림·저장 {self.pub:.1f}초 · 녹음 쓰기 {self.rec:.1f}초')
            if self.q is not None: msg += f' · 대기열 최대 {self.maxq * 0.1:.1f}초'
            if self.flags: msg += ' · ' + ', '.join(f'{k} {v}' for k, v in self.flags.items())
            new = ffmpeg_err[self.n_err:]; self.n_err = len(ffmpeg_err)
            if new: msg += ' · ffmpeg: ' + ' / '.join(new[-3:])
            print(msg, file=sys.stderr, flush=True)


def _audio_loop():
    global proc, ENGINE
    eng = ENGINE = Engine(publish, max_speakers=A.max_speakers, model_dir=f'{A.models}/zipformer-174m', vad_model=f'{A.models}/speaker/silero_vad.onnx',
                 chunk=A.chunk, hotwords=A.hotwords, hotwords_score=A.hotwords_score, recover=not A.no_recover,
                 spk_model=f'{A.models}/speaker/nemo_en_titanet_small.onnx', diar=not A.no_diar, alt=not A.no_alt,
                 max_len=A.max_len, overlap=A.overlap, blank_penalty=A.blank_penalty)
    stream = None; sdq = None; flags = {}
    if A.wav:
        w = wave.open(A.wav); assert w.getframerate() == SR and w.getnchannels() == 1 and w.getsampwidth() == 2
        read = lambda: w.readframes(1600)
    elif A.input == 'sd':
        # CoreAudio가 자기 스레드에서 0.1초 조각을 대기열에 넣음 → 처리가 잠깐 늦어도 입력이 버려지지 않음
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
    else:
        # 별도 프로세스 그룹: 터미널 Ctrl+C가 ffmpeg에 직접 가지 않고, 서버가 정상 종료('q')시킴
        proc = subprocess.Popen([FFMPEG, '-hide_banner', '-loglevel', 'warning' if A.diag else 'error', '-f', 'avfoundation',
                                 '-i', f':{A.device or 0}', '-ac', '1', '-ar', str(SR), '-f', 's16le', '-'],
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        threading.Thread(target=lambda: ffmpeg_err.extend(l.decode(errors='replace').rstrip() for l in proc.stderr),
                         daemon=True).start()
        read = lambda: proc.stdout.read(3200)
    D = Diag(sdq, flags) if A.diag else None
    if D:   # 엔진이 부르는 화면 알림·파일 저장 시간을 따로 셈 (다른 결과 스레드의 알림은 세지 않음)
        def timed(ev, _p=publish):
            t = time.perf_counter(); _p(ev); D.pub += time.perf_counter() - t
        eng.on_event = timed
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
            if not b:
                if proc and not stop.is_set():  # 서버가 끄지 않았는데 ffmpeg가 멈춤 → 원인 메시지를 보여줌
                    time.sleep(0.2)
                    msg = ' / '.join(ffmpeg_err[-3:]) or f'ffmpeg 종료 (코드 {proc.poll()})'
                    raise RuntimeError(f'마이크 입력이 멈춤: {msg}')
                break
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
    eng.finish(); status['running'] = False
    publish({'type': 'status', 'running': False})


# ---- 웹 ----
class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def do_GET(self):
        if self.path in ('/', '/index.html'):
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
    srv = ThreadingHTTPServer(('127.0.0.1', A.port), H); srv.daemon_threads = True
    print(f'http://localhost:{A.port}  입력: {status["source"]}  (Ctrl+C로 종료)', flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        # 정리(마이크 끄기·저장)는 최대 약 10초. 그 사이 Ctrl+C를 또 누르면 저장이 끊기므로 무시함 (강제 종료는 Ctrl+\)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        print('\n끄는 중… 녹음·전사 파일을 저장하고 있으니 잠시 기다려 주세요 (최대 약 10초)', flush=True)
        stop.set()
        if proc and proc.poll() is None:  # 이 프로그램이 띄운 ffmpeg만 종료: 먼저 정상 종료('q'), 안 되면 신호
            try:
                proc.stdin.write(b'q'); proc.stdin.flush()
            except (BrokenPipeError, OSError):
                pass
            try: proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.terminate()
                try: proc.wait(timeout=2)
                except subprocess.TimeoutExpired: proc.kill()
        th.join(timeout=5)
        with lock:
            rec_close()
            if SESS and not SESS['complete']: SENDER.event('end')   # 완료를 누르지 않고 끈 경우
            if SESS:
                save_session(); print(f'전사문 저장: {SESS["file"]}' + (f'  녹음: {SESS["wav_path"]}' if SESS['wav_path'] else ''))
        srv.server_close()


if __name__ == '__main__':
    main()
