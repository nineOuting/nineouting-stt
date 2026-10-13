"""회의 실시간 전사 웹 화면 (로컬 전용).

마이크(ffmpeg avfoundation) 또는 wav를 엔진에 넣고, 이벤트를 SSE로 브라우저에 보냄.
http://localhost:8765 에서 열림. 외부로 보내지 않음. 녹음은 저장하지 않음.

실행 (nineouting-stt/prototype 기준)
  마이크 장치 목록:  .venv/bin/python app/server.py --list-devices
  마이크로 전사:     .venv/bin/python app/server.py --device 0
  wav로 시험:       .venv/bin/python app/server.py --wav ~/Dev/nineouting-ref/stt_eval/own/eval_own_01.wav
  전사문 저장:       ... --out transcripts/회의.jsonl   (종료 시 시각순으로 정렬해 다시 씀)
종료: Ctrl+C (이 프로그램이 띄운 ffmpeg만 종료)
"""
import argparse, json, os, queue, shutil, subprocess, sys, threading, time, wave
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from engine import Engine, SR, DEFAULT_MODELS, line_kind  # noqa: E402

STATIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')
p = argparse.ArgumentParser()
p.add_argument('--device', default='0'); p.add_argument('--list-devices', action='store_true')
p.add_argument('--wav', default=''); p.add_argument('--port', type=int, default=8765)
p.add_argument('--models', default=DEFAULT_MODELS, help='stt_models 폴더 (기본: nineouting-ref/stt_models)')
p.add_argument('--chunk', default='64', choices=['16', '64'])
p.add_argument('--hotwords', default=''); p.add_argument('--hotwords-score', type=float, default=1.0)
p.add_argument('--no-recover', action='store_true'); p.add_argument('--out', default='')
p.add_argument('--no-diar', action='store_true', help='화자 번호 표시 끄기')
p.add_argument('--no-alt', action='store_true', help='다른 인식 결과 끄기 (메모리 약 300MB 절약, 추정)')
p.add_argument('--max-speakers', type=int, default=0, help='참석자 수 (0이면 자동). 화면에서도 바꿀 수 있음')
A = p.parse_args()

FFMPEG = shutil.which('ffmpeg')
if A.list_devices:
    if not FFMPEG: sys.exit('ffmpeg를 찾지 못함')
    r = subprocess.run([FFMPEG, '-hide_banner', '-f', 'avfoundation', '-list_devices', 'true', '-i', ''], capture_output=True, text=True)
    print('\n'.join(l for l in r.stderr.splitlines() if 'AVFoundation' in l)); sys.exit(0)

# ---- 이벤트 저장·배포 ----
lock = threading.Lock()
lines = {}          # id → 확정·보완 줄 (새로 접속한 화면에 보냄)
partial = None      # 지금 말하는 중인 줄
clients = []        # 화면별 큐
status = {'source': A.wav or f'마이크 {A.device}', 'audio_sec': 0.0, 'running': True, 'max_speakers': A.max_speakers}
ENGINE = None       # 화면에서 화자 합치기·참석자 수를 바꿀 때 씀
manual_seq = 0      # 직접 추가한 줄 번호


clear_at = -1.0     # 마지막으로 지운 오디오 시각. 이보다 앞에서 시작한 줄은 나중에 도착해도 버림


def publish(ev):
    global partial, clear_at
    with lock:
        if ev['type'] in ('partial', 'final', 'recovered') and ev['start'] < clear_at:
            return   # 지우기 전에 시작한 줄 (말하던 중이던 줄, 판단 대기 중이던 보완)
        if ev['type'] == 'partial':
            partial = ev
        elif ev['type'] == 'status':
            pass
        elif ev['type'] == 'alt':  # 이미 있는 줄에 다른 인식 결과를 붙임 (새로 접속한 화면에도 보내기 위해)
            if ev['id'] not in lines: return   # 지운 줄에 늦게 온 결과는 버림
            lines[ev['id']]['alts'] = ev['alts']
        elif ev['type'] == 'clear':  # 전사 기록 지우기 (모든 화면에 알림)
            lines.clear(); partial = None; clear_at = status['audio_sec']
        elif ev['type'] == 'merge':  # 화자 src를 dst로 합침: 지난 줄 번호를 모두 바꿈
            for ln in lines.values():
                if ln.get('spk') == ev['src']:
                    ln['spk'] = ev['dst']; ln['merged_from'] = ev['src']
            if partial and partial.get('spk') == ev['src']: partial['spk'] = ev['dst']
        elif ev['type'] == 'edit':  # 사용자가 후보를 고르거나 직접 고침. 처음 텍스트는 orig로 남겨 되돌릴 수 있게 함
            ln = lines.get(ev['id'])
            if not ln: return
            if 'orig' not in ln: ln['orig'], ln['orig_conf'] = ln['text'], ln.get('conf')
            if ev['text'] == ln['orig']:   # 원래대로 되돌림
                ln['text'], ln['conf'] = ln['orig'], ln['orig_conf']; ln.pop('edited', None)
            else:
                ln['text'], ln['conf'], ln['edited'] = ev['text'], None, ev['source']
            ln['kind'] = line_kind(ln['text'])   # 고친 텍스트로 맞장구·간투사 다시 판별
            ev = {'type': 'edit', 'id': ev['id'], 'text': ln['text'], 'conf': ln['conf'],
                  'edited': ln.get('edited'), 'orig': ln['orig'], 'kind': ln['kind']}
        else:
            lines[ev['id']] = ev
            if partial and partial['id'] == ev['id']: partial = None
        for q in clients: q.put(ev)


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


def _audio_loop():
    global proc, ENGINE
    eng = ENGINE = Engine(publish, max_speakers=A.max_speakers, model_dir=f'{A.models}/zipformer-174m', vad_model=f'{A.models}/speaker/silero_vad.onnx',
                 chunk=A.chunk, hotwords=A.hotwords, hotwords_score=A.hotwords_score, recover=not A.no_recover,
                 spk_model=f'{A.models}/speaker/nemo_en_titanet_small.onnx', diar=not A.no_diar, alt=not A.no_alt)
    if A.wav:
        w = wave.open(A.wav); assert w.getframerate() == SR and w.getnchannels() == 1 and w.getsampwidth() == 2
        read = lambda: w.readframes(1600)
    else:
        # 별도 프로세스 그룹: 터미널 Ctrl+C가 ffmpeg에 직접 가지 않고, 서버가 정상 종료('q')시킴
        proc = subprocess.Popen([FFMPEG, '-hide_banner', '-loglevel', 'error', '-f', 'avfoundation', '-i', f':{A.device}',
                                 '-ac', '1', '-ar', str(SR), '-f', 's16le', '-'],
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        threading.Thread(target=lambda: ffmpeg_err.extend(l.decode(errors='replace').rstrip() for l in proc.stderr),
                         daemon=True).start()
        read = lambda: proc.stdout.read(3200)
    t0 = time.perf_counter(); fed = 0.0
    while not stop.is_set():
        b = read()
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
        eng.feed(c); fed += len(c) / SR; status['audio_sec'] = round(fed, 1)
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
                hist = sorted(lines.values(), key=lambda e: e['start']) + ([partial] if partial else [])
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
        if A.out:
            os.makedirs(os.path.dirname(os.path.abspath(A.out)), exist_ok=True)
            with lock, open(A.out, 'w', encoding='utf-8') as f:
                for ev in sorted(lines.values(), key=lambda e: e['start']):
                    f.write(json.dumps({k: ev.get(k) for k in ('start', 'end', 'spk', 'spk_info', 'text', 'type', 'kind', 'conf', 'alts', 'edited', 'orig')},
                                       ensure_ascii=False) + '\n')
            print(f'전사문 저장: {A.out}')
        srv.server_close()


if __name__ == '__main__':
    main()
