r"""시험용 가짜 백엔드: STT 서버(backend_sender.py)가 보내는 전사 줄을 받아, CN팀 2장 2.2 규칙대로 합쳐 화면에 보여줌.
실제 meeting-service가 아님. STT 쪽 전송을 눈으로 확인하고, 백엔드가 받은 줄을 어떻게 합치면 되는지 보여주는 참고용.

받는 곳  POST /internal/meetings/{meetingId}/transcripts   {"segments": [...]}
         POST /internal/meetings/{meetingId}/events        {"event": "start"|"pause"|"resume"|"end", "atMs": 0}
           같은 알림이 두 번 와도 한 번만 센다는 전제로, (event, atMs)가 같으면 중복으로 표시함
합치는 규칙 (2.2 + AI팀 추가 필드, 2026-10-15 확정)
  - 같은 seq는 덮어씀 (이어 붙이지 않음). rev가 지금 것보다 클 때만 반영
  - final: true인 줄은 final: false로 덮지 않음
  - 화면·저장 순서는 startMs 기준 (seq는 시간 순서가 아님, 보완 줄이 앞 시각으로 늦게 옴)
규칙 위반으로 세는 것: 옛 rev(지금 것 이하)가 온 경우, 확정 뒤 같은 seq가 다시 온 경우

실행 (nineouting-stt/prototype 기준, 윈도우는 python 대신 .venv\Scripts\python, 맥은 .venv/bin/python)
  python tools/mock_backend.py              → http://localhost:8767 (확인 화면, 포트는 --port 또는 .env MOCK_PORT)
  python app/server.py --input ws --backend-url http://127.0.0.1:8767
화면의 "10초 동안 실패" 버튼: 그동안 503으로 답해 STT의 재전송을 확인함
"""
import argparse, json, os, queue, re, sys, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'app'))
from envfile import load_env  # noqa: E402
load_env()
p = argparse.ArgumentParser(); p.add_argument('--port', type=int, default=os.environ.get('MOCK_PORT', 8767)); A = p.parse_args()
lock = threading.Lock()
meetings = {}       # meetingId → {'seg': {seq: segment}, 'stats': {...}}
reqlog = []         # 최근 요청
clients = []
fail_until = 0.0
t0 = time.monotonic(); last_req = [None]
PATH = re.compile(r'^/internal/meetings/([^/]+)/transcripts$')
EVPATH = re.compile(r'^/internal/meetings/([^/]+)/events$')


def push(ev):
    for q in clients: q.put(ev)


def mget(mid):
    return meetings.setdefault(mid, {'seg': {}, 'events': [], 'seen_events': set(),
                                     'stats': {'requests': 0, 'segments': 0, 'stale_rev': 0, 'after_final': 0,
                                               'events': 0, 'dup_events': 0}})


def apply_event(mid, ev):
    """회의 알림. (event, atMs)가 같으면 중복으로 세고 한 번만 반영 (백엔드가 한 번만 처리한다는 전제)"""
    m = mget(mid); st = m['stats']; st['requests'] += 1
    key = (ev.get('event'), ev.get('atMs'))
    if key in m['seen_events']:
        st['dup_events'] += 1
        return [f'중복 알림 {key[0]} atMs {key[1]}']
    m['seen_events'].add(key); m['events'].append(ev); st['events'] += 1
    return []


def apply(mid, segs):
    """2.2 규칙으로 합침. 바뀐 줄과 위반 내용을 돌려줌"""
    m = mget(mid)
    st = m['stats']; st['requests'] += 1; changed, notes = [], []
    for s in segs:
        st['segments'] += 1
        cur = m['seg'].get(s['seq'])
        if cur and cur.get('final'):
            st['after_final'] += 1; notes.append(f'확정 뒤 다시 옴 seq {s["seq"]} rev {s["rev"]}'); continue
        if cur and s['rev'] <= cur['rev']:
            st['stale_rev'] += 1; notes.append(f'옛 rev seq {s["seq"]} rev {s["rev"]} ≤ {cur["rev"]}'); continue
        m['seg'][s['seq']] = dict(s, _new=cur is None); changed.append(m['seg'][s['seq']])
    return changed, notes


class H(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'   # STT 쪽이 연결을 유지해 재사용함
    def log_message(self, *a): pass

    def _send(self, code, body=b'', ctype='text/plain; charset=utf-8'):
        self.send_response(code); self.send_header('Content-Type', ctype); self.send_header('Content-Length', str(len(body)))
        self.end_headers(); self.wfile.write(body)

    def do_POST(self):
        global fail_until
        n = int(self.headers.get('Content-Length') or 0); raw = self.rfile.read(n)
        if self.path == '/fail':
            fail_until = time.monotonic() + 10; push({'type': 'fail', 'until': 10}); return self._send(204)
        ev = EVPATH.match(self.path)
        if ev:
            now = time.monotonic(); failing = now < fail_until
            try: body = json.loads(raw)
            except ValueError: return self._send(400, '잘못된 형식'.encode())
            with lock:
                notes = [] if failing else apply_event(ev.group(1), body)
                entry = {'t': round(now - t0, 2), 'gap': None, 'meeting': ev.group(1), 'n': 0, 'kind': 'event',
                         'result': 503 if failing else 204, 'notes': notes, 'body': body}
                reqlog.append(entry); del reqlog[:-300]
                st = meetings.get(ev.group(1), {}).get('stats')
            push({'type': 'req', 'entry': entry, 'changed': [], 'stats': st})
            return self._send(503 if failing else 204)
        mt = PATH.match(self.path)
        if not mt: return self._send(404)
        now = time.monotonic(); gap = None if last_req[0] is None else now - last_req[0]; last_req[0] = now
        failing = now < fail_until
        try: body = json.loads(raw); segs = body['segments']
        except (ValueError, KeyError, TypeError):
            return self._send(400, '잘못된 형식'.encode())
        with lock:
            changed, notes = ([], []) if failing else apply(mt.group(1), segs)
            entry = {'t': round(now - t0, 2), 'gap': gap and round(gap, 2), 'meeting': mt.group(1), 'n': len(segs),
                     'result': 503 if failing else 204, 'notes': notes, 'body': body}
            reqlog.append(entry); del reqlog[:-300]
            st = meetings.get(mt.group(1), {}).get('stats')
        push({'type': 'req', 'entry': entry, 'changed': changed, 'stats': st})
        self._send(503 if failing else 204)

    def do_GET(self):
        if self.path in ('/', '/index.html'):
            return self._send(200, PAGE.encode(), 'text/html; charset=utf-8')
        if self.path == '/events':
            self.protocol_version = 'HTTP/1.0'
            self.send_response(200); self.send_header('Content-Type', 'text/event-stream'); self.send_header('Cache-Control', 'no-cache')
            self.send_header('Access-Control-Allow-Origin', '*')   # 다른 포트의 시험용 보내는 화면(/sender)도 자막을 구독할 수 있게 (시험용)
            self.end_headers()
            q = queue.Queue()
            with lock:
                snap = {'type': 'snap', 'meetings': {k: {'seg': list(v['seg'].values()), 'stats': v['stats']} for k, v in meetings.items()},
                        'log': reqlog[-100:]}
                clients.append(q)
            try:
                self.wfile.write(f'data: {json.dumps(snap, ensure_ascii=False)}\n\n'.encode()); self.wfile.flush()
                while True:
                    try: ev = q.get(timeout=5)
                    except queue.Empty:
                        self.wfile.write(b': ping\n\n'); self.wfile.flush(); continue
                    self.wfile.write(f'data: {json.dumps(ev, ensure_ascii=False)}\n\n'.encode()); self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                with lock:
                    if q in clients: clients.remove(q)
            return
        self._send(404)


PAGE = r"""<!doctype html><html lang="ko"><head><meta charset="utf-8"><title>가짜 백엔드</title>
<style>
body{font-family:-apple-system,"Apple SD Gothic Neo",sans-serif;margin:0;display:flex;height:100vh;color:#222}
#left{flex:3;overflow:auto;padding:16px 20px;border-right:1px solid #ddd}#right{flex:2;overflow:auto;padding:16px;background:#fafafa;font-size:13px}
h2{font-size:15px;margin:0 0 8px}.row{display:flex;gap:10px;padding:5px 0;border-bottom:1px solid #f0f0f0;transition:background 1.5s}
.ts{color:#999;font-size:12px;min-width:52px;font-variant-numeric:tabular-nums}.meta{color:#aaa;font-size:11px;min-width:84px}
.partial .tx{color:#999;font-style:italic}.rec{background:#fff3b0}.tag{font-size:11px;padding:0 5px;border-radius:3px;margin-left:6px}
.t-rec{background:#ffe27a}.t-low{border-bottom:1px dotted #c00}.t-k{background:#eee;color:#777}
#stats span{display:inline-block;margin:0 12px 6px 0}button{font-size:13px;padding:4px 10px}
.req{padding:3px 0;border-bottom:1px solid #eee}.req.f{color:#c00}details pre{white-space:pre-wrap;font-size:11px;background:#fff;border:1px solid #eee;padding:6px}
#banner{color:#c00;font-weight:bold}
</style></head><body>
<div id="left"><h2>자막 (백엔드 규칙으로 합친 결과, startMs 순) <select id="mt"></select></h2>
<div style="color:#888;font-size:12px;margin-bottom:8px">회색 기울임 = 말하는 중 · 노란색 = 보완 줄(잠깐 강조 뒤 표시만) · 점선 = lowConfidence</div>
<div id="lines"></div></div>
<div id="right"><h2>받은 것</h2><div id="stats"></div>
<button onclick="fetch('/fail',{method:'POST'})">10초 동안 실패(503)</button> <span id="banner"></span>
<h2 style="margin-top:14px">요청 기록 (최근이 위)</h2><div id="log"></div></div>
<script>
const M={};let cur=null;const fmt=ms=>{const s=ms/1000;return String(Math.floor(s/60)).padStart(2,'0')+':'+(s%60).toFixed(1).padStart(4,'0')};
function stats(){const m=M[cur];if(!m)return;const s=m.stats||{},seg=Object.values(m.seg);
 document.getElementById('stats').innerHTML=`<span>요청 ${s.requests||0}</span><span>받은 segment ${s.segments||0}</span>`+
 `<span>확정 줄 ${seg.filter(x=>x.final).length}</span><span>보완 줄 ${seg.filter(x=>x.recovered).length}</span>`+
 `<span>말하는 중 ${seg.filter(x=>!x.final).length}</span><span style="color:${(s.stale_rev||s.after_final)?'#c00':'#393'}">규칙 위반: 옛 rev ${s.stale_rev||0} · 확정 뒤 다시 옴 ${s.after_final||0}</span>`}
function render(flash){const m=M[cur];const el=document.getElementById('lines');if(!m){el.innerHTML='';return}
 const atBottom=el.parentNode.scrollTop+el.parentNode.clientHeight>=el.parentNode.scrollHeight-40;
 el.innerHTML=Object.values(m.seg).sort((a,b)=>a.startMs-b.startMs).map(s=>
  `<div class="row ${s.final?'':'partial'} ${flash&&flash.has(s.seq)&&s.recovered?'rec':''}" data-seq="${s.seq}"><span class="ts">${fmt(s.startMs)}</span>`+
  `<span class="meta">#${s.seq} r${s.rev}</span><span class="tx ${s.lowConfidence?'t-low':''}">${(s.text||'').replace(/</g,'&lt;')}</span>`+
  (s.recovered?'<span class="tag t-rec">보완</span>':'')+(s.kind?`<span class="tag t-k">${s.kind==='backchannel'?'맞장구':'간투사'}</span>`:'')+`</div>`).join('');
 if(flash)setTimeout(()=>document.querySelectorAll('.rec').forEach(e=>e.classList.remove('rec')),1600);
 if(atBottom)el.parentNode.scrollTop=el.parentNode.scrollHeight;stats()}
function sel(){const s=document.getElementById('mt');const ks=Object.keys(M);s.innerHTML=ks.map(k=>`<option ${k===cur?'selected':''}>${k}</option>`).join('')}
document.getElementById('mt').onchange=e=>{cur=e.target.value;render()};
function logRow(e){const d=document.createElement('div');d.className='req'+(e.result!==204?' f':'');
 d.innerHTML=`<details><summary>${e.t.toFixed(1)}초 · 간격 ${e.gap==null?'-':e.gap.toFixed(2)+'초'} · ${e.n}줄 · ${e.result}${e.notes.length?' · ⚠ '+e.notes.join(', '):''}</summary><pre>${JSON.stringify(e.body,null,1).replace(/</g,'&lt;')}</pre></details>`;
 const L=document.getElementById('log');L.prepend(d);while(L.children.length>200)L.lastChild.remove()}
const es=new EventSource('/events');es.onmessage=x=>{const ev=JSON.parse(x.data);
 if(ev.type==='snap'){for(const[k,v]of Object.entries(ev.meetings)){M[k]={seg:{},stats:v.stats};v.seg.forEach(s=>M[k].seg[s.seq]=s)}
  cur=Object.keys(M).pop()||null;ev.log.forEach(logRow);sel();render()}
 else if(ev.type==='req'){const k=ev.entry.meeting;if(!M[k]){M[k]={seg:{},stats:{}};cur=k;sel()}
  ev.changed.forEach(s=>M[k].seg[s.seq]=s);if(ev.stats)M[k].stats=ev.stats;logRow(ev.entry);
  if(k===cur)render(new Set(ev.changed.filter(s=>s._new).map(s=>s.seq)))}
 else if(ev.type==='fail'){const b=document.getElementById('banner');let n=ev.until;b.textContent=`503으로 답하는 중 ${n}초`;
  const t=setInterval(()=>{n--;b.textContent=n>0?`503으로 답하는 중 ${n}초`:'';if(n<=0)clearInterval(t)},1000)}};
</script></body></html>"""

class Server(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        """유지하던 연결을 상대가 끊은 경우(STT 서버 종료, 화면 새로고침)는 정상이라 조용히 넘김. 다른 오류는 그대로 출력"""
        import sys
        if isinstance(sys.exc_info()[1], (ConnectionResetError, BrokenPipeError)): return
        super().handle_error(request, client_address)


if __name__ == '__main__':
    srv = Server(('127.0.0.1', A.port), H)
    print(f'가짜 백엔드: http://localhost:{A.port}  (받는 곳 POST /internal/meetings/<id>/transcripts, Ctrl+C로 종료)', flush=True)
    try: srv.serve_forever()
    except KeyboardInterrupt: pass
