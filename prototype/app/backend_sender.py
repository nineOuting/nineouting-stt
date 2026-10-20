"""백엔드(meeting-service)로 전사 줄과 회의 진행 알림을 보냄.

전사 줄 형식은 CN팀 2장 2.2 + AI팀 추가 필드 (2026-10-15 확정). 발화자 구분은 하지 않기로 해 speakerLabel은 보내지 않음 (2026-10-16)

  POST {url}/internal/meetings/{meetingId}/transcripts   {"segments": [segment, ...]}
  segment: {"seq", "rev", "text", "startMs", "final"}                  말하는 중 (endMs 없음)
           {"seq", "rev", "text", "startMs", "endMs", "final": true,
            "lowConfidence", "kind", "recovered"}                      확정·보완
규칙
  - 같은 줄은 같은 seq(엔진 줄 id)로 매번 줄 전체 글자를 보냄. rev는 seq마다 1부터 늘어남.
    백엔드는 rev가 큰 것만 반영하고, final: true는 false로 덮지 않음
  - period(기본 0.5초)마다 그사이 바뀐 줄만 최신 상태로 보냄. 말하는 중 줄은 최신 것 하나만. 바뀐 것이 없으면 보내지 않음
  - 확정한 줄은 다시 보내지 않음 (확정 뒤 수정·삭제는 백엔드가 처리)
  - 보완 줄은 말하는 중 단계 없이 final·recovered로 바로 오고, 앞 시각에 끼워질 수 있음 (seq는 시간 순서가 아님)
  - 보내지 못하면 그 줄들을 다음 주기에 다시 보냄. rev로 덮어쓰므로 두 번 받아도 결과가 같음
  - startMs·endMs: 회의(세션) 시작 기준 음성 밀리초 (멈춘 동안은 늘지 않음)

회의 진행 알림 (CN팀 S2, 2026-10-20)

  POST {url}/internal/meetings/{meetingId}/events   {"event": "start"|"pause"|"resume"|"end", "atMs": 600000}
규칙
  - 생긴 순서대로 하나씩 보내고, 앞의 알림이 성공해야 다음을 보냄. 중간에 실패하면 복구될 때까지 뒤의 알림을 보류함
    (백엔드가 중복을 상태로 거르든 (event, atMs) 키로 거르든 순서만 지키면 맞게 동작함)
  - 끊겼던 동안 밀린 알림은 복구 뒤 순서대로 나감 (CN팀 S8)
  - 다시 보낼 때 atMs는 새로 계산하지 않고 생길 때의 값을 그대로 씀
  - 알림보다 앞선 전사 줄을 먼저 보냄. end는 못 보낸 줄이 빠질 때까지 잠깐 기다림 (CN팀 S4)
  - 실제 시각은 보내지 않음. 백엔드가 받은 시각으로 기록함 (2026-10-16 정리, 2026-10-20 CN팀 확인)

url이 없으면 파일(<세션>.backend.jsonl)에만 기록. 있으면 파일에도 기록 (결과와 함께)
on_update(dict): 개발 화면 상태창용. 보낼 때마다 {'type': 'send', 'entry', 'snap'}
"""
import collections, datetime, http.client, json, threading, time, urllib.parse

LOW, LOW_RATIO = 0.3, 0.2


def low_confidence(text, conf):
    """줄 글자(공백 제외) 중 확률 0.3 미만이 20% 이상인지. 기준은 docs/notes.md"""
    if not conf or len(conf) != len(text): return False
    c = [x for ch, x in zip(text, conf) if not ch.isspace()]
    return bool(c) and sum(x < LOW for x in c) / len(c) >= LOW_RATIO


def wall():
    return datetime.datetime.now().astimezone().isoformat(timespec='milliseconds')


class BackendSender:
    RETRY = 2.0   # 알림을 보내지 못했을 때 다시 시도하기까지 (초)

    def __init__(self, url='', period=0.5, now=lambda: 0.0, log=print, timeout=2.0, on_update=lambda ev: None):
        self.url, self.period, self.now, self.log, self.timeout = url.rstrip('/'), period, now, log, timeout
        self.on_update = on_update
        self.lock, self.io = threading.Lock(), threading.Lock()
        self.sending = threading.Lock()   # 주기 스레드와 멈춤·종료 처리가 같은 연결로 동시에 보내지 않게
        self.meeting = None          # {'id', 't0'(엔진 초), 'logfile', 'gaps'}
        self.seg, self.dirty = {}, set()   # seq → 최신 segment, 다음에 보낼 seq
        self.events = []             # 아직 못 보낸 회의 알림 (앞에서부터 하나씩)
        self.retry_at = 0.0          # 알림 전송에 실패했을 때 다음 시도 시각
        self.history = collections.deque(maxlen=200)   # 상태창용 최근 전송 기록
        self.stats = {'sent': 0, 'failed': 0, 'requests': 0, 'events': 0}
        self.conn = None
        if self.url:
            u = urllib.parse.urlsplit(self.url)
            self.scheme, self.host, self.base = u.scheme, u.netloc, u.path
        threading.Thread(target=self._loop, daemon=True).start()

    def _ms(self, t):
        """엔진 시각(초) → 회의 시작 기준 밀리초. 멈춤 처리로 넣은 무음 구간은 회의 시간에서 뺌"""
        cut = sum(min(dur, max(0.0, t - at)) for at, dur in self.meeting['gaps'])
        return max(0, int(round((t - self.meeting['t0'] - cut) * 1000)))

    def snapshot(self):
        """개발 화면 상태창용"""
        with self.lock:
            return {'url': self.url, 'period': self.period, 'stats': dict(self.stats),
                    'meeting_id': self.meeting and self.meeting['id'],
                    'logfile': self.meeting and self.meeting['logfile'],
                    'pending_lines': len(self.dirty), 'pending_events': [dict(e) for e in self.events]}

    def start(self, meeting_id, t0, logfile):
        """새 회의. 이전 회의에서 못 보낸 줄과 알림은 먼저 보냄"""
        self.flush(); self._flush_events()
        post = str(meeting_id).isdigit()   # 백엔드 주소는 숫자 회의 번호만 받음 (CN팀 S5)
        with self.lock:
            self.meeting = {'id': meeting_id, 't0': t0, 'logfile': logfile, 'gaps': [], 'post': post}
            self.seg, self.dirty, self.events = {}, set(), []
        if self.url and not post:
            self.log(f'회의 번호가 숫자가 아니라 백엔드로 보내지 않고 파일에만 기록함 ({meeting_id}). '
                     f'보내려면 --meeting-id로 백엔드 회의 번호를 주세요')
        self.event('start')

    def gap(self, at, dur):
        """엔진 시각 at부터 dur초는 회의 시간이 아님 (멈춤·종료 때 줄 마무리용으로 넣은 무음)"""
        with self.lock:
            if self.meeting: self.meeting['gaps'].append((at, dur))

    def partial(self, ev):
        """엔진 partial: 말하는 중 줄. 이미 확정한 줄이면 무시"""
        with self.lock:
            if not self.meeting: return
            s = self.seg.get(ev['id'])
            if s and s['final']: return
            rev = s['rev'] + 1 if s else 1
            self.seg[ev['id']] = {'seq': ev['id'], 'rev': rev, 'text': ev['text'], 'startMs': self._ms(ev['start']), 'final': False}
            self.dirty.add(ev['id'])

    def line(self, ev):
        """엔진 final·recovered: 확정. 한 번만 보냄"""
        with self.lock:
            if not self.meeting: return
            s = self.seg.get(ev['id'])
            if s and s['final']: return
            self.seg[ev['id']] = {'seq': ev['id'], 'rev': s['rev'] + 1 if s else 1, 'text': ev['text'],
                                  'startMs': self._ms(ev['start']), 'endMs': self._ms(ev['end']), 'final': True,
                                  'lowConfidence': low_confidence(ev['text'], ev.get('conf')), 'kind': ev.get('kind'),
                                  'recovered': ev['type'] == 'recovered'}
            self.dirty.add(ev['id'])

    def event(self, name, wait_lines=0.0):
        """회의 알림을 큐에 넣고 보냄. 앞선 줄을 먼저 보냄.
        wait_lines초: 못 보낸 줄이 남아 있으면 그동안 다시 시도함 (end가 줄보다 먼저 가지 않게)"""
        self.flush()
        t0 = time.monotonic()
        while wait_lines > 0:
            with self.lock:
                if not self.dirty: break
            if time.monotonic() - t0 >= wait_lines:
                self.log(f'못 보낸 줄이 남은 채로 {name} 알림을 보냄 ({wait_lines}초 기다림)')
                break
            time.sleep(0.2); self.flush()
        with self.lock:
            if not self.meeting: return
            self.events.append({'event': name, 'atMs': self._ms(self.now())})
        self._flush_events()

    def flush(self):
        """바뀐 줄을 지금 보냄 (주기마다, 멈춤·완료·종료 때)"""
        with self.sending:
            self._flush()

    def _flush(self):
        with self.lock:
            if not self.meeting or not self.dirty: return
            segs = sorted((dict(self.seg[i]) for i in self.dirty), key=lambda s: s['startMs'])
            sent_ids = set(self.dirty); self.dirty = set()
            m = dict(self.meeting)
        ok, err = self._post(m, 'transcripts', {'segments': segs})
        with self.lock:
            if not ok and self.meeting and self.meeting['id'] == m['id']:
                self.dirty |= sent_ids   # 다음 주기에 그 줄들의 최신 상태를 다시 보냄
        self._record(m, ok, err, segments=segs)

    def _flush_events(self):
        """큐에 쌓인 알림을 앞에서부터 하나씩. 하나라도 실패하면 멈추고 잠시 뒤 다시 함.
        줄과 달리 실패하면 RETRY초를 띄움 (알림은 몇 개뿐이라 0.5초마다 두드릴 이유가 없음)"""
        with self.sending:
            while True:
                if time.monotonic() < self.retry_at: return
                with self.lock:
                    if not self.meeting or not self.events: return
                    ev, m = dict(self.events[0]), dict(self.meeting)
                ok, err = self._post(m, 'events', ev)
                self._record(m, ok, err, event=ev)
                with self.lock:
                    if not ok:
                        self.retry_at = time.monotonic() + self.RETRY
                        return
                    if self.events and self.events[0] == ev: self.events.pop(0)
                    self.stats['events'] += 1; self.retry_at = 0.0

    def _record(self, m, ok, err, segments=None, event=None):
        """파일에 남기고 상태창에 알림"""
        sending = self.url and m.get('post')
        entry = {'wall': wall(), 'result': 'file' if not sending else ('ok' if ok else 'fail'), 'error': err}
        entry.update({'segments': segments} if segments is not None else {'event': event})
        with self.io:
            with open(m['logfile'], 'a', encoding='utf-8') as f:
                f.write(json.dumps(entry, ensure_ascii=False) + '\n')
        self.history.append(entry)
        self.on_update({'type': 'send', 'entry': entry, 'snap': self.snapshot()})

    def _post(self, m, kind, body):
        """kind: transcripts(전사 줄) 또는 events(회의 알림). 주소가 없거나 회의 번호가 숫자가 아니면 파일에만 남김"""
        if not self.url or not m.get('post'): return True, ''
        path = f'{self.base}/internal/meetings/{m["id"]}/{kind}'
        data = json.dumps(body, ensure_ascii=False).encode()
        self.stats['requests'] += 1
        for attempt in (1, 2):   # 끊긴 연결을 재사용하다 실패하면 새로 연결해 한 번 더
            try:
                if self.conn is None:
                    cls = http.client.HTTPSConnection if self.scheme == 'https' else http.client.HTTPConnection
                    self.conn = cls(self.host, timeout=self.timeout)   # 연결을 유지해 0.5초마다 새로 연결하지 않음
                self.conn.request('POST', path, body=data, headers={'Content-Type': 'application/json'})
                r = self.conn.getresponse(); r.read()
                if 200 <= r.status < 300:
                    self.stats['sent'] += 1; return True, ''
                err = f'HTTP {r.status}'
                break
            except (OSError, http.client.HTTPException) as e:
                err = str(e) or type(e).__name__
                self.conn.close(); self.conn = None
                if attempt == 2: break
        self.stats['failed'] += 1
        if self.stats['failed'] in (1, 10, 100, 1000): self.log(f'백엔드로 보내지 못함 ({self.stats["failed"]}번째): {err}')
        return False, err

    def _loop(self):
        nxt = time.monotonic()
        while True:
            nxt += self.period
            time.sleep(max(0.0, nxt - time.monotonic()))
            if time.monotonic() - nxt > self.period: nxt = time.monotonic()   # 보내기가 오래 걸리면 밀린 주기는 건너뜀
            try:
                self.flush()
                self._flush_events()   # 끊겼던 동안 밀린 알림을 순서대로 다시 보냄
            except Exception as e:
                self.log(f'백엔드 전송 오류: {e}')
