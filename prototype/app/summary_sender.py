"""요약 쪽(AI-02 이후)으로 보내는 메시지를 만듦. 형식은 2026-10-14 회의에서 정함.

메시지 두 종류
  알림 (바로 보냄):  {"meeting_id", "event": "start"|"pause"|"resume"|"end", "at_ms", "wall"}
  줄 묶음 (주기마다): {"meeting_id", "seq", "sent_ms", "lines": [...], "edits": [...]}
    lines: {"id", "start_ms", "end_ms", "type", "kind", "low_confidence", "text"}
    edits: {"id", "rev", "text"} 또는 {"id", "rev", "deleted": true}
규칙
  - 시간은 밀리초 정수, 회의(세션) 시작 기준 음성 시간. 멈춘 동안은 늘지 않음 → 실제 시각은 알림의 wall로 계산
  - 줄 순서는 start_ms 기준. id는 식별용이지 순서가 아님 (보완 줄은 이미 보낸 줄보다 앞자리로 올 수 있음)
  - edits는 같은 id에서 rev가 가장 큰 것만 반영. 아직 보내지 않은 줄을 고치거나 지우면 그 줄 자체를 고치거나 뺌
  - low_confidence: 줄 글자(공백 제외) 중 확률 0.3 미만 비율이 20% 이상 (회의 2 정답으로 정함, 개발하며 조정). 사람이 고친 줄은 false
  - 보낼 것이 없는 주기에는 보내지 않음
받는 주소가 없으면 파일(outbox)에만 한 줄씩 기록함. 주소가 있으면 JSON으로 POST도 함 (실패해도 다시 보내지 않음, 파일에는 남음)
on_update(dict): 상태창용. 대기 목록이 바뀌면 {'type': 'send_pending', 'snap'}, 보낼 때마다 {'type': 'send', 'entry', 'snap'}
"""
import collections, datetime, json, threading, time, urllib.error, urllib.request

LOW, LOW_RATIO = 0.3, 0.2


def low_confidence(text, conf):
    if not conf or len(conf) != len(text): return False
    c = [x for ch, x in zip(text, conf) if not ch.isspace()]
    return bool(c) and sum(x < LOW for x in c) / len(c) >= LOW_RATIO


def wall():
    return datetime.datetime.now().astimezone().isoformat(timespec='milliseconds')


class Sender:
    def __init__(self, url='', period=10.0, now=lambda: 0.0, log=print, on_update=lambda ev: None):
        self.url, self.period, self.now, self.log, self.on_update = url, period, now, log, on_update
        self.lock, self.io = threading.Lock(), threading.Lock()
        self.meeting = None          # {'id', 't0'(엔진 초), 'outbox'(파일 경로), 'seq'}
        self.lines, self.edits = [], {}
        self.stats = {'sent': 0, 'failed': 0, 'filed': 0}
        self.history = collections.deque(maxlen=300)   # 상태창용 최근 전송 기록
        self.tick = time.time()      # 마지막 주기 시각 (다음 전송까지 남은 초 계산용)
        threading.Thread(target=self._loop, daemon=True).start()

    def snapshot(self):
        with self.lock:
            return {'url': self.url, 'period': self.period, 'tick': self.tick, 'stats': dict(self.stats),
                    'meeting_id': self.meeting and self.meeting['id'], 'seq': self.meeting and self.meeting['seq'],
                    'outbox': self.meeting and self.meeting['outbox'],
                    'pending_lines': [dict(x) for x in self.lines], 'pending_edits': list(self.edits.values())}

    def _pending_changed(self):
        self.on_update({'type': 'send_pending', 'snap': self.snapshot()})

    def _ms(self, t):
        """엔진 시각(초) → 회의 시작 기준 밀리초. 멈춤 처리로 넣은 무음 구간(gap)은 회의 시간에서 뺌"""
        cut = sum(min(dur, max(0.0, t - at)) for at, dur in self.meeting.get('gaps', []))
        return max(0, int(round((t - self.meeting['t0'] - cut) * 1000)))

    def start(self, meeting_id, t0, outbox):
        """새 회의. 이전 회의에서 못 보낸 것은 먼저 보냄"""
        self.flush()
        with self.lock:
            self.meeting = {'id': meeting_id, 't0': t0, 'outbox': outbox, 'seq': 0, 'gaps': []}
            self.lines, self.edits = [], {}
        self.event('start')

    def gap(self, at, dur):
        """엔진 시각 at부터 dur초는 회의 시간이 아님 (브라우저 입력에서 멈출 때 줄 마무리용으로 넣은 무음)"""
        with self.lock:
            if self.meeting: self.meeting['gaps'].append((at, dur))

    def event(self, name):
        if name in ('pause', 'end'): self.flush()   # 멈추거나 끝낼 때는 남은 줄을 먼저 보냄
        with self.lock:
            if not self.meeting: return
            msg = {'meeting_id': self.meeting['id'], 'event': name, 'at_ms': self._ms(self.now()), 'wall': wall()}
            out = self.meeting['outbox']
        self._send(msg, out)

    def line(self, ev):
        with self.lock:
            if not self.meeting: return
            self.lines.append({'id': ev['id'], 'start_ms': self._ms(ev['start']), 'end_ms': self._ms(ev['end']),
                               'type': ev['type'], 'kind': ev.get('kind'),
                               'low_confidence': low_confidence(ev['text'], ev.get('conf')), 'text': ev['text']})
        self._pending_changed()

    def edit(self, i, rev, text=None, kind=None, deleted=False):
        with self.lock:
            if not self.meeting: return
            for x in self.lines:            # 아직 안 보낸 줄
                if x['id'] == i:
                    if deleted: self.lines.remove(x)
                    else: x.update(text=text, kind=kind, low_confidence=False)
                    break
            else:
                self.edits[i] = {'id': i, 'rev': rev, 'deleted': True} if deleted else {'id': i, 'rev': rev, 'text': text}
        self._pending_changed()

    def flush(self):
        with self.lock:
            if not self.meeting or not (self.lines or self.edits): return
            self.meeting['seq'] += 1
            msg = {'meeting_id': self.meeting['id'], 'seq': self.meeting['seq'], 'sent_ms': self._ms(self.now()),
                   'lines': self.lines, 'edits': list(self.edits.values())}
            out = self.meeting['outbox']; self.lines, self.edits = [], {}
        self._send(msg, out)

    def _loop(self):
        while True:
            time.sleep(max(0.0, self.tick + self.period - time.time()))
            self.tick = time.time()
            try: self.flush()
            except Exception as e: self.log(f'요약 전송 오류: {e}')

    def _send(self, msg, outbox):
        body = json.dumps(msg, ensure_ascii=False)
        with self.io:
            with open(outbox, 'a', encoding='utf-8') as f: f.write(body + '\n')
        entry = {'wall': wall(), 'result': 'file', 'error': '', 'msg': msg}
        if not self.url:
            self.stats['filed'] += 1
        else:
            try:
                req = urllib.request.Request(self.url, data=body.encode(), headers={'Content-Type': 'application/json'}, method='POST')
                urllib.request.urlopen(req, timeout=3).close(); self.stats['sent'] += 1; entry['result'] = 'ok'
            except (urllib.error.URLError, OSError) as e:
                self.stats['failed'] += 1; entry.update(result='fail', error=str(getattr(e, 'reason', e)))
                if self.stats['failed'] in (1, 10, 100): self.log(f'요약 쪽으로 보내지 못함 ({self.stats["failed"]}번째): {e}')
        self.history.append(entry)
        self.on_update({'type': 'send', 'entry': entry, 'snap': self.snapshot()})
