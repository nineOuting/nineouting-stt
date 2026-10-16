"""백엔드(meeting-service)로 전사 줄을 보냄. 형식은 CN팀 2장 2.2 + AI팀 추가 필드 (2026-10-15 확정,
nineouting-ref stt_eval/meeting2/cn_review_stt_2026-10-15.md 3절). 발화자 구분은 하지 않기로 해 speakerLabel은 보내지 않음 (2026-10-16)

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
url이 없으면 파일(<세션>.backend.jsonl)에만 기록. 있으면 파일에도 기록 (결과와 함께)
"""
import datetime, http.client, json, threading, time, urllib.parse

from summary_sender import low_confidence


class BackendSender:
    def __init__(self, url='', period=0.5, log=print, timeout=2.0):
        self.url, self.period, self.log, self.timeout = url.rstrip('/'), period, log, timeout
        self.lock, self.io = threading.Lock(), threading.Lock()
        self.sending = threading.Lock()   # 주기 스레드와 멈춤·종료 처리가 같은 연결로 동시에 보내지 않게
        self.meeting = None          # {'id', 't0'(엔진 초), 'logfile'}
        self.seg, self.dirty = {}, set()   # seq → 최신 segment, 다음에 보낼 seq
        self.stats = {'sent': 0, 'failed': 0, 'requests': 0}
        self.conn = None
        if self.url:
            u = urllib.parse.urlsplit(self.url)
            self.scheme, self.host, self.base = u.scheme, u.netloc, u.path
        threading.Thread(target=self._loop, daemon=True).start()

    def _ms(self, t):
        """엔진 시각(초) → 회의 시작 기준 밀리초. 멈춤 처리로 넣은 무음 구간은 회의 시간에서 뺌"""
        cut = sum(min(dur, max(0.0, t - at)) for at, dur in self.meeting['gaps'])
        return max(0, int(round((t - self.meeting['t0'] - cut) * 1000)))

    def start(self, meeting_id, t0, logfile):
        """새 회의. 이전 회의에서 못 보낸 것은 먼저 보냄"""
        self.flush()
        with self.lock:
            self.meeting = {'id': meeting_id, 't0': t0, 'logfile': logfile, 'gaps': []}
            self.seg, self.dirty = {}, set()

    def gap(self, at, dur):
        """엔진 시각 at부터 dur초는 회의 시간이 아님 (브라우저 입력에서 멈출 때 줄 마무리용으로 넣은 무음)"""
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
        ok, err = self._post(m['id'], {'segments': segs})
        with self.lock:
            if not ok and self.meeting and self.meeting['id'] == m['id']:
                self.dirty |= sent_ids   # 다음 주기에 그 줄들의 최신 상태를 다시 보냄
        with self.io:
            with open(m['logfile'], 'a', encoding='utf-8') as f:
                f.write(json.dumps({'wall': datetime.datetime.now().astimezone().isoformat(timespec='milliseconds'),
                                    'result': 'file' if not self.url else ('ok' if ok else 'fail'), 'error': err,
                                    'segments': segs}, ensure_ascii=False) + '\n')

    def _post(self, meeting_id, body):
        if not self.url: return True, ''
        path = f'{self.base}/internal/meetings/{urllib.parse.quote(str(meeting_id), safe="")}/transcripts'
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
            try: self.flush()
            except Exception as e: self.log(f'백엔드 전송 오류: {e}')
