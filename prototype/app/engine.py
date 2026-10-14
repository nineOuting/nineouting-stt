"""실시간 전사 엔진: 메인 스트리밍 STT + VAD 보완 + 화자 번호 + 확신도 + 다른 인식 결과.

설정 근거는 nineouting-ref/stt_eval/meeting2/stt_improve.md (SCRUM-23)와 scrum25_vad_recover.md (SCRUM-25).
- STT: zipformer-174m chunk-64, modified_beam_search, 후보 4, 발화 끊기 2.4/0.8/20초
  (end_silence=말한 뒤 쉼, max_len=최대 길이. 최대 길이로 강제로 끊을 때는 걸쳐 있던 단어를 잃음 →
   overlap초만큼 직전 소리를 새 인식기에 다시 넣어 살림. 측정은 nineouting-ref review/endpoint_cut_check.py)
  다시 넣은 소리에서 앞 줄과 겹쳐 나온 글자는 새 줄 앞에서 뺌 (_trim): 실제 시각이 앞 줄 마지막 글자 이하인 글자 →
  남은 앞부분 중 다시 넣은 구간 안의 글자가 앞 줄 끝(최대 6글자)과 같으면 그만큼. 앞 줄이 내지 못하고 들고 있던 글자는 남김.
  회의 2 정답 10분 CER 9.0 → 5.6%(삽입 111 → 29자, 삭제 42 → 43자), 회의 1 22.6 → 20.7% (nineouting-ref review/overlap_dedup)
  blank_penalty: 디코딩 때 blank(글자 안 냄)에 주는 벌점. 말을 건너뛰는 삭제를 줄이는 대신 대체·삽입이 조금 늚.
  0.5에서 삭제 회의 1 89 → 47자, 회의 2 43 → 23자, CER 20.7 → 19.8%, 5.6 → 5.7% (1.0부터 삽입 급증, nineouting-ref review/decode_opts).
  빠진 말은 화면에서 안 보여 사용자가 고치기 어렵고, 틀린 글자는 보여서 고칠 수 있어 삭제를 줄이는 쪽을 고름. 서버 기본 0.5
- VAD 보완: Silero VAD가 말소리라고 본 구간 중 메인 STT 출력이 덮지 않는 0.5초 이상 부분을,
  말소리 끝 + 2.0초 뒤에 새 스트림으로 다시 인식해 끼움 (한글이 없는 출력은 버림)
- 화자 번호: TitaNet-small, 발화마다 누적 비교 (Diarizer). 번호만 나옴
- 확신도: 고른 글자에 모델이 준 확률(ys_probs, 로그 확률 → 0~1). 메인 STT 글자 기준
  0.3 미만이면 실제 오류일 확률 약 27%, 오류의 약 24%가 걸림 (회의 2 정답 10분, confidence_check.py)
- 다른 인식 결과: 보완 줄 전부와 확신도 0.3 미만 글자가 있는 메인 줄을 chunk-16(빔 탐색)·chunk-64(greedy)로
  다시 인식. 기본 결과와 다를 때만 알림. 별도 스레드에서 처리해 메인 전사를 막지 않음

소리를 feed()로 0.1초씩 넣으면 on_event(dict)가 호출됨 (alt는 작업 스레드에서 호출됨).
  {'type': 'partial',   'id', 'start', 'text'}                          말하는 중 (같은 id가 계속 갱신)
  {'type': 'final',     'id', 'start', 'end', 'text', 'conf', 'spk', 'spk_info', 'kind'}   발화 확정. conf는 text 글자별 확신도(0~1)
  {'type': 'recovered', 'id', 'start', 'end', 'text', 'conf', 'spk', 'spk_info', 'kind'}   VAD 보완 (시각이 앞선 자리에 끼워야 함)
  spk_info: 화자 판단 근거 (Diarizer.assign 참고)
  kind: 'backchannel'(맞장구) / 'filler'(간투사) / None (line_kind 참고)
  {'type': 'alt',       'id', 'alts': [{'name', 'text'}, ...]}         같은 구간의 다른 인식 결과 (기본과 다른 것만)
"""
import os, queue, re, threading, types
import numpy as np
import sherpa_onnx

SR = 16000
DEFAULT_MODELS = os.path.expanduser('~/Dev/nineouting-ref/stt_models')
LOW_CONF = 0.3
DEDUP_CHARS = 6   # 강제 분할 뒤 새 줄 앞에서 앞 줄 끝과 맞춰 볼 최대 글자 수


def make_rec(model_dir, chunk, method, endpoint=False, hotwords='', hotwords_score=1.0, end_silence=0.8, max_len=20.0, blank_penalty=0.0):
    st = f'-epoch-99-avg-1-chunk-{chunk}-left-128.int8.onnx'
    for f in (f'{model_dir}/tokens.txt', f'{model_dir}/encoder{st}'):
        if not os.path.exists(f):
            raise FileNotFoundError(f'모델 파일 없음: {f} (--models로 경로 지정)')
    return sherpa_onnx.OnlineRecognizer.from_transducer(
        tokens=f'{model_dir}/tokens.txt', encoder=f'{model_dir}/encoder{st}', decoder=f'{model_dir}/decoder{st}',
        joiner=f'{model_dir}/joiner{st}', num_threads=1, sample_rate=SR, feature_dim=80, decoding_method=method,
        enable_endpoint_detection=endpoint, rule1_min_trailing_silence=2.4, rule2_min_trailing_silence=end_silence,
        rule3_min_utterance_length=max_len, hotwords_file=hotwords if method == 'modified_beam_search' else '',
        hotwords_score=hotwords_score, modeling_unit='cjkchar', blank_penalty=blank_penalty)


def decode(rec, seg):
    """구간 소리를 새 스트림으로 끝까지 인식해 결과 객체를 돌려줌."""
    x = rec.create_stream()
    for i in range(0, len(seg), 1600): x.accept_waveform(SR, seg[i:i + 1600])
    x.accept_waveform(SR, np.zeros(int(SR * 0.8), np.float32)); x.input_finished()
    while rec.is_ready(x): rec.decode_stream(x)
    return rec.get_result_all(x)


def text_conf(r):
    """결과 객체 → (텍스트, 글자별 확신도). 글자 조각(▁는 공백)마다 그 조각의 확률을 붙임."""
    chars, conf = [], []
    for tk, p in zip(r.tokens, r.ys_probs):
        pr = float(np.exp(p))
        for ch in tk.replace('▁', ' '):
            chars.append(ch); conf.append(1.0 if ch == ' ' else pr)
    while chars and chars[0] == ' ': chars.pop(0); conf.pop(0)
    while chars and chars[-1] == ' ': chars.pop(); conf.pop()
    return ''.join(chars), [round(c, 2) for c in conf]


def squash(t):
    return re.sub(r'\s+', '', t)


# 맞장구·간투사 구분 (SCRUM-25 8-1절: VAD 보완의 짧은 줄은 대부분 실제 맞장구·간투사라 버리지 않고 표시만 함)
# 줄 전체가 아래 낱말로만 이뤄졌을 때만 표시. "네 그건 좋은데요"처럼 다른 말이 섞이면 표시하지 않음
BACKCHANNEL = ['알겠습니다', '그러네요', '맞습니다', '그렇죠', '맞아요', '그쵸', '맞아', '오케이', '네', '예', '응']
FILLER = ['그러니까', '뭔가', '어', '음', '아', '으', '그', '저', '뭐', '이', '좀']
_KIND_RE = re.compile('|'.join(sorted(BACKCHANNEL + FILLER, key=len, reverse=True)))


def line_kind(text):
    """'backchannel'(맞장구) / 'filler'(간투사만) / None(일반 발언). 맞장구 낱말이 하나라도 있으면 맞장구."""
    t = re.sub(r'[\s.,?!~…]', '', text or '')
    if not t or len(t) > 8: return None
    words = _KIND_RE.findall(t)
    if ''.join(words) != t: return None
    return 'backchannel' if any(w in BACKCHANNEL for w in words) else 'filler'


class Diarizer:
    """발화마다 TitaNet-small 임베딩을 뽑아 누적 화자 중심과 비교하는 온라인 화자 판별.
    방식과 설정은 nineouting-ref/stt_eval/poc/summary.md 4절 (pipe.py·stream_sim.py와 같은 방식).
    설정 E: 유사도 0.60 이상이면 그 화자, 미만이면 새 화자. 단 2초 미만 발화는 새 화자로 등록하지 않고 가장 가까운 화자에 배정.
    화자 번호는 1부터. 이름은 모름."""

    def __init__(self, model, th=0.60, min_new=2.0, max_speakers=0):
        if not os.path.exists(model):
            raise FileNotFoundError(f'화자 모델 없음: {model}')
        self.ex = sherpa_onnx.SpeakerEmbeddingExtractor(sherpa_onnx.SpeakerEmbeddingExtractorConfig(model=model, num_threads=1))
        self.th, self.min_new, self.cents = th, min_new, []
        self.max = max_speakers          # 참석자 수 (0이면 자동). 다 차면 새 화자를 만들지 않음 (PoC 4.6절의 조건부 설정)
        self.merged = {}                 # 합쳐진 화자 번호 → 합친 대상 번호. 합친 번호는 다시 쓰지 않음
        self.lock = threading.Lock()     # 화면에서 합치기·인원 변경이 인식 스레드와 동시에 올 수 있음

    def active(self):
        return [k + 1 for k in range(len(self.cents)) if (k + 1) not in self.merged]

    def assign(self, audio, dur):
        """(화자 번호, 판단 근거). 근거: sims=화자별 유사도(화자 1부터, 합쳐진 화자는 None), best=가장 비슷한 화자, sim=그 유사도,
        reason=first(첫 발화)/match(기준 이상)/short(기준 미만이지만 짧아 가장 가까운 화자로)/
               cap(기준 미만이지만 참석자 수가 다 차서 가장 가까운 화자로)/new(기준 미만 → 새 화자)"""
        x = self.ex.create_stream(); x.accept_waveform(SR, audio); x.input_finished()
        v = np.array(self.ex.compute(x)); v /= np.linalg.norm(v) + 1e-9
        with self.lock:
            info = {'th': self.th, 'min_new': self.min_new, 'max': self.max, 'dur': round(dur, 2), 'sims': []}
            act = self.active()
            if act:
                sims = [None if (k + 1) in self.merged else float(c @ v / np.linalg.norm(c)) for k, c in enumerate(self.cents)]
                k = max(act, key=lambda n: sims[n - 1]) - 1
                info.update(sims=[None if s is None else round(s, 2) for s in sims], best=k + 1, sim=round(sims[k], 2))
                full = self.max and len(act) >= self.max
                if sims[k] >= self.th or dur < self.min_new or full:
                    self.cents[k] = self.cents[k] + v
                    info['reason'] = 'match' if sims[k] >= self.th else ('short' if dur < self.min_new else 'cap')
                    return k + 1, info
                info['reason'] = 'new'
            else:
                info['reason'] = 'first'
            self.cents.append(v.copy())
            return len(self.cents), info

    def merge(self, src, dst):
        """화자 src를 dst로 합침. 이후 판단에서 src는 쓰지 않고 dst 중심에 src 특징을 더함."""
        with self.lock:
            n = len(self.cents)
            if not (1 <= src <= n and 1 <= dst <= n) or src == dst or src in self.merged or dst in self.merged:
                return False
            self.cents[dst - 1] = self.cents[dst - 1] + self.cents[src - 1]
            self.merged[src] = dst
            return True


class AltWorker:
    """같은 구간을 다른 설정으로 다시 인식하는 작업 스레드. 메인 전사를 막지 않음."""

    def __init__(self, model_dir, on_event):
        self.recs = [('chunk-16', make_rec(model_dir, '16', 'modified_beam_search')),
                     ('chunk-64 greedy', make_rec(model_dir, '64', 'greedy_search'))]
        self.on_event, self.q = on_event, queue.Queue()
        threading.Thread(target=self._run, daemon=True).start()

    def submit(self, i, seg, main_text):
        self.q.put((i, seg, main_text))

    def _run(self):
        while True:
            i, seg, main = self.q.get()
            try:
                alts, seen = [], {squash(main)}
                for name, rec in self.recs:
                    t = text_conf(decode(rec, seg))[0]
                    if t and re.search('[가-힣]', t) and squash(t) not in seen:
                        alts.append({'name': name, 'text': t}); seen.add(squash(t))
                if alts: self.on_event({'type': 'alt', 'id': i, 'alts': alts})
            finally:
                self.q.task_done()

    def drain(self):
        self.q.join()


class Engine:
    def __init__(self, on_event, model_dir=None, vad_model=None, chunk='64', hotwords='', hotwords_score=1.0,
                 wait=2.0, min_gap=0.5, recover=True, keep_seconds=120, spk_model=None, diar=True, alt=True,
                 max_speakers=0, end_silence=0.8, max_len=20.0, overlap=0.0, blank_penalty=0.0):
        model_dir = model_dir or f'{DEFAULT_MODELS}/zipformer-174m'
        vad_model = vad_model or f'{DEFAULT_MODELS}/speaker/silero_vad.onnx'
        if not os.path.exists(vad_model):
            raise FileNotFoundError(f'VAD 모델 없음: {vad_model}')
        self.rec = make_rec(model_dir, chunk, 'modified_beam_search', endpoint=True, hotwords=hotwords, hotwords_score=hotwords_score,
                            end_silence=end_silence, max_len=max_len, blank_penalty=blank_penalty)   # VAD 보완 다시 인식도 이 인식기를 씀
        self.max_len, self.overlap, self.end_silence = max_len, overlap, end_silence
        self.shift = 0.0     # 인식기 시각 - 실제 시각 (강제 분할 때 소리를 다시 넣은 만큼 앞섬)
        self.seg_len = 0.0   # 지금 줄을 시작한 뒤 인식기에 넣은 소리 길이 (강제 분할 판단용)
        self.dedup = None    # 강제 분할 직후: {'t': 앞 줄 마지막 글자 실제 시각, 'cut': 끊은 시각, 'tail': 앞 줄 끝 글자}
        self.diar = Diarizer(spk_model or f'{DEFAULT_MODELS}/speaker/nemo_en_titanet_small.onnx',
                             max_speakers=max_speakers) if diar else None
        self.alt = AltWorker(model_dir, on_event) if alt else None
        cfg = sherpa_onnx.VadModelConfig()
        cfg.silero_vad.model = vad_model; cfg.silero_vad.threshold = 0.5
        cfg.silero_vad.min_silence_duration = 0.25; cfg.silero_vad.min_speech_duration = 0.25
        cfg.silero_vad.max_speech_duration = 120; cfg.sample_rate = SR; cfg.num_threads = 1
        self.vad = sherpa_onnx.VoiceActivityDetector(cfg, buffer_size_in_seconds=60)
        self.win = cfg.silero_vad.window_size
        self.on_event, self.wait, self.min_gap, self.recover_on = on_event, wait, min_gap, recover
        self.keep = keep_seconds
        self.s = self.rec.create_stream()
        self.fed = 0.0; self.buf = np.zeros(0, np.float32); self.buf_t0 = 0.0  # 최근 소리 (메모리에만)
        self.vad_q = np.zeros(0, np.float32); self.cover = []; self.pending = []
        self.next_id = 0; self.cur_id = None; self.shown = ''
        self.restart = False   # 화면 지우기 때 서버가 켬: 다음 소리부터 새 줄로 시작 (지우기 전에 시작한 줄에 이어 붙어 버려지는 것을 막음)

    def _new_id(self):
        self.next_id += 1
        return self.next_id

    def _span(self, r):
        t = r.start_time - self.shift
        return (t + r.timestamps[0], t + r.timestamps[-1] + 0.2) if len(r.timestamps) else None

    def _trim(self, r):
        """강제 분할 뒤 새 줄 결과에서 앞 줄과 겹친 앞부분 글자를 뺀 결과 (겹친 글자가 없으면 r 그대로)."""
        d = self.dedup
        if not d or not len(r.timestamps): return r
        t0 = r.start_time - self.shift
        toks, tss = list(r.tokens), list(r.timestamps)
        k = 0
        while k < len(toks) and t0 + tss[k] <= d['t']: k += 1   # 앞 줄이 이미 낸 소리 구간의 글자
        cand = [i for i in range(k, len(toks)) if t0 + tss[i] < d['cut']][:DEDUP_CHARS]
        head = ''.join(toks[i].replace('▁', '') for i in cand)
        best = next((n for n in range(min(len(head), len(d['tail'])), 0, -1) if head[:n] == d['tail'][-n:]), 0)
        n = 0
        while n < best and k < len(toks):   # 글자 시각이 늦게 찍혀 위에서 빠져나간 중복
            n += len(toks[k].replace('▁', '')); k += 1
        if not k: return r
        return types.SimpleNamespace(tokens=toks[k:], ys_probs=list(r.ys_probs)[k:], timestamps=tss[k:],
                                     start_time=r.start_time, text=''.join(toks[k:]).replace('▁', ' ').strip())

    def _audio(self, t0, t1):
        b0, b1 = max(self.buf_t0, t0), min(self.fed, t1)
        return self.buf[int((b0 - self.buf_t0) * SR):int((b1 - self.buf_t0) * SR)].copy()

    def feed(self, c):
        """0.1초 안팎의 float32 모노 16kHz 소리."""
        s, rec = self.s, self.rec
        if self.restart:   # 말하던 줄은 버리고(지우기 전 내용) 인식기를 새로 시작
            self.restart = False
            while rec.is_ready(s): rec.decode_stream(s)
            rec.reset(s); self.cur_id = None; self.shown = ''; self.seg_len = 0.0; self.dedup = None
            self.pending = [q for q in self.pending if q[1] > self.fed]
        s.accept_waveform(SR, c); self.fed += len(c) / SR; self.seg_len += len(c) / SR
        self.buf = np.concatenate([self.buf, c])
        if len(self.buf) > SR * self.keep:
            cut = len(self.buf) - SR * self.keep; self.buf = self.buf[cut:]; self.buf_t0 += cut / SR
        while rec.is_ready(s): rec.decode_stream(s)
        raw = rec.get_result_all(s); r = self._trim(raw)
        cur = self._span(r); text = text_conf(r)[0]  # 확정 줄과 같게 띄어쓰기 포함
        if rec.is_endpoint(s):
            # 쉼이 아니라 최대 길이로 끊김: 줄이 최대 길이에 닿았고, 마지막 글자 뒤 쉼이 쉼 기준보다 짧음
            tail = self.seg_len - (raw.timestamps[-1] if len(raw.timestamps) else 0.0)
            forced = self.seg_len >= self.max_len - 0.3 and tail < self.end_silence
            if text and cur: self._final(cur, r)
            self.dedup = None
            if forced and self.overlap > 0 and len(raw.timestamps):
                self.dedup = {'t': raw.start_time - self.shift + raw.timestamps[-1], 'cut': self.fed, 'tail': squash(text)[-DEDUP_CHARS:]}
            rec.reset(s); cur = None; self.cur_id = None; self.shown = ''; self.seg_len = 0.0
            if forced and self.overlap > 0:   # 걸쳐 있던 단어를 새 줄에서 다시 듣게 함 (겹친 글자는 _trim이 뺌)
                seg = self._audio(self.fed - self.overlap, self.fed)
                s.accept_waveform(SR, seg); self.shift += len(seg) / SR; self.seg_len = len(seg) / SR
                while rec.is_ready(s): rec.decode_stream(s)
        elif text and text != self.shown and cur:
            if self.cur_id is None: self.cur_id = self._new_id()
            self.shown = text
            self.on_event({'type': 'partial', 'id': self.cur_id, 'start': round(cur[0], 2), 'text': text})
        if self.recover_on:
            self.vad_q = np.concatenate([self.vad_q, c])
            while len(self.vad_q) >= self.win:
                self.vad.accept_waveform(self.vad_q[:self.win]); self.vad_q = self.vad_q[self.win:]
            while not self.vad.empty():
                vs = self.vad.front.start / SR; self.pending.append((vs, vs + len(self.vad.front.samples) / SR)); self.vad.pop()
            for seg in [q for q in self.pending if self.fed >= q[1] + self.wait]:
                self.pending.remove(seg); self._recover(seg[0], seg[1], cur)

    def _speaker(self, t0, t1):
        """t0~t1 구간 (화자 번호, 판단 근거). 화자 판별을 끄거나 너무 짧으면 (None, None)."""
        if not self.diar: return None, None
        seg = self._audio(t0, t1)
        return self.diar.assign(seg, t1 - t0) if len(seg) >= SR * 0.3 else (None, None)

    def _final(self, cur, r):
        i = self.cur_id or self._new_id()
        end = min(cur[1], self.fed)
        self.cover.append((cur[0], end))
        text, conf = text_conf(r)
        spk, spk_info = self._speaker(cur[0] - 0.2, end + 0.1)  # PoC와 같은 범위: 첫 글자 -0.2초 ~ 마지막 글자 +0.3초
        self.on_event({'type': 'final', 'id': i, 'start': round(cur[0], 2), 'end': round(end, 2),
                       'text': text, 'conf': conf, 'spk': spk, 'spk_info': spk_info, 'kind': line_kind(text)})
        if self.alt and any(c < LOW_CONF for c in conf):
            self.alt.submit(i, self._audio(cur[0] - 0.2, end + 0.2), text)

    def _recover(self, vs, ve, cur):
        cov = sorted(self.cover + ([cur] if cur else []))
        gaps, x = [], vs
        for c0, c1 in cov:
            if c1 <= x or c0 >= ve: continue
            if c0 > x: gaps.append((x, c0))
            x = max(x, c1)
        if x < ve: gaps.append((x, ve))
        for g0, g1 in gaps:
            if g1 - g0 < self.min_gap: continue
            seg = self._audio(g0 - 0.2, g1 + 0.2)
            if len(seg) < SR * 0.3: continue
            text, conf = text_conf(decode(self.rec, seg))
            if re.search('[가-힣]', text):
                self.cover.append((g0, g1))
                i = self._new_id(); spk, spk_info = self._speaker(g0 - 0.2, g1 + 0.2)
                self.on_event({'type': 'recovered', 'id': i, 'start': round(g0, 2), 'end': round(g1, 2),
                               'text': text, 'conf': conf, 'spk': spk, 'spk_info': spk_info, 'kind': line_kind(text)})
                if self.alt: self.alt.submit(i, seg, text)

    def finish(self):
        s, rec = self.s, self.rec
        s.accept_waveform(SR, np.zeros(int(SR * 0.8), np.float32)); s.input_finished()
        while rec.is_ready(s): rec.decode_stream(s)
        r = self._trim(rec.get_result_all(s)); cur = self._span(r)
        if r.text.strip() and cur: self._final(cur, r)
        if self.alt: self.alt.drain()
