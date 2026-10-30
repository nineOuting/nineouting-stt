"""STT 프로토타입에 필요한 모델을 공식 배포처에서 받아 체크섬을 확인함. 파이썬 표준 라이브러리만 씀 (윈도우·맥 같은 명령).

  python tools/download_models.py            필수 모델만 (STT chunk-64 + VAD, 약 160MB)
  python tools/download_models.py --all      선택 모델까지 (다른 인식 결과 chunk-16 + 화자 번호, 약 360MB)
  python tools/download_models.py --alt      필수 + 다른 인식 결과(chunk-16)
  python tools/download_models.py --diar     필수 + 화자 번호(TitaNet)
  python tools/download_models.py --check    받지 않고 있는 파일만 검사
  --dir 폴더                                  저장 위치 (기본: .env의 NINEOUTING_STT_MODELS, 없으면 prototype/models)

- 이미 있고 체크섬이 맞는 파일은 건너뜀
- 받는 동안은 <이름>.part에 쓰고, 체크섬이 맞을 때만 원래 이름으로 바꿈. 맞지 않으면 지우고 알림
- 프록시가 필요하면 HTTPS_PROXY 환경 변수를 씀 (urllib 기본 동작)
"""
import argparse, hashlib, os, sys, time, urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # prototype/
sys.path.insert(0, os.path.join(ROOT, 'app'))
from envfile import load_env  # noqa: E402
load_env()   # 서버와 같은 모델 폴더를 쓰도록

HF = 'https://huggingface.co/kangkyu/icefall-asr-ko-streaming-zipformer-174m/resolve/f0e73b1653c3ea75898c6d949dd71c690c9121da'   # 2026-05-08 커밋으로 고정
GH = 'https://github.com/k2-fsa/sherpa-onnx/releases/download'
ST = 'epoch-99-avg-1-chunk-{}-left-128.int8.onnx'

# (묶음, 저장 경로, 주소, SHA-256, 크기 바이트). 체크섬은 배포처 공개값과 맞춰 봄 (2026-10-16)
MODELS = [
    ('필수', 'zipformer-174m/tokens.txt', f'{HF}/tokens.txt', '435dfb9e0a2b6a79124f1a4d8f0f33a951b25384726e2e0d854f081533e6ec9d', 20844),
    ('필수', f'zipformer-174m/encoder-{ST.format(64)}', f'{HF}/encoder-{ST.format(64)}', 'b8fb024cab6991f64ca5d2cecbd8a038c7c85fc95593f4bf90f86afd91eacf76', 156826043),
    ('필수', f'zipformer-174m/decoder-{ST.format(64)}', f'{HF}/decoder-{ST.format(64)}', 'f5dfa6c8609b29da86c739d4904889475c33b62e28e70c619952a0ee6c31416f', 1544210),
    ('필수', f'zipformer-174m/joiner-{ST.format(64)}', f'{HF}/joiner-{ST.format(64)}', '64efd9aeb71fb2278c713b3d5eb5ec45edf6b2ccd8716d3709044f17723c13fd', 1270777),
    ('필수', 'speaker/silero_vad.onnx', f'{GH}/asr-models/silero_vad.onnx', '9e2449e1087496d8d4caba907f23e0bd3f78d91fa552479bb9c23ac09cbb1fd6', 643854),
    ('alt', f'zipformer-174m/encoder-{ST.format(16)}', f'{HF}/encoder-{ST.format(16)}', 'e595e2e37f46078387868ffa656b775de787fa511cf851cf8e1892d7dbdabe54', 156840518),
    ('alt', f'zipformer-174m/decoder-{ST.format(16)}', f'{HF}/decoder-{ST.format(16)}', 'f5dfa6c8609b29da86c739d4904889475c33b62e28e70c619952a0ee6c31416f', 1544210),
    ('alt', f'zipformer-174m/joiner-{ST.format(16)}', f'{HF}/joiner-{ST.format(16)}', '64efd9aeb71fb2278c713b3d5eb5ec45edf6b2ccd8716d3709044f17723c13fd', 1270777),
    ('diar', 'speaker/nemo_en_titanet_small.onnx', f'{GH}/speaker-recongition-models/nemo_en_titanet_small.onnx', 'ad4a1802485d8b34c722d2a9d04249662f2ece5d28a7a039063ca22f515a789e', 40257283),
]


def sha256(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for b in iter(lambda: f.read(1 << 20), b''): h.update(b)
    return h.hexdigest()


def mb(n):
    return f'{n / 1e6:.1f}MB'


def download(url, dest, size):
    part = dest + '.part'
    req = urllib.request.Request(url, headers={'User-Agent': 'nineouting-stt-download-models'})
    with urllib.request.urlopen(req, timeout=60) as r, open(part, 'wb') as f:
        total = int(r.headers.get('Content-Length') or size); got = 0; last = 0.0
        while True:
            b = r.read(1 << 20)
            if not b: break
            f.write(b); got += len(b)
            if time.monotonic() - last > 1 or got == total:
                last = time.monotonic()
                print(f'\r    {mb(got)} / {mb(total)} ({got / max(total, 1) * 100:.0f}%)', end='', flush=True)
    print()
    return part


def main():
    p = argparse.ArgumentParser(description='STT 프로토타입 모델 받기')
    p.add_argument('--all', action='store_true', help='선택 모델까지 모두')
    p.add_argument('--alt', action='store_true', help='다른 인식 결과용 chunk-16 모델')
    p.add_argument('--diar', action='store_true', help='화자 번호용 TitaNet 모델')
    p.add_argument('--check', action='store_true', help='받지 않고 있는 파일만 검사')
    p.add_argument('--dir', default=os.environ.get('NINEOUTING_STT_MODELS') or os.path.join(ROOT, 'models'))
    a = p.parse_args()
    groups = {'필수'} | ({'alt', 'diar'} if a.all else set()) | ({'alt'} if a.alt else set()) | ({'diar'} if a.diar else set())
    todo = [m for m in MODELS if m[0] in groups]
    print(f'저장 위치: {a.dir}')
    print(f'대상: {", ".join(sorted(groups))} · 파일 {len(todo)}개 · 합계 {mb(sum(m[4] for m in todo))}')
    bad = 0
    for group, rel, url, digest, size in todo:
        dest = os.path.join(a.dir, *rel.split('/'))
        if os.path.exists(dest):
            if sha256(dest) == digest:
                print(f'[있음] {rel}'); continue
            print(f'[다름] {rel}: 체크섬이 맞지 않음' + (' → 다시 받음' if not a.check else ''))
            if a.check: bad += 1; continue
        elif a.check:
            print(f'[없음] {rel}'); bad += 1; continue
        print(f'[받기] {rel} ({mb(size)})')
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        try:
            part = download(url, dest, size)
        except OSError as e:
            print(f'    실패: {e}'); bad += 1; continue
        if sha256(part) != digest:
            os.remove(part); print('    체크섬이 맞지 않아 지움 (받는 중 손상 또는 배포 파일 변경)'); bad += 1; continue
        os.replace(part, dest); print('    체크섬 확인')
    if bad:
        print(f'\n문제 {bad}개. 위 메시지를 확인하세요'); sys.exit(1)
    print('\n모두 준비됨' + ('' if groups >= {'alt', 'diar'} else
          '. 선택 모델이 없으면 서버를 --no-alt(다른 인식 결과 끔)로 켜거나, 없을 때 자동으로 꺼짐'))


if __name__ == '__main__':
    main()
