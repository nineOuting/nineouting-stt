r"""이 컴퓨터(또는 파드)에서 동시 회의를 몇 개까지 실시간으로 처리하는지 재는 프로그램 (SCRUM-288).

시험용 STT 서버를 다른 포트로 하나 더 띄우고(입장권·백엔드 없음, 운영과 같은 --no-alt --no-diar),
회의 N개가 녹음의 다른 구간을 실제 속도로 보내면서, 서버 /health의 backlog(아직 처리하지 못한 소리)가 쌓이는지 봄.
  따라감: 뒤 절반 동안 backlog 평균이 1초 미만이고, 끝날 때도 1초 미만
  밀림:   그 밖 (CPU가 모자라 소리가 쌓임)
최대 속도로 넣어 CPU 비용을 곱하는 방식은 쓰지 않음: 실제 속도로 넣으면 같은 소리에 CPU가 약 2배 들고(개발자 맥 측정),
회의 수가 늘수록 회의당 CPU도 늘어 계산값이 실제보다 크게 나옴

전사 내용은 출력하지 않고 숫자만 출력함. 시험 서버의 파일은 임시 폴더에 쓰고 끝나면 지움.
시험 서버는 이 프로그램이 띄운 프로세스만 끔

실행 (prototype 기준. 파드 안에서는 /app 기준)
  python tools/capacity_probe.py --wav 녹음.wav
  python tools/capacity_probe.py --wav 녹음.wav --n 1,2,3,4,6 --sec 90

녹음: 16kHz 모노 16비트 wav, 말소리가 있는 것 (잡음·무음이면 실제보다 가볍게 나옴). 길이는 --sec 이상 권장
  바꾸기: ffmpeg -i 원본 -ar 16000 -ac 1 -sample_fmt s16 녹음.wav

파드에서 돌릴 때
  - 실행 중인 STT와 CPU를 같이 쓰므로 진행 중인 회의가 없을 때 돌림.
    시작 전에 같은 파드의 /health를 보고 열린 회의가 있으면 멈춤 (--force로 무시)
  - 회의 수 하나마다 --sec초 + 밀린 소리를 비우는 시간이 걸림 (기본 설정으로 약 10분)
  - 그동안 CPU를 파드 상한까지 쓸 수 있음
"""
import argparse, json, os, platform, signal, subprocess, sys, tempfile, threading, time, urllib.request, wave

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
p = argparse.ArgumentParser(description='동시 회의 실시간 처리 용량 측정 (숫자만 출력)')
p.add_argument('--wav', required=True, help='16kHz 모노 16비트 wav (말소리)')
p.add_argument('--sec', type=float, default=90, help='회의 하나에 넣을 소리 길이(초)')
p.add_argument('--n', default='1,2,3,4,6', help='동시 회의 수 (쉼표로 여러 개, 작은 것부터)')
p.add_argument('--port', type=int, default=18770, help='시험 서버의 음성 포트 (화면 포트는 이 값 - 5)')
p.add_argument('--health', default='http://127.0.0.1:8770/health', help='실행 중인 STT의 상태 주소. 비우면 확인하지 않음')
p.add_argument('--force', action='store_true', help='열린 회의가 있어도 돌림')
A = p.parse_args()
SR = 16000


def cpu_limit():
    """컨테이너 CPU 상한 (cgroup). 없거나 읽지 못하면 None"""
    try:
        q, per = open('/sys/fs/cgroup/cpu.max').read().split()   # cgroup v2: "200000 100000" 또는 "max 100000"
        return None if q == 'max' else int(q) / int(per)
    except (OSError, ValueError):
        pass
    try:
        q = int(open('/sys/fs/cgroup/cpu/cpu.cfs_quota_us').read()); per = int(open('/sys/fs/cgroup/cpu/cpu.cfs_period_us').read())
        return None if q <= 0 else q / per
    except (OSError, ValueError):
        return None


def proc_cpu(pid):
    """프로세스가 지금까지 쓴 CPU 초. 리눅스 /proc, 없으면 ps"""
    try:
        f = open(f'/proc/{pid}/stat').read().rsplit(')', 1)[1].split()
        return (int(f[11]) + int(f[12])) / os.sysconf('SC_CLK_TCK')
    except OSError:
        t = subprocess.run(['ps', '-o', 'time=', '-p', str(pid)], capture_output=True, text=True).stdout.strip()
        parts = [float(v) for v in t.replace('-', ':').split(':')] if t else [0.0]
        s = 0.0
        for v in parts: s = s * 60 + v
        return s


def proc_rss_mib(pid):
    try:
        for line in open(f'/proc/{pid}/status'):
            if line.startswith('VmRSS:'): return round(int(line.split()[1]) / 1024)
    except OSError:
        out = subprocess.run(['ps', '-o', 'rss=', '-p', str(pid)], capture_output=True, text=True).stdout.strip()
        return round(int(out) / 1024) if out else None


def get(url):
    return json.load(urllib.request.urlopen(url, timeout=3))


def main():
    from websockets.sync.client import connect
    if A.health:
        try:
            h = get(A.health)
            if h.get('meetings') and not A.force:
                sys.exit(f'열린 회의가 있어 멈춤: {len(h["meetings"])}개. 회의가 없을 때 다시 실행 (무시하려면 --force)')
        except (OSError, ValueError):
            pass   # STT가 없는 컴퓨터에서 돌릴 때
    w = wave.open(A.wav)
    if (w.getframerate(), w.getnchannels(), w.getsampwidth()) != (SR, 1, 2):
        sys.exit(f'녹음 형식이 {w.getframerate()}Hz {w.getnchannels()}채널 {w.getsampwidth() * 8}비트임. '
                 f'16kHz 모노 16비트로 바꿔 주세요: ffmpeg -i 원본 -ar 16000 -ac 1 -sample_fmt s16 녹음.wav')
    total = w.getnframes() / SR
    sec = min(A.sec, total)
    if total < A.sec: print(f'[알림] 녹음이 {total:.0f}초라 --sec {A.sec:.0f}보다 짧음. {sec:.0f}초만 넣음', flush=True)
    pcm = w.readframes(w.getnframes())
    nb = int(sec * SR) * 2
    ns = [int(v) for v in A.n.split(',') if v.strip()]

    def segment(i):   # 회의마다 다른 구간 (녹음이 짧으면 처음부터 다시)
        start = (int(i * 37.0 * SR) * 2) % max(2, len(pcm) - nb + 2)
        return pcm[start - start % 2:start - start % 2 + nb]

    ws_url, health = f'ws://127.0.0.1:{A.port}', f'http://127.0.0.1:{A.port}/health'
    with tempfile.TemporaryDirectory(prefix='stt-probe-') as tmp:
        cmd = [sys.executable, os.path.join(ROOT, 'app', 'server.py'), '--input', 'ws', '--no-alt', '--no-diar', '--no-ticket',
               '--ws-host', '127.0.0.1', '--ws-port', str(A.port), '--port', str(A.port - 5), '--host', '127.0.0.1',
               '--max-meetings', str(max(ns) + 1), '--save-dir', tmp]
        env = dict(os.environ, BACKEND_URL='')   # 백엔드로 보내지 않음 (파일에만, 임시 폴더라 끝나면 지워짐)
        srv = subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL, stderr=open(os.path.join(tmp, 'server.log'), 'w'))
        try:
            for _ in range(120):
                try: get(health); break
                except (OSError, ValueError): time.sleep(1)
                if srv.poll() is not None: sys.exit('시험 서버가 뜨지 못함:\n' + open(os.path.join(tmp, 'server.log')).read()[-2000:])
            else:
                sys.exit('시험 서버가 120초 안에 뜨지 않음')
            print(f'환경: {platform.machine()} · CPU {os.cpu_count()}개 보임 · 컨테이너 CPU 상한 {cpu_limit() or "없음"} · '
                  f'회의당 소리 {sec:.0f}초 · 시험 서버 메모리 {proc_rss_mib(srv.pid)}MiB (회의 없음)', flush=True)
            print('| 동시 회의 | 결과 | backlog 최대(초) | backlog 뒤 절반 평균(초) | backlog 끝(초) | 시험 서버 CPU 평균(코어) | 메모리(MiB) |', flush=True)
            print('|---|---|---|---|---|---|---|', flush=True)
            for n in ns:
                errs = []

                def feed(i, mid):
                    try:
                        data = segment(i)
                        with connect(f'{ws_url}/ws/stt/meetings/{mid}') as c:
                            c.send(json.dumps({'type': 'start', 'meetingId': mid, 'offsetMs': 0})); c.recv(timeout=10)
                            t0 = time.perf_counter()
                            for k, q in enumerate(range(0, len(data), 3200)):
                                c.send(data[q:q + 3200]); d = (k + 1) * 0.1 - (time.perf_counter() - t0)
                                if d > 0: time.sleep(d)
                            c.send(json.dumps({'type': 'end'})); c.recv(timeout=30)
                    except Exception as e:
                        errs.append(repr(e))
                ths = [threading.Thread(target=feed, args=(i, str(900000 + n * 100 + i))) for i in range(n)]
                c0, w0 = proc_cpu(srv.pid), time.perf_counter()
                for h in ths: h.start()
                back, mem = [], 0
                while any(h.is_alive() for h in ths):
                    try:
                        b = get(health).get('backlog') or {}
                        if b: back.append(max(b.values()))
                    except (OSError, ValueError): pass
                    mem = max(mem, proc_rss_mib(srv.pid) or 0)
                    time.sleep(1)
                for h in ths: h.join()
                cpu = (proc_cpu(srv.pid) - c0) / (time.perf_counter() - w0)
                half = back[len(back) // 2:] or [0.0]
                late, end = sum(half) / len(half), (back[-1] if back else 0.0)
                ok = late < 1.0 and end < 1.0 and not errs
                print(f'| {n} | {"따라감" if ok else "밀림"} | {max(back or [0.0]):.1f} | {late:.2f} | {end:.1f} | {cpu:.2f} | {mem} |', flush=True)
                if errs: print(f'  연결 오류 {len(errs)}개: {errs[0][:200]}', flush=True)
                # 밀린 소리가 다 빠질 때까지 기다린 뒤 다음 회의 수 (최대 5분). 서버는 end를 받으면 회의를 목록에서 바로 빼고
                # 밀린 소리는 그 뒤에도 처리하므로, 목록이 아니라 시험 서버 CPU가 거의 쉬는지(2초 동안 0.05코어 미만)로 봄
                for _ in range(150):
                    a = proc_cpu(srv.pid); time.sleep(2)
                    if (proc_cpu(srv.pid) - a) / 2 < 0.05: break
        finally:
            if srv.poll() is None:
                srv.send_signal(signal.SIGTERM)   # 이 프로그램이 띄운 시험 서버만 끔
                try: srv.wait(timeout=20)
                except subprocess.TimeoutExpired: srv.kill(); srv.wait()


if __name__ == '__main__':
    main()
