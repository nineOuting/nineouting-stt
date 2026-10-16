"""prototype/.env 읽기 (표준 라이브러리만). 형식은 .env.example 참고: 한 줄에 이름=값, #으로 시작하는 줄은 주석.
값을 정하는 순서: 명령 옵션 > 이미 설정된 환경 변수 > .env > 코드 기본값 (.env는 환경 변수를 덮지 않음)
"""
import os

ENV_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')


def load_env(path=ENV_FILE):
    """.env가 있으면 읽어 환경 변수에 없는 값만 채움. 읽었으면 경로, 없으면 None"""
    if not os.path.exists(path): return None
    with open(path, encoding='utf-8-sig') as f:   # -sig: 윈도우 메모장이 붙이는 BOM을 뗌
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line: continue
            k, v = (s.strip() for s in line.split('=', 1))
            if len(v) >= 2 and v[0] == v[-1] and v[0] in '"\'': v = v[1:-1]
            if v: os.environ.setdefault(k, v)   # 빈 값은 기본값을 쓰도록 넣지 않음
    return path
