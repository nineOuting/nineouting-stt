# STT 서버 이미지. 빌드 컨텍스트는 이 폴더(prototype/)
#   docker build -t nineouting-stt prototype/
#   docker run -p 8770:8770 -e BACKEND_URL=http://meeting-service:8083 nineouting-stt
#
# 층 순서는 "잘 안 바뀌는 것부터". 코드만 고치면 모델 160MB를 다시 받지 않음
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# 1) 의존성 (requirements.txt가 바뀔 때만 다시 깔림)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2) 모델 약 160MB (모델이 바뀔 때만 다시 받음). 공식 배포처에서 받고 SHA-256을 확인함
#    받는 스크립트가 envfile을 쓰므로 그 파일만 먼저 넣음
COPY app/envfile.py app/envfile.py
COPY tools/download_models.py tools/download_models.py
RUN python tools/download_models.py

# 3) 코드 (커밋마다 바뀌는 부분이라 맨 뒤)
COPY app/ app/
COPY tools/ tools/

# root가 아닌 사용자로 실행 (ai-service와 같은 uid·방식). 실행 중에 쓰는 곳은 전사 폴더뿐이라
# 그 폴더만 소유자를 바꾸고, 모델·코드는 root 소유 그대로 읽기만 함
RUN useradd --uid 10001 --create-home app \
 && mkdir -p /app/transcripts && chown 10001 /app/transcripts
USER 10001

# 음성 포트만 밖으로 열고, 개발 화면은 컨테이너 안에서만 (로그인이 없는 화면이라 노출하지 않음).
# 두 값 모두 실행할 때 환경변수로 덮을 수 있음
ENV WS_HOST=0.0.0.0 \
    STT_HOST=127.0.0.1

EXPOSE 8770

# 엔진이 뜨는 데 시간이 걸려 start-period를 둠. /health는 음성 포트에도 있음
HEALTHCHECK --interval=30s --timeout=5s --start-period=90s --retries=3 \
    CMD python -c "import urllib.request as u, sys; sys.exit(0 if u.urlopen('http://127.0.0.1:8770/health', timeout=4).status == 200 else 1)"

# 전사 파일은 /app/transcripts에 쌓임. 백엔드로도 보내므로 남기지 않아도 되지만,
# 보관하려면 그 경로를 볼륨으로 붙이면 됨 (uid 10001이 쓸 수 있어야 함)
CMD ["python", "app/server.py", "--input", "ws", "--no-alt", "--no-diar"]
