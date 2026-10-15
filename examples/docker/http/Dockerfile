# CPU test image. POST / echoes the JSON body. GET /health is the ready check.
#   docker build -t ghcr.io/surfra-io/http:0.1.0 examples/docker/http
FROM python:3.12-slim
ARG TARGETARCH
ADD --chmod=755 https://github.com/surfra-io/surfra-worker/releases/latest/download/surfra-linux-${TARGETARCH} /usr/local/bin/surfra
COPY server.py /server.py
ENV SURFRA_RUNTIME=http \
    SURFRA_HTTP_PORT=8000 \
    SURFRA_HTTP_PATH=/ \
    SURFRA_HTTP_READY_PATH=/health \
    SURFRA_ENTRYPOINT="python /server.py"
ENTRYPOINT ["surfra", "worker"]
