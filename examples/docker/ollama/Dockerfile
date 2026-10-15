# Ollama with the Surfra supervisor in front.
#   docker build -t ghcr.io/surfra-io/ollama:0.1.0 examples/docker/ollama
FROM ollama/ollama:latest
ARG TARGETARCH
ADD --chmod=755 https://github.com/surfra-io/surfra-worker/releases/latest/download/surfra-linux-${TARGETARCH} /usr/local/bin/surfra
ENV OLLAMA_HOST=127.0.0.1:11434 \
    SURFRA_RUNTIME=http \
    SURFRA_HTTP_PORT=11434 \
    SURFRA_HTTP_PATH=/api/generate \
    SURFRA_HTTP_READY_PATH=/api/version \
    SURFRA_HTTP_READY_TIMEOUT=1800 \
    SURFRA_ENTRYPOINT="ollama serve"
ENTRYPOINT ["surfra", "worker"]
