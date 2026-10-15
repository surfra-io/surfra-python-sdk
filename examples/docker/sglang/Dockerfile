# SGLang with the Surfra supervisor in front.
#   docker build -t ghcr.io/surfra-io/sglang:0.1.0 examples/docker/sglang
FROM lmsysorg/sglang:latest
RUN curl -fsSL https://surfra.io/install.sh | sh
ENV SURFRA_RUNTIME=http \
    SURFRA_HTTP_PORT=8000 \
    SURFRA_HTTP_PATH=/v1/chat/completions \
    SURFRA_HTTP_READY_PATH=/health \
    SURFRA_HTTP_READY_TIMEOUT=1800 \
    SURFRA_ENTRYPOINT="sglang serve --model-path Qwen/Qwen3-0.6B --host 127.0.0.1 --port 8000"
ENTRYPOINT ["surfra", "worker"]
