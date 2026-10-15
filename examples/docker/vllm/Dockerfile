# vLLM with the Surfra supervisor in front. The child stays on localhost.
#   docker build -t ghcr.io/surfra-io/vllm:0.1.0 examples/docker/vllm
FROM vllm/vllm-openai:latest
RUN curl -fsSL https://surfra.io/install.sh | sh
ENV SURFRA_RUNTIME=http \
    SURFRA_HTTP_PORT=8000 \
    SURFRA_HTTP_PATH=/v1/chat/completions \
    SURFRA_HTTP_READY_PATH=/health \
    SURFRA_HTTP_READY_TIMEOUT=1800 \
    SURFRA_ENTRYPOINT="vllm serve --model Qwen/Qwen3-0.6B --host 127.0.0.1 --port 8000"
ENTRYPOINT ["surfra", "worker"]
