# Hugging Face TGI with the Surfra supervisor in front.
#   docker build -t ghcr.io/surfra-io/tgi:0.1.0 examples/docker/tgi
FROM ghcr.io/huggingface/text-generation-inference:latest
RUN curl -fsSL https://surfra.io/install.sh | sh
ENV SURFRA_RUNTIME=http \
    SURFRA_HTTP_PORT=8000 \
    SURFRA_HTTP_PATH=/generate \
    SURFRA_HTTP_READY_PATH=/health \
    SURFRA_HTTP_READY_TIMEOUT=1800 \
    SURFRA_ENTRYPOINT="text-generation-launcher --model-id Qwen/Qwen3-0.6B --hostname 127.0.0.1 --port 8000"
ENTRYPOINT ["surfra", "worker"]
