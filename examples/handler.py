"""A model file. The author owns this. Surfra only calls warmup and handler."""

import surfra

model = None
tokenizer = None


@surfra.warmup()
def warmup():
    global model, tokenizer
    # Load weights here. This runs once when the worker starts.
    tokenizer = _Tokenizer()
    model = _Model()


@surfra.handler()
def main(request: surfra.Request):
    text = request.input["text"]
    surfra.progress(0.2, "encode")
    tokens = tokenizer.encode(text)
    surfra.progress(0.6, "infer")
    model_output = model(tokens)
    surfra.progress(1.0, "decode")
    return {"output": tokenizer.decode(model_output)}


class _Tokenizer:
    def encode(self, text: str) -> list[str]:
        return text.split()

    def decode(self, tokens: list[str]) -> str:
        return " ".join(tokens)


class _Model:
    def __call__(self, tokens: list[str]) -> list[str]:
        return [*tokens, "ok"]
