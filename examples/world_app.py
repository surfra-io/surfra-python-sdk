import surfra


@surfra.handler()
def main(request: surfra.Request):
    prior = (request.input.get("upstream") or {}).get("hello") or {}
    surfra.progress(1.0, "world")
    return {"text": f"{prior.get('text', '')} world".strip()}
