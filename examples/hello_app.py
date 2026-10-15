import surfra


@surfra.handler()
def main(request: surfra.Request):
    surfra.progress(1.0, "hello")
    return {"text": "hello"}
