import pytest


def test_app_name_is_a_single_token(sx):
    app = sx.surfra.App("myllm-staging")
    assert app.name == "myllm-staging"
    with pytest.raises(ValueError):
        sx.surfra.App("my llm")
    with pytest.raises(ValueError):
        sx.surfra.Workflow("my flow")
