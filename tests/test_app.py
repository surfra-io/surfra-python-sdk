import pytest

from surfra import App


def test_app_name_is_a_single_token():
    app = App("myllm-staging")
    assert app.name == "myllm-staging"
    with pytest.raises(ValueError):
        App("my llm")
