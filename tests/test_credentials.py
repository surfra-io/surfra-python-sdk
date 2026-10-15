from surfra.credentials import Credentials, as_registry


def test_docker_credential_uses_the_name():
    body = as_registry(Credentials.Docker("my-docker-creds"))
    assert body == {"docker_secret_id": "my-docker-creds"}


def test_other_credentials_use_secret_id():
    assert as_registry(Credentials.Doppler("prod-env")) == {"secret_id": "prod-env"}
    assert as_registry(Credentials.Cloudflare("cf-prod")) == {"secret_id": "cf-prod"}
    assert as_registry({"docker_secret_id": "sec_123"}) == {"docker_secret_id": "sec_123"}
