"""Named credentials. The name is what you already gave the secret in the console.

An id still works anywhere a name works. The control plane resolves either one.
"""

from __future__ import annotations


class _Named:
    field = "secret_id"

    def __init__(self, name: str):
        if not name or any(char.isspace() for char in name):
            raise ValueError("a credential name is a single token")
        self.name = name

    def as_registry(self) -> dict[str, str]:
        return {self.field: self.name}


class Credentials:
    """Attach a stored credential to an app by the name you chose."""

    class Docker(_Named):
        """Registry login. `surfra.Credentials.Docker('my-docker-creds')`."""

        field = "docker_secret_id"

    class Doppler(_Named):
        field = "secret_id"

    class Cloudflare(_Named):
        field = "secret_id"

    class Generic(_Named):
        field = "secret_id"


def as_registry(value: object) -> dict[str, str]:
    if value is None:
        return {}
    if hasattr(value, "as_registry"):
        return value.as_registry()
    return {str(key): str(item) for key, item in dict(value).items()}
