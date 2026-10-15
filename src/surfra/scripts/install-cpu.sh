#!/bin/bash
# CPU machines. Installs Docker so a Surfra runner can pull and run images.
set -euo pipefail
if command -v docker >/dev/null 2>&1; then
  echo "Docker is already installed."
  exit 0
fi
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker "${USER}"
echo "Docker is installed. Open a new shell so the docker group applies, then run: surfra runner start"
