#!/bin/bash
# GPU machines. Installs Docker and the NVIDIA container toolkit so the GPU is visible inside containers.
set -euo pipefail
if ! command -v docker >/dev/null 2>&1; then
  curl -fsSL https://get.docker.com | sh
  sudo usermod -aG docker "${USER}"
fi
if ! command -v nvidia-smi >/dev/null 2>&1; then
  echo "nvidia-smi is not on this machine. Install the NVIDIA driver, then run this script again." >&2
  exit 1
fi
if command -v nvidia-ctk >/dev/null 2>&1 && docker info --format '{{json .Runtimes}}' 2>/dev/null | grep -q nvidia; then
  echo "Docker can already use the GPU. Run: surfra runner start"
  exit 0
fi
if [ -r /etc/os-release ]; then
  # shellcheck disable=SC1091
  . /etc/os-release
fi
if [ "${ID:-}" != "ubuntu" ] && [ "${ID:-}" != "debian" ]; then
  echo "This script installs the NVIDIA container toolkit with apt. On other distributions, follow https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html" >&2
  exit 1
fi
curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg
curl -fsSL "https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list" |
  sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' |
  sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list >/dev/null
sudo apt-get update
sudo apt-get install -y nvidia-container-toolkit
sudo nvidia-ctk runtime configure --runtime=docker
sudo systemctl restart docker
echo "GPU containers are enabled. Open a new shell if you were just added to the docker group, then run: surfra runner start"
