#!/bin/sh
# Installs the surfra command on Linux or macOS. --cpu or --gpu also sets up Docker for a local runner.
#   curl -fsSL https://surfra.io/runner.sh | sh
#   curl -fsSL https://surfra.io/runner.sh | sh -s -- --gpu
# SURFRA_HOME holds the Python environment (default ~/.surfra). SURFRA_BIN_DIR gets the command (default ~/.local/bin).
# SURFRA_PACKAGE installs a different wheel, path, or version spec.
set -eu

repo="surfra-io/surfra-python-sdk"
home="${SURFRA_HOME:-$HOME/.surfra}"
bindir="${SURFRA_BIN_DIR:-$HOME/.local/bin}"
mode=""
for arg in "$@"; do
  case "$arg" in
    --cpu) mode=cpu ;;
    --gpu) mode=gpu ;;
    *) echo "surfra: unknown option $arg. Use --cpu or --gpu." >&2; exit 1 ;;
  esac
done

case "$(uname -s)" in
  Linux | Darwin) ;;
  *) echo "surfra: this installer is for Linux and macOS. On Windows run: irm https://surfra.io/runner.ps1 | iex" >&2; exit 1 ;;
esac
command -v curl >/dev/null 2>&1 || { echo "surfra: the installer needs curl" >&2; exit 1; }

package="${SURFRA_PACKAGE:-}"
if [ -z "$package" ]; then
  latest="$(curl -fsSLI -o /dev/null -w '%{url_effective}' "https://github.com/$repo/releases/latest")"
  tag="${latest##*/}"
  case "$tag" in
    v*) ;;
    *) echo "surfra: could not find the latest release" >&2; exit 1 ;;
  esac
  package="https://github.com/$repo/releases/download/$tag/surfra-${tag#v}-py3-none-any.whl"
fi

python=""
for candidate in python3.13 python3.12 python3.11 python3; do
  if command -v "$candidate" >/dev/null 2>&1 && "$candidate" -c 'import sys; sys.exit(sys.version_info < (3, 11))' 2>/dev/null; then
    python="$candidate"
    break
  fi
done

mkdir -p "$home" "$bindir"
if [ -x "$home/venv/bin/python" ] && "$home/venv/bin/python" -m pip --version >/dev/null 2>&1; then
  # Upgrade in place so a running runner keeps working until it restarts itself between apps.
  "$home/venv/bin/python" -m pip install --quiet --upgrade "$package"
elif [ -x "$home/venv/bin/python" ] && [ -x "$home/bin/uv" ]; then
  "$home/bin/uv" pip install --quiet --python "$home/venv/bin/python" "$package"
elif rm -rf "$home/venv" && [ -n "$python" ] && "$python" -m venv "$home/venv" >/dev/null 2>&1; then
  "$home/venv/bin/python" -m pip install --quiet --upgrade pip
  "$home/venv/bin/python" -m pip install --quiet --upgrade "$package"
else
  # No Python 3.11+ with venv here, so uv brings its own Python into SURFRA_HOME.
  echo "Python 3.11 or newer was not found. Installing a private copy with uv."
  rm -rf "$home/venv"
  curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR="$home/bin" UV_NO_MODIFY_PATH=1 sh >/dev/null
  "$home/bin/uv" venv --quiet --python 3.12 "$home/venv"
  "$home/bin/uv" pip install --quiet --python "$home/venv/bin/python" "$package"
fi
ln -sf "$home/venv/bin/surfra" "$bindir/surfra"
echo "surfra $("$home/venv/bin/python" -c 'from importlib.metadata import version; print(version("surfra"))') installed to $bindir/surfra"
if command -v systemctl >/dev/null 2>&1 && systemctl --user is-active --quiet surfra-runner 2>/dev/null; then
  echo "The runner service switches to this version on its own the next time it is idle."
fi

case ":$PATH:" in
  *":$bindir:"*) ;;
  *) echo "Add $bindir to your PATH, for example: echo 'export PATH=\"$bindir:\$PATH\"' >> ~/.profile" ;;
esac

if [ -n "$mode" ]; then
  "$home/venv/bin/surfra" runner install "$mode"
  echo
  echo "Next:"
  echo "  surfra login                 # or export SURFRA_API_KEY=sfk_..."
  echo "  surfra runner register       # add --name my-box to skip the browser"
  echo "  surfra runner start          # or: surfra runner service install"
else
  echo "Next: surfra login, or export SURFRA_API_KEY=sfk_..."
fi
