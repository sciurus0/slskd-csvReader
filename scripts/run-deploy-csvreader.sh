#!/bin/bash
# Deploy slskd-csvReader web UI to the Synology NAS (home-hunt twin).
#
# Usage (from Mac, repo root or scripts/):
#   export NAS_HOST='ratatuskr@192.168.0.245'
#   bash scripts/run-deploy-csvreader.sh
#   bash scripts/run-deploy-csvreader.sh --upload-only
#
# Access after deploy: http://<NAS_LAN_IP>:8766/ (LAN/Tailscale).
# See docs/NAS_DEPLOY.md

set -euo pipefail

: "${NAS_HOST:?Set NAS_HOST to your DSM SSH target, e.g. export NAS_HOST=user@nas-ip}"
HOST="${NAS_HOST}"
PORT="${NAS_SSH_PORT:-220}"
REPO="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

UPLOAD_ONLY=0

for arg in "$@"; do
  case "$arg" in
    --upload-only) UPLOAD_ONLY=1 ;;
    -h | --help)
      sed -n '1,14p' "$0" | sed 's/^# \?//'
      exit 0
      ;;
    *)
      echo "Unknown option: $arg" >&2
      exit 1
      ;;
  esac
done

upload() {
  local dest="$1"
  shift
  ssh -p "$PORT" "$HOST" "mkdir -p $(dirname "$dest") && cat > '$dest'" <"$1"
}

upload_tree() {
  local local_dir="$1"
  local remote_dir="$2"
  ssh -p "$PORT" "$HOST" "rm -rf '$remote_dir' && mkdir -p '$remote_dir'"
  tar -C "$local_dir" -cf - . | ssh -p "$PORT" "$HOST" "tar -xf - -C '$remote_dir'"
}

download_python_wheels() {
  local wheels="${REPO}/docker/wheels"
  rm -rf "$wheels"
  mkdir -p "$wheels"

  download_with_python3() {
    local py=""
    for candidate in \
      "$(command -v python3 2>/dev/null || true)" \
      /usr/local/bin/python3 \
      /opt/homebrew/bin/python3; do
      if [[ -n "$candidate" && -x "$candidate" ]]; then
        py="$candidate"
        break
      fi
    done
    if [[ -z "$py" ]]; then
      return 1
    fi
    echo "Downloading Python wheels with ${py} (linux/amd64, offline NAS pip)..."
    "$py" -m pip download -r "${REPO}/docker/requirements.txt" -d "$wheels" \
      --platform manylinux2014_x86_64 \
      --platform manylinux_2_17_x86_64 \
      --python-version 3.12 \
      --implementation cp \
      --abi cp312 \
      --only-binary=:all:
  }

  download_with_docker() {
    echo "Downloading Python wheels with Docker (linux/amd64, offline NAS pip)..."
    docker run --rm --platform linux/amd64 \
      -v "${REPO}/docker:/w" -w /w python:3.12-slim \
      pip download -r requirements.txt -d wheels
  }

  if download_with_python3; then
    :
  elif command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    download_with_docker
  else
    echo "ERROR: need python3 (preferred) or a running Docker daemon to vendor wheels." >&2
    exit 1
  fi

  if [[ -z "$(ls -A "$wheels" 2>/dev/null)" ]]; then
    echo "ERROR: wheel download produced an empty ${wheels} directory." >&2
    exit 1
  fi
  echo "Vendored $(ls -1 "$wheels" | wc -l | tr -d ' ') wheel(s) into docker/wheels/"
}

stage_deploy_tree() {
  local stage="${REPO}/.deploy-csvreader"
  rm -rf "$stage"
  mkdir -p "$stage/docker/wheels" "$stage/webapp"

  cp "${REPO}/docker/Dockerfile" "$stage/docker/"
  cp "${REPO}/docker/requirements.txt" "$stage/docker/"
  cp -a "${REPO}/docker/wheels/." "$stage/docker/wheels/"
  cp "${REPO}/config.ini.example" "$stage/"
  # All top-level Python modules the pipeline needs
  find "${REPO}" -maxdepth 1 -name '*.py' -exec cp {} "$stage/" \;
  cp -a "${REPO}/webapp/." "$stage/webapp/"

  echo "$stage"
}

download_python_wheels
STAGE="$(stage_deploy_tree)"

echo "Staging csvreader on ${HOST}..."
upload_tree "$STAGE" /tmp/csvreader-deploy
rm -rf "$STAGE"

upload "/tmp/docker-compose.csvreader.yml" "${REPO}/examples/docker-compose.csvreader.yml"
upload "/tmp/install-csvreader.sh" "${SCRIPT_DIR}/nas-csvreader-install.sh"

if [[ "$UPLOAD_ONLY" -eq 1 ]]; then
  echo ""
  echo "Uploaded to /tmp on NAS. Finish install (sudo password required):"
  echo "  ssh -t -p ${PORT} ${HOST} 'sudo bash /tmp/install-csvreader.sh'"
  exit 0
fi

REMOTE=$(cat <<'EOS'
set -euo pipefail
sudo bash /tmp/install-csvreader.sh
EOS
)

if [[ -t 0 ]]; then
  ssh -t -p "$PORT" "$HOST" "$REMOTE"
else
  echo ""
  echo "Cannot run sudo non-interactively. Staged on NAS — finish with:"
  echo "  ssh -t -p ${PORT} ${HOST} 'sudo bash /tmp/install-csvreader.sh'"
  exit 2
fi

echo "Done."
