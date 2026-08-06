#!/bin/bash
# Mac-side Spotify re-auth + copy tokens to NAS csvReader appdata.
#
# NAS containers cannot complete loopback OAuth. Run this on a Mac that can
# open a browser (or print --no-browser URL) against http://127.0.0.1:8765/callback.
#
# Usage:
#   export NAS_HOST=nas                    # or ratatuskr@192.168.0.245
#   bash scripts/nas-spotify-reauth.sh
#   bash scripts/nas-spotify-reauth.sh --no-browser
#   bash scripts/nas-spotify-reauth.sh --no-restart
#
# See docs/NAS_DEPLOY.md

set -euo pipefail

: "${NAS_HOST:?Set NAS_HOST to your DSM SSH target, e.g. export NAS_HOST=nas}"
HOST="${NAS_HOST}"
PORT="${NAS_SSH_PORT:-220}"
REPO="$(cd "$(dirname "$0")/.." && pwd)"
TOKEN_LOCAL="${SPOTIFY_REAUTH_TOKEN_PATH:-/tmp/spotify_tokens.json}"
APPDATA_TOKEN="/volume1/Docker/appdata/csvreader/spotify_tokens.json"

NO_BROWSER=0
NO_RESTART=0
for arg in "$@"; do
  case "$arg" in
    --no-browser) NO_BROWSER=1 ;;
    --no-restart) NO_RESTART=1 ;;
    -h | --help)
      sed -n '1,20p' "$0" | sed 's/^# \?//'
      exit 0
      ;;
    *)
      echo "Unknown option: $arg" >&2
      exit 1
      ;;
  esac
done

LOGIN=(python3 "${REPO}/spotify_playlist_fetch.py" --login-only --token-cache "${TOKEN_LOCAL}")
if [[ "$NO_BROWSER" -eq 1 ]]; then
  LOGIN+=(--no-browser)
fi

echo "=== Spotify login (Mac loopback OAuth) ==="
"${LOGIN[@]}"

if [[ ! -s "$TOKEN_LOCAL" ]]; then
  echo "ERROR: expected token file at ${TOKEN_LOCAL}" >&2
  exit 1
fi
chmod 600 "$TOKEN_LOCAL" || true

echo "=== Upload to ${HOST} ==="
scp -P "$PORT" "$TOKEN_LOCAL" "${HOST}:/tmp/spotify_tokens.json"

echo "=== Install into appdata (sudo) ==="
REMOTE=$(cat <<EOS
set -euo pipefail
sudo mv /tmp/spotify_tokens.json '${APPDATA_TOKEN}'
sudo chmod 600 '${APPDATA_TOKEN}'
sudo chown root:root '${APPDATA_TOKEN}' 2>/dev/null || true
ls -la '${APPDATA_TOKEN}'
EOS
)

if [[ -t 0 ]]; then
  ssh -t -p "$PORT" "$HOST" "$REMOTE"
else
  # Prefer passwordless sudo when available (common on this NAS).
  ssh -p "$PORT" "$HOST" "sudo -n bash -c $(printf '%q' "$REMOTE")" || {
    echo "Cannot sudo non-interactively. Finish with:" >&2
    echo "  ssh -t -p ${PORT} ${HOST} 'sudo mv /tmp/spotify_tokens.json ${APPDATA_TOKEN} && sudo chmod 600 ${APPDATA_TOKEN}'" >&2
    exit 2
  }
fi

if [[ "$NO_RESTART" -eq 0 ]]; then
  echo "=== Restart csvreader container ==="
  ssh -p "$PORT" "$HOST" 'sudo -n /usr/local/bin/docker restart csvreader' \
    || echo "Note: could not restart csvreader (sudo -n). Restart manually if needed."
  sleep 3
  echo "=== Health ==="
  curl -sf --connect-timeout 5 "http://192.168.0.245:8766/api/spotify/token-status" \
    | python3 -m json.tool \
    || curl -sf --connect-timeout 5 "http://192.168.0.245:8766/api/health" || true
fi

echo "Done. Tokens at ${APPDATA_TOKEN}"
