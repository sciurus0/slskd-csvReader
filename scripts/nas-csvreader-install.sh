#!/bin/bash
# Run on NAS as root: sudo bash /tmp/install-csvreader.sh
# Aligns with nas-media-stack: /volume1/Docker + appdata/<service>
set -euo pipefail

DOCKER=/usr/local/bin/docker
COMPOSE="$DOCKER compose"
STACK=/volume1/Docker
APPDATA="$STACK/appdata/csvreader"

mkdir -p "$STACK/csvreader" "$APPDATA/exports" "$APPDATA/logs" "$APPDATA/archive"

if [[ ! -d /tmp/csvreader-deploy ]]; then
  echo "ERROR: missing /tmp/csvreader-deploy — re-run Mac deploy." >&2
  exit 1
fi

cp -a /tmp/csvreader-deploy/. "$STACK/csvreader/"

if [[ ! -d "$STACK/csvreader/docker/wheels" ]] || [[ -z "$(ls -A "$STACK/csvreader/docker/wheels" 2>/dev/null)" ]]; then
  echo "ERROR: missing docker/wheels — re-run deploy from Mac (vendors PyPI wheels)." >&2
  exit 1
fi

if [[ -f /tmp/docker-compose.csvreader.yml ]]; then
  cp /tmp/docker-compose.csvreader.yml "$STACK/docker-compose.csvreader.yml"
fi

# Seed config.ini once from example
if [[ ! -f "$APPDATA/config.ini" ]]; then
  if [[ -f "$STACK/csvreader/config.ini.example" ]]; then
    cp "$STACK/csvreader/config.ini.example" "$APPDATA/config.ini"
    chmod 600 "$APPDATA/config.ini"
    echo "Created $APPDATA/config.ini from example — set api_key + base_url (NAS SLSKD)."
  else
    touch "$APPDATA/config.ini"
    chmod 600 "$APPDATA/config.ini"
  fi
fi

cd "$STACK"
if [[ -f "$STACK/.env" ]]; then
  $COMPOSE -f docker-compose.csvreader.yml --env-file .env up -d --build
else
  echo "Note: no $STACK/.env — using compose defaults (set SLSKD_BASE_URL / NAS_LAN_IP)."
  $COMPOSE -f docker-compose.csvreader.yml up -d --build
fi

echo ""
echo "csvReader UI is up."
echo "  LAN / Tailscale: http://\${NAS_LAN_IP:-<nas-ip>}:8766/"
echo "  Never WAN-forward 8766."
echo "  Data (queue/ledger/tokens): $APPDATA"
echo "  Downloads: SLSKD complete dir (e.g. /volume1/Media/downloads/complete/slskd)"
echo "  Bootstrap Spotify tokens on Mac, then copy to $APPDATA/spotify_tokens.json"
echo "  See docs/NAS_DEPLOY.md"
