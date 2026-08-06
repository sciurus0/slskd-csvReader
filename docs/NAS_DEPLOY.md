# NAS deploy — slskd-csvReader web UI

Companion FastAPI UI (port **8766**) that runs `run_pipeline.py --saved -y`, reconcile, and trim without remembering Mac paths. Same ship pattern as Home Hunt.

## What lands where

| Path | Contents |
|------|----------|
| `/volume1/Docker/appdata/csvreader/` | Workspace: `to_queue.csv`, ledger, logs, `config.ini`, `spotify_tokens.json`, `saved_playlists.json` |
| `/volume1/Media/downloads/complete/slskd` | **Downloaded audio** (SLSKD writes here — not the csvReader container) |
| `http://NAS:8766/` | Button UI (LAN / Tailscale only) |

## Prerequisites

1. NAS media stack SLSKD healthy at `http://NAS:5030`.
2. `SLSKD_API_KEY` (same key as SLSKD) available in `/volume1/Docker/.env` and/or `appdata/csvreader/config.ini`.
3. Set `SLSKD_BASE_URL=http://<NAS_LAN_IP>:5030` in `/volume1/Docker/.env` (or `base_url` in appdata `config.ini`).
4. Spotify: one-time Mac login (loopback OAuth), then copy tokens to NAS.

## Spotify token bootstrap (Mac)

```bash
# On Mac, against your existing Spotify app credentials:
python3 spotify_playlist_fetch.py --login-only --token-cache /tmp/spotify_tokens.json
# Also populate saved playlists once if needed:
python3 run_pipeline.py --pick 1,2 -y --skip-slskd --workspace /path/to/staging
scp -P 220 /tmp/spotify_tokens.json "$NAS_HOST:/tmp/"
ssh -t -p 220 "$NAS_HOST" \
  'sudo mv /tmp/spotify_tokens.json /volume1/Docker/appdata/csvreader/spotify_tokens.json && sudo chmod 600 /volume1/Docker/appdata/csvreader/spotify_tokens.json'
```

Copy `saved_playlists.json` into appdata the same way if the NAS workspace is empty.

## Deploy from Mac

```bash
export NAS_HOST='ratatuskr@192.168.0.245'   # or your DSM user@host
export NAS_SSH_PORT=220                    # optional; default 220

cd /Users/harvey/Documents/Development/slskd-csvReader/DEV
bash scripts/run-deploy-csvreader.sh --upload-only
ssh -t -p 220 "$NAS_HOST" 'sudo bash /tmp/install-csvreader.sh'
```

Or omit `--upload-only` to run the sudo install in one shot (interactive TTY).

## Local Mac smoke (no NAS)

```bash
python3 -m pip install -r requirements.txt
export CSVREADER_WORKSPACE="$(pwd)/data"
python3 -m uvicorn webapp.app:app --host 127.0.0.1 --port 8766
# open http://127.0.0.1:8766/
```

## Security

- Do **not** port-forward 8766 to the WAN.
- Prefer Tailscale or LAN; optional HTTP basic can be added later (stack-ui pattern).
- Keep `config.ini` and `spotify_tokens.json` mode `600` in appdata.
