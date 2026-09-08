# NAS deploy — slskd-csvReader web UI

Companion FastAPI **operator control panel** (port **8766**) for Spotify library refresh → saved playlist selection, full `run_pipeline.py` options, and queue ops (reconcile / trim / resume / merge / retry / cleanup). Same ship pattern as Home Hunt.

## What lands where

| Path | Contents |
|------|----------|
| `/volume1/mcp/appdata/csvreader/` | Workspace: `to_queue.csv`, ledger, logs, `config.ini`, `spotify_tokens.json`, `saved_playlists.json` |
| `/volume1/Media/downloads/complete/slskd` | **Downloaded audio** (SLSKD writes here — not the csvReader container) |
| `http://NAS:8766/` | Control panel (LAN / Tailscale only) |

## Prerequisites

1. NAS media stack SLSKD healthy at `http://NAS:5030`.
2. `SLSKD_API_KEY` (same key as SLSKD) available in `/volume1/mcp/.env` and/or `appdata/csvreader/config.ini`.
3. Set `SLSKD_BASE_URL=http://<NAS_LAN_IP>:5030` in `/volume1/mcp/.env` (or `base_url` in appdata `config.ini`).
4. Spotify: Mac login (loopback OAuth), then copy tokens to NAS (see below).

## Spotify tokens (Mac re-auth)

OAuth redirect is **loopback-only** (`http://127.0.0.1:8765/callback`). The NAS container cannot complete browser login. When the UI shows a token banner, run this **on a Mac**:

```bash
export NAS_HOST=harveymcp-root   # SSH Host alias, or root@192.168.0.196
export NAS_SSH_PORT=227
cd /Users/harvey/Documents/Development/slskd-csvReader/DEV
bash scripts/nas-spotify-reauth.sh
# or: bash scripts/nas-spotify-reauth.sh --no-browser
```

That script:

1. Runs `spotify_playlist_fetch.py --login-only` on the Mac (writes `/tmp/spotify_tokens.json`).
2. `scp`s the file to the NAS.
3. Installs it at `/volume1/mcp/appdata/csvreader/spotify_tokens.json` (mode `600`).
4. Restarts the `csvreader` container and checks `/api/spotify/token-status`.

Manual equivalent (legacy):

```bash
python3 spotify_playlist_fetch.py --login-only --token-cache /tmp/spotify_tokens.json
scp -P 227 /tmp/spotify_tokens.json "$NAS_HOST:/tmp/"
ssh -t -p 227 "$NAS_HOST" \
  'mv /tmp/spotify_tokens.json /volume1/mcp/appdata/csvreader/spotify_tokens.json && chmod 600 /volume1/mcp/appdata/csvreader/spotify_tokens.json'
```

Populate saved playlists from the UI (**Library → Refresh from Spotify → Save selection**) or once via CLI `--pick`.

## Deploy from Mac

```bash
export NAS_HOST='root@192.168.0.196'   # or Host alias `harveymcp-root`
export NAS_SSH_PORT=227

cd /Users/harvey/Documents/Development/slskd-csvReader
bash scripts/run-deploy-csvreader.sh --upload-only
ssh -t -p 227 "$NAS_HOST" 'bash /tmp/install-csvreader.sh'
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
