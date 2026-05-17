# slskd-csvReader

Spotify playlist export → merge into a Soulseek download queue → drive [SLSKD](https://github.com/slskd/slskd) over HTTP until tracks land on disk. Personal automation tooling (DEV/PROD copies); not the SLSKD application itself.

**Operator guide:** [docs/DEV_OPS.md](docs/DEV_OPS.md) (workspace layout, golden path vs advanced flags).

## Prerequisites

| Requirement | Notes |
| --- | --- |
| **Python 3.10+** | Stdlib + packages in `requirements.txt` |
| **SLSKD daemon** | Running locally (default API `http://localhost:5030`) |
| **`api.txt`** (gitignored) | SLSKD API key; optional Spotify OAuth block — see below |
| **Spotify app** | For export: Client ID (and secret if required) in `api.txt` or env vars |

Install dependencies:

```bash
cd /path/to/slskd-csvReader/DEV
python3 -m pip install -r requirements.txt
```

## `api.txt` (repo root, gitignored)

**Legacy** — one line, SLSKD key only:

```text
your-slskd-api-key
```

**INI** (recommended if you use Spotify export):

```ini
[slskd]
api_key = your-slskd-api-key

[spotify]
client_id = ...
client_secret = ...
redirect_uri = http://127.0.0.1:8765/callback
```

Environment variables override `api.txt` when set (`SLSKD_API_KEY`, `SPOTIFY_CLIENT_ID`, etc.).

First Spotify run opens a browser (or prints an authorize URL with `--no-browser`). Tokens cache under `~/.config/slskd/spotify_tokens.json` by default.

## Golden path

From the **DEV** repo root (scripts default to `./data/`):

```bash
# Full flow: pick playlists → export → merge → download
python3 run_pipeline.py --pick 1,4,7 -y

# Resume a long slskd run after interrupt
python3 run_pipeline.py --resume -y

# Process the queue only (after merge)
python3 slskd_spotify.py --trim-queue

# Refresh queue from latest Spotify export (no slskd)
python3 merge_queue.py
```

Canonical files live under `data/` — see [docs/DEV_OPS.md](docs/DEV_OPS.md).

## Scripts (by role)

| Script | Role |
| --- | --- |
| `run_pipeline.py` | Interactive Spotify → merge → slskd orchestrator |
| `spotify_playlist_fetch.py` | Export playlists to `data/exports/` |
| `merge_queue.py` | Merge export into `data/to_queue.csv` |
| `slskd_spotify.py` | Search, enqueue, reconcile downloads |
| `trim_queue.py` | Drop ledger successes from queue (same as `--trim-queue`) |
| `pipeline_cleanup.py` | Remove ephemeral pending CSVs |
| `backfill_ledger.py` | One-time `artist_primary` column on old ledgers |

## DEV vs PROD

- **`DEV`** — development copy (this tree).
- **`PROD`** — same scripts; separate `data/` when you sync code.

SLSKD app/config on macOS: `/Applications/slskd`, `~/Library/Application Support/slskd`.

## Tests

```bash
python3 -m unittest discover -s tests -p 'test_*.py' -v
```

SRCH regression slice: [fixtures/srch/README.md](fixtures/srch/README.md).
