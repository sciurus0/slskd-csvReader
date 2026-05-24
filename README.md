# slskd-csvReader

Personal tooling that turns **Spotify playlists** into a **download queue** and drives the **[SLSKD](https://github.com/slskd/slskd)** Soulseek daemon over HTTP until files land on disk.

This repository is **not** the SLSKD app itself. You install and run SLSKD separately; these Python scripts talk to it at `http://localhost:5030` by default.

**Day-to-day operations** (resume, trim, recovery flags, logs): [docs/DEV_OPS.md](docs/DEV_OPS.md)

---

## What it does (three steps)

1. **Export** — Read your Spotify playlists (OAuth) into `data/exports/YYYYMMDD-spotify-export.csv`.
2. **Merge** — Append new tracks into `data/to_queue.csv`, respecting watermarks and a success ledger so you do not re-queue finished downloads.
3. **Download** — `slskd_spotify.py` searches Soulseek per row, enqueues files in SLSKD, reconciles transfer status, trims successes from the queue, and writes reports under `data/logs/`.

The usual entry point is **`run_pipeline.py`**, which runs export → merge → download in one flow.

---

## Before you start

| You need | Why |
| --- | --- |
| **Python 3.10+** | Run the scripts (`pip install -r requirements.txt`). |
| **[SLSKD](https://github.com/slskd/slskd) installed and running** | Handles Soulseek search, queue, and downloads. |
| **SLSKD API key** | In repo-root `api.txt` or `SLSKD_API_KEY` — required before `slskd_spotify.py` starts. |
| **Spotify Developer app** (for export) | Client ID (+ secret if your app type needs it) for OAuth. |
| **Soulseek account** | Configured inside the SLSKD app (not in this repo). |

**macOS paths (typical):** SLSKD app under `/Applications/slskd`; config/state in `~/Library/Application Support/slskd`.

---

## First-time setup

### 1. Install Python dependencies

From this repo directory (e.g. `slskd-csvReader/DEV`):

```bash
python3 -m pip install -r requirements.txt
```

### 2. Create `api.txt` (gitignored)

At the **repo root** (same folder as `run_pipeline.py`), create `api.txt`.

**SLSKD only** (queue processing, no Spotify export):

```text
your-slskd-api-key
```

**SLSKD + Spotify** (recommended for the full pipeline):

```ini
[slskd]
api_key = your-slskd-api-key

[spotify]
client_id = your-spotify-client-id
client_secret = your-spotify-client-secret
redirect_uri = http://127.0.0.1:8765/callback
```

Register that **exact** redirect URI in the [Spotify Developer Dashboard](https://developer.spotify.com/dashboard). See the checklist in [docs/DEV_OPS.md](docs/DEV_OPS.md#spotify-developer-dashboard-ops-04).

Environment variables override `api.txt` when set (`SLSKD_API_KEY`, `SPOTIFY_CLIENT_ID`, `SPOTIFY_REDIRECT_URI`, etc.).

### 3. Start SLSKD

Open the SLSKD app (or your usual launch method) and confirm the web UI/API responds at `http://localhost:5030`.

### 4. First Spotify login

The first export opens a browser for Spotify OAuth (or prints a URL with `--no-browser`). Tokens are cached by default at `~/.config/slskd/spotify_tokens.json` — do not commit them.

---

## First run

From the **DEV** repo root. Scripts use `./data/` as the workspace (created automatically).

```bash
# List playlists, pick by number, then export → merge → download
python3 run_pipeline.py --pick 1,4,7 -y
```

- `-y` skips the confirmation before Soulseek processing starts.
- Playlist numbers are **1-based** and match the list shown in the terminal (order can change if you add/remove playlists in Spotify — stable playlist IDs are planned; see project backlog **GOAL-04**).

**Merge only** (no downloads):

```bash
python3 run_pipeline.py --pick 1 -y --skip-slskd
# or, after an export already exists:
python3 merge_queue.py
```

**Resume** after interrupting a long download run:

```bash
python3 run_pipeline.py --resume -y
```

Uses `data/checkpoint.json` (legacy `checkpoint.pkl` is migrated once on load).

---

## Golden path commands

| Goal | Command |
| --- | --- |
| Full pipeline | `python3 run_pipeline.py --pick 1,4,7 -y` |
| Resume downloads | `python3 run_pipeline.py --resume -y` |
| Process queue only | `python3 slskd_spotify.py` (trims queue when done) |
| Refresh queue from Spotify | `python3 merge_queue.py` |
| Preview trim | `python3 trim_queue.py --dry-run` |

Important files under `data/`:

| File | Role |
| --- | --- |
| `to_queue.csv` | Work queue (search/download) |
| `success_ledger.csv` | Finished tracks — merge/trim skip these |
| `merge_state.json` | Per-playlist Spotify watermarks |
| `exports/*-spotify-export.csv` | Raw Spotify exports |
| `logs/` | Import logs and `results_*.csv` reports |
| `checkpoint.json` | Resume state for interrupted runs |

---

## Scripts (by role)

| Script | Role |
| --- | --- |
| `run_pipeline.py` | Interactive Spotify → merge → slskd orchestrator |
| `spotify_playlist_fetch.py` | Export playlists only |
| `merge_queue.py` | Merge latest export into `to_queue.csv` |
| `slskd_spotify.py` | Search, enqueue, reconcile downloads |
| `trim_queue.py` | Trim queue vs ledger without a full slskd run |
| `pipeline_cleanup.py` | Remove ephemeral pending CSVs |
| `scripts/backfill_ledger.py` | One-time `artist_primary` fix for old ledgers |

---

## DEV vs PROD

This project is often kept in two copies:

- **`DEV`** — development tree (this folder); safe place to try flags and inspect `data/`.
- **`PROD`** — same scripts, **separate `data/`** when you sync code; use when you want a stable production queue.

Only sync code between them unless you intentionally copy data.

---

## Do not commit

Keep secrets and runtime data out of git (see `.gitignore`): `api.txt`, `.env`, Spotify token JSON, everything under `data/`, `.cursor/`, and local `PROJECT_PLAN.md`.

---

## Tests

```bash
python3 -m unittest discover -s tests -p 'test_*.py' -v
```

Search regression fixtures: [fixtures/srch/README.md](fixtures/srch/README.md).

---

## Getting help

- **Operator workflows** — [docs/DEV_OPS.md](docs/DEV_OPS.md)
- **SLSKD** — [slskd documentation](https://github.com/slskd/slskd)
- **Spotify API** — [Spotify Web API docs](https://developer.spotify.com/documentation/web-api)
