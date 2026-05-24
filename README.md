# slskd-csvReader

Personal tooling that turns **Spotify playlists** into a **download queue** and drives the **[SLSKD](https://github.com/slskd/slskd)** Soulseek daemon over HTTP until files land on disk.

This repository is **not** the SLSKD app itself. You install and run SLSKD separately; these Python scripts talk to it at `http://localhost:5030` by default.

**Day-to-day operations** (resume, trim, recovery flags, logs): [docs/DEV_OPS.md](docs/DEV_OPS.md)

**Platform:** This project is developed and run on **macOS** (paths and examples below assume that). The scripts use portable Python (`pathlib`, loopback OAuth, `http://localhost:5030`) and are expected to work on **Linux** and **Windows** with SLSKD installed, but those platforms are not documented or tested in-repo yet — see backlog **PLAT-01** on the [GitHub project board](https://github.com/users/sciurus0/projects/2).

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
| **[SLSKD](https://github.com/slskd/slskd)** running locally | Soulseek search, download queue, and transfers. |
| **SLSKD web API key** | Same key in SLSKD config and this repo (`api.txt` or `SLSKD_API_KEY`). |
| **Spotify Developer app** | OAuth for playlist export (full pipeline only). |
| **Soulseek account** | Log in through the SLSKD app — not configured in this repo. |

---

## First-time setup

### 1. Install Python dependencies

From the repo root (same folder as `run_pipeline.py`):

```bash
python3 -m pip install -r requirements.txt
```

### 2. Set up SLSKD

[SLSKD](https://github.com/slskd/slskd) is the Soulseek client this project drives over HTTP. Install it from the [project releases](https://github.com/slskd/slskd/releases). **macOS:** app under `/Applications/slskd`, config under `~/Library/Application Support/slskd`. **Linux / Windows:** use release binaries or [Docker](https://github.com/slskd/slskd/blob/master/docs/docker.md) (`slskd.yml` under `~/.local/share/slskd` or `%LOCALAPPDATA%\slskd` — see [SLSKD README](https://github.com/slskd/slskd)).

1. **Install and sign in** — Open SLSKD and log in with your [Soulseek](https://www.slsknet.org/news/) username and password.
2. **Create a web API key** — Scripts send `X-API-Key` on each request. The key must be **16–255 characters**. Configure it in SLSKD using one of:
   - **`slskd.yml`** — under `web.authentication.api_keys` (see [SLSKD config: API keys](https://github.com/slskd/slskd/blob/master/docs/config.md#api-keys));
   - **Startup flags / env** — `-k` / `--api-key` or `SLSKD_API_KEY` when launching SLSKD (same doc section).
   Use a role that can search and enqueue downloads (the default primary key is **Administrator**).
3. **Start SLSKD** — Leave it running. Confirm the web UI loads at [http://localhost:5030](http://localhost:5030) (default; change only if you customized SLSKD’s listen URL).
4. **Copy the key into this repo** — You will put the **same** string in `api.txt` (step 4 below) or export `SLSKD_API_KEY` before running `slskd_spotify.py`. Without it, the download step exits immediately.

Further SLSKD options (paths, auth, YAML): [configuration guide](https://github.com/slskd/slskd/blob/master/docs/config.md).

### 3. Set up Spotify (playlist export)

Needed for `run_pipeline.py` / `spotify_playlist_fetch.py`. Queue-only runs (`slskd_spotify.py` on an existing `data/to_queue.csv`) skip Spotify.

1. **Create a Developer app** — In the [Spotify Developer Dashboard](https://developer.spotify.com/dashboard), create an app and note the **Client ID** and **Client secret** (Web API console apps use both).
2. **Register a redirect URI** — Under app settings → **Redirect URIs**, add exactly:
   `http://127.0.0.1:8765/callback`
   (Loopback only — do not use `0.0.0.0` or a LAN address.) This must match `redirect_uri` in `api.txt` below.
3. **Allow your account** — While the app is in **Development** mode, add your Spotify user under **User Management** → test users. Only playlists visible to that account can be exported.
4. **Scopes** — Export uses playlist-read scopes only; accept the consent screen on first login.

Operator checklist (redirect, secrets, test users): [docs/DEV_OPS.md — Spotify Developer Dashboard](docs/DEV_OPS.md#spotify-developer-dashboard-ops-04). API reference: [Spotify Web API](https://developer.spotify.com/documentation/web-api).

### 4. Create `api.txt`

At the **repo root**, create `api.txt` with the credentials from the steps above.

**SLSKD only** (process `data/to_queue.csv` without exporting):

```text
your-slskd-api-key
```

**SLSKD + Spotify** (full pipeline):

```ini
[slskd]
api_key = your-slskd-api-key

[spotify]
client_id = your-spotify-client-id
client_secret = your-spotify-client-secret
redirect_uri = http://127.0.0.1:8765/callback
```

Environment variables override `api.txt` when set (`SLSKD_API_KEY`, `SPOTIFY_CLIENT_ID`, `SPOTIFY_REDIRECT_URI`, etc.).

### 5. First Spotify login

Run an export (or full pipeline). The first time, a browser opens for Spotify OAuth (or use `--no-browser` and open the printed URL). After you approve access, tokens are cached locally (default `~/.config/slskd/spotify_tokens.json`) for later runs.

---

## First run

From the repo root. Scripts use `./data/` as the workspace (created automatically).

```bash
# First run: pick from library list (saves IDs to data/saved_playlists.json)
python3 run_pipeline.py --pick 1,4,7 -y

# Later runs: export by saved Spotify ID (stable when library order changes)
python3 run_pipeline.py --saved -y
python3 run_pipeline.py --saved 1,3 -y
python3 run_pipeline.py --list-saved
```

- `-y` skips the confirmation before Soulseek processing starts.
- **`--pick`** uses **1-based** indices from the live Spotify library list and updates **`data/saved_playlists.json`** (use **`--no-save-picks`** to skip).
- **`--saved`** exports from that file by playlist ID (all enabled, or indices into the saved list).
- **`--playlist-id`** exports ad hoc IDs/URLs without updating the saved file.

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
| `saved_playlists.json` | Stable playlist IDs for `--saved` exports |

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
