# DEV workspace operations

Short guide for the **slskd-csvReader** DEV copy. Production uses the same scripts under `PROD/` with its own `data/` tree.

**New here?** Start with [README.md](../README.md) (prerequisites, `api.txt`, install). This doc is the day-to-day operator reference.

## Golden path vs everything else

Use these daily; ignore the rest unless you have a specific recovery or tuning need.

| Tier | What | Examples |
| --- | --- | --- |
| **Golden** | End-to-end or queue processing | `run_pipeline.py --pick … -y`, `--resume -y`, `slskd_spotify.py`, `merge_queue.py` |
| **Tuning** | Rate, formats, debug | `slskd_spotify.py --delay`, `--formats`, `--batch-size`, `--download-settle-seconds` |
| **Hygiene** | Manual trim or drop ephemeral CSVs | `trim_queue.py` (optional if you used `--no-trim-queue`), `pipeline_cleanup.py --ephemeral` |
| **Recovery** | Fix a past run without re-searching | `slskd_spotify.py --reconcile-downloads`, `--gen-report`, `--retry-failed` |

`slskd_spotify.py --help` groups flags: golden path, tuning, recovery.

Validate-input runs still auto-remove `to_queue_pending_validate.csv` after each slskd run. For other pending files, use `pipeline_cleanup.py`.

## One queue

There is a **single canonical work queue**:

| Path | Meaning |
| --- | --- |
| `data/to_queue.csv` | Rows still to search/download |

Everything else is derived or ephemeral.

| Path | Meaning |
| --- | --- |
| `data/success_ledger.csv` | Completed downloads — do not merge or re-queue these keys |
| `data/merge_state.json` | Per-playlist Spotify `added_at` watermarks |
| `data/to_queue_pending.csv` | **Ephemeral** — written after a slskd run; failures only; safe to delete after you copy rows you care about |
| `data/to_queue_pending_validate.csv` | **Ephemeral** — small slice for SRCH/regression; removed after validate runs or via `pipeline_cleanup.py` |
| `data/checkpoint.json` | Resume pointer for interrupted slskd runs (legacy `checkpoint.pkl` migrated on load) |
| `data/exports/` | Spotify exports (`YYYYMMDD-spotify-export.csv`) |
| `data/logs/` | Import logs and `results_*.csv` reports |
| `data/archive/csv-YYYYMMDD/` | Dated backups before merge or trim |

Do not treat `data/to_queue_pending.csv` as a second source of truth. After a full run it lists what still failed; the next full run should start from `data/to_queue.csv` (trimmed automatically after each slskd run unless you pass `--no-trim-queue`).

## Typical flows

### Refresh queue from Spotify

```bash
python3 run_pipeline.py --pick 1,4,7 -y
# or: export → merge_queue.py separately
```

Writes export to `data/exports/`, merges into `data/to_queue.csv`, optionally runs slskd.

### Process the queue

```bash
python3 slskd_spotify.py --csv data/to_queue.csv
```

- Searches each row, reconciles downloads, appends successes to `data/success_ledger.csv`
- Writes `data/to_queue_pending.csv` (failures)
- By default rewrites `data/to_queue.csv` minus ledger keys (backup in `data/archive/`). Use `--no-trim-queue` to skip trim (debug only).

### Resume a long run

```bash
python3 run_pipeline.py --resume -y
# or: python3 slskd_spotify.py --csv data/to_queue.csv --resume
```

Uses `data/checkpoint.json` in the workspace (legacy `.pkl` is migrated once if present).

### Merge only (no slskd)

```bash
python3 merge_queue.py
python3 merge_queue.py --dry-run   # preview counts; no queue write
```

### Trim without a slskd run

```bash
python3 trim_queue.py
python3 trim_queue.py --dry-run
```

Same trim as the default slskd post-run step: drop ledger keys and dedupe; backup under `data/archive/`.

### Clean ephemeral pending files

```bash
python3 pipeline_cleanup.py                  # validate slice only
python3 pipeline_cleanup.py --ephemeral      # validate + retry pending
```

### Backfill ledger `artist_primary` (legacy ledgers)

If `data/success_ledger.csv` predates NORM-06 (no `artist_primary` column), rewrite once:

```bash
python3 scripts/backfill_ledger.py --dry-run
python3 scripts/backfill_ledger.py
```

Otherwise the column appears automatically the next time slskd appends successes.

## Merge vs trim vs pending

| Action | When | Effect on `to_queue.csv` |
| --- | --- | --- |
| **merge_queue** | New Spotify export | Adds new rows (watermark + ledger filter); dedupes |
| **trim_queue** / default slskd trim | After downloads | Removes ledger successes; dedupes |
| **pending CSV** | After slskd run | Does not change `to_queue.csv`; report of failures only |

## SRCH regression slice

Committed fixtures and a log index live under [`fixtures/srch/README.md`](../fixtures/srch/README.md).

Quick validate run from repo root:

```bash
python3 slskd_spotify.py --csv fixtures/srch/validate_input.csv --output-dir data/logs
```

Compare new `data/logs/results_*.csv` to the baseline noted in that README.

## Security (operator notes)

**Trust model:** single-user machine; you run the scripts, own `data/`, and trust local SLSKD (`http://localhost:5030`). Treat Soulseek filenames and CSV fields as untrusted when opened in Excel (reports use quoted CSV; see SEC-06 in the project plan).

| Asset | Location | Notes |
| --- | --- | --- |
| SLSKD API key | `config.ini` / `api.txt` `[slskd]` or `SLSKD_API_KEY` | Required for `slskd_spotify.py`; chmod **600** recommended |
| SLSKD base URL | `config.ini` `base_url` or `SLSKD_BASE_URL` | Default `http://localhost:5030`; set to NAS IP:5030 for remote daemon |

| Spotify tokens | `~/.config/slskd/spotify_tokens.json` (default) | OAuth refresh; do not commit |
| Resume state | `data/checkpoint.json` | Only resume checkpoints you created; legacy `.pkl` auto-migrates once |
| Logs | `data/logs/` | May contain search text and usernames |

**OAuth redirect:** must be loopback only — default `http://127.0.0.1:8765/callback`. Do not use `0.0.0.0` or a LAN IP.

**Debug:** `SPOTIFY_DEBUG=1` logs request flow; error bodies are redacted when JSON contains token fields.

## Spotify Developer Dashboard (OPS-04)

When creating or reviewing your Spotify app:

1. **Redirect URIs** — add exactly `http://127.0.0.1:8765/callback` (must match `api.txt` / `SPOTIFY_REDIRECT_URI`).
2. **Client ID** — copy to `api.txt` `[spotify]` or `SPOTIFY_CLIENT_ID`.
3. **Client secret** — if your app type requires it, store in `api.txt` or env only (never commit).
4. **Scopes** — playlist read scopes used by export; no extra scopes unless you add features.
5. **Users** — add your Spotify account as a test user while the app is in Development mode.
