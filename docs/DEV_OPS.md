# DEV workspace operations

Short guide for the **slskd-csvReader** DEV copy. Production uses the same scripts under `PROD/` with its own `data/` tree.

**New here?** Start with [README.md](../README.md) (prerequisites, `api.txt`, install). This doc is the day-to-day operator reference.

## Golden path vs everything else

Use these daily; ignore the rest unless you have a specific recovery or tuning need.

| Tier | What | Examples |
| --- | --- | --- |
| **Golden** | End-to-end or queue processing | `run_pipeline.py --pick … -y`, `--resume -y`, `slskd_spotify.py --trim-queue`, `merge_queue.py` |
| **Tuning** | Rate, formats, debug | `slskd_spotify.py --delay`, `--formats`, `--batch-size`, `--download-settle-seconds` |
| **Hygiene** | Same behavior as flags on slskd | `trim_queue.py`, `pipeline_cleanup.py --ephemeral` |
| **Recovery** | Fix a past run without re-searching | `slskd_spotify.py --reconcile-downloads`, `--gen-report`, `--retry-failed` |
| **Legacy** | Pre–SRCH-02 / enqueue-only; avoid | `--skip-download-reconcile`, `--direct-api`, `--exact-match`, `--album-preferred-search` |

`slskd_spotify.py --help` groups flags the same way. Legacy flags still work but log a deprecation warning.

### CLI audit (local logs)

Under `data/logs/`, recent import runs use the **default** path: reconciliation on, normalized SRCH-02/03 ranking, no `--album-preferred-search`, `--exact-match`, `--direct-api`, or `--retry-failed`. Treat legacy flags as unused unless you know you need them.

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
| `data/checkpoint.pkl` | Resume pointer for interrupted slskd runs |
| `data/exports/` | Spotify exports (`YYYYMMDD-spotify-export.csv`) |
| `data/logs/` | Import logs and `results_*.csv` reports |
| `data/archive/csv-YYYYMMDD/` | Dated backups before merge or trim |

Do not treat `data/to_queue_pending.csv` as a second source of truth. After a full run it lists what still failed; the next full run should start from `data/to_queue.csv` (often after `--trim-queue`).

## Typical flows

### Refresh queue from Spotify

```bash
python3 run_pipeline.py --pick 1,4,7 -y
# or: export → merge_queue.py separately
```

Writes export to `data/exports/`, merges into `data/to_queue.csv`, optionally runs slskd.

### Process the queue

```bash
python3 slskd_spotify.py --csv data/to_queue.csv --trim-queue
```

- Searches each row, reconciles downloads, appends successes to `data/success_ledger.csv`
- Writes `data/to_queue_pending.csv` (failures)
- With `--trim-queue`, rewrites `data/to_queue.csv` minus ledger keys (backup in `data/archive/`)

### Resume a long run

```bash
python3 run_pipeline.py --resume -y
# or: python3 slskd_spotify.py --csv data/to_queue.csv --resume
```

Uses `data/checkpoint.pkl` in the workspace.

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

Same idea as `--trim-queue`: drop ledger keys and dedupe; backup under `data/archive/`.

### Clean ephemeral pending files

```bash
python3 pipeline_cleanup.py                  # validate slice only
python3 pipeline_cleanup.py --ephemeral      # validate + retry pending
```

### Backfill ledger `artist_primary` (legacy ledgers)

If `data/success_ledger.csv` predates NORM-06 (no `artist_primary` column), rewrite once:

```bash
python3 backfill_ledger.py --dry-run
python3 backfill_ledger.py
```

Otherwise the column appears automatically the next time slskd appends successes.

## Merge vs trim vs pending

| Action | When | Effect on `to_queue.csv` |
| --- | --- | --- |
| **merge_queue** | New Spotify export | Adds new rows (watermark + ledger filter); dedupes |
| **trim_queue** / **--trim-queue** | After downloads | Removes ledger successes; dedupes |
| **pending CSV** | After slskd run | Does not change `to_queue.csv`; report of failures only |

## SRCH regression slice

Committed fixtures and a log index live under [`fixtures/srch/README.md`](../fixtures/srch/README.md).

Quick validate run from repo root:

```bash
python3 slskd_spotify.py --csv fixtures/srch/validate_input.csv --output-dir data/logs
```

Compare new `data/logs/results_*.csv` to the baseline noted in that README.
