# SRCH analysis artifacts (POLISH-04)

Index for **SRCH-01** (query strategy) and **SRCH-02** (normalized path ranking) investigations.  
Full import logs stay in gitignored `data/logs/`; this folder holds **small committed fixtures** and pointers.

## Committed fixtures (this directory)

| File | Purpose |
| --- | --- |
| [`validate_input.csv`](validate_input.csv) | 10-row queue slice used for validate runs (wide queue columns) |
| [`processor_lily_queue_row.csv`](processor_lily_queue_row.csv) | Single row: spaced title **SRCH-06** candidate |
| [`processor_lily_log_excerpt.txt`](processor_lily_log_excerpt.txt) | Log excerpt: **247–248 responses**, zero ranked candidates |

### Re-run validate slice

From repo root (DEV):

```bash
python3 slskd_spotify.py \
  --csv fixtures/srch/validate_input.csv \
  --output-dir data/logs \
  --skip-pending-csv
```

Optional: copy fixture to workspace for pending-default naming:

```bash
cp fixtures/srch/validate_input.csv data/to_queue_pending_validate.csv
python3 slskd_spotify.py --csv data/to_queue_pending_validate.csv --output-dir data/logs
python3 pipeline_cleanup.py
```

## Local reference artifacts (`data/logs/`)

These files are **not in git** (see `.gitignore`). Paths are relative to DEV repo root.

| Artifact | Run / notes |
| --- | --- |
| `data/logs/slskd_import_20260517_081520.log` | Validate last-10 style run; **Processor** row shows 247 responses, no rank |
| `data/logs/results_20260517_082136.csv` | Results for that run |
| `data/logs/to_queue_pending_validate_results_20260517_082136_baseline.csv` | **Baseline** for regression compare (10 rows) |
| `data/logs/slskd_import_20260517_092609.log` | Post–PR #10 validate; 7/10 successes in chat notes |
| `data/logs/to_queue_pending_validate_results.csv` | Later validate (e.g. SRCH-03 smoke; Michael Wilbur / alternates) |
| `data/logs/slskd_import_20260517_110832.log` | Full DEV queue run (~206 rows, May 17) |
| `data/logs/validate_pending_last10.log` | Copy/snapshot related to validate slice |

## Failure modes to study

| Pattern | Example row | SRCH follow-up |
| --- | --- | --- |
| Many responses, **zero ranked** | Processor — `L I L Y` | **SRCH-06** spaced-letter / stylized title matching |
| Zero responses | Parts & Labor — `Haunted Limbs` | **SRCH-07** index-gap diagnostics |
| Wrong format noise | ratbag rows (`.jpg` in message) | Rank/format filters working; not a rank miss |
| Alternate artist needed | Michael Wilbur — `Tonio Sagan` | **SRCH-03** (on `main`); see latest validate results CSV |

## Processor excerpt (why it is a fixture)

Soulseek returns hundreds of files for query `Processor L I L Y`, but normalized path match finds **no valid candidates** — peers often store the track as `Lily` without spaces. See [`processor_lily_log_excerpt.txt`](processor_lily_log_excerpt.txt).

Baseline result row (20260517_082136):

```text
Processor,L I L Y,L I L Y,failed,No valid candidates found: 560 file(s): no match ...
```

## Related code

| Module | Role |
| --- | --- |
| `slskd_search.py` | Search poll, rank, `path_matches_row` |
| `slskd_query.py` | Query attempts (`artist_track`, `artist_album_track`, …) |
| `slskd_normalize.py` | `normalize_for_match`, artist alternates |

## Ops doc

Workspace layout and merge/trim/pending rules: [`docs/DEV_OPS.md`](../../docs/DEV_OPS.md).
