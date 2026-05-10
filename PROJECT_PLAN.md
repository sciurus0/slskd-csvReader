## Project Plan

### Overview

High-level goals for expanding the existing `slskd-spotify.py` workflow into a more automated, end-to-end system.

### Roadmap

- Auto-download a tracklist from a specific Spotify playlist. *(Implemented by `spotify_playlist_fetch.py`: **OAuth 2.0 + PKCE**, refresh token cache default `~/.config/slskd/spotify_tokens.json`, env `**SPOTIFY_CLIENT_ID*`* required; register `**SPOTIFY_REDIRECT_URI**` (default `http://127.0.0.1:8765/callback`) on the Spotify app. Optional `**SPOTIFY_CLIENT_SECRET**` if your app type requires it for token exchange. Optional `**api.txt**` INI `[spotify]` section. End-to-end **API pull + export still to be smoke-tested** as the pipeline input.)*
- Convert the downloaded tracklist into the proper sanitized format for use by the existing script.
- Append those new songs to the existing download file.
- Automatically clean up the output file after the script is complete to remove successfully downloaded items.
- Prepare/move that file to a location where it is ready to be re-run.
- Create a scheduler variable/configuration that will run this entire script on a defined cadence.

### Spotify feature scope (planning backlog)

This section tracks **what “Spotify feature complete” means** and **recommended phases**, adjusted per project direction. **No code changes implied here**—documentation only until implementation is explicitly authorized.

#### Definition — “Spotify feature complete”

Complete means **Phase A** and **Phase B** below. However, **before implementing Phase B**, **pause** for a dedicated pass to scope **data normalization** (the fragile part: multi-artist rules, dedup identity, edge cases). Normalization must not be rushed.

#### Canonical scope statement

With user OAuth, **reliably enumerate playlist tracks** via the Web API, **map them to our pipeline row contract**, and **write export output** suitable for normalization and later merge. **Wiring into `slskd-spotify.py`** and full pipeline orchestration remain **separate milestones** unless explicitly added.

---

#### Phase A — Ingestion (API → structured output)


| Topic                  | Decision                                                                                                                                                                                                                                                                                                                                |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Playlist selection** | **Priority:** pick from the **user’s playlist list** (API-driven list UX). **Proof of concept:** smoke-test with a single **playlist URL or ID** first.                                                                                                                                                                                 |
| **Completeness**       | Export the **full list** of playlist items as the API returns them. **Do not** filter to “Spotify-playable” or regional availability only. Include rows where `track` is missing or incomplete (removed tracks, local files, etc.) using **best-effort columns** / empty fields—capture the **whole list**, not only streamable tracks. |
| **Market**             | **Do not** rely on `market` behavior for availability; we record API output as-is.                                                                                                                                                                                                                                                      |
| **Artifact**           | **CSV only** for now. Optional JSON sidecar (e.g. for `added_at`, Spotify IDs, delta imports) is **shelved** until there is a concrete need.                                                                                                                                                                                            |
| **Current status**     | OAuth **token cache works**, but **nothing else has been completed end-to-end** yet (no verified full pull-through as the sole automation input).                                                                                                                                                                                       |


#### Lean API mode (anti-throttle strategy)

Design principle: **treat Spotify API as a constrained resource** and optimize for predictable, low-volume access over maximum convenience metadata.

| Area | Lean default | Why |
| --- | --- | --- |
| **Discovery vs extraction** | Keep playlist discovery (`/me/playlists`) separate from track extraction (`/playlists/{id}/tracks`). | Prevents broad scans when only one playlist is needed. |
| **Optional metadata** | Do not request/resolve track counts during listing by default. | Avoids extra per-playlist calls that look like abusive fan-out. |
| **Selection scope** | Use `--list-one` / `--list-limit` to cap discovery reads. | Keeps discovery deterministic and cheap. |
| **Extraction cadence** | Export selected playlists only; no full-library scrape loops in one run. | Reduces burstiness and total call volume. |
| **429 handling** | Treat high `Retry-After` as a hard stop; defer work. | Avoids retry loops that burn budget and prolong throttling. |
| **Concurrency** | Single process/token at a time (no parallel workers). | Prevents self-induced spikes and racey backoff behavior. |
| **Incremental sync (future)** | Add snapshot-based skip logic (e.g. `snapshot_id`) before full re-fetch. | Avoids unnecessary reads when playlists are unchanged. |

#### Lean-mode checklist (implementation order)

1. **Keep listing minimal**: ID + owner + name only, capped via list limit.
2. **Require explicit extraction target**: URL/ID or picked row; no broad auto-enumeration.
3. **Fail fast on throttle**: persist cooldown window and abort early while active.
4. **Add light observability**: debug-only request/status/retry logs (already in place).
5. **Add incremental guardrails**: skip unchanged playlists once snapshot tracking is introduced.

#### Rate-limit incident playbook (ops)

- On `429` with short retry: back off once, then continue if window is small.
- On `429` with large retry window: stop run, log cooldown expiry, retry later.
- Avoid rerunning repeatedly during cooldown; this can extend or re-trigger penalties.

**Phase C items** (headless refresh, scheduling expectations, dashboard checklist) stay **after** A/B unless small doc wins are pulled forward.

---

#### Pause gate — before Phase B (normalization)

Work **stops before** the **normalize** step (see jobs **J3** below) until normalization is **scoped in detail** (rules, edge cases, relationship to Soulseek search quality). This gate is **intentional**.

---

#### Phase B — Normalization (pipeline-ready rows)

Map exported CSV rows into the **exact shape** the rest of the pipeline expects (aligned with roadmap: “convert tracklist…”). **Dedup identity** (e.g. normalized `(artist, album, track)` vs Spotify `track.id`) is an **open design topic**—to be discussed **before** execution.

---

#### Jobs reference (API vs our code)

Theoretical job split is fine; **implementation pauses before J3**.


| Job    | Description                                                     | Status note            |
| ------ | --------------------------------------------------------------- | ---------------------- |
| **J1** | Resolve source (list UX + URL/ID for POC)                       | List-first priority    |
| **J2** | Pull full track list (paginate; **no** playability-only filter) | Aligns with Phase A    |
| **J3** | **Normalize**                                                   | **Pause** until scoped |
| **J4** | Persist output (CSV)                                            | CSV only for now       |
| **J5** | Optional delta / “new since last run”                           | Deferred until needed  |


---

#### Product choices (locked for planning)

1. **Export format:** CSV only for now; JSON optional **later** unless a strong case appears.
2. **Dedup / identity:** **Discuss in detail before** implementing normalization or merge behavior.
3. **Multiple playlists:** **In scope for v1**; **development order:** validate with **one playlist first**, then multi-playlist.
4. **Market:** **Not** part of the capture strategy—no availability-driven filtering.

---

### Feature backlog (not part of structural refactor)

Track polish items here so they are not lost among refactor work.

- **Search query string construction (featured artists / multi-artist rows)**  
Retrying with featured artists or alternate artist strings is fine, but the **literal string passed to Soulseek search** needs refinement. Example observed: primary attempt `86LOVE - BAD SIDE - BAD SIDE` yielded no results; a follow-up used something like `86LOVE;The Kids;Tinywiings - BAD SIDE - BAD SIDE`, also with no results—the semicolon-joined or composite artist field should be normalized into queries users (and peers) actually share on the network, rather than passing through raw CSV joining. Decide rules for: stripping featured credits, primary-artist-only fallback, album/track delimiter consistency, and max query length.

---

### Approved implementation backlog (deferred — dedicated feature branches)

Items below are **approved** to schedule on appropriate branches when implementation time is authorized. Each should update docs/help/examples when landed.

| Topic | Intent |
| --- | --- |
| **UTF-8 as preferred encoding** | **Reads:** fail fast on non–UTF-8; **no** encoding conversion. **Writes:** **UTF-8 with BOM** (`utf-8-sig`). Consolidate duplicated encoding try-lists (`slskd_worker` / `slskd_csv`) and align streaming vs non-streaming paths so behavior matches the above. |
| **Queue merge + sanitize once** | **Canonical queue file:** `to_queue.csv` (same default as `slskd-spotify.py` / `slskd_config`). **When building the merged queue:** (1) Copy existing `to_queue.csv` to **`YYYYMMDD-to_queue.csv`** where `YYYYMMDD` is the **local** calendar date of execution **for this run** (same-day overwrite of that backup file is acceptable). (2) **Combine** **`YYYYMMDD-spotify-export.csv`** with **`YYYYMMDD-to_queue.csv`** (the backup just written) and **overwrite** **`to_queue.csv`** with the result. **All** data/file sanitization happens **only** in this merge step—**not** in `slskd_worker`, search, or elsewhere; when implemented, remove per-row sanitization outside this path so `to_queue.csv` is the single sanitized artifact. Phase B / J3 remains the place for **semantic** normalization (e.g. Soulseek query rules) if distinct from this sanitization. |
| **Podcasts / episodes out of scope** | **Omit at the API** (do not request podcast/episode item types on playlist-items calls). Remove exporter code paths that exist only for episodes; **no** episode rows in CSV output. Align Phase A “completeness” with **music tracks only** (still include removed/local/incomplete **track** rows as returned). |
| **Default Spotify export filename** | Default output from `spotify_playlist_fetch.py`: **`YYYYMMDD-spotify-export.csv`** with **`YYYYMMDD` = local calendar date** of the run. Preserve **`--output` / `-o`**. **Same-day overwrite** of the default export filename is acceptable. Update CLI help and examples that still reference `playlist_import.csv`. |

#### Decisions (locked — approved backlog answers)

1. **UTF-8:** Fail fast; no conversion; writes use **UTF-8 with BOM**.
2. **Merge:** Backup `to_queue.csv` → `YYYYMMDD-to_queue.csv`; combine **`YYYYMMDD-spotify-export.csv`** + **`YYYYMMDD-to_queue.csv`** → overwrite **`to_queue.csv`**; sanitization **only** here, **nowhere else** in the project.
3. **Episodes:** **Omit at API** only.
4. **Datestamps:** **Local** `YYYYMMDD` for dated filenames (`*-spotify-export.csv`, `*-to_queue.csv`).
5. **Collisions:** Same-day overwrite is **acceptable** (no extra disambiguation beyond optional `-o`).

### Post-OAuth / Spotify tooling follow-ups

Items to revisit as the Spotify feature matures (overlap with **Phase C** above).

- **Optional client-credentials mode** — unattended fetch for **public** playlists only (no user session), useful for servers where interactive login is impossible; keep clearly separate from user OAuth.
- **Token cache hardening** — encrypt cache at rest, or integrate OS keychain/credential store instead of plain JSON.
- **Dashboard / ops docs** — short checklist for Spotify Developer Dashboard (redirect URIs, app type, rotating secrets).
- **Scheduling guide** — document cron/systemd expectations: refresh tokens enable unattended runs until revoked; how to recover when refresh fails.