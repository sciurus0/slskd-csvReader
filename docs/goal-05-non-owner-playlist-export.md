# GOAL-05 — Non-owner playlist export (research)

**Status:** Research complete (May 2026). **No code change required** for followed-only playlists — Spotify Web API does not expose item data to this project’s auth model.

**Related backlog:** **GOAL-04** (stable playlist IDs), **OPS-01** (client-credentials mode), **GOAL-03** (`snapshot_id` incremental sync).

---

## Question

Can **slskd-csvReader** export tracks from playlists the authenticated user does **not** own — e.g. followed playlists, arbitrary public playlist URLs, or “someone else’s” editorial lists — using the Spotify Web API (user OAuth or client credentials)?

---

## Short answer

| Relationship to playlist | Listed in `GET /v1/me/playlists`? | `GET /v1/playlists/{id}/items` (full tracks)? |
| --- | --- | --- |
| **You own it** | Yes | **Yes** |
| **You are a collaborator** | Yes (with `playlist-read-collaborative`) | **Yes** |
| **You only follow it** (saved to library, not owner/collab) | Often yes | **No — HTTP 403** |
| **Public playlist by URL** (not owner/collab) | Only if you follow or open via ID | **No — HTTP 403** |

**Client credentials** (`OPS-01`) cannot replace user OAuth for playlist **items**; that flow has no user context and does not grant access to another user’s playlist contents under current API rules.

**Practical path for operators:** duplicate the playlist in the Spotify app (creates an **owned** copy), get invited as a **collaborator**, or import tracks from another source — not via API for follow-only lists.

---

## What this repo does today

| Piece | Behavior |
| --- | --- |
| **Auth** | Authorization Code + PKCE; scopes `playlist-read-private playlist-read-collaborative` (`spotify_playlist_fetch.py`) |
| **List** | `GET /v1/me/playlists` → numbered pick list (`--list-playlists`, `--pick`) |
| **Export** | `GET /v1/playlists/{id}/items` (paginated; uses `item`, not deprecated `/tracks`) |
| **403 handling** | User-facing note: followed playlists may list but fail on export (`spotify_playlist_fetch.py`) |

The pipeline already assumes **owner or collaborator** for successful export. Merge/queue logic does not need changes for GOAL-05; the gap is **Spotify policy**, not missing scopes in this app.

---

## Spotify Web API (relevant endpoints)

### User OAuth (current approach)

| Endpoint | Scopes (typical) | Items / tracks |
| --- | --- | --- |
| `GET /v1/me/playlists` | `playlist-read-private`, `playlist-read-collaborative` | Summary only (`tracks.total` may be present; not full list) |
| `GET /v1/playlists/{id}` | Same | Playlist metadata; **`items` field only if owner or collaborator** (per API reference) |
| `GET /v1/playlists/{id}/items` | Same | **Full paginated items only if owner or collaborator**; else **403** |

Official note on [Get Playlist’s Items](https://developer.spotify.com/documentation/web-api/reference/get-playlists-items):

> This endpoint is only accessible for playlists owned by the current user or playlists the user is a collaborator of. A 403 Forbidden status code will be returned if the user is neither the owner nor a collaborator of the playlist.

Use **`/items`**, not **`/tracks`**. The `/tracks` path is deprecated; calling it after Spotify’s 2026 migration can produce 403 even when `/items` would work for an owned playlist.

### Client credentials (`OPS-01` — not implemented)

| Capability | Supported? |
| --- | --- |
| Server-to-server token (`grant_type=client_credentials`) | Yes |
| `GET /v1/me/*` (user library, user playlists) | **No** — no user context |
| Browse/search catalog (albums, artists, some playlist discovery) | Yes (catalog endpoints) |
| `GET /v1/playlists/{id}/items` for **another user’s** public playlist | **No** — same owner/collaborator rule applies to item access; not unlocked by client credentials |

**Conclusion for OPS-01:** Client credentials are useful for **public catalog search** or metadata, not for “export any public playlist URL” into `to_queue.csv`. Any future OPS-01 work should be scoped to **search-by-name / URI** flows, not bulk export of arbitrary third-party playlists.

---

## Policy change (February 2026)

Spotify tightened playlist **item** access for standard developer apps (community reports ~11 Feb 2026). Effects relevant here:

- **Followed** and **other users’ public** playlists: still visible in the client and often in `GET /me/playlists`, but **`/items` returns 403** unless you own or collaborate.
- **Extended / partner quota** (e.g. large commercial apps) may differ; this project uses a normal **Development** app with user OAuth — treat 403 on non-owned lists as **expected**, not a bug in csvReader.

References:

- [Spotify Community — 403 on playlist items](https://community.spotify.com/t5/Spotify-for-Developers/Persistent-HTTP-403-on-v1-playlists-playlist-id-items-same-token/td-p/7410582)
- [Get Playlist’s Items](https://developer.spotify.com/documentation/web-api/reference/get-playlists-items)
- [Playlists concept](https://developer.spotify.com/documentation/web-api/concepts/playlists)

---

## Playlist types (operator language)

| Type | How you know | Export via csvReader? |
| --- | --- | --- |
| **Owned** | You created it; you appear as owner in Spotify | **Yes** |
| **Collaborative** | Owner enabled collaboration; you can edit tracks | **Yes** (needs `playlist-read-collaborative`; already requested) |
| **Followed / saved** | Added to library; owner is someone else; you are not a collaborator | **No** (403 on `/items`) |
| **Public link only** | Open URL; not in your library | **No**, unless you duplicate or are added as collaborator |
| **Liked Songs** | Special “playlist” in UI | Separate API (`GET /v1/me/tracks`); **out of scope** for current export script (playlist-oriented) |

**Tip:** In `--list-playlists` output, compare the **owner** column to your Spotify display name. If the owner is not you and the playlist is not collaborative, expect export to fail until you duplicate or collaborate.

---

## Workarounds (no API)

| Approach | Pros | Cons |
| --- | --- | --- |
| **Duplicate playlist** in Spotify (Create playlist → Add songs from followed list) | Becomes **owned**; full API export | Manual; large lists are tedious |
| **Collaborator invite** from owner | Keeps single shared list | Needs owner action |
| **Paste playlist URL with `--pick` after duplicate** | Works with current CLI if you own the copy | Still manual setup |
| **Third-party “playlist export” sites** | Fast | Privacy, ToS, quality; not integrated here |
| **Manual CSV** into `data/to_queue.csv` | Always works | Must match queue column contract |

---

## Recommendations for csvReader

### Do not build (low value / blocked by API)

- Export path for **follow-only** or **arbitrary public** playlists via Web API.
- **OPS-01** mode advertised as “export any public playlist by URL” using client credentials only.

### Optional follow-ups (separate tasks)

| ID | Idea | Effort |
| --- | --- | --- |
| **UX** | After `GET /me`, flag list rows where `owner.id != me.id` and print `follow-only?` in `--list-playlists` | Small |
| **UX** | On 403, print duplicate-playlist steps (link to this doc) | Small |
| **GOAL-04** | Persist `playlist_id` so re-export targets the same list after duplicate | Medium |
| **OPS-01** | Client-credentials **search** helper (find track/album on Soulseek queue) | Medium; scope separately |

### GOAL-03 note

Incremental sync (`snapshot_id`) only applies to playlists where **items are readable**. Follow-only playlists remain blocked until ownership/collaboration changes.

---

## Verification checklist (manual)

Use a Development app and the same OAuth setup as production:

1. **Owned playlist** — `python3 spotify_playlist_fetch.py --pick <n>` → CSV rows with tracks.
2. **Followed-only playlist** (owner ≠ you, not collaborative) — same command → **403** with repo’s note about follow vs own.
3. **Collaborative playlist** (you are editor, owner ≠ you) → export succeeds if Spotify lists you as collaborator.
4. **Deprecated endpoint** — confirm code uses `/items` only (already true in `fetch_playlist_track_rows`).

---

## References

| Resource | URL |
| --- | --- |
| Get Playlist’s Items | https://developer.spotify.com/documentation/web-api/reference/get-playlists-items |
| Get Playlist | https://developer.spotify.com/documentation/web-api/reference/get-playlist |
| Playlists (concepts) | https://developer.spotify.com/documentation/web-api/concepts/playlists |
| Client Credentials flow | https://developer.spotify.com/documentation/web-api/tutorials/client-credentials-flow |
| This repo export module | `spotify_playlist_fetch.py` |
| Operator dashboard checklist | `docs/DEV_OPS.md` — Spotify Developer Dashboard |
