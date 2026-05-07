## Project Plan

### Overview

High-level goals for expanding the existing `slskd-spotify.py` workflow into a more automated, end-to-end system.

### Roadmap

- [x] Auto-download a tracklist from a specific Spotify playlist. *(Implemented by `spotify_playlist_fetch.py`: **OAuth 2.0 + PKCE**, refresh token cache default `~/.config/slskd/spotify_tokens.json`, env **`SPOTIFY_CLIENT_ID`** required; register **`SPOTIFY_REDIRECT_URI`** (default `http://127.0.0.1:8765/callback`) on the Spotify app. Optional **`SPOTIFY_CLIENT_SECRET`** if your app type requires it for token exchange.)*
- [ ] Convert the downloaded tracklist into the proper sanitized format for use by the existing script.
- [ ] Append those new songs to the existing download file.
- [ ] Automatically clean up the output file after the script is complete to remove successfully downloaded items.
- [ ] Prepare/move that file to a location where it is ready to be re-run.
- [ ] Create a scheduler variable/configuration that will run this entire script on a defined cadence.

### Feature backlog (not part of structural refactor)

Track polish items here so they are not lost among refactor work.

- [ ] **Search query string construction (featured artists / multi-artist rows)**  
  Retrying with featured artists or alternate artist strings is fine, but the **literal string passed to Soulseek search** needs refinement. Example observed: primary attempt `86LOVE - BAD SIDE - BAD SIDE` yielded no results; a follow-up used something like `86LOVE;The Kids;Tinywiings - BAD SIDE - BAD SIDE`, also with no results—the semicolon-joined or composite artist field should be normalized into queries users (and peers) actually share on the network, rather than passing through raw CSV joining. Decide rules for: stripping featured credits, primary-artist-only fallback, album/track delimiter consistency, and max query length.

### Post-OAuth / Spotify tooling follow-ups

Items to revisit once playlist export with OAuth + refresh cache is stable in daily use.

- [ ] **Optional client-credentials mode** — unattended fetch for **public** playlists only (no user session), useful for servers where interactive login is impossible; keep clearly separate from user OAuth.
- [ ] **Token cache hardening** — encrypt cache at rest, or integrate OS keychain/credential store instead of plain JSON.
- [ ] **CLI UX** — list “my playlists”, export multiple playlists to a folder or ZIP (similar to Exportify “export all”).
- [ ] **Dashboard / ops docs** — short checklist for Spotify Developer Dashboard (redirect URIs, app type, rotating secrets).
- [ ] **Scheduling guide** — document cron/systemd expectations: refresh tokens enable unattended runs until revoked; how to recover when refresh fails.
