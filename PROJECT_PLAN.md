## Project Plan

### Overview

High-level goals for expanding the existing `slskd-spotify.py` workflow into a more automated, end-to-end system.

### Roadmap

- [ ] Auto-download a tracklist from a specific Spotify playlist.
- [ ] Convert the downloaded tracklist into the proper sanitized format for use by the existing script.
- [ ] Append those new songs to the existing download file.
- [ ] Automatically clean up the output file after the script is complete to remove successfully downloaded items.
- [ ] Prepare/move that file to a location where it is ready to be re-run.
- [ ] Create a scheduler variable/configuration that will run this entire script on a defined cadence.

### Feature backlog (not part of structural refactor)

Track polish items here so they are not lost among refactor work.

- [ ] **Search query string construction (featured artists / multi-artist rows)**  
  Retrying with featured artists or alternate artist strings is fine, but the **literal string passed to Soulseek search** needs refinement. Example observed: primary attempt `86LOVE - BAD SIDE - BAD SIDE` yielded no results; a follow-up used something like `86LOVE;The Kids;Tinywiings - BAD SIDE - BAD SIDE`, also with no results—the semicolon-joined or composite artist field should be normalized into queries users (and peers) actually share on the network, rather than passing through raw CSV joining.
  
  **Decision updates for implementation:**
  - Use loose token queries by default (no hard `artist - album - track` separator requirement).
  - Do **not** globally strip punctuation from source metadata; punctuation in names can be meaningful (for example `Wu-Tang`, `C.R.E.A.M.`).
  - Generate query variants in order: literal (punctuation preserved) first, punctuation-softened fallback second.
  - Keep candidate count bounded (small fixed fallback sequence, no combinatorial expansion).
  - Use Spotify `duration_ms` only as a **weak closest-match tie-breaker** among already-ranked candidates (never an exact-match gate).

