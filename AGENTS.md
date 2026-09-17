# slskd-csvReader — agent instructions

Python tooling that pipelines CSV / Spotify playlist tracks into an [slskd](https://github.com/slskd/slskd) download queue over HTTP.

## Layout

- Active development tree: this directory (`DEV`). Production copy: sibling `PROD/`.
- Does **not** contain the slskd daemon; it expects an HTTP API (local default `http://localhost:5030`).

## Local setup

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

## Cursor Cloud specific instructions

Cloud agents run on Ubuntu and do **not** have your home SLSKD daemon or local download library. This file is the cloud contract (User Rules and Mac rule-symlinks do not load). Push before switching devices or starting a Cloud Agent.

- Install via `.cursor/environment.json` (`pip install -r requirements.txt` in a venv).
- Secrets/dashboard if needed: host URL and credentials for a reachable slskd instance (values used in `slskd_config.py`). Prefer not committing real endpoints.
- Cloud-safe work: pure python, dry-run paths, unit-style parsing of fixtures under `fixtures/`, docs, refactors that don't require a live slskd.
- Do **not** assume `localhost:5030` is available or that queue mutations are reversible in cloud — skip live queue runs unless a test endpoint is provided as a secret.
- Product board: [slskd-csvReader #2](https://github.com/users/sciurus0/projects/2).

## Git

Remote: https://github.com/sciurus0/slskd-csvReader
