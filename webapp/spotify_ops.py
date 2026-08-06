"""In-process Spotify helpers for the web UI (no interactive OAuth)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from slskd_config import load_api_txt
from spotify_playlist_fetch import (
    DEFAULT_REDIRECT_URI,
    fetch_user_playlists,
    try_access_token_noninteractive,
)
from webapp import runner

REAUTH_COMMAND = "NAS_HOST=nas bash scripts/nas-spotify-reauth.sh"


def token_cache_path() -> Path:
    env = os.environ.get("SPOTIFY_TOKEN_CACHE", "").strip()
    if env:
        return Path(env).expanduser().resolve()
    return (runner.workspace_path() / "spotify_tokens.json").resolve()


def spotify_credentials() -> Tuple[str, Optional[str], str]:
    secrets = load_api_txt()
    client_id = (
        os.environ.get("SPOTIFY_CLIENT_ID", "").strip()
        or secrets.get("spotify_client_id", "").strip()
    )
    client_secret = (
        os.environ.get("SPOTIFY_CLIENT_SECRET", "").strip()
        or secrets.get("spotify_client_secret", "").strip()
    ) or None
    redirect = (
        os.environ.get("SPOTIFY_REDIRECT_URI", "").strip()
        or secrets.get("spotify_redirect_uri", "").strip()
        or DEFAULT_REDIRECT_URI
    )
    return client_id, client_secret, redirect


def token_status() -> Dict[str, Any]:
    path = token_cache_path()
    client_id, client_secret, _redirect = spotify_credentials()
    exists = path.is_file()
    if not client_id:
        return {
            "ok": False,
            "path": str(path),
            "exists": exists,
            "detail": "SPOTIFY_CLIENT_ID not configured (env or config.ini / api.txt)",
            "reauth_command": REAUTH_COMMAND,
        }
    token, detail = try_access_token_noninteractive(client_id, client_secret, path)
    return {
        "ok": bool(token),
        "path": str(path),
        "exists": exists,
        "detail": detail,
        "reauth_command": REAUTH_COMMAND,
    }


def fetch_library(*, max_playlists: Optional[int] = None) -> List[Dict[str, Any]]:
    """Return Spotify library playlists or raise RuntimeError with operator hint."""
    path = token_cache_path()
    client_id, client_secret, _redirect = spotify_credentials()
    if not client_id:
        raise RuntimeError(
            "SPOTIFY_CLIENT_ID not configured. "
            f"After fixing credentials on Mac: {REAUTH_COMMAND}"
        )
    token, detail = try_access_token_noninteractive(client_id, client_secret, path)
    if not token:
        raise RuntimeError(f"{detail}. Re-auth on Mac: {REAUTH_COMMAND}")
    playlists = fetch_user_playlists(
        token,
        max_playlists=max_playlists,
        resolve_track_counts=False,
    )
    return [
        {
            "index": i,
            "id": pl.get("id") or "",
            "name": pl.get("name") or pl.get("id") or "?",
            "tracks_total": pl.get("tracks_total", -1),
            "owner": pl.get("owner") or "",
            "public": bool(pl.get("public")),
        }
        for i, pl in enumerate(playlists, start=1)
    ]
