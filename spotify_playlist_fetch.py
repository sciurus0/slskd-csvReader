#!/usr/bin/env python3
"""
Export a Spotify playlist to a CSV compatible with slskd-spotify.py (columns: artist, album, track).

Authentication — OAuth 2.0 with PKCE (Authorization Code) + refresh token cache.

Credentials (precedence: environment variables, then api.txt — see below):

    SPOTIFY_CLIENT_ID (required unless set in api.txt under [spotify])
    SPOTIFY_CLIENT_SECRET (optional for some app types)
    SPOTIFY_REDIRECT_URI (optional; default http://127.0.0.1:8765/callback)

Optional api.txt (same directory as this script / repo root), gitignored, INI format:

    [slskd]
    api_key = ...          # optional if you only use this script for Spotify

    [spotify]
    client_id = ...
    client_secret = ...
    redirect_uri = http://127.0.0.1:8765/callback

If you use a legacy single-line api.txt containing only the SLSKD key, add Spotify by
switching to the INI format above (preserving [slskd] api_key).

Token cache (JSON, not api.txt):

    SPOTIFY_TOKEN_CACHE (optional)
        Path to JSON storing access + refresh tokens after first login.
        Default: ~/.config/slskd/spotify_tokens.json

First run opens a browser (or prints the authorize URL if --no-browser). After that,
scheduled/unattended runs use the cached refresh token until it is revoked.

Scopes: playlist-read-private, playlist-read-collaborative

Usage:
    python3 spotify_playlist_fetch.py "https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M"
    python3 spotify_playlist_fetch.py 37i9dQZF1DXcBWIGoYBM5M -o my_playlist.csv
    python3 spotify_playlist_fetch.py --login-only

    Credentials: export SPOTIFY_CLIENT_* env vars and/or add [spotify] to api.txt as documented above.
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import json
import os
import re
import secrets
import sys
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, urlencode, urlparse

import requests

from slskd_config import load_api_txt

ACCOUNTS_AUTHORIZE_URL = "https://accounts.spotify.com/authorize"
ACCOUNTS_TOKEN_URL = "https://accounts.spotify.com/api/token"
API_BASE = "https://api.spotify.com/v1"

DEFAULT_REDIRECT_URI = "http://127.0.0.1:8765/callback"
DEFAULT_TOKEN_CACHE = Path.home() / ".config" / "slskd" / "spotify_tokens.json"

SCOPES = "playlist-read-private playlist-read-collaborative"


def _die(msg: str, code: int = 1) -> None:
    print(msg, file=sys.stderr)
    raise SystemExit(code)


def parse_playlist_id(raw: str) -> str:
    """Accept bare 22-char Spotify ID or open.spotify.com / spotify:playlist: URLs."""
    s = raw.strip()
    if re.fullmatch(r"[0-9A-Za-z]{22}", s):
        return s
    m = re.search(r"playlist[/:]([0-9A-Za-z]{22})", s)
    if m:
        return m.group(1)
    _die(f"Could not parse playlist ID from: {raw!r}")


def _pkce_verifier_and_challenge() -> Tuple[str, str]:
    # Spotify: code_verifier must be 43–128 characters (url-safe string).
    verifier = secrets.token_urlsafe(48)
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    return verifier, challenge


def _redirect_host_port(redirect_uri: str) -> Tuple[str, int]:
    p = urlparse(redirect_uri)
    if p.scheme not in ("http", "https") or not p.hostname:
        _die(f"Invalid SPOTIFY_REDIRECT_URI / redirect URI: {redirect_uri!r}")
    port = p.port or (443 if p.scheme == "https" else 80)
    return p.hostname, port


def _load_token_cache(path: Path) -> Optional[Dict[str, Any]]:
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict) and data.get("refresh_token"):
            return data
    except (OSError, json.JSONDecodeError):
        pass
    return None


def _save_token_cache(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    try:
        path.chmod(0o600)
    except OSError:
        pass


def _token_expired(expires_at: Optional[float], skew_seconds: float = 60.0) -> bool:
    if expires_at is None:
        return True
    return time.time() >= expires_at - skew_seconds


def _exchange_code_for_tokens(
    code: str,
    redirect_uri: str,
    client_id: str,
    client_secret: Optional[str],
    code_verifier: str,
) -> Dict[str, Any]:
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "client_id": client_id,
        "code_verifier": code_verifier,
    }
    if client_secret:
        data["client_secret"] = client_secret
    r = requests.post(ACCOUNTS_TOKEN_URL, data=data, timeout=60)
    if r.status_code != 200:
        _die(f"Token exchange failed ({r.status_code}): {r.text[:800]}")
    return r.json()


def _refresh_access_token(
    refresh_token: str,
    client_id: str,
    client_secret: Optional[str],
) -> Optional[Dict[str, Any]]:
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
    }
    if client_secret:
        data["client_secret"] = client_secret
    r = requests.post(ACCOUNTS_TOKEN_URL, data=data, timeout=60)
    if r.status_code != 200:
        print(
            f"Refresh failed ({r.status_code}): {r.text[:400]}\n"
            "Will open interactive login.",
            file=sys.stderr,
        )
        return None
    return r.json()


def interactive_authorize(
    redirect_uri: str,
    auth_url: str,
    expected_state: str,
    no_browser: bool,
    timeout_seconds: float = 180.0,
) -> str:
    """Listen first, then open browser — avoids losing the OAuth redirect race."""
    host, port = _redirect_host_port(redirect_uri)
    result: Dict[str, Any] = {}
    done = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: Any) -> None:
            pass

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            qs = parse_qs(parsed.query)
            if qs.get("error"):
                result["error"] = qs["error"][0]
                body = b"<html><body>Authorization failed. You may close this window.</body></html>"
            elif qs.get("code") and qs.get("state"):
                if qs["state"][0] != expected_state:
                    result["error"] = "state_mismatch"
                    body = b"<html><body>State mismatch. Close this window and try again.</body></html>"
                else:
                    result["code"] = qs["code"][0]
                    body = b"<html><body>Success. You may close this window.</body></html>"
            else:
                result["error"] = "missing_code"
                body = b"<html><body>Invalid callback.</body></html>"
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            done.set()
            threading.Thread(target=self.server.shutdown, daemon=True).start()

    server = HTTPServer((host, port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    time.sleep(0.3)

    if no_browser:
        print("Open this URL in a browser and sign in:\n", file=sys.stderr)
        print(auth_url, file=sys.stderr)
    else:
        webbrowser.open(auth_url)
        print("Waiting for browser redirect after login...", file=sys.stderr)

    if not done.wait(timeout=timeout_seconds):
        try:
            server.shutdown()
        except Exception:
            pass
        _die("Timed out waiting for Spotify redirect (login took too long).")

    try:
        server.shutdown()
    except Exception:
        pass
    thread.join(timeout=2)

    if result.get("error"):
        _die(f"Authorization failed: {result['error']}")
    code = result.get("code")
    if not code:
        _die("Authorization failed: no authorization code received.")
    return code


def ensure_user_access_token(
    client_id: str,
    client_secret: Optional[str],
    redirect_uri: str,
    token_path: Path,
    no_browser: bool,
) -> str:
    """Return a valid access token, running OAuth or refresh as needed."""
    cache = _load_token_cache(token_path)
    now = time.time()

    if cache and cache.get("access_token") and not _token_expired(cache.get("expires_at")):
        return cache["access_token"]

    if cache and cache.get("refresh_token"):
        refreshed = _refresh_access_token(cache["refresh_token"], client_id, client_secret)
        if refreshed:
            new_refresh = refreshed.get("refresh_token") or cache["refresh_token"]
            expires_in = float(refreshed.get("expires_in", 3600))
            updated = {
                "access_token": refreshed["access_token"],
                "refresh_token": new_refresh,
                "expires_at": now + expires_in,
                "token_type": refreshed.get("token_type", "Bearer"),
            }
            _save_token_cache(token_path, updated)
            return refreshed["access_token"]

    code_verifier, code_challenge = _pkce_verifier_and_challenge()
    state = secrets.token_urlsafe(16)
    params = {
        "client_id": client_id,
        "response_type": "code",
        "redirect_uri": redirect_uri,
        "scope": SCOPES,
        "state": state,
        "code_challenge_method": "S256",
        "code_challenge": code_challenge,
    }
    auth_url = f"{ACCOUNTS_AUTHORIZE_URL}?{urlencode(params)}"
    code = interactive_authorize(redirect_uri, auth_url, state, no_browser)
    token_payload = _exchange_code_for_tokens(
        code, redirect_uri, client_id, client_secret, code_verifier
    )
    expires_in = float(token_payload.get("expires_in", 3600))
    refresh = token_payload.get("refresh_token")
    if not refresh:
        _die("Spotify did not return a refresh_token; check app settings and scopes.")
    store = {
        "access_token": token_payload["access_token"],
        "refresh_token": refresh,
        "expires_at": now + expires_in,
        "token_type": token_payload.get("token_type", "Bearer"),
    }
    _save_token_cache(token_path, store)
    return token_payload["access_token"]


def _track_row(track: Dict[str, Any]) -> Optional[Tuple[str, str, str]]:
    """Return (artist, album, track) or None to skip."""
    if not track:
        return None
    if track.get("type") == "episode":
        return None
    if track.get("is_local"):
        return None

    artists = track.get("artists") or []
    names = [a.get("name", "").strip() for a in artists if a.get("name")]
    artist = "; ".join(names) if names else ""

    album = (track.get("album") or {}).get("name") or ""
    album = album.strip()
    title = (track.get("name") or "").strip()

    if not title:
        return None
    return (artist, album, title)


def fetch_playlist_track_rows(
    token: str,
    playlist_id: str,
    *,
    market: Optional[str],
) -> List[Tuple[str, str, str]]:
    rows: List[Tuple[str, str, str]] = []
    url = f"{API_BASE}/playlists/{playlist_id}/tracks"
    params: Dict[str, Any] = {"limit": 100}
    if market:
        params["market"] = market

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})

    while url:
        r = session.get(url, params=params if url.startswith(API_BASE) else None, timeout=60)
        if r.status_code == 429:
            retry = int(r.headers.get("Retry-After", "3"))
            time.sleep(min(retry, 60))
            continue
        if r.status_code != 200:
            _die(f"Playlist items request failed ({r.status_code}): {r.text[:800]}")

        payload = r.json()
        for item in payload.get("items") or []:
            tr = item.get("track")
            parsed = _track_row(tr)
            if parsed:
                rows.append(parsed)

        next_url = payload.get("next")
        url = next_url if next_url else None
        params = {}

    return rows


def write_csv(path: str, rows: Iterable[Tuple[str, str, str]]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["artist", "album", "track"])
        w.writeheader()
        for artist, album, track in rows:
            w.writerow({"artist": artist, "album": album, "track": track})


def main() -> None:
    file_secrets = load_api_txt()
    default_redirect = (
        os.environ.get("SPOTIFY_REDIRECT_URI", "").strip()
        or file_secrets.get("spotify_redirect_uri", "").strip()
        or DEFAULT_REDIRECT_URI
    )

    parser = argparse.ArgumentParser(
        description="Download a Spotify playlist tracklist as CSV (artist, album, track). "
        "Uses OAuth PKCE + cached refresh token."
    )
    parser.add_argument(
        "playlist",
        nargs="?",
        help="Playlist Spotify URL or 22-character playlist ID",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="playlist_import.csv",
        help="Output CSV path (default: playlist_import.csv)",
    )
    parser.add_argument(
        "--market",
        default=os.environ.get("SPOTIFY_MARKET"),
        help="ISO country code for track availability (optional; env SPOTIFY_MARKET)",
    )
    parser.add_argument(
        "--token-cache",
        default=os.environ.get("SPOTIFY_TOKEN_CACHE"),
        type=Path,
        help=f"Token cache JSON path (default: {DEFAULT_TOKEN_CACHE})",
    )
    parser.add_argument(
        "--redirect-uri",
        default=default_redirect,
        help="Must match Spotify app settings (default: env SPOTIFY_REDIRECT_URI, api.txt [spotify], or "
        f"{DEFAULT_REDIRECT_URI})",
    )
    parser.add_argument(
        "--login-only",
        action="store_true",
        help="Only obtain/refresh tokens and exit (no playlist export)",
    )
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="Print the authorize URL instead of opening a browser",
    )
    args = parser.parse_args()

    token_path = args.token_cache or DEFAULT_TOKEN_CACHE
    client_id = (
        os.environ.get("SPOTIFY_CLIENT_ID", "").strip()
        or file_secrets.get("spotify_client_id", "").strip()
    )
    client_secret = (
        os.environ.get("SPOTIFY_CLIENT_SECRET", "").strip()
        or file_secrets.get("spotify_client_secret", "").strip()
    ) or None

    if not client_id:
        _die(
            "Set SPOTIFY_CLIENT_ID or add client_id under [spotify] in api.txt.\n"
            "See: https://developer.spotify.com/dashboard/applications\n"
            f"Register redirect URI: {args.redirect_uri}"
        )

    access_token = ensure_user_access_token(
        client_id,
        client_secret,
        args.redirect_uri,
        token_path,
        args.no_browser,
    )

    if args.login_only:
        print(f"Tokens saved to {token_path}", file=sys.stderr)
        return

    if not args.playlist:
        _die("playlist URL or ID is required unless --login-only is set.")

    playlist_id = parse_playlist_id(args.playlist)
    rows = fetch_playlist_track_rows(access_token, playlist_id, market=args.market)
    if not rows:
        print("Warning: no tracks exported (empty playlist or no readable tracks).", file=sys.stderr)

    write_csv(args.output, rows)
    print(f"Wrote {len(rows)} rows to {args.output}")


if __name__ == "__main__":
    main()
