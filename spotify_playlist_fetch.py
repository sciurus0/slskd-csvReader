#!/usr/bin/env python3
"""
Export a Spotify playlist to a UTF-8 CSV. Core columns (artist, album, track) feed merge_queue.py;
additional columns capture API metadata for future matching/dedup (ignored by merge until wired).

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

Throttle cooldown cache (JSON, automatic):

    ~/.config/slskd/spotify_throttle.json
        Written when Spotify returns HTTP 429 with Retry-After.
        Later runs will short-circuit while cooldown is active to avoid repeated throttling.
        Delete this file only if you intentionally want to ignore the cached cooldown window.

First run opens a browser (or prints the authorize URL if --no-browser). After that,
scheduled/unattended runs use the cached refresh token until it is revoked.

Scopes: playlist-read-private, playlist-read-collaborative

Usage:
    python3 spotify_playlist_fetch.py "https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M"
    python3 spotify_playlist_fetch.py 37i9dQZF1DXcBWIGoYBM5M -o my_playlist.csv
    python3 spotify_playlist_fetch.py --list 1
    python3 spotify_playlist_fetch.py --list-playlists --list-limit 1
    python3 spotify_playlist_fetch.py --pick 3 -o my_export.csv
    python3 spotify_playlist_fetch.py --pick 1,4,7
    python3 spotify_playlist_fetch.py --login-only
    SPOTIFY_DEBUG=1 python3 spotify_playlist_fetch.py --list 1

Exports one CSV row per music track (including removed / unplayable tracks; ``is_unavailable``
may be set). Podcast episodes are the **only** playlist items omitted (NORM-05).
No market/availability filter. Playlist listing shows track counts only when the API includes them (? otherwise).

Throttle/stall protections:
    - Runtime guard aborts long calls after ~180s.
    - Consecutive 429 guard aborts after a small retry budget.
    - Very large Retry-After values abort immediately (fail fast).
    - 429 cooldown is cached locally to prevent repeated API hits during penalty windows.

Debug mode:
    Set SPOTIFY_DEBUG=1 to log request/response status, retry timing, and paging progress to stderr.

Credentials: export SPOTIFY_CLIENT_* env vars and/or add [spotify] to api.txt as documented above.
"""

from __future__ import annotations

import argparse
import base64
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
from datetime import datetime
from pathlib import Path

from slskd_export_paths import default_new_export_path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, urlencode, urlparse

import requests

from slskd_config import load_api_txt
from slskd_csv import atomic_write_pipeline_csv

ACCOUNTS_AUTHORIZE_URL = "https://accounts.spotify.com/authorize"
ACCOUNTS_TOKEN_URL = "https://accounts.spotify.com/api/token"
API_BASE = "https://api.spotify.com/v1"
# GET /playlists/{id}/items — max limit 50; use "item" on each row (legacy "track" is deprecated).
PLAYLIST_ITEMS_PAGE_LIMIT = 50

# Locked export schema (wide capture). merge_queue.py stores artist/album/track + duration_ms,
# spotify_track_id, is_unavailable on to_queue.csv.
EXPORT_CSV_COLUMNS: Tuple[str, ...] = (
    "artist",
    "album",
    "track",
    "duration_ms",
    "spotify_track_id",
    "playlist_id",
    "disc_number",
    "track_number",
    "added_at",
    "is_unavailable",
)

DEFAULT_REDIRECT_URI = "http://127.0.0.1:8765/callback"
DEFAULT_TOKEN_CACHE = Path.home() / ".config" / "slskd" / "spotify_tokens.json"
DEFAULT_THROTTLE_CACHE = Path.home() / ".config" / "slskd" / "spotify_throttle.json"

SCOPES = "playlist-read-private playlist-read-collaborative"
MAX_API_RUNTIME_SECONDS = 180
MAX_429_RETRIES = 5
MAX_RETRY_AFTER_SECONDS = 300


def _die(msg: str, code: int = 1) -> None:
    print(msg, file=sys.stderr)
    raise SystemExit(code)


def _debug_enabled() -> bool:
    return bool(os.environ.get("SPOTIFY_DEBUG"))


def _debug_log(msg: str) -> None:
    if _debug_enabled():
        ts = time.strftime("%H:%M:%S")
        print(f"[spotify-debug {ts}] {msg}", file=sys.stderr)


def _check_deadline(start_ts: float, context: str) -> None:
    elapsed = time.time() - start_ts
    if elapsed > MAX_API_RUNTIME_SECONDS:
        _die(
            f"{context} aborted after {elapsed:.1f}s (stall guard). "
            "Try again later or reduce scope with --list N, --list-one, or --list-limit."
        )


def _retry_after_seconds(headers: Dict[str, Any], default: int = 3) -> int:
    raw = headers.get("Retry-After")
    if raw is None:
        return default
    try:
        return max(1, int(str(raw).strip()))
    except (TypeError, ValueError):
        return default


def _spotify_get_json(
    session: requests.Session,
    *,
    url: str,
    params: Optional[Dict[str, Any]],
    start_ts: float,
    context: str,
    request_idx: int,
    throttle_cache_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """GET JSON with lean stall protection and clear throttle aborts."""
    rate_limit_retries = 0
    while True:
        _check_deadline(start_ts, context)
        _debug_log(f"{context} request {request_idx}: GET {url} params={params}")
        try:
            r = session.get(url, params=params, timeout=60)
        except requests.RequestException as exc:
            _die(f"{context} request failed: {exc}")
        _debug_log(f"{context} response {request_idx}: HTTP {r.status_code}")

        if r.status_code == 429:
            rate_limit_retries += 1
            retry = _retry_after_seconds(r.headers)
            _debug_log(f"{context} retry-after={retry}s (429 #{rate_limit_retries})")
            if throttle_cache_path is not None:
                now = time.time()
                _save_throttle_cache(
                    throttle_cache_path,
                    {
                        "source": context,
                        "cooldown_until_epoch": now + retry,
                        "retry_after_seconds": retry,
                        "recorded_at_epoch": now,
                    },
                )
            if retry > MAX_RETRY_AFTER_SECONDS:
                _die(
                    f"{context} throttled: Retry-After={retry}s is too high. "
                    "Aborting early to avoid a long hang."
                )
            if rate_limit_retries > MAX_429_RETRIES:
                _die(
                    f"{context} aborted after {MAX_429_RETRIES} consecutive 429 responses "
                    "(stall guard)."
                )
            time.sleep(retry)
            continue

        if r.status_code != 200:
            extra = ""
            if r.status_code == 403 and (
                context.startswith("Playlist export") or context.startswith("Track-count lookup")
            ):
                extra = (
                    "\nNote: Spotify allows playlist items only for playlists you own or collaborate on; "
                    "playlists you only follow may appear in /me/playlists but return 403 here."
                )
            _die(f"{context} request failed ({r.status_code}): {r.text[:800]}{extra}")

        try:
            payload = r.json()
        except json.JSONDecodeError:
            _die(f"{context} returned non-JSON response.")
        if not isinstance(payload, dict):
            _die(f"{context} returned unexpected payload type.")
        return payload


def _safe_non_negative_int(value: Any) -> Optional[int]:
    """Coerce API numeric fields that may be int-like."""
    if value is None or isinstance(value, bool):
        return None
    try:
        n = int(value)
    except (TypeError, ValueError):
        return None
    return n if n >= 0 else None


def _tracks_total_from_summary_playlist(pl: Dict[str, Any]) -> Optional[int]:
    """Read tracks.total from a playlist object as returned by GET /me/playlists items."""
    tw = pl.get("tracks")
    if isinstance(tw, dict):
        return _safe_non_negative_int(tw.get("total"))
    return None


def parse_pick_indices(raw: str) -> List[int]:
    """Parse --pick value: one index or comma-separated 1-based indices (e.g. 3 or 1,4,7)."""
    s = (raw or "").strip()
    if not s:
        _die("--pick requires at least one playlist number.")
    parts = [p.strip() for p in s.split(",") if p.strip()]
    if not parts:
        _die("--pick requires at least one playlist number.")
    indices: List[int] = []
    for part in parts:
        try:
            n = int(part)
        except ValueError:
            _die(f"Invalid --pick index {part!r} (use integers, e.g. 1 or 1,4,7).")
        if n < 1:
            _die(f"--pick index must be >= 1, got {n}.")
        indices.append(n)
    # Preserve order; drop duplicate indices (same playlist exported once).
    return list(dict.fromkeys(indices))


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


def _load_throttle_cache(path: Path) -> Optional[Dict[str, Any]]:
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def _save_throttle_cache(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    try:
        path.chmod(0o600)
    except OSError:
        pass


def _enforce_throttle_cooldown(path: Path) -> None:
    data = _load_throttle_cache(path)
    if not data:
        return
    until = data.get("cooldown_until_epoch")
    if until is None:
        return
    try:
        until_f = float(until)
    except (TypeError, ValueError):
        return
    now = time.time()
    if now >= until_f:
        return
    remaining = int(until_f - now)
    source = data.get("source", "Spotify API")
    _die(
        f"Throttle cooldown active ({source}). Retry in ~{remaining}s. "
        "Skipping API calls to avoid repeated 429s."
    )


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
    _debug_log("Token exchange: POST /api/token (authorization_code)")
    r = requests.post(ACCOUNTS_TOKEN_URL, data=data, timeout=60)
    _debug_log(f"Token exchange response: HTTP {r.status_code}")
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
    _debug_log("Token refresh: POST /api/token (refresh_token)")
    r = requests.post(ACCOUNTS_TOKEN_URL, data=data, timeout=60)
    _debug_log(f"Token refresh response: HTTP {r.status_code}")
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


def _export_int_field(value: Any) -> str:
    if value is None or value == "":
        return ""
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return ""


def _empty_export_row(
    *,
    playlist_id: str = "",
    added_at: str = "",
    is_unavailable: str = "true",
) -> Dict[str, str]:
    return {col: "" for col in EXPORT_CSV_COLUMNS} | {
        "playlist_id": playlist_id,
        "added_at": added_at,
        "is_unavailable": is_unavailable,
    }


def playlist_item_to_row(item: Dict[str, Any], *, playlist_id: str = "") -> Dict[str, str]:
    """
    Map one playlist item to an export row (EXPORT_CSV_COLUMNS).

    Always returns a row for non-episode items — no playability/market filter.
    Missing or removed tracks keep artist/album/track empty with is_unavailable=true.

    Accepts PlaylistTrackObject from GET /playlists/{id}/items: primary field is ``item``;
    legacy ``track`` is still recognized. ``added_at`` comes from the playlist item envelope.
    """
    added_at = (item.get("added_at") or "").strip()
    tr = item.get("item") or item.get("track")
    if tr is None:
        return _empty_export_row(
            playlist_id=playlist_id,
            added_at=added_at,
            is_unavailable="true",
        )

    artists = tr.get("artists") or []
    names = [a.get("name", "").strip() for a in artists if a.get("name")]
    artist = "; ".join(names) if names else ""

    album_obj = tr.get("album") or {}
    album = (album_obj.get("name") or "").strip()
    title = (tr.get("name") or "").strip()

    is_playable = tr.get("is_playable")
    is_unavailable = "true" if is_playable is False else "false"

    return {
        "artist": artist,
        "album": album,
        "track": title,
        "duration_ms": _export_int_field(tr.get("duration_ms")),
        "spotify_track_id": (tr.get("id") or "").strip(),
        "playlist_id": (playlist_id or "").strip(),
        "disc_number": _export_int_field(tr.get("disc_number")),
        "track_number": _export_int_field(tr.get("track_number")),
        "added_at": added_at,
        "is_unavailable": is_unavailable,
    }


def fetch_playlist_track_rows(token: str, playlist_id: str) -> List[Dict[str, str]]:
    """Fetch every playlist item row (paginated). One CSV row per API item, no market filter."""
    rows: List[Dict[str, str]] = []
    url = f"{API_BASE}/playlists/{playlist_id}/items"
    params: Dict[str, Any] = {"limit": PLAYLIST_ITEMS_PAGE_LIMIT}
    start_ts = time.time()

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})
    page_no = 0

    while url:
        page_no += 1
        payload = _spotify_get_json(
            session,
            url=url,
            params=params if url.startswith(API_BASE) else None,
            start_ts=start_ts,
            context="Playlist export",
            request_idx=page_no,
            throttle_cache_path=DEFAULT_THROTTLE_CACHE,
        )
        for item in payload.get("items") or []:
            tr = item.get("item") or item.get("track")
            # NORM-05: episodes are never exported; all music tracks are kept regardless of playability.
            if tr and tr.get("type") == "episode":
                continue
            rows.append(playlist_item_to_row(item, playlist_id=playlist_id))

        next_url = payload.get("next")
        url = next_url if next_url else None
        params = {}
        _debug_log(
            f"Playlist export page {page_no}: items={len(payload.get('items') or [])} "
            f"next={'yes' if url else 'no'} elapsed={time.time() - start_ts:.1f}s"
        )

    return rows


def _fetch_playlist_total_quick(session: requests.Session, playlist_id: str) -> int:
    """Best-effort total count from GET /playlists/{id}/items."""
    pid = (playlist_id or "").strip()
    if not pid:
        return -1
    url = f"{API_BASE}/playlists/{pid}/items"
    params: Dict[str, Any] = {"limit": 1}

    try:
        payload = _spotify_get_json(
            session,
            url=url,
            params=params,
            start_ts=time.time(),
            context=f"Track-count lookup ({pid})",
            request_idx=1,
        )
    except SystemExit:
        return -1
    n = _safe_non_negative_int(payload.get("total"))
    return n if n is not None else -1


def fetch_user_playlists(
    token: str,
    *,
    max_playlists: Optional[int] = None,
    resolve_track_counts: bool = False,
) -> List[Dict[str, Any]]:
    """Return current user's playlists (paginated). Each dict has id, name, tracks_total, owner, public.

    max_playlists: stop after this many playlists (fewer HTTP calls to /me/playlists).

    tracks_total is only what GET /me/playlists includes per item unless resolve_track_counts=True.
    """
    out: List[Dict[str, Any]] = []
    url = f"{API_BASE}/me/playlists"
    page_limit = min(50, max_playlists) if max_playlists is not None else 50
    page_limit = max(1, page_limit)
    params: Dict[str, Any] = {"limit": page_limit}
    start_ts = time.time()

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})
    page_no = 0

    while url:
        page_no += 1
        payload = _spotify_get_json(
            session,
            url=url,
            params=params if url.startswith(API_BASE) else None,
            start_ts=start_ts,
            context="Playlist listing",
            request_idx=page_no,
            throttle_cache_path=DEFAULT_THROTTLE_CACHE,
        )
        for pl in payload.get("items") or []:
            if max_playlists is not None and len(out) >= max_playlists:
                break
            tid = (pl.get("id") or "").strip()
            name = (pl.get("name") or "").strip()
            parsed_total = _tracks_total_from_summary_playlist(pl)
            tracks_total = parsed_total if parsed_total is not None else -1
            owner = (pl.get("owner") or {}).get("display_name") or (pl.get("owner") or {}).get("id") or ""
            public = pl.get("public")
            out.append(
                {
                    "id": tid,
                    "name": name,
                    "tracks_total": tracks_total,
                    "owner": owner,
                    "public": public,
                }
            )

        if max_playlists is not None and len(out) >= max_playlists:
            break

        next_url = payload.get("next")
        url = next_url if next_url else None
        params = {}
        _debug_log(
            f"Playlist listing page {page_no}: items={len(payload.get('items') or [])} "
            f"total_so_far={len(out)} next={'yes' if url else 'no'} "
            f"elapsed={time.time() - start_ts:.1f}s"
        )

    if resolve_track_counts:
        for entry in out:
            if entry["tracks_total"] < 0 and entry["id"]:
                n = _fetch_playlist_total_quick(session, entry["id"])
                if n >= 0:
                    entry["tracks_total"] = n

    return out


def print_user_playlists(playlists: List[Dict[str, Any]]) -> None:
    """Print playlists for picking (stdout)."""
    if not playlists:
        print("No playlists returned.", file=sys.stderr)
        return
    # Track column intentionally omitted for now; API count resolution is disabled.
    print(f"{'#':>3}  {'owner':<16}  {'id':<24}  name")
    print("-" * 92)
    for i, pl in enumerate(playlists, start=1):
        name = pl["name"].replace("\n", " ")
        if len(name) > 56:
            name = name[:53] + "..."
        oid = pl["id"] or "?"
        owner = (pl["owner"] or "?")[:16]
        print(f"{i:3}  {owner:<16}  {oid:<24}  {name}")


def write_csv(path: str, rows: Iterable[Dict[str, str]]) -> None:
    """Write export CSV (raw API fields). Sanitization happens in merge_queue.py only."""
    normalized: List[Dict[str, str]] = []
    for row in rows:
        normalized.append({col: (row.get(col) or "") for col in EXPORT_CSV_COLUMNS})
    atomic_write_pipeline_csv(path, normalized, fieldnames=EXPORT_CSV_COLUMNS)


def main() -> None:
    file_secrets = load_api_txt()
    default_redirect = (
        os.environ.get("SPOTIFY_REDIRECT_URI", "").strip()
        or file_secrets.get("spotify_redirect_uri", "").strip()
        or DEFAULT_REDIRECT_URI
    )

    parser = argparse.ArgumentParser(
        description="Download a Spotify playlist tracklist as CSV (artist, album, track). "
        "Uses OAuth PKCE + cached refresh token. Includes 429 cooldown caching and stall guards."
    )
    parser.add_argument(
        "playlist",
        nargs="?",
        help="Playlist Spotify URL or 22-character playlist ID (omit if using --pick)",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=None,
        help="Output CSV path (default: <cwd>/YYYYMMDD-spotify-export.csv, today's local date)",
    )
    parser.add_argument(
        "--list-playlists",
        action="store_true",
        help="List your playlists (numbered, stdout) and exit",
    )
    parser.add_argument(
        "--list",
        type=int,
        metavar="N",
        default=None,
        help="List your playlists; fetch at most N (implies --list-playlists; same as --list-limit N)",
    )
    parser.add_argument(
        "--list-limit",
        type=int,
        metavar="N",
        default=None,
        help="With --list-playlists: fetch at most N playlists (smaller N = fewer /me/playlists calls)",
    )
    parser.add_argument(
        "--list-one",
        action="store_true",
        help="With --list-playlists: same as --list 1 / --list-limit 1 (single row, minimal listing traffic)",
    )
    parser.add_argument(
        "--pick",
        metavar="N[,N...]",
        default=None,
        help="Export playlist(s) by 1-based index (same order as --list-playlists). "
        "Examples: --pick 3 or --pick 1,4,7 (combined into one output CSV)",
    )
    parser.add_argument(
        "--token-cache",
        default=os.environ.get("SPOTIFY_TOKEN_CACHE"),
        type=Path,
        help=(
            f"Token cache JSON path (default: {DEFAULT_TOKEN_CACHE}). "
            "429 cooldown cache is separate and automatic at ~/.config/slskd/spotify_throttle.json."
        ),
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
        help="Obtain/refresh tokens and exit without writing a playlist CSV (still runs --list-playlists if set)",
    )
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="Print the authorize URL instead of opening a browser",
    )
    args = parser.parse_args()

    if args.pick is not None and args.playlist:
        _die("Specify either a playlist URL/ID or --pick N, not both.")
    if args.list_playlists and args.pick is not None:
        _die("Do not combine --list-playlists with --pick.")

    if args.list is not None:
        if args.list < 1:
            _die("--list N requires N >= 1")
        if args.list_limit is not None:
            _die("Use either --list N or --list-limit N, not both.")
        if args.list_one:
            _die("Use either --list N or --list-one, not both.")
        args.list_playlists = True
        args.list_limit = args.list

    if args.list_one:
        if args.list_limit is not None:
            _die("Use either --list-one or --list-limit, not both.")
        args.list_limit = 1

    if args.list_limit is not None:
        if args.list_limit < 1:
            _die("--list-limit must be >= 1")
        if not args.list_playlists:
            _die("--list-limit only applies with --list-playlists.")

    needs_export = (
        not args.list_playlists
        and not args.login_only
        and (args.pick is not None or bool(args.playlist))
    )
    if needs_export and args.output is None:
        path, _ = default_new_export_path(Path.cwd())
        args.output = str(path)

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

    # Skip runs during known API cooldown windows.
    _enforce_throttle_cooldown(DEFAULT_THROTTLE_CACHE)

    if args.list_playlists:
        playlists = fetch_user_playlists(
            access_token,
            max_playlists=args.list_limit,
            # Track-count resolution is temporarily disabled to keep listing fast/reliable.
            resolve_track_counts=False,
        )
        print_user_playlists(playlists)
        if args.login_only:
            print(f"Tokens saved to {token_path}", file=sys.stderr)
        return

    if args.login_only:
        print(f"Tokens saved to {token_path}", file=sys.stderr)
        return

    if args.pick is not None:
        pick_indices = parse_pick_indices(args.pick)
        max_index = max(pick_indices)
        playlists = fetch_user_playlists(access_token, max_playlists=max_index)
        if max_index > len(playlists):
            _die(
                f"--pick {max_index} is out of range (you have {len(playlists)} playlists in this listing)."
            )
        all_rows: List[Dict[str, str]] = []
        for idx in pick_indices:
            pl = playlists[idx - 1]
            playlist_id = pl["id"]
            if not playlist_id:
                _die(f"Playlist #{idx} has no id; cannot export.")
            label = pl.get("name") or playlist_id
            rows = fetch_playlist_track_rows(access_token, playlist_id)
            print(f"Playlist #{idx} ({label}): {len(rows)} rows", file=sys.stderr)
            all_rows.extend(rows)
        if not all_rows:
            print(
                "Warning: no tracks exported (empty playlist(s) or no readable tracks).",
                file=sys.stderr,
            )
        write_csv(args.output, all_rows)
        print(f"Wrote {len(all_rows)} rows to {args.output}")
    elif args.playlist:
        playlist_id = parse_playlist_id(args.playlist)
        rows = fetch_playlist_track_rows(access_token, playlist_id)
        if not rows:
            print("Warning: no tracks exported (empty playlist or no readable tracks).", file=sys.stderr)
        write_csv(args.output, rows)
        print(f"Wrote {len(rows)} rows to {args.output}")
    else:
        _die("playlist URL/ID, or --pick N, is required unless --login-only or --list-playlists is set.")


if __name__ == "__main__":
    main()
