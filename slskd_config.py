"""
Configuration defaults for the slskd_spotify workflow.
"""

import configparser
import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional

HOST = "http://localhost:5030"
API_PATH = "/api/v0"
SLSKD_API_KEY_PLACEHOLDER = "your-api-key-here"


def load_api_txt(path: Optional[Path] = None) -> Dict[str, str]:
    """
    Parse optional repo-root api.txt.

    Two formats:

    1) Legacy (unchanged): a single API key on the first non-empty, non-comment line —
       used only as the SLSKD X-API-KEY. No '=' or '[' in the file.

    2) INI-style (optional Spotify block):

        [slskd]
        api_key = ...

        [spotify]
        client_id = ...
        client_secret = ...
        redirect_uri = http://127.0.0.1:8765/callback

    Environment variables still override file values when set.
    """
    root = Path(__file__).resolve().parent
    path = path or (root / "api.txt")
    out: Dict[str, str] = {}
    if not path.is_file():
        return out
    _warn_api_txt_permissions(path)
    text = path.read_text(encoding="utf-8")
    if not text.strip():
        return out
    # Legacy single-line SLSKD key (do not treat as INI)
    if "=" not in text and "[" not in text:
        for line in text.splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                out["slskd_api_key"] = line
                break
        return out

    cp = configparser.ConfigParser()
    try:
        cp.read_string(text)
    except configparser.Error:
        return out

    if cp.has_section("slskd") and cp.has_option("slskd", "api_key"):
        v = cp.get("slskd", "api_key", fallback="").strip()
        if v:
            out["slskd_api_key"] = v

    if cp.has_section("spotify"):
        mapping = (
            ("client_id", "spotify_client_id"),
            ("client_secret", "spotify_client_secret"),
            ("redirect_uri", "spotify_redirect_uri"),
        )
        for opt, key in mapping:
            if cp.has_option("spotify", opt):
                v = cp.get("spotify", opt, fallback="").strip()
                if v:
                    out[key] = v

    return out


def _warn_api_txt_permissions(path: Path) -> None:
    """SEC-04: warn when api.txt is group/world readable."""
    try:
        mode = path.stat().st_mode & 0o777
    except OSError:
        return
    if mode & 0o077:
        print(
            f"Warning: {path} permissions are {oct(mode)}; "
            "recommend chmod 600 for API secrets.",
            file=sys.stderr,
        )


def read_slskd_api_key() -> str:
    """Return SLSKD API key from env or api.txt (empty if unset)."""
    env_key = os.environ.get("SLSKD_API_KEY", "").strip()
    if env_key:
        return env_key
    return _api_txt().get("slskd_api_key", "").strip()


def ensure_slskd_api_key() -> str:
    """SEC-03: exit with a clear message when the SLSKD API key is missing."""
    key = read_slskd_api_key()
    if not key or key == SLSKD_API_KEY_PLACEHOLDER:
        print(
            "SLSKD API key is required.\n"
            "Set SLSKD_API_KEY or add api_key under [slskd] in api.txt "
            f"(repo root, gitignored). Placeholder {SLSKD_API_KEY_PLACEHOLDER!r} is not allowed.",
            file=sys.stderr,
        )
        raise SystemExit(1)
    return key


_API_TXT_CACHE: Dict[str, str] = {}
_API_TXT_LOADED = False


def _api_txt() -> Dict[str, str]:
    global _API_TXT_CACHE, _API_TXT_LOADED
    if not _API_TXT_LOADED:
        _API_TXT_CACHE = load_api_txt()
        _API_TXT_LOADED = True
    return _API_TXT_CACHE


# Legacy import surface; slskd_spotify validates via ensure_slskd_api_key() at startup.
API_KEY = read_slskd_api_key() or SLSKD_API_KEY_PLACEHOLDER

CSV_FILE = "data/to_queue.csv"
QUEUE_LIMIT = 0

RATE_LIMIT_DELAY = 1.0
BATCH_SIZE = 10
MAX_RETRIES = 3
SEARCH_TIMEOUT = 60
ENQUEUE_TIMEOUT = 30
CHECKPOINT_BASENAME = "checkpoint.json"
CHECKPOINT_FILE = f"data/{CHECKPOINT_BASENAME}"
EXCLUDED_EXTENSIONS = [".lrc"]
CIRCUIT_BREAKER_THRESHOLD = 5
CIRCUIT_BREAKER_TIMEOUT = 300
ALLOWED_FORMATS = [".mp3", ".m4a", ".flac"]
POLL_INTERVAL = 2
MAX_POLLS = 20

# After the queue CSV is fully processed, wait before polling SLSKD transfer state.
DEFAULT_DOWNLOAD_SETTLE_SECONDS = 300


def make_headers(api_key: str) -> Dict[str, Any]:
    """Build API headers for SLSKD requests."""
    return {
        "X-API-KEY": api_key,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def initial_circuit_breaker_state() -> Dict[str, Any]:
    """Return the initial circuit breaker state dict."""
    return {
        "consecutive_errors": 0,
        "circuit_open": False,
        "circuit_open_time": 0,
    }

