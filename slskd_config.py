"""
Configuration defaults for the slskd-spotify workflow.
"""

import configparser
import os
from pathlib import Path
from typing import Dict, Any, Optional

HOST = "http://localhost:5030"
API_PATH = "/api/v0"


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


_API_TXT_CACHE: Dict[str, str] = {}
_API_TXT_LOADED = False


def _api_txt() -> Dict[str, str]:
    global _API_TXT_CACHE, _API_TXT_LOADED
    if not _API_TXT_LOADED:
        _API_TXT_CACHE = load_api_txt()
        _API_TXT_LOADED = True
    return _API_TXT_CACHE


API_KEY = (
    os.environ.get("SLSKD_API_KEY", "").strip()
    or _api_txt().get("slskd_api_key", "").strip()
    or "your-api-key-here"
)

CSV_FILE = "data/to_queue.csv"
QUEUE_LIMIT = 0

RATE_LIMIT_DELAY = 1.0
BATCH_SIZE = 10
MAX_RETRIES = 3
SEARCH_TIMEOUT = 60
ENQUEUE_TIMEOUT = 30
CHECKPOINT_BASENAME = "checkpoint.pkl"
CHECKPOINT_FILE = f"data/{CHECKPOINT_BASENAME}"
EXCLUDED_EXTENSIONS = [".lrc"]
CIRCUIT_BREAKER_THRESHOLD = 5
CIRCUIT_BREAKER_TIMEOUT = 300
USE_DIRECT_API = False
EXACT_MATCH = False
ALBUM_PREFERRED_SEARCH = False

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

