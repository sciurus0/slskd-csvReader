"""
Configuration defaults for the slskd_spotify workflow.
"""

from __future__ import annotations

import configparser
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional

DEFAULT_SLSKD_BASE_URL = "http://localhost:5030"
DEFAULT_SLSKD_API_PATH = "/api/v0"
SLSKD_API_KEY_PLACEHOLDER = "your-api-key-here"

# Backward-compatible names; prefer read_slskd_base_url() / read_slskd_api_path() at runtime.
HOST = DEFAULT_SLSKD_BASE_URL
API_PATH = DEFAULT_SLSKD_API_PATH


def _repo_root() -> Path:
    return Path(__file__).resolve().parent


def _normalize_base_url(url: str) -> str:
    return url.strip().rstrip("/")


def _normalize_api_path(path: str) -> str:
    path = path.strip() or DEFAULT_SLSKD_API_PATH
    if not path.startswith("/"):
        path = "/" + path
    return path.rstrip("/") or DEFAULT_SLSKD_API_PATH


def load_local_config(path: Optional[Path] = None) -> Dict[str, str]:
    """
    Parse optional local secrets/config.

    Precedence of *file* discovery (first existing wins):
      1) Explicit ``path`` argument
      2) ``CSVREADER_CONFIG_FILE`` env
      3) ``$CSVREADER_WORKSPACE/config.ini`` (NAS appdata)
      4) repo-root ``config.ini`` (preferred local)
      5) repo-root ``api.txt`` (legacy)

    Two file formats:

    1) Legacy (unchanged): a single API key on the first non-empty, non-comment line —
       used only as the SLSKD X-API-KEY. No '=' or '[' in the file.

    2) INI-style:

        [slskd]
        api_key = ...
        base_url = http://localhost:5030
        api_path = /api/v0

        [spotify]
        client_id = ...
        client_secret = ...
        redirect_uri = http://127.0.0.1:8765/callback

    Environment variables still override file values when set (see read_* helpers).
    """
    if path is not None:
        return _parse_local_config_file(path)

    env_path = os.environ.get("CSVREADER_CONFIG_FILE", "").strip()
    if env_path:
        return _parse_local_config_file(Path(env_path))

    workspace = os.environ.get("CSVREADER_WORKSPACE", "").strip()
    if workspace:
        ws_ini = Path(workspace) / "config.ini"
        if ws_ini.is_file():
            return _parse_local_config_file(ws_ini)

    root = _repo_root()
    config_ini = root / "config.ini"
    api_txt = root / "api.txt"
    if config_ini.is_file():
        return _parse_local_config_file(config_ini)
    if api_txt.is_file():
        out = _parse_local_config_file(api_txt)
        if out:
            print(
                "Note: using legacy api.txt; prefer config.ini "
                "(see config.ini.example).",
                file=sys.stderr,
            )
        return out
    return {}


def load_api_txt(path: Optional[Path] = None) -> Dict[str, str]:
    """Alias for load_local_config (historical name)."""
    return load_local_config(path)


def _parse_local_config_file(path: Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not path.is_file():
        return out
    _warn_secret_file_permissions(path)
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

    if cp.has_section("slskd"):
        if cp.has_option("slskd", "api_key"):
            v = cp.get("slskd", "api_key", fallback="").strip()
            if v:
                out["slskd_api_key"] = v
        if cp.has_option("slskd", "base_url"):
            v = cp.get("slskd", "base_url", fallback="").strip()
            if v:
                out["slskd_base_url"] = _normalize_base_url(v)
        if cp.has_option("slskd", "api_path"):
            v = cp.get("slskd", "api_path", fallback="").strip()
            if v:
                out["slskd_api_path"] = _normalize_api_path(v)

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


def _warn_secret_file_permissions(path: Path) -> None:
    """SEC-04: warn when secret config is group/world readable."""
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


# Keep historical name for tests / callers.
_warn_api_txt_permissions = _warn_secret_file_permissions


def read_slskd_api_key() -> str:
    """Return SLSKD API key from env or local config (empty if unset)."""
    env_key = os.environ.get("SLSKD_API_KEY", "").strip()
    if env_key:
        return env_key
    return _local_config().get("slskd_api_key", "").strip()


def read_slskd_base_url() -> str:
    """Return SLSKD HTTP base URL (no trailing slash). Env overrides file."""
    env = os.environ.get("SLSKD_BASE_URL", "").strip()
    if env:
        return _normalize_base_url(env)
    from_file = _local_config().get("slskd_base_url", "").strip()
    if from_file:
        return _normalize_base_url(from_file)
    return DEFAULT_SLSKD_BASE_URL


def read_slskd_api_path() -> str:
    """Return SLSKD API path prefix (e.g. /api/v0). Env overrides file."""
    env = os.environ.get("SLSKD_API_PATH", "").strip()
    if env:
        return _normalize_api_path(env)
    from_file = _local_config().get("slskd_api_path", "").strip()
    if from_file:
        return _normalize_api_path(from_file)
    return DEFAULT_SLSKD_API_PATH


def ensure_slskd_api_key() -> str:
    """SEC-03: exit with a clear message when the SLSKD API key is missing."""
    key = read_slskd_api_key()
    if not key or key == SLSKD_API_KEY_PLACEHOLDER:
        print(
            "SLSKD API key is required.\n"
            "Set SLSKD_API_KEY or add api_key under [slskd] in config.ini "
            f"(or legacy api.txt). Placeholder {SLSKD_API_KEY_PLACEHOLDER!r} is not allowed.",
            file=sys.stderr,
        )
        raise SystemExit(1)
    return key


_API_TXT_CACHE: Dict[str, str] = {}
_API_TXT_LOADED = False


def _local_config() -> Dict[str, str]:
    global _API_TXT_CACHE, _API_TXT_LOADED
    if not _API_TXT_LOADED:
        _API_TXT_CACHE = load_local_config()
        _API_TXT_LOADED = True
    return _API_TXT_CACHE


def _api_txt() -> Dict[str, str]:
    """Historical alias for _local_config()."""
    return _local_config()


def reset_local_config_cache() -> None:
    """Clear cached config (tests / after writing config.ini)."""
    global _API_TXT_CACHE, _API_TXT_LOADED
    _API_TXT_CACHE = {}
    _API_TXT_LOADED = False


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
