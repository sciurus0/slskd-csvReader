"""
Configuration defaults for the slskd-spotify workflow.
"""

import os
from typing import Dict, Any

HOST = "http://localhost:5030"
API_PATH = "/api/v0"
API_KEY = os.environ.get("SLSKD_API_KEY", "your-api-key-here")

CSV_FILE = "to_queue.csv"
QUEUE_LIMIT = 0

RATE_LIMIT_DELAY = 1.0
BATCH_SIZE = 10
MAX_RETRIES = 3
SEARCH_TIMEOUT = 60
ENQUEUE_TIMEOUT = 30
CHECKPOINT_FILE = "checkpoint.pkl"
EXCLUDED_EXTENSIONS = [".lrc"]
CIRCUIT_BREAKER_THRESHOLD = 5
CIRCUIT_BREAKER_TIMEOUT = 300
USE_DIRECT_API = False
EXACT_MATCH = False
ALBUM_PREFERRED_SEARCH = False

ALLOWED_FORMATS = [".mp3", ".m4a", ".flac"]
POLL_INTERVAL = 2
MAX_POLLS = 20

SOURCE = "HeadphonesVip"


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

