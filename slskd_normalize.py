"""
Semantic normalization for queue metadata (Track B) and Soulseek filename matching (SRCH-02).

Merge-time rules are enabled one at a time after sign-off. See merge_queue.py.
``normalize_for_match`` is used when comparing CSV fields to SLSKD paths at rank time.
"""

from __future__ import annotations

import re
from typing import Dict, Optional, Tuple

_FEAT_PATTERN = re.compile(r"\b(?:feat\.?|featuring|ft\.?)\b", re.IGNORECASE)
_MATCH_PUNCT_PATTERN = re.compile(r"[^\w\s]", re.UNICODE)
_MATCH_WS_PATTERN = re.compile(r"\s+")

# Query strategies that search without album text — ranking should not require album in path.
TRACK_FOCUSED_SEARCH_STRATEGIES = frozenset(
    {"artist_track", "primary_artist_track"}
)


def normalize_artist_n1_primary_semicolon(artist: str) -> str:
    """
    NORM-01 — Primary artist for Spotify ``;``-separated credit lists.

    ``Artist A; Artist B`` → ``Artist A``. Does not split on ``,``, ``&``, or ``and``.
    """
    if not artist or ";" not in artist:
        return artist
    return artist.split(";", 1)[0].strip()


def normalize_artist_n2_strip_featuring(artist: str) -> str:
    """
    NORM-02 — Drop featuring credit tail from artist (merge column only).

    ``Artist feat. Guest`` → ``Artist``. Does not modify ``track``; bracketed feats in
    titles are unchanged.
    """
    if not artist:
        return artist
    match = _FEAT_PATTERN.search(artist)
    if not match:
        return artist
    head = artist[: match.start()].strip()
    return head.rstrip("-–—:").strip() or artist


def normalize_for_match(text: str) -> str:
    """
    Normalize text for loose substring comparison (CSV vs Soulseek path).

    Lowercase; underscores and punctuation become spaces; collapse whitespace.
    """
    if not text:
        return ""
    lowered = text.lower().replace("_", " ")
    softened = _MATCH_PUNCT_PATTERN.sub(" ", lowered)
    return _MATCH_WS_PATTERN.sub(" ", softened).strip()


def normalized_contains(haystack: str, needle: str) -> bool:
    """True when ``needle`` appears in ``haystack`` after both are normalized for match."""
    if not needle:
        return True
    if not haystack:
        return False
    norm_needle = normalize_for_match(needle)
    norm_haystack = normalize_for_match(haystack)
    return norm_needle in norm_haystack


def path_matches_row(
    path: str,
    *,
    artist: str,
    album: Optional[str] = None,
    track: Optional[str] = None,
    search_strategy: Optional[str] = None,
    require_album: Optional[bool] = None,
) -> Tuple[bool, bool]:
    """
    Match a Soulseek file path against CSV row fields using normalized comparison.

    Returns (match_ok, has_album_match).
    """
    album_s = (album or "").strip()
    track_s = (track or "").strip()
    artist_s = (artist or "").strip()

    if require_album is None:
        if album_s and track_s:
            require_album = (search_strategy or "") not in TRACK_FOCUSED_SEARCH_STRATEGIES
        else:
            require_album = bool(album_s)

    artist_ok = normalized_contains(path, artist_s) if artist_s else True
    track_ok = normalized_contains(path, track_s) if track_s else True
    album_ok = normalized_contains(path, album_s) if album_s else False

    if track_s and album_s:
        if normalize_for_match(album_s) == normalize_for_match(track_s):
            match_ok = artist_ok and track_ok
        elif require_album:
            match_ok = artist_ok and track_ok and album_ok
        else:
            match_ok = artist_ok and track_ok
    elif track_s:
        match_ok = artist_ok and track_ok
    elif album_s:
        match_ok = artist_ok and album_ok
    else:
        match_ok = artist_ok

    return match_ok, album_ok


def normalize_queue_row(row: Dict[str, str]) -> Dict[str, str]:
    """Apply enabled normalization rules to a queue-shaped row (in place)."""
    artist = row.get("artist", "")
    if not artist:
        return row
    if ";" in artist:
        artist = normalize_artist_n1_primary_semicolon(artist)
    artist = normalize_artist_n2_strip_featuring(artist)
    row["artist"] = artist
    return row
