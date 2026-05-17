"""
Semantic normalization for queue metadata (Track B).

Rules are enabled one at a time after sign-off. See merge_queue.py (called before sanitize).
"""

from __future__ import annotations

import re
from typing import Dict

_FEAT_PATTERN = re.compile(r"\b(?:feat\.?|featuring|ft\.?)\b", re.IGNORECASE)


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
