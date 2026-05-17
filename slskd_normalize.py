"""
Semantic normalization for queue metadata (Track B).

Rules are enabled one at a time after sign-off. See merge_queue.py (called before sanitize).
"""

from __future__ import annotations

from typing import Dict


def normalize_artist_n1_primary_semicolon(artist: str) -> str:
    """
    N1 — Primary artist for Spotify ``;``-separated credit lists.

    ``Artist A; Artist B`` → ``Artist A``. Does not split on ``,``, ``&``, or ``and``.
    """
    if not artist or ";" not in artist:
        return artist
    return artist.split(";", 1)[0].strip()


def normalize_queue_row(row: Dict[str, str]) -> Dict[str, str]:
    """Apply enabled normalization rules to a queue-shaped row (in place)."""
    artist = row.get("artist", "")
    if artist and ";" in artist:
        row["artist"] = normalize_artist_n1_primary_semicolon(artist)
    return row
