"""
Semantic normalization for queue metadata (Track B) and Soulseek filename matching (SRCH-02).

Merge-time rules are enabled one at a time after sign-off. See merge_queue.py.
``normalize_for_match`` is used when comparing CSV fields to SLSKD paths at rank time.
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple

from slskd_sanitize import sanitize_queue_field

_FEAT_PATTERN = re.compile(r"\b(?:feat\.?|featuring|ft\.?)\b", re.IGNORECASE)
_COLLAB_LIST_SPLIT = re.compile(r"\s*[,;]\s*")
_MATCH_PUNCT_PATTERN = re.compile(r"[^\w\s]", re.UNICODE)
_MATCH_WS_PATTERN = re.compile(r"\s+")

# Separator for ``artist_alternates`` column (pipe avoids CSV comma issues).
ARTIST_ALTERNATES_SEP = " | "


def parse_artist_alternates_list(alternates: str) -> List[str]:
    """Split ``artist_alternates`` column text into non-empty collaborator names."""
    if not (alternates or "").strip():
        return []
    return [p.strip() for p in alternates.split(ARTIST_ALTERNATES_SEP) if p.strip()]

# Query strategies that search without album text — ranking should not require album in path.
TRACK_FOCUSED_SEARCH_STRATEGIES = frozenset(
    {
        "artist_track",
        "primary_artist_track",
        "alternate_artist_track",
        "alternate_artist_album",
    }
)


def normalize_artist_n1_primary_semicolon(artist: str) -> str:
    """
    NORM-01 — Primary artist for Spotify ``;``-separated credit lists.

    ``Artist A; Artist B`` → ``Artist A``. Does not split on ``,``, ``&``, or ``and``.
    """
    if not artist or ";" not in artist:
        return artist
    return artist.split(";", 1)[0].strip()


def normalize_artist_n3_primary_comma(artist: str) -> str:
    """
    NORM-03 — Primary artist for comma-separated credit lists (merge column only).

    ``Artist A, Artist B`` → ``Artist A``. Does not split on ``&`` or ``and`` so band
    names like ``Nick Cave & The Bad Seeds`` stay intact (same rules at search time).
    """
    if not artist or "," not in artist:
        return artist
    return artist.split(",", 1)[0].strip()


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


def split_artist_credits(artist: str) -> Tuple[str, str, str]:
    """
    Split sanitized artist credits for wide queue columns (NORM-03).

    Returns (artist_full, artist_primary, artist_alternates).

    - ``artist_full``: full credit string (unchanged).
    - ``artist_primary``: first segment after feat. strip; split only on ``,`` / ``;``.
    - ``artist_alternates``: remaining segments joined with ``ARTIST_ALTERNATES_SEP``.

    Does not split on ``&`` or ``and`` (band names stay one segment).
    """
    full = (artist or "").strip()
    if not full:
        return "", "", ""

    after_feat = normalize_artist_n2_strip_featuring(full)
    parts = [p for p in _COLLAB_LIST_SPLIT.split(after_feat) if p and p.strip()]
    if not parts:
        return full, full, ""
    if len(parts) == 1:
        return full, parts[0].strip(), ""

    primary = parts[0].strip()
    alternates = ARTIST_ALTERNATES_SEP.join(p.strip() for p in parts[1:] if p.strip())
    return full, primary, alternates


def primary_artist_for_merge(artist: str) -> str:
    """Primary artist for dedupe / default search (first credit segment)."""
    _full, primary, _alternates = split_artist_credits(artist)
    return primary or (artist or "").strip()


def normalize_album_track_n4(row: Dict[str, str]) -> Dict[str, str]:
    """
    NORM-04 — album and track columns at merge (sanitize only).

    Applies :func:`slskd_sanitize.sanitize_queue_field` to ``album`` and ``track`` only.
    Does not strip remix/live/feat./soundtrack markers. See ``slskd_sanitize`` module doc
    for the approved transform list (Phase A sign-off).
    """
    row["album"] = sanitize_queue_field((row.get("album") or "").strip())
    row["track"] = sanitize_queue_field((row.get("track") or "").strip())
    return row


def normalize_queue_row(row: Dict[str, str]) -> Dict[str, str]:
    """
    Apply merge-time **artist** rules (in place).

    Expects ``artist`` already sanitized. Preserves full credits in ``artist``;
    sets ``artist_primary`` and ``artist_alternates`` for dedupe and search.

    Album and track are **not** modified here — use :func:`normalize_album_track_n4`.
    """
    artist = (row.get("artist") or "").strip()
    if not artist:
        row["artist_primary"] = ""
        row["artist_alternates"] = ""
        return row

    full, primary, alternates = split_artist_credits(artist)
    row["artist"] = full
    row["artist_primary"] = primary
    row["artist_alternates"] = alternates
    return row
