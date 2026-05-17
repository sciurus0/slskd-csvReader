"""
Search query construction utilities for Soulseek/slskd.

Design goals:
- Prefer loose token searches over strict separator-based patterns.
- Preserve punctuation in the first attempt (literal form).
- Add punctuation-softened fallbacks as needed.
- Keep the number of search attempts bounded and deterministic.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from slskd_normalize import TRACK_FOCUSED_SEARCH_STRATEGIES, normalized_contains

DEFAULT_MAX_QUERY_ATTEMPTS = 3
SOLID_CLEANLINESS_THRESHOLD = 6.0
STRONG_CLEANLINESS_THRESHOLD = 8.0

_FEAT_PATTERN = re.compile(r"\b(?:feat\.?|featuring|ft\.?)\b", re.IGNORECASE)
_MULTI_ARTIST_SPLIT = re.compile(r"\s*(?:;|,|&|\band\b|\bx\b)\s*", re.IGNORECASE)
_PUNCT_SOFTEN_PATTERN = re.compile(r"[^\w\s]", re.UNICODE)
_WHITESPACE_PATTERN = re.compile(r"\s+")


def _compact_spaces(text: str) -> str:
    return _WHITESPACE_PATTERN.sub(" ", text or "").strip()


def soften_query_text(text: str) -> str:
    """Return a punctuation-softened variant while preserving token content."""
    softened = _PUNCT_SOFTEN_PATTERN.sub(" ", text or "")
    return _compact_spaces(softened)


def split_artist_variants(artist: str) -> Tuple[str, List[str]]:
    """
    Return (primary_artist, artist_variants).

    - primary_artist: best first artist token for fallback queries
    - artist_variants: cleaned artist chunks from composite fields
    """
    cleaned = _compact_spaces(_FEAT_PATTERN.sub(";", artist or ""))
    parts = [p for p in _MULTI_ARTIST_SPLIT.split(cleaned) if p]
    if not parts:
        cleaned_artist = _compact_spaces(artist)
        return cleaned_artist, [cleaned_artist] if cleaned_artist else []
    primary = _compact_spaces(parts[0])
    variants = [_compact_spaces(p) for p in parts if _compact_spaces(p)]
    return primary, variants


def is_solid_enqueue_pick(
    candidate: Dict[str, Any],
    *,
    album: str,
    track: str,
    search_strategy: str,
    album_preferred_search: bool = False,
) -> bool:
    """
    True when a ranked file is good enough to enqueue without trying another query.

    Used after search + rank + queue-slot pick: weak matches may trigger a fallback query.
    """
    if not candidate:
        return False

    cleanliness = float(candidate.get("cleanliness") or 0.0)
    if cleanliness >= STRONG_CLEANLINESS_THRESHOLD:
        return True

    if album_preferred_search:
        return cleanliness >= SOLID_CLEANLINESS_THRESHOLD

    album_s = (album or "").strip()
    track_s = (track or "").strip()
    path = candidate.get("filename") or ""

    if album_s and track_s:
        if candidate.get("has_album_match") and cleanliness >= 5.0:
            return True
        if search_strategy in TRACK_FOCUSED_SEARCH_STRATEGIES:
            return (
                cleanliness >= SOLID_CLEANLINESS_THRESHOLD
                and normalized_contains(path, track_s)
            )
        if search_strategy == "artist_album_track":
            return (
                cleanliness >= SOLID_CLEANLINESS_THRESHOLD
                and normalized_contains(path, album_s)
                and normalized_contains(path, track_s)
            )
        return False

    if track_s:
        return cleanliness >= SOLID_CLEANLINESS_THRESHOLD

    return cleanliness >= SOLID_CLEANLINESS_THRESHOLD


def enqueue_pick_score(
    pick: Optional[Dict[str, Any]], ranked: List[Dict[str, Any]]
) -> float:
    """Sort key for retaining the best weak match across query attempts."""
    if pick is not None:
        return float(pick.get("cleanliness") or 0.0)
    if ranked:
        return float(ranked[0].get("cleanliness") or 0.0)
    return 0.0


def build_query_candidates(
    *,
    artist: str,
    album: str,
    track: str,
    max_query_attempts: int = DEFAULT_MAX_QUERY_ATTEMPTS,
) -> List[Dict[str, object]]:
    """
    Build ordered, bounded Soulseek search queries (not download/file picks).

    Candidate order:
    1) literal query with punctuation preserved
    2) punctuation-softened query
    3) primary-artist fallback (if distinct), then optional album-expanded variant
    """
    primary_artist, _artist_variants = split_artist_variants(artist)

    candidates: List[Dict[str, object]] = []
    seen = set()
    def _append(strategy: str, query: str, variant: str) -> bool:
        q = _compact_spaces(query)
        if not q:
            return False
        key = q.lower()
        if key in seen:
            return False
        seen.add(key)
        candidates.append({"query": q, "strategy": strategy, "variant": variant})
        return len(candidates) >= max_query_attempts

    if track:
        artist_track = _compact_spaces(f"{artist} {track}")
        primary_artist_track = (
            _compact_spaces(f"{primary_artist} {track}")
            if primary_artist and primary_artist.lower() != _compact_spaces(artist).lower()
            else ""
        )
        artist_album_track = _compact_spaces(f"{artist} {album} {track}") if album else ""

        # Preferred order: literal artist+track, literal primary fallback, then softened forms.
        if _append("artist_track", artist_track, "literal"):
            return candidates
        if primary_artist_track and _append("primary_artist_track", primary_artist_track, "literal"):
            return candidates
        softened_artist_track = soften_query_text(artist_track)
        if softened_artist_track.lower() != artist_track.lower():
            if _append("artist_track", softened_artist_track, "softened"):
                return candidates
        if primary_artist_track:
            softened_primary = soften_query_text(primary_artist_track)
            if softened_primary.lower() != primary_artist_track.lower():
                if _append("primary_artist_track", softened_primary, "softened"):
                    return candidates
        if artist_album_track and _append("artist_album_track", artist_album_track, "literal"):
            return candidates
        softened_album_track = soften_query_text(artist_album_track) if artist_album_track else ""
        if softened_album_track and softened_album_track.lower() != artist_album_track.lower():
            if _append("artist_album_track", softened_album_track, "softened"):
                return candidates
    elif album:
        artist_album = _compact_spaces(f"{artist} {album}")
        primary_artist_album = (
            _compact_spaces(f"{primary_artist} {album}")
            if primary_artist and primary_artist.lower() != _compact_spaces(artist).lower()
            else ""
        )
        if _append("artist_album", artist_album, "literal"):
            return candidates
        if primary_artist_album and _append("primary_artist_album", primary_artist_album, "literal"):
            return candidates
        softened_artist_album = soften_query_text(artist_album)
        if softened_artist_album.lower() != artist_album.lower():
            if _append("artist_album", softened_artist_album, "softened"):
                return candidates
        if primary_artist_album:
            softened_primary_album = soften_query_text(primary_artist_album)
            if softened_primary_album.lower() != primary_artist_album.lower():
                if _append("primary_artist_album", softened_primary_album, "softened"):
                    return candidates
    else:
        _append("artist_only", artist, "literal")

    return candidates
