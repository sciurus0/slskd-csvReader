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
from typing import Any, Dict, List, Optional, Sequence, Tuple

from slskd_normalize import (
    TRACK_FOCUSED_SEARCH_STRATEGIES,
    normalized_contains,
    parse_artist_alternates_list,
    split_artist_credits,
)

DEFAULT_MAX_QUERY_ATTEMPTS = 3
DEFAULT_MAX_QUERY_ATTEMPTS_WITH_ALTERNATE = 4
SOLID_CLEANLINESS_THRESHOLD = 6.0
STRONG_CLEANLINESS_THRESHOLD = 8.0

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
    Return (primary_artist, artist_variants) for query fallbacks.

    Uses NORM-03 credit rules via ``split_artist_credits`` (comma/semicolon only;
    ``&`` / ``and`` band names stay one segment). Callers should pass
    ``artist_primary`` from the queue when available.
    """
    _full, primary, alternates_str = split_artist_credits(artist or "")
    variants: List[str] = []
    for name in [primary, *parse_artist_alternates_list(alternates_str)]:
        compact = _compact_spaces(name)
        if compact and compact not in variants:
            variants.append(compact)
    if not variants:
        cleaned = _compact_spaces(artist)
        return cleaned, [cleaned] if cleaned else []
    return variants[0], variants


def first_distinct_alternate(
    primary_artist: str, alternates: Optional[Sequence[str]]
) -> str:
    """First ``artist_alternates`` entry that differs from ``primary_artist`` (case-insensitive)."""
    primary_key = _compact_spaces(primary_artist).lower()
    for alt in alternates or []:
        compact = _compact_spaces(alt)
        if compact and compact.lower() != primary_key:
            return compact
    return ""


def is_solid_enqueue_pick(
    candidate: Dict[str, Any],
    *,
    album: str,
    track: str,
    search_strategy: str,
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
    artist_alternates: Optional[Sequence[str]] = None,
    max_query_attempts: int = DEFAULT_MAX_QUERY_ATTEMPTS,
) -> List[Dict[str, object]]:
    """
    Build ordered, bounded Soulseek search queries (not download/file picks).

    Candidate order:
    1) literal query with punctuation preserved
    2) punctuation-softened query
    3) primary-artist fallback (if distinct), then optional album-expanded variant
    4) first distinct ``artist_alternates`` name (literal), when provided

    When an alternate is present, ``max_query_attempts`` is typically 4: up to 3 primary
    attempts plus one alternate query. Alternate candidates include ``search_artist`` for ranking.
    """
    primary_artist, _artist_variants = split_artist_variants(artist)
    alternate_artist = first_distinct_alternate(artist, artist_alternates)
    primary_limit = (
        max_query_attempts - 1 if alternate_artist else max_query_attempts
    )

    candidates: List[Dict[str, object]] = []
    seen = set()

    def _append(
        strategy: str,
        query: str,
        variant: str,
        *,
        search_artist: Optional[str] = None,
        limit: int = primary_limit,
    ) -> bool:
        q = _compact_spaces(query)
        if not q:
            return False
        key = q.lower()
        if key in seen:
            return False
        seen.add(key)
        entry: Dict[str, object] = {
            "query": q,
            "strategy": strategy,
            "variant": variant,
        }
        if search_artist:
            entry["search_artist"] = search_artist
        candidates.append(entry)
        return len(candidates) >= limit

    def _append_alternate(strategy: str, query: str, variant: str, search_artist: str) -> None:
        if len(candidates) >= max_query_attempts:
            return
        _append(
            strategy,
            query,
            variant,
            search_artist=search_artist,
            limit=max_query_attempts,
        )

    if track:
        artist_track = _compact_spaces(f"{artist} {track}")
        primary_artist_track = (
            _compact_spaces(f"{primary_artist} {track}")
            if primary_artist and primary_artist.lower() != _compact_spaces(artist).lower()
            else ""
        )
        artist_album_track = _compact_spaces(f"{artist} {album} {track}") if album else ""

        # Preferred order: literal artist+track, literal primary fallback, then softened forms.
        if len(candidates) < primary_limit:
            _append("artist_track", artist_track, "literal")
        if len(candidates) < primary_limit and primary_artist_track:
            _append("primary_artist_track", primary_artist_track, "literal")
        softened_artist_track = soften_query_text(artist_track)
        if (
            len(candidates) < primary_limit
            and softened_artist_track.lower() != artist_track.lower()
        ):
            _append("artist_track", softened_artist_track, "softened")
        if len(candidates) < primary_limit and primary_artist_track:
            softened_primary = soften_query_text(primary_artist_track)
            if softened_primary.lower() != primary_artist_track.lower():
                _append("primary_artist_track", softened_primary, "softened")
        if len(candidates) < primary_limit and artist_album_track:
            _append("artist_album_track", artist_album_track, "literal")
        softened_album_track = soften_query_text(artist_album_track) if artist_album_track else ""
        if (
            len(candidates) < primary_limit
            and softened_album_track
            and softened_album_track.lower() != artist_album_track.lower()
        ):
            _append("artist_album_track", softened_album_track, "softened")
    elif album:
        artist_album = _compact_spaces(f"{artist} {album}")
        primary_artist_album = (
            _compact_spaces(f"{primary_artist} {album}")
            if primary_artist and primary_artist.lower() != _compact_spaces(artist).lower()
            else ""
        )
        if len(candidates) < primary_limit:
            _append("artist_album", artist_album, "literal")
        if len(candidates) < primary_limit and primary_artist_album:
            _append("primary_artist_album", primary_artist_album, "literal")
        softened_artist_album = soften_query_text(artist_album)
        if (
            len(candidates) < primary_limit
            and softened_artist_album.lower() != artist_album.lower()
        ):
            _append("artist_album", softened_artist_album, "softened")
        if len(candidates) < primary_limit and primary_artist_album:
            softened_primary_album = soften_query_text(primary_artist_album)
            if softened_primary_album.lower() != primary_artist_album.lower():
                _append("primary_artist_album", softened_primary_album, "softened")
    else:
        _append("artist_only", artist, "literal")

    if alternate_artist and len(candidates) < max_query_attempts:
        if track:
            _append_alternate(
                "alternate_artist_track",
                _compact_spaces(f"{alternate_artist} {track}"),
                "literal",
                alternate_artist,
            )
        elif album:
            _append_alternate(
                "alternate_artist_album",
                _compact_spaces(f"{alternate_artist} {album}"),
                "literal",
                alternate_artist,
            )

    return candidates
