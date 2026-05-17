"""
Search and ranking utilities for the slskd_spotify workflow.
"""

import asyncio
import concurrent.futures
import os
import re
import time
from asyncio import TimeoutError
from typing import Any, Dict, List, Optional, Tuple

import requests

from slskd_config import (
    ALBUM_PREFERRED_SEARCH,
    ALLOWED_FORMATS,
    API_PATH,
    CIRCUIT_BREAKER_THRESHOLD,
    CIRCUIT_BREAKER_TIMEOUT,
    EXACT_MATCH,
    EXCLUDED_EXTENSIONS,
    HOST,
    MAX_POLLS,
    MAX_RETRIES,
    POLL_INTERVAL,
    RATE_LIMIT_DELAY,
    SEARCH_TIMEOUT,
    initial_circuit_breaker_state,
    make_headers,
    API_KEY,
)
from slskd_logging import logger
from slskd_normalize import normalized_contains, path_matches_row

_host = HOST
_api_path = API_PATH
_headers = make_headers(API_KEY)
_rate_limit_delay = RATE_LIMIT_DELAY
_max_retries = MAX_RETRIES
_search_timeout = SEARCH_TIMEOUT
_poll_interval = POLL_INTERVAL
_max_polls = MAX_POLLS
_circuit_breaker_threshold = CIRCUIT_BREAKER_THRESHOLD
_circuit_breaker_timeout = CIRCUIT_BREAKER_TIMEOUT
_album_preferred_search = ALBUM_PREFERRED_SEARCH
_exact_match = EXACT_MATCH
_excluded_extensions = list(EXCLUDED_EXTENSIONS)
_allowed_formats = list(ALLOWED_FORMATS)
_circuit_breaker_state = initial_circuit_breaker_state()


def configure_search_context(
    *,
    host: str,
    api_path: str,
    headers: Dict[str, str],
    rate_limit_delay: float,
    max_retries: int,
    search_timeout: int,
    poll_interval: int,
    max_polls: int,
    circuit_breaker_threshold: int,
    circuit_breaker_timeout: int,
    album_preferred_search: bool,
    exact_match: bool,
    excluded_extensions: List[str],
    allowed_formats: List[str],
) -> None:
    """Configure runtime values used by search/ranking logic."""
    global _host, _api_path, _headers, _rate_limit_delay, _max_retries, _search_timeout
    global _poll_interval, _max_polls, _circuit_breaker_threshold, _circuit_breaker_timeout
    global _album_preferred_search, _exact_match, _excluded_extensions, _allowed_formats

    _host = host
    _api_path = api_path
    _headers = headers
    _rate_limit_delay = rate_limit_delay
    _max_retries = max_retries
    _search_timeout = search_timeout
    _poll_interval = poll_interval
    _max_polls = max_polls
    _circuit_breaker_threshold = circuit_breaker_threshold
    _circuit_breaker_timeout = circuit_breaker_timeout
    _album_preferred_search = album_preferred_search
    _exact_match = exact_match
    _excluded_extensions = excluded_extensions
    _allowed_formats = allowed_formats


def _responses_from_search_payload(data: object) -> List[Dict[str, Any]]:
    if isinstance(data, dict):
        raw = data.get("responses") or data.get("Responses") or []
        return raw if isinstance(raw, list) else []
    if isinstance(data, list):
        return data
    return []


def _search_job_complete(data: Dict[str, Any]) -> bool:
    if data.get("isComplete") is True:
        return True
    state = str(data.get("state") or "")
    return "completed" in state.lower()


async def _fetch_search_responses(job_id: str) -> List[Dict[str, Any]]:
    """Fetch results from the responses endpoint (canonical list when a search finishes)."""
    responses_url = f"{_host.rstrip('/')}{_api_path}/searches/{job_id}/responses"
    with concurrent.futures.ThreadPoolExecutor() as pool:
        resp = await asyncio.get_event_loop().run_in_executor(
            pool, lambda: requests.get(responses_url, headers=_headers)
        )
    resp.raise_for_status()
    return _responses_from_search_payload(resp.json())


async def poll_responses_async(job_id):
    """Poll search job state until complete, then return responses from the responses endpoint."""
    state_url = f"{_host.rstrip('/')}{_api_path}/searches/{job_id}"
    latest_inline: List[Dict[str, Any]] = []
    last_state: Optional[str] = None

    for poll_num in range(_max_polls):
        if poll_num > 0:
            await asyncio.sleep(_poll_interval)

        try:
            with concurrent.futures.ThreadPoolExecutor() as pool:
                resp = await asyncio.get_event_loop().run_in_executor(
                    pool, lambda: requests.get(state_url, headers=_headers)
                )
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict):
                continue

            last_state = str(data.get("state") or "")
            inline = _responses_from_search_payload(data)
            if inline:
                latest_inline = inline

            if _search_job_complete(data):
                results = await _fetch_search_responses(job_id)
                if not results and latest_inline:
                    results = latest_inline
                response_count = data.get("responseCount") or data.get("ResponseCount")
                logger.info(
                    "Search complete: %d response(s) fetched, state=%r, responseCount=%r",
                    len(results),
                    last_state,
                    response_count,
                )
                return results
        except Exception as e:
            logger.warning(f"Error during polling: {e}")

    if latest_inline:
        logger.info(
            "Search poll limit reached (%d poll(s)); returning %d inline response(s)",
            _max_polls,
            len(latest_inline),
        )
        return latest_inline

    try:
        results = await _fetch_search_responses(job_id)
        if results:
            logger.info(
                "Search poll limit reached (%d poll(s)); fetched %d response(s) from responses endpoint",
                _max_polls,
                len(results),
            )
        return results
    except Exception as e:
        logger.warning(f"Error fetching responses after poll limit: {e}")
        return []


async def search_slskd_async(pattern):
    """Run a search with timeout and retry logic."""
    post_url = f"{_host.rstrip('/')}{_api_path}/searches"
    logger.info(f"⬢ POST {post_url} payload: {{'SearchText': '{pattern}'}}")

    global _circuit_breaker_state
    if _circuit_breaker_state["circuit_open"]:
        current_time = time.time()
        if (
            current_time - _circuit_breaker_state["circuit_open_time"]
            < _circuit_breaker_timeout
        ):
            logger.warning("Circuit breaker open, skipping API call")
            return []
        logger.info("Circuit breaker timeout expired, resetting")
        _circuit_breaker_state["circuit_open"] = False
        _circuit_breaker_state["consecutive_errors"] = 0

    retries = 0
    while retries <= _max_retries:
        try:
            with concurrent.futures.ThreadPoolExecutor() as pool:
                resp = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(
                        pool,
                        lambda: requests.post(
                            post_url, json={"SearchText": pattern}, headers=_headers
                        ),
                    ),
                    timeout=_search_timeout,
                )
            resp.raise_for_status()
            data = resp.json()
            job_id = data.get("id") or data.get("SearchId")
            if not job_id:
                raise RuntimeError(f"No job ID returned by POST: {data}")

            _circuit_breaker_state["consecutive_errors"] = 0
            await asyncio.sleep(_rate_limit_delay)
            responses = await asyncio.wait_for(
                poll_responses_async(job_id), timeout=_search_timeout
            )
            return responses

        except (TimeoutError, requests.exceptions.RequestException, RuntimeError) as e:
            logger.warning(
                f"Search operation error for '{pattern}': {e}. Retry {retries+1}/{_max_retries+1}"
            )
            retries += 1
            _circuit_breaker_state["consecutive_errors"] += 1

            if (
                _circuit_breaker_state["consecutive_errors"]
                >= _circuit_breaker_threshold
            ):
                logger.error(
                    f"Circuit breaker threshold reached after {_circuit_breaker_threshold} consecutive errors"
                )
                _circuit_breaker_state["circuit_open"] = True
                _circuit_breaker_state["circuit_open_time"] = time.time()
                return []

            if retries > _max_retries:
                logger.error(f"Max retries reached for '{pattern}'")
                return []

            await asyncio.sleep(_rate_limit_delay * (2**retries))

    return []


def file_has_excluded_extension(filename, excluded_extensions):
    """Check if a file has one of the excluded extensions."""
    if not filename:
        return False
    return any(filename.lower().endswith(ext.lower()) for ext in excluded_extensions)


def get_priority_group(filename):
    """Return format-priority index for a filename, or 999 if disallowed."""
    if not filename:
        return 999

    if file_has_excluded_extension(filename, _excluded_extensions):
        return 999

    for i, ext in enumerate(_allowed_formats):
        if filename.lower().endswith(ext.lower()):
            return i
    return 999


def is_exact_match(filename, artist, album, track):
    """Check for exact match of artist, album, and track in filename."""
    if not filename:
        return False

    filename_lower = filename.lower()
    artist_lower = artist.lower()
    album_lower = album.lower()
    track_lower = track.lower()
    expected_pattern = f"{artist_lower} - {album_lower} - {track_lower}"
    basename = os.path.basename(filename_lower)
    file_without_ext = os.path.splitext(basename)[0]

    if file_without_ext == expected_pattern:
        return True

    def normalize(s):
        s = s.replace("_", " ").replace(".", " ").replace("-", " ")
        return " ".join(s.split())

    return normalize(file_without_ext) == normalize(expected_pattern)


def score_filename_cleanliness(filename, artist, album, track):
    """Score how well a filename matches expected music naming patterns."""
    if not filename:
        return 0.0
    score = 5.0
    basename = os.path.basename(filename)
    name_without_ext = os.path.splitext(basename)[0]
    name_lower = name_without_ext.lower()
    artist_lower = artist.lower()
    album_lower = album.lower()
    track_lower = track.lower()

    if album_lower and track_lower:
        perfect_pattern = f"{artist_lower} - {album_lower} - {track_lower}"
        if name_lower == perfect_pattern:
            return 10.0
    elif track_lower and not album_lower:
        perfect_pattern = f"{artist_lower} - {track_lower}"
        if name_lower == perfect_pattern:
            return 9.5
    elif album_lower and not track_lower:
        perfect_pattern = f"{artist_lower} - {album_lower}"
        if name_lower == perfect_pattern:
            return 9.0

    track_number_pattern = r"^\d{1,2}[-.\s]+"
    if re.search(track_number_pattern, name_lower):
        score += 0.5

    safe_artist = re.escape(artist_lower)
    safe_album = re.escape(album_lower) if album_lower else ""
    safe_track = re.escape(track_lower) if track_lower else ""

    if album_lower and track_lower:
        patterns = [
            rf"{safe_artist} - {safe_album} - \d+[-.\s]+{safe_track}",
            rf"{safe_artist} - {safe_album} - {safe_track}",
        ]
    elif track_lower and not album_lower:
        patterns = [
            rf"{safe_artist} - \d+[-.\s]+{safe_track}",
            rf"{safe_artist} - {safe_track}",
            rf"\d+[-.\s]+{safe_artist} - {safe_track}",
        ]
    elif album_lower and not track_lower:
        patterns = [rf"{safe_artist} - {safe_album}"]
    else:
        patterns = [rf"{safe_artist}"]

    for pattern in patterns:
        if re.search(pattern, name_lower):
            score += 1.5
            break

    if artist_lower in name_lower:
        score += 1.0
    else:
        score -= 2.0

    if track_lower and normalized_contains(filename, track):
        score += 1.0
    elif track_lower:
        score -= 3.0

    if album_lower and normalized_contains(filename, album):
        score += 0.5

    suspicious_terms = [
        "remix",
        "mix",
        "edit",
        "live",
        "cover",
        "instrumental",
        "acoustic",
        "demo",
        "alternate",
        "dj",
        "radio edit",
        "extended",
        "version",
    ]
    for term in suspicious_terms:
        if term in name_lower:
            score -= 1.5

    if len(name_without_ext) > 60:
        score -= 0.5
    return max(0.0, min(score, 10.0))


def detect_search_intent(artist: str, album: str, track: str) -> Dict[str, bool]:
    """Detect if user is explicitly searching for version variants."""
    search_terms = f"{artist} {album} {track}".lower()
    album_suggests_live = album and any(
        term in album.lower()
        for term in ["live", "unplugged", "concert", "mtv unplugged", "in concert"]
    )
    return {
        "allow_remix": "remix" in search_terms
        or "remixed" in search_terms
        or "re-mix" in search_terms,
        "allow_live": "live" in search_terms or album_suggests_live,
        "allow_cover": "cover" in search_terms,
        "allow_acoustic": "acoustic" in search_terms,
        "allow_instrumental": "instrumental" in search_terms,
        "allow_demo": "demo" in search_terms,
        "allow_karaoke": "karaoke" in search_terms,
    }


def should_reject_version(
    filename: str, track: str, search_intent: Dict[str, bool]
) -> Optional[str]:
    """Check if file contains unwanted version markers."""
    if not _album_preferred_search:
        return None

    basename = os.path.basename(filename).lower()
    track_words = set(track.lower().split()) if track else set()
    suspicious_patterns: Dict[str, Tuple[str, bool]] = {
        "remix": (r"\b(remix|remixed|re-mix)\b", search_intent.get("allow_remix", False)),
        "live": (r"\b(live|concert)\b", search_intent.get("allow_live", False)),
        "cover": (r"\bcover\b", search_intent.get("allow_cover", False)),
        "acoustic": (r"\bacoustic\b", search_intent.get("allow_acoustic", False)),
        "instrumental": (
            r"\binstrumental\b",
            search_intent.get("allow_instrumental", False),
        ),
        "demo": (r"\bdemo\b", search_intent.get("allow_demo", False)),
        "alternate": (r"\b(alternate|alternative|alt\s+version)\b", False),
        "edit": (r"\b(radio\s+edit|extended|club\s+mix|dj\s+mix)\b", False),
        "karaoke": (r"\bkaraoke\b", search_intent.get("allow_karaoke", False)),
        "tribute": (r"\btribute\b", False),
    }

    for pattern_name, (pattern, is_allowed) in suspicious_patterns.items():
        match = re.search(pattern, basename)
        if match:
            matched_word = match.group(0)
            if matched_word in track_words:
                continue
            if not is_allowed:
                return f"unwanted version: {pattern_name}"
    return None


def rank_all_results(
    responses: List[Dict[str, Any]],
    artist: str,
    album: Optional[str] = None,
    track: Optional[str] = None,
    search_intent: Optional[Dict[str, bool]] = None,
    target_duration_ms: Optional[int] = None,
    search_strategy: Optional[str] = None,
):
    """Rank valid results and return (ranked_candidates, rejection_reasons)."""
    if search_intent is None:
        search_intent = {}
    all_candidates = []
    rejection_reasons = []

    def _extract_duration_ms(file_info: Dict[str, Any]) -> Optional[int]:
        # slskd payloads are not fully stable across versions; try common keys.
        for key in ("duration_ms", "durationMs"):
            val = file_info.get(key)
            if val is not None:
                try:
                    return int(float(val))
                except (TypeError, ValueError):
                    pass
        # Some payloads may expose seconds-style durations.
        for key in ("duration", "length"):
            val = file_info.get(key)
            if val is not None:
                try:
                    num = float(val)
                    # Heuristic: small values likely seconds, large values likely ms.
                    return int(num * 1000) if num < 10000 else int(num)
                except (TypeError, ValueError):
                    pass
        return None

    for response in responses:
        username = response.get("username")
        for file_info in response.get("files", []):
            filename = file_info.get("filename", "")
            if file_has_excluded_extension(filename, _excluded_extensions):
                rejection_reasons.append(f"excluded extension: {os.path.splitext(filename)[1]}")
                continue

            priority = get_priority_group(filename)
            if priority >= len(_allowed_formats):
                rejection_reasons.append(f"not allowed format: {os.path.splitext(filename)[1]}")
                continue

            if _album_preferred_search:
                rejection_reason = should_reject_version(filename, track or "", search_intent)
                if rejection_reason:
                    rejection_reasons.append(rejection_reason)
                    continue

            has_album_match = False
            if track and album:
                if _exact_match:
                    match_found = is_exact_match(filename, artist, album, track)
                    has_album_match = bool(
                        album and normalized_contains(filename, album)
                    )
                elif _album_preferred_search:
                    match_found = normalized_contains(filename, track)
                    has_album_match = bool(
                        album and normalized_contains(filename, album)
                    )
                else:
                    match_found, has_album_match = path_matches_row(
                        filename,
                        artist=artist,
                        album=album,
                        track=track,
                        search_strategy=search_strategy,
                    )
            elif track and not album:
                if _exact_match:
                    match_found = is_exact_match(filename, artist, "", track)
                else:
                    match_found, _ = path_matches_row(
                        filename,
                        artist=artist,
                        track=track,
                        search_strategy=search_strategy,
                    )
            elif album and not track:
                match_found, has_album_match = path_matches_row(
                    filename,
                    artist=artist,
                    album=album,
                    search_strategy=search_strategy,
                )
            else:
                match_found = normalized_contains(filename, artist)

            if not match_found:
                rejection_reasons.append("no match for search criteria")
                continue

            cleanliness_score = score_filename_cleanliness(
                filename, artist, album or "", track or ""
            )
            candidate = {
                "username": username,
                "filename": filename,
                "size": file_info.get("size", 0),
                "priority": priority,
                "cleanliness": cleanliness_score,
                "has_album_match": has_album_match,
                "file_id": file_info.get("id") or file_info.get("file_id"),
                "original_response": response,
                "file_info": file_info,
            }
            duration_ms = _extract_duration_ms(file_info)
            candidate["duration_ms"] = duration_ms
            if target_duration_ms is not None and duration_ms is not None:
                candidate["duration_delta_ms"] = abs(duration_ms - target_duration_ms)
            else:
                candidate["duration_delta_ms"] = None
            all_candidates.append(candidate)

    if _album_preferred_search:
        all_candidates.sort(
            key=lambda x: (
                not x.get("has_album_match", False),
                x["priority"],
                -x["cleanliness"],
                -x["size"],
                x["duration_delta_ms"] if x["duration_delta_ms"] is not None else 10**12,
            )
        )
    else:
        all_candidates.sort(
            key=lambda x: (
                x["priority"],
                -x["cleanliness"],
                -x["size"],
                x["duration_delta_ms"] if x["duration_delta_ms"] is not None else 10**12,
            )
        )

    if all_candidates:
        logger.info(f"Ranked {len(all_candidates)} valid candidates")
        if _album_preferred_search:
            album_matches = sum(1 for c in all_candidates if c.get("has_album_match", False))
            logger.info(
                f"  {album_matches} with album match, {len(all_candidates) - album_matches} without"
            )
        top_n = min(5, len(all_candidates))
        logger.info(f"Top {top_n} candidates:")
        for i, candidate in enumerate(all_candidates[:top_n], 1):
            format_type = _allowed_formats[candidate["priority"]]
            album_indicator = " [ALBUM MATCH]" if candidate.get("has_album_match", False) else ""
            duration_indicator = ""
            if target_duration_ms is not None and candidate.get("duration_delta_ms") is not None:
                duration_indicator = f", Duration Δ: {candidate['duration_delta_ms']}ms"
            logger.info(
                f"  {i}. {os.path.basename(candidate['filename'])} "
                f"from {candidate['username']} - "
                f"Format: {format_type}, "
                f"Cleanliness: {candidate['cleanliness']:.1f}, "
                f"Size: {candidate['size']:,} bytes{duration_indicator}{album_indicator}"
            )
    else:
        logger.info("No valid candidates found")

    return all_candidates, rejection_reasons

