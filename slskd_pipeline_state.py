"""
Cross-run pipeline state: merge watermarks and success ledger (Track C).

Files live in the pipeline workspace (gitignored), e.g. ``data/``:
``merge_state.json``, ``success_ledger.csv``.
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from slskd_csv import atomic_write_pipeline_csv, decode_pipeline_text
from slskd_normalize import primary_artist_for_merge

MERGE_STATE_FILENAME = "merge_state.json"
SUCCESS_LEDGER_FILENAME = "success_ledger.csv"
UNKNOWN_PLAYLIST_KEY = "_unknown"

SUCCESS_LEDGER_COLUMNS: Tuple[str, ...] = (
    "spotify_track_id",
    "artist",
    "artist_primary",
    "album",
    "track",
    "completed_at",
    "playlist_id",
)

DedupeKey = Tuple[str, ...]


def ledger_identity_artist(row: Dict[str, str]) -> str:
    """Artist token for dedupe / ledger keys (NORM-06: ``artist_primary`` when set)."""
    return (
        (row.get("artist_primary") or "").strip()
        or primary_artist_for_merge((row.get("artist") or "").strip())
    )


def normalize_ledger_row(row: Dict[str, str]) -> Dict[str, str]:
    """Ensure ledger CSV shape including ``artist_primary`` (backfill for legacy rows)."""
    artist = (row.get("artist") or "").strip()
    primary = (row.get("artist_primary") or "").strip() or primary_artist_for_merge(artist)
    return {
        "spotify_track_id": (row.get("spotify_track_id") or "").strip(),
        "artist": artist,
        "artist_primary": primary,
        "album": (row.get("album") or "").strip(),
        "track": (row.get("track") or "").strip(),
        "completed_at": (row.get("completed_at") or "").strip(),
        "playlist_id": (row.get("playlist_id") or "").strip(),
    }


def export_row_eligible_for_merge(row: Dict[str, str]) -> bool:
    """
    NORM-05 — whether an export row may enter the merge queue.

    Episodes are omitted at Spotify fetch (``spotify_playlist_fetch``). All remaining
    music rows are eligible regardless of ``is_unavailable`` or playability.
    """
    return True


def pipeline_row_dedupe_key(row: Dict[str, str]) -> DedupeKey:
    """Hybrid identity: Spotify track id when present, else (artist_primary, album, track)."""
    track_id = (row.get("spotify_track_id") or "").strip()
    if track_id:
        return ("id", track_id.lower())
    artist = ledger_identity_artist(row).lower()
    album = (row.get("album") or "").strip().lower()
    track = (row.get("track") or "").strip().lower()
    return ("triple", artist, album, track)


def parse_spotify_timestamp(raw: str) -> Optional[datetime]:
    """Parse Spotify ``added_at`` (ISO-8601, often Zulu)."""
    text = (raw or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def format_spotify_timestamp(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def merge_state_path(workspace: Path) -> Path:
    return workspace / MERGE_STATE_FILENAME


def success_ledger_path(workspace: Path) -> Path:
    return workspace / SUCCESS_LEDGER_FILENAME


def load_merge_state(workspace: Path) -> Dict[str, Any]:
    path = merge_state_path(workspace)
    if not path.is_file():
        return {"playlists": {}}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {"playlists": {}}
    if not isinstance(data, dict):
        return {"playlists": {}}
    data.setdefault("playlists", {})
    return data


def save_merge_state(workspace: Path, state: Dict[str, Any]) -> None:
    path = merge_state_path(workspace)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, sort_keys=True)
        f.write("\n")
        f.flush()
    tmp.replace(path)


def playlist_watermark(state: Dict[str, Any], playlist_id: str) -> Optional[datetime]:
    playlists = state.get("playlists") or {}
    entry = playlists.get(playlist_id or UNKNOWN_PLAYLIST_KEY) or {}
    return parse_spotify_timestamp(str(entry.get("last_added_at") or ""))


def row_passes_watermark(
    row: Dict[str, str],
    state: Dict[str, Any],
    *,
    force_full_import: bool,
) -> bool:
    if force_full_import:
        return True
    playlist_id = (row.get("playlist_id") or "").strip() or UNKNOWN_PLAYLIST_KEY
    added = parse_spotify_timestamp(row.get("added_at") or "")
    if added is None:
        return True
    last = playlist_watermark(state, playlist_id)
    if last is None:
        return True
    return added > last


def advance_watermarks_from_export(
    state: Dict[str, Any],
    export_rows: List[Dict[str, str]],
    *,
    merge_date: str,
) -> None:
    """Advance per-playlist watermark to max ``added_at`` seen in this export file."""
    playlists: Dict[str, Any] = state.setdefault("playlists", {})
    max_by_playlist: Dict[str, datetime] = {}

    for row in export_rows:
        playlist_id = (row.get("playlist_id") or "").strip() or UNKNOWN_PLAYLIST_KEY
        added = parse_spotify_timestamp(row.get("added_at") or "")
        if added is None:
            continue
        current = max_by_playlist.get(playlist_id)
        if current is None or added > current:
            max_by_playlist[playlist_id] = added

    for playlist_id, added in max_by_playlist.items():
        playlists[playlist_id] = {
            "last_added_at": format_spotify_timestamp(added),
            "last_merge_date": merge_date,
        }


@dataclass
class ExportFilterStats:
    skipped_watermark: int = 0
    skipped_ledger: int = 0
    export_rows_in: int = 0
    export_rows_kept: int = 0


def filter_export_for_merge(
    export_rows: List[Dict[str, str]],
    ledger_keys: Set[DedupeKey],
    merge_state: Dict[str, Any],
    *,
    force_full_import: bool,
) -> Tuple[List[Dict[str, str]], ExportFilterStats]:
    stats = ExportFilterStats(export_rows_in=len(export_rows))
    kept: List[Dict[str, str]] = []

    for row in export_rows:
        if not export_row_eligible_for_merge(row):
            continue
        if not row_passes_watermark(row, merge_state, force_full_import=force_full_import):
            stats.skipped_watermark += 1
            continue
        if pipeline_row_dedupe_key(row) in ledger_keys:
            stats.skipped_ledger += 1
            continue
        kept.append(row)
        stats.export_rows_kept += 1

    return kept, stats


@dataclass
class TrimQueueStats:
    """Counts from trimming ``to_queue.csv`` against the success ledger."""

    rows_in: int = 0
    skipped_ledger: int = 0
    skipped_duplicate: int = 0
    rows_out: int = 0
    ledger_key_count: int = 0


def trim_queue_rows(
    rows: List[Dict[str, str]],
    ledger_keys: Set[DedupeKey],
) -> Tuple[List[Dict[str, str]], TrimQueueStats]:
    """
    Drop rows whose dedupe key is in the success ledger; dedupe remaining (first wins).

    Expects queue-shaped rows (same keys as ``to_queue.csv``).
    """
    stats = TrimQueueStats(rows_in=len(rows), ledger_key_count=len(ledger_keys))
    seen: Set[DedupeKey] = set()
    kept: List[Dict[str, str]] = []

    for row in rows:
        key = pipeline_row_dedupe_key(row)
        if key in ledger_keys:
            stats.skipped_ledger += 1
            continue
        if key in seen:
            stats.skipped_duplicate += 1
            continue
        seen.add(key)
        kept.append(row)

    stats.rows_out = len(kept)
    return kept, stats


def load_ledger_keys(workspace: Path) -> Set[DedupeKey]:
    path = success_ledger_path(workspace)
    if not path.is_file():
        return set()
    keys: Set[DedupeKey] = set()
    try:
        text = decode_pipeline_text(path.read_bytes())
        for row in csv.DictReader(StringIO(text)):
            keys.add(pipeline_row_dedupe_key(normalize_ledger_row(row)))
    except (OSError, UnicodeDecodeError):
        return set()
    return keys


def append_success_ledger(
    workspace: Path,
    rows: List[Dict[str, str]],
) -> int:
    """Append ledger rows (already sanitized keys). Returns count appended."""
    if not rows:
        return 0

    path = success_ledger_path(workspace)
    existing_keys = load_ledger_keys(workspace)
    to_write: List[Dict[str, str]] = []

    for row in rows:
        key = pipeline_row_dedupe_key(row)
        if key in existing_keys:
            continue
        existing_keys.add(key)
        to_write.append(
            normalize_ledger_row(
                {
                    "spotify_track_id": row.get("spotify_track_id", ""),
                    "artist": row.get("artist", ""),
                    "artist_primary": row.get("artist_primary", ""),
                    "album": row.get("album", ""),
                    "track": row.get("track", ""),
                    "completed_at": row.get("completed_at", ""),
                    "playlist_id": row.get("playlist_id", ""),
                }
            )
        )

    if not to_write:
        return 0

    if path.is_file():
        text = decode_pipeline_text(path.read_bytes())
        prior = [normalize_ledger_row(row) for row in csv.DictReader(StringIO(text))]
        combined = prior + to_write
    else:
        combined = to_write

    atomic_write_pipeline_csv(path, combined, fieldnames=SUCCESS_LEDGER_COLUMNS)
    return len(to_write)


def success_rows_from_results_log(
    results_log: List[Dict[str, Any]],
) -> List[Dict[str, str]]:
    """Build ledger rows from reconciled ``success`` results."""
    out: List[Dict[str, str]] = []
    now = format_spotify_timestamp(datetime.now(timezone.utc))
    for entry in results_log:
        if (entry.get("status") or "").lower() != "success":
            continue
        artist = str(entry.get("artist") or "")
        out.append(
            normalize_ledger_row(
                {
                    "spotify_track_id": str(entry.get("spotify_track_id") or ""),
                    "artist": artist,
                    "artist_primary": str(entry.get("artist_primary") or ""),
                    "album": str(entry.get("album") or ""),
                    "track": str(entry.get("track") or ""),
                    "completed_at": str(entry.get("completed_at") or now),
                    "playlist_id": str(entry.get("playlist_id") or ""),
                }
            )
        )
    return out


def record_successful_downloads(
    workspace: Path,
    results_log: List[Dict[str, Any]],
) -> int:
    rows = success_rows_from_results_log(results_log)
    return append_success_ledger(workspace, rows)
