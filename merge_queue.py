#!/usr/bin/env python3
"""
Merge the latest Spotify export into the Soulseek queue CSV.

Steps:

1. Require a ``YYYYMMDD-spotify-export.csv`` (default: newest dated file in the workspace).
2. If ``to_queue.csv`` exists, copy it to ``<export-date>-to_queue.csv`` (overwrite OK).
3. Filter export rows by per-playlist ``added_at`` watermarks (``merge_state.json``) and
   ``success_ledger.csv`` unless ``--force-full-import``.
4. Load backup rows, then append filtered Spotify export rows (existing queue first).
5. Sanitize artist, NORM artist credits (``artist_primary`` / ``artist_alternates``), NORM-04 sanitize-only album/track; pass through
   ``duration_ms``, ``spotify_track_id``, ``is_unavailable`` when present.
6. Dedupe with hybrid key: ``spotify_track_id`` when set, else sanitized (artist, album, track).
   Queue rows win; duplicate export rows are skipped.
7. Write wide ``to_queue.csv`` as UTF-8 with BOM (atomic replace); advance watermarks.

Pipeline CSVs must be UTF-8 (optional BOM). No transcoding.

Usage::

    python3 merge_queue.py
    python3 merge_queue.py --workspace data
    python3 merge_queue.py --date 20260510 --dry-run
    python3 merge_queue.py --force-full-import
"""

from __future__ import annotations

import argparse
import csv
import shutil
import sys
from io import StringIO
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple

from slskd_csv import atomic_write_pipeline_csv, decode_pipeline_text
from slskd_pipeline_state import (
    advance_watermarks_from_export,
    filter_export_for_merge,
    load_ledger_keys,
    load_merge_state,
    pipeline_row_dedupe_key,
    save_merge_state,
)
from slskd_export_paths import parse_export_date, resolve_spotify_export
from slskd_normalize import normalize_album_track_n4, normalize_queue_row
from slskd_sanitize import sanitize_queue_field
from slskd_workspace import (
    QUEUE_FILENAME,
    archive_csv_path,
    ensure_workspace_layout,
    queue_csv_path,
    resolve_workspace,
)

# Wide queue contract (Track A). Extra export columns (disc_number, added_at, …) are not stored yet.
QUEUE_CSV_COLUMNS: Tuple[str, ...] = (
    "artist",
    "artist_primary",
    "artist_alternates",
    "album",
    "track",
    "duration_ms",
    "spotify_track_id",
    "is_unavailable",
)

DedupeKey = Tuple[str, ...]


def _die(msg: str, code: int = 1) -> None:
    print(msg, file=sys.stderr)
    raise SystemExit(code)


def _parse_date(s: str) -> str:
    try:
        return parse_export_date(s)
    except ValueError as e:
        _die(f"--date {e}")


def _norm_keys(row: Dict[str, str]) -> Dict[str, str]:
    return {(k or "").strip().lower(): (v or "").strip() for k, v in row.items()}


def _validate_headers(fieldnames: Iterable[str] | None) -> None:
    if not fieldnames:
        _die("CSV has no header row.")
    lower = {(f or "").strip().lower() for f in fieldnames}
    required = {"artist", "album", "track"}
    if not required <= lower:
        _die(f"CSV must have columns {sorted(required)}, got {sorted(lower)}")


def _boolish_field(raw: str) -> str:
    s = (raw or "").strip().lower()
    if s in ("true", "1", "yes"):
        return "true"
    if s in ("false", "0", "no"):
        return "false"
    return s


def _sanitize_pipeline_row(row: Dict[str, str]) -> Dict[str, str]:
    """Sanitize artist, NORM credits (NORM-02/03), NORM-04 album/track; pass through identity fields."""
    prepared = normalize_queue_row(
        {
            "artist": sanitize_queue_field(row.get("artist", "")),
            "album": row.get("album", ""),
            "track": row.get("track", ""),
            "duration_ms": (row.get("duration_ms") or "").strip(),
            "spotify_track_id": (row.get("spotify_track_id") or "").strip(),
            "is_unavailable": row.get("is_unavailable", ""),
        }
    )
    prepared = normalize_album_track_n4(prepared)
    return {
        "artist": prepared["artist"],
        "artist_primary": prepared.get("artist_primary", ""),
        "artist_alternates": prepared.get("artist_alternates", ""),
        "album": prepared.get("album", ""),
        "track": prepared.get("track", ""),
        "duration_ms": prepared.get("duration_ms", ""),
        "spotify_track_id": prepared.get("spotify_track_id", ""),
        "is_unavailable": _boolish_field(prepared.get("is_unavailable", "")),
    }


def _export_row_to_queue_shape(row: Dict[str, str]) -> Dict[str, str]:
    return _sanitize_pipeline_row(
        {
            "artist": row.get("artist", ""),
            "album": row.get("album", ""),
            "track": row.get("track", ""),
            "duration_ms": row.get("duration_ms", ""),
            "spotify_track_id": row.get("spotify_track_id", ""),
            "is_unavailable": row.get("is_unavailable", ""),
        }
    )


def load_queue_csv(path: Path) -> List[Dict[str, str]]:
    """Load wide ``to_queue.csv`` rows with NORM + sanitize (same as merge)."""
    return _load_queue_rows(path)


def _load_queue_rows(path: Path) -> List[Dict[str, str]]:
    text = decode_pipeline_text(Path(path).read_bytes())
    dr = csv.DictReader(StringIO(text))
    raw_rows = list(dr)
    _validate_headers(dr.fieldnames)
    out: List[Dict[str, str]] = []
    for row in raw_rows:
        n = _norm_keys(row)
        artist = n.get("artist", "")
        album = n.get("album", "")
        track = n.get("track", "")
        if not artist and not album and not track:
            continue
        if not artist:
            print(f"Warning: skipping row with missing artist in {path}: {row!r}", file=sys.stderr)
            continue
        loaded = {
            "artist": artist,
            "album": album,
            "track": track,
            "duration_ms": n.get("duration_ms", ""),
            "spotify_track_id": n.get("spotify_track_id", ""),
            "is_unavailable": n.get("is_unavailable", ""),
        }
        out.append(_sanitize_pipeline_row(loaded))
    return out


def _load_export_rows(path: Path) -> List[Dict[str, str]]:
    """Load export CSV rows with ``playlist_id`` and ``added_at`` for merge filtering."""
    text = decode_pipeline_text(Path(path).read_bytes())
    dr = csv.DictReader(StringIO(text))
    raw_rows = list(dr)
    _validate_headers(dr.fieldnames)
    out: List[Dict[str, str]] = []
    for row in raw_rows:
        n = _norm_keys(row)
        artist = n.get("artist", "")
        album = n.get("album", "")
        track = n.get("track", "")
        # NORM-05: not availability — skip empty stubs only (episodes never reach export).
        if not artist and not album and not track:
            continue
        if not artist:
            print(f"Warning: skipping row with missing artist in {path}: {row!r}", file=sys.stderr)
            continue
        queue_row = _export_row_to_queue_shape(
            {
                "artist": artist,
                "album": album,
                "track": track,
                "duration_ms": n.get("duration_ms", ""),
                "spotify_track_id": n.get("spotify_track_id", ""),
                "is_unavailable": n.get("is_unavailable", ""),
            }
        )
        queue_row["playlist_id"] = n.get("playlist_id", "")
        queue_row["added_at"] = n.get("added_at", "")
        out.append(queue_row)
    return out


def merge_rows_with_dedupe(
    existing: List[Dict[str, str]],
    incoming: List[Dict[str, str]],
) -> Tuple[List[Dict[str, str]], int]:
    """
    Keep all existing rows (first wins), then append incoming rows with new dedupe keys.

    Returns (merged_rows, skipped_duplicate_incoming_count).
    """
    seen: Set[DedupeKey] = set()
    merged: List[Dict[str, str]] = []

    for row in existing:
        key = pipeline_row_dedupe_key(row)
        if key in seen:
            continue
        seen.add(key)
        merged.append(row)

    skipped = 0
    for row in incoming:
        key = pipeline_row_dedupe_key(row)
        if key in seen:
            skipped += 1
            continue
        seen.add(key)
        merged.append(_export_row_to_queue_shape(row))

    return merged, skipped


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Merge YYYYMMDD-spotify-export.csv into to_queue.csv (sanitized, deduped, UTF-8 BOM)."
    )
    parser.add_argument(
        "--workspace",
        type=Path,
        default=None,
        help="Pipeline data directory (default: ./data/, or cwd if legacy flat layout)",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        metavar="YYYYMMDD",
        help="Export/backup date prefix (default: date from chosen export, or newest export)",
    )
    parser.add_argument(
        "--spotify-export",
        type=Path,
        default=None,
        help="Path to Spotify export CSV (default: newest *-spotify-export.csv in workspace)",
    )
    parser.add_argument(
        "--queue",
        type=Path,
        default=None,
        help=f"Output queue path (default: <workspace>/{QUEUE_FILENAME})",
    )
    parser.add_argument(
        "--backup",
        type=Path,
        default=None,
        help="Backup path for existing queue (default: archive/csv-<DATE>/<DATE>-to_queue.csv)",
    )
    parser.add_argument(
        "--force-full-import",
        action="store_true",
        help="Ignore per-playlist added_at watermarks (ledger + dedupe still apply)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned row counts and exit without writing files",
    )
    args = parser.parse_args()

    workspace = resolve_workspace(args.workspace)
    ensure_workspace_layout(workspace)

    try:
        spotify_path, date_str = resolve_spotify_export(
            workspace,
            date_str=_parse_date(args.date) if args.date else None,
            export_path=args.spotify_export,
        )
    except FileNotFoundError as e:
        _die(f"Spotify export not found (required): {e}")
    except ValueError as e:
        _die(str(e))
    queue_path = args.queue.resolve() if args.queue else queue_csv_path(workspace)
    backup_path = (
        args.backup.resolve()
        if args.backup
        else archive_csv_path(workspace, date_str, f"{date_str}-to_queue.csv")
    )

    if not spotify_path.is_file():
        _die(f"Spotify export not found (required): {spotify_path}")

    rows_backup: List[Dict[str, str]] = []
    if queue_path.is_file():
        if args.dry_run:
            rows_backup = _load_queue_rows(queue_path)
        else:
            shutil.copy2(queue_path, backup_path)
            rows_backup = _load_queue_rows(backup_path)

    export_rows = _load_export_rows(spotify_path)
    merge_state = load_merge_state(workspace)
    ledger_keys = load_ledger_keys(workspace)
    rows_spotify, filter_stats = filter_export_for_merge(
        export_rows,
        ledger_keys,
        merge_state,
        force_full_import=args.force_full_import,
    )
    merged, deduped_spotify = merge_rows_with_dedupe(rows_backup, rows_spotify)

    print(
        f"merge_queue: date={date_str} export={spotify_path.name} backup_rows={len(rows_backup)} "
        f"export_rows={filter_stats.export_rows_in} "
        f"skipped_watermark={filter_stats.skipped_watermark} "
        f"skipped_ledger={filter_stats.skipped_ledger} "
        f"spotify_rows={filter_stats.export_rows_kept} "
        f"deduped_spotify={deduped_spotify} written={len(merged)}",
        file=sys.stderr,
    )

    if args.dry_run:
        print(
            "Dry-run: no queue backup, merge_state, or to_queue.csv write.",
            file=sys.stderr,
        )
        return

    atomic_write_pipeline_csv(queue_path, merged, fieldnames=QUEUE_CSV_COLUMNS)
    advance_watermarks_from_export(merge_state, export_rows, merge_date=date_str)
    save_merge_state(workspace, merge_state)
    print(f"Wrote {len(merged)} rows to {queue_path}")


if __name__ == "__main__":
    main()
