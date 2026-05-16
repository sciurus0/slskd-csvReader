#!/usr/bin/env python3
"""
Merge today's Spotify export into the Soulseek queue CSV.

Steps (local calendar date ``YYYYMMDD``):

1. Require ``YYYYMMDD-spotify-export.csv`` (fail if missing; never wipe the queue).
2. If ``to_queue.csv`` exists, copy it to ``YYYYMMDD-to_queue.csv`` (overwrite OK).
3. Load backup rows, then append Spotify export rows (existing queue first).
4. Sanitize ``artist`` / ``album`` / ``track``; pass through ``duration_ms``, ``spotify_track_id``,
   ``is_unavailable`` when present.
5. Dedupe with hybrid key: ``spotify_track_id`` when set, else sanitized (artist, album, track).
   Queue rows win; duplicate export rows are skipped.
6. Write wide ``to_queue.csv`` as UTF-8 with BOM (atomic replace).

Pipeline CSVs must be UTF-8 (optional BOM). No transcoding.

Usage::

    python3 merge_queue.py
    python3 merge_queue.py --workspace /path/to/repo
    python3 merge_queue.py --date 20260510 --dry-run
"""

from __future__ import annotations

import argparse
import csv
import shutil
import sys
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple

from slskd_csv import atomic_write_pipeline_csv, decode_pipeline_text
from slskd_sanitize import sanitize_queue_field

QUEUE_FILENAME = "to_queue.csv"

# Wide queue contract (Track A). Extra export columns (disc_number, added_at, …) are not stored yet.
QUEUE_CSV_COLUMNS: Tuple[str, ...] = (
    "artist",
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
    if len(s) != 8 or not s.isdigit():
        _die(f"--date must be YYYYMMDD, got {s!r}")
    try:
        datetime.strptime(s, "%Y%m%d")
    except ValueError:
        _die(f"Invalid calendar date: {s!r}")
    return s


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
    """File hygiene on names; pass through identity and duration fields."""
    return {
        "artist": sanitize_queue_field(row.get("artist", "")),
        "album": sanitize_queue_field(row.get("album", "")),
        "track": sanitize_queue_field(row.get("track", "")),
        "duration_ms": (row.get("duration_ms") or "").strip(),
        "spotify_track_id": (row.get("spotify_track_id") or "").strip(),
        "is_unavailable": _boolish_field(row.get("is_unavailable", "")),
    }


def pipeline_row_dedupe_key(row: Dict[str, str]) -> DedupeKey:
    """
    Hybrid identity for merge dedupe: Spotify track id when present, else sanitized triple.
    """
    track_id = (row.get("spotify_track_id") or "").strip()
    if track_id:
        return ("id", track_id.lower())
    artist = (row.get("artist") or "").strip().lower()
    album = (row.get("album") or "").strip().lower()
    track = (row.get("track") or "").strip().lower()
    return ("triple", artist, album, track)


def _load_rows(path: Path) -> List[Dict[str, str]]:
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
        merged.append(row)

    return merged, skipped


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Merge YYYYMMDD-spotify-export.csv into to_queue.csv (sanitized, deduped, UTF-8 BOM)."
    )
    parser.add_argument(
        "--workspace",
        type=Path,
        default=Path.cwd(),
        help="Directory containing queue and export files (default: current directory)",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        metavar="YYYYMMDD",
        help="Calendar date for filenames (default: today's local date)",
    )
    parser.add_argument(
        "--spotify-export",
        type=Path,
        default=None,
        help="Path to Spotify export CSV (default: <workspace>/<DATE>-spotify-export.csv)",
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
        help="Backup path for existing queue (default: <workspace>/<DATE>-to_queue.csv)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned row counts and exit without writing files",
    )
    args = parser.parse_args()

    workspace = args.workspace.resolve()
    date_str = _parse_date(args.date) if args.date else datetime.now().strftime("%Y%m%d")

    spotify_path = (
        args.spotify_export.resolve()
        if args.spotify_export
        else workspace / f"{date_str}-spotify-export.csv"
    )
    queue_path = args.queue.resolve() if args.queue else workspace / QUEUE_FILENAME
    backup_path = (
        args.backup.resolve() if args.backup else workspace / f"{date_str}-to_queue.csv"
    )

    if not spotify_path.is_file():
        _die(f"Spotify export not found (required): {spotify_path}")

    rows_backup: List[Dict[str, str]] = []
    if queue_path.is_file():
        shutil.copy2(queue_path, backup_path)
        rows_backup = _load_rows(backup_path)

    rows_spotify = _load_rows(spotify_path)
    merged, deduped_spotify = merge_rows_with_dedupe(rows_backup, rows_spotify)

    print(
        f"merge_queue: date={date_str} backup_rows={len(rows_backup)} "
        f"spotify_rows={len(rows_spotify)} deduped_spotify={deduped_spotify} "
        f"written={len(merged)}",
        file=sys.stderr,
    )

    if args.dry_run:
        print("Dry-run: not writing files.", file=sys.stderr)
        return

    atomic_write_pipeline_csv(queue_path, merged, fieldnames=QUEUE_CSV_COLUMNS)
    print(f"Wrote {len(merged)} rows to {queue_path}")


if __name__ == "__main__":
    main()
