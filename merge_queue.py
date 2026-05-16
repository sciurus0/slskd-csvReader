#!/usr/bin/env python3
"""
Merge today's Spotify export into the Soulseek queue CSV.

Steps (local calendar date ``YYYYMMDD``):

1. Require ``YYYYMMDD-spotify-export.csv`` (fail if missing; never wipe the queue).
2. If ``to_queue.csv`` exists, copy it to ``YYYYMMDD-to_queue.csv`` (overwrite OK).
3. Load backup rows (from ``YYYYMMDD-to_queue.csv`` if that file exists from step 2)
   then append rows from the Spotify export. Order: existing queue first, new imports last.
4. Sanitize ``artist`` / ``album`` / ``track`` with ``slskd_sanitize.sanitize_queue_field``
   and write ``to_queue.csv`` as UTF-8 with BOM (atomic replace).

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
from typing import Dict, Iterable, List

from slskd_csv import atomic_write_pipeline_csv, decode_pipeline_text
from slskd_sanitize import sanitize_queue_field

QUEUE_FILENAME = "to_queue.csv"


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
        out.append({"artist": artist, "album": album, "track": track})
    return out


def _sanitize_rows(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for row in rows:
        out.append(
            {
                "artist": sanitize_queue_field(row["artist"]),
                "album": sanitize_queue_field(row["album"]),
                "track": sanitize_queue_field(row["track"]),
            }
        )
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Merge YYYYMMDD-spotify-export.csv into to_queue.csv (sanitized, UTF-8 BOM)."
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
    combined = rows_backup + rows_spotify
    sanitized = _sanitize_rows(combined)

    print(
        f"merge_queue: date={date_str} backup_rows={len(rows_backup)} "
        f"spotify_rows={len(rows_spotify)} combined={len(combined)} "
        f"after_skip={len(sanitized)}",
        file=sys.stderr,
    )

    if args.dry_run:
        print("Dry-run: not writing files.", file=sys.stderr)
        return

    atomic_write_pipeline_csv(queue_path, sanitized)
    print(f"Wrote {len(sanitized)} rows to {queue_path}")


if __name__ == "__main__":
    main()
