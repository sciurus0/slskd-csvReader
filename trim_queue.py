#!/usr/bin/env python3
"""
Trim ``to_queue.csv``: remove success-ledger rows and dedupe (POLISH-01).

Re-reads the queue with the same NORM + sanitize path as ``merge_queue.py``, then
rewrites ``to_queue.csv`` without keys present in ``success_ledger.csv``.

Usage::

    python3 trim_queue.py --dry-run
    python3 trim_queue.py
    python3 trim_queue.py --workspace data
    python3 slskd_spotify.py --csv data/to_queue.csv --trim-queue
"""

from __future__ import annotations

import argparse
import shutil
import sys
from datetime import datetime
from pathlib import Path

from merge_queue import QUEUE_CSV_COLUMNS, load_queue_csv
from slskd_workspace import archive_csv_path, queue_csv_path, resolve_workspace
from slskd_csv import atomic_write_pipeline_csv
from slskd_pipeline_state import TrimQueueStats, load_ledger_keys, trim_queue_rows


def _die(msg: str, code: int = 1) -> None:
    print(msg, file=sys.stderr)
    raise SystemExit(code)


def trim_queue_workspace(
    workspace: Path,
    *,
    dry_run: bool = False,
    backup: bool = True,
) -> TrimQueueStats:
    """
    Trim ``workspace/to_queue.csv`` against ``success_ledger.csv``.

    When ``backup`` is true and not dry-run, copies the queue to
    ``archive/csv-YYYYMMDD/<timestamp>-to_queue-pre-trim.csv`` first.
    """
    workspace = workspace.resolve()
    queue_path = queue_csv_path(workspace)
    if not queue_path.is_file():
        _die(f"Queue not found: {queue_path}")

    incoming = load_queue_csv(queue_path)
    ledger_keys = load_ledger_keys(workspace)
    kept, stats = trim_queue_rows(incoming, ledger_keys)

    print(
        f"trim_queue: in={stats.rows_in} skipped_ledger={stats.skipped_ledger} "
        f"skipped_dupe={stats.skipped_duplicate} out={stats.rows_out} "
        f"ledger_keys={stats.ledger_key_count}",
        file=sys.stderr,
    )

    if dry_run:
        print("Dry-run: not writing files.", file=sys.stderr)
        return stats

    if backup:
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_path = archive_csv_path(
            workspace, stamp[:8], f"{stamp}-to_queue-pre-trim.csv"
        )
        shutil.copy2(queue_path, backup_path)
        print(f"Archived pre-trim backup: {backup_path}", file=sys.stderr)

    atomic_write_pipeline_csv(queue_path, kept, fieldnames=QUEUE_CSV_COLUMNS)
    print(f"Wrote {stats.rows_out} rows to {queue_path}", file=sys.stderr)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Trim to_queue.csv (drop success_ledger keys, dedupe)."
    )
    parser.add_argument(
        "--workspace",
        type=Path,
        default=None,
        help="Pipeline data directory (default: ./data/, or cwd if legacy flat layout)",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Do not copy queue to <timestamp>-to_queue-pre-trim.csv first",
    )
    args = parser.parse_args()
    workspace = resolve_workspace(args.workspace)
    trim_queue_workspace(workspace, dry_run=args.dry_run, backup=not args.no_backup)


if __name__ == "__main__":
    main()
