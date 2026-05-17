#!/usr/bin/env python3
"""
Backfill ``data/success_ledger.csv`` with ``artist_primary`` (POLISH-06).

Legacy ledgers (pre-NORM-06) gain the column without a slskd run. New appends also
normalize rows, but this rewrites the full file in one shot.

Usage::

    python3 scripts/backfill_ledger.py --dry-run
    python3 scripts/backfill_ledger.py --workspace data
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from slskd_pipeline_state import backfill_success_ledger
from slskd_workspace import ensure_workspace_layout, resolve_workspace


def _die(msg: str, code: int = 1) -> None:
    print(msg, file=sys.stderr)
    raise SystemExit(code)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rewrite success_ledger.csv with artist_primary (POLISH-06)."
    )
    parser.add_argument(
        "--workspace",
        type=Path,
        default=None,
        help="Pipeline data directory (default: ./data/)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report counts only; do not write",
    )
    args = parser.parse_args()

    workspace = resolve_workspace(args.workspace)
    ensure_workspace_layout(workspace)

    rows_in, rows_out = backfill_success_ledger(workspace, dry_run=args.dry_run)
    if rows_in == 0:
        print("No success_ledger.csv to backfill.", file=sys.stderr)
        return

    dupes = rows_in - rows_out
    print(
        f"success_ledger: {rows_in} row(s) in → {rows_out} after normalize"
        + (f" ({dupes} duplicate key(s) dropped)" if dupes else ""),
        file=sys.stderr,
    )
    if args.dry_run:
        print("Dry-run: not writing.", file=sys.stderr)
    else:
        print(f"Wrote {rows_out} row(s) to {workspace / 'success_ledger.csv'}", file=sys.stderr)


if __name__ == "__main__":
    main()
