#!/usr/bin/env python3
"""
Queue tracks from a pipeline CSV via the SLSKD HTTP API.

Input: wide ``data/to_queue.csv`` from ``merge_queue.py``.
Docs: ``README.md``, ``docs/DEV_OPS.md`` (golden path vs advanced flags).
"""

import argparse
import asyncio
import logging
import sys
from typing import Any, Dict, List, Optional

from slskd_logging import DEFAULT_OUTPUT_DIR, logger, setup_logging
from slskd_csv import (
    checkpoint_resume_row,
    load_tracker_from_newest_import_log,
    find_matching_results_csv,
    find_most_recent_results_csv,
    generate_report,
    generate_report_on_demand,
    load_checkpoint,
    load_results_log_from_csv,
    rebuild_tracker_from_import_log,
)
from slskd_search import configure_search_context
from slskd_queue import configure_queue_context
from slskd_worker import Stats, configure_worker_context, process_csv, reconcile_downloads_only
from slskd_config import (
    HOST,
    API_PATH,
    API_KEY,
    CSV_FILE,
    QUEUE_LIMIT,
    RATE_LIMIT_DELAY,
    BATCH_SIZE,
    MAX_RETRIES,
    SEARCH_TIMEOUT,
    ENQUEUE_TIMEOUT,
    CHECKPOINT_FILE,
    DEFAULT_DOWNLOAD_SETTLE_SECONDS,
    EXCLUDED_EXTENSIONS,
    CIRCUIT_BREAKER_THRESHOLD,
    CIRCUIT_BREAKER_TIMEOUT,
    ALLOWED_FORMATS,
    POLL_INTERVAL,
    MAX_POLLS,
    make_headers,
)


# ======== Logging ========
# log_dir is set in main() from setup_logging(); use DEFAULT_OUTPUT_DIR for parser default
log_dir = DEFAULT_OUTPUT_DIR

# ======== Configuration ========
# Defaults are imported from slskd_config and may be overridden by CLI args.

HEADERS = make_headers(API_KEY)


stats = Stats()
results_log = []  # To track detailed results for reporting
queued_files_tracker = []  # Global list to track all queued files for status checking

_GOLDEN_EPILOG = """
Golden path (from repo root):
  python3 slskd_spotify.py --trim-queue
  python3 slskd_spotify.py --resume

See docs/DEV_OPS.md for merge, trim, pending CSV, and legacy flags.
"""


def _build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Queue tracks from a merge_queue CSV via SLSKD",
        epilog=_GOLDEN_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    golden = parser.add_argument_group("golden path")
    golden.add_argument(
        "--csv",
        "-c",
        default=CSV_FILE,
        help=f"Queue CSV (default: {CSV_FILE})",
    )
    golden.add_argument(
        "--resume",
        "-r",
        action="store_true",
        help="Resume from checkpoint (default: data/checkpoint.pkl)",
    )
    golden.add_argument(
        "--trim-queue",
        action="store_true",
        help="After run: rewrite input CSV minus success ledger (see trim_queue.py)",
    )
    golden.add_argument(
        "--checkpoint-file",
        default=CHECKPOINT_FILE,
        help=f"Checkpoint pickle (default: {CHECKPOINT_FILE})",
    )
    golden.add_argument(
        "--skip-pending-csv",
        action="store_true",
        help="Do not write <input>_pending.csv (e.g. validate runs)",
    )
    golden.add_argument(
        "--pending-csv",
        metavar="PATH",
        default=None,
        help="Pending failures CSV (default: <input-stem>_pending.csv)",
    )

    tuning = parser.add_argument_group("tuning")
    tuning.add_argument(
        "--batch-size",
        "-b",
        type=int,
        default=BATCH_SIZE,
        help=f"Rows per batch (default: {BATCH_SIZE})",
    )
    tuning.add_argument(
        "--delay",
        "-d",
        type=float,
        default=RATE_LIMIT_DELAY,
        help=f"Delay between API calls in seconds (default: {RATE_LIMIT_DELAY})",
    )
    tuning.add_argument(
        "--formats",
        "-f",
        nargs="+",
        default=ALLOWED_FORMATS,
        help=f"Allowed formats in priority order (default: {ALLOWED_FORMATS})",
    )
    tuning.add_argument(
        "--exclude",
        "-e",
        nargs="+",
        default=EXCLUDED_EXTENSIONS,
        help=f"Excluded extensions (default: {EXCLUDED_EXTENSIONS})",
    )
    tuning.add_argument(
        "--output-dir",
        "-o",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Logs and results directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    tuning.add_argument("--debug", action="store_true", help="Debug logging")
    tuning.add_argument(
        "--queue-limit",
        type=int,
        default=QUEUE_LIMIT,
        help=f"Max queue items per user, 0=unlimited (default: {QUEUE_LIMIT})",
    )
    tuning.add_argument(
        "--download-settle-seconds",
        type=float,
        default=DEFAULT_DOWNLOAD_SETTLE_SECONDS,
        help=(
            "Wait after queue processing before download reconciliation "
            f"(default: {DEFAULT_DOWNLOAD_SETTLE_SECONDS})"
        ),
    )
    recovery = parser.add_argument_group("recovery (past runs)")
    recovery.add_argument(
        "--reconcile-downloads",
        action="store_true",
        help="Re-poll SLSKD for download status only (no new searches)",
    )
    recovery.add_argument(
        "--reconcile-from-csv",
        metavar="PATH",
        default=None,
        help="Results CSV for --reconcile-downloads (default: newest in output dir)",
    )
    recovery.add_argument(
        "--reconcile-log",
        metavar="PATH",
        default=None,
        help="Import log for --reconcile-downloads (default: newest slskd_import_*.log)",
    )
    recovery.add_argument(
        "--gen-report",
        action="store_true",
        help="Regenerate results CSV from checkpoint (no new processing)",
    )
    recovery.add_argument(
        "--retry-failed",
        "-rf",
        action="store_true",
        help="Retry rows that failed in the newest results CSV",
    )

    return parser


async def _run_reconcile_downloads_mode(args: argparse.Namespace) -> None:
    """Load prior run artifacts and re-run download reconciliation only."""
    checkpoint = load_checkpoint(args.checkpoint_file)
    reconcile_results: List[Dict[str, Any]] = []
    reconcile_tracker: List[Dict[str, Any]] = []
    reconcile_stats: Optional[Stats] = None

    if checkpoint and checkpoint.get("results_log"):
        reconcile_results = list(checkpoint["results_log"])
        logger.info("Using %d result row(s) from checkpoint", len(reconcile_results))
        if checkpoint.get("queued_files_tracker"):
            reconcile_tracker = list(checkpoint["queued_files_tracker"])
            logger.info("Using %d tracked file(s) from checkpoint", len(reconcile_tracker))
        if checkpoint.get("stats"):
            reconcile_stats = Stats()
            reconcile_stats.total_processed = checkpoint["stats"].get("total_processed", 0)
            reconcile_stats.successful = checkpoint["stats"].get("successful", 0)
            reconcile_stats.failed = checkpoint["stats"].get("failed", 0)
            reconcile_stats.enqueued_files = checkpoint["stats"].get("enqueued_files", 0)

    if args.reconcile_from_csv:
        reconcile_results = load_results_log_from_csv(args.reconcile_from_csv)
    else:
        matched_csv = find_matching_results_csv(log_dir, CSV_FILE)
        if matched_csv:
            reconcile_results = load_results_log_from_csv(matched_csv)
        elif not reconcile_results:
            results_path = find_most_recent_results_csv(log_dir)
            if results_path:
                reconcile_results = load_results_log_from_csv(results_path)

    if args.reconcile_log:
        reconcile_tracker = rebuild_tracker_from_import_log(args.reconcile_log)
    elif not reconcile_tracker:
        reconcile_tracker = load_tracker_from_newest_import_log(log_dir)

    queued_files_tracker.clear()
    queued_files_tracker.extend(reconcile_tracker)

    settle = args.download_settle_seconds
    if args.reconcile_downloads and settle == DEFAULT_DOWNLOAD_SETTLE_SECONDS:
        settle = 0.0
        logger.info("Reconcile-only mode: skipping settle wait (use --download-settle-seconds to wait)")

    await reconcile_downloads_only(
        results_log=reconcile_results,
        queued_files_tracker=queued_files_tracker,
        csv_file=CSV_FILE,
        log_dir=log_dir,
        checkpoint_file=args.checkpoint_file,
        settle_seconds=settle,
        stats=reconcile_stats,
        write_pending_csv=not args.skip_pending_csv,
        pending_csv_path=args.pending_csv,
    )


async def main():
    """Main function with argument parsing."""
    # Declare globals at the beginning of the function
    global CSV_FILE, BATCH_SIZE, RATE_LIMIT_DELAY, stats, results_log, log_dir
    global EXCLUDED_EXTENSIONS, ALLOWED_FORMATS, QUEUE_LIMIT

    parser = _build_argument_parser()
    args = parser.parse_args()

    log_level = logging.DEBUG if args.debug else logging.INFO
    _, log_dir = setup_logging(log_level, args.output_dir)

    CSV_FILE = args.csv
    BATCH_SIZE = args.batch_size
    RATE_LIMIT_DELAY = args.delay
    ALLOWED_FORMATS = args.formats
    EXCLUDED_EXTENSIONS = args.exclude
    QUEUE_LIMIT = args.queue_limit

    configure_search_context(
        host=HOST,
        api_path=API_PATH,
        headers=HEADERS,
        rate_limit_delay=RATE_LIMIT_DELAY,
        max_retries=MAX_RETRIES,
        search_timeout=SEARCH_TIMEOUT,
        poll_interval=POLL_INTERVAL,
        max_polls=MAX_POLLS,
        circuit_breaker_threshold=CIRCUIT_BREAKER_THRESHOLD,
        circuit_breaker_timeout=CIRCUIT_BREAKER_TIMEOUT,
        excluded_extensions=EXCLUDED_EXTENSIONS,
        allowed_formats=ALLOWED_FORMATS,
    )

    configure_queue_context(
        host=HOST,
        api_path=API_PATH,
        api_key=API_KEY,
        headers=HEADERS,
        enqueue_timeout=ENQUEUE_TIMEOUT,
        queue_limit=QUEUE_LIMIT,
        queued_files_tracker=queued_files_tracker,
        stats=stats,
    )

    logger.info(f"Allowed formats (in priority order): {ALLOWED_FORMATS}")
    logger.info(f"Excluded formats: {EXCLUDED_EXTENSIONS}")

    if args.reconcile_downloads:
        await _run_reconcile_downloads_mode(args)
        return

    # Check if user just wants to generate a report
    if args.gen_report:
        checkpoint = load_checkpoint(args.checkpoint_file)
        if checkpoint and 'results_log' in checkpoint:
            results_log = checkpoint['results_log']
            logger.info(f"Loaded {len(results_log)} entries from checkpoint for report generation")
            generate_report_on_demand(CSV_FILE, results_log, log_dir)
            return
        else:
            logger.error("No checkpoint found to generate report from")
            return
    
    # Process with or without resuming
    start_row = 0
    if args.resume and not args.retry_failed:
        checkpoint = load_checkpoint(args.checkpoint_file)
        if checkpoint:
            start_row = checkpoint_resume_row(checkpoint)
            stats.total_processed = checkpoint['stats']['total_processed']
            stats.successful = checkpoint['stats']['successful']
            stats.failed = checkpoint['stats']['failed']
            stats.enqueued_files = checkpoint['stats']['enqueued_files']
            results_log = checkpoint['results_log']
            if checkpoint.get("queued_files_tracker"):
                queued_files_tracker.clear()
                queued_files_tracker.extend(checkpoint["queued_files_tracker"])

    configure_worker_context(
        results_log=results_log,
        stats=stats,
        log_dir=log_dir,
        csv_file=CSV_FILE,
        batch_size=BATCH_SIZE,
        checkpoint_file=args.checkpoint_file,
        download_settle_seconds=args.download_settle_seconds,
        write_pending_csv=not args.skip_pending_csv,
        pending_csv_path=args.pending_csv,
        trim_queue_after_run=args.trim_queue,
    )

    try:
        await process_csv(CSV_FILE, start_row, args.retry_failed)
    finally:
        # Always try to generate the report, even if an exception occurs
        if results_log:
            logger.info("Generating final report...")
            generate_report(CSV_FILE, results_log, log_dir)

if __name__ == '__main__':
    # Run the async main function
    asyncio.run(main())
