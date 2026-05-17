"""
CSV batch processing and row workflow for the slskd_spotify script.
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from slskd_csv import (
    read_pipeline_csv_rows,
    generate_report,
    find_most_recent_results_csv,
    load_failed_rows,
    save_checkpoint,
    write_pending_queue_csv,
)
from slskd_logging import logger
from slskd_queue import (
    check_queue_status,
    enqueue_files_async,
    find_best_available_candidate,
    initialize_slskd_client,
    snapshot_queued_files_tracker,
    wait_and_reconcile_downloads,
)
from slskd_normalize import parse_artist_alternates_list
from slskd_query import (
    DEFAULT_MAX_QUERY_ATTEMPTS,
    DEFAULT_MAX_QUERY_ATTEMPTS_WITH_ALTERNATE,
    build_query_candidates,
    enqueue_pick_score,
    first_distinct_alternate,
    is_solid_enqueue_pick,
)
from slskd_pipeline_state import record_successful_downloads
from slskd_search import rank_all_results, search_slskd_async

# Runtime mirrors CLI-overridden settings in the entry script (see configure_worker_context).
_results_log: List[Dict[str, Any]] = []
_stats: Any = None  # Stats
_log_dir: str = ""
_csv_file: str = ""
_batch_size: int = 0
_checkpoint_file: str = ""
_download_settle_seconds: float = 300.0
_write_pending_csv: bool = True
_pending_csv_path: Optional[str] = None
_trim_queue_after_run: bool = False


def configure_worker_context(
    *,
    results_log: List[Dict[str, Any]],
    stats: Any,
    log_dir: str,
    csv_file: str,
    batch_size: int,
    checkpoint_file: str,
    download_settle_seconds: float = 300.0,
    write_pending_csv: bool = True,
    pending_csv_path: Optional[str] = None,
    trim_queue_after_run: bool = False,
) -> None:
    """Bind mutable state and paths used by the CSV workflow engine."""
    global _results_log, _stats, _log_dir, _csv_file, _batch_size
    global _checkpoint_file, _download_settle_seconds
    global _write_pending_csv, _pending_csv_path, _trim_queue_after_run

    _results_log = results_log
    _stats = stats
    _log_dir = log_dir
    _csv_file = csv_file
    _batch_size = batch_size
    _checkpoint_file = checkpoint_file
    _download_settle_seconds = download_settle_seconds
    _write_pending_csv = write_pending_csv
    _pending_csv_path = pending_csv_path
    _trim_queue_after_run = trim_queue_after_run


def _maybe_cleanup_ephemeral_pending_csvs() -> None:
    from slskd_workspace import is_validate_pending_csv, remove_ephemeral_pending_csvs

    if not is_validate_pending_csv(_csv_file):
        return
    workspace = Path(_csv_file).resolve().parent
    removed = remove_ephemeral_pending_csvs(
        workspace,
        include_retry_pending=False,
    )
    for path in removed:
        logger.info("Removed ephemeral pending CSV: %s", path)


def _maybe_write_pending_queue_csv() -> None:
    if not _write_pending_csv or not _results_log:
        return
    write_pending_queue_csv(
        _csv_file,
        _results_log,
        pending_csv=_pending_csv_path,
    )


def _maybe_record_success_ledger() -> None:
    if not _results_log:
        return
    workspace = Path(_csv_file).resolve().parent
    appended = record_successful_downloads(workspace, _results_log)
    if appended:
        logger.info("Appended %d row(s) to success ledger", appended)


def _maybe_trim_queue_from_ledger() -> None:
    if not _trim_queue_after_run:
        return
    from trim_queue import trim_queue_workspace

    workspace = Path(_csv_file).resolve().parent
    stats = trim_queue_workspace(workspace, dry_run=False, backup=True)
    logger.info(
        "Trimmed to_queue.csv: %d in → %d out (ledger=%d, dupes=%d)",
        stats.rows_in,
        stats.rows_out,
        stats.skipped_ledger,
        stats.skipped_duplicate,
    )


async def reconcile_downloads_only(
    *,
    results_log: List[Dict[str, Any]],
    queued_files_tracker: List[Dict[str, Any]],
    csv_file: str,
    log_dir: str,
    checkpoint_file: str,
    settle_seconds: float = 0.0,
    stats: Optional[Any] = None,
    write_pending_csv: bool = True,
    pending_csv_path: Optional[str] = None,
) -> bool:
    """
    Poll SLSKD for download completion and refresh results/report from saved state.

    Returns True if reconciliation ran, False if there was nothing to reconcile.
    """
    if not queued_files_tracker:
        logger.error(
            "No tracked enqueues. Use a checkpoint with queued_files_tracker, "
            "or --reconcile-log pointing at an slskd_import_*.log from the run."
        )
        return False

    if not results_log:
        logger.error("No results to reconcile.")
        return False

    logger.info(
        "Standalone download reconciliation: %d result row(s), %d tracked file(s)",
        len(results_log),
        len(queued_files_tracker),
    )

    await wait_and_reconcile_downloads(
        results_log,
        settle_seconds=settle_seconds,
        prepare_rows=True,
        reverify_success=True,
    )

    if stats is not None:
        stats.recalculate_from_results(results_log)
        logger.info(
            "After reconciliation — successful downloads: %s, not successful: %s",
            stats.successful,
            stats.failed,
        )

    generate_report(csv_file, results_log, log_dir)
    if write_pending_csv:
        write_pending_queue_csv(
            csv_file,
            results_log,
            pending_csv=pending_csv_path,
        )

    workspace = Path(csv_file).resolve().parent
    appended = record_successful_downloads(workspace, results_log)
    from slskd_workspace import is_validate_pending_csv, remove_ephemeral_pending_csvs

    if is_validate_pending_csv(csv_file):
        for path in remove_ephemeral_pending_csvs(workspace, include_retry_pending=False):
            logger.info("Removed ephemeral pending CSV: %s", path)
    if appended:
        logger.info("Appended %d row(s) to success ledger", appended)

    total_rows = len(results_log)
    save_checkpoint(
        checkpoint_file,
        next_row_index=total_rows,
        total_rows=total_rows,
        stats=stats if stats is not None else Stats(),
        results_log=results_log,
        queued_files_tracker=queued_files_tracker,
    )
    return True


class Stats:
    def __init__(self):
        self.total_processed = 0
        self.successful = 0
        self.failed = 0
        self.enqueued_files = 0
        self.start_time = time.time()
        self.last_update_time = time.time()
        self.files_per_minute = 0.0
        self.estimated_completion: Optional[str] = None

    def to_dict(self):
        return {
            "total_processed": self.total_processed,
            "successful": self.successful,
            "failed": self.failed,
            "enqueued_files": self.enqueued_files,
            "success_rate": f"{(self.successful / max(1, self.total_processed)) * 100:.1f}%",
            "elapsed_time": self.format_time(time.time() - self.start_time),
            "files_per_minute": self.files_per_minute,
            "estimated_completion": self.estimated_completion,
        }

    def format_time(self, seconds):
        hours, remainder = divmod(int(seconds), 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def update_metrics(self, total_remaining):
        current_time = time.time()
        elapsed = current_time - self.last_update_time

        if elapsed < 30:
            return

        if self.total_processed > 0 and elapsed > 0:
            processed_since_last = self.total_processed - getattr(self, "last_total_processed", 0)
            self.files_per_minute = (processed_since_last / elapsed) * 60

            if self.files_per_minute > 0 and total_remaining > 0:
                minutes_remaining = total_remaining / self.files_per_minute
                eta_timestamp = current_time + (minutes_remaining * 60)
                self.estimated_completion = datetime.fromtimestamp(eta_timestamp).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )

        self.last_update_time = current_time
        self.last_total_processed = self.total_processed

    def recalculate_from_results(self, results_log: List[Dict[str, Any]]) -> None:
        """Recompute success/fail counts after download reconciliation."""
        terminal = [r for r in results_log if (r.get("status") or "").lower() != "skipped"]
        self.successful = sum(1 for r in terminal if (r.get("status") or "").lower() == "success")
        self.failed = len(terminal) - self.successful

    def print_progress(self, total_remaining):
        self.update_metrics(total_remaining)

        logger.info("\n" + "-" * 50)
        logger.info("PROGRESS REPORT:")
        logger.info(
            f"Processed: {self.total_processed} ({self.successful} successful, {self.failed} failed)"
        )
        logger.info(f"Success rate: {(self.successful / max(1, self.total_processed)) * 100:.1f}%")
        logger.info(f"Files enqueued: {self.enqueued_files}")
        logger.info(f"Elapsed time: {self.format_time(time.time() - self.start_time)}")

        if self.files_per_minute > 0:
            logger.info(f"Processing speed: {self.files_per_minute:.1f} files/minute")

        if self.estimated_completion:
            logger.info(f"Estimated completion: {self.estimated_completion}")

        logger.info("-" * 50)


async def process_row(client, row, row_index, total_rows):
    """Process a single row from the CSV."""
    result_entry = {key: value for key, value in row.items()}

    result_entry.update(
        {
            "status": "skipped",
            "message": "",
            "files_queued": 0,
            "timestamp": datetime.now().isoformat(),
            "attempts": 0,
            "fallback_used": False,
            "search_query_used": "",
            "search_strategy": "",
            "search_variant": "",
            "row_index": row_index,
        }
    )

    # Rows are expected to be sanitized when written by merge_queue.py (to_queue.csv).
    artist_full = (row.get("artist") or "").strip()
    artist = (row.get("artist_primary") or artist_full).strip()
    album = (row.get("album") or "").strip()
    track = (row.get("track") or "").strip()

    if any(ord(c) > 127 for c in artist_full + album + track):
        logger.info(
            f"Unicode in row {row_index}: artist={artist_full!r} album={album!r} track={track!r}"
        )

    if not artist:
        result_entry["message"] = "Missing artist (required)"
        _results_log.append(result_entry)
        logger.warning(f"Skipping row with missing artist: {row}")
        return False

    if not album and not track:
        result_entry["message"] = "Missing both album and track (at least one required)"
        _results_log.append(result_entry)
        logger.warning(f"Skipping row with only artist: {row}")
        return False

    target_duration_ms: Optional[int] = None
    raw_duration = row.get("duration_ms")
    if raw_duration not in (None, ""):
        try:
            target_duration_ms = int(float(raw_duration))
        except (TypeError, ValueError):
            target_duration_ms = None

    alternates = parse_artist_alternates_list(row.get("artist_alternates") or "")
    first_alternate = first_distinct_alternate(artist, alternates)
    max_query_attempts = (
        DEFAULT_MAX_QUERY_ATTEMPTS_WITH_ALTERNATE
        if first_alternate
        else DEFAULT_MAX_QUERY_ATTEMPTS
    )
    query_candidates = build_query_candidates(
        artist=artist,
        album=album,
        track=track,
        artist_alternates=[first_alternate] if first_alternate else None,
        max_query_attempts=max_query_attempts,
    )
    if not query_candidates:
        result_entry["message"] = "No valid search query could be generated"
        _results_log.append(result_entry)
        return False

    try:
        files_queued = 0
        ranked_candidates: List[Dict[str, Any]] = []
        rejection_reasons: List[str] = []
        selected_pick: Optional[Dict[str, Any]] = None
        best_weak_attempt: Optional[Dict[str, Any]] = None

        for attempt_idx, candidate in enumerate(query_candidates, start=1):
            query = str(candidate["query"])
            strategy = str(candidate["strategy"])
            variant = str(candidate["variant"])
            rank_artist = str(candidate.get("search_artist") or artist).strip() or artist
            result_entry["attempts"] = attempt_idx

            logger.info(
                f"🔍 Searching for: {query} ({strategy}/{variant}"
                f"{f', rank_artist={rank_artist!r}' if rank_artist != artist else ''}) "
                f"({row_index}/{total_rows})"
            )
            resp_list = await search_slskd_async(query)
            if not resp_list:
                logger.info(f"No results for query attempt {attempt_idx}: {query}")
                continue

            attempt_ranked, rejection_reasons = rank_all_results(
                resp_list,
                rank_artist,
                album if album else None,
                track if track else None,
                target_duration_ms=target_duration_ms,
                search_strategy=strategy,
            )
            if not attempt_ranked:
                logger.info(f"No valid candidates for query attempt {attempt_idx}: {query}")
                continue

            if track:
                attempt_pick = await find_best_available_candidate(attempt_ranked)
            else:
                attempt_pick = attempt_ranked[0]

            attempt_state = {
                "ranked": attempt_ranked,
                "pick": attempt_pick,
                "query": query,
                "strategy": strategy,
                "variant": variant,
                "attempt_idx": attempt_idx,
            }

            if attempt_pick and is_solid_enqueue_pick(
                attempt_pick,
                album=album,
                track=track,
                search_strategy=strategy,
            ):
                ranked_candidates = attempt_ranked
                selected_pick = attempt_pick
                result_entry["search_query_used"] = query
                result_entry["search_strategy"] = strategy
                result_entry["search_variant"] = variant
                result_entry["fallback_used"] = attempt_idx > 1
                logger.info(
                    "Solid enqueue pick on query attempt %s (%s); skipping remaining queries",
                    attempt_idx,
                    strategy,
                )
                break

            if best_weak_attempt is None or enqueue_pick_score(
                attempt_pick, attempt_ranked
            ) > enqueue_pick_score(
                best_weak_attempt.get("pick"), best_weak_attempt["ranked"]
            ):
                best_weak_attempt = attempt_state

            remaining = len(query_candidates) - attempt_idx
            if remaining:
                logger.info(
                    "No solid enqueue pick on query attempt %s (%s); "
                    "%s more query attempt(s) available",
                    attempt_idx,
                    strategy,
                    remaining,
                )
            else:
                logger.info(
                    "No solid enqueue pick on query attempt %s (%s); "
                    "using best weak match from prior attempts",
                    attempt_idx,
                    strategy,
                )

        if not ranked_candidates and best_weak_attempt:
            ranked_candidates = best_weak_attempt["ranked"]
            selected_pick = best_weak_attempt.get("pick")
            result_entry["search_query_used"] = best_weak_attempt["query"]
            result_entry["search_strategy"] = best_weak_attempt["strategy"]
            result_entry["search_variant"] = best_weak_attempt["variant"]
            result_entry["fallback_used"] = best_weak_attempt["attempt_idx"] > 1
            logger.info(
                "Using best weak match from query attempt %s (%s)",
                best_weak_attempt["attempt_idx"],
                best_weak_attempt["strategy"],
            )

        if not ranked_candidates:
            result_entry["status"] = "failed"
            if rejection_reasons:
                from collections import Counter

                reason_counts = Counter(rejection_reasons)
                reason_summary = "; ".join(
                    f"{count} file(s): {reason}" for reason, count in reason_counts.items()
                )
                result_entry["message"] = f"No valid candidates found: {reason_summary}"
            else:
                result_entry["message"] = "No valid candidates found from generated queries"
            _results_log.append(result_entry)
            logger.info("No valid candidates found across all query attempts")
            return False

        if not track:
            best_username = ranked_candidates[0]["username"]
            album_tracks = [c for c in ranked_candidates if c["username"] == best_username]

            has_open_slot, queue_count = await check_queue_status(best_username)
            if has_open_slot:
                files_to_queue = [t["file_info"] for t in album_tracks]
                files_queued = await enqueue_files_async(
                    client, best_username, files_to_queue, row_index=row_index
                )
            else:
                result_entry["status"] = "failed"
                result_entry["message"] = f"Queue full for user {best_username}"
                _results_log.append(result_entry)
                return False
        else:
            best_candidate = selected_pick
            if not best_candidate:
                best_candidate = await find_best_available_candidate(ranked_candidates)
            if not best_candidate:
                result_entry["status"] = "failed"
                result_entry["message"] = "No candidates with open queue slots found"
                _results_log.append(result_entry)
                logger.info("No candidates with open queue slots for generated queries")
                return False

            files_queued = await enqueue_files_async(
                client,
                best_candidate["username"],
                [best_candidate["file_info"]],
                row_index=row_index,
            )

            if best_candidate:
                result_entry["has_album_match"] = best_candidate.get("has_album_match", False)

        if files_queued > 0:
            result_entry["status"] = "enqueued"
            result_entry["files_queued"] = files_queued
            result_entry["message"] = (
                f"Enqueued {files_queued} file(s); awaiting download reconciliation"
            )

            _results_log.append(result_entry)
            return True

        result_entry["status"] = "failed"
        result_entry["message"] = "Failed to queue file"
        _results_log.append(result_entry)
        return False

    except Exception as e:
        result_entry["status"] = "error"
        result_entry["message"] = f"Error: {str(e)}"
        _results_log.append(result_entry)
        logger.error(f"Error processing generated queries: {e}")
        return False


async def process_csv(csv_file, start_row=0, retry_failed=False):
    """Process the CSV file in batches with error handling and resumability."""
    if not os.path.isfile(csv_file):
        logger.error(f"CSV file not found: {csv_file}")
        sys.exit(1)

    client = initialize_slskd_client()

    rows_to_process: List[Dict[str, Any]] = []

    if retry_failed:
        results_csv = find_most_recent_results_csv(_log_dir)
        if not results_csv:
            logger.error("Cannot find results CSV file for retry-failed mode")
            sys.exit(1)

        failed_rows = load_failed_rows(results_csv)
        if not failed_rows:
            logger.error("No failed rows found to retry")
            sys.exit(1)

        rows_to_process = failed_rows
        total_rows = len(rows_to_process)
        logger.info(f"Retry mode: Processing {total_rows} failed rows from previous run")
    else:
        try:
            all_rows = read_pipeline_csv_rows(csv_file)
        except UnicodeDecodeError as e:
            logger.error(
                "CSV must be UTF-8 (optional BOM). Re-export or run merge_queue.py to rebuild "
                "the queue CSV: %s (%s)",
                csv_file,
                e,
            )
            sys.exit(1)
        except OSError as e:
            logger.error("Could not read CSV file %s: %s", csv_file, e)
            sys.exit(1)
        total_rows = len(all_rows)
        if total_rows == 0:
            logger.error("CSV file has no data rows (UTF-8): %s", csv_file)
            sys.exit(1)

        rows_to_process = all_rows[start_row:]
        logger.info(f"Starting CSV processing: {total_rows} rows total, beginning at row {start_row}")

    try:
        await process_batch(client, rows_to_process, start_row, total_rows)

    except Exception as e:
        logger.error(f"Error processing CSV: {e}")

    finally:
        logger.info("\n" + "=" * 50)
        logger.info("PROCESSING COMPLETE")
        logger.info(f"Total rows processed: {_stats.total_processed}/{len(rows_to_process)}")
        logger.info(
            f"Successful: {_stats.successful} ({(_stats.successful/max(1, _stats.total_processed))*100:.1f}%)"
        )
        logger.info(f"Failed: {_stats.failed}")
        logger.info(f"Total files enqueued: {_stats.enqueued_files}")

        await wait_and_reconcile_downloads(
            _results_log, settle_seconds=_download_settle_seconds
        )
        _stats.recalculate_from_results(_results_log)

        logger.info(
            f"After reconciliation — successful downloads: {_stats.successful}, "
            f"not successful: {_stats.failed}"
        )
        logger.info("=" * 50)

        generate_report(_csv_file, _results_log, _log_dir)
        _maybe_write_pending_queue_csv()
        _maybe_record_success_ledger()
        _maybe_trim_queue_from_ledger()
        _maybe_cleanup_ephemeral_pending_csvs()

        save_checkpoint(
            _checkpoint_file,
            next_row_index=total_rows,
            total_rows=total_rows,
            stats=_stats,
            results_log=_results_log,
            queued_files_tracker=snapshot_queued_files_tracker(),
        )


async def process_batch(client, rows, start_index, total_rows):
    """Process a batch of rows."""
    for batch_start in range(0, len(rows), _batch_size):
        batch_end = min(batch_start + _batch_size, len(rows))
        batch = rows[batch_start:batch_end]

        batch_info = f"rows {batch_start + 1} to {batch_end} of {len(rows)}"
        logger.info(f"Processing batch: {batch_info}")

        batch_start_time = time.time()

        for i, row in enumerate(batch):
            row_index = start_index + batch_start + i
            _stats.total_processed += 1

            success = await process_row(client, row, row_index, total_rows)
            if success:
                _stats.successful += 1
            else:
                _stats.failed += 1

            if row_index % 5 == 0 or row_index == total_rows - 1:
                save_checkpoint(
                    _checkpoint_file,
                    next_row_index=row_index + 1,
                    total_rows=total_rows,
                    stats=_stats,
                    results_log=_results_log,
                    queued_files_tracker=snapshot_queued_files_tracker(),
                )

            if _stats.total_processed % 20 == 0:
                remaining = total_rows - row_index
                _stats.print_progress(remaining)

        batch_time = time.time() - batch_start_time
        avg_time_per_item = batch_time / len(batch) if batch else 0
        logger.info(
            f"Batch summary: {batch_end - batch_start} rows processed in {batch_time:.1f}s "
            f"({avg_time_per_item:.1f}s per item), "
            f"{_stats.successful}/{_stats.total_processed} successful ({_stats.failed} failed)"
        )
