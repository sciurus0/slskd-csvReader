"""
CSV batch processing and row workflow for the slskd-spotify script.
"""

from __future__ import annotations

import csv
import os
import re
import sys
import time
import unicodedata
from datetime import datetime
from typing import Any, Dict, List, Optional

from slskd_config import ALBUM_PREFERRED_SEARCH as _CONFIG_ALBUM_PREF
from slskd_csv import (
    count_csv_rows,
    generate_report,
    find_most_recent_results_csv,
    load_failed_rows,
    save_checkpoint,
)
from slskd_logging import logger
from slskd_queue import (
    check_queue_status,
    enqueue_files_async,
    find_best_available_candidate,
    initialize_slskd_client,
)
from slskd_query import build_query_candidates
from slskd_search import detect_search_intent, rank_all_results, search_slskd_async

# Runtime mirrors CLI-overridden settings in the entry script (see configure_worker_context).
_results_log: List[Dict[str, Any]] = []
_stats: Any = None  # Stats
_log_dir: str = ""
_csv_file: str = ""
_batch_size: int = 0
_checkpoint_file: str = ""
_album_preferred_search: bool = _CONFIG_ALBUM_PREF


def configure_worker_context(
    *,
    results_log: List[Dict[str, Any]],
    stats: Any,
    log_dir: str,
    csv_file: str,
    batch_size: int,
    checkpoint_file: str,
    album_preferred_search: bool,
) -> None:
    """Bind mutable state and paths used by the CSV workflow engine."""
    global _results_log, _stats, _log_dir, _csv_file, _batch_size
    global _checkpoint_file, _album_preferred_search

    _results_log = results_log
    _stats = stats
    _log_dir = log_dir
    _csv_file = csv_file
    _batch_size = batch_size
    _checkpoint_file = checkpoint_file
    _album_preferred_search = album_preferred_search


def sanitize_filename(name: str, replacement: str = " ") -> str:
    """
    Sanitize a filename by replacing forbidden characters with spaces.
    Preserves Unicode characters like accented letters, Japanese, Chinese, etc.
    """
    if not name:
        return ""

    name = unicodedata.normalize("NFKC", name)
    name = re.sub(r"[\x00-\x1f\x7f]", "", name)
    forbidden = r'[\/:*?"<>|]'
    name = re.sub(forbidden, replacement, name)
    name = name.rstrip(" .")
    name = re.sub(r"\s+", " ", name)

    if not name.strip():
        return "unnamed"

    return name.strip()


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
        }
    )

    raw_artist = (row.get("artist") or "").strip()
    raw_album = (row.get("album") or "").strip()
    raw_track = (row.get("track") or "").strip()

    if any(ord(c) > 127 for c in raw_artist + raw_album + raw_track):
        logger.info(f"Found Unicode characters in row {row_index}:")
        logger.info(f"  Raw artist: {repr(raw_artist)}")
        logger.info(f"  Raw album: {repr(raw_album)}")
        logger.info(f"  Raw track: {repr(raw_track)}")

    artist = sanitize_filename(raw_artist)
    album = sanitize_filename(raw_album)
    track = sanitize_filename(raw_track)

    if any(ord(c) > 127 for c in artist + album + track):
        logger.info("After sanitization:")
        logger.info(f"  Artist: {repr(artist)}")
        logger.info(f"  Album: {repr(album)}")
        logger.info(f"  Track: {repr(track)}")

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

    query_candidates = build_query_candidates(artist=artist, album=album, track=track, max_candidates=3)
    if not query_candidates:
        result_entry["message"] = "No valid search query could be generated"
        _results_log.append(result_entry)
        return False

    try:
        files_queued = 0
        search_intent = detect_search_intent(artist, album or "", track or "")
        ranked_candidates = []
        rejection_reasons: List[str] = []

        for attempt_idx, candidate in enumerate(query_candidates, start=1):
            query = str(candidate["query"])
            strategy = str(candidate["strategy"])
            variant = str(candidate["variant"])
            result_entry["attempts"] = attempt_idx

            logger.info(
                f"🔍 Searching for: {query} ({strategy}/{variant}) ({row_index}/{total_rows})"
            )
            resp_list = await search_slskd_async(query)
            if not resp_list:
                logger.info(f"No results for query attempt {attempt_idx}: {query}")
                continue

            ranked_candidates, rejection_reasons = rank_all_results(
                resp_list,
                artist,
                album if album else None,
                track if track else None,
                search_intent,
                target_duration_ms=target_duration_ms,
            )
            if ranked_candidates:
                result_entry["search_query_used"] = query
                result_entry["search_strategy"] = strategy
                result_entry["search_variant"] = variant
                result_entry["fallback_used"] = attempt_idx > 1
                break
            logger.info(f"No valid candidates for query attempt {attempt_idx}: {query}")

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

            if _album_preferred_search:
                result_entry["has_album_match"] = ranked_candidates[0].get("has_album_match", False)

            has_open_slot, queue_count = await check_queue_status(best_username)
            if has_open_slot:
                files_to_queue = [t["file_info"] for t in album_tracks]
                files_queued = await enqueue_files_async(client, best_username, files_to_queue)
            else:
                result_entry["status"] = "failed"
                result_entry["message"] = f"Queue full for user {best_username}"
                _results_log.append(result_entry)
                return False
        else:
            best_candidate = await find_best_available_candidate(ranked_candidates)
            if not best_candidate:
                result_entry["status"] = "failed"
                result_entry["message"] = "No candidates with open queue slots found"
                _results_log.append(result_entry)
                logger.info("No candidates with open queue slots for generated queries")
                return False

            files_queued = await enqueue_files_async(
                client, best_candidate["username"], [best_candidate["file_info"]]
            )

            if _album_preferred_search and best_candidate:
                result_entry["has_album_match"] = best_candidate.get("has_album_match", False)

        if files_queued > 0:
            result_entry["status"] = "success"
            result_entry["files_queued"] = files_queued

            if _album_preferred_search and not result_entry.get("has_album_match", True):
                result_entry[
                    "message"
                ] = f"Successfully queued {files_queued} file(s) [FALLBACK - no album match]"
            else:
                result_entry["message"] = f"Successfully queued {files_queued} file(s)"

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
        total_rows = count_csv_rows(csv_file)
        if total_rows == 0:
            logger.error("CSV file is empty or couldn't be read")
            sys.exit(1)

        if total_rows > 1000:
            logger.info(f"Large file detected ({total_rows} rows). Using streaming mode.")
            encodings = [
                "utf-8",
                "utf-8-sig",
                "latin-1",
                "cp1252",
                "iso-8859-1",
                "utf-16",
                "utf-16-le",
                "utf-16-be",
            ]
            success = False

            for encoding in encodings:
                try:
                    with open(csv_file, newline="", encoding=encoding) as f:
                        reader = csv.DictReader(f)
                        for _ in range(start_row):
                            next(reader, None)

                        batch: List[Dict[str, Any]] = []
                        batch_count = 0
                        for i, row in enumerate(reader, start=start_row):
                            batch.append(row)

                            if len(batch) >= _batch_size or i == total_rows - 1:
                                batch_count += 1
                                logger.info(
                                    f"Processing batch {batch_count}: rows {i - len(batch) + 1} to {i} of {total_rows}"
                                )

                                await process_batch(client, batch, i - len(batch) + 1, total_rows)

                                batch = []

                                save_checkpoint(
                                    _checkpoint_file, i, total_rows, _stats, _results_log
                                )

                    success = True
                    logger.info(f"Successfully processed CSV with {encoding} encoding")
                    break
                except UnicodeDecodeError as e:
                    if "utf-16" in encoding and "BOM" in str(e):
                        logger.debug(f"Skipping {encoding} - file doesn't have BOM")
                    else:
                        logger.warning(f"Failed to decode CSV with {encoding} encoding, trying next...")
                except Exception as e:
                    logger.error(f"Error processing CSV with {encoding} encoding: {e}")

            if not success:
                logger.error("Failed to process CSV with any encoding")
                sys.exit(1)

            generate_report(_csv_file, _results_log, _log_dir)
            return

        encodings = [
            "utf-8-sig",
            "utf-8",
            "latin-1",
            "cp1252",
            "iso-8859-1",
            "utf-16",
            "utf-16-le",
            "utf-16-be",
        ]
        success = False

        for encoding in encodings:
            try:
                with open(csv_file, newline="", encoding=encoding) as f:
                    reader = csv.DictReader(f)
                    all_rows = list(reader)

                    rows_to_process = all_rows[start_row:]

                success = True
                logger.info(f"Successfully read CSV with {encoding} encoding")
                break
            except UnicodeDecodeError as e:
                if "utf-16" in encoding and "BOM" in str(e):
                    logger.debug(f"Skipping {encoding} - file doesn't have BOM")
                else:
                    logger.warning(f"Failed to decode CSV with {encoding} encoding, trying next...")
            except Exception as e:
                logger.error(f"Error reading CSV with {encoding} encoding: {e}")

        if not success:
            logger.error("Failed to read CSV with any encoding")
            sys.exit(1)

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
        logger.info("=" * 50)

        generate_report(_csv_file, _results_log, _log_dir)


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

            if row_index % 5 == 0 or row_index == total_rows:
                save_checkpoint(_checkpoint_file, row_index, total_rows, _stats, _results_log)

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
