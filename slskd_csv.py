"""
CSV and reporting utilities for the slskd_spotify workflow.

This module centralizes CSV encoding detection, row counting, checkpointing,
and report generation so they can be reused independently of the main script.
"""

from __future__ import annotations

import csv
import os
import pickle
import re
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from slskd_logging import logger

# Pipeline queue/report inputs must be UTF-8 (optional BOM). No legacy encoding fallback.
PIPELINE_UTF_ENCODINGS: Tuple[str, ...] = ("utf-8-sig", "utf-8")


def decode_pipeline_text(raw: bytes) -> str:
    """Decode bytes as UTF-8 with optional BOM, or plain UTF-8. Fail fast — no transcoding."""
    last_err: Optional[UnicodeDecodeError] = None
    for enc in PIPELINE_UTF_ENCODINGS:
        try:
            return raw.decode(enc)
        except UnicodeDecodeError as e:
            last_err = e
    assert last_err is not None
    raise UnicodeDecodeError(
        last_err.encoding,
        last_err.object,
        last_err.start,
        last_err.end,
        last_err.reason,
    ) from last_err


def read_pipeline_csv_bytes(path: str | Path) -> bytes:
    """Read raw bytes from a pipeline CSV path."""
    return Path(path).read_bytes()


def read_pipeline_csv_with_fieldnames(
    path: str | Path,
) -> Tuple[List[str], List[Dict[str, str]]]:
    """
    Read a pipeline CSV (UTF-8 / UTF-8 BOM).

    Returns ``(fieldnames, rows)``. Raises ``UnicodeDecodeError`` or ``OSError``.
    """
    text = decode_pipeline_text(read_pipeline_csv_bytes(path))
    reader = csv.DictReader(StringIO(text))
    fieldnames = list(reader.fieldnames or [])
    return fieldnames, list(reader)


def read_pipeline_csv_rows(path: str | Path) -> List[Dict[str, str]]:
    """Read a CSV into dict rows. Raises UnicodeDecodeError if file is not valid UTF-8."""
    _, rows = read_pipeline_csv_with_fieldnames(path)
    return rows


def atomic_write_pipeline_csv(
    path: str | Path,
    rows: List[Dict[str, str]],
    *,
    fieldnames: Tuple[str, ...] = ("artist", "album", "track"),
) -> None:
    """
    Write UTF-8 BOM pipeline CSV: quote all fields, Unix newlines (Excel-safe apostrophes, etc.).
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    try:
        with open(tmp, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(
                f,
                fieldnames=list(fieldnames),
                quoting=csv.QUOTE_ALL,
                lineterminator="\n",
            )
            w.writeheader()
            for row in rows:
                w.writerow({k: row.get(k, "") for k in fieldnames})
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        if tmp.is_file():
            try:
                tmp.unlink()
            except OSError:
                pass
        raise


_CANDIDATE_LOG_RE = re.compile(
    r"Found available candidate: (.+?) from (\S+) \(queue count:"
)


def save_checkpoint(
    checkpoint_file: str,
    next_row_index: int,
    total_rows: int,
    stats: Any,
    results_log: List[Dict[str, Any]],
    queued_files_tracker: Optional[List[Dict[str, Any]]] = None,
) -> None:
    """
    Save progress to a checkpoint file for resuming later.

    ``next_row_index`` is the next CSV row to process on resume (0-based).
    """
    checkpoint_data: Dict[str, Any] = {
        "next_row_index": next_row_index,
        "row_index": max(0, next_row_index - 1),
        "total_rows": total_rows,
        "timestamp": datetime.now().isoformat(),
        "stats": stats.to_dict(),
        "results_log": results_log,
    }
    if queued_files_tracker is not None:
        checkpoint_data["queued_files_tracker"] = queued_files_tracker

    try:
        with open(checkpoint_file, "wb") as f:
            pickle.dump(checkpoint_data, f)
        logger.info(
            "Checkpoint saved (next row %s/%s, %d tracked file(s))",
            next_row_index,
            total_rows,
            len(queued_files_tracker or []),
        )
    except Exception as e:
        logger.error(f"Error saving checkpoint: {e}")


def load_checkpoint(checkpoint_file: str) -> Optional[Dict[str, Any]]:
    """
    Load progress from a checkpoint file.

    Returns:
        Dictionary with checkpoint data or None if no checkpoint exists.
    """
    try:
        if os.path.exists(checkpoint_file):
            with open(checkpoint_file, "rb") as f:
                checkpoint_data = pickle.load(f)
            next_row = checkpoint_data.get("next_row_index")
            if next_row is None:
                next_row = int(checkpoint_data.get("row_index", -1)) + 1
            logger.info(
                "Checkpoint loaded: resume at row %s/%s",
                next_row,
                checkpoint_data["total_rows"],
            )
            return checkpoint_data
        logger.info("No checkpoint file found")
        return None
    except Exception as e:
        logger.error(f"Error loading checkpoint: {e}")
        return None


def generate_report(
    csv_file: str,
    results_log: List[Dict[str, Any]],
    log_dir: str,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Generate a CSV report of all processed entries that mirrors the input CSV
    format with added status columns.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = os.path.join(log_dir, f"results_{timestamp}.csv")
    input_csv_basename = os.path.basename(csv_file)

    try:
        if not results_log:
            logger.warning("No results to report. Report will not be generated.")
            return None, None

        report_file_abs = os.path.abspath(report_file)

        original_fieldnames: List[str] = []
        try:
            original_fieldnames, _ = read_pipeline_csv_with_fieldnames(csv_file)
            logger.info("Read CSV headers from %s (UTF-8)", csv_file)
        except UnicodeDecodeError:
            logger.warning("Could not decode input CSV as UTF-8: %s", csv_file)
        except StopIteration:
            logger.warning("Empty CSV: %s", csv_file)
        except OSError as e:
            logger.warning("Could not read input CSV: %s", e)

        if not original_fieldnames:
            logger.warning(
                "Using default headers as original headers couldn't be read"
            )
            original_fieldnames = ["artist", "album", "track"]

        output_fieldnames = original_fieldnames + [
            "status",
            "message",
            "files_queued",
            "timestamp",
        ]
        output_fieldnames.extend(
            [
                "attempts",
                "fallback_used",
                "candidate_used",
                "candidate_format",
                "candidate_cleanliness",
                "candidate_username",
                "user_attempt",
                "user_selected",
                "has_album_match",
            ]
        )

        os.makedirs(log_dir, exist_ok=True)

        with open(report_file, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=output_fieldnames)
            writer.writeheader()

            for result in results_log:
                row = {field: result.get(field, "") for field in original_fieldnames}
                row.update(
                    {
                        "status": result.get("status", ""),
                        "message": result.get("message", ""),
                        "files_queued": result.get("files_queued", 0),
                        "timestamp": result.get("timestamp", ""),
                    }
                )
                for field in [
                    "attempts",
                    "fallback_used",
                    "candidate_used",
                    "candidate_format",
                    "candidate_cleanliness",
                    "candidate_username",
                    "user_attempt",
                    "user_selected",
                    "has_album_match",
                ]:
                    row[field] = result.get(field, "")

                writer.writerow(row)

        base, ext = os.path.splitext(input_csv_basename)
        matching_report = os.path.join(log_dir, f"{base}_results{ext}")
        matching_report_abs = os.path.abspath(matching_report)

        import shutil

        shutil.copy2(report_file, matching_report)

        logger.info("CSV Reports generated:")
        logger.info("  1. Timestamped report: %s", report_file_abs)
        logger.info("  2. Input-matched report: %s", matching_report_abs)

        print("\nCSV Reports generated:")
        print(f"  1. Timestamped report: {report_file_abs}")
        print(f"  2. Input-matched report: {matching_report_abs}\n")

        return report_file_abs, matching_report_abs
    except Exception as e:
        logger.error(f"Error generating report: {e}")
        import traceback

        logger.error(traceback.format_exc())
        return None, None


_DOWNLOAD_SUCCESS_STATUS = "success"


def default_pending_csv_path(csv_file: str) -> str:
    """``to_queue.csv`` → ``to_queue_pending.csv`` in the same directory."""
    base, ext = os.path.splitext(csv_file)
    if base.endswith("_pending"):
        return csv_file
    return f"{base}_pending{ext or '.csv'}"


def write_pending_queue_csv(
    csv_file: str,
    results_log: List[Dict[str, Any]],
    *,
    pending_csv: Optional[str] = None,
) -> Optional[str]:
    """
    Write a retry-ready queue CSV with rows that did not complete download.

    Only ``success`` (completed download after reconciliation) is omitted.
    The original input CSV is never modified.
    """
    if not results_log:
        logger.info("No results; skipping pending queue CSV.")
        return None

    pending_path = pending_csv or default_pending_csv_path(csv_file)
    pending_rows: List[Dict[str, str]] = []
    skipped_success = 0

    ordered = sorted(
        results_log,
        key=lambda r: (
            int(r["row_index"]) if r.get("row_index") not in (None, "") else 10**9
        ),
    )
    for entry in ordered:
        status = (entry.get("status") or "").lower()
        if status == _DOWNLOAD_SUCCESS_STATUS:
            skipped_success += 1
            continue
        pending_rows.append(
            {
                "artist": str(entry.get("artist") or ""),
                "album": str(entry.get("album") or ""),
                "track": str(entry.get("track") or ""),
            }
        )

    try:
        atomic_write_pipeline_csv(pending_path, pending_rows)
        pending_abs = os.path.abspath(pending_path)
        logger.info(
            "Pending queue CSV: %s (%d row(s); %d completed download(s) omitted)",
            pending_abs,
            len(pending_rows),
            skipped_success,
        )
        return pending_abs
    except OSError as e:
        logger.error("Error writing pending queue CSV %s: %s", pending_path, e)
        return None


def generate_report_on_demand(
    csv_file: str,
    results_log: List[Dict[str, Any]],
    log_dir: str,
) -> None:
    """Generate a report explicitly when called, even outside the normal process flow."""
    logger.info("Generating CSV report on demand...")
    if not results_log:
        logger.warning("No results to report. No rows have been processed yet.")
        return

    report_paths = generate_report(csv_file, results_log, log_dir)
    if report_paths:
        logger.info("Report generation completed successfully.")
    else:
        logger.error("Failed to generate reports.")


def find_most_recent_import_log(log_dir: str) -> Optional[str]:
    """Find the newest slskd_import_*.log file in the output directory."""
    try:
        candidates: List[Tuple[str, float]] = []
        for filename in os.listdir(log_dir):
            if filename.startswith("slskd_import_") and filename.endswith(".log"):
                file_path = os.path.join(log_dir, filename)
                candidates.append((file_path, os.path.getmtime(file_path)))
        if not candidates:
            return None
        candidates.sort(key=lambda x: x[1], reverse=True)
        newest = candidates[0][0]
        logger.info("Found most recent import log: %s", newest)
        return newest
    except OSError as e:
        logger.error("Error finding import log in %s: %s", log_dir, e)
        return None


def load_tracker_from_newest_import_log(log_dir: str) -> List[Dict[str, Any]]:
    """Rebuild tracker from the newest import log that has enqueue candidate lines."""
    try:
        candidates: List[Tuple[str, float]] = []
        for filename in os.listdir(log_dir):
            if filename.startswith("slskd_import_") and filename.endswith(".log"):
                file_path = os.path.join(log_dir, filename)
                candidates.append((file_path, os.path.getmtime(file_path)))
        candidates.sort(key=lambda x: x[1], reverse=True)
        for file_path, _mtime in candidates:
            tracker = rebuild_tracker_from_import_log(file_path)
            if tracker:
                return tracker
        return []
    except OSError as e:
        logger.error("Error finding import log in %s: %s", log_dir, e)
        return []


def find_matching_results_csv(log_dir: str, csv_file: str) -> Optional[str]:
    """Path to <input_stem>_results.csv beside other reports, if it exists."""
    base, ext = os.path.splitext(os.path.basename(csv_file))
    path = os.path.join(log_dir, f"{base}_results{ext}")
    if os.path.isfile(path):
        logger.info("Found input-matched results CSV: %s", path)
        return path
    return None


def rebuild_tracker_from_import_log(log_path: str) -> List[Dict[str, Any]]:
    """
    Rebuild queued_files_tracker entries from enqueue lines in an import log.

    Expects lines like: Found available candidate: <path> from <user> (queue count: N)
    """
    tracker: List[Dict[str, Any]] = []
    try:
        with open(log_path, encoding="utf-8", errors="replace") as f:
            for line in f:
                match = _CANDIDATE_LOG_RE.search(line)
                if not match:
                    continue
                filename = match.group(1)
                username = match.group(2)
                tracker.append(
                    {
                        "username": username,
                        "filename": filename,
                        "basename": os.path.basename(filename.replace("\\", "/")),
                        "download_status": "queued",
                        "row_index": len(tracker),
                    }
                )
    except OSError as e:
        logger.error("Could not read import log %s: %s", log_path, e)
        return []

    if tracker:
        logger.info("Rebuilt %d tracked file(s) from %s", len(tracker), log_path)
    return tracker


def load_results_log_from_csv(results_csv: str) -> List[Dict[str, Any]]:
    """Load a prior results CSV into results_log dicts (UTF-8 / BOM)."""
    results_log: List[Dict[str, Any]] = []
    try:
        text = decode_pipeline_text(Path(results_csv).read_bytes())
        reader = csv.DictReader(StringIO(text))
        for i, row in enumerate(reader):
            entry = dict(row)
            raw_queued = entry.get("files_queued", "")
            try:
                entry["files_queued"] = int(raw_queued) if str(raw_queued).strip() else 0
            except (TypeError, ValueError):
                entry["files_queued"] = 0
            if entry.get("row_index") not in (None, ""):
                entry["row_index"] = int(entry["row_index"])
            else:
                entry["row_index"] = i
            for flag in ("fallback_used", "has_album_match"):
                if flag in entry and isinstance(entry[flag], str):
                    entry[flag] = entry[flag].lower() in ("true", "1", "yes")
            results_log.append(entry)
        logger.info("Loaded %d row(s) from results CSV: %s", len(results_log), results_csv)
    except UnicodeDecodeError:
        logger.error("Results CSV must be UTF-8 (optional BOM): %s", results_csv)
    except OSError as e:
        logger.error("Could not read results CSV %s: %s", results_csv, e)
    return results_log


def checkpoint_resume_row(checkpoint: Dict[str, Any]) -> int:
    """Return the 0-based row index to resume from a checkpoint."""
    if "next_row_index" in checkpoint:
        return int(checkpoint["next_row_index"])
    return int(checkpoint.get("row_index", -1)) + 1


def find_most_recent_results_csv(log_dir: str) -> Optional[str]:
    """Find the most recent results CSV file in the log directory."""
    try:
        result_files = []
        for filename in os.listdir(log_dir):
            if filename.endswith(".csv") and (
                "results_" in filename or "_results" in filename
            ):
                file_path = os.path.join(log_dir, filename)
                result_files.append((file_path, os.path.getmtime(file_path)))

        if not result_files:
            logger.error(f"No results CSV files found in {log_dir}")
            return None

        result_files.sort(key=lambda x: x[1], reverse=True)
        newest_file = result_files[0][0]
        logger.info(f"Found most recent results file: {newest_file}")
        return newest_file
    except Exception as e:
        logger.error(f"Error finding most recent results CSV: {e}")
        return None


def load_failed_rows(results_csv: str) -> List[Dict[str, Any]]:
    """Load rows that failed in the previous run from a results CSV file (UTF-8 BOM or UTF-8)."""
    failed_rows: List[Dict[str, Any]] = []
    try:
        text = decode_pipeline_text(Path(results_csv).read_bytes())
        reader = csv.DictReader(StringIO(text))
        for row in reader:
            status = row.get("status", "").lower()
            if status in ("failed", "error"):
                clean_row = {
                    k: v
                    for k, v in row.items()
                    if k not in ("status", "message", "files_queued", "timestamp")
                }
                failed_rows.append(clean_row)
        logger.info("Read results CSV as UTF-8: %s", results_csv)
    except UnicodeDecodeError:
        logger.error("Results CSV must be UTF-8 (optional BOM): %s", results_csv)
    except OSError as e:
        logger.error("Could not read results CSV %s: %s", results_csv, e)

    logger.info(f"Loaded {len(failed_rows)} failed rows for retry")
    return failed_rows

