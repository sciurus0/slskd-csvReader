"""
CSV and reporting utilities for the slskd-spotify workflow.

This module centralizes CSV encoding detection, row counting, checkpointing,
and report generation so they can be reused independently of the main script.
"""

from __future__ import annotations

import csv
import os
import pickle
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


def read_pipeline_csv_rows(path: str) -> List[Dict[str, str]]:
    """Read a CSV into dict rows. Raises UnicodeDecodeError if file is not valid UTF-8."""
    raw = Path(path).read_bytes()
    text = decode_pipeline_text(raw)
    return list(csv.DictReader(StringIO(text)))


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


def detect_encoding(file_path: str) -> str:
    """
    Return which UTF-8 variant successfully decodes the file (pipeline CSVs only).

    Raises UnicodeDecodeError if the file is not valid UTF-8 with or without BOM.
    """
    raw = Path(file_path).read_bytes()
    decode_pipeline_text(raw)
    # We do not distinguish which member of PIPELINE_UTF_ENCODINGS matched without re-decoding.
    logger.info("Decoded %s as UTF-8 (with or without BOM)", file_path)
    return "utf-8-sig"


def count_csv_rows(file_path: str) -> int:
    """Count data rows (excluding header) in a UTF-8 pipeline CSV."""
    try:
        rows = read_pipeline_csv_rows(file_path)
        n = len(rows)
        logger.info("Counted %s rows in %s", n, file_path)
        return n
    except UnicodeDecodeError as e:
        logger.error(
            "CSV must be UTF-8 (optional BOM), no transcoding: %s (%s)",
            file_path,
            e,
        )
        return 0
    except OSError as e:
        logger.error("Could not read CSV file %s: %s", file_path, e)
        return 0


def save_checkpoint(
    checkpoint_file: str,
    row_index: int,
    total_rows: int,
    stats: Any,
    results_log: List[Dict[str, Any]],
) -> None:
    """
    Save progress to a checkpoint file for resuming later.
    """
    checkpoint_data: Dict[str, Any] = {
        "row_index": row_index,
        "total_rows": total_rows,
        "timestamp": datetime.now().isoformat(),
        "stats": stats.to_dict(),
        "results_log": results_log,
    }

    try:
        with open(checkpoint_file, "wb") as f:
            pickle.dump(checkpoint_data, f)
        logger.info(f"Checkpoint saved at row {row_index}/{total_rows}")
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
            logger.info(
                f"Checkpoint loaded: row {checkpoint_data['row_index']}/"
                f"{checkpoint_data['total_rows']}"
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
            raw = Path(csv_file).read_bytes()
            text = decode_pipeline_text(raw)
            reader = csv.reader(StringIO(text))
            original_fieldnames = next(reader)
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

