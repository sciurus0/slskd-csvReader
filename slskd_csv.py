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
from typing import Any, Dict, List, Optional, Tuple

from slskd_logging import logger


def detect_encoding(file_path: str) -> str:
    """
    Detect the encoding of a file by trying multiple encodings.

    Returns the detected encoding or 'utf-8' as a safe fallback.
    """
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

    for encoding in encodings:
        try:
            with open(file_path, "r", newline="", encoding=encoding) as f:
                _ = f.read(1024)
            logger.info(f"Detected encoding for {file_path}: {encoding}")
            return encoding
        except UnicodeDecodeError:
            logger.debug(f"Failed to decode {file_path} with {encoding} encoding")
        except Exception as e:
            logger.warning(f"Error testing {encoding} encoding for {file_path}: {e}")

    logger.warning(f"Could not detect encoding for {file_path}, using utf-8 as fallback")
    return "utf-8"


def count_csv_rows(file_path: str) -> int:
    """Count the total number of data rows in the CSV file (excluding header)."""
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

    for encoding in encodings:
        try:
            with open(file_path, newline="", encoding=encoding) as f:
                count = sum(1 for _ in csv.reader(f)) - 1  # subtract header row
                logger.info(f"Successfully read CSV with {encoding} encoding")
                return count
        except UnicodeDecodeError as e:
            if "utf-16" in encoding and "BOM" in str(e):
                logger.debug(f"Skipping {encoding} - file doesn't have BOM")
            else:
                logger.warning(
                    f"Failed to decode CSV with {encoding} encoding, trying next..."
                )
        except Exception as e:
            logger.error(f"Error counting CSV rows: {e}")
            return 0

    logger.error(f"Could not read CSV file with any encoding: {file_path}")
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

        # Read the original headers from the input CSV
        original_fieldnames: List[str] = []
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
        for encoding in encodings:
            try:
                with open(csv_file, "r", newline="", encoding=encoding) as f:
                    reader = csv.reader(f)
                    original_fieldnames = next(reader)
                logger.info(f"Successfully read CSV headers with {encoding} encoding")
                break
            except UnicodeDecodeError as e:
                if "utf-16" in encoding and "BOM" in str(e):
                    logger.debug("Skipping %s - file doesn't have BOM", encoding)
                else:
                    logger.warning(
                        "Failed to decode CSV headers with %s encoding, trying next...",
                        encoding,
                    )
            except Exception as e:
                logger.warning(
                    "Could not read headers from input CSV with %s encoding: %s",
                    encoding,
                    e,
                )

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

        with open(report_file, "w", newline="", encoding="utf-8") as f:
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
    """Load rows that failed in the previous run from a results CSV file."""
    failed_rows: List[Dict[str, Any]] = []
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
            with open(results_csv, "r", newline="", encoding=encoding) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    status = row.get("status", "").lower()
                    if status in ("failed", "error"):
                        clean_row = {
                            k: v
                            for k, v in row.items()
                            if k
                            not in ("status", "message", "files_queued", "timestamp")
                        }
                        failed_rows.append(clean_row)

            success = True
            logger.info(
                "Successfully read results CSV with %s encoding", encoding
            )
            break
        except UnicodeDecodeError as e:
            if "utf-16" in encoding and "BOM" in str(e):
                logger.debug(f"Skipping {encoding} - file doesn't have BOM")
            else:
                logger.warning(
                    f"Failed to decode results CSV with {encoding} encoding, trying next..."
                )
        except Exception as e:
            logger.error(
                f"Error loading failed rows with {encoding} encoding: {e}"
            )

    if not success and not failed_rows:
        logger.error(
            f"Could not read results CSV with any encoding: {results_csv}"
        )

    logger.info(f"Loaded {len(failed_rows)} failed rows for retry")
    return failed_rows

