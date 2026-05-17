"""
Queue utilities for the slskd_spotify workflow: client init, enqueue, per-user
queue selection, and post-run download reconciliation against the SLSKD transfers API.
"""

import asyncio
import concurrent.futures
import json
import os
from asyncio import TimeoutError
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
import slskd_api

from slskd_config import (
    API_KEY,
    API_PATH,
    ENQUEUE_TIMEOUT,
    HOST,
    QUEUE_LIMIT,
    USE_DIRECT_API,
    make_headers,
)
from slskd_logging import logger

_host = HOST
_api_path = API_PATH
_api_key = API_KEY
_headers = make_headers(API_KEY)
_enqueue_timeout = ENQUEUE_TIMEOUT
_use_direct_api = USE_DIRECT_API
_queue_limit = QUEUE_LIMIT
_queued_files_tracker: List[Dict[str, Any]] = []
_stats: Any = None

# Normalized transfer states stored on queued_files_tracker entries.
_ACTIVE_DOWNLOAD_STATES = frozenset({"queued", "downloading", "requested", "inprogress"})
_SUCCESS_DOWNLOAD_STATES = frozenset({"completed"})
_FAILURE_DOWNLOAD_STATES = frozenset({"cancelled", "failed", "errored", "rejected"})

_STATE_MAP = {
    "requested": "queued",
    "inprogress": "downloading",
    "completed": "completed",
    "cancelled": "cancelled",
    "failed": "failed",
    "errored": "errored",
    "rejected": "rejected",
}

_RECONCILEABLE_RESULT_STATUSES = frozenset(
    {"enqueued", "download_incomplete", "download_failed"}
)


def configure_queue_context(
    *,
    host: str,
    api_path: str,
    api_key: str,
    headers: Dict[str, str],
    enqueue_timeout: int,
    use_direct_api: bool,
    queue_limit: int,
    queued_files_tracker: List[Dict[str, Any]],
    stats: Any,
) -> None:
    """Configure runtime values used by queue/download helpers."""
    global _host, _api_path, _api_key, _headers, _enqueue_timeout
    global _use_direct_api, _queue_limit, _queued_files_tracker, _stats
    _host = host
    _api_path = api_path
    _api_key = api_key
    _headers = headers
    _enqueue_timeout = enqueue_timeout
    _use_direct_api = use_direct_api
    _queue_limit = queue_limit
    _queued_files_tracker = queued_files_tracker
    _stats = stats


def initialize_slskd_client() -> slskd_api.SlskdClient:
    """Initialize and return a properly configured SLSKD API client."""
    return slskd_api.SlskdClient(host=_host, api_key=_api_key)


def _normalize_path_key(path: str) -> str:
    """Normalize Soulseek paths for comparison (OS separators, case)."""
    return (path or "").replace("\\", "/").strip().lower()


def _flatten_transfer_downloads(payload: Any) -> List[Dict[str, Any]]:
    """
    Flatten SLSKD transfer API payloads to per-file dicts.

    Modern slskd returns downloads grouped by user and directory::

        [{"username": "...", "directories": [{"files": [{...}, ...]}]}]

    Older or other endpoints may return a flat list of file transfers.
    """
    if payload is None:
        return []
    if isinstance(payload, dict):
        if isinstance(payload.get("downloads"), list):
            return _flatten_transfer_downloads(payload["downloads"])
        if payload.get("filename") or payload.get("id"):
            return [payload]
        return []

    if not isinstance(payload, list):
        return []

    flat: List[Dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        directories = item.get("directories")
        if directories is not None:
            user = (item.get("username") or "").strip()
            for directory in directories:
                if not isinstance(directory, dict):
                    continue
                for file_entry in directory.get("files") or []:
                    if not isinstance(file_entry, dict):
                        continue
                    row = dict(file_entry)
                    if user and not row.get("username"):
                        row["username"] = user
                    flat.append(row)
            continue
        if item.get("filename") or item.get("id"):
            flat.append(item)
    return flat


def _normalize_transfer_state(raw: Any) -> str:
    """Map slskd state strings (e.g. 'Completed, Succeeded') to internal status tokens."""
    s = str(raw or "").strip().lower()
    if not s:
        return "unknown"
    # Non-success terminal states before generic "completed" (e.g. "Completed, Aborted").
    if "aborted" in s:
        return "failed"
    if "cancel" in s:
        return "cancelled"
    if any(tok in s for tok in ("failed", "errored", "rejected")):
        return "failed"
    if "completed" in s and ("succeed" in s or "success" in s):
        return "completed"
    if "completed" in s:
        return "failed"
    if "in progress" in s or "inprogress" in s or "transferring" in s:
        return "downloading"
    if "requested" in s or "queued" in s or "enqueued" in s:
        return "queued"
    mapped = _STATE_MAP.get(s)
    if mapped:
        return mapped
    return s.split(",")[0].strip() or "unknown"


async def _http_get_json(url: str) -> Any:
    with concurrent.futures.ThreadPoolExecutor() as pool:
        resp = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(
                pool, lambda: requests.get(url, headers=_headers)
            ),
            timeout=_enqueue_timeout,
        )
    resp.raise_for_status()
    return resp.json()


def _filename_matches(
    queued_file: Dict[str, Any], download: Dict[str, Any]
) -> bool:
    username = (queued_file.get("username") or "").strip()
    filename = queued_file.get("filename") or ""
    basename = queued_file.get("basename") or os.path.basename(filename)
    dl_filename = download.get("filename", "") or ""
    dl_basename = os.path.basename(dl_filename) if dl_filename else ""
    dl_username = (download.get("username") or "").strip()
    if username and dl_username and dl_username != username:
        return False

    fn_key = _normalize_path_key(filename)
    dl_key = _normalize_path_key(dl_filename)
    bn_key = _normalize_path_key(basename)
    dl_bn_key = _normalize_path_key(dl_basename)

    if fn_key and dl_key and fn_key == dl_key:
        return True
    if bn_key and dl_bn_key and bn_key == dl_bn_key:
        return True
    if bn_key and dl_bn_key and (bn_key in dl_bn_key or dl_bn_key in bn_key):
        return True
    return False


def _record_tracked_enqueue(
    username: str,
    file_info: Dict[str, Any],
    *,
    row_index: Optional[int],
) -> None:
    filename = file_info.get("filename")
    if not filename:
        return
    now = datetime.now().isoformat()
    tracked: Dict[str, Any] = {
        "username": username,
        "filename": filename,
        "basename": os.path.basename(filename),
        "queued_at": now,
        "download_status": "queued",
        "last_checked": now,
        "size": file_info.get("size", 0),
    }
    if row_index is not None:
        tracked["row_index"] = row_index
    if "_search_terms" in file_info:
        tracked["search_terms"] = file_info["_search_terms"]
    _queued_files_tracker.append(tracked)


async def enqueue_files_async(
    client: slskd_api.SlskdClient,
    username: str,
    search_results: List[Dict[str, Any]],
    *,
    row_index: Optional[int] = None,
) -> int:
    """Enqueue one or more files via slskd_api client."""
    try:
        logger.info(f"Enqueueing files from {username}: {len(search_results)} file(s)")
        file_ids = []
        for file_info in search_results:
            if "id" in file_info:
                file_ids.append(file_info["id"])
            elif "file_id" in file_info:
                file_ids.append(file_info["file_id"])

        if not file_ids:
            file_info_list = search_results
            logger.info(f"Sending full file info: {len(file_info_list)} files")
        else:
            file_info_list = file_ids
            logger.info(f"Sending file IDs: {file_ids}")

        if _use_direct_api:
            try:
                post_url = f"{_host.rstrip('/')}{_api_path}/transfers/downloads/{username}"
                logger.debug(f"Direct API call to: {post_url}")
                logger.debug(f"Payload: {json.dumps(file_info_list)}")

                with concurrent.futures.ThreadPoolExecutor() as pool:
                    resp = await asyncio.wait_for(
                        asyncio.get_event_loop().run_in_executor(
                            pool,
                            lambda: requests.post(
                                post_url, json=file_info_list, headers=_headers
                            ),
                        ),
                        timeout=_enqueue_timeout,
                    )
                resp.raise_for_status()
                logger.info(
                    f"Successfully enqueued {len(file_info_list)} file(s) for {username} using direct API"
                )

                for file_info in search_results:
                    _record_tracked_enqueue(username, file_info, row_index=row_index)

                if _stats is not None:
                    _stats.enqueued_files += len(file_info_list)
                return len(file_info_list)
            except Exception:
                if _use_direct_api:
                    raise
                logger.warning("Direct API enqueue failed, trying library method")

        with concurrent.futures.ThreadPoolExecutor() as pool:
            await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(
                    pool, lambda: client.transfers.enqueue(username, file_info_list)
                ),
                timeout=_enqueue_timeout,
            )

        for file_info in search_results:
            _record_tracked_enqueue(username, file_info, row_index=row_index)

        logger.info(
            f"Successfully enqueued {len(file_info_list)} file(s) for {username} using client library"
        )
        if _stats is not None:
            _stats.enqueued_files += len(file_info_list)
        return len(file_info_list)

    except TimeoutError:
        logger.error(f"Enqueue operation timed out for {username}")
        return 0
    except Exception as e:
        logger.error(f"Client enqueue failed for {username}: {e}")
        logger.error(f"Failed payload: {search_results}")
        return 0


async def refresh_tracked_download_status() -> Dict[str, int]:
    """Poll SLSKD and update download_status on all tracked enqueued files."""
    summary: Dict[str, int] = {}
    if not _queued_files_tracker:
        return summary

    try:
        active_url = f"{_host.rstrip('/')}{_api_path}/transfers/downloads"
        active_downloads = _flatten_transfer_downloads(await _http_get_json(active_url))
        logger.info(
            "Reconciliation: %d active transfer file(s) from SLSKD",
            len(active_downloads),
        )

        completed_downloads: List[Dict[str, Any]] = []
        try:
            completed_url = f"{_host.rstrip('/')}{_api_path}/transfers/downloads/completed"
            completed_downloads = _flatten_transfer_downloads(
                await _http_get_json(completed_url)
            )
        except Exception as e:
            logger.warning("Error getting completed downloads: %s", e)

        download_dir_files: List[Dict[str, Any]] = []
        try:
            folder_url = f"{_host.rstrip('/')}{_api_path}/filesystem/downloads"
            folder_data = await _http_get_json(folder_url)
            if isinstance(folder_data, dict):
                if isinstance(folder_data.get("files"), list):
                    download_dir_files = folder_data["files"]
                elif isinstance(folder_data.get("content"), list):
                    download_dir_files = folder_data["content"]
        except Exception as e:
            logger.warning("Error checking download folder: %s", e)

        for queued_file in _queued_files_tracker:
            old_status = queued_file.get("download_status", "unknown")
            current_status = "not_found"

            for download in active_downloads:
                if not _filename_matches(queued_file, download):
                    continue
                raw_state = download.get("state") or download.get("stateDescription") or ""
                current_status = _normalize_transfer_state(raw_state)
                queued_file["size"] = download.get("size", queued_file.get("size", 0))
                queued_file["transfer_id"] = download.get("id", queued_file.get("transfer_id"))
                break

            if current_status == "not_found":
                for download in completed_downloads:
                    if _filename_matches(queued_file, download):
                        current_status = "completed"
                        break

            if current_status == "not_found" and download_dir_files:
                basename = queued_file.get("basename") or ""
                for file_info in download_dir_files:
                    dl_name = file_info.get("name", "") if isinstance(file_info, dict) else ""
                    if basename and (basename == dl_name or basename in dl_name):
                        current_status = "completed"
                        break

            if current_status != old_status:
                logger.info(
                    "Download status for %s: %s → %s",
                    queued_file.get("basename"),
                    old_status,
                    current_status,
                )

            queued_file["download_status"] = current_status
            queued_file["last_checked"] = datetime.now().isoformat()
            summary[current_status] = summary.get(current_status, 0) + 1

        if summary:
            logger.info("Download status summary: %s", summary)
    except Exception as e:
        logger.error("Error refreshing tracked download status: %s", e)

    return summary


def _row_download_outcome(states: List[str]) -> str:
    """Map tracked download_status values for one row to a results_log status."""
    if not states:
        return "download_incomplete"
    if all(s in _SUCCESS_DOWNLOAD_STATES for s in states):
        return "success"
    if any(s in _FAILURE_DOWNLOAD_STATES for s in states):
        return "download_failed"
    if any(s in _ACTIVE_DOWNLOAD_STATES or s == "not_found" for s in states):
        return "download_incomplete"
    if all(s == "not_found" for s in states):
        return "download_incomplete"
    return "download_incomplete"


def snapshot_queued_files_tracker() -> List[Dict[str, Any]]:
    """Return a shallow copy of tracked enqueues for checkpointing."""
    return [dict(entry) for entry in _queued_files_tracker]


def prepare_results_for_reconciliation(
    results_log: List[Dict[str, Any]],
    *,
    reverify_success: bool = False,
) -> int:
    """Reset terminal download statuses so apply_download_status_to_results can update them."""
    statuses = set(_RECONCILEABLE_RESULT_STATUSES)
    if reverify_success:
        statuses.add("success")
    count = 0
    for entry in results_log:
        status = (entry.get("status") or "").lower()
        if status in statuses:
            entry["status"] = "enqueued"
            count += 1
    return count


def apply_download_status_to_results(results_log: List[Dict[str, Any]]) -> Dict[str, int]:
    """Update enqueued rows in results_log from tracked transfer outcomes."""
    counts: Dict[str, int] = {}
    by_row: Dict[int, List[str]] = {}
    for tracked in _queued_files_tracker:
        row_index = tracked.get("row_index")
        if row_index is None:
            continue
        by_row.setdefault(int(row_index), []).append(
            (tracked.get("download_status") or "unknown").lower()
        )

    for entry in results_log:
        if (entry.get("status") or "").lower() != "enqueued":
            continue
        row_index = entry.get("row_index")
        if row_index is None:
            continue
        states = by_row.get(int(row_index), [])
        final_status = _row_download_outcome(states)
        entry["status"] = final_status
        if final_status == "success":
            entry["message"] = f"Download completed ({len(states)} transfer(s))"
        elif final_status == "download_failed":
            entry["message"] = f"Download failed or rejected ({', '.join(states)})"
        else:
            entry["message"] = (
                f"Download not finished after reconciliation ({', '.join(states) or 'no tracker match'})"
            )
        counts[final_status] = counts.get(final_status, 0) + 1

    return counts


async def wait_and_reconcile_downloads(
    results_log: List[Dict[str, Any]],
    *,
    settle_seconds: float,
    prepare_rows: bool = False,
    reverify_success: bool = False,
) -> Dict[str, int]:
    """
    Wait after queue processing, then poll SLSKD once and update results_log statuses.

    ``success`` means all tracked transfers for the row completed; non-terminal states
  become ``download_incomplete`` or ``download_failed``.

    When ``prepare_rows`` is True, rows in download_incomplete/download_failed are reset
    to enqueued first (for ``--reconcile-downloads``). If ``reverify_success`` is also
    True, existing success rows are re-polled (correct false positives).
    """
    if prepare_rows:
        prepared = prepare_results_for_reconciliation(
            results_log, reverify_success=reverify_success
        )
        if reverify_success and prepared:
            logger.info(
                "Prepared %d row(s) for download reconciliation (including success re-verify)",
                prepared,
            )
        else:
            logger.info("Prepared %d row(s) for download reconciliation", prepared)

    enqueued_count = sum(1 for r in results_log if (r.get("status") or "").lower() == "enqueued")
    if enqueued_count == 0:
        logger.info("No enqueued rows; skipping download reconciliation.")
        return {}

    if settle_seconds > 0:
        logger.info(
            "Queue processing finished; waiting %.0fs before download reconciliation...",
            settle_seconds,
        )
        await asyncio.sleep(settle_seconds)

    logger.info("Reconciling download status with SLSKD transfers API...")
    await refresh_tracked_download_status()
    outcome_counts = apply_download_status_to_results(results_log)
    logger.info("Download reconciliation outcomes: %s", outcome_counts)
    return outcome_counts


async def check_queue_status(username: str) -> Tuple[bool, int]:
    """Check a user's queue status."""
    try:
        url = f"{_host.rstrip('/')}{_api_path}/transfers/downloads"
        with concurrent.futures.ThreadPoolExecutor() as pool:
            resp = await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(
                    pool, lambda: requests.get(url, headers=_headers)
                ),
                timeout=_enqueue_timeout,
            )
        resp.raise_for_status()
        downloads = _flatten_transfer_downloads(resp.json())

        user_queue_count = 0
        for download in downloads:
            if isinstance(download, dict) and download.get("username") == username:
                user_queue_count += 1

        has_open_slot = _queue_limit == 0 or user_queue_count < _queue_limit
        return has_open_slot, user_queue_count
    except Exception as e:
        logger.warning(f"Failed to check queue status for {username}: {e}")
        return False, 999


async def find_best_available_candidate(
    ranked_candidates: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """Find the first ranked candidate that has an open queue slot."""
    checked_users = set()
    for candidate in ranked_candidates:
        username = candidate["username"]
        if username in checked_users:
            continue
        checked_users.add(username)
        has_open_slot, queue_count = await check_queue_status(username)
        if has_open_slot:
            logger.info(
                f"Found available candidate: {os.path.basename(candidate['filename'])} "
                f"from {username} (queue count: {queue_count})"
            )
            return candidate
        logger.debug(f"Skipping {username} - queue count: {queue_count}")
    return None

