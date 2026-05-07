"""
Queue and download status utilities for the slskd-spotify workflow.
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


async def enqueue_files_async(
    client: slskd_api.SlskdClient, username: str, search_results: List[Dict[str, Any]]
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
                    filename = file_info.get("filename")
                    if filename:
                        now = datetime.now().isoformat()
                        tracked_file = {
                            "username": username,
                            "filename": filename,
                            "basename": os.path.basename(filename),
                            "queued_at": now,
                            "download_status": "queued",
                            "last_checked": now,
                            "size": file_info.get("size", 0),
                        }
                        if "_search_terms" in file_info:
                            tracked_file["search_terms"] = file_info["_search_terms"]
                        _queued_files_tracker.append(tracked_file)

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
            filename = file_info.get("filename")
            if filename:
                now = datetime.now().isoformat()
                _queued_files_tracker.append(
                    {
                        "username": username,
                        "filename": filename,
                        "basename": os.path.basename(filename),
                        "queued_at": now,
                        "download_status": "queued",
                        "last_checked": now,
                        "size": file_info.get("size", 0),
                    }
                )

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


async def check_downloads_status() -> None:
    """Check status of all tracked queued downloads."""
    try:
        url = f"{_host.rstrip('/')}{_api_path}/transfers/downloads"
        with concurrent.futures.ThreadPoolExecutor() as pool:
            resp = await asyncio.get_event_loop().run_in_executor(
                pool, lambda: requests.get(url, headers=_headers)
            )
        resp.raise_for_status()
        active_downloads = resp.json()

        if isinstance(active_downloads, dict):
            if "downloads" in active_downloads:
                active_downloads = active_downloads["downloads"]

        completed_downloads = []
        try:
            completed_url = f"{_host.rstrip('/')}{_api_path}/transfers/downloads/completed"
            with concurrent.futures.ThreadPoolExecutor() as pool:
                completed_resp = await asyncio.get_event_loop().run_in_executor(
                    pool, lambda: requests.get(completed_url, headers=_headers)
                )
            if completed_resp.status_code == 200:
                completed_data = completed_resp.json()
                if isinstance(completed_data, dict) and "downloads" in completed_data:
                    completed_downloads = completed_data["downloads"]
                elif isinstance(completed_data, list):
                    completed_downloads = completed_data
        except Exception as e:
            logger.warning(f"Error getting completed downloads: {e}")

        download_dir_files = []
        try:
            folder_url = f"{_host.rstrip('/')}{_api_path}/filesystem/downloads"
            with concurrent.futures.ThreadPoolExecutor() as pool:
                folder_resp = await asyncio.get_event_loop().run_in_executor(
                    pool, lambda: requests.get(folder_url, headers=_headers)
                )
            if folder_resp.status_code == 200:
                folder_data = folder_resp.json()
                if isinstance(folder_data, dict):
                    if "files" in folder_data:
                        download_dir_files = folder_data["files"]
                    elif "content" in folder_data:
                        download_dir_files = folder_data.get("content", [])
        except Exception as e:
            logger.warning(f"Error checking download folder: {e}")

        status_map = {
            "requested": "queued",
            "inprogress": "downloading",
            "completed": "completed",
            "cancelled": "cancelled",
        }

        status_updated = False
        for queued_file in _queued_files_tracker:
            username = queued_file.get("username")
            filename = queued_file.get("filename")
            basename = queued_file.get("basename") or os.path.basename(filename)
            old_status = queued_file.get("download_status", "unknown")
            if not filename:
                continue

            current_status = "not_found"
            for download in active_downloads:
                dl_filename = download.get("filename", "")
                dl_basename = os.path.basename(dl_filename) if dl_filename else ""
                dl_username = download.get("username", "")
                dl_state = download.get("state", "").lower()
                if (dl_username == username or not username) and any(
                    [
                        dl_filename == filename,
                        dl_basename == basename,
                        basename in dl_basename,
                        dl_basename in basename,
                    ]
                ):
                    current_status = status_map.get(dl_state, dl_state)
                    queued_file["size"] = download.get("size")
                    queued_file["bytesTransferred"] = download.get("bytesTransferred")
                    queued_file["progress"] = (
                        f"{(download.get('bytesTransferred', 0) / max(1, download.get('size', 1)) * 100):.1f}%"
                        if download.get("size")
                        else "unknown"
                    )
                    break

            if current_status == "not_found":
                for download in completed_downloads:
                    dl_filename = download.get("filename", "")
                    dl_basename = os.path.basename(dl_filename) if dl_filename else ""
                    dl_username = download.get("username", "")
                    if (dl_username == username or not username) and any(
                        [
                            dl_filename == filename,
                            dl_basename == basename,
                            basename in dl_basename,
                            dl_basename in basename,
                        ]
                    ):
                        current_status = "completed"
                        queued_file["size"] = download.get("size")
                        queued_file["bytesTransferred"] = download.get("size")
                        queued_file["progress"] = "100%"
                        break

            if current_status == "not_found" and download_dir_files:
                for file_info in download_dir_files:
                    dl_filename = file_info.get("name", "")
                    if basename == dl_filename or basename in dl_filename:
                        current_status = "completed"
                        queued_file["size"] = file_info.get("size")
                        queued_file["bytesTransferred"] = file_info.get("size")
                        queued_file["progress"] = "100%"
                        break

            if current_status != old_status:
                status_updated = True
                logger.info(f"Download status for {basename}: {old_status} → {current_status}")

            queued_file["download_status"] = current_status
            queued_file["last_checked"] = datetime.now().isoformat()

        if status_updated:
            status_counts: Dict[str, int] = {}
            for file in _queued_files_tracker:
                status = file.get("download_status", "unknown")
                status_counts[status] = status_counts.get(status, 0) + 1
            logger.info(f"Download status summary: {status_counts}")
    except Exception as e:
        logger.error(f"Error checking downloads status: {e}")


async def start_status_monitoring(check_interval=300):
    """Start a background task that periodically checks download status."""
    logger.info(f"Starting download status monitoring (checking every {check_interval} seconds)")
    while True:
        await asyncio.sleep(check_interval)
        if _queued_files_tracker:
            await check_downloads_status()
        else:
            logger.debug("No queued files to check status for")


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
        downloads = resp.json()

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

