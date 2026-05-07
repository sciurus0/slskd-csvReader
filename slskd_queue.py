"""
Queue utilities for the slskd-spotify workflow: client init, enqueue, and per-user
queue selection. Enqueued files are recorded in a shared tracker for optional use
by the application; this module does not run background download-status polling.
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

