#!/usr/bin/env python3
"""
queue_albums_slskd_hybrid.py

Hybrid: always run a fresh search via POST, and use slskd_api client for enqueueing.
Supports single-track or full-album queueing.
"""

import csv
import time
import os
import sys
import logging
import requests
from collections import namedtuple

import slskd_api

# ======== Logging Setup ========
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# ======== Configuration ========
HOST = "http://localhost:5030"          # Base URL of your SLSKD server
API_PATH = "/api/v0"                    # API prefix path (used by direct requests)
# Load API key from environment variable - REQUIRED for security
API_KEY = os.environ.get("SLSKD_API_KEY")
if not API_KEY:
    print("ERROR: SLSKD_API_KEY environment variable is required but not set.")
    print("Please set it with: export SLSKD_API_KEY='your-api-key-here'")
    sys.exit(1)
CSV_FILE = "to_queue.csv"               # CSV should contain columns: artist, album, [track]
QUEUE_LIMIT = 0                         # Maximum items in queue per user (0 for no limit)

HEADERS = {
    "X-API-KEY": API_KEY,
    "Accept": "application/json",
    "Content-Type": "application/json"
}

POLL_INTERVAL = 2    # seconds between polling search responses
MAX_POLLS = 20       # maximum poll attempts per search

# Default source identifier for queueing downloads
# Must match one of the DownloadSource enum values in SLSKD
SOURCE = "HeadphonesVip"

# Named tuple for best match
Result = namedtuple('Result', ['username','result_id','filename'])


def poll_responses(job_id):
    """Poll until the given search job has responses, then return them."""
    base = f"{HOST.rstrip('/')}{API_PATH}/searches/{job_id}/responses"
    for _ in range(MAX_POLLS):
        time.sleep(POLL_INTERVAL)
        resp = requests.get(base, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict):
            results = data.get('responses') or data.get('Responses') or []
        else:
            results = data
        if results:
            return results
    return []


def search_slskd(pattern):
    """Always run a fresh search: POST new search, then poll."""
    post_url = f"{HOST.rstrip('/')}{API_PATH}/searches"
    logger.info(f"⬢ POST {post_url} payload: {{'SearchText': '{pattern}'}}")
    resp = requests.post(post_url, json={"SearchText": pattern}, headers=HEADERS)
    resp.raise_for_status()
    data = resp.json()
    job_id = data.get('id') or data.get('SearchId')
    if not job_id:
        raise RuntimeError(f"No job ID returned by POST: {data}")
    return poll_responses(job_id)


def select_best(responses, album_name):
    """From search responses, pick best match by filename or largest size."""
    candidates = []
    alb_lower = album_name.lower()
    for r in responses:
        user = r.get('username')
        rid = r.get('id') or r.get('SearchResultId') or r.get('token')
        for f in r.get('files', []):
            fn = f.get('filename')
            size = f.get('size', 0)
            candidates.append((user, rid, size, fn))
    if not candidates:
        return None
    for user, rid, _, fn in candidates:
        if alb_lower in (fn or '').lower():
            return Result(user, rid, fn)
    user, rid, _, fn = max(candidates, key=lambda x: x[2])
    return Result(user, rid, fn)


def initialize_slskd_client():
    """Return a SlskdClient with correct host and API key."""
    return slskd_api.SlskdClient(host=HOST, api_key=API_KEY)


def enqueue_files(client, username, ids):
    """Enqueue one or more searchResultIds via slskd_api client."""
    payload = {"SearchResultIds": ids, "Source": SOURCE}
    logger.debug(f"Enqueue payload={payload} for user={username}")
    try:
        client.transfers.enqueue(username, payload)
        logger.info(f"Enqueued {len(ids)} file(s) for {username}")
    except Exception as e:
        logger.error(f"Client enqueue failed for {username}: {e}")
        raise


if __name__ == '__main__':
    if not os.path.isfile(CSV_FILE):
        logger.error(f"CSV file not found: {CSV_FILE}")
        sys.exit(1)

    client = initialize_slskd_client()
    with open(CSV_FILE, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            artist = (row.get('artist') or '').strip()
            album = (row.get('album') or '').strip()
            track = (row.get('track') or '').strip()
            if not artist or not album:
                logger.warning(f"Skipping incomplete row: {row}")
                continue

            pattern = f"{artist} - {album}" + (f" - {track}" if track else '')
            logger.info(f"🔍 Searching for: {pattern}")
            try:
                resp_list = search_slskd(pattern)
            except Exception as e:
                logger.error(f"Search error for '{pattern}': {e}")
                continue
            if not resp_list:
                logger.info(f"No results for: {pattern}")
                continue

            if track:
                for r in resp_list:
                    for f in r.get('files', []):
                        if track.lower() in (f.get('filename') or '').lower():
                            enqueue_files(client, r.get('username'), [r.get('id') or r.get('token')])
                            logger.info(f"Queued single track '{track}' from {r.get('username')}")
                            break
                    else:
                        continue
                    break
            else:
                best = select_best(resp_list, album)
                if not best:
                    logger.info(f"No valid result for: {pattern}")
                    continue
                enqueue_files(client, best.username, [best.result_id])
                logger.info(f"Queued result {best.result_id} ({os.path.basename(best.filename)}) from {best.username}")
