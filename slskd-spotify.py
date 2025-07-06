#!/usr/bin/env python3
"""
queue_albums_slskd_hybrid.py

Hybrid: always run a fresh search via POST, and use slskd_api client for enqueueing.
Supports single-track or full-album queueing with advanced features:
- Rate limiting
- Error resilience
- Comprehensive logging
- Batch processing
- Resumability
- Timeout handling
- Format prioritization (.mp3 → .m4a → .flac)
- Retry-failed mode for failed downloads
- Download status monitoring
- Queue management with configurable limits
- Filename sanitization for special characters

Command-line Options:
------------------
  -h, --help            Show this help message and exit
  
  -c CSV, --csv CSV     Path to CSV file (default: to_queue.csv)
                        The CSV should contain columns: artist, album, [track]
                        If track is provided, only that track will be downloaded
                        If track is empty, the entire album will be downloaded
  
  -r, --resume          Resume from last checkpoint
                        Continues processing from where previous run stopped
                        
  --retry-failed, -rf   Only retry rows that failed in previous runs
                        Uses the most recent results CSV file in the output directory
  
  -b N, --batch-size N  Number of rows to process in each batch (default: 10)
                        Lower for less memory usage, higher for better performance
  
  -d SEC, --delay SEC   Delay between API calls in seconds (default: 1.0)
                        Increase to avoid rate limiting or high server load
  
  -f EXT, --formats EXT [EXT ...]
                        Allowed file formats in priority order (default: ['.mp3', '.m4a', '.flac'])
                        Only these formats will be downloaded, in the order specified
                        Files in formats not in this list will be ignored
                        
  -e EXT, --exclude EXT [EXT ...]
                        File extensions to exclude entirely (default: ['.lrc'])
                        These file types will never be downloaded
                        
  -o DIR, --output-dir DIR
                        Directory for output files (default: logs)
                        Stores log files and results CSV files
                        
  --debug               Enable debug logging for more detailed output
                        Includes API call details and file processing

  --direct-api          Use direct API calls instead of client library
                        May help when client library is having issues

  --gen-report          Generate a report from existing checkpoint data
                        Creates CSV reports without processing any new files
                        Useful if you need to recreate reports
                        
  --exact-match         Require exact artist-album-track matching instead of partial matching
                        When enabled, only files with exact matches to the specified pattern are selected
                        When disabled (default), files that contain the track name are selected

  --queue-limit N       Maximum items in queue per user (default: 0)
                        Set to 0 for no limit
                        Use to prevent overloading specific users' queues

Examples:
--------
  # Basic usage with default settings (no queue limit)
  python slskd-spotify.py
  
  # Process a specific CSV file with increased delay between requests
  python slskd-spotify.py --csv my_music.csv --delay 2.5
  
  # Resume a previous run that was interrupted, with larger batch size
  python slskd-spotify.py --resume --batch-size 20
  
  # Retry only failed downloads from previous run
  python slskd-spotify.py --retry-failed
  
  # Change allowed formats and their priority
  python slskd-spotify.py --formats .mp3 .flac
  
  # Change excluded file types
  python slskd-spotify.py --exclude .lrc .nfo .m3u
  
  # Specify a custom output directory for reports and logs
  python slskd-spotify.py --output-dir results/may2025
  
  # Run with debug logging
  python slskd-spotify.py --debug
  
  # Use direct API mode (bypass client library)
  python slskd-spotify.py --direct-api
  
  # Generate reports from previous run
  python slskd-spotify.py --gen-report
  
  # Use exact matching for tracks
  python slskd-spotify.py --exact-match
  
  # Set a queue limit of 50 items per user
  python slskd-spotify.py --queue-limit 50
"""

import csv
import time
import os
import sys
import logging
import requests
import json
import inspect
import argparse
from collections import namedtuple
from datetime import datetime
import pickle
import asyncio
import concurrent.futures
from asyncio import TimeoutError
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Tuple, Optional, Any, Union, Set, Callable, Awaitable
import re
import unicodedata

import slskd_api

# ======== Filename Sanitization ========
def detect_encoding(file_path: str) -> str:
    """
    Detect the encoding of a file by trying multiple encodings.
    
    Args:
        file_path: Path to the file to detect encoding for
        
    Returns:
        The detected encoding or 'utf-8' as fallback
    """
    # Prioritize UTF-8 variants and Unicode-aware encodings
    # Note: utf-8-sig handles BOM automatically, utf-16 variants require BOM
    encodings = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252', 'iso-8859-1', 'utf-16', 'utf-16-le', 'utf-16-be']
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', newline='', encoding=encoding) as f:
                # Try to read a small sample to test the encoding
                sample = f.read(1024)
                # If we get here, the encoding worked
                logger.info(f"Detected encoding for {file_path}: {encoding}")
                return encoding
        except UnicodeDecodeError:
            logger.debug(f"Failed to decode {file_path} with {encoding} encoding")
            continue
        except Exception as e:
            logger.warning(f"Error testing {encoding} encoding for {file_path}: {e}")
            continue
    
    logger.warning(f"Could not detect encoding for {file_path}, using utf-8 as fallback")
    return 'utf-8'

def sanitize_filename(name: str, replacement: str = " ") -> str:
    """
    Sanitize a filename by replacing forbidden characters with spaces.
    Preserves Unicode characters like accented letters, Japanese, Chinese, etc.
    
    Args:
        name: The original filename/string to sanitize
        replacement: Character to replace forbidden characters with (default: space)
        
    Returns:
        Sanitized filename safe for all operating systems while preserving Unicode
    """
    if not name:
        return ""
    
    # Normalize Unicode to handle weird invisible characters and ensure consistency
    # NFKC: Normalization Form Compatibility Composition
    # This combines characters and ensures consistent representation
    name = unicodedata.normalize("NFKC", name)
    
    # Remove control characters (ASCII 0-31) but preserve Unicode control chars
    # Only remove ASCII control chars, not Unicode ones
    name = re.sub(r'[\x00-\x1f\x7f]', '', name)
    
    # Replace forbidden characters with replacement
    # Windows: \ / : * ? " < > |
    # Unix/Linux: / (null byte is already handled above)
    # Note: We only replace filesystem-forbidden chars, not Unicode chars
    forbidden = r'[\/:*?"<>|]'
    name = re.sub(forbidden, replacement, name)
    
    # Remove trailing spaces and dots (Windows doesn't like these)
    name = name.rstrip(" .")
    
    # Collapse multiple consecutive spaces into single space
    name = re.sub(r'\s+', ' ', name)
    
    # Ensure the name isn't empty after sanitization
    if not name.strip():
        return "unnamed"
    
    return name.strip()

# ======== Logging Setup ========
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_filename = os.path.join(log_dir, f"slskd_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
# Initialize logger without handlers (these will be added by setup_logging)
logger = logging.getLogger(__name__)

# Setup log rotation (keep 10 log files of 10MB each)
def setup_logging(log_level=logging.INFO):
    """Setup logging with proper configuration and log rotation"""
    global logger
    
    # Remove existing handlers to prevent duplicate output
    for handler in logger.handlers[:]:
        handler.close()  # Properly close the handler
        logger.removeHandler(handler)
        
    # Create handlers with proper formatting
    log_format = '%(asctime)s %(levelname)s: %(message)s'
    formatter = logging.Formatter(log_format)
    
    # File handler with rotation
    file_handler = RotatingFileHandler(
        log_filename, 
        maxBytes=10*1024*1024,  # 10MB
        backupCount=10          # Keep 10 files
    )
    file_handler.setFormatter(formatter)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    # Configure logger
    logger.setLevel(log_level)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # Prevent propagation to root logger
    logger.propagate = False
    
    # Suppress verbose logging from libraries
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    
    logger.info(f"Logging initialized at level: {logging.getLevelName(log_level)}")
    logger.info(f"Log file: {log_filename}")

# ======== Configuration ========
HOST = "http://localhost:5030"          # Base URL of your SLSKD server
API_PATH = "/api/v0"                    # API prefix path (used by direct requests)
# Load API key from environment variable for security, fall back to hardcoded value
API_KEY = os.environ.get("SLSKD_API_KEY", "your-api-key-here")  # Your SLSKD API key
CSV_FILE = "to_queue.csv"               # CSV should contain columns: artist, album, [track]
QUEUE_LIMIT = 0                         # Maximum items in queue per user (0 for no limit)

# ======== Advanced Configuration ========
RATE_LIMIT_DELAY = 1.0                  # Seconds to wait between API calls
BATCH_SIZE = 10                         # Number of rows to process in each batch
MAX_RETRIES = 3                         # Maximum retry attempts for failed operations
SEARCH_TIMEOUT = 60                     # Timeout in seconds for search operations
ENQUEUE_TIMEOUT = 30                    # Timeout in seconds for enqueue operations
CHECKPOINT_FILE = "checkpoint.pkl"      # File to save progress for resuming
EXCLUDED_EXTENSIONS = ['.lrc']          # File extensions to exclude entirely
CIRCUIT_BREAKER_THRESHOLD = 5           # Number of consecutive errors before circuit breaks
CIRCUIT_BREAKER_TIMEOUT = 300           # Seconds to wait after circuit breaks before retrying
USE_DIRECT_API = False                  # Whether to use direct API calls instead of client library
EXACT_MATCH = False                     # Whether to require exact matching for track names

# Strict prioritization for audio formats - only these formats will be considered
ALLOWED_FORMATS = ['.mp3', '.m4a', '.flac']  # In order of priority

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

# For stats tracking
class Stats:
    def __init__(self):
        self.total_processed = 0
        self.successful = 0
        self.failed = 0
        self.enqueued_files = 0
        self.start_time = time.time()
        self.last_update_time = time.time()
        self.files_per_minute = 0
        self.estimated_completion = None
        
    def to_dict(self):
        return {
            'total_processed': self.total_processed,
            'successful': self.successful,
            'failed': self.failed,
            'enqueued_files': self.enqueued_files,
            'success_rate': f"{(self.successful / max(1, self.total_processed)) * 100:.1f}%",
            'elapsed_time': self.format_time(time.time() - self.start_time),
            'files_per_minute': self.files_per_minute,
            'estimated_completion': self.estimated_completion
        }
    
    def format_time(self, seconds):
        """Format seconds into a human-readable time string."""
        hours, remainder = divmod(int(seconds), 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    
    def update_metrics(self, total_remaining):
        """Update performance metrics like files/minute and ETA."""
        current_time = time.time()
        elapsed = current_time - self.last_update_time
        
        # Only update metrics every 30 seconds for stability
        if elapsed < 30:
            return
            
        # Calculate files per minute
        if self.total_processed > 0 and elapsed > 0:
            processed_since_last = self.total_processed - getattr(self, 'last_total_processed', 0)
            self.files_per_minute = (processed_since_last / elapsed) * 60
            
            # Estimate completion time
            if self.files_per_minute > 0 and total_remaining > 0:
                minutes_remaining = total_remaining / self.files_per_minute
                eta_timestamp = current_time + (minutes_remaining * 60)
                self.estimated_completion = datetime.fromtimestamp(eta_timestamp).strftime('%Y-%m-%d %H:%M:%S')
            
        # Store for next update
        self.last_update_time = current_time
        self.last_total_processed = self.total_processed
        
    def print_progress(self, total_remaining):
        """Print progress information with metrics."""
        self.update_metrics(total_remaining)
        
        logger.info("\n" + "-"*50)
        logger.info(f"PROGRESS REPORT:")
        logger.info(f"Processed: {self.total_processed} ({self.successful} successful, {self.failed} failed)")
        logger.info(f"Success rate: {(self.successful / max(1, self.total_processed)) * 100:.1f}%")
        logger.info(f"Files enqueued: {self.enqueued_files}")
        logger.info(f"Elapsed time: {self.format_time(time.time() - self.start_time)}")
        
        if self.files_per_minute > 0:
            logger.info(f"Processing speed: {self.files_per_minute:.1f} files/minute")
            
        if self.estimated_completion:
            logger.info(f"Estimated completion: {self.estimated_completion}")
            
        logger.info("-"*50)

# Global stats object
stats = Stats()
results_log = []  # To track detailed results for reporting
queued_files_tracker = []  # Global list to track all queued files for status checking
status_monitor_task = None  # Global reference to the status monitoring task

def debug_enqueue_direct_api(username, file_id):
    """Try to enqueue a file directly using the REST API instead of the client library."""
    url = f"{HOST.rstrip('/')}{API_PATH}/transfers/downloads/{username}"
    logger.info(f"DEBUG: Sending direct API request to: {url}")
    
    # Try different payload formats
    # Format 1: Array of IDs
    payload1 = [file_id]
    
    # Format 2: Object with SearchResultIds property (array)
    payload2 = {"SearchResultIds": [file_id], "Source": SOURCE}
    
    # Format 3: QueueDownloadRequest format
    payload3 = {"Username": username, "SearchResultIds": [file_id], "Source": SOURCE}

    payloads = [payload1, payload2, payload3]
    
    for i, payload in enumerate(payloads, 1):
        try:
            logger.info(f"DEBUG: Trying payload format {i}: {json.dumps(payload)}")
            resp = requests.post(url, json=payload, headers=HEADERS)
            if resp.status_code == 200:
                logger.info(f"DEBUG: Success with payload format {i}")
                return True
            else:
                logger.error(f"DEBUG: Failed with payload format {i}: {resp.status_code} {resp.reason}")
                if resp.text:
                    logger.error(f"DEBUG: Response: {resp.text}")
        except Exception as e:
            logger.error(f"DEBUG: Exception with payload format {i}: {e}")
    
    logger.error("DEBUG: All payload formats failed")
    return False


async def poll_responses_async(job_id):
    """Poll until the given search job has responses, then return them."""
    base = f"{HOST.rstrip('/')}{API_PATH}/searches/{job_id}/responses"
    
    # Strategy: Continue polling longer to get more complete results
    # First, poll quickly until we get any results (POLL_INTERVAL)
    # Then, continue polling at a slower rate (EXTENDED_POLL_INTERVAL) to get more results
    got_initial_results = False
    initial_results = []
    extended_poll_count = 0
    EXTENDED_POLL_INTERVAL = 3.0  # Additional seconds to wait after initial results
    MAX_EXTENDED_POLLS = 5  # Additional polls after finding initial results
    
    for poll_attempt in range(MAX_POLLS + MAX_EXTENDED_POLLS):
        try:
            # If we have initial results, use extended poll interval
            if got_initial_results:
                await asyncio.sleep(EXTENDED_POLL_INTERVAL)
                extended_poll_count += 1
                if extended_poll_count >= MAX_EXTENDED_POLLS:
                    # Comment out debug output
                    # logger.info(f"Completed extended polling, collected {len(initial_results)} responses")
                    # We've done our extended polling, return all results
                    return initial_results
            else:
                await asyncio.sleep(POLL_INTERVAL)
            
            with concurrent.futures.ThreadPoolExecutor() as pool:
                resp = await asyncio.get_event_loop().run_in_executor(
                    pool, lambda: requests.get(base, headers=HEADERS)
                )
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict):
                results = data.get('responses') or data.get('Responses') or []
            else:
                results = data
                
            if results:
                if not got_initial_results:
                    logger.info(f"Got initial results ({len(results)} responses), continuing to poll for more complete results...")
                    got_initial_results = True
                    initial_results = results
                else:
                    # Add new results
                    found_new = False
                    for new_result in results:
                        if not any(r.get('username') == new_result.get('username') for r in initial_results):
                            initial_results.append(new_result)
                            found_new = True
                    
                    if found_new:
                        # Comment out debug output
                        # logger.info(f"Extended polling found additional results (now {len(initial_results)} responses)")
                        pass
                    
                # Check if first result has files with the expected format
                if results and len(results) > 0 and 'files' in results[0]:
                    first_file = results[0]['files'][0] if results[0]['files'] else None
                    if first_file:
                        # Comment out debug output
                        # logger.debug(f"Example file structure: {first_file}")
                        pass
        except Exception as e:
            logger.warning(f"Error during polling: {e}")
    
    # Return what we've got, even if incomplete
    return initial_results if got_initial_results else []


async def search_slskd_async(pattern):
    """Run a search with timeout and retry logic."""
    post_url = f"{HOST.rstrip('/')}{API_PATH}/searches"
    logger.info(f"⬢ POST {post_url} payload: {{'SearchText': '{pattern}'}}")
    
    # Circuit breaker pattern to prevent excessive retries when service is down
    global consecutive_errors
    if hasattr(search_slskd_async, 'circuit_open') and search_slskd_async.circuit_open:
        current_time = time.time()
        if current_time - search_slskd_async.circuit_open_time < CIRCUIT_BREAKER_TIMEOUT:
            logger.warning("Circuit breaker open, skipping API call")
            return []
        else:
            # Reset circuit breaker after timeout
            logger.info("Circuit breaker timeout expired, resetting")
            search_slskd_async.circuit_open = False
            search_slskd_async.consecutive_errors = 0
    
    retries = 0
    while retries <= MAX_RETRIES:
        try:
            # Use timeout for the search request
            with concurrent.futures.ThreadPoolExecutor() as pool:
                resp = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(
                        pool, lambda: requests.post(post_url, json={"SearchText": pattern}, headers=HEADERS)
                    ),
                    timeout=SEARCH_TIMEOUT
                )
            resp.raise_for_status()
            data = resp.json()
            job_id = data.get('id') or data.get('SearchId')
            if not job_id:
                raise RuntimeError(f"No job ID returned by POST: {data}")
            
            # Reset circuit breaker on success
            if hasattr(search_slskd_async, 'consecutive_errors'):
                search_slskd_async.consecutive_errors = 0
            
            # Apply rate limiting
            await asyncio.sleep(RATE_LIMIT_DELAY)
            
            # Poll for responses with timeout
            responses = await asyncio.wait_for(
                poll_responses_async(job_id),
                timeout=SEARCH_TIMEOUT
            )
            return responses
        
        except (TimeoutError, requests.exceptions.RequestException, RuntimeError) as e:
            logger.warning(f"Search operation error for '{pattern}': {e}. Retry {retries+1}/{MAX_RETRIES+1}")
            retries += 1
            
            # Update consecutive errors for circuit breaker
            if not hasattr(search_slskd_async, 'consecutive_errors'):
                search_slskd_async.consecutive_errors = 0
            search_slskd_async.consecutive_errors += 1
            
            # Check if circuit breaker threshold reached
            if search_slskd_async.consecutive_errors >= CIRCUIT_BREAKER_THRESHOLD:
                logger.error(f"Circuit breaker threshold reached after {CIRCUIT_BREAKER_THRESHOLD} consecutive errors")
                search_slskd_async.circuit_open = True
                search_slskd_async.circuit_open_time = time.time()
                return []
            
            if retries > MAX_RETRIES:
                logger.error(f"Max retries reached for '{pattern}'")
                return []
                
            # Exponential backoff
            await asyncio.sleep(RATE_LIMIT_DELAY * (2 ** retries))
    
    return []


def select_best(responses: List[Dict[str, Any]], album_name: str) -> Optional[namedtuple]:
    """
    From search responses, pick best match by filename or largest size.
    
    Args:
        responses: List of response objects from search API
        album_name: Album name to match in filenames
        
    Returns:
        namedtuple with username, result_id and filename, or None if no matches
    """
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


def initialize_slskd_client() -> slskd_api.SlskdClient:
    """
    Initialize and return a properly configured SLSKD API client.
    
    Returns:
        A configured SlskdClient instance ready for API calls
    """
    return slskd_api.SlskdClient(host=HOST, api_key=API_KEY)


async def enqueue_files_async(client: slskd_api.SlskdClient, 
                              username: str, 
                              search_results: List[Dict[str, Any]]) -> int:
    """
    Enqueue one or more files via slskd_api client.
    
    Args:
        client: The SLSKD client
        username: Username to download from
        search_results: List of dicts containing file info with at least 'filename' and 'size'
        
    Returns:
        Number of files successfully queued
        
    Raises:
        TimeoutError: If the enqueue operation times out
        Exception: For any other errors during enqueuing
    """
    try:
        logger.info(f"Enqueueing files from {username}: {len(search_results)} file(s)")
        
        # The slskd_api.transfers.enqueue method expects search results with specific format
        # Important: Extract the file IDs (previously successful approach)
        file_ids = []
        for file_info in search_results:
            if 'id' in file_info:
                file_ids.append(file_info['id'])
            elif 'file_id' in file_info:
                file_ids.append(file_info['file_id'])
        
        if not file_ids:
            # If no IDs found, try the original approach with full file info
            file_info_list = search_results
            logger.info(f"Sending full file info: {len(file_info_list)} files")
        else:
            # Use file IDs only (previously working approach)
            file_info_list = file_ids
            logger.info(f"Sending file IDs: {file_ids}")
        
        # Use direct API calls if specified or fall back to it on failure
        if USE_DIRECT_API or True:  # Always try direct API first
            try:
                post_url = f"{HOST.rstrip('/')}{API_PATH}/transfers/downloads/{username}"
                logger.debug(f"Direct API call to: {post_url}")
                logger.debug(f"Payload: {json.dumps(file_info_list)}")
                
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    resp = await asyncio.wait_for(
                        asyncio.get_event_loop().run_in_executor(
                            pool, lambda: requests.post(post_url, json=file_info_list, headers=HEADERS)
                        ),
                        timeout=ENQUEUE_TIMEOUT
                    )
                resp.raise_for_status()
                
                logger.info(f"Successfully enqueued {len(file_info_list)} file(s) for {username} using direct API")
                
                # Track the files we've queued for status checking
                for file_info in search_results:
                    filename = file_info.get('filename')
                    if filename:
                        # Store more metadata about the download to help with matching later
                        now = datetime.now().isoformat()
                        tracked_file = {
                            'username': username,
                            'filename': filename,
                            'basename': os.path.basename(filename),
                            'queued_at': now,
                            'download_status': 'queued',  # Initial status
                            'last_checked': now,
                            'size': file_info.get('size', 0)
                        }
                        
                        # Add search terms that can help identify the file later
                        if hasattr(file_info, '_search_terms'):
                            tracked_file['search_terms'] = file_info._search_terms
                        
                        queued_files_tracker.append(tracked_file)
                
                stats.enqueued_files += len(file_info_list)
                return len(file_info_list)
            except Exception as e:
                if USE_DIRECT_API:  # If direct API was explicitly requested, don't fall back
                    raise
                logger.warning(f"Direct API enqueue failed, trying library method: {e}")
        
        # Only reach here if direct API failed and USE_DIRECT_API is False
        with concurrent.futures.ThreadPoolExecutor() as pool:
            await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(
                    pool, lambda: client.transfers.enqueue(username, file_info_list)
                ),
                timeout=ENQUEUE_TIMEOUT
            )
        
        # Track the files we've queued for status checking (client library method)
        for file_info in search_results:
            filename = file_info.get('filename')
            if filename:
                # Store more metadata about the download to help with matching later
                now = datetime.now().isoformat()
                queued_files_tracker.append({
                    'username': username,
                    'filename': filename,
                    'basename': os.path.basename(filename),
                    'queued_at': now,
                    'download_status': 'queued',  # Initial status
                    'last_checked': now,
                    'size': file_info.get('size', 0)
                })
        
        logger.info(f"Successfully enqueued {len(file_info_list)} file(s) for {username} using client library")
        stats.enqueued_files += len(file_info_list)
        return len(file_info_list)
    
    except TimeoutError:
        logger.error(f"Enqueue operation timed out for {username}")
        return 0
    except Exception as e:
        logger.error(f"Client enqueue failed for {username}: {e}")
        logger.error(f"Failed payload: {search_results}")
        return 0


def save_checkpoint(row_index: int, total_rows: int) -> None:
    """
    Save progress to a checkpoint file for resuming later.
    
    Args:
        row_index: Current row index being processed
        total_rows: Total number of rows in the CSV
    """
    checkpoint_data = {
        'row_index': row_index,
        'total_rows': total_rows,
        'timestamp': datetime.now().isoformat(),
        'stats': stats.to_dict(),
        'results_log': results_log
    }
    
    try:
        with open(CHECKPOINT_FILE, 'wb') as f:
            pickle.dump(checkpoint_data, f)
        logger.info(f"Checkpoint saved at row {row_index}/{total_rows}")
    except Exception as e:
        logger.error(f"Error saving checkpoint: {e}")


def load_checkpoint() -> Optional[Dict[str, Any]]:
    """
    Load progress from a checkpoint file.
    
    Returns:
        Dictionary with checkpoint data or None if no checkpoint exists
    """
    try:
        if os.path.exists(CHECKPOINT_FILE):
            with open(CHECKPOINT_FILE, 'rb') as f:
                checkpoint_data = pickle.load(f)
            logger.info(f"Checkpoint loaded: row {checkpoint_data['row_index']}/{checkpoint_data['total_rows']}")
            return checkpoint_data
        else:
            logger.info("No checkpoint file found")
            return None
    except Exception as e:
        logger.error(f"Error loading checkpoint: {e}")
        return None


def generate_report():
    """Generate a CSV report of all processed entries that mirrors the input CSV format with added status columns."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = os.path.join(log_dir, f"results_{timestamp}.csv")
    input_csv_basename = os.path.basename(CSV_FILE)
    
    try:
        if not results_log:
            logger.warning("No results to report. Report will not be generated.")
            return

        # Create absolute paths for the report files
        report_file_abs = os.path.abspath(report_file)
        
        # Read the original headers from the input CSV
        original_fieldnames = []
        
        # Try multiple encodings for reading the headers
        # Prioritize UTF-8 variants and Unicode-aware encodings
        # Note: utf-8-sig handles BOM automatically, utf-16 variants require BOM
        encodings = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252', 'iso-8859-1', 'utf-16', 'utf-16-le', 'utf-16-be']
        for encoding in encodings:
            try:
                with open(CSV_FILE, 'r', newline='', encoding=encoding) as f:
                    reader = csv.reader(f)
                    original_fieldnames = next(reader)  # Get header row
                logger.info(f"Successfully read CSV headers with {encoding} encoding")
                break
            except UnicodeDecodeError as e:
                if 'utf-16' in encoding and 'BOM' in str(e):
                    logger.debug(f"Skipping {encoding} - file doesn't have BOM")
                else:
                    logger.warning(f"Failed to decode CSV headers with {encoding} encoding, trying next...")
            except Exception as e:
                logger.warning(f"Could not read headers from input CSV with {encoding} encoding: {e}")
        
        if not original_fieldnames:
            # Default headers if we can't read the original
            logger.warning("Using default headers as original headers couldn't be read")
            original_fieldnames = ['artist', 'album', 'track']
        
        # Output headers: original headers + status columns
        output_fieldnames = original_fieldnames + ['status', 'message', 'files_queued', 'timestamp']
        
        # Add fallback tracking columns
        output_fieldnames.extend([
            'attempts',
            'fallback_used',
            'candidate_used',
            'candidate_format',
            'candidate_cleanliness',
            'candidate_username',
            'user_attempt',
            'user_selected'
        ])
            
        # Ensure log directory exists
        os.makedirs(log_dir, exist_ok=True)
        
        # Write to the new report file - always use UTF-8 for output
        with open(report_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=output_fieldnames)
            writer.writeheader()
            
            # Write each result row
            for result in results_log:
                # Ensure we have all original fields (even if empty)
                row = {field: result.get(field, '') for field in original_fieldnames}
                # Add the status fields
                row.update({
                    'status': result.get('status', ''),
                    'message': result.get('message', ''),
                    'files_queued': result.get('files_queued', 0),
                    'timestamp': result.get('timestamp', '')
                })
                
                # Add fallback tracking fields
                for field in ['attempts', 'fallback_used', 'candidate_used', 'candidate_format', 
                             'candidate_cleanliness', 'candidate_username', 'user_attempt', 'user_selected']:
                    row[field] = result.get(field, '')
                
                writer.writerow(row)
        
        # Also create a copy with the same filename pattern as the input but with a _results suffix
        base, ext = os.path.splitext(input_csv_basename)
        matching_report = os.path.join(log_dir, f"{base}_results{ext}")
        matching_report_abs = os.path.abspath(matching_report)
        
        import shutil
        shutil.copy2(report_file, matching_report)
        
        logger.info(f"CSV Reports generated:")
        logger.info(f"  1. Timestamped report: {report_file_abs}")
        logger.info(f"  2. Input-matched report: {matching_report_abs}")
        
        # Print a direct terminal message too for visibility
        print(f"\nCSV Reports generated:")
        print(f"  1. Timestamped report: {report_file_abs}")
        print(f"  2. Input-matched report: {matching_report_abs}\n")
        
        return report_file_abs, matching_report_abs
    except Exception as e:
        logger.error(f"Error generating report: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None, None


# Create a separate function to generate reports on demand
def generate_report_on_demand():
    """Generate a report explicitly when called, even outside the normal process flow."""
    logger.info("Generating CSV report on demand...")
    if not results_log:
        logger.warning("No results to report. No rows have been processed yet.")
        return
        
    report_paths = generate_report()
    if report_paths:
        logger.info("Report generation completed successfully.")
    else:
        logger.error("Failed to generate reports.")


def count_csv_rows(file_path):
    """Count the total number of rows in the CSV file."""
    # Try multiple encodings in order of likelihood
    # Prioritize UTF-8 variants and Unicode-aware encodings
    # Note: utf-8-sig handles BOM automatically, utf-16 variants require BOM
    encodings = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252', 'iso-8859-1', 'utf-16', 'utf-16-le', 'utf-16-be']
    
    for encoding in encodings:
        try:
            with open(file_path, newline='', encoding=encoding) as f:
                count = sum(1 for _ in csv.reader(f)) - 1  # Subtract header row
                logger.info(f"Successfully read CSV with {encoding} encoding")
                return count
        except UnicodeDecodeError as e:
            if 'utf-16' in encoding and 'BOM' in str(e):
                logger.debug(f"Skipping {encoding} - file doesn't have BOM")
            else:
                logger.warning(f"Failed to decode CSV with {encoding} encoding, trying next...")
        except Exception as e:
            logger.error(f"Error counting CSV rows: {e}")
            return 0
    
    # If all encodings fail
    logger.error(f"Could not read CSV file with any encoding: {file_path}")
    return 0


def file_has_excluded_extension(filename, excluded_extensions):
    """Check if a file has one of the excluded extensions."""
    if not filename:
        return False
    return any(filename.lower().endswith(ext.lower()) for ext in excluded_extensions)


def get_priority_group(filename):
    """
    Get the priority group for a file based on its extension.
    
    Returns:
        0-2: Position in ALLOWED_FORMATS list (lower is better)
        999: Not in allowed formats (will be skipped)
    """
    if not filename:
        return 999
        
    # Skip excluded extensions first
    if file_has_excluded_extension(filename, EXCLUDED_EXTENSIONS):
        return 999
        
    # Check for allowed formats in priority order
    for i, ext in enumerate(ALLOWED_FORMATS):
        if filename.lower().endswith(ext.lower()):
            return i
            
    # Not an allowed format
    return 999


def debug_print_file_priorities(files, log_all=False):
    """Print file priorities for debugging."""
    if not files:
        # Comment out debug output
        # logger.debug("No files to prioritize")
        return
        
    priorities = {}
    for f in files:
        filename = f.get('filename') or ''
        priority = get_priority_group(filename)
        if priority not in priorities:
            priorities[priority] = []
        priorities[priority].append(filename)
    
    for p in sorted(priorities.keys()):
        num_files = len(priorities[p])
        if p < len(ALLOWED_FORMATS) and p >= 0:
            priority_name = f"{ALLOWED_FORMATS[p]} files"
        elif p == 999:
            priority_name = "Ignored format"
        else:
            priority_name = "Unknown"
            
        if num_files > 0:  # Only log if we have files in this category
            if log_all:
                # Comment out debug output
                # logger.debug(f"{priority_name} ({num_files}): {priorities[p]}")
                pass
            else:
                # Just log the first few filenames to avoid cluttering the logs
                sample = priorities[p][:5]
                # Comment out debug output
                # logger.debug(f"{priority_name} ({num_files}), examples: {sample}")
                pass


def is_exact_match(filename, artist, album, track):
    """
    Check for exact match of artist, album, and track in filename.
    
    Args:
        filename (str): The filename to check
        artist (str): Artist name to match
        album (str): Album name to match
        track (str): Track name to match
        
    Returns:
        bool: True if filename matches the pattern exactly, False otherwise
    """
    if not filename:
        return False
        
    # Normalize strings for comparison
    filename_lower = filename.lower()
    artist_lower = artist.lower()
    album_lower = album.lower()
    track_lower = track.lower()
    
    # Most common pattern: "Artist - Album - Track"
    expected_pattern = f"{artist_lower} - {album_lower} - {track_lower}"
    
    # Check for exact match (allowing for extension and path)
    basename = os.path.basename(filename_lower)
    file_without_ext = os.path.splitext(basename)[0]
    
    # Exact match
    if file_without_ext == expected_pattern:
        return True
        
    # Almost exact match (might have different spacing or punctuation)
    # Replace multiple spaces with single space and remove common punctuation
    def normalize(s):
        s = s.replace('_', ' ').replace('.', ' ').replace('-', ' ')
        return ' '.join(s.split())
    
    norm_file = normalize(file_without_ext)
    norm_pattern = normalize(expected_pattern)
    
    return norm_file == norm_pattern


def score_filename_cleanliness(filename, artist, album, track):
    """
    Score a filename based on how well it matches standard naming patterns.
    Higher scores indicate cleaner, more matching filenames.
    
    Args:
        filename (str): The filename to score
        artist (str): Artist name to match
        album (str): Album name to match
        track (str): Track name to match
        
    Returns:
        float: A score from 0.0 (poor) to 10.0 (perfect match)
    """
    if not filename:
        return 0.0
    
    # Start with a base score
    score = 5.0
    
    # Get basename and remove extension
    basename = os.path.basename(filename)
    name_without_ext = os.path.splitext(basename)[0]
    
    # Convert everything to lowercase for comparison
    name_lower = name_without_ext.lower()
    artist_lower = artist.lower()
    album_lower = album.lower()
    track_lower = track.lower()
    
    # Check for exact matches of artist, album, and track (case insensitive)
    # Full perfect match "Artist - Album - Track"
    perfect_pattern = f"{artist_lower} - {album_lower} - {track_lower}"
    if name_lower == perfect_pattern:
        score = 10.0  # Perfect score for perfect match
        return score
    
    # Recognize track numbers in various formats
    track_number_pattern = r'^\d{1,2}[-.\s]+'  # Matches "01 - ", "01. ", "01 ", etc.
    track_number_match = re.search(track_number_pattern, name_lower)
    if track_number_match:
        score += 0.5  # Small bonus for having a track number prefix
    
    # Standard album filename patterns (with track number variations)
    # Escape artist, album and track names for regex safety
    safe_artist = re.escape(artist_lower)
    safe_album = re.escape(album_lower)
    safe_track = re.escape(track_lower)
    
    album_patterns = [
        rf"{safe_artist} - {safe_album} - \d+[-.\s]+{safe_track}",  # Artist - Album - 01 - Track
        rf"{safe_artist} - {safe_album} - {safe_track}",           # Artist - Album - Track
        rf"\d+[-.\s]+{safe_artist} - {safe_track}",                # 01 - Artist - Track
        rf"{safe_artist} - \d+[-.\s]+{safe_track}",                # Artist - 01 - Track
    ]
    
    for pattern in album_patterns:
        if re.search(pattern, name_lower):
            score += 1.5
            break
    
    # Check if all required components are present (regardless of order)
    if artist_lower in name_lower:
        score += 1.0
    else:
        score -= 2.0  # Significant penalty for missing artist
        
    if track_lower in name_lower:
        score += 1.0
    else:
        score -= 3.0  # Major penalty for missing track name
        
    if album_lower in name_lower:
        score += 0.5
    
    # Penalties for suspicious content
    suspicious_terms = ["remix", "mix", "edit", "live", "cover", "instrumental", 
                        "acoustic", "demo", "alternate", "dj", "radio edit", 
                        "extended", "version"]
    for term in suspicious_terms:
        if term in name_lower:
            score -= 1.5
    
    # Penalty for extremely long filenames (often indicate special versions)
    if len(name_without_ext) > 60:
        score -= 0.5
    
    # Ensure score stays within bounds
    return max(0.0, min(score, 10.0))


async def process_row(client, row, row_index, total_rows):
    """Process a single row from the CSV."""
    # Copy all fields from the original CSV row to preserve structure
    result_entry = {key: value for key, value in row.items()}
    
    # Add status fields
    result_entry.update({
        'status': 'skipped',
        'message': '',
        'files_queued': 0,
        'timestamp': datetime.now().isoformat(),
        'attempts': 0,
        'fallback_used': False
    })
    
    # Get raw values from CSV
    raw_artist = (row.get('artist') or '').strip()
    raw_album = (row.get('album') or '').strip()
    raw_track = (row.get('track') or '').strip()
    
    # Log raw values for debugging Unicode issues
    if any(ord(c) > 127 for c in raw_artist + raw_album + raw_track):
        logger.info(f"Found Unicode characters in row {row_index}:")
        logger.info(f"  Raw artist: {repr(raw_artist)}")
        logger.info(f"  Raw album: {repr(raw_album)}")
        logger.info(f"  Raw track: {repr(raw_track)}")
    
    # Sanitize artist, album, and track names to handle special characters
    artist = sanitize_filename(raw_artist)
    album = sanitize_filename(raw_album)
    track = sanitize_filename(raw_track)
    
    # Log sanitized values for debugging
    if any(ord(c) > 127 for c in artist + album + track):
        logger.info(f"After sanitization:")
        logger.info(f"  Artist: {repr(artist)}")
        logger.info(f"  Album: {repr(album)}")
        logger.info(f"  Track: {repr(track)}")
    
    if not artist or not album:
        result_entry['message'] = "Incomplete row"
        results_log.append(result_entry)
        logger.warning(f"Skipping incomplete row: {row}")
        return False

    pattern = f"{artist} - {album}" + (f" - {track}" if track else '')
    logger.info(f"🔍 Searching for: {pattern} ({row_index}/{total_rows})")
    
    try:
        resp_list = await search_slskd_async(pattern)
        
        if not resp_list:
            result_entry['status'] = 'failed'
            result_entry['message'] = "No results found"
            results_log.append(result_entry)
            logger.info(f"No results for: {pattern}")
            return False
        
        files_queued = 0
        
        # Get ranked list of all candidates
        ranked_candidates = rank_all_results(resp_list, artist, album, track)
        if not ranked_candidates:
            result_entry['status'] = 'failed'
            result_entry['message'] = "No valid candidates found"
            results_log.append(result_entry)
            logger.info(f"No valid candidates found for: {pattern}")
            return False
            
        if not track:  # Album download case
            # Group all tracks from the same best user
            best_username = ranked_candidates[0]['username']
            album_tracks = [c for c in ranked_candidates if c['username'] == best_username]
            
            # Check queue availability
            has_open_slot, queue_count = await check_queue_status(best_username)
            if has_open_slot:
                # Queue all tracks from this user
                files_to_queue = [track['file_info'] for track in album_tracks]
                files_queued = await enqueue_files_async(client, best_username, files_to_queue)
            else:
                result_entry['status'] = 'failed'
                result_entry['message'] = f"Queue full for user {best_username}"
                results_log.append(result_entry)
                return False
        else:  # Single track case - existing logic
            best_candidate = await find_best_available_candidate(ranked_candidates)
            if not best_candidate:
                result_entry['status'] = 'failed'
                result_entry['message'] = "No candidates with open queue slots found"
                results_log.append(result_entry)
                logger.info(f"No candidates with open queue slots for: {pattern}")
                return False
            
            files_queued = await enqueue_files_async(client, best_candidate['username'], [best_candidate['file_info']])
        
        if files_queued > 0:
            result_entry['status'] = 'success'
            result_entry['files_queued'] = files_queued
            result_entry['message'] = f"Successfully queued {files_queued} file(s)"
            results_log.append(result_entry)
            return True
        else:
            result_entry['status'] = 'failed'
            result_entry['message'] = "Failed to queue file"
            results_log.append(result_entry)
            return False
            
    except Exception as e:
        result_entry['status'] = 'error'
        result_entry['message'] = f"Error: {str(e)}"
        results_log.append(result_entry)
        logger.error(f"Error processing '{pattern}': {e}")
        return False


def find_most_recent_results_csv():
    """Find the most recent results CSV file in the log directory."""
    try:
        result_files = []
        for filename in os.listdir(log_dir):
            if filename.endswith(".csv") and ("results_" in filename or "_results" in filename):
                file_path = os.path.join(log_dir, filename)
                result_files.append((file_path, os.path.getmtime(file_path)))
        
        if not result_files:
            logger.error(f"No results CSV files found in {log_dir}")
            return None
            
        # Sort by modification time (newest first)
        result_files.sort(key=lambda x: x[1], reverse=True)
        newest_file = result_files[0][0]
        logger.info(f"Found most recent results file: {newest_file}")
        return newest_file
    except Exception as e:
        logger.error(f"Error finding most recent results CSV: {e}")
        return None


def load_failed_rows(results_csv):
    """Load rows that failed in the previous run from a results CSV file."""
    failed_rows = []
    
    # Try multiple encodings
    # Prioritize UTF-8 variants and Unicode-aware encodings
    # Note: utf-16 variants require BOM, so we'll try them last
    encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'iso-8859-1', 'utf-16', 'utf-16-le', 'utf-16-be']
    success = False
    
    for encoding in encodings:
        try:
            with open(results_csv, 'r', newline='', encoding=encoding) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Get the status from the row
                    status = row.get('status', '').lower()
                    
                    # If status is failed or error, add this row to retry
                    if status in ('failed', 'error'):
                        # Create a new row without the status columns
                        clean_row = {k: v for k, v in row.items() 
                                    if k not in ('status', 'message', 'files_queued', 'timestamp')}
                        failed_rows.append(clean_row)
            
            # If we reached here without exception, we succeeded
            success = True
            logger.info(f"Successfully read results CSV with {encoding} encoding")
            break
        except UnicodeDecodeError as e:
            if 'utf-16' in encoding and 'BOM' in str(e):
                logger.debug(f"Skipping {encoding} - file doesn't have BOM")
            else:
                logger.warning(f"Failed to decode results CSV with {encoding} encoding, trying next...")
        except Exception as e:
            logger.error(f"Error loading failed rows with {encoding} encoding: {e}")
    
    if not success and not failed_rows:
        logger.error(f"Could not read results CSV with any encoding: {results_csv}")
    
    logger.info(f"Loaded {len(failed_rows)} failed rows for retry")
    return failed_rows


async def process_csv(csv_file, start_row=0, retry_failed=False):
    """Process the CSV file in batches with error handling and resumability.
    
    Args:
        csv_file (str): Path to the CSV file to process
        start_row (int): Row to start processing from (for resume)
        retry_failed (bool): Whether to only process rows that failed in previous runs
    """
    if not os.path.isfile(csv_file):
        logger.error(f"CSV file not found: {csv_file}")
        sys.exit(1)
    
    client = initialize_slskd_client()
    
    # Handle retry-failed mode
    rows_to_process = []
    
    if retry_failed:
        # Find most recent results CSV
        results_csv = find_most_recent_results_csv()
        if not results_csv:
            logger.error("Cannot find results CSV file for retry-failed mode")
            sys.exit(1)
            
        # Load previously failed rows
        failed_rows = load_failed_rows(results_csv)
        if not failed_rows:
            logger.error("No failed rows found to retry")
            sys.exit(1)
            
        rows_to_process = failed_rows
        total_rows = len(rows_to_process)
        logger.info(f"Retry mode: Processing {total_rows} failed rows from previous run")
    else:
        # Regular processing mode
        total_rows = count_csv_rows(csv_file)
        if total_rows == 0:
            logger.error("CSV file is empty or couldn't be read")
            sys.exit(1)
            
        # For very large files, use a memory-efficient approach
        if total_rows > 1000:
            logger.info(f"Large file detected ({total_rows} rows). Using streaming mode.")
            # Try multiple encodings for large files
            # Prioritize UTF-8 variants and Unicode-aware encodings
            # Note: utf-16 variants require BOM, so we'll try them last
            encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'iso-8859-1', 'utf-16', 'utf-16-le', 'utf-16-be']
            success = False
            
            for encoding in encodings:
                try:
                    with open(csv_file, newline='', encoding=encoding) as f:
                        reader = csv.DictReader(f)
                        # Skip to start_row
                        for _ in range(start_row):
                            next(reader, None)
                        
                        # Process in batches
                        batch = []
                        batch_count = 0
                        for i, row in enumerate(reader, start=start_row):
                            batch.append(row)
                            
                            # When batch is full or we reach end, process it
                            if len(batch) >= BATCH_SIZE or i == total_rows - 1:
                                batch_count += 1
                                logger.info(f"Processing batch {batch_count}: rows {i-len(batch)+1} to {i} of {total_rows}")
                                
                                await process_batch(client, batch, i-len(batch)+1, total_rows)
                                
                                # Reset batch
                                batch = []
                                
                                # Save checkpoint
                                save_checkpoint(i, total_rows)
                    
                    # If we reached here without exception, we succeeded
                    success = True
                    logger.info(f"Successfully processed CSV with {encoding} encoding")
                    break
                except UnicodeDecodeError as e:
                    if 'utf-16' in encoding and 'BOM' in str(e):
                        logger.debug(f"Skipping {encoding} - file doesn't have BOM")
                    else:
                        logger.warning(f"Failed to decode CSV with {encoding} encoding, trying next...")
                except Exception as e:
                    logger.error(f"Error processing CSV with {encoding} encoding: {e}")
            
            if not success:
                logger.error("Failed to process CSV with any encoding")
                sys.exit(1)
            
            # Final report
            generate_report()
            return
        else:
            # For smaller files, load everything into memory
            # Try multiple encodings
            # Prioritize UTF-8 variants and Unicode-aware encodings
            # Note: utf-16 variants require BOM, so we'll try them last
            encodings = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252', 'iso-8859-1', 'utf-16', 'utf-16-le', 'utf-16-be']
            success = False
            
            for encoding in encodings:
                try:
                    with open(csv_file, newline='', encoding=encoding) as f:
                        reader = csv.DictReader(f)
                        all_rows = list(reader)  # Load all rows
                        
                        # Skip to the starting row (for resume)
                        rows_to_process = all_rows[start_row:]
                    
                    # If we reached here without exception, we succeeded
                    success = True
                    logger.info(f"Successfully read CSV with {encoding} encoding")
                    break
                except UnicodeDecodeError as e:
                    if 'utf-16' in encoding and 'BOM' in str(e):
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
        # Print final summary and generate report
        logger.info("\n" + "="*50)
        logger.info("PROCESSING COMPLETE")
        logger.info(f"Total rows processed: {stats.total_processed}/{len(rows_to_process)}")
        logger.info(f"Successful: {stats.successful} ({(stats.successful/max(1, stats.total_processed))*100:.1f}%)")
        logger.info(f"Failed: {stats.failed}")
        logger.info(f"Total files enqueued: {stats.enqueued_files}")
        logger.info("="*50)
        
        generate_report()


async def process_batch(client, rows, start_index, total_rows):
    """Process a batch of rows.
    
    Args:
        client: The SLSKD API client
        rows: List of rows to process
        start_index: Starting index for progress reporting
        total_rows: Total number of rows for progress reporting
    """
    # Process in batches
    for batch_start in range(0, len(rows), BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, len(rows))
        batch = rows[batch_start:batch_end]
        
        batch_info = f"rows {batch_start + 1} to {batch_end} of {len(rows)}"
        logger.info(f"Processing batch: {batch_info}")
        
        # Track batch start time for performance metrics
        batch_start_time = time.time()
        
        for i, row in enumerate(batch):
            row_index = start_index + batch_start + i  # for display
            stats.total_processed += 1
            
            # Process the row
            success = await process_row(client, row, row_index, total_rows)
            if success:
                stats.successful += 1
            else:
                stats.failed += 1
            
            # Save checkpoint every 5 rows processed
            if row_index % 5 == 0 or row_index == total_rows:
                save_checkpoint(row_index, total_rows)
                
            # Print progress report every 20 items
            if stats.total_processed % 20 == 0:
                remaining = total_rows - row_index
                stats.print_progress(remaining)
        
        # Print batch summary with timing
        batch_time = time.time() - batch_start_time
        avg_time_per_item = batch_time / len(batch) if batch else 0
        logger.info(f"Batch summary: {batch_end - batch_start} rows processed in {batch_time:.1f}s " + 
                    f"({avg_time_per_item:.1f}s per item), " +
                    f"{stats.successful}/{stats.total_processed} successful ({stats.failed} failed)")


async def check_downloads_status():
    """
    Check status of all queued downloads and update their status in the tracker.
    This runs as a background task and doesn't interfere with the main script.
    """
    global queued_files_tracker
    
    try:
        # First check active downloads
        url = f"{HOST.rstrip('/')}{API_PATH}/transfers/downloads"
        logger.debug(f"Checking active downloads from: {url}")
        
        with concurrent.futures.ThreadPoolExecutor() as pool:
            resp = await asyncio.get_event_loop().run_in_executor(
                pool, lambda: requests.get(url, headers=HEADERS)
            )
        resp.raise_for_status()
        active_downloads = resp.json()
        
        # Process the response structure based on what the API returns
        if isinstance(active_downloads, dict):
            if 'downloads' in active_downloads:
                # Handle nested structure
                logger.debug(f"Active downloads response has 'downloads' key with {len(active_downloads['downloads'])} items")
                active_downloads = active_downloads['downloads']
            else:
                # Log all keys at the top level to help understand structure
                logger.debug(f"Active downloads response keys: {list(active_downloads.keys())}")
        
        # Now check completed downloads with a separate API call
        # This endpoint should return downloads that have finished
        logger.debug(f"Checking completed downloads")
        completed_downloads = []
        try:
            completed_url = f"{HOST.rstrip('/')}{API_PATH}/transfers/downloads/completed"
            logger.debug(f"Requesting completed downloads from: {completed_url}")
            
            with concurrent.futures.ThreadPoolExecutor() as pool:
                completed_resp = await asyncio.get_event_loop().run_in_executor(
                    pool, lambda: requests.get(completed_url, headers=HEADERS)
                )
            
            if completed_resp.status_code == 200:
                completed_data = completed_resp.json()
                if isinstance(completed_data, dict) and 'downloads' in completed_data:
                    logger.debug(f"Completed downloads response has 'downloads' key with {len(completed_data['downloads'])} items")
                    completed_downloads = completed_data['downloads']
                elif isinstance(completed_data, list):
                    logger.debug(f"Completed downloads response is a list with {len(completed_data)} items")
                    completed_downloads = completed_data
                else:
                    # Log all keys at the top level to help understand structure
                    logger.debug(f"Completed downloads response keys: {list(completed_data.keys()) if isinstance(completed_data, dict) else 'Not a dict'}")
                
                logger.info(f"Found {len(completed_downloads)} completed downloads")
            else:
                logger.warning(f"Failed to get completed downloads: {completed_resp.status_code}")
                if completed_resp.text:
                    logger.warning(f"Response text: {completed_resp.text[:500]}")  # First 500 chars to avoid massive logs
        except Exception as e:
            logger.warning(f"Error getting completed downloads: {e}")

        # Debug the structure of both endpoints
        if active_downloads and len(active_downloads) > 0:
            logger.debug(f"Active download structure example: {json.dumps(active_downloads[0], indent=2)}")
        
        if completed_downloads and len(completed_downloads) > 0:
            logger.debug(f"Completed download structure example: {json.dumps(completed_downloads[0], indent=2)}")
        
        # Check download directory as well for any completed files
        logger.debug(f"Checking download directory for completed files")
        download_dir_files = []
        try:
            folder_url = f"{HOST.rstrip('/')}{API_PATH}/filesystem/downloads"
            with concurrent.futures.ThreadPoolExecutor() as pool:
                folder_resp = await asyncio.get_event_loop().run_in_executor(
                    pool, lambda: requests.get(folder_url, headers=HEADERS)
                )
            
            if folder_resp.status_code == 200:
                folder_data = folder_resp.json()
                if isinstance(folder_data, dict):
                    # Log the keys to help understand the structure
                    logger.debug(f"Download folder response keys: {list(folder_data.keys())}")
                    
                    if 'files' in folder_data:
                        download_dir_files = folder_data['files']
                        logger.info(f"Found {len(download_dir_files)} files in download directory")
                    elif 'content' in folder_data:
                        # Some API versions might use different keys
                        logger.debug(f"Download folder uses 'content' key instead of 'files'")
                        download_dir_files = folder_data.get('content', [])
                        logger.info(f"Found {len(download_dir_files)} files in download directory")
            else:
                logger.warning(f"Failed to get download directory: {folder_resp.status_code}")
                if folder_resp.text:
                    logger.warning(f"Response text: {folder_resp.text[:500]}")  # First 500 chars to avoid massive logs
        except Exception as e:
            logger.warning(f"Error checking download folder: {e}")
        
        # Debug logging to understand what's happening
        logger.debug(f"Found {len(active_downloads)} active and {len(completed_downloads)} completed downloads")
        
        # Map SLSKD states to our status values
        # SLSKD uses state values like "Requested", "InProgress", "Completed", "Cancelled"
        status_map = {
            "requested": "queued",
            "inprogress": "downloading",
            "completed": "completed", 
            "cancelled": "cancelled",
            # Add other mappings as needed
        }
        
        # Update status for all tracked files
        status_updated = False
        for queued_file in queued_files_tracker:
            username = queued_file.get('username')
            filename = queued_file.get('filename')
            basename = queued_file.get('basename') or os.path.basename(filename)
            old_status = queued_file.get('download_status', 'unknown')
            
            if not filename:
                continue
                
            # Find matching download in active downloads
            current_status = "not_found"
            for download in active_downloads:
                dl_filename = download.get('filename', '')
                dl_basename = os.path.basename(dl_filename) if dl_filename else ''
                dl_username = download.get('username', '')
                dl_state = download.get('state', '').lower()
                
                # Try different matching approaches
                if (dl_username == username or not username) and any([
                    # Exact matches
                    dl_filename == filename,
                    dl_basename == basename,
                    # Partial matches 
                    basename in dl_basename,
                    dl_basename in basename,
                ]):
                    current_status = status_map.get(dl_state, dl_state)
                    # Add additional details that might be useful
                    queued_file['size'] = download.get('size')
                    queued_file['bytesTransferred'] = download.get('bytesTransferred')
                    queued_file['progress'] = (
                        f"{(download.get('bytesTransferred', 0) / max(1, download.get('size', 1)) * 100):.1f}%"
                        if download.get('size') else "unknown"
                    )
                    break
            
            # If not found in active downloads, check completed downloads
            if current_status == "not_found":
                for download in completed_downloads:
                    dl_filename = download.get('filename', '')
                    dl_basename = os.path.basename(dl_filename) if dl_filename else ''
                    dl_username = download.get('username', '')
                    
                    if (dl_username == username or not username) and any([
                        # Exact matches
                        dl_filename == filename,
                        dl_basename == basename,
                        # Partial matches 
                        basename in dl_basename,
                        dl_basename in basename,
                    ]):
                        current_status = "completed"  # Force completed status
                        queued_file['size'] = download.get('size')
                        queued_file['bytesTransferred'] = download.get('size')  # Assume full transfer for completed
                        queued_file['progress'] = "100%"
                        break
            
            # If still not found but we have the file in download directory, mark as completed
            if current_status == "not_found" and download_dir_files:
                for file_info in download_dir_files:
                    dl_filename = file_info.get('name', '')
                    if basename == dl_filename or basename in dl_filename:
                        current_status = "completed"
                        queued_file['size'] = file_info.get('size')
                        queued_file['bytesTransferred'] = file_info.get('size')
                        queued_file['progress'] = "100%"
                        break
            
            # Only log if status has changed
            if current_status != old_status:
                status_updated = True
                logger.info(f"Download status for {basename}: {old_status} → {current_status}")
                
            queued_file['download_status'] = current_status
            queued_file['last_checked'] = datetime.now().isoformat()
        
        # Print summary of download statuses if any changed
        if status_updated:
            status_counts = {}
            for file in queued_files_tracker:
                status = file.get('download_status', 'unknown')
                status_counts[status] = status_counts.get(status, 0) + 1
                
            logger.info(f"Download status summary: {status_counts}")
        else:
            logger.debug("No status changes detected")
            
    except Exception as e:
        logger.error(f"Error checking downloads status: {e}")
        import traceback
        logger.error(traceback.format_exc())

async def start_status_monitoring(check_interval=300):  # 5 minutes by default
    """Start a background task that periodically checks download status."""
    logger.info(f"Starting download status monitoring (checking every {check_interval} seconds)")
    
    while True:
        # Wait first to allow initial downloads to be queued
        await asyncio.sleep(check_interval)
        
        # Check status if we have any queued files
        if queued_files_tracker:
            await check_downloads_status()
        else:
            logger.debug("No queued files to check status for")

async def main():
    """Main function with argument parsing."""
    # Declare globals at the beginning of the function
    global CSV_FILE, BATCH_SIZE, RATE_LIMIT_DELAY, stats, results_log, log_dir
    global EXCLUDED_EXTENSIONS, ALLOWED_FORMATS, QUEUE_LIMIT
    global USE_DIRECT_API, EXACT_MATCH, status_monitor_task
    
    parser = argparse.ArgumentParser(description="Queue albums or tracks from CSV to SLSKD")
    parser.add_argument("--csv", "-c", default=CSV_FILE, help="Path to CSV file (default: to_queue.csv)")
    parser.add_argument("--resume", "-r", action="store_true", help="Resume from last checkpoint")
    parser.add_argument("--retry-failed", "-rf", action="store_true", 
                        help="Only retry rows that failed in previous runs")
    parser.add_argument("--batch-size", "-b", type=int, default=BATCH_SIZE, 
                        help=f"Number of rows to process in each batch (default: {BATCH_SIZE})")
    parser.add_argument("--delay", "-d", type=float, default=RATE_LIMIT_DELAY,
                        help=f"Delay between API calls in seconds (default: {RATE_LIMIT_DELAY})")
    parser.add_argument("--formats", "-f", nargs="+", default=ALLOWED_FORMATS,
                        help=f"Allowed file formats in priority order (default: {ALLOWED_FORMATS})")
    parser.add_argument("--exclude", "-e", nargs="+", default=EXCLUDED_EXTENSIONS,
                        help=f"File extensions to exclude entirely (default: {EXCLUDED_EXTENSIONS})")
    parser.add_argument("--output-dir", "-o", default=log_dir,
                        help=f"Directory for output files (default: {log_dir})")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--direct-api", action="store_true", help="Use direct API calls instead of client library")
    parser.add_argument("--gen-report", action="store_true", help="Generate a report without processing new files")
    parser.add_argument("--exact-match", action="store_true", help="Require exact artist-album-track matching")
    parser.add_argument("--queue-limit", type=int, default=QUEUE_LIMIT,
                        help="Maximum items in queue per user (0 for no limit, default: 0)")
    args = parser.parse_args()
    
    # Setup logging based on debug flag
    log_level = logging.DEBUG if args.debug else logging.INFO
    setup_logging(log_level)
    
    # Update global configuration based on arguments
    CSV_FILE = args.csv
    BATCH_SIZE = args.batch_size
    RATE_LIMIT_DELAY = args.delay
    ALLOWED_FORMATS = args.formats
    EXCLUDED_EXTENSIONS = args.exclude
    USE_DIRECT_API = args.direct_api
    EXACT_MATCH = args.exact_match
    QUEUE_LIMIT = args.queue_limit
    
    if EXACT_MATCH:
        logger.info("Exact matching mode enabled - only exact artist-album-track matches will be selected")
    
    logger.info(f"Allowed formats (in priority order): {ALLOWED_FORMATS}")
    logger.info(f"Excluded formats: {EXCLUDED_EXTENSIONS}")
    
    if USE_DIRECT_API:
        logger.info("Direct API mode enabled - bypassing client library")
        
    log_dir = args.output_dir
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Check if user just wants to generate a report
    if args.gen_report:
        checkpoint = load_checkpoint()
        if checkpoint and 'results_log' in checkpoint:
            results_log = checkpoint['results_log']
            logger.info(f"Loaded {len(results_log)} entries from checkpoint for report generation")
            generate_report_on_demand()
            return
        else:
            logger.error("No checkpoint found to generate report from")
            return
    
    # Process with or without resuming
    start_row = 0
    if args.resume and not args.retry_failed:
        checkpoint = load_checkpoint()
        if checkpoint:
            start_row = checkpoint['row_index']
            # Use the globals already declared at the beginning
            stats.total_processed = checkpoint['stats']['total_processed']
            stats.successful = checkpoint['stats']['successful']
            stats.failed = checkpoint['stats']['failed']
            stats.enqueued_files = checkpoint['stats']['enqueued_files']
            results_log = checkpoint['results_log']
    
    # Comment out status monitoring
    # if not args.no_status_check and args.status_check > 0:
    #     # Start as background task that won't block the main script
    #     status_monitor_task = asyncio.create_task(start_status_monitoring(args.status_check))
    #     logger.info(f"Download status monitoring enabled (checking every {args.status_check} seconds)")
    # else:
    #     logger.info("Download status monitoring disabled")
    
    try:
        await process_csv(CSV_FILE, start_row, args.retry_failed)
    finally:
        # Comment out status monitoring cleanup
        # if status_monitor_task and not status_monitor_task.done():
        #     try:
        #         # Run one more status check if we have queued files
        #         if queued_files_tracker:
        #             logger.info("Running final status check...")
        #             await check_downloads_status()
        #         # Cancel the monitoring task
        #         status_monitor_task.cancel()
        #     except Exception:
        #         pass
                
        # Always try to generate the report, even if an exception occurs
        if results_log:
            logger.info("Generating final report...")
            generate_report()

def rank_all_results(responses: List[Dict[str, Any]], artist: str, album: str, track: str = None) -> List[Dict[str, Any]]:
    """
    Rank all valid results from all users based on format priority, cleanliness, and size.
    
    Args:
        responses: List of response objects from search API
        artist: Artist name for matching
        album: Album name for matching
        track: Optional track name for matching
        
    Returns:
        List of ranked candidates, each containing complete file and user information
    """
    all_candidates = []
    
    for response in responses:
        username = response.get('username')
        
        for file_info in response.get('files', []):
            filename = file_info.get('filename', '')
            
            # Skip excluded extensions
            if file_has_excluded_extension(filename, EXCLUDED_EXTENSIONS):
                continue
                
            # Get format priority (999 if not in allowed formats)
            priority = get_priority_group(filename)
            if priority >= len(ALLOWED_FORMATS):
                continue
            
            # Check if this file matches what we're looking for
            match_found = False
            if track:
                if EXACT_MATCH:
                    match_found = is_exact_match(filename, artist, album, track)
                else:
                    match_found = track.lower() in filename.lower()
            else:
                # For albums, just check album name
                match_found = album.lower() in filename.lower()
            
            if match_found:
                # Score the filename cleanliness
                cleanliness_score = score_filename_cleanliness(filename, artist, album, track)
                
                # Create candidate entry with all necessary information
                candidate = {
                    'username': username,
                    'filename': filename,
                    'size': file_info.get('size', 0),
                    'priority': priority,
                    'cleanliness': cleanliness_score,
                    'file_id': file_info.get('id') or file_info.get('file_id'),
                    'original_response': response,  # Keep original response data
                    'file_info': file_info  # Keep complete file info
                }
                
                all_candidates.append(candidate)
    
    # Sort candidates by priority (format), then cleanliness, then size
    all_candidates.sort(key=lambda x: (
        x['priority'],  # Lower priority (format) is better
        -x['cleanliness'],  # Higher cleanliness is better
        -x['size']  # Larger size is better
    ))
    
    # Log the ranking results
    if all_candidates:
        logger.info(f"Ranked {len(all_candidates)} valid candidates")
        top_n = min(5, len(all_candidates))
        logger.info(f"Top {top_n} candidates:")
        for i, candidate in enumerate(all_candidates[:top_n], 1):
            format_type = ALLOWED_FORMATS[candidate['priority']]
            logger.info(f"  {i}. {os.path.basename(candidate['filename'])} "
                       f"from {candidate['username']} - "
                       f"Format: {format_type}, "
                       f"Cleanliness: {candidate['cleanliness']:.1f}, "
                       f"Size: {candidate['size']:,} bytes")
    else:
        logger.info("No valid candidates found")
    
    return all_candidates

async def check_queue_status(username: str) -> Tuple[bool, int]:
    """
    Check a user's queue status.
    
    Args:
        username: Username to check
        
    Returns:
        Tuple of (has_open_slot: bool, queue_count: int)
    """
    try:
        # Get user's current downloads
        url = f"{HOST.rstrip('/')}{API_PATH}/transfers/downloads"
        
        with concurrent.futures.ThreadPoolExecutor() as pool:
            resp = await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(
                    pool, lambda: requests.get(url, headers=HEADERS)
                ),
                timeout=ENQUEUE_TIMEOUT
            )
        resp.raise_for_status()
        
        downloads = resp.json()
        
        # Count current downloads for this user
        user_queue_count = 0
        for download in downloads:
            if isinstance(download, dict) and download.get('username') == username:
                user_queue_count += 1
        
        # Consider queue "open" if limit is 0 (no limit) or count is below limit
        has_open_slot = QUEUE_LIMIT == 0 or user_queue_count < QUEUE_LIMIT
        
        return has_open_slot, user_queue_count
        
    except Exception as e:
        logger.warning(f"Failed to check queue status for {username}: {e}")
        # If we can't check, assume it's not available
        return False, 999

async def find_best_available_candidate(ranked_candidates: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Find the first candidate from the ranked list that has an open queue slot.
    
    Args:
        ranked_candidates: List of candidates, already sorted by priority
        
    Returns:
        Best available candidate or None if none are available
    """
    checked_users = set()  # Track users we've already checked to avoid duplicate checks
    
    for candidate in ranked_candidates:
        username = candidate['username']
        
        # Skip if we've already checked this user
        if username in checked_users:
            continue
            
        checked_users.add(username)
        
        # Check queue status
        has_open_slot, queue_count = await check_queue_status(username)
        
        if has_open_slot:
            logger.info(f"Found available candidate: {os.path.basename(candidate['filename'])} "
                       f"from {username} (queue count: {queue_count})")
            return candidate
        else:
            logger.debug(f"Skipping {username} - queue count: {queue_count}")
    
    return None

if __name__ == '__main__':
    # Run the async main function
    asyncio.run(main())
