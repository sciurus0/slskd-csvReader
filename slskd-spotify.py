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

API Key Loading:
----------------
This script requires an API key to access your SLSKD server. The API key can be provided in one of two ways:

1. Environment Variable (Recommended for CI/servers):
   Set the environment variable `SLSKD_API_KEY` before running the script:
       export SLSKD_API_KEY=your-api-key-here
       python3 slskd-spotify.py

2. api.txt File (Recommended for local use):
   Create a file named `api.txt` in the same directory as this script and put your API key in it (no quotes, no extra whitespace):
       your-api-key-here
   The script will automatically read the key from this file if the environment variable is not set.

Security Notes:
- The `api.txt` file is included in `.gitignore` and will not be committed to version control.
- Never share your API key or commit it to a public repository.
- If neither the environment variable nor the `api.txt` file is found, the script will log an error and exit.

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

  --album-preferred-search
                        Use broader search with album preference and quality filtering
                        Searches for Artist-Track (broader) instead of Artist-Album-Track (strict)
                        Prioritizes files where album appears in the path (directory or filename)
                        Falls back to files without album match when no better option exists
                        Automatically filters out unwanted versions (remixes, live, covers, etc.)
                        unless explicitly requested in the search query
                        Increases success rate while maintaining quality control

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
  
  # Use album-preferred search for higher success rate
  python slskd-spotify.py --album-preferred-search
  
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
import asyncio
import concurrent.futures
from asyncio import TimeoutError
from typing import Dict, List, Tuple, Optional, Any, Union, Set, Callable, Awaitable
import re
import unicodedata

import slskd_api

from slskd_logging import DEFAULT_OUTPUT_DIR, logger, setup_logging
from slskd_csv import (
    detect_encoding,
    count_csv_rows,
    save_checkpoint,
    load_checkpoint,
    generate_report,
    generate_report_on_demand,
    find_most_recent_results_csv,
    load_failed_rows,
)
from slskd_search import (
    configure_search_context,
    search_slskd_async,
    detect_search_intent,
    rank_all_results,
)
from slskd_config import (
    HOST,
    API_PATH,
    API_KEY,
    CSV_FILE,
    QUEUE_LIMIT,
    RATE_LIMIT_DELAY,
    BATCH_SIZE,
    MAX_RETRIES,
    SEARCH_TIMEOUT,
    ENQUEUE_TIMEOUT,
    CHECKPOINT_FILE,
    EXCLUDED_EXTENSIONS,
    CIRCUIT_BREAKER_THRESHOLD,
    CIRCUIT_BREAKER_TIMEOUT,
    USE_DIRECT_API,
    EXACT_MATCH,
    ALBUM_PREFERRED_SEARCH,
    ALLOWED_FORMATS,
    POLL_INTERVAL,
    MAX_POLLS,
    SOURCE,
    make_headers,
)

# ======== Filename Sanitization ========

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

# ======== Logging ========
# log_dir is set in main() from setup_logging(); use DEFAULT_OUTPUT_DIR for parser default
log_dir = DEFAULT_OUTPUT_DIR

# ======== Configuration ========
# Defaults are imported from slskd_config and may be overridden by CLI args.

HEADERS = make_headers(API_KEY)

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
        
        # Use direct API calls if specified
        if USE_DIRECT_API:
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
                        if '_search_terms' in file_info:
                            tracked_file['search_terms'] = file_info['_search_terms']
                        
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
    
    # Validate required fields based on what we have
    if not artist:
        result_entry['message'] = "Missing artist (required)"
        results_log.append(result_entry)
        logger.warning(f"Skipping row with missing artist: {row}")
        return False
    
    # Determine search pattern based on available information
    if album and track:
        if ALBUM_PREFERRED_SEARCH:
            # Album-preferred mode: Search with Artist - Track (broader)
            # Album will be used for filtering/preference, not search
            pattern = f"{artist} - {track}"
            search_type = "artist-track-with-album-preference"
        else:
            # Standard mode: Artist - Album - Track
            pattern = f"{artist} - {album} - {track}"
            search_type = "artist-album-track"
    elif album and not track:
        # Album download: Artist - Album
        pattern = f"{artist} - {album}"
        search_type = "artist-album"
    elif track and not album:
        # Single track without album: Artist - Track
        pattern = f"{artist} - {track}"
        search_type = "artist-track"
    else:
        # Only artist provided: throw error
        result_entry['message'] = "Missing both album and track (at least one required)"
        results_log.append(result_entry)
        logger.warning(f"Skipping row with only artist: {row}")
        return False
    
    logger.info(f"🔍 Searching for: {pattern} ({search_type}) ({row_index}/{total_rows})")
    
    try:
        resp_list = await search_slskd_async(pattern)
        
        if not resp_list:
            result_entry['status'] = 'failed'
            result_entry['message'] = "No results found"
            results_log.append(result_entry)
            logger.info(f"No results for: {pattern}")
            return False
        
        files_queued = 0
        
        # Detect search intent for quality filtering
        search_intent = detect_search_intent(artist, album or "", track or "")
        
        # Get ranked list of all candidates
        ranked_candidates, rejection_reasons = rank_all_results(
            resp_list, artist, album if album else None, track if track else None, search_intent
        )
        if not ranked_candidates:
            result_entry['status'] = 'failed'
            if rejection_reasons:
                from collections import Counter
                reason_counts = Counter(rejection_reasons)
                reason_summary = "; ".join(f"{count} file(s): {reason}" for reason, count in reason_counts.items())
                result_entry['message'] = f"No valid candidates found: {reason_summary}"
            else:
                result_entry['message'] = "No valid candidates found"
            results_log.append(result_entry)
            logger.info(f"No valid candidates found for: {pattern}")
            return False
            
        if not track:  # Album download case
            # Group all tracks from the same best user
            best_username = ranked_candidates[0]['username']
            album_tracks = [c for c in ranked_candidates if c['username'] == best_username]
            
            # Store album match status for album downloads
            if ALBUM_PREFERRED_SEARCH:
                result_entry['has_album_match'] = ranked_candidates[0].get('has_album_match', False)
            
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
            
            # Store album match status for reporting
            if ALBUM_PREFERRED_SEARCH and best_candidate:
                result_entry['has_album_match'] = best_candidate.get('has_album_match', False)
        
        if files_queued > 0:
            result_entry['status'] = 'success'
            result_entry['files_queued'] = files_queued
            
            # Update message to indicate if this was a fallback
            if ALBUM_PREFERRED_SEARCH and not result_entry.get('has_album_match', True):
                result_entry['message'] = f"Successfully queued {files_queued} file(s) [FALLBACK - no album match]"
            else:
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
        results_csv = find_most_recent_results_csv(log_dir)
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
                                save_checkpoint(CHECKPOINT_FILE, i, total_rows, stats, results_log)
                    
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
            generate_report(CSV_FILE, results_log, log_dir)
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
        
        generate_report(CSV_FILE, results_log, log_dir)


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
                save_checkpoint(CHECKPOINT_FILE, row_index, total_rows, stats, results_log)
                
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
    global USE_DIRECT_API, EXACT_MATCH, ALBUM_PREFERRED_SEARCH
    
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
    parser.add_argument("--output-dir", "-o", default=DEFAULT_OUTPUT_DIR,
                        help=f"Directory for output files (default: {DEFAULT_OUTPUT_DIR})")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--direct-api", action="store_true", help="Use direct API calls instead of client library")
    parser.add_argument("--gen-report", action="store_true", help="Generate a report without processing new files")
    parser.add_argument("--exact-match", action="store_true", help="Require exact artist-album-track matching")
    parser.add_argument("--album-preferred-search", action="store_true",
                        help="Use broader search (Artist-Track) with album preference and quality filtering")
    parser.add_argument("--queue-limit", type=int, default=QUEUE_LIMIT,
                        help="Maximum items in queue per user (0 for no limit, default: 0)")
    args = parser.parse_args()
    
    # Setup logging based on debug flag (also creates output dir and sets log_dir)
    log_level = logging.DEBUG if args.debug else logging.INFO
    _, log_dir = setup_logging(log_level, args.output_dir)
    
    # Update global configuration based on arguments
    CSV_FILE = args.csv
    BATCH_SIZE = args.batch_size
    RATE_LIMIT_DELAY = args.delay
    ALLOWED_FORMATS = args.formats
    EXCLUDED_EXTENSIONS = args.exclude
    USE_DIRECT_API = args.direct_api
    EXACT_MATCH = args.exact_match
    ALBUM_PREFERRED_SEARCH = args.album_preferred_search
    QUEUE_LIMIT = args.queue_limit

    # Configure search/ranking module with runtime overrides
    configure_search_context(
        host=HOST,
        api_path=API_PATH,
        headers=HEADERS,
        rate_limit_delay=RATE_LIMIT_DELAY,
        max_retries=MAX_RETRIES,
        search_timeout=SEARCH_TIMEOUT,
        poll_interval=POLL_INTERVAL,
        max_polls=MAX_POLLS,
        circuit_breaker_threshold=CIRCUIT_BREAKER_THRESHOLD,
        circuit_breaker_timeout=CIRCUIT_BREAKER_TIMEOUT,
        album_preferred_search=ALBUM_PREFERRED_SEARCH,
        exact_match=EXACT_MATCH,
        excluded_extensions=EXCLUDED_EXTENSIONS,
        allowed_formats=ALLOWED_FORMATS,
    )
    
    if EXACT_MATCH:
        logger.info("Exact matching mode enabled - only exact artist-album-track matches will be selected")
    
    if ALBUM_PREFERRED_SEARCH:
        logger.info("Album-preferred search mode enabled - broader search with album preference and quality filtering")
    
    logger.info(f"Allowed formats (in priority order): {ALLOWED_FORMATS}")
    logger.info(f"Excluded formats: {EXCLUDED_EXTENSIONS}")
    
    if USE_DIRECT_API:
        logger.info("Direct API mode enabled - bypassing client library")
    
    # Check if user just wants to generate a report
    if args.gen_report:
        checkpoint = load_checkpoint(CHECKPOINT_FILE)
        if checkpoint and 'results_log' in checkpoint:
            results_log = checkpoint['results_log']
            logger.info(f"Loaded {len(results_log)} entries from checkpoint for report generation")
            generate_report_on_demand(CSV_FILE, results_log, log_dir)
            return
        else:
            logger.error("No checkpoint found to generate report from")
            return
    
    # Process with or without resuming
    start_row = 0
    if args.resume and not args.retry_failed:
        checkpoint = load_checkpoint(CHECKPOINT_FILE)
        if checkpoint:
            start_row = checkpoint['row_index']
            # Use the globals already declared at the beginning
            stats.total_processed = checkpoint['stats']['total_processed']
            stats.successful = checkpoint['stats']['successful']
            stats.failed = checkpoint['stats']['failed']
            stats.enqueued_files = checkpoint['stats']['enqueued_files']
            results_log = checkpoint['results_log']
    

    
    try:
        await process_csv(CSV_FILE, start_row, args.retry_failed)
    finally:
        # Always try to generate the report, even if an exception occurs
        if results_log:
            logger.info("Generating final report...")
            generate_report(CSV_FILE, results_log, log_dir)

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
