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
- Queued download tracking (records enqueued files; no background status polling)
- Queue management with configurable limits
- Queue CSV (to_queue.csv) should be produced by merge_queue.py so artist/album/track fields are sanitized once before searching

API Key Loading:
----------------
This script requires an API key to access your SLSKD server. The key can be provided in one of these ways:

1. Environment Variable (Recommended for CI/servers):
   Set the environment variable `SLSKD_API_KEY` before running the script:
       export SLSKD_API_KEY=your-api-key-here
       python3 slskd-spotify.py

2. api.txt File (Recommended for local use):
   Create `api.txt` in the same directory as this script (repo root). Two supported layouts:

   **Legacy:** one line, SLSKD API key only (no '=' or '[' in the file):
       your-slskd-api-key-here

   **INI (includes optional Spotify block used by spotify_playlist_fetch.py):**
       [slskd]
       api_key = your-slskd-api-key-here

       [spotify]
       client_id = ...
       client_secret = ...
       redirect_uri = http://127.0.0.1:8765/callback

   Environment variables override values from api.txt when both are set.

Security Notes:
- The `api.txt` file is included in `.gitignore` and will not be committed to version control.
- Never share your API keys or commit them to a public repository.
- If neither `SLSKD_API_KEY` nor a readable key in api.txt is available, SLSKD requests will fail once used.

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

import argparse
import asyncio
import logging
import sys

from slskd_logging import DEFAULT_OUTPUT_DIR, logger, setup_logging
from slskd_csv import load_checkpoint, generate_report, generate_report_on_demand
from slskd_search import configure_search_context
from slskd_queue import configure_queue_context
from slskd_worker import Stats, configure_worker_context, process_csv
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
    make_headers,
)


# ======== Logging ========
# log_dir is set in main() from setup_logging(); use DEFAULT_OUTPUT_DIR for parser default
log_dir = DEFAULT_OUTPUT_DIR

# ======== Configuration ========
# Defaults are imported from slskd_config and may be overridden by CLI args.

HEADERS = make_headers(API_KEY)


stats = Stats()
results_log = []  # To track detailed results for reporting
queued_files_tracker = []  # Global list to track all queued files for status checking


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

    configure_queue_context(
        host=HOST,
        api_path=API_PATH,
        api_key=API_KEY,
        headers=HEADERS,
        enqueue_timeout=ENQUEUE_TIMEOUT,
        use_direct_api=USE_DIRECT_API,
        queue_limit=QUEUE_LIMIT,
        queued_files_tracker=queued_files_tracker,
        stats=stats,
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

    configure_worker_context(
        results_log=results_log,
        stats=stats,
        log_dir=log_dir,
        csv_file=CSV_FILE,
        batch_size=BATCH_SIZE,
        checkpoint_file=CHECKPOINT_FILE,
        album_preferred_search=ALBUM_PREFERRED_SEARCH,
    )

    try:
        await process_csv(CSV_FILE, start_row, args.retry_failed)
    finally:
        # Always try to generate the report, even if an exception occurs
        if results_log:
            logger.info("Generating final report...")
            generate_report(CSV_FILE, results_log, log_dir)

if __name__ == '__main__':
    # Run the async main function
    asyncio.run(main())
