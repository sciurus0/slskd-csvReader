"""
Logging setup for slskd_spotify workflow.

Provides a shared logger and setup_logging() to configure file + console output
with rotation. Used by slskd_spotify.py and related modules.
"""

import logging
import os
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Optional, Tuple

DEFAULT_OUTPUT_DIR = "data/logs"

logger = logging.getLogger(__name__)


def setup_logging(
    log_level: int = logging.INFO,
    output_dir: Optional[str] = None,
) -> Tuple[logging.Logger, str]:
    """
    Configure logging with file rotation and console output.

    Args:
        log_level: Logging level (e.g. logging.INFO, logging.DEBUG).
        output_dir: Directory for log files. Defaults to DEFAULT_OUTPUT_DIR if None.

    Returns:
        Tuple of (logger instance, output_dir in use).
    """
    log_dir = output_dir or DEFAULT_OUTPUT_DIR
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_filename = os.path.join(
        log_dir, f"slskd_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )

    # Remove existing handlers to prevent duplicate output
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)

    log_format = "%(asctime)s %(levelname)s: %(message)s"
    formatter = logging.Formatter(log_format)

    file_handler = RotatingFileHandler(
        log_filename,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=10,
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    logger.setLevel(log_level)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = False

    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    logger.info(f"Logging initialized at level: {logging.getLevelName(log_level)}")
    logger.info(f"Log file: {log_filename}")

    return logger, log_dir
