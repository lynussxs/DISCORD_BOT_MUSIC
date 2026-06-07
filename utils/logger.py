"""
utils/logger.py — Logging setup.

Creates a logger that writes to both the console and a rotating log file.
Import `get_logger(__name__)` in any module for consistent log output.
"""

import logging
import os
from logging.handlers import RotatingFileHandler

import config


def setup_logging() -> None:
    """
    Configure the root logger once at bot startup.
    All subsequent `logging.getLogger(...)` calls inherit this config.
    """
    # Ensure the logs directory exists before opening the file handler
    os.makedirs(os.path.dirname(config.LOG_FILE), exist_ok=True)

    level = getattr(logging, config.LOG_LEVEL, logging.INFO)

    # Shared format: timestamp | level | module | message
    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler — always at the configured level
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)
    console_handler.setLevel(level)

    # Rotating file handler — 5 MB per file, keep last 3
    file_handler = RotatingFileHandler(
        config.LOG_FILE,
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)
    file_handler.setLevel(level)

    # Apply to root logger so discord.py's own logs are captured too
    root = logging.getLogger()
    root.setLevel(level)
    root.addHandler(console_handler)
    root.addHandler(file_handler)

    # Quiet noisy discord.py internals unless we're in DEBUG mode
    if level > logging.DEBUG:
        logging.getLogger("discord.gateway").setLevel(logging.WARNING)
        logging.getLogger("discord.http").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Return a named logger. Call as: logger = get_logger(__name__)"""
    return logging.getLogger(name)
