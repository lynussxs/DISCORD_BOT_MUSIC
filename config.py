"""
config.py — Centralized configuration loader.

Reads all bot settings from environment variables (via .env).
Import `config` anywhere in the project to access settings.
"""

import os
from dotenv import load_dotenv

load_dotenv()


def _require(key: str) -> str:
    """Fetch a required env var, raising a clear error if missing."""
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(
            f"Missing required environment variable: '{key}'. "
            f"Check your .env file against .env.example."
        )
    return value


# ── Required ──────────────────────────────────────────────────────────────────

BOT_TOKEN: str = _require("BOT_TOKEN")

# ── Optional with sensible defaults ───────────────────────────────────────────

# Set to your server ID during development for instant slash-command syncing.
# Leave empty to sync globally (takes up to 1 hour on first deploy).
DEV_GUILD_ID: int | None = (
    int(os.environ["DEV_GUILD_ID"]) if os.getenv("DEV_GUILD_ID") else None
)

# Log level: DEBUG | INFO | WARNING | ERROR | CRITICAL
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()

# Path to the rotating log file
LOG_FILE: str = os.getenv("LOG_FILE", "logs/bot.log")

# Default volume (0–100)
DEFAULT_VOLUME: int = int(os.getenv("DEFAULT_VOLUME", "50"))
