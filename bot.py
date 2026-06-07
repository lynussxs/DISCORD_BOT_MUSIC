"""
bot.py — Entry point.

Run with: python bot.py

Command sync strategy
─────────────────────
Discord maintains two separate command registries per bot:
  • Global commands   — visible in every server (propagation: up to 1 hour)
  • Guild commands    — visible only in one server (propagation: instant)

The duplicate-command problem occurs when BOTH registries contain the same
command names — Discord's client renders them side by side.

This bot uses guild-only sync for all environments:
  1. Copy the in-memory command tree to the dev guild scope.
  2. Sync to the guild  → registers/updates 8 commands in that guild only.
  3. Clear the global scope locally, then sync it → pushes an EMPTY global
     tree to Discord, removing any stale global commands from previous runs.

DEV_GUILD_ID is required. The bot will refuse to start without it so that
a misconfigured .env can never cause a silent global-sync and duplicates.
"""

import asyncio
import sys
from threading import Thread

import discord
from flask import Flask
from discord import app_commands
from discord.ext import commands

import config
from utils.logger import setup_logging, get_logger

# ── Keep-alive web server ────────────────────────────────────────────────────
# A minimal Flask app that external ping services (e.g. UptimeRobot) can hit
# so the hosting platform never considers the process idle.  It runs on a
# daemon thread so it never blocks the asyncio event loop that drives the bot.

_flask_app = Flask(__name__)


@_flask_app.route("/")
def _index():
    return "Bot is alive!"


def keep_alive() -> None:
    """Start the Flask keep-alive server on a background daemon thread."""
    thread = Thread(target=lambda: _flask_app.run(host="0.0.0.0", port=8080), daemon=True)
    thread.start()

setup_logging()
logger = get_logger(__name__)

EXTENSIONS: list[str] = ["cogs.music"]

# The exact set of commands this bot is supposed to register.
# Used at startup to assert the cog loaded everything expected.
EXPECTED_COMMANDS: frozenset[str] = frozenset({
    "play", "pause", "resume", "skip",
    "stop", "queue", "nowplaying", "volume",
})


class MusicBot(commands.Bot):

    def __init__(self) -> None:
        # message_content intent is required to parse any text content;
        # we don't use text commands, but it avoids a gateway warning.
        intents = discord.Intents.default()
        intents.message_content = True

        super().__init__(
            # Null-byte prefix makes the prefix unreachable, effectively
            # disabling legacy text commands without raising exceptions.
            command_prefix="\x00",
            intents=intents,
            help_command=None,
        )

    # ── setup_hook ─────────────────────────────────────────────────────────────
    # Runs once after login, before the bot connects to the Gateway.
    # This is the correct place to load extensions and sync the command tree.

    async def setup_hook(self) -> None:
        # ── Guard: DEV_GUILD_ID is mandatory ──────────────────────────────────
        if not config.DEV_GUILD_ID:
            logger.critical(
                "DEV_GUILD_ID is not set in .env. "
                "This bot uses guild-only command sync to prevent duplicate "
                "commands. Set DEV_GUILD_ID to your server's ID and restart."
            )
            await self.close()
            sys.exit(1)

        # ── Load cogs ─────────────────────────────────────────────────────────
        for ext in EXTENSIONS:
            try:
                await self.load_extension(ext)
                logger.info("Loaded extension: %s", ext)
            except Exception as exc:
                logger.exception("Failed to load extension '%s': %s", ext, exc)
                # A missing cog means the command tree is incomplete.
                # Exit immediately rather than syncing partial commands.
                sys.exit(1)

        # ── Audit: verify the cog registered exactly the expected commands ─────
        registered = {cmd.name for cmd in self.tree.get_commands()}
        missing  = EXPECTED_COMMANDS - registered
        extra    = registered - EXPECTED_COMMANDS

        if missing:
            logger.error("Commands missing from tree: %s", ", ".join(sorted(missing)))
        if extra:
            logger.warning("Unexpected commands in tree: %s", ", ".join(sorted(extra)))
        if not missing and not extra:
            logger.info(
                "Command tree OK — %d commands: %s",
                len(registered),
                ", ".join(f"/{c}" for c in sorted(registered)),
            )

        # ── Guild-only sync ────────────────────────────────────────────────────
        await self._sync_guild_only(config.DEV_GUILD_ID)

    async def _sync_guild_only(self, guild_id: int) -> None:
        """
        Register all commands in `guild_id` and purge the global tree.

        Why both steps are needed
        ──────────────────────────
        Step A — guild sync:
          copy_global_to() makes the global command set visible to
          tree.sync(guild=…).  Without it, guild scope would be empty.

        Step B — global purge:
          If this bot was ever run with a global sync (or DEV_GUILD_ID was
          previously unset), Discord's servers still hold global copies of
          the commands.  Pushing an empty global tree removes them so the
          user never sees both a global copy and a guild copy side-by-side.
        """
        guild_obj = discord.Object(id=guild_id)

        # ── Step A: sync to guild (instant propagation) ────────────────────────
        self.tree.copy_global_to(guild=guild_obj)
        try:
            guild_cmds = await self.tree.sync(guild=guild_obj)
        except discord.HTTPException as exc:
            logger.error("Guild sync failed: %s", exc)
            logger.error(
                "Make sure the bot has the 'applications.commands' scope "
                "in guild %d and that DEV_GUILD_ID is correct.",
                guild_id,
            )
            return

        logger.info(
            "Guild sync complete [guild %d] — %d command(s) registered:",
            guild_id,
            len(guild_cmds),
        )
        for cmd in sorted(guild_cmds, key=lambda c: c.name):
            logger.info("  /%s — %s", cmd.name, cmd.description)

        # ── Step B: purge global commands (removes stale duplicates) ───────────
        self.tree.clear_commands(guild=None)   # wipe the in-memory global scope
        try:
            removed = await self.tree.sync()   # push empty tree → Discord deletes globals
            logger.info(
                "Global command purge complete — %d global command(s) remaining "
                "(should be 0).",
                len(removed),
            )
        except discord.HTTPException as exc:
            logger.warning(
                "Global purge failed (non-fatal): %s. "
                "Global commands may still appear briefly until they expire.",
                exc,
            )

    # ── on_ready ───────────────────────────────────────────────────────────────

    async def on_ready(self) -> None:
        logger.info(
            "Ready — logged in as %s (ID: %d)",
            self.user,
            self.user.id,  # type: ignore[union-attr]
        )
        await self.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.listening,
                name="/play",
            )
        )

    # ── Error handlers ─────────────────────────────────────────────────────────

    async def on_error(self, event_method: str, *args, **kwargs) -> None:
        logger.exception("Unhandled exception in event '%s'", event_method)

    async def on_app_command_error(
        self,
        interaction: discord.Interaction,
        error: app_commands.AppCommandError,
    ) -> None:
        # Unwrap the wrapper discord.py adds around unhandled exceptions
        if isinstance(error, app_commands.CommandInvokeError):
            error = error.original  # type: ignore[assignment]

        logger.error("App command error in /%s: %s",
                     interaction.command.name if interaction.command else "?",
                     error, exc_info=True)

        msg = "An unexpected error occurred. Please try again."
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message(msg, ephemeral=True)
            else:
                await interaction.followup.send(msg, ephemeral=True)
        except discord.HTTPException:
            pass   # interaction already expired — nothing we can do


# ── Entry point ─────────────────────────────────────────────────────────────────

async def main() -> None:
    bot = MusicBot()
    try:
        await bot.start(config.BOT_TOKEN)
    except discord.LoginFailure:
        logger.critical(
            "Invalid BOT_TOKEN. Check your .env file — "
            "copy .env.example and fill in the token from "
            "https://discord.com/developers/applications"
        )
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Shutdown requested — closing.")
        await bot.close()


if __name__ == "__main__":
    keep_alive()
    asyncio.run(main())
