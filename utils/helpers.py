"""
utils/helpers.py — Shared embed builders for consistent UI across the bot.
"""

import discord


def music_embed(title: str, description: str = "", color: discord.Color | None = None) -> discord.Embed:
    """Standard embed for music responses."""
    return discord.Embed(
        title=title,
        description=description,
        color=color or discord.Color.blurple(),
    )


def success_embed(title: str, description: str = "") -> discord.Embed:
    return discord.Embed(title=f"✅  {title}", description=description, color=discord.Color.green())


def error_embed(title: str, description: str = "") -> discord.Embed:
    return discord.Embed(title=f"❌  {title}", description=description, color=discord.Color.red())
