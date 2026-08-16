# Discord Music Bot 🎵

A polished, production-ready Discord music bot built with discord.py (v2). Streams audio from YouTube into voice channels with per-guild players, queues, and modern embeds. This README documents only features verified in the repository source code.

[![Python](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/)
[![discord.py](https://img.shields.io/badge/discord.py-v2.5-blueviolet.svg)](https://discordpy.readthedocs.io/)
[![Docker](https://img.shields.io/badge/docker-supported-2496ed.svg)](https://www.docker.com/)

---

## Quick overview

- ✅ Per-guild music players (independent queues and state)
- ✅ Play from YouTube via yt-dlp (direct stream URLs)
- ✅ Spotify support (track / album / playlist) with API + fallbacks
- ✅ Queue, queue history and duplicates protection
- ✅ Playback controls: play, pause, resume, skip, stop, seek, volume
- ✅ Audio modes: Bassboost and Nightcore (toggleable)
- ✅ 24/7 mode (prevent auto-disconnect)
- ✅ DJ role system (persistent) to grant broader control
- ✅ Cookie support (top-level `cookies.txt`) to help with YouTube extraction
- ✅ yt-dlp PO-Token provider + plugin sync + Deno download helpers (runtime aids for yt-dlp)
- ✅ Keep‑alive / health Flask endpoints for deployments
- ✅ Dockerfile + Railway / Nixpacks notes included

---

## Commands (slash commands)

All of the commands below are implemented as application (slash) commands in the music cog. The descriptions are taken from the source code.

| Command | Description | Permissions / notes |
|---|---|---|
| `/play <query>` | Search YouTube or paste a YouTube URL — plays or enqueues the result | Guild-only; cooldown per user (1/3s)
| `/pause` | Pause the current track | DJ-check enforced
| `/resume` | Resume the paused track | DJ-check enforced
| `/skip` | Skip the current track | DJ-check enforced
| `/stop` | Stop playback, clear queue, and disconnect | DJ-check enforced
| `/queue` | Show the current song queue (includes current track and progress) | Guild-only
| `/nowplaying` | Show detailed info about the current song (elapsed, requester, volume) | Guild-only
| `/volume <0–100>` | Set the playback volume (0–100) | DJ-check enforced
| `/seek <position>` | Seek to a position inside the current track (e.g., `1:30` or `90`) | DJ-check enforced
| `/history` | Show recent track history (last 10 entries) | Guild-only (ephemeral)
| `/spotify <url|query>` | Play from Spotify — supports track, album, playlist (resolves to YouTube queries) | Guild-only; uses Spotify API if configured, with scraping/fallbacks
| `/247` | Toggle 24/7 mode for the guild (prevent auto-disconnect) | DJ-check enforced
| `/bassboost` | Toggle Bassboost (applies from the next track) | DJ-check enforced; mutually exclusive with nightcore
| `/nightcore` | Toggle Nightcore (speed + pitch; applies from next track) | DJ-check enforced; mutually exclusive with bassboost
| `/djrole` | Set / view / remove a role that can control all tracks (admin command) | `manage_guild` required to set/clear; stored persistently in data/dj_roles.json

(“DJ-check enforced” means commands check ownership/role/DJ config before allowing control — see `cogs/music.py`.)

<details>
<summary>Command behaviour notes (click to expand)</summary>

- `/play` performs a quick defer then resolves the query with yt-dlp (in a thread) and enqueues the resolved Track object. If the bot is idle, it posts a "Fetched" message while loading.
- `/spotify` first attempts to resolve Spotify using a configured Spotify client (spotipy). If Spotify API access is not available or returns 403, the implementation falls back to scraping the track page or oEmbed to build search queries and then resolves them via yt-dlp.
- Duplicate detection: enqueue() returns -1 for duplicates and informs the user.
- Volume changes are applied by updating the player volume; a note in the response explains whether the change takes effect immediately or on the next track.
</details>

---

## Supported platforms & sources

- YouTube via yt-dlp (primary source). The bot resolves queries/URLs to streamable audio URLs used by FFmpeg.
- Spotify (track / album / playlist): repository contains `_resolve_spotify()` that uses the Spotify Web API when SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET are provided, and multiple fallbacks (scraping / oEmbed) when the API is not available.

Note: Spotify track/playlist resolution converts Spotify items to YouTube search queries and then resolves them via yt-dlp — streaming is always performed via YouTube/yt-dlp.

---

## Audio & playback features

- FFmpeg-based playback; the code prefers FFmpeg's Opus output to avoid requiring system libopus.
- Per-guild GuildPlayer objects manage queue, current track, history, volume, flags (bassboost/nightcore/24/7) and a background player loop.
- Queue features:
  - Enqueue single tracks and playlists (Spotify playlists, YouTube playlists via query resolution).
  - Duplicate detection (avoids enqueueing identical tracks).
  - History tracking (recently played tracks stored in memory per player; `/history` shows up to the last 10 played entries).
- Playback features:
  - Seek into the current track (`/seek`).
  - Pause / resume / skip / stop.
  - Volume control (0–100).
- Audio modes:
  - `bassboost` and `nightcore` toggles (mutually exclusive). These set flags on the player; the effects are applied when building the FFmpeg parameters for the next song.
- 24/7 mode: toggles a long idle timeout so the bot remains in voice even when the queue is empty.

---

## Deployment & runtime helpers

- Dockerfile provided: installs `ffmpeg` and runs `python bot.py`. Secrets should not be baked into the image; pass them at runtime via environment variables.
- Railway / Nixpacks: `railway.toml` and `nixpacks.toml` are present — the repository contains notes to support deployments on Railway and similar hosts.
- Keep-alive & health endpoints: the bot includes a small Flask app that serves health & status endpoints to aid platform health checks (the code defines `/`, `/health`, `/ping` and `/status` endpoints).
- yt-dlp runtime helpers:
  - The project attempts to download and/or run auxiliary helpers such as a local bgutil-pot provider (PO-Token provider) and copies plugin files into yt-dlp's plugin directory when available. This is performed in background threads and is non-fatal (the bot continues if these helpers fail).
  - Deno (a JS runtime) download helper is included — newer yt-dlp formats sometimes require a JS runtime for signature solving; the bot downloads a deno binary at runtime if needed.

---

## Configuration (environment variables)

These are the environment variables the code reads at runtime (verified by inspecting `config.py` and other modules). Provide them via `.env` for local development or via your host's secret/config panel.

| Variable | Required | Default | Description |
|---|---:|---|---|
| `BOT_TOKEN` | ✅ | — | Discord bot token from the Developer Portal (required)
| `DEV_GUILD_ID` | | *(optional)* | If set, the bot syncs slash commands instantly to this guild (dev mode). Leave empty to sync globally.
| `DEFAULT_VOLUME` | | `50` | Default volume for new GuildPlayer instances (0–100)
| `LOG_LEVEL` | | `INFO` | Log verbosity: DEBUG / INFO / WARNING / ERROR
| `LOG_FILE` | | `logs/bot.log` | Rotating log file path
| `PORT` | | `8080` | Flask keep-alive server port (deployment health checks)
| `SPOTIFY_CLIENT_ID` | | — | Optional: enable Spotify Web API calls for `/spotify` resolution
| `SPOTIFY_CLIENT_SECRET` | | — | Optional: Spotify client secret paired with the client id

Important: `BOT_TOKEN` is required. `DEV_GUILD_ID` is optional but useful during development (instant command sync to one guild). The repository contains a `.env.example` file — copy that to `.env` and set your values.

> Cookie helper: place a valid `cookies.txt` (exported from your browser) at the repository root to help yt-dlp bypass YouTube bot checks. The code checks `cookies.txt` size and will notify the user when cookies are needed to resolve a query.

---

## Installation (local)

```bash
# Clone
git clone https://github.com/lynussxs/DISCORD_BOT_MUSIC.git
cd DISCORD_BOT_MUSIC

# Python venv
python -m venv .venv
source .venv/bin/activate       # macOS / Linux
# .venv\Scripts\activate      # Windows

pip install -r requirements.txt
cp .env.example .env
# Edit .env and add BOT_TOKEN and other optional variables

# Ensure ffmpeg is installed on the host
# Start the bot
python bot.py
```

---

## Docker

Build and run locally (pass secrets at runtime):

```bash
docker build -t discord-music-bot .
docker run -e BOT_TOKEN="$BOT_TOKEN" -e PORT=8080 --rm discord-music-bot
```

Notes:
- The `Dockerfile` installs `ffmpeg` and then runs `python bot.py`.
- Do not bake secrets into the image. Inject env vars at container start.

---

## Troubleshooting

- "Couldn't find anything" from `/play` → try a more specific query or a direct YouTube URL. If yt-dlp reports a bot-check/DRM error, consider adding a `cookies.txt` exported from a logged-in browser.
- Spotify playlist failing to resolve → the bot will try the Spotify Web API if `SPOTIFY_CLIENT_*` are set; otherwise it falls back to scraping/oEmbed which may return less accurate results for playlists/albums.
- `ffmpeg` missing → the bot requires `ffmpeg` installed on the host or inside the container.
- yt-dlp JS signature / n-signature errors → the bot includes runtime helpers (Deno + bgutil provider) to improve compatibility; check logs for messages about downloading Deno or starting the PO-Token provider.

---

## Project structure (verified)

```
discord-bot/
├── bot.py              # Entry point — bot class, cog loading, Flask health endpoints
├── config.py           # Settings loaded from .env
├── .env.example        # Example env variables
├── requirements.txt
├── Dockerfile
├── railway.toml
├── nixpacks.toml
├── cookies.txt         # Optional cookie file (present in repo)

├── cogs/
│   └── music.py        # Track, GuildPlayer, Music cog — main commands & playback
│
├── utils/
│   ├── logger.py       # Rotating file + console logging
│   └── helpers.py      # Shared embed builders and helpers

└── data/
    └── dj_roles.json?  # Created by the bot when `/djrole` is used
```

---

## Contribution

Contributions are welcome. Please open issues for bugs and feature discussions. For documentation fixes or small improvements, open a PR against `main` (or the provided branch).

---

## Changes in this update

- Expanded README to include every feature verified in code: Spotify, seek, history, 24/7, bassboost, nightcore, djrole, cookies, PO-Token provider, Deno helper, health endpoints, Docker & Railway notes.
- Reorganized configuration and commands into clear tables for easier reading.

---

## License

This repository does not include an explicit license file — check the repo root for `LICENSE` if you require licensing information.
