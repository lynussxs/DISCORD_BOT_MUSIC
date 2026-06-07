# Discord Music Bot

A professional, production-ready Discord music bot built with [discord.py](https://discordpy.readthedocs.io/) v2.  
Streams audio from YouTube directly into your voice channel with a per-guild queue and modern embeds.

---

## Commands

| Command | Description |
|---|---|
| `/play <query>` | Search YouTube or paste a URL — starts playing or adds to queue |
| `/pause` | Pause the current track |
| `/resume` | Resume a paused track |
| `/skip` | Skip the current track |
| `/stop` | Clear the queue and disconnect the bot |
| `/queue` | Show the queue with progress bar for the current track |
| `/nowplaying` | Detailed now-playing card with progress bar, volume, requester |
| `/volume <0–100>` | Set the playback volume |

---

## Audio Pipeline

```
/play "song name"
  │
  ├─ yt-dlp resolves query → stream URL + metadata (title, thumbnail, duration)
  │    (runs in thread pool — event loop stays unblocked)
  │
  └─ GuildPlayer._player_loop (background asyncio task):
       ├─ FFmpegOpusAudio.from_probe(url, -filter:a volume=X)
       │    └─ FFmpeg encodes PCM → Opus internally
       │         No system libopus required — FFmpeg ships its own encoder
       ├─ voice_client.play(source)
       ├─ Posts "Now Playing" embed
       └─ Waits for track to end → repeats
            └─ Queue empty for 3 min → auto-disconnect
```

**Why `FFmpegOpusAudio` instead of `FFmpegPCMAudio`?**  
`FFmpegPCMAudio` + `PCMVolumeTransformer` requires the system `libopus` shared library
for Python-side Opus encoding. `FFmpegOpusAudio` tells FFmpeg to output Opus frames
directly — FFmpeg ships its own built-in Opus encoder, so no `libopus.so` is needed.

---

## Project Structure

```
discord-bot/
├── bot.py              # Entry point — bot class, cog loading, slash sync
├── config.py           # All settings loaded from .env
├── .env                # Your secrets (gitignored)
├── .env.example        # Copy this to .env
├── requirements.txt    # Python dependencies
│
├── cogs/
│   └── music.py        # Track, GuildPlayer, Music cog, all 8 commands
│
├── utils/
│   ├── logger.py       # Rotating file + console logging
│   └── helpers.py      # Shared embed builders
│
└── logs/
    └── bot.log         # Auto-created, max 5 MB × 3 files
```

---

## Quick Start

### 1. Create a Discord Application

1. Go to the [Discord Developer Portal](https://discord.com/developers/applications)
2. **New Application** → name it → **Bot** tab → **Add Bot**
3. Copy the **Token**
4. Under **Privileged Gateway Intents**, enable **Message Content Intent**
5. **OAuth2 → URL Generator**:
   - Scopes: `bot` + `applications.commands`
   - Permissions: `Connect`, `Speak`, `Send Messages`, `Embed Links`
6. Open the generated URL to invite the bot to your server

### 2. Install System Dependency

FFmpeg is required for audio processing:

```bash
# Ubuntu / Debian
sudo apt update && sudo apt install ffmpeg

# macOS (Homebrew)
brew install ffmpeg

# Replit
# Already installed as a system dependency — no action needed
```

### 3. Install Python Dependencies

```bash
cd discord-bot
python -m venv .venv
source .venv/bin/activate        # macOS / Linux
# .venv\Scripts\activate         # Windows

pip install -r requirements.txt
```

### 4. Configure

```bash
cp .env.example .env
```

Edit `.env`:

```env
BOT_TOKEN=your_actual_token_here

# For instant slash-command sync during development:
# Right-click your server icon (Developer Mode on) to get your guild ID
DEV_GUILD_ID=your_server_id_here
```

### 5. Run

```bash
python bot.py
```

Expected startup output:
```
2024-01-15 12:00:00 | INFO  | __main__ | Loaded extension: cogs.music
2024-01-15 12:00:01 | INFO  | __main__ | Synced 8 commands to dev guild 123456789
2024-01-15 12:00:01 | INFO  | __main__ | Logged in as MusicBot#1234 (ID: 987654321)
```

---

## Configuration Reference

| Variable | Required | Default | Description |
|---|---|---|---|
| `BOT_TOKEN` | ✅ | — | Bot token from the Developer Portal |
| `DEV_GUILD_ID` | | *(global)* | Guild ID for instant dev sync |
| `DEFAULT_VOLUME` | | `50` | Starting volume (0–100) for new players |
| `LOG_LEVEL` | | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |
| `LOG_FILE` | | `logs/bot.log` | Path to the rotating log file |

---

## Architecture Decisions

| Decision | Reason |
|---|---|
| `FFmpegOpusAudio` over `FFmpegPCMAudio` | No system `libopus` needed — FFmpeg encodes Opus natively |
| `GuildPlayer` per guild | Multiple servers work fully independently |
| `asyncio.Event` in player loop | Clean, non-polling way to advance the queue after each track |
| Volume baked into FFmpeg filter | Avoids `PCMVolumeTransformer` which requires libopus encoding |
| `setup_hook` for initialization | Async cog loading and command sync before gateway connection |
| Rotating log files | Caps disk usage at ~15 MB (5 MB × 3 files), always keeps history |

---

## Deployment Notes

- Remove `DEV_GUILD_ID` in production — global sync takes up to 1 hour on first deploy.
- Use `systemd`, `pm2`, or Docker so the bot restarts on crashes.
- Rotate your token immediately if it is ever exposed.
- FFmpeg must be installed on the production server.
