"""
cogs/music.py — Music playback cog.

Audio pipeline
──────────────
  yt-dlp  →  stream URL + metadata
  FFmpegOpusAudio (direct constructor)  →  decode → volume filter → libopus encode
  discord.VoiceClient  →  Opus frames to Discord gateway

  Why the direct constructor (not from_probe):
    from_probe() detects YouTube's Opus streams and injects -c:a copy.
    -c:a copy is incompatible with -filter:a volume=X.  Direct constructor
    always decodes and re-encodes, so the volume filter works every time.

Interaction-response contract
──────────────────────────────
  Discord's 3-second acknowledgement window is enforced by this rule:
    1. Synchronous checks  →  response.send_message()   (before defer)
    2. defer()             →  must happen within 3 s
    3. All async work      →  interaction.followup.send()   (after defer)

  Button interactions use interaction.response.edit_message() for fast
  in-place updates, and defer() + message.edit() for async operations.

Commands: /play /pause /resume /skip /stop /queue /nowplaying /volume
"""

from __future__ import annotations

import asyncio
import functools
import os
import re
import requests
import time
from typing import Any

import discord
import yt_dlp
from discord import app_commands
from discord.ext import commands

import config
from utils.logger import get_logger

logger = get_logger(__name__)

# ── Colours ────────────────────────────────────────────────────────────────────
COLOUR_PLAY    = 0x1DB954   # Spotify green
COLOUR_PAUSE   = 0xF5A623   # amber
COLOUR_STOP    = 0xED4245   # red
COLOUR_QUEUE   = 0x5865F2   # blurple
COLOUR_SUCCESS = 0x57F287   # mint

# ── Webshare Proxy Manager ─────────────────────────────────────────────────────

class WebshareProxyManager:
    """Tự động rotate proxy từ Webshare API."""

    def __init__(self) -> None:
        self.api_key   = os.environ.get("WEBSHARE_API_KEY", "")
        self._fallback = os.environ.get("PROXY_URL", "http://fywznozi:gv94cmc9t7qs@142.111.67.146:5611")
        self._proxies  : list[dict] = []
        self._idx      : int = 0
        self._dead     : set[str] = set()  # proxy đã biết là chết

    def _fetch(self) -> bool:
        if not self.api_key:
            return False
        try:
            r = requests.get(
                "https://proxy.webshare.io/api/v2/proxy/list/?mode=direct&page=1&page_size=25",
                headers={"Authorization": f"Token {self.api_key}"},
                timeout=10,
            )
            if r.status_code == 200:
                self._proxies = r.json().get("results", [])
                self._idx = 0
                self._dead.clear()
                return bool(self._proxies)
        except Exception:
            pass
        return False

    def _proxy_url(self, p: dict) -> str:
        return f"http://{p['username']}:{p['password']}@{p['proxy_address']}:{p['port']}"

    def _test(self, url: str) -> bool:
        """Test proxy có còn sống không."""
        try:
            r = requests.get(
                "https://www.youtube.com",
                proxies={"http": url, "https": url},
                timeout=8,
            )
            return r.status_code == 200
        except Exception:
            return False

    def get(self) -> str:
        """Lấy proxy — chỉ dùng fallback Japan, không dùng proxy US từ Webshare."""
        return self._fallback

    def mark_dead(self, url: str) -> str:
        """Đánh dấu proxy hiện tại là chết, chuyển sang cái tiếp theo."""
        self._dead.add(url)
        if self._proxies:
            self._idx = (self._idx + 1) % len(self._proxies)
        return self.get()

    def rotate(self) -> str:
        """Chuyển proxy tiếp theo."""
        current = self.get()
        return self.mark_dead(current)

_proxy = WebshareProxyManager()

# ── yt-dlp options ─────────────────────────────────────────────────────────────

def _ytdl_opts(cookies: bool = True) -> dict[str, Any]:
    proxy = _proxy.get()
    opts: dict[str, Any] = {
        "format"         : "249/139/251/140/18/bestaudio/best",
        "default_search" : "ytsearch",
        "noplaylist"     : False,
        "quiet"          : True,
        "no_warnings"    : True,
        "extractor_args" : {
            "youtube": {
                "player_client": ["android", "android_vr", "tv_embedded"] if cookies
                                  else ["android_vr", "tv_embedded"],
            }
        },
        "http_headers": {
            "User-Agent": "com.google.android.youtube/19.09.37 (Linux; U; Android 11) gzip",
        },
    }
    if proxy:
        opts["proxy"] = proxy
    if cookies:
        cp = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "cookies.txt")
        if os.path.exists(cp):
            opts["cookiefile"] = cp
    return opts

# Giữ lại tên cũ để không cần sửa nhiều chỗ — dùng hàm thay vì dict tĩnh
def _get_ytdl_options(cookies: bool = True) -> dict[str, Any]:
    return _ytdl_opts(cookies)

def _ffmpeg_before() -> str:
    return (
        "-reconnect 1 "
        "-reconnect_streamed 1 "
        "-reconnect_delay_max 30 "
        "-reconnect_at_eof 1 "
        "-reconnect_on_network_error 1 "
        "-reconnect_on_http_error 4xx,5xx "
        "-analyzeduration 0 "
        "-timeout 30000000"
    )

# Matches http:// and https:// URLs so we can detect non-URL queries.
_URL_RE = re.compile(r"^https?://", re.IGNORECASE)

# ── Track ──────────────────────────────────────────────────────────────────────

class Track:
    """Metadata + streaming URL for one song, resolved by yt-dlp."""

    def __init__(
        self,
        data: dict[str, Any],
        requester: discord.Member | discord.User,
    ) -> None:
        self.title: str            = data.get("title", "Unknown Title")
        self.url: str              = data.get("url", "")
        self.webpage_url: str      = data.get("webpage_url", self.url)
        self.thumbnail: str | None = data.get("thumbnail")
        self.uploader: str         = data.get("uploader") or data.get("channel", "Unknown")
        self.duration: int         = int(data.get("duration") or 0)
        self.requester             = requester
        self.webpage_url: str      = data.get("webpage_url", self.url)
        self.thumbnail: str | None = data.get("thumbnail")
        self.uploader: str         = data.get("uploader") or data.get("channel", "Unknown")
        self.duration: int         = int(data.get("duration") or 0)
        self.requester             = requester

    @classmethod
    async def from_query(
        cls,
        query: str,
        requester: discord.Member | discord.User,
        loop: asyncio.AbstractEventLoop,
    ) -> "Track":
        """
        Resolve a search term or YouTube URL via yt-dlp (runs in a thread pool).

        If the query is not a URL, prepend 'ytsearch:' to force a YouTube
        search rather than relying on the default_search fallback.  This
        improves result quality for artist/title searches.
        """
        resolved = query if _URL_RE.match(query) else f"ytsearch1:{query}"

        # Bước 1: Search tối giản để lấy video ID
        search_opts = {
            "default_search" : "ytsearch",
            "noplaylist"     : False,
            "quiet"          : True,
            "no_warnings"    : True,
            "extract_flat"   : True,
        }
        with yt_dlp.YoutubeDL(search_opts) as ytdl:
            partial = functools.partial(ytdl.extract_info, resolved, download=False)
            search_data: dict[str, Any] = await loop.run_in_executor(None, partial)

        if "entries" in search_data:
            entries = [e for e in search_data["entries"] if e]
            if not entries:
                raise ValueError(f"No results found for: {query}")
            entry = entries[0]
            video_url = entry.get("url") or entry.get("webpage_url") or f"https://www.youtube.com/watch?v={entry['id']}"
        else:
            video_url = resolved

        # Bước 2: Lấy stream URL — thử tối đa 4 lần với proxy khác nhau
        last_err: Exception | None = None
        for attempt in range(4):
            use_cookies = attempt == 0  # lần đầu dùng cookies, sau đó không
            opts = _ytdl_opts(use_cookies)
            try:
                with yt_dlp.YoutubeDL(opts) as ytdl:
                    partial = functools.partial(ytdl.extract_info, video_url, download=False)
                    data: dict[str, Any] = await loop.run_in_executor(None, partial)
                break  # thành công
            except Exception as e:
                last_err = e
                err_str = str(e)
                is_retryable = any(x in err_str for x in [
                    "Requested format", "Sign in", "bot", "403", "Connection refused",
                    "Connection reset", "Unable to download"
                ])
                if is_retryable and attempt < 3:
                    current = opts.get("proxy", "")
                    new_proxy = _proxy.mark_dead(current) if current else _proxy.get()
                    logger.warning(
                        "yt-dlp failed (attempt %d), switching proxy to: %s",
                        attempt + 1, new_proxy[:40] if new_proxy else "none",
                    )
                    continue
                raise last_err  # type: ignore

        return cls(data, requester)

    @property
    def duration_str(self) -> str:
        h, r   = divmod(self.duration, 3600)
        m, sec = divmod(r, 60)
        return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"

    async def make_source(self, volume: float) -> discord.FFmpegOpusAudio:
        """
        Build an FFmpegOpusAudio source with the given volume baked in.

        Uses the direct constructor (not from_probe) because from_probe
        detects Opus input and injects -c:a copy, which is incompatible
        with -filter:a volume=X (FFmpeg error: filtering and streamcopy
        cannot be used together).  The direct constructor always uses
        libopus re-encoding, so the volume filter works correctly.

        Effective FFmpeg command:
            ffmpeg -reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5
                   -i <url>
                   -c:a libopus -b:a 128k
                   -vn -filter:a volume=<X>
                   -f opus pipe:1
        """
        safe_vol = max(0.01, min(2.0, volume))
        return discord.FFmpegOpusAudio(
            self.url,
            before_options=_ffmpeg_before(),
            options=f"-vn -filter:a volume={safe_vol:.3f} -b:a 64k",  # 64k đủ nghe, nhẹ hơn qua proxy
        )


# ── Embed helpers ──────────────────────────────────────────────────────────────

def _fmt(s: int) -> str:
    h, r   = divmod(s, 3600)
    m, sec = divmod(r, 60)
    return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"


def _progress_bar(elapsed: int, total: int, length: int = 17) -> str:
    if total <= 0:
        return f"`{'▬' * length}`  🔴 Live"
    ratio  = min(elapsed / total, 1.0)
    filled = int(length * ratio)
    return f"`{'▬' * filled}🔘{'▬' * (length - filled)}`  {_fmt(elapsed)} / {_fmt(total)}"


def _vol_bar(pct: int, n: int = 10) -> str:
    filled = round(n * pct / 100)
    bar    = "█" * filled + "░" * (n - filled)
    icon   = "🔇" if pct == 0 else ("🔈" if pct < 40 else ("🔉" if pct < 70 else "🔊"))
    return f"{icon} `{bar}` **{pct}%**"


def _e_err(title: str, desc: str = "") -> discord.Embed:
    return discord.Embed(title=f"❌  {title}", description=desc, colour=COLOUR_STOP)


def _e_np(
    track: Track,
    elapsed: int,
    vol_pct: int,
    q_len: int,
    paused: bool,
    loop: bool = False,
    shuffle: bool = False,
) -> discord.Embed:
    icon   = "⏸" if paused else "▶️"
    colour = COLOUR_PAUSE if paused else COLOUR_PLAY

    e = discord.Embed(colour=colour)
    e.set_author(name="🎵 MUSIC PANEL")
    e.description = f"### [{track.title}]({track.webpage_url})"
    e.add_field(name="👤 Requested By", value=track.requester.mention, inline=True)
    e.add_field(name="⏱ Music Duration", value=f"`{track.duration_str}`", inline=True)
    e.add_field(name="🎤 Music Author", value=f"`{track.uploader}`", inline=True)
    e.add_field(
        name  = "Progress",
        value = _progress_bar(elapsed, track.duration),
        inline= False,
    )
    flags = []
    if loop:    flags.append("🔁 Loop ON")
    if shuffle: flags.append("🔀 Shuffle ON")
    if flags:
        e.add_field(name="Mode", value="  ".join(flags), inline=False)
    if track.thumbnail:
        e.set_thumbnail(url=track.thumbnail)
    e.set_footer(text=f"🔊 Volume: {vol_pct}%  •  Queue: {q_len} track(s)")
    return e


def _e_queued(track: Track, pos: int) -> discord.Embed:
    e = discord.Embed(
        title       = "➕  Added to Queue",
        description = f"### [{track.title}]({track.webpage_url})",
        colour      = COLOUR_QUEUE,
    )
    e.add_field(name="Position",     value=f"`#{pos}`",               inline=True)
    e.add_field(name="Duration",     value=f"`{track.duration_str}`", inline=True)
    e.add_field(name="Requested by", value=track.requester.mention,   inline=True)
    if track.thumbnail:
        e.set_thumbnail(url=track.thumbnail)
    return e


# ── MusicControlView ───────────────────────────────────────────────────────────

class MusicControlView(discord.ui.View):
    """
    Music control panel giống Lara bot.

    Layout
    ───────
      Row 0:  🔉 Down  |  ⏮ Back  |  ⏸ Pause  |  ⏭ Skip
      Row 1:  🔊 Up
      Row 2:  🔀 Shuffle  |  🔁 Loop  |  ⏹ Stop
      Row 3:  ▶️ AutoPlay  |  📋 Playlist
    """

    def __init__(self, player: "GuildPlayer") -> None:
        super().__init__(timeout=86_400)
        self.player = player
        self._sync()

    def _sync(self) -> None:
        p      = self.player
        has    = p.current is not None
        paused = p.vc.is_paused() if p.vc and p.vc.is_connected() else False
        for item in self.children:
            if not isinstance(item, discord.ui.Button):
                continue
            cid = item.custom_id or ""
            if cid == "music_pp":
                item.disabled = not has
                item.emoji    = discord.PartialEmoji.from_str("▶️" if paused else "⏸")
                item.label    = "Resume" if paused else "Pause"
            elif cid in ("music_skip", "music_stop", "music_back"):
                item.disabled = not has
            elif cid == "music_loop":
                item.style = discord.ButtonStyle.success if p.loop else discord.ButtonStyle.secondary
            elif cid == "music_shuffle":
                item.style = discord.ButtonStyle.success if p.shuffle else discord.ButtonStyle.secondary
            elif cid == "music_autoplay":
                item.style = discord.ButtonStyle.success if p.autoplay else discord.ButtonStyle.secondary

    def _build_embed(self) -> discord.Embed:
        p = self.player
        if p.current:
            return _e_np(
                p.current,
                p.elapsed,
                round(p.volume * 100),
                len(p.queue),
                p.vc.is_paused() if p.vc else False,
                loop    = p.loop,
                shuffle = p.shuffle,
            )
        return discord.Embed(
            title       = "⏹  Stopped",
            description = "No track is currently playing.",
            colour      = COLOUR_STOP,
        )

    async def _quick_edit(self, interaction: discord.Interaction) -> None:
        self._sync()
        await interaction.response.edit_message(embed=self._build_embed(), view=self)

    def _disable_all(self) -> None:
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                item.disabled = True

    # ── Row 0 ──────────────────────────────────────────────────────────────────

    @discord.ui.button(emoji="🔉", label="Down", style=discord.ButtonStyle.secondary, custom_id="music_vdn", row=0)
    async def btn_vol_down(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        new_pct = max(0, round(self.player.volume * 100) - 10)
        self.player.set_volume(new_pct)
        logger.info("BUTTON vol − → %d%% [guild %d]", new_pct, self.player.vc.guild.id)
        await self._quick_edit(interaction)

    @discord.ui.button(emoji="⏮", label="Back", style=discord.ButtonStyle.secondary, custom_id="music_back", row=0)
    async def btn_back(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        p = self.player
        if not p.current or not p._history:
            await interaction.response.defer()
            return
        # Đưa bài hiện tại về đầu queue, lấy bài trước từ history
        p._queue.insert(0, p.current)
        prev = p._history.pop()
        p._queue.insert(0, prev)
        p.skip()
        self._disable_all()
        await interaction.response.edit_message(
            embed=discord.Embed(title="⏮  Back", description=f"Playing previous track…", colour=COLOUR_SUCCESS),
            view=self,
        )
        logger.info("BUTTON back [guild %d]", p.vc.guild.id)

    @discord.ui.button(emoji="⏸", label="Pause", style=discord.ButtonStyle.secondary, custom_id="music_pp", row=0)
    async def btn_pp(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        p = self.player
        if not p.current:
            await interaction.response.defer()
            return
        if p.vc.is_paused():
            p.resume()
            logger.info("BUTTON resume [guild %d]", p.vc.guild.id)
        else:
            p.pause()
            logger.info("BUTTON pause [guild %d]", p.vc.guild.id)
        await self._quick_edit(interaction)

    @discord.ui.button(emoji="⏭", label="Skip", style=discord.ButtonStyle.secondary, custom_id="music_skip", row=0)
    async def btn_skip(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        p = self.player
        if not p.current:
            await interaction.response.defer()
            return
        title = p.current.title
        p.skip()
        logger.info("BUTTON skip → '%s' [guild %d]", title, p.vc.guild.id)
        self._disable_all()
        await interaction.response.edit_message(
            embed=discord.Embed(title="⏭  Skipped", description=f"Skipped **{title}**.\nLoading next track…", colour=COLOUR_SUCCESS),
            view=self,
        )

    # ── Row 1 ──────────────────────────────────────────────────────────────────

    @discord.ui.button(emoji="🔊", label="Up", style=discord.ButtonStyle.secondary, custom_id="music_vup", row=1)
    async def btn_vol_up(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        new_pct = min(100, round(self.player.volume * 100) + 10)
        self.player.set_volume(new_pct)
        logger.info("BUTTON vol + → %d%% [guild %d]", new_pct, self.player.vc.guild.id)
        await self._quick_edit(interaction)

    # ── Row 2 ──────────────────────────────────────────────────────────────────

    @discord.ui.button(emoji="🔀", label="Shuffle", style=discord.ButtonStyle.secondary, custom_id="music_shuffle", row=2)
    async def btn_shuffle(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        p = self.player
        p.shuffle = not p.shuffle
        logger.info("BUTTON shuffle → %s [guild %d]", p.shuffle, p.vc.guild.id)
        await self._quick_edit(interaction)

    @discord.ui.button(emoji="🔁", label="Loop", style=discord.ButtonStyle.secondary, custom_id="music_loop", row=2)
    async def btn_loop(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        p = self.player
        p.loop = not p.loop
        logger.info("BUTTON loop → %s [guild %d]", p.loop, p.vc.guild.id)
        await self._quick_edit(interaction)

    @discord.ui.button(emoji="⏹", label="Stop", style=discord.ButtonStyle.danger, custom_id="music_stop", row=2)
    async def btn_stop(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        self._disable_all()
        await interaction.response.edit_message(
            embed=discord.Embed(title="⏹  Stopping…", description="Disconnecting from voice channel.", colour=COLOUR_STOP),
            view=self,
        )
        gid = self.player.vc.guild.id
        await self.player.stop()
        logger.info("BUTTON stop [guild %d]", gid)

    # ── Row 3 ──────────────────────────────────────────────────────────────────

    @discord.ui.button(emoji="▶️", label="AutoPlay", style=discord.ButtonStyle.secondary, custom_id="music_autoplay", row=3)
    async def btn_autoplay(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        p = self.player
        p.autoplay = not p.autoplay
        logger.info("BUTTON autoplay → %s [guild %d]", p.autoplay, p.vc.guild.id)
        await self._quick_edit(interaction)

    @discord.ui.button(emoji="📋", label="Playlist", style=discord.ButtonStyle.secondary, custom_id="music_playlist", row=3)
    async def btn_playlist(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        p = self.player
        if not p._queue:
            await interaction.response.send_message(
                embed=discord.Embed(title="📋 Queue", description="Queue is empty!", colour=COLOUR_QUEUE),
                ephemeral=True,
            )
            return
        desc = "\n".join(
            f"`{i+1}.` [{t.title}]({t.webpage_url}) — `{t.duration_str}`"
            for i, t in enumerate(p._queue[:10])
        )
        if len(p._queue) > 10:
            desc += f"\n*... and {len(p._queue) - 10} more*"
        await interaction.response.send_message(
            embed=discord.Embed(title=f"📋 Queue ({len(p._queue)} tracks)", description=desc, colour=COLOUR_QUEUE),
            ephemeral=True,
        )
        logger.info("BUTTON playlist [guild %d]", p.vc.guild.id)


# ── GuildPlayer ────────────────────────────────────────────────────────────────

class GuildPlayer:
    """
    Per-guild voice client, queue, and background playback loop.

    Lifecycle
    ──────────
    Created by /play when the bot first joins a channel.
    Destroyed by /stop or on_voice_state_update (force-kick).
    The background task (_player_loop) is cancelled on destroy.

    Volume
    ───────
    self.volume is the single source of truth.  It is used when a new source
    is created (volume baked into FFmpeg filter) and shown in the UI.
    Volume changes via set_volume() are reflected in the UI immediately but
    only affect audio at the start of the next track.
    """

    IDLE_TIMEOUT = 180  # seconds of silence before auto-disconnect

    def __init__(
        self,
        vc: discord.VoiceClient,
        text_ch: discord.abc.Messageable,
        loop: asyncio.AbstractEventLoop,
        volume: float,
    ) -> None:
        self.vc      = vc
        self.text_ch = text_ch
        self._loop   = loop

        self._queue: list[Track]   = []
        self.current: Track | None = None
        self.volume: float         = volume
        self._start: float | None  = None
        self.loop: bool            = False   # loop current track
        self.shuffle: bool         = False   # shuffle queue
        self.autoplay: bool        = False   # autoplay related tracks
        self._history: list[Track] = []      # for back button

        # References to the last "Now Playing" message + view so we can
        # disable its buttons when the track ends or the bot disconnects.
        self._np_msg:  discord.Message | None   = None
        self._np_view: MusicControlView | None  = None

        self._next: asyncio.Event = asyncio.Event()
        self._task: asyncio.Task  = loop.create_task(self._player_loop())

    # ── Queue ──────────────────────────────────────────────────────────────────

    def enqueue(self, track: Track) -> int:
        """Append a track and return its 1-based queue position."""
        self._queue.append(track)
        # Wake the player loop if it is idle-waiting.
        if not self.vc.is_playing() and not self.vc.is_paused():
            self._next.set()
        return len(self._queue)

    @property
    def queue(self) -> list[Track]:
        return list(self._queue)

    @property
    def elapsed(self) -> int:
        return int(time.monotonic() - self._start) if self._start else 0

    # ── Controls ───────────────────────────────────────────────────────────────

    def pause(self) -> bool:
        if self.vc.is_playing():
            self.vc.pause()
            return True
        return False

    def resume(self) -> bool:
        if self.vc.is_paused():
            self.vc.resume()
            return True
        return False

    def skip(self) -> bool:
        if self.vc.is_playing() or self.vc.is_paused():
            self.vc.stop()   # fires _after → _next.set()
            return True
        return False

    def set_volume(self, pct: int) -> None:
        """
        Set the target volume for the next track (0–100 → 0.01–2.0 internally).

        The current track's audio is not affected because its volume is already
        baked into the FFmpeg filter at source-creation time.  The updated
        self.volume is reflected in the UI immediately and applied when the
        next source is created.
        """
        self.volume = max(0.01, min(2.0, pct / 100))

    async def stop(self) -> None:
        """
        Full teardown: clear queue, cancel the playback task, disconnect voice.

        Order matters:
          1. Clear queue + current so the loop cannot start a new track if it
             wakes between cancel and task completion.
          2. Cancel the task — throws CancelledError at the loop's next await.
          3. Await the task — ensures it has exited before we touch the VC.
          4. Stop audio — safe now that the task is not using the VC.
          5. Disconnect — clean voice leave.
        """
        self._queue.clear()
        self.current = None

        self._task.cancel()
        try:
            await self._task
        except (asyncio.CancelledError, Exception):
            pass

        # Task is done — VoiceClient is no longer used by the loop.
        if self.vc.is_playing() or self.vc.is_paused():
            self.vc.stop()
        if self.vc.is_connected():
            await self.vc.disconnect()
            logger.info("STOP | disconnected from voice [guild %d]", self.vc.guild.id)

    # ── Playback loop ──────────────────────────────────────────────────────────

    async def _player_loop(self) -> None:
        """
        Background task that drives playback.

        Waits for tracks, creates FFmpeg sources, manages the _next event,
        and posts / updates the 'Now Playing' message with control buttons.
        """
        try:
            while True:
                self._next.clear()

                # ── Idle wait ─────────────────────────────────────────────────
                if not self._queue:
                    try:
                        await asyncio.wait_for(
                            self._next.wait(), timeout=self.IDLE_TIMEOUT
                        )
                    except asyncio.TimeoutError:
                        logger.info(
                            "IDLE | auto-disconnect after %ds [guild %d]",
                            self.IDLE_TIMEOUT, self.vc.guild.id,
                        )
                        await self.text_ch.send(
                            embed=discord.Embed(
                                title       = "👋  Disconnected",
                                description = (
                                    f"Left the voice channel after "
                                    f"**{self.IDLE_TIMEOUT // 60} min** of inactivity."
                                ),
                                colour=COLOUR_QUEUE,
                            )
                        )
                        await self.vc.disconnect()
                        return

                if not self._queue:
                    continue

                # ── Pop next track (shuffle support) ───────────────────────────
                if self.shuffle and len(self._queue) > 1:
                    import random
                    idx = random.randrange(len(self._queue))
                    track = self._queue.pop(idx)
                else:
                    track = self._queue.pop(0)
                self.current = track
                self._start  = time.monotonic()
                logger.info(
                    "PLAY | '%s' (%.0fs) vol=%.0f%% queue=%d [guild %d]",
                    track.title, track.duration, self.volume * 100,
                    len(self._queue), self.vc.guild.id,
                )

                # ── Create audio source ────────────────────────────────────────
                try:
                    source = await track.make_source(self.volume)
                except Exception as exc:
                    logger.error(
                        "SOURCE | failed for '%s': %s [guild %d]",
                        track.title, exc, self.vc.guild.id,
                    )
                    await self.text_ch.send(
                        embed=_e_err(
                            "Playback Error",
                            f"Could not load **{track.title}**.\n`{exc}`\nSkipping…",
                        )
                    )
                    self.current = None
                    self._start  = None
                    self._next.set()
                    continue

                # ── Closure-safe _after callback ───────────────────────────────
                # Capture loop-local references at definition time so the
                # closure remains valid even if self._next is replaced.
                _loop = self._loop
                _ev   = self._next
                _403_flag = [False]

                def _after(err: Exception | None, _l: asyncio.AbstractEventLoop = _loop, _e: asyncio.Event = _ev) -> None:
                    if err:
                        logger.error("VC after-error: %s", err)
                        if "403" in str(err) or "Forbidden" in str(err):
                            _403_flag[0] = True
                    _l.call_soon_threadsafe(_e.set)

                self.vc.play(source, after=_after)

                # ── Now Playing message with control buttons ────────────────────
                view = MusicControlView(self)
                self._np_view = view
                try:
                    self._np_msg = await self.text_ch.send(
                        embed=_e_np(
                            track,
                            elapsed = 0,
                            vol_pct = round(self.volume * 100),
                            q_len   = len(self._queue),
                            paused  = False,
                            loop    = self.loop,
                            shuffle = self.shuffle,
                        ),
                        view=view,
                    )
                except discord.HTTPException as exc:
                    logger.warning("NP message failed: %s", exc)
                    self._np_msg = None

                # ── Wait for track to finish ───────────────────────────────────
                await self._next.wait()

                # Notify if 403
                elapsed = int(time.monotonic() - self._start) if self._start else 0
                if _403_flag[0] or (elapsed < 3 and not self.vc.is_playing()):
                    try:
                        await self.text_ch.send(
                            embed=_e_err(
                                "⚠️ Không thể phát",
                                f"**{track.title}** bị chặn bởi YouTube (403).\nVideo này bị hạn chế theo khu vực hoặc bản quyền. Thử bài khác nhé!",
                            )
                        )
                    except discord.HTTPException:
                        pass
                else:
                    # Lưu vào history cho back button (tối đa 10 bài)
                    self._history.append(track)
                    if len(self._history) > 10:
                        self._history.pop(0)

                    # Loop: đưa lại bài vừa xong vào đầu queue
                    if self.loop:
                        self._queue.insert(0, track)

                # ── Disable buttons on the finished NP card ────────────────────
                await self._expire_np_message()

                self._start  = None
                self.current = None

        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Unhandled error in player loop [guild %d]", self.vc.guild.id)

    async def _expire_np_message(self) -> None:
        """Disable all buttons on the last NP message after a track ends."""
        if self._np_msg and self._np_view:
            try:
                self._np_view._disable_all()
                await self._np_msg.edit(view=self._np_view)
            except discord.NotFound:
                pass   # message was deleted
            except discord.HTTPException as exc:
                logger.debug("Could not expire NP message: %s", exc)
        self._np_msg  = None
        self._np_view = None


# ── Music Cog ──────────────────────────────────────────────────────────────────

class Music(commands.Cog):
    """All music slash commands.  One GuildPlayer per active guild."""

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self._players: dict[int, GuildPlayer] = {}

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _get_player(self, guild_id: int) -> GuildPlayer | None:
        """Return the player for this guild, or None if not connected/initialised."""
        p = self._players.get(guild_id)
        if p and not p.vc.is_connected():
            del self._players[guild_id]
            return None
        return p

    def _voice_precheck(self, interaction: discord.Interaction) -> str | None:
        """
        Synchronous voice pre-checks for /play.

        Returns an error message string if anything is wrong, None if OK.
        Must be called BEFORE defer() so response.send_message() is still valid.
        """
        user = interaction.user
        if not isinstance(user, discord.Member):
            return "Use this command inside a server."
        if not user.voice or not user.voice.channel:
            return "Join a voice channel first, then use `/play`."
        guild = interaction.guild
        if guild is None:
            return "Use this command inside a server."
        existing = guild.voice_client
        if (
            isinstance(existing, discord.VoiceClient)
            and existing.is_connected()
            and existing.channel.id != user.voice.channel.id
        ):
            return (
                f"I'm already in <#{existing.channel.id}>. "
                "Join that channel or use `/stop` first."
            )
        return None

    # ── /play ──────────────────────────────────────────────────────────────────

    @app_commands.command(name="play", description="Play a song — search by name or paste a YouTube URL.")
    @app_commands.describe(query="Song name or YouTube URL")
    @app_commands.guild_only()
    async def play(self, interaction: discord.Interaction, query: str) -> None:
        # ── 1. Fast voice checks (before defer) ───────────────────────────────
        err = self._voice_precheck(interaction)
        if err:
            await interaction.response.send_message(
                embed=_e_err("Cannot Play", err), ephemeral=True
            )
            return

        # ── 2. Defer — yt-dlp can take several seconds ────────────────────────
        await interaction.response.defer()

        user  = interaction.user
        guild = interaction.guild
        assert isinstance(user, discord.Member) and guild is not None

        # ── 3. Connect to voice if not already connected ───────────────────────
        # Check is_connected() explicitly — guild.voice_client can return a
        # stale VoiceClient after /stop (disconnect takes a moment to propagate).
        existing_vc = guild.voice_client
        if isinstance(existing_vc, discord.VoiceClient) and existing_vc.is_connected():
            voice_client = existing_vc
        else:
            try:
                voice_client = await user.voice.channel.connect(self_deaf=True)  # type: ignore[union-attr]
                logger.info(
                    "CONNECT | joined #%s [guild %d]",
                    user.voice.channel.name, guild.id,  # type: ignore[union-attr]
                )
            except discord.ClientException as exc:
                # Already connected but in a different state — rare edge case.
                await interaction.followup.send(
                    embed=_e_err("Connection Failed", f"`{exc}`"),
                    ephemeral=True,
                )
                return
            except Exception as exc:
                logger.error("Voice connect failed: %s", exc)
                await interaction.followup.send(
                    embed=_e_err(
                        "Connection Failed",
                        f"Could not join your voice channel.\n`{exc}`",
                    ),
                    ephemeral=True,
                )
                return

        # ── 4. Resolve query via yt-dlp (thread pool) ─────────────────────────
        try:
            track = await Track.from_query(query, user, self.bot.loop)
        except yt_dlp.utils.DownloadError as exc:
            logger.warning("yt-dlp DownloadError for '%s': %s", query, exc)
            await interaction.followup.send(
                embed=_e_err(
                    "Not Found",
                    f"Couldn't find anything for **{query}**.\n"
                    "Try a more specific search term or a direct YouTube URL.",
                ),
                ephemeral=True,
            )
            return
        except Exception as exc:
            logger.exception("yt-dlp unexpected error for '%s'", query)
            await interaction.followup.send(
                embed=_e_err("Error", f"Something went wrong:\n`{exc}`"),
                ephemeral=True,
            )
            return

        # ── 5. Get or create the guild player ─────────────────────────────────
        guild_id = interaction.guild_id
        assert guild_id is not None

        player = self._get_player(guild_id)
        if player is None:
            player = GuildPlayer(
                vc      = voice_client,
                text_ch = interaction.channel,  # type: ignore[arg-type]
                loop    = self.bot.loop,
                volume  = config.DEFAULT_VOLUME / 100,
            )
            self._players[guild_id] = player

        position = player.enqueue(track)
        logger.info(
            "ENQUEUE | '%s' pos=%d queue=%d [guild %d] by %s",
            track.title, position, len(player.queue), guild_id, user,
        )

        # ── 6. Reply ──────────────────────────────────────────────────────────
        if player.vc.is_playing() or player.vc.is_paused():
            await interaction.followup.send(embed=_e_queued(track, position))
        else:
            await interaction.followup.send(
                embed=discord.Embed(
                    title       = "🔎  Fetched",
                    description = f"Loading **[{track.title}]({track.webpage_url})**…",
                    colour      = COLOUR_SUCCESS,
                )
            )

    # ── /pause ─────────────────────────────────────────────────────────────────

    @app_commands.command(name="pause", description="Pause the current song.")
    @app_commands.guild_only()
    async def pause(self, interaction: discord.Interaction) -> None:
        p = self._get_player(interaction.guild_id)  # type: ignore[arg-type]
        if not p or not p.current:
            await interaction.response.send_message(
                embed=_e_err("Nothing Playing", "There's nothing to pause."),
                ephemeral=True,
            )
            return

        if p.pause():
            await interaction.response.send_message(
                embed=discord.Embed(
                    title       = "⏸  Paused",
                    description = f"**[{p.current.title}]({p.current.webpage_url})**",
                    colour      = COLOUR_PAUSE,
                )
            )
            logger.info("PAUSE | '%s' [guild %d]", p.current.title, interaction.guild_id)
        else:
            await interaction.response.send_message(
                embed=_e_err("Already Paused", "Use `/resume` to continue."),
                ephemeral=True,
            )

    # ── /resume ────────────────────────────────────────────────────────────────

    @app_commands.command(name="resume", description="Resume the paused song.")
    @app_commands.guild_only()
    async def resume(self, interaction: discord.Interaction) -> None:
        p = self._get_player(interaction.guild_id)  # type: ignore[arg-type]
        if not p or not p.current:
            await interaction.response.send_message(
                embed=_e_err("Nothing Paused", "There's nothing to resume."),
                ephemeral=True,
            )
            return

        if p.resume():
            await interaction.response.send_message(
                embed=discord.Embed(
                    title       = "▶️  Resumed",
                    description = f"**[{p.current.title}]({p.current.webpage_url})**",
                    colour      = COLOUR_PLAY,
                )
            )
            logger.info("RESUME | '%s' [guild %d]", p.current.title, interaction.guild_id)
        else:
            await interaction.response.send_message(
                embed=_e_err("Not Paused", "Playback isn't paused right now."),
                ephemeral=True,
            )

    # ── /skip ──────────────────────────────────────────────────────────────────

    @app_commands.command(name="skip", description="Skip the current song.")
    @app_commands.guild_only()
    async def skip(self, interaction: discord.Interaction) -> None:
        p = self._get_player(interaction.guild_id)  # type: ignore[arg-type]
        if not p or not p.current:
            await interaction.response.send_message(
                embed=_e_err("Nothing Playing", "There's nothing to skip."),
                ephemeral=True,
            )
            return

        skipped = p.current.title
        nxt     = p.queue[0].title if p.queue else None
        p.skip()

        desc = f"Skipped **{skipped}**."
        if nxt:
            desc += f"\nUp next: **{nxt}**"
        else:
            desc += "\nQueue is now empty."

        await interaction.response.send_message(
            embed=discord.Embed(
                title       = "⏭  Skipped",
                description = desc,
                colour      = COLOUR_SUCCESS,
            )
        )
        logger.info("SKIP | '%s' next='%s' [guild %d]", skipped, nxt, interaction.guild_id)

    # ── /stop ──────────────────────────────────────────────────────────────────

    @app_commands.command(name="stop", description="Stop playback, clear the queue, and disconnect.")
    @app_commands.guild_only()
    async def stop(self, interaction: discord.Interaction) -> None:
        gid = interaction.guild_id
        assert gid is not None

        p = self._get_player(gid)
        if not p:
            await interaction.response.send_message(
                embed=_e_err("Not Connected", "I'm not in a voice channel."),
                ephemeral=True,
            )
            return

        # Defer immediately — stop() awaits task cancellation and voice disconnect.
        await interaction.response.defer()
        await p.stop()
        self._players.pop(gid, None)   # pop instead of del — safe if already removed

        await interaction.followup.send(
            embed=discord.Embed(
                title       = "⏹  Stopped",
                description = "Cleared the queue and disconnected.",
                colour      = COLOUR_STOP,
            )
        )
        logger.info("STOP | /stop command [guild %d]", gid)

    # ── /queue ─────────────────────────────────────────────────────────────────

    @app_commands.command(name="queue", description="Show the current song queue.")
    @app_commands.guild_only()
    async def queue_cmd(self, interaction: discord.Interaction) -> None:
        p = self._get_player(interaction.guild_id)  # type: ignore[arg-type]
        if not p or (not p.current and not p.queue):
            await interaction.response.send_message(
                embed=discord.Embed(
                    title       = "📋  Queue is Empty",
                    description = "Use `/play <song>` to add something.",
                    colour      = COLOUR_QUEUE,
                ),
                ephemeral=True,
            )
            return

        lines: list[str] = []

        if p.current:
            t    = p.current
            icon = "⏸" if p.vc.is_paused() else "▶️"
            lines.append(
                f"{icon}  **[{t.title}]({t.webpage_url})**  `{t.duration_str}`\n"
                f"\u00a0\u00a0\u00a0{_progress_bar(p.elapsed, t.duration, 16)}"
            )

        upcoming = p.queue[:10]
        if upcoming:
            lines.append("")
            for i, t in enumerate(upcoming, 1):
                lines.append(
                    f"`{i:>2}.`  [{t.title}]({t.webpage_url})"
                    f"  `{t.duration_str}`  — {t.requester.mention}"
                )

        overflow = len(p.queue) - len(upcoming)
        if overflow > 0:
            lines.append(f"\n*…and **{overflow}** more track(s)*")

        embed = discord.Embed(
            title       = "📋  Queue",
            description = "\n".join(lines),
            colour      = COLOUR_QUEUE,
        )
        embed.set_footer(text=f"{len(p.queue)} track(s) waiting  •  /skip to advance")
        await interaction.response.send_message(embed=embed)

    # ── /nowplaying ────────────────────────────────────────────────────────────

    @app_commands.command(name="nowplaying", description="Show detailed info about the current song.")
    @app_commands.guild_only()
    async def nowplaying(self, interaction: discord.Interaction) -> None:
        p = self._get_player(interaction.guild_id)  # type: ignore[arg-type]
        if not p or not p.current:
            await interaction.response.send_message(
                embed=_e_err("Nothing Playing", "No track is currently playing."),
                ephemeral=True,
            )
            return

        await interaction.response.send_message(
            embed=_e_np(
                p.current,
                elapsed = p.elapsed,
                vol_pct = round(p.volume * 100),
                q_len   = len(p.queue),
                paused  = p.vc.is_paused(),
            )
        )

    # ── /volume ────────────────────────────────────────────────────────────────

    @app_commands.command(name="volume", description="Set the playback volume (0–100).")
    @app_commands.describe(level="Volume level from 0 (mute) to 100 (max)")
    @app_commands.guild_only()
    async def volume(
        self,
        interaction: discord.Interaction,
        level: app_commands.Range[int, 0, 100],
    ) -> None:
        p = self._get_player(interaction.guild_id)  # type: ignore[arg-type]
        if not p:
            await interaction.response.send_message(
                embed=_e_err("Not Connected", "I'm not in a voice channel."),
                ephemeral=True,
            )
            return

        p.set_volume(level)

        note = (
            "\n-# Takes effect on the next track."
            if p.current else ""
        )
        await interaction.response.send_message(
            embed=discord.Embed(
                title       = "🔊  Volume Updated",
                description = _vol_bar(level) + note,
                colour      = COLOUR_SUCCESS,
            )
        )
        logger.info("VOLUME | %s → %d%% [guild %d]", interaction.user, level, interaction.guild_id)

    # ── Event: forced disconnect ───────────────────────────────────────────────

    @commands.Cog.listener()
    async def on_voice_state_update(
        self,
        member: discord.Member,
        before: discord.VoiceState,
        after: discord.VoiceState,
    ) -> None:
        """
        Clean up the player when the bot is kicked or disconnected externally.

        Also disables the NP message buttons so orphaned controls don't
        appear interactive after the bot has left the channel.
        """
        if member.id != self.bot.user.id:  # type: ignore[union-attr]
            return
        if before.channel is None or after.channel is not None:
            return   # not a disconnect event

        gid = member.guild.id
        p   = self._players.pop(gid, None)
        if not p:
            return

        p._task.cancel()
        logger.info("DISCONNECT | force-removed from voice [guild %d]", gid)

        # Disable buttons on the last NP message so they don't mislead users.
        if p._np_msg and p._np_view:
            try:
                p._np_view._disable_all()
                await p._np_msg.edit(view=p._np_view)
            except (discord.NotFound, discord.HTTPException):
                pass


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Music(bot))
