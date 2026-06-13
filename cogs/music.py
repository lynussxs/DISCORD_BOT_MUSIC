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

import anthropic as _anthropic
import httpx as _httpx  # cho OpenRouter async calls

# ── AI Error Handler ───────────────────────────────────────────────────────────
# Khi bot gặp lỗi, tự gọi AI API phân tích và thử fix runtime
# Ưu tiên: Anthropic → OpenRouter free (deepseek-r1)

_ai_client: _anthropic.AsyncAnthropic | None = None

_AI_PROMPT = """Discord music bot gặp lỗi khi dùng yt-dlp:

ERROR: {error}
CONTEXT: {context}

Trả về JSON với format:
{{
  "player_clients": ["android_vr", "tv_embedded"],
  "use_proxy": true,
  "format": "bestaudio/best",
  "reason": "ngắn gọn lý do"
}}

Chỉ trả về JSON, không giải thích thêm."""

def _get_ai_client() -> "_anthropic.AsyncAnthropic | None":
    global _ai_client
    if _ai_client is None:
        key = os.environ.get("ANTHROPIC_API_KEY", "")
        if key:
            _ai_client = _anthropic.AsyncAnthropic(api_key=key)
    return _ai_client

async def _ai_suggest_via_anthropic(error: str, context: str) -> dict[str, Any] | None:
    """Dùng Anthropic Claude API."""
    client = _get_ai_client()
    if not client:
        return None
    try:
        import json
        msg = await client.messages.create(
            model      = "claude-sonnet-4-6",
            max_tokens = 300,
            messages   = [{"role": "user", "content": _AI_PROMPT.format(error=error, context=context)}],
        )
        text = re.sub(r"```json|```", "", msg.content[0].text).strip()
        return json.loads(text)
    except Exception as exc:
        logger.debug("Anthropic AI failed: %s", exc)
        return None

async def _ai_suggest_via_openrouter(error: str, context: str) -> dict[str, Any] | None:
    """Dùng OpenRouter free làm fallback — thử lần lượt các model."""
    key = os.environ.get("OPENROUTER_API_KEY", "")
    if not key:
        return None

    # Thử lần lượt — model nào OK thì dùng
    FREE_MODELS = [
        "google/gemma-4-31b-it:free",
        "nvidia/nemotron-3-ultra-550b-a55b:free",
        "nvidia/nemotron-3-super-120b-a12b:free",
        "nex-agi/nex-n2-pro:free",
    ]

    try:
        import json
        async with _httpx.AsyncClient(timeout=20) as http:
            for model in FREE_MODELS:
                try:
                    r = await http.post(
                        "https://openrouter.ai/api/v1/chat/completions",
                        headers={
                            "Authorization": f"Bearer {key}",
                            "Content-Type" : "application/json",
                            "HTTP-Referer"  : "https://discord-music-bot",
                        },
                        json={
                            "model"     : model,
                            "messages"  : [{"role": "user", "content": _AI_PROMPT.format(error=error, context=context)}],
                            "max_tokens": 300,
                        },
                    )
                    if r.status_code != 200:
                        logger.debug("OpenRouter model %s failed: %d", model, r.status_code)
                        continue

                    data = r.json()
                    text = data["choices"][0]["message"]["content"]
                    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL)
                    text = re.sub(r"```json|```", "", text).strip()
                    match = re.search(r"\{.*\}", text, re.DOTALL)
                    if match:
                        result = json.loads(match.group())
                        logger.info("AI (OpenRouter/%s) suggested: %s", model.split("/")[-1], result.get("reason", ""))
                        return result
                except Exception as exc:
                    logger.debug("OpenRouter model %s error: %s", model, exc)
                    continue
    except Exception as exc:
        logger.debug("OpenRouter AI failed: %s", exc)
    return None

async def _ai_suggest_fix(error: str, context: str) -> dict[str, Any] | None:
    """
    Gọi AI API phân tích lỗi và đề xuất fix runtime.
    Thử Anthropic trước, fallback OpenRouter nếu fail.
    """
    # Thử Anthropic trước
    result = await _ai_suggest_via_anthropic(error, context)
    if result:
        logger.info("AI (Anthropic) suggested: %s", result.get("reason", ""))
        return result

    # Fallback OpenRouter free
    result = await _ai_suggest_via_openrouter(error, context)
    if result:
        logger.info("AI (OpenRouter) suggested: %s", result.get("reason", ""))
        return result

    return None

# Spotify API (tuỳ chọn — chỉ dùng nếu có credentials)
try:
    import spotipy
    from spotipy.oauth2 import SpotifyClientCredentials
    _sp_client = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
        client_id     = os.environ.get("SPOTIFY_CLIENT_ID", ""),
        client_secret = os.environ.get("SPOTIFY_CLIENT_SECRET", ""),
    )) if os.environ.get("SPOTIFY_CLIENT_ID") else None
except Exception:
    _sp_client = None

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
    """Proxy manager: chỉ dùng Japan → Germany, KHÔNG dùng proxy US."""

    # Chỉ dùng proxy châu Á/EU — không bao giờ dùng proxy US (geo-block)
    PRIORITY = [
        "http://fywznozi:gv94cmc9t7qs@142.111.67.146:5611",  # Japan ⭐
        "http://fywznozi:gv94cmc9t7qs@31.58.9.4:6077",       # Germany
    ]

    # Các quốc gia được phép dùng từ Webshare API
    ALLOWED_COUNTRIES = {"JP", "DE", "SG", "HK", "KR", "TW", "NL", "FR"}

    def __init__(self) -> None:
        self.api_key  = os.environ.get("WEBSHARE_API_KEY", "")
        self._proxies : list[str] = list(self.PRIORITY)
        self._idx     : int = 0
        self._dead    : set[str] = set()
        self._fetched : bool = False
        self._dead_until: dict[str, float] = {}  # proxy → thời gian hết bị chặn

    def _fetch_api(self) -> None:
        """Lấy thêm proxy từ Webshare API — chỉ lấy proxy châu Á/EU."""
        if self._fetched or not self.api_key:
            return
        self._fetched = True
        try:
            r = requests.get(
                "https://proxy.webshare.io/api/v2/proxy/list/?mode=direct&page=1&page_size=25",
                headers={"Authorization": f"Token {self.api_key}"},
                timeout=10,
            )
            if r.status_code == 200:
                for p in r.json().get("results", []):
                    # Chỉ thêm proxy từ nước được phép
                    country = p.get("country_code", "US").upper()
                    if country not in self.ALLOWED_COUNTRIES:
                        continue
                    url = f"http://{p['username']}:{p['password']}@{p['proxy_address']}:{p['port']}"
                    if url not in self._proxies:
                        self._proxies.append(url)
        except Exception:
            pass

    def get(self) -> str:
        self._fetch_api()
        now = time.monotonic()
        for i in range(len(self._proxies)):
            url = self._proxies[(self._idx + i) % len(self._proxies)]
            # Bỏ qua nếu dead hoặc đang trong thời gian timeout
            if url in self._dead:
                continue
            if self._dead_until.get(url, 0) > now:
                continue
            return url
        # Tất cả đang bị chặn tạm → reset và dùng Japan
        self._dead.clear()
        self._dead_until.clear()
        self._idx = 0
        return self._proxies[0]

    def mark_dead(self, url: str, temporary: bool = True) -> str:
        """
        Đánh dấu proxy bị chặn.
        temporary=True → chỉ tạm thời (rate limit), thử lại sau 5 phút.
        temporary=False → dead hẳn.
        """
        if temporary:
            # Rate limit: thử lại sau 5 phút
            self._dead_until[url] = time.monotonic() + 300
        else:
            self._dead.add(url)
        return self.get()

    def rotate(self) -> str:
        current = self.get()
        return self.mark_dead(current, temporary=True)

_proxy = WebshareProxyManager()

# ── yt-dlp options ─────────────────────────────────────────────────────────────

def _ytdl_opts(cookies: bool = True, use_proxy: bool = True) -> dict[str, Any]:
    opts: dict[str, Any] = {
        # Ưu tiên opus/webm chất lượng cao nhất cho 320kbps
        "format"          : "bestaudio[ext=webm][abr>=128]/bestaudio[ext=m4a]/bestaudio/best",
        "default_search"  : "ytsearch",
        "noplaylist"      : False,
        "quiet"           : True,
        "no_warnings"     : True,
        "extractor_args"  : {
            "youtube": {
                "player_client": ["android_vr", "tv_embedded"],
            }
        },
        "http_chunk_size" : 10485760,  # 10MB chunks — tối ưu cho video dài
    }
    if use_proxy:
        proxy = _proxy.get()
        if proxy:
            opts["proxy"] = proxy
    if cookies:
        cp = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "cookies.txt")
        if os.path.exists(cp):
            opts["cookiefile"] = cp
    return opts

# Giữ lại tên cũ
def _get_ytdl_options(cookies: bool = True) -> dict[str, Any]:
    return _ytdl_opts(cookies)

def _ffmpeg_before() -> str:
    proxy = _proxy.get()
    base = (
        "-reconnect 1 "
        "-reconnect_streamed 1 "
        "-reconnect_delay_max 30 "
        "-reconnect_at_eof 1 "
        "-reconnect_on_network_error 1 "
        "-reconnect_on_http_error 4xx,5xx "
        "-analyzeduration 0 "
        "-fflags +nobuffer "     # giảm buffer delay
        "-flags low_delay"       # giảm latency
    )
    if proxy:
        base += f" -http_proxy {proxy}"
    return base

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
        self.video_id: str         = data.get("id", "")
        self._url_fetched_at: float = time.monotonic()

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

        # Bước 2: Thử không proxy trước → bị chặn thì dùng proxy → AI suggest fix
        last_err: Exception | None = None
        ai_opts: dict[str, Any] | None = None

        for attempt in range(6):
            if attempt == 5 and ai_opts is None:
                break

            if attempt == 5 and ai_opts:
                opts = _ytdl_opts(False, use_proxy=ai_opts.get("use_proxy", True))
                if ai_opts.get("player_clients"):
                    opts["extractor_args"] = {
                        "youtube": {"player_client": ai_opts["player_clients"]}
                    }
                if ai_opts.get("format"):
                    opts["format"] = ai_opts["format"]
                logger.info("AI FIX | trying: %s", ai_opts.get("reason", ""))
            else:
                use_proxy = attempt > 0
                opts = _ytdl_opts(False, use_proxy=use_proxy)

            try:
                with yt_dlp.YoutubeDL(opts) as ytdl:
                    partial = functools.partial(ytdl.extract_info, video_url, download=False)
                    data: dict[str, Any] = await loop.run_in_executor(None, partial)
                break
            except Exception as e:
                last_err = e
                err_str = str(e)

                if any(x in err_str for x in ["not available", "unavailable", "private", "removed"]):
                    raise last_err

                is_rate_limit = any(x in err_str for x in [
                    "Sign in", "bot", "Requested format", "403",
                    "Connection refused", "Connection reset", "Unable to download"
                ])
                if is_rate_limit and attempt < 5:
                    current = opts.get("proxy", "")
                    if current:
                        _proxy.mark_dead(current, temporary=True)
                    delay = 2 * attempt
                    logger.warning(
                        "yt-dlp attempt %d failed, waiting %ds%s",
                        attempt + 1, delay,
                        " (with proxy)" if attempt > 0 else " (no proxy)",
                    )
                    if delay > 0:
                        await asyncio.sleep(delay)

                    # Sau attempt 3 → nhờ AI suggest fix
                    if attempt == 3 and ai_opts is None:
                        ai_opts = await _ai_suggest_fix(
                            error   = err_str[:300],
                            context = f"video_url={video_url}, proxy={'yes' if current else 'no'}",
                        )
                        if ai_opts:
                            logger.info("AI suggested: %s", ai_opts.get("reason", ""))
                    continue
                raise last_err  # type: ignore

        if last_err is not None and 'data' not in dir():
            raise last_err  # type: ignore

        return cls(data, requester)  # type: ignore[possibly-undefined]

    @property
    def duration_str(self) -> str:
        h, r   = divmod(self.duration, 3600)
        m, sec = divmod(r, 60)
        return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"

    @property
    def url_is_fresh(self) -> bool:
        """URL stream còn hạn không (YouTube expire sau ~6 tiếng)."""
        return (time.monotonic() - self._url_fetched_at) < 18_000  # 5 tiếng

    async def refresh_url(self, loop: asyncio.AbstractEventLoop) -> None:
        """Refresh URL stream nếu sắp hết hạn."""
        if self.url_is_fresh:
            return
        try:
            opts = _ytdl_opts(False)
            with yt_dlp.YoutubeDL(opts) as ytdl:
                partial = functools.partial(ytdl.extract_info, self.webpage_url, download=False)
                data = await loop.run_in_executor(None, partial)
            self.url = data.get("url", self.url)
            self._url_fetched_at = time.monotonic()
            logger.info("URL refreshed for '%s'", self.title)
        except Exception as exc:
            logger.warning("Failed to refresh URL for '%s': %s", self.title, exc)

    async def make_source(self, volume: float, seek: int = 0) -> discord.FFmpegOpusAudio:
        safe_vol = max(0.01, min(2.0, volume))
        filters = [f"volume={safe_vol:.3f}"]
        if getattr(self, '_bassboost', False):
            filters.append("bass=g=10:f=110:w=0.3")
        if getattr(self, '_nightcore', False):
            filters.append("asetrate=44100*1.25,aresample=44100")
        filter_str = ",".join(filters)
        seek_opt = f"-ss {seek} " if seek > 0 else ""
        return discord.FFmpegOpusAudio(
            self.url,
            before_options=seek_opt + _ffmpeg_before(),
            options=(
                f"-vn "
                f"-filter:a \"{filter_str}\" "
                f"-b:a 320k "
                f"-application audio"
            ),
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

    IDLE_TIMEOUT = 60   # seconds of silence before auto-disconnect (1 phút)

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
        self.loop: bool            = False
        self.shuffle: bool         = False
        self.autoplay: bool        = False
        self._history: list[Track] = []
        self._preloaded: Track | None = None      # pre-load bài tiếp
        self._old_np_msgs: list[discord.Message] = []  # NP cũ để xóa
        self._247_mode: bool       = False        # 24/7 mode
        self._bassboost: bool      = False        # bassboost effect
        self._nightcore: bool      = False        # nightcore effect

        # References to the last "Now Playing" message + view so we can
        # disable its buttons when the track ends or the bot disconnects.
        self._np_msg:  discord.Message | None   = None
        self._np_view: MusicControlView | None  = None

        self._next: asyncio.Event = asyncio.Event()
        self._task: asyncio.Task  = loop.create_task(self._player_loop())

    # ── Queue ──────────────────────────────────────────────────────────────────

    def enqueue(self, track: Track) -> int:
        """Append track (bỏ qua nếu trùng) và return vị trí 1-based. -1 = trùng."""
        if any(t.webpage_url == track.webpage_url for t in self._queue):
            return -1
        self._queue.append(track)
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

                # ── Idle wait — CHỈ khi queue trống và không phát nhạc ────────
                if not self._queue and not self.vc.is_playing() and not self.vc.is_paused():
                    self._next.clear()
                    try:
                        await asyncio.wait_for(
                            self._next.wait(), timeout=self.IDLE_TIMEOUT if not self._247_mode else None
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
                    await asyncio.sleep(0.5)
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

                # ── Now Playing message — xóa tin nhắn NP cũ ──────────────────
                view = MusicControlView(self)
                self._np_view = view
                for old_msg in list(self._old_np_msgs):
                    try:
                        await old_msg.delete()
                        self._old_np_msgs.remove(old_msg)
                    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                        self._old_np_msgs.remove(old_msg)
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

                # ── Pre-load bài tiếp theo trong background ────────────────────
                if self._queue and not self._preloaded:
                    asyncio.ensure_future(self._preload_next(self._queue[0]))

                # ── Wait for track to finish — auto-refresh nếu stream đứt ────
                self._next.clear()
                while True:
                    await self._next.wait()
                    self._next.clear()

                    elapsed = int(time.monotonic() - self._start) if self._start else 0

                    # Nếu bài đã phát xong hoặc bị skip/stop → thoát
                    if self.current is None or self.current != track:
                        break
                    if not self.vc.is_playing() and not self.vc.is_paused():
                        # Stream đứt giữa chừng — thử refresh URL nếu bài chưa hết
                        if track.duration > 0 and elapsed < track.duration - 5:
                            logger.warning(
                                "STREAM DROP at %ds/%ds for '%s', refreshing URL…",
                                elapsed, track.duration, track.title,
                            )
                            try:
                                await track.refresh_url(self._loop)
                                new_src = await track.make_source(self.volume, seek=elapsed)
                                self.vc.play(new_src, after=_after)
                                logger.info("STREAM RESUMED at %ds for '%s'", elapsed, track.title)
                                continue  # tiếp tục chờ
                            except Exception as exc:
                                logger.error("STREAM REFRESH FAILED: %s", exc)
                        break
                    break

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

                # ── Thông báo hết nhạc nếu queue trống ────────────────────────
                if not self._queue and not self.loop:
                    try:
                        e = discord.Embed(
                            title       = "🎵 Queue Ended",
                            description = (
                                f"Bài cuối: **[{track.title}]({track.webpage_url})**\n\n"
                                f"Hết nhạc rồi! Dùng `/play` để thêm bài mới."
                            ),
                            colour      = COLOUR_QUEUE,
                        )
                        e.set_footer(text=f"⏱ Bot sẽ tự rời sau {self.IDLE_TIMEOUT}s • Đã phát {len(self._history)} bài")
                        if track.thumbnail:
                            e.set_thumbnail(url=track.thumbnail)
                        await self.text_ch.send(embed=e)
                    except discord.HTTPException:
                        pass

        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Unhandled error in player loop [guild %d]", self.vc.guild.id)

    async def _preload_next(self, track: Track) -> None:
        """Pre-load URL bài tiếp theo trong background."""
        try:
            await track.refresh_url(self._loop)
            self._preloaded = track
            logger.debug("Pre-loaded: '%s'", track.title)
        except Exception as exc:
            logger.debug("Pre-load failed for '%s': %s", track.title, exc)

    async def _expire_np_message(self) -> None:
        """Disable buttons NP cũ và xóa tin nhắn cũ để dọn chat."""
        if self._np_msg and self._np_view:
            try:
                self._np_view._disable_all()
                await self._np_msg.edit(view=self._np_view)
                self._old_np_msgs.append(self._np_msg)
                # Giữ tối đa 2 message cũ, xóa cái còn lại
                while len(self._old_np_msgs) > 2:
                    old = self._old_np_msgs.pop(0)
                    try:
                        await old.delete()
                    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                        pass
            except discord.NotFound:
                pass
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
        if position == -1:
            await interaction.followup.send(
                embed=discord.Embed(
                    title       = "⚠️ Trùng bài",
                    description = f"**{track.title}** đã có trong queue rồi!",
                    colour      = COLOUR_PAUSE,
                ),
                ephemeral=True,
            )
        elif player.vc.is_playing() or player.vc.is_paused():
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

        # Lấy player HOẶC tìm bot đang trong voice
        p = self._get_player(gid)
        guild = interaction.guild
        assert guild is not None

        # Nếu không có player nhưng bot vẫn trong voice → disconnect luôn
        if not p:
            vc = guild.voice_client
            if vc and isinstance(vc, discord.VoiceClient):
                await interaction.response.defer()
                await vc.disconnect()
                await interaction.followup.send(
                    embed=discord.Embed(
                        title       = "⏹  Disconnected",
                        description = "Đã rời voice channel.",
                        colour      = COLOUR_STOP,
                    )
                )
                return
            await interaction.response.send_message(
                embed=_e_err("Not Connected", "Bot không có trong voice channel."),
                ephemeral=True,
            )
            return

        await interaction.response.defer()
        await p.stop()
        self._players.pop(gid, None)

        await interaction.followup.send(
            embed=discord.Embed(
                title       = "⏹  Stopped",
                description = "Đã dừng nhạc và rời voice channel.",
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
        Also auto-leave when all users leave the voice channel.
        """
        guild = member.guild
        gid   = guild.id

        # ── Bot bị kick/disconnect ─────────────────────────────────────────────
        if member.id == self.bot.user.id:  # type: ignore[union-attr]
            if before.channel is None or after.channel is not None:
                return
            p = self._players.pop(gid, None)
            if not p:
                return
            p._task.cancel()
            logger.info("DISCONNECT | force-removed from voice [guild %d]", gid)
            if p._np_msg and p._np_view:
                try:
                    p._np_view._disable_all()
                    await p._np_msg.edit(view=p._np_view)
                except (discord.NotFound, discord.HTTPException):
                    pass
            return

        # ── User rời voice — kiểm tra còn ai không ────────────────────────────
        p = self._players.get(gid)
        if not p or not p.vc.is_connected():
            return
        # Nếu user rời khỏi channel mà bot đang ở
        if before.channel and before.channel.id == p.vc.channel.id:
            # Đếm số người thật (không phải bot)
            humans = [m for m in p.vc.channel.members if not m.bot]
            if not humans:
                # Không còn ai → tự rời sau 1 phút
                await asyncio.sleep(60)
                # Kiểm tra lại
                p2 = self._players.get(gid)
                if p2 and p2.vc.is_connected():
                    humans2 = [m for m in p2.vc.channel.members if not m.bot]
                    if not humans2:
                        logger.info("ALONE | auto-leaving empty voice [guild %d]", gid)
                        try:
                            await p2.text_ch.send(
                                embed=discord.Embed(
                                    title       = "👋  Rời kênh",
                                    description = "Không còn ai trong kênh nên mình rời rồi nhé!",
                                    colour      = COLOUR_QUEUE,
                                )
                            )
                        except discord.HTTPException:
                            pass
                        await p2.stop()
                        self._players.pop(gid, None)


    # ── /spotify ───────────────────────────────────────────────────────────────

    @app_commands.command(name="spotify", description="Phát nhạc từ Spotify — track, album hoặc playlist.")
    @app_commands.describe(url="Spotify URL hoặc tên bài hát")
    @app_commands.guild_only()
    async def spotify(self, interaction: discord.Interaction, url: str) -> None:
        err = self._voice_precheck(interaction)
        if err:
            await interaction.response.send_message(
                embed=_e_err("Cannot Play", err), ephemeral=True
            )
            return

        await interaction.response.defer()

        user  = interaction.user
        guild = interaction.guild
        assert isinstance(user, discord.Member) and guild is not None

        # Resolve Spotify → queries
        result = await self._resolve_spotify(url)
        if not result:
            await interaction.followup.send(
                embed=_e_err("Spotify Error", "Không thể đọc link này!"),
                ephemeral=True,
            )
            return

        # Connect voice
        voice_client = guild.voice_client
        if not isinstance(voice_client, discord.VoiceClient) or not voice_client.is_connected():
            voice_client = await user.voice.channel.connect()  # type: ignore

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

        # Handle single track vs album/playlist
        queries = result if isinstance(result, list) else [result]

        added = 0
        first_track = None
        for q in queries:
            try:
                track = await Track.from_query(q, user, self.bot.loop)
                pos = player.enqueue(track)
                if pos != -1:
                    added += 1
                    if first_track is None:
                        first_track = track
            except Exception:
                continue

        if not first_track:
            await interaction.followup.send(
                embed=_e_err("Not Found", "Không tìm thấy bài nào!"),
                ephemeral=True,
            )
            return

        if len(queries) == 1:
            # Single track
            if player.vc.is_playing() or player.vc.is_paused():
                await interaction.followup.send(embed=_e_queued(first_track, added))
            else:
                await interaction.followup.send(
                    embed=discord.Embed(
                        title       = "🎵 Spotify",
                        description = f"Loading **[{first_track.title}]({first_track.webpage_url})**…",
                        colour      = 0x1DB954,
                    )
                )
        else:
            # Album/Playlist
            await interaction.followup.send(
                embed=discord.Embed(
                    title       = "🎵 Spotify Playlist",
                    description = f"Đã thêm **{added}** bài vào queue!\nĐang phát: **{first_track.title}**",
                    colour      = 0x1DB954,
                ).set_thumbnail(url=first_track.thumbnail or "")
            )

    async def _resolve_spotify(self, url: str) -> list[str] | str | None:
        """Convert Spotify URL → search queries cho YouTube."""
        spotify_re = re.compile(
            r"https?://open\.spotify\.com/(track|album|playlist)/([A-Za-z0-9]+)"
        )
        m = spotify_re.match(url.strip())

        if m and _sp_client:
            stype, sid = m.group(1), m.group(2)
            try:
                if stype == "track":
                    data = _sp_client.track(sid)
                    artists = ", ".join(a["name"] for a in data["artists"])
                    return f"{data['name']} {artists}"
                elif stype == "album":
                    data = _sp_client.album_tracks(sid)
                    queries = []
                    for item in data["items"][:50]:
                        artists = ", ".join(a["name"] for a in item["artists"])
                        queries.append(f"{item['name']} {artists}")
                    return queries
                elif stype == "playlist":
                    data = _sp_client.playlist_tracks(sid)
                    queries = []
                    for item in data["items"][:50]:
                        track = item.get("track")
                        if not track:
                            continue
                        artists = ", ".join(a["name"] for a in track["artists"])
                        queries.append(f"{track['name']} {artists}")
                    return queries
            except Exception as exc:
                logger.warning("Spotify API error: %s", exc)

        # Fallback oembed
        if m:
            try:
                r = requests.get(
                    f"https://open.spotify.com/oembed?url={url}",
                    timeout=8, headers={"User-Agent": "Mozilla/5.0"},
                )
                if r.status_code == 200:
                    return r.json().get("title", "")
            except Exception:
                pass

        if url.strip():
            return url.strip()
        return None

    # ── /seek ──────────────────────────────────────────────────────────────────

    @app_commands.command(name="seek", description="Tua đến vị trí bất kỳ. Ví dụ: 1:30 hoặc 90")
    @app_commands.describe(position="Thời gian (vd: 1:30 hoặc 90)")
    @app_commands.guild_only()
    async def seek(self, interaction: discord.Interaction, position: str) -> None:
        p = self._get_player(interaction.guild_id)  # type: ignore[arg-type]
        if not p or not p.current:
            await interaction.response.send_message(
                embed=_e_err("Nothing Playing", "Không có bài nào đang phát."), ephemeral=True
            )
            return
        try:
            if ":" in position:
                parts = position.split(":")
                secs = int(parts[-1]) + int(parts[-2]) * 60
                if len(parts) == 3:
                    secs += int(parts[0]) * 3600
            else:
                secs = int(position)
        except ValueError:
            await interaction.response.send_message(
                embed=_e_err("Invalid", "Dùng `1:30` hoặc `90` giây."), ephemeral=True
            )
            return
        await interaction.response.defer()
        track = p.current
        safe_vol = max(0.01, min(2.0, p.volume))
        try:
            source = discord.FFmpegOpusAudio(
                track.url,
                before_options=f"-ss {secs} " + _ffmpeg_before(),
                options=f"-vn -filter:a volume={safe_vol:.3f} -b:a 64k",
            )
            p.vc.stop()
            await asyncio.sleep(0.3)
            p._start = time.monotonic() - secs
            p.vc.play(source, after=lambda e: p._loop.call_soon_threadsafe(p._next.set))
            await interaction.followup.send(
                embed=discord.Embed(
                    title       = "⏩ Seeked",
                    description = f"Tua đến **{_fmt(secs)}** trong **{track.title}**",
                    colour      = COLOUR_SUCCESS,
                )
            )
        except Exception as exc:
            await interaction.followup.send(embed=_e_err("Seek Failed", str(exc)), ephemeral=True)

    # ── /history ───────────────────────────────────────────────────────────────

    @app_commands.command(name="history", description="Xem lịch sử bài đã phát.")
    @app_commands.guild_only()
    async def history(self, interaction: discord.Interaction) -> None:
        p = self._get_player(interaction.guild_id)  # type: ignore[arg-type]
        if not p or not p._history:
            await interaction.response.send_message(
                embed=discord.Embed(title="📜 History", description="Chưa có bài nào!", colour=COLOUR_QUEUE),
                ephemeral=True,
            )
            return
        desc = "\n".join(
            f"`{i+1}.` [{t.title}]({t.webpage_url}) — `{t.duration_str}` • {t.requester.mention}"
            for i, t in enumerate(reversed(p._history[-10:]))
        )
        await interaction.response.send_message(
            embed=discord.Embed(
                title       = f"📜 History ({len(p._history)} bài)",
                description = desc,
                colour      = COLOUR_QUEUE,
            ),
            ephemeral=True,
        )

    # ── /247 ───────────────────────────────────────────────────────────────────

    @app_commands.command(name="247", description="Bật/tắt chế độ 24/7 — bot không tự rời.")
    @app_commands.guild_only()
    async def mode_247(self, interaction: discord.Interaction) -> None:
        p = self._get_player(interaction.guild_id)  # type: ignore[arg-type]
        if not p:
            await interaction.response.send_message(
                embed=_e_err("Not Connected", "Bot chưa trong voice channel."), ephemeral=True
            )
            return
        p._247_mode = not p._247_mode
        if p._247_mode:
            p.IDLE_TIMEOUT = 999999  # type: ignore[attr-defined]
        else:
            p.IDLE_TIMEOUT = 60  # type: ignore[attr-defined]
        status = "✅ BẬT" if p._247_mode else "❌ TẮT"
        await interaction.response.send_message(
            embed=discord.Embed(
                title       = f"🕐 Chế độ 24/7: {status}",
                description = "Bot sẽ ở lại voice suốt." if p._247_mode
                              else "Bot sẽ tự rời sau 1 phút không có nhạc.",
                colour      = COLOUR_SUCCESS if p._247_mode else COLOUR_QUEUE,
            )
        )

    # ── /bassboost ─────────────────────────────────────────────────────────────

    @app_commands.command(name="bassboost", description="Bật/tắt Bassboost (áp dụng từ bài tiếp).")
    @app_commands.guild_only()
    async def bassboost(self, interaction: discord.Interaction) -> None:
        p = self._get_player(interaction.guild_id)  # type: ignore[arg-type]
        if not p:
            await interaction.response.send_message(
                embed=_e_err("Not Connected", "Bot chưa trong voice channel."), ephemeral=True
            )
            return
        p._bassboost = not p._bassboost
        if p._nightcore and p._bassboost:
            p._nightcore = False  # tắt nightcore nếu bật bassboost
        status = "✅ BẬT" if p._bassboost else "❌ TẮT"
        await interaction.response.send_message(
            embed=discord.Embed(
                title       = f"🎸 Bassboost: {status}",
                description = "Âm bass được tăng cường. Áp dụng từ bài tiếp theo!" if p._bassboost
                              else "Đã tắt Bassboost.",
                colour      = COLOUR_SUCCESS if p._bassboost else COLOUR_QUEUE,
            )
        )

    # ── /nightcore ─────────────────────────────────────────────────────────────

    @app_commands.command(name="nightcore", description="Bật/tắt Nightcore (speed up + pitch).")
    @app_commands.guild_only()
    async def nightcore(self, interaction: discord.Interaction) -> None:
        p = self._get_player(interaction.guild_id)  # type: ignore[arg-type]
        if not p:
            await interaction.response.send_message(
                embed=_e_err("Not Connected", "Bot chưa trong voice channel."), ephemeral=True
            )
            return
        p._nightcore = not p._nightcore
        if p._bassboost and p._nightcore:
            p._bassboost = False  # tắt bassboost nếu bật nightcore
        status = "✅ BẬT" if p._nightcore else "❌ TẮT"
        await interaction.response.send_message(
            embed=discord.Embed(
                title       = f"🌙 Nightcore: {status}",
                description = "Nhạc được tăng tốc và pitch. Áp dụng từ bài tiếp theo!" if p._nightcore
                              else "Đã tắt Nightcore.",
                colour      = COLOUR_SUCCESS if p._nightcore else COLOUR_QUEUE,
            )
        )


async def _check_ai_on_startup() -> None:
    """Kiểm tra AI models khi bot khởi động và log kết quả."""
    import json

    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info("🤖 AI HANDLER STARTUP CHECK")

    # Check Anthropic
    client = _get_ai_client()
    if client:
        try:
            msg = await client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=10,
                messages=[{"role": "user", "content": "hi"}],
            )
            logger.info("✅ Anthropic Claude: ONLINE")
        except Exception as e:
            if "credit" in str(e).lower() or "balance" in str(e).lower():
                logger.warning("⚠️  Anthropic Claude: OUT OF CREDITS")
            else:
                logger.warning("❌ Anthropic Claude: %s", str(e)[:50])
    else:
        logger.info("⏭️  Anthropic Claude: NO API KEY")

    # Check OpenRouter models
    key = os.environ.get("OPENROUTER_API_KEY", "")
    if key:
        FREE_MODELS = [
            "google/gemma-4-31b-it:free",
            "nvidia/nemotron-3-ultra-550b-a55b:free",
            "nvidia/nemotron-3-super-120b-a12b:free",
            "nex-agi/nex-n2-pro:free",
        ]
        working = []
        try:
            async with _httpx.AsyncClient(timeout=10) as http:
                for model in FREE_MODELS:
                    try:
                        r = await http.post(
                            "https://openrouter.ai/api/v1/chat/completions",
                            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                            json={"model": model, "messages": [{"role": "user", "content": "hi"}], "max_tokens": 5},
                        )
                        short = model.split("/")[-1]
                        if r.status_code == 200:
                            working.append(model)
                            logger.info("✅ OpenRouter %s: ONLINE", short)
                        else:
                            logger.info("❌ OpenRouter %s: %d", short, r.status_code)
                    except Exception:
                        logger.info("❌ OpenRouter %s: TIMEOUT", model.split("/")[-1])
        except Exception:
            pass

        if working:
            logger.info("🎯 Primary AI: OpenRouter/%s", working[0].split("/")[-1])
        else:
            logger.warning("⚠️  No OpenRouter models available!")
    else:
        logger.info("⏭️  OpenRouter: NO API KEY")

    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Music(bot))
    # Check AI models on startup
    asyncio.ensure_future(_check_ai_on_startup())
