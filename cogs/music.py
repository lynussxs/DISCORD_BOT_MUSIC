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
import base64
import collections
import functools
import json
import os
import re
import threading
import time
from typing import Any

import httpx as _httpx  # dùng cho Piped/Invidious fallback async calls

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
from utils import memstat as _mem
from utils import pot_provider as _pot

logger = get_logger(__name__)

# ── Colours ────────────────────────────────────────────────────────────────────
COLOUR_PLAY    = 0x1DB954   # Spotify green
COLOUR_PAUSE   = 0xF5A623   # amber
COLOUR_STOP    = 0xED4245   # red
COLOUR_QUEUE   = 0x5865F2   # blurple
COLOUR_SUCCESS = 0x57F287   # mint

# ── Webshare Proxy Manager ─────────────────────────────────────────────────────

# Chỉ IP:port (không có mật khẩu) — mật khẩu lấy từ biến môi trường WEBSHARE_CRED.
_WS_CRED_ENV = os.environ.get("WEBSHARE_CRED", "").strip()
_WS_HOSTS = [
    "31.56.127.193:7684", "198.23.243.226:6361", "38.154.185.97:6370", "191.96.254.138:6185",
    "142.111.67.146:5611", "31.59.20.176:6754", "45.38.107.97:6014", "198.105.121.200:6462",
    "64.137.96.74:6641", "84.247.60.125:6095",
]


class WebshareProxyManager:
    """Proxy manager: Bright Data residential (ưu tiên cao nhất) → US → Japan → free datacenter."""

    # US proxy để bypass video chỉ cho Mỹ xem. Set env var PROXY_US=http://user:pass@host:port
    PROXY_US = os.environ.get("PROXY_US", "")

    # Credentials proxy KHÔNG còn nằm trong source (repo công khai!). Đặt trên Railway:
    #   WEBSHARE_API_KEY  — bot tự lấy danh sách proxy + credentials HIỆN TẠI qua API (khuyên dùng)
    #   WEBSHARE_CRED     — "user:pass" dùng chung cho danh sách IP cố định bên dưới (tuỳ chọn)
    #   PROXY_US          — http://user:pass@host:port (proxy Mỹ, tuỳ chọn)
    WEBSHARE_10 = [f"http://{_WS_CRED_ENV}@{_hp}" for _hp in _WS_HOSTS] if _WS_CRED_ENV else []

    # Public free proxies (fallback cuối — không auth, dễ chết, CHƯA kiểm
    # chứng như WEBSHARE_10 ở trên, chỉ dùng khi mọi proxy có auth đều fail)
    PRIORITY = [
        *([ PROXY_US ] if PROXY_US else []),
        *WEBSHARE_10,
        # Germany proxy đã bỏ — 31.58.9.4:6077 luôn 407 (credentials chết), gây lãng phí 1 attempt mỗi lần
        # ── Free public proxies (sort by latency) ──────────────────────────
        "http://34.43.46.91:80",           # US 325ms
        "http://178.212.144.7:80",         # PL 625ms
        "http://185.135.69.34:80",         # IQ 974ms
        # ── Proxy Việt Nam (free, chưa kiểm chứng hoạt động với YouTube) ────
        "http://14.186.61.187:10034",      # VN
        "http://14.241.80.37:8080",        # VN
        "http://27.74.219.51:30453",       # VN
        "http://14.186.61.187:10028",      # VN
        "http://14.186.61.187:10039",      # VN
        "http://171.252.168.231:5109",     # VN
        "http://116.103.93.156:16000",     # VN
        "http://113.176.118.150:1080",     # VN
        "http://14.181.228.19:1080",       # VN
        "http://118.69.62.188:57140",      # VN
        "http://52.34.243.150:8080",       # US 537ms
        "http://34.43.46.91:443",          # US 642ms
        "http://205.215.247.164:3128",     # US 646ms
        "http://34.122.187.196:80",        # US 661ms
        "http://34.44.49.215:80",          # US 668ms
        "http://71.198.208.169:443",       # US 699ms
        "http://142.93.202.130:3128",      # US 711ms
        "http://159.65.245.255:80",        # US 713ms
        "http://137.66.1.45:80",           # US 717ms
        "http://23.81.87.202:8118",        # US 718ms
        "http://16.163.88.228:80",         # HK 859ms
        "http://141.98.153.86:80",         # DE 866ms
        "http://140.238.32.108:3128",      # JP 935ms
        "http://43.167.187.233:3128",      # JP 1037ms
        "http://1.231.81.166:3128",        # KR 1044ms
        "http://182.155.254.159:80",       # TW 1140ms
        "http://47.236.86.147:443",        # SG 1364ms
        "http://138.2.83.219:3128",        # SG 1433ms
        "http://43.99.100.108:3128",       # HK 1581ms
        "http://43.167.16.253:3128",       # JP 1995ms
    ]

    ALLOWED_COUNTRIES = {"US", "JP", "DE", "SG", "HK", "KR", "TW", "NL", "FR", "GB", "ES", "PL"}


    def __init__(self) -> None:
        self.api_key     = os.environ.get("WEBSHARE_API_KEY", "")
        self._proxies    : list[str] = [p for p in self.PRIORITY if p]
        self._idx        : int = 0
        self._dead       : set[str] = set()
        self._fetched    : bool = False
        self._fetched_at : float = 0.0
        self._fetching   : bool = False
        self._dead_until : dict[str, float] = {}
        self._force_us   : bool = False

    def _fetch_api(self) -> None:
        """
        Lấy thêm proxy từ Webshare API — chỉ lấy proxy châu Á/EU.
        Refresh định kỳ mỗi 2 tiếng (không chỉ 1 lần lúc khởi động) — Webshare
        có thể xoay vòng/thay đổi pool proxy của họ theo thời gian.

        QUAN TRỌNG: chạy NGẦM trong thread riêng, KHÔNG block event loop.
        Trước đây gọi httpx.get() đồng bộ ngay trong hàm này — nếu hàm bị gọi
        từ code async (qua _proxy.get() dùng trong _ffmpeg_before chẳng hạn),
        request mất tới 10s sẽ ĐÓNG BĂNG TOÀN BỘ event loop, chặn cả heartbeat
        gửi Discord → đây rất có thể là nguyên nhân chính gây "Bot disconnected
        from Discord" lặp lại nhiều lần trong log dài hạn.
        """
        REFRESH_INTERVAL = 7200.0  # 2 giờ
        now = time.monotonic()
        if not self.api_key:
            return
        if self._fetched and (now - self._fetched_at) < REFRESH_INTERVAL:
            return
        if self._fetching:  # đã có 1 lần fetch đang chạy ngầm, khỏi trigger thêm
            return
        self._fetched    = True
        self._fetched_at = now
        self._fetching   = True

        def _bg_fetch() -> None:
            try:
                import httpx
                r = httpx.get(
                    "https://proxy.webshare.io/api/v2/proxy/list/?mode=direct&page=1&page_size=25",
                    headers={"Authorization": f"Token {self.api_key}"},
                    timeout=10,
                )
                if r.status_code == 200:
                    fresh: list[str] = []
                    for p in r.json().get("results", []):
                        country = p.get("country_code", "US").upper()
                        if country not in self.ALLOWED_COUNTRIES:
                            continue
                        url = f"http://{p['username']}:{p['password']}@{p['proxy_address']}:{p['port']}"
                        if url not in fresh:
                            fresh.append(url)
                    if fresh:
                        # API trả credentials HIỆN TẠI → xếp lên ĐẦU, trước danh sách WEBSHARE_10
                        # ghi cứng (có thể đã cũ: log 11:38 có proxy báo 407 dù API vẫn trả 200).
                        keep = [u for u in self._proxies if u not in fresh]
                        self._proxies = fresh + keep
                        logger.info("Webshare API: %d proxy mới nhất được xếp lên đầu danh sách.", len(fresh))
            except Exception:
                pass
            finally:
                self._fetching = False

        threading.Thread(target=_bg_fetch, daemon=True, name="webshare-proxy-fetch").start()

    def get(self) -> str:
        self._fetch_api()
        now = time.monotonic()
        # Nếu force US (do gcr=us geo-lock) → trả US proxy trước
        if self._force_us and self.PROXY_US:
            self._force_us = False
            return self.PROXY_US
        for i in range(len(self._proxies)):
            url = self._proxies[(self._idx + i) % len(self._proxies)]
            if url in self._dead:
                continue
            if self._dead_until.get(url, 0) > now:
                continue
            return url
        self._dead.clear()
        self._dead_until.clear()
        self._idx = 0
        return self._proxies[0]

    def pick(self, n: int, exclude: set[str] | None = None) -> list[str]:
        """n proxy KHÁC NHAU còn sống (để đua song song); xoay _idx để bài sau dùng IP khác."""
        self._fetch_api()
        now = time.monotonic()
        out: list[str] = []
        total = len(self._proxies)
        last = 0
        for i in range(total):
            url = self._proxies[(self._idx + i) % total]
            if url in self._dead or self._dead_until.get(url, 0) > now or (exclude and url in exclude):
                continue
            out.append(url)
            last = i + 1
            if len(out) >= n:
                break
        if out:
            self._idx = (self._idx + last) % total
        return out

    def force_us(self) -> None:
        """Ép dùng proxy US cho lần yt-dlp tiếp theo (bypass gcr=us geo-lock)."""
        if self.PROXY_US:
            self._force_us = True
            logger.info("Forcing US proxy: %s", re.sub(r"//[^@/]+@", "//***:***@", self.PROXY_US))
        else:
            logger.warning("PROXY_US chưa set — không thể bypass gcr=us!")

    def mark_dead(self, url: str, temporary: bool = True) -> str:
        """
        Đánh dấu proxy bị chặn.
        temporary=True → chỉ tạm thời (rate limit), thử lại sau 5 phút.
        temporary=False → dead hẳn.
        """
        if temporary:
            # Rate limit: thử lại sau 5 phút
            self._dead_until[url] = time.monotonic() + 60  # 60s cho free proxy rotate nhanh hơn
        else:
            self._dead.add(url)
        return self.get()

    def rotate(self) -> str:
        current = self.get()
        return self.mark_dead(current, temporary=True)

_proxy = WebshareProxyManager()
_proxy._fetch_api()   # lấy danh sách Webshare NGAY lúc khởi động (nền), không đợi tới /play đầu tiên

# ── yt-dlp options ─────────────────────────────────────────────────────────────

# Ưu tiên audio-only. Khi YouTube ép SABR chỉ còn định dạng có kèm video, chọn bản NHỎ NHẤT có
# tiếng (18=360p mp4 ~96k, 93/94=HLS 360p/480p, 92/91=HLS 240p/144p) thay vì "best" (có thể là
# 1080p — ffmpeg vẫn phải tải cả phần video dù có -vn, rất tốn băng thông/RAM trên gói free).
_YT_FORMAT = "bestaudio/18/93/94/92/91/best[height<=480]/best"


def _set_proxy(opts: dict[str, Any], proxy: str) -> None:
    """Gắn proxy vào opts yt-dlp (kèm header Proxy-Authorization riêng để tránh 407)."""
    import urllib.parse
    opts["proxy"] = proxy
    parsed = urllib.parse.urlparse(proxy)
    hdrs = dict(opts.get("http_headers") or {})
    hdrs.pop("Proxy-Authorization", None)
    if parsed.username and parsed.password:
        creds = base64.b64encode(f"{parsed.username}:{parsed.password}".encode()).decode()
        hdrs["Proxy-Authorization"] = f"Basic {creds}"
    if hdrs:
        opts["http_headers"] = hdrs


def _ytdl_opts(cookies: bool = True, use_proxy: bool = True) -> dict[str, Any]:
    # tv_embedded/android_vr được thiết kế để BYPASS auth — nếu gửi kèm cookies,
    # YouTube trả về format list rỗng/hạn chế → "Requested format is not available".
    # Khi có cookies hợp lệ → ưu tiên client hỗ trợ auth (web, android).
    # Thêm "tv" — hiện là 1 trong các client MẶC ĐỊNH của chính yt-dlp (cùng
    # ios/web) từ giữa 2025, khá bền với PO-Token so với mweb/web đơn thuần.
    cookie_file_exists = cookies and _cookies_valid()
    player_clients = ["tv_samsung", "web_embedded", "web"] if cookie_file_exists else ["tv_samsung", "visionos", "web"]

    opts: dict[str, Any] = {
        "format"          : _YT_FORMAT,  # 18=progressive mp4, 140=m4a, 251=opus — fallback tường minh khi bestaudio selector không match được (client trả format thiếu tag acodec/vcodec)
        # Ưu tiên định dạng HLS (m3u8, tv_embedded trả về loại này) hơn DASH/
        # progressive thường — HLS hiện CHƯA bị YouTube bắt buộc PO-Token,
        # trong khi non-HLS âm thanh (itag 251/140 từ android/android_vr) thì
        # có. Chỉ có tác dụng khi 1 lần extract trộn nhiều client cùng lúc
        # (attempt cuối) — các attempt tách riêng client thì không bị ảnh hưởng.
        "format_sort"     : ["proto:m3u8_native", "abr"],
        "default_search"  : "ytsearch",
        "noplaylist"      : False,
        "quiet"           : True,
        "no_warnings"     : True,
        "http_chunk_size" : 1048576,  # 1MB — giảm RAM

        "extractor_args"  : {
            "youtube": {
                "player_client": player_clients,
                "skip"         : ["translated_subs", "comments"],  # KHÔNG skip dash/hls — audio hiện đại nằm ở đó
            },
            # PO-Token provider (bgutil-pot) chạy ngầm ở bot.py, cổng mặc định 4416.
            # Nếu server này không chạy, yt-dlp tự bỏ qua — không lỗi gì cả.
            "youtubepot-bgutilhttp": {
                "base_url": ["http://127.0.0.1:4416"],
            },
        },
        "geo_bypass"         : True,
        "geo_bypass_country" : "US",
        "age_limit"          : None,
        "extractor_retries"  : 1,  # code đã tự retry qua client rotation ở tầng ngoài,
                                    # để 3 ở đây sẽ nhân 3 lần thời gian chờ mỗi lần fail
        "socket_timeout"     : 8,
        # Chống rate-limit từ YouTube: giãn cách giữa các request. LƯU Ý:
        # sleep_interval/max_sleep_interval chỉ có tác dụng khi thực sự TẢI
        # file (download=True) — bot luôn gọi extract_info(download=False) nên
        # 2 giá trị này gần như không ảnh hưởng gì tới /play. Giá trị thật sự
        # có tác dụng giảm rate-limit ở đây là sleep_interval_requests (áp
        # dụng cho mọi request kể cả khi chỉ extract metadata).
        "sleep_interval_requests": 0,  # 0 (trước là 1): mỗi lần extract gồm vài request, mỗi cái ngủ thêm 1s → mất 3-5s/attempt vô ích; bot chỉ resolve 1 video mỗi lệnh nên không gây rate-limit
        "sleep_interval"     : 1,
        "max_sleep_interval" : 3,
        "ratelimit"          : 3_000_000,  # 3MB/s cap — tránh spike bị flag bot
    }
    if use_proxy:
        proxy = _proxy.get()
        if proxy:
            _set_proxy(opts, proxy)
    if cookies:
        cp = _cookiefile_path()
        if os.path.exists(cp):
            opts["cookiefile"] = cp
    return opts

# Giữ lại tên cũ
def _get_ytdl_options(cookies: bool = True) -> dict[str, Any]:
    return _ytdl_opts(cookies)

def _ffmpeg_before(
    seek: float = 0,
    no_proxy: bool = False,
    proxy_override: str | None = None,
    headers: dict[str, str] | None = None,
    hls: bool = False,
) -> str:
    """
    FFmpeg input options cho YouTube stream.

    hls=True (URL .m3u8, vd client visionos): BỎ -reconnect_at_eof/-reconnect_streamed. Playlist HLS
    đọc tới EOF là bình thường, nhưng với 2 flag này ffmpeg "reconnect" vào chính EOF đó và ngủ
    backoff 0s/1s/0s/1s cho MỖI playlist (log: "Will reconnect at 44516 in 0/1 second(s)") →
    đầu bài trễ thêm ~2-3s. Đo bằng ffmpeg 6.1 + server HLS giả: 3.05s → 0.05s tới frame đầu.
    KHÔNG dùng -fflags +nobuffer hay -flags low_delay — gây crash/giật.
    KHÔNG dùng -timeout — không hỗ trợ trên mọi build FFmpeg.

    proxy_override: dùng ĐÚNG proxy đã resolve ra URL (Track._proxy_used).
    KHÔNG tự ý gọi _proxy.get() ở đây nữa vì proxy manager có thể đã đổi
    proxy hiện tại kể từ lúc resolve → dùng nhầm proxy khác = IP mismatch = 403.

    headers: http_headers yt-dlp đã dùng để negotiate URL (chủ yếu User-Agent).
    Một số client extraction (android_vr, android, tv_embedded) trả về URL mà
    CDN chỉ chấp nhận nếu request thật sự mang đúng header này — thiếu là bị
    403 ngay cả khi URL còn hạn. Đây từng là nguyên nhân gây lỗi
    "HTTP error 403 Forbidden" / "Error opening input" ngay khi vừa PLAY.

    -tls_verify 0: tắt xác thực chứng chỉ TLS phía ffmpeg (tên option ĐÚNG của
    ffmpeg cho protocol TLS/HTTPS — KHÔNG phải "-no_check_certificate", đó là
    tên flag của yt-dlp/curl, dùng nhầm trong ffmpeg sẽ báo lỗi "Unrecognized
    option" và crash ngay khi mở bài). LƯU Ý bảo mật: mở khả năng MITM về lý
    thuyết nếu proxy ác ý — nhưng hầu hết build ffmpeg vốn đã tắt sẵn TLS
    verify theo mặc định (cần tự cấp CA bundle mới bật được), nên option này
    chủ yếu là "chốt chặn" cho các build có bật sẵn, tránh ffmpeg fail
    handshake khi 1 số proxy free/rẻ tự chèn/terminate TLS bằng chứng chỉ
    self-signed.

    Buffer khởi động vừa phải (không cần lớn như hồi host cũ mạng chập chờn) —
    host hiện tại (Railway) đã xác nhận mạng ổn định, ưu tiên start nhanh.
    """
    probesize  = "96k"
    analyzedur = "1000000"  # 1s — đủ ổn định, không làm chậm start
    base = (
        "-tls_verify 0 "
        "-reconnect 1 "
        + ("" if hls else "-reconnect_streamed 1 ") +
        "-reconnect_delay_max 2 "         # giảm từ 5s→2s: rút ngắn thời gian "đứng
                                           # hình" tối đa mỗi lần ffmpeg tự dò kết nối
                                           # lại gần cuối bài (reconnect_at_eof)
        + ("" if hls else "-reconnect_at_eof 1 ") +
        "-reconnect_on_network_error 1 "
        "-reconnect_on_http_error 5xx "   # chỉ retry 5xx, KHÔNG retry 4xx (403 = cần URL mới)
        # ĐÃ THỬ "-limit_rate 3M" nhưng KHÔNG PHẢI flag hợp lệ của ffmpeg
        # (nhầm theo kiểu đặt tên "curl --limit-rate") — gây lỗi
        # "Unrecognized option 'limit_rate'" khiến ffmpeg từ chối parse toàn
        # bộ before_options, mọi bài phát đều fail ngay lập tức. Đã gỡ hẳn,
        # không thêm lại option throttle rate cho ffmpeg nữa trừ khi xác minh
        # kỹ tên option đúng trước.
        f"-analyzeduration {analyzedur} "
        f"-probesize {probesize} "
        "-rw_timeout 15000000"            # 15s — tránh treo vô hạn nếu mạng đứng hình hẳn
    )
    if headers:
        header_lines = "".join(f"{k}: {v}\r\n" for k, v in headers.items() if k and v)
        if header_lines:
            base = f'-headers "{header_lines}" ' + base
    if seek > 0:
        base = f"-ss {seek} " + base
    if not no_proxy:
        proxy = proxy_override if proxy_override is not None else _proxy.get()
        if proxy:
            base += f" -http_proxy {proxy}"
    return base

def _is_hls(url: str | None) -> bool:
    u = (url or "").lower()
    return ".m3u8" in u or "hls_playlist" in u or "/hls" in u


# Matches http:// and https:// URLs so we can detect non-URL queries.
_URL_RE = re.compile(r"^https?://", re.IGNORECASE)


def _extract_sync(opts: dict[str, Any], url: str) -> dict[str, Any]:
    """
    Chạy TRỌN VẸN yt-dlp (mở, extract, và __exit__/close() → save_cookies())
    trong 1 lời gọi sync — PHẢI được gọi qua loop.run_in_executor().
    Không tách extract_info() riêng khỏi with-block: __exit__ gọi save_cookies()
    ghi file đồng bộ, có thể mất 10-20s+ trên storage chậm — nếu chạy trên main
    thread sẽ block toàn bộ event loop, làm Discord gateway heartbeat bị miss
    và bot rớt kết nối.
    """
    with yt_dlp.YoutubeDL(opts) as ytdl:
        return ytdl.extract_info(url, download=False)


# Danh sách dự phòng cứng — chỉ dùng khi API khám phá instance động bị lỗi.
_PIPED_FALLBACK_HARDCODED = [
    "https://pipedapi.reallyaweso.me",
    "https://pipedapi.leptons.xyz",
    "https://pipedapi.ducks.party",
    "https://ytapi.dc09.ru",
    "https://api.piped.private.coffee",
    "https://pipedapi.smnz.de",
    "https://pipedapi.drgns.space",
    "https://piped-api.hostux.net",
    "https://pipedapi.r4fo.com",
    "https://pipedapi.osphost.fi",
]

_piped_instances_cache: list[str] | None = None
_piped_instances_cached_at: float = 0.0


async def _get_piped_instances(client: "Any") -> list[str]:
    """
    Lấy danh sách instance Piped ĐANG SỐNG từ API chính thức (tự cập nhật theo thời gian
    thực — tránh việc hardcode domain rồi domain đó chết/đổi theo thời gian).
    Cache 30 phút để tránh gọi API khám phá quá thường xuyên.
    """
    global _piped_instances_cache, _piped_instances_cached_at
    now = time.monotonic()
    if _piped_instances_cache and (now - _piped_instances_cached_at) < 1800:
        return _piped_instances_cache

    try:
        resp = await client.get("https://piped-instances.kavin.rocks/", timeout=6.0)
        if resp.status_code == 200:
            instances = resp.json()
            # QUAN TRỌNG: API có lúc trả về None/dict lỗi thay vì list (server-side
            # issue hoặc rate-limit trả về body khác dạng) — kiểm tra kiểu trước khi
            # iterate, tránh crash "'NoneType' object is not iterable".
            if isinstance(instances, list):
                # Mỗi entry có "api_url" — lọc lấy https, bỏ trùng
                urls = []
                for inst in instances:
                    if not isinstance(inst, dict):
                        continue
                    api_url = inst.get("api_url", "").rstrip("/")
                    if api_url and api_url.startswith("https://") and api_url not in urls:
                        urls.append(api_url)
                if urls:
                    # Gộp thêm danh sách dự phòng cứng — tăng tổng số lựa chọn thay vì chỉ
                    # dùng discovery đơn thuần (có thể trả về rất ít do API không ổn định).
                    merged = urls + [u for u in _PIPED_FALLBACK_HARDCODED if u not in urls]
                    _piped_instances_cache = merged
                    _piped_instances_cached_at = now
                    logger.info("Piped instance discovery OK — %d live + %d fallback = %d total",
                                len(urls), len(merged) - len(urls), len(merged))
                    return merged
    except Exception as exc:
        logger.warning("Piped instance discovery failed: %s", exc)

    return _PIPED_FALLBACK_HARDCODED


_invidious_instances_cache: list[str] | None = None
_invidious_instances_cached_at: float = 0.0

_INVIDIOUS_FALLBACK_HARDCODED = [
    "https://invidious.nerdvpn.de",
    "https://iv.melmac.space",
    "https://invidious.jing.rocks",
    "https://yewtu.be",
    "https://inv.nadeko.net",
    "https://invidious.privacyredirect.com",
    "https://invidious.f5.si",
    "https://iv.ggtyler.dev",
    "https://invidious.protokolla.fi",
]


async def _get_invidious_instances(client: "Any") -> list[str]:
    """Tương tự _get_piped_instances nhưng cho Invidious — nguồn dự phòng độc lập thứ 2."""
    global _invidious_instances_cache, _invidious_instances_cached_at
    now = time.monotonic()
    if _invidious_instances_cache and (now - _invidious_instances_cached_at) < 1800:
        return _invidious_instances_cache
    try:
        resp = await client.get(
            "https://api.invidious.io/instances.json?sort_by=type,health", timeout=6.0
        )
        if resp.status_code == 200:
            data = resp.json()
            urls = []
            if isinstance(data, list):
                for entry in data:
                    # Mỗi entry là [name, {...}] — lấy "uri", chỉ https, chỉ api hoạt động
                    info = entry[1] if isinstance(entry, list) and len(entry) > 1 else {}
                    if not isinstance(info, dict):
                        continue
                    uri = (info.get("uri") or "").rstrip("/")
                    # Không lọc theo "api" field nữa — field này thường không phản ánh đúng
                    # tình trạng thực tế, có thể loại bỏ nhầm instance vẫn hoạt động tốt.
                    if uri and uri.startswith("https://") and uri not in urls:
                        urls.append(uri)
            if urls:
                merged = urls + [u for u in _INVIDIOUS_FALLBACK_HARDCODED if u not in urls]
                _invidious_instances_cache = merged
                _invidious_instances_cached_at = now
                logger.info("Invidious instance discovery OK — %d live + %d fallback = %d total",
                            len(urls), len(merged) - len(urls), len(merged))
                return merged
    except Exception as exc:
        logger.warning("Invidious instance discovery failed: %s", exc)
    return _INVIDIOUS_FALLBACK_HARDCODED


async def _stream_url_reachable(client: "Any", url: str) -> bool:
    """
    Thử tải 2 byte đầu của stream (Range request) để chắc chắn ffmpeg sẽ mở được.
    Trả False nếu 403/404/timeout — dùng để loại instance Piped/Invidious "sống
    nhưng trả URL chết" trước khi bot tuyên bố đang phát.
    """
    try:
        r = await client.get(
            url,
            headers={"Range": "bytes=0-1", "User-Agent": "Mozilla/5.0"},
            timeout=6.0,
        )
        return r.status_code in (200, 206)
    except Exception:
        return False


async def _invidious_fallback(video_id: str, client: "Any") -> dict[str, Any] | None:
    """
    Nguồn dự phòng thứ 2 nếu tất cả instance Piped đều fail.
    Query các instance SONG SONG (không tuần tự) — trước đây thử lần lượt
    12 instance, nhiều cái chết/timeout riêng lẻ có thể cộng dồn tới cả phút.
    Giờ bắn hết cùng lúc, lấy kết quả nào về trước.
    """
    instances = await _get_invidious_instances(client)

    async def _try_one(base: str) -> dict[str, Any] | None:
        try:
            # local=true: BẮT BUỘC. Không có nó, Invidious trả URL googlevideo.com
            # trỏ thẳng, bị KHOÁ THEO IP của server Invidious (param ip=...).
            # ffmpeg trên Railway mở URL đó từ IP khác → 403 → bot vào voice,
            # hiện "đang phát" nhưng KHÔNG có tiếng. local=true bắt instance
            # tự proxy stream, URL mở được từ bất kỳ IP nào.
            resp = await client.get(
                f"{base}/api/v1/videos/{video_id}",
                params={"local": "true"},
                timeout=6.0,
            )
            if resp.status_code != 200:
                return None
            data = resp.json()
            formats = (data.get("adaptiveFormats") or []) + (data.get("formatStreams") or [])
            audio_only = [f for f in formats if str(f.get("type", "")).startswith("audio")]
            candidates = sorted(
                audio_only or formats,
                key=lambda f: int(f.get("bitrate", 0) or 0),
                reverse=True,
            )
            for fmt in candidates[:3]:
                stream_url = fmt.get("url") or ""
                if stream_url.startswith("/"):          # một số instance trả URL tương đối
                    stream_url = base + stream_url
                if not stream_url.startswith("http"):
                    continue
                # Kiểm tra THẬT SỰ tải được từ IP của Railway trước khi chọn instance
                # này — tránh chọn nhầm instance trả URL 403 rồi im lặng.
                if not await _stream_url_reachable(client, stream_url):
                    continue
                logger.info("Invidious fallback OK via %s for video_id=%s", base, video_id)
                return {
                    "id"          : video_id,
                    "title"       : data.get("title", "Unknown Title"),
                    "duration"    : data.get("lengthSeconds", 0),
                    "url"         : stream_url,
                    "thumbnail"   : (data.get("videoThumbnails") or [{}])[0].get("url", ""),
                    "uploader"    : data.get("author", "Unknown"),
                    "webpage_url" : f"https://www.youtube.com/watch?v={video_id}",
                }
            return None
        except Exception:
            return None

    tasks = [asyncio.create_task(_try_one(b)) for b in instances[:8]]
    try:
        for coro in asyncio.as_completed(tasks, timeout=8.0):
            result = await coro
            if result:
                for t in tasks:
                    t.cancel()
                return result
    except (asyncio.TimeoutError, TimeoutError):
        pass
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()
    return None


async def _piped_fallback(video_id: str, loop: asyncio.AbstractEventLoop) -> dict[str, Any] | None:
    """
    Phương án cuối khi yt-dlp thất bại hoàn toàn (YouTube siết PO-Token/bot-check).
    Query các instance Piped SONG SONG (không tuần tự), lấy kết quả về trước
    tiên. Nếu Piped fail hết trong thời gian cho phép, thử Invidious.
    Tự khám phá instance đang sống thay vì hardcode domain (domain hay chết/đổi liên tục).
    """
    import httpx as _httpx
    async with _httpx.AsyncClient(timeout=8.0, follow_redirects=True) as client:
        instances = await _get_piped_instances(client)

        async def _try_one(base: str) -> dict[str, Any] | None:
            try:
                resp = await client.get(f"{base}/streams/{video_id}", timeout=6.0)
                if resp.status_code != 200:
                    return None
                data = resp.json()
                audio_streams = data.get("audioStreams") or []
                if not audio_streams:
                    return None
                best = max(audio_streams, key=lambda s: s.get("bitrate", 0))
                if not best.get("url"):
                    return None
                if not await _stream_url_reachable(client, best["url"]):
                    return None
                logger.info("Piped fallback OK via %s for video_id=%s", base, video_id)
                return {
                    "id"          : video_id,
                    "title"       : data.get("title", "Unknown Title"),
                    "duration"    : data.get("duration", 0),
                    "url"         : best.get("url", ""),
                    "thumbnail"   : data.get("thumbnailUrl", ""),
                    "uploader"    : data.get("uploader", "Unknown"),
                    "webpage_url" : f"https://www.youtube.com/watch?v={video_id}",
                }
            except Exception:
                return None

        tasks = [asyncio.create_task(_try_one(b)) for b in instances[:8]]
        result = None
        try:
            for coro in asyncio.as_completed(tasks, timeout=8.0):
                r = await coro
                if r:
                    result = r
                    break
        except (asyncio.TimeoutError, TimeoutError):
            pass
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()

        if result:
            return result

        # Piped hết instance sống trong thời gian cho phép → thử Invidious
        logger.warning("Piped không có kết quả cho %s, trying Invidious…", video_id)
        return await _invidious_fallback(video_id, client)


def _cookiefile_path() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "cookies.txt")


def _cookies_valid() -> bool:
    """
    Kiểm tra cookies.txt có tồn tại và THẬT SỰ dùng được — không chỉ "không
    rỗng". Nếu không đủ điều kiện, bot-check (Sign in to confirm you're not a
    bot) gần như chắc chắn không thể vượt qua.

    LƯU Ý (phát hiện 19/08/2026): ngưỡng cũ chỉ check ">100 byte" — 1 file
    RÁC/CŨ nằm sẵn trong container (1599 byte, 15 dòng, không có cookie đăng
    nhập thật) vẫn "qua cửa" ngưỡng này, khiến _bootstrap_cookiefile() tưởng
    đã có cookie hợp lệ sẵn rồi nên KHÔNG BAO GIỜ ghi đè bằng cookie thật từ
    biến môi trường — cookie mới set qua YTDLP_COOKIES_B64_* im lặng vô tác
    dụng suốt thời gian dài mà không ai biết. Giờ check chặt hơn: cookie thật
    xuất từ browser cho youtube.com luôn có HÀNG CHỤC dòng (nhiều domain con
    google/youtube khác nhau) VÀ phải có ít nhất 1 cookie xác thực đăng nhập
    thật sự (SAPISID/APISID/SID/HSID/LOGIN_INFO) — thiếu là coi như vô dụng
    dù file không rỗng.
    """
    cp = _cookiefile_path()
    try:
        if not os.path.exists(cp) or os.path.getsize(cp) < 2048:
            return False
        with open(cp, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()
        has_auth_cookie = any(
            name in content for name in ("SAPISID", "APISID", "SID\t", "HSID", "LOGIN_INFO")
        )
        return has_auth_cookie
    except OSError:
        return False


def _normalize_netscape(text: str) -> bytes:
    """
    Chuẩn hoá cookies Netscape dán thẳng vào biến môi trường:
      • "\\n" dạng chuỗi (khi nền tảng nuốt xuống dòng) → xuống dòng thật;
      • dòng dữ liệu bị đổi TAB thành khoảng trắng → trả lại TAB (đủ 7 trường);
      • thiếu dòng tiêu đề "# Netscape HTTP Cookie File" → tự thêm (yt-dlp đòi).
    """
    text = text.strip().replace("\r\n", "\n").replace("\r", "\n")
    if "\n" not in text and "\\n" in text:
        text = text.replace("\\n", "\n").strip()
    out: list[str] = []
    for line in text.split("\n"):
        line = line.rstrip()
        if line and "\t" not in line and (not line.startswith("#") or line.startswith("#HttpOnly_")):
            fields = line.split(None, 6)
            if len(fields) == 7:
                line = "\t".join(fields)
        out.append(line)
    if not out or not out[0].startswith("# Netscape HTTP Cookie File"):
        out.insert(0, "# Netscape HTTP Cookie File")
    return ("\n".join(out) + "\n").encode("utf-8")


def _bootstrap_cookiefile() -> None:
    """
    Tự sinh cookies.txt từ biến môi trường lúc khởi động — hữu ích khi deploy
    (Railway...) không cho phép upload file trực tiếp vào container.

    Hỗ trợ 2 cách:
      1. YTDLP_COOKIES_B64          — 1 biến duy nhất (base64 của cookies.txt).
      2. YTDLP_COOKIES_B64_1, _2, _3, ... — chia nhiều phần, bot tự nối lại
         theo đúng thứ tự trước khi giải mã. BẮT BUỘC dùng cách này nếu chuỗi
         base64 dài hơn 32768 ký tự — đây là giới hạn CỨNG của Railway cho 1
         biến môi trường (thấy lỗi "Variable value exceeds maximum length of
         32768" nếu cố nhét hết vào 1 biến).

    Nếu cookies.txt đã tồn tại sẵn (VD build từ Docker image có sẵn file) thì
    KHÔNG ghi đè — chỉ sinh khi file chưa có/rỗng.
    """
    cp = _cookiefile_path()
    if _cookies_valid():
        logger.info("cookies.txt đã có sẵn và hợp lệ — bỏ qua bootstrap từ biến môi trường.")
        _lock_cookiefile_readonly(cp)
        return

    # Ưu tiên ghép nhiều phần YTDLP_COOKIES_B64_1, _2, _3... nếu có
    parts: list[str] = []
    i = 1
    while True:
        val = os.environ.get(f"YTDLP_COOKIES_B64_{i}", "")
        if not val:
            break
        parts.append(val)
        i += 1

    b64 = "".join(parts) if parts else os.environ.get("YTDLP_COOKIES_B64", "")
    plain = os.environ.get("YTDLP_COOKIES", "")
    if plain.strip() and not b64:
        # Cách nhanh nhất: dán THẲNG nội dung cookies.txt (Netscape) vào biến
        # YTDLP_COOKIES trên Railway — không cần mã hoá base64.
        try:
            raw = _normalize_netscape(plain)
            with open(cp, "wb") as f:
                f.write(raw)
            logger.info("cookies.txt đã được sinh từ YTDLP_COOKIES (dán thẳng, %d byte).", len(raw))
            if _cookies_valid():
                _lock_cookiefile_readonly(cp)
            else:
                logger.warning("YTDLP_COOKIES đã ghi nhưng KHÔNG hợp lệ (quá ngắn hoặc thiếu cookie đăng nhập "
                               "SAPISID/SID/HSID/LOGIN_INFO) — kiểm tra lại đã export đúng định dạng Netscape.")
        except Exception as exc:
            logger.warning("Không thể sinh cookies.txt từ YTDLP_COOKIES: %s", exc)
        return
    if not b64:
        # QUAN TRỌNG: trước đây return im lặng ở đây — không cách nào biết
        # được là do thiếu biến môi trường hay do lỗi khác. Giờ log rõ ràng
        # để không còn debug mù nữa.
        logger.warning(
            "Không tìm thấy YTDLP_COOKIES, YTDLP_COOKIES_B64 hay YTDLP_COOKIES_B64_1 nào trong "
            "biến môi trường — cookies.txt sẽ KHÔNG được tự sinh. Kiểm tra lại "
            "tên biến đã set trên Railway (Variables tab) có đúng chính xác "
            "'YTDLP_COOKIES_B64_1', '_2', '_3'... không (phân biệt hoa/thường, "
            "không thừa khoảng trắng)."
        )
        return

    try:
        raw = base64.b64decode(b64)
        with open(cp, "wb") as f:
            f.write(raw)
        source = f"{len(parts)} phần YTDLP_COOKIES_B64_1..{len(parts)}" if parts else "YTDLP_COOKIES_B64"
        logger.info("cookies.txt đã được sinh tự động từ %s (%d byte).", source, len(raw))
        if not _cookies_valid():
            logger.warning(
                "cookies.txt VỪA được sinh nhưng vẫn KHÔNG hợp lệ (thiếu cookie "
                "xác thực đăng nhập như SAPISID/APISID/SID/HSID/LOGIN_INFO). "
                "Nhiều khả năng lúc export cookie từ trình duyệt, tài khoản Google "
                "chưa thật sự đăng nhập, hoặc extension export thiếu cookie "
                "'HttpOnly' (1 số extension bỏ sót). Thử export lại bằng "
                "'Get cookies.txt LOCALLY' trong lúc đã chắc chắn đăng nhập "
                "YouTube/Google, mở đúng trang youtube.com trước khi export."
            )
        else:
            _lock_cookiefile_readonly(cp)
    except Exception as exc:
        logger.warning("Không thể sinh cookies.txt từ biến môi trường: %s", exc)


def _lock_cookiefile_readonly(cp: str) -> None:
    """
    Khoá cookies.txt thành chỉ-đọc (chmod 0444) — phòng trường hợp yt-dlp tự
    ghi ngược lại cookie jar vào CHÍNH file này giữa lúc chạy (1 số phiên bản
    yt-dlp có cơ chế tự "save" cookie jar sau mỗi request để cập nhật session
    token mới nhận được). Nếu điều đó xảy ra và ghi ra 1 bản thiếu/hỏng, file
    gốc coi như "chết" ngay giữa phiên chạy dù lúc khởi động vẫn còn hợp lệ —
    đúng hiện tượng quan sát được (valid lúc boot, invalid lúc /play). Khoá
    read-only khiến yt-dlp gặp lỗi permission khi cố ghi thay vì ghi đè âm
    thầm — yt-dlp tự bỏ qua lỗi ghi cookie jar (không ảnh hưởng đọc/dùng
    cookie để request), nhưng file gốc luôn được bảo toàn nguyên vẹn.
    """
    try:
        os.chmod(cp, 0o444)
    except OSError as exc:
        logger.warning("Không khoá được cookies.txt thành read-only: %s", exc)


_bootstrap_cookiefile()



def _dave_state(vc: Any) -> str:
    """Tóm tắt trạng thái mã hoá E2EE (DAVE) của voice connection — để chẩn đoán 'có tiếng ra nhưng không ai nghe'."""
    try:
        c = vc._connection
        sess = c.dave_session
        return (f"dave_version={c.dave_protocol_version} session={'yes' if sess else 'no'} "
                f"ready={getattr(sess, 'ready', None)} can_encrypt={c.can_encrypt} "
                f"downgraded={getattr(c, 'dave_downgraded', None)}")
    except Exception as exc:
        return f"dave_state_unavailable({exc})"


async def _wait_dave_ready(vc: Any, timeout: float = 6.0) -> bool:
    """
    Chờ phiên mã hoá DAVE sẵn sàng trước khi phát. Nếu bắt đầu gửi audio khi
    phiên chưa ready, discord.py gửi gói KHÔNG mã hoá → kênh bắt buộc E2EE sẽ
    bỏ gói → bot 'đang phát' nhưng không ai nghe thấy.
    """
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            c = vc._connection
            if c.dave_protocol_version == 0 or c.can_encrypt:
                return True
        except Exception:
            return True
        await asyncio.sleep(0.25)
    return False


class _CountingSource(discord.PCMVolumeTransformer):
    """
    PCMVolumeTransformer có đếm số byte PCM ffmpeg thực sự đã đưa ra. Dùng để
    phân biệt 2 kiểu "không có tiếng":
      • bytes_read == 0 sau vài giây → ffmpeg KẸT (không mở được/không tải được URL)
      • bytes_read > 0 mà vẫn không nghe gì → lỗi phía Discord/voice (thiếu quyền
        Speak, bot bị server-mute, DAVE/mã hoá voice...), không phải do YouTube.
    """
    def __init__(self, original, volume: float = 1.0) -> None:
        super().__init__(original, volume)
        self.bytes_read = 0
        self.peak = 0                      # biên độ lớn nhất (0..32767) — =0 nghĩa là toàn im lặng
        self.first_byte_at: float | None = None

    @property
    def underruns(self) -> int:
        return getattr(self.original, "underruns", 0)

    @property
    def buffered_s(self) -> float:
        return getattr(self.original, "buffered_s", 0.0)

    def read(self) -> bytes:
        data = super().read()
        if data:
            if self.first_byte_at is None:
                self.first_byte_at = time.monotonic()
            self.bytes_read += len(data)
            try:
                import array
                a = array.array("h")
                a.frombytes(data[: len(data) // 2 * 2])
                if a:
                    self.peak = max(self.peak, max(max(a), -min(a)))
            except Exception:
                pass
        return data


def _new_pcm(url: str, before_options: str, options: str) -> "discord.FFmpegPCMAudio":
    """
    Tạo FFmpegPCMAudio và GOM stderr của ffmpeg vào file tạm để ghi log khi bài
    kết thúc/chết. Trước đây stderr bị nuốt hết nên khi 403 / "Error opening
    input" bot chỉ im lặng — không có cách nào biết vì sao không ra tiếng.
    """
    import tempfile
    errf = tempfile.TemporaryFile()
    pcm = discord.FFmpegPCMAudio(
        url,
        before_options="-loglevel warning " + before_options,
        options=options,
        stderr=errf,
    )
    pcm._err_file = errf  # type: ignore[attr-defined]
    return pcm


def _log_ffmpeg_stderr(source: Any) -> str:
    """Đọc phần cuối stderr ffmpeg (nếu có), log ra và trả về chuỗi."""
    try:
        pcm = getattr(source, "original", source)
        errf = getattr(pcm, "_err_file", None)
        if errf is None:
            return ""
        errf.seek(0)
        tail = errf.read()[-800:].decode("utf-8", errors="ignore").strip()
        if tail:
            logger.warning("FFMPEG stderr: %s", tail.replace("\n", " | "))
        return tail
    except Exception:
        return ""


# ── Resolve: chiến lược lấy stream, circuit breaker, cache ─────────────────────
#
# Log thực tế cho thấy 3 chiến lược "direct" (không proxy) gần như LUÔN fail trên
# server (Railway = IP datacenter), mỗi lần tốn ~5s, rồi chiến lược proxy mới
# thành công → 15-20s từ lúc /play tới lúc có tiếng. Breaker ghi nhớ chiến lược
# nào đang chết (≥3 video KHÁC NHAU liên tiếp fail) và bỏ qua nó 10 phút, nên các
# bài sau đi thẳng vào chiến lược đang sống. Sau 10 phút nó thử lại 1 lần
# (half-open); fail tiếp là khoá lại ngay.

_YT_STRATEGIES: list[dict[str, Any]] = [
    # CẬP NHẬT 21/11/2026 theo source yt-dlp 2026.10.26 (INNERTUBE_CLIENTS) + log thực tế:
    #  • 'tv_embedded' và 'android' KHÔNG còn tồn tại trong yt-dlp → bị bỏ qua âm thầm, các chiến
    #    lược cũ chỉ còn 1 client thật. Client MẶC ĐỊNH hiện tại của yt-dlp là tv_samsung/visionos/web
    #    (có đăng nhập: web_embedded/tv_samsung/web) — bot chưa từng thử tv_samsung & visionos.
    #  • tv_samsung / tv_downgraded / web_embedded / visionos KHÔNG đòi GVS PO-Token (web/mweb/ios/
    #    android_vr thì đòi). Khi YouTube bật thí nghiệm "SABR-only" cho session/tài khoản
    #    ("Some web client https formats have been skipped… forcing SABR streaming"), web/mweb chỉ
    #    còn storyboard → "Requested format is not available"; các client TV vẫn trả link thẳng.
    #  • Client có hỗ trợ cookies mà gửi kèm cookies thì client KHÔNG hỗ trợ (visionos, android_vr,
    #    ios) bị yt-dlp bỏ qua → tách riêng strategy có cookies / không cookies.
    # THỨ TỰ THEO LOG 21/11 (6 bài liên tiếp): chỉ 'vision' thắng (~1.1s, không cookies/PO-Token/Deno);
    # samsung_ck & web luôn dính SABR ("Requested format is not available") và 'web' còn làm
    # bgutil-pot 17→69MB + python +30MB. → vision ĐẦU TIÊN; cookies-client chỉ là dự phòng
    # (video giới hạn tuổi cần đăng nhập).
    {"name": "vision",     "clients": ["visionos"],                       "cookies": False, "proxy": False},
    {"name": "samsung_ck", "clients": ["tv_samsung"],                    "cookies": True,  "proxy": False},
    # 'tv' = client DUY NHẤT (cùng tv_samsung/tv_downgraded/web_embedded/visionos) không đòi GVS PO-Token
    # mà bot CHƯA từng thử (đã kiểm bằng INNERTUBE_CLIENTS). Cookies vượt được bot-check (log 1LJKk3UFzwM:
    # samsung_ck lỗi format chứ không phải bot-check) nên đây là hướng hợp lý cho video bị chặn IP.
    {"name": "tv_ck",      "clients": ["tv"],                              "cookies": True,  "proxy": False},
    {"name": "embed_vr",   "clients": ["web_embedded", "android_vr"],     "cookies": False, "proxy": False},
    {"name": "tv_dg_ck",   "clients": ["tv_downgraded"],                  "cookies": True,  "proxy": False},
    # 'late' = chỉ thử SAU proxy. Log 21/11 (nhiều video): web luôn "Requested format is not available"
    # (SABR), web_nock luôn bot-check, chưa thắng lần nào; web còn tốn ~5s + làm RAM cgroup vọt lên 70-80%.
    {"name": "web",        "clients": ["web", "mweb"],                    "cookies": True,  "proxy": False, "late": True},
    {"name": "web_nock",   "clients": ["web", "mweb"],                    "cookies": False, "proxy": False, "late": True},
    # web_safari trả HLS (m3u8) — không dính "SABR-only" của https; cần GVS PO-Token (bgutil cấp được, log DIAG
    # 'Retrieved a gvs PO Token'). Nặng RAM (Deno + bgutil) nên 'heavy': bỏ qua khi cgroup ≥ 65%. THỬ NGHIỆM —
    # chưa kiểm chứng với YouTube thật; xem log 'RESOLVE OK | [safari_ck]'.
    {"name": "safari_ck",  "clients": ["web_safari"],                      "cookies": True,  "proxy": False, "late": True, "heavy": True},
    {"name": "proxy",      "clients": ["tv_samsung", "visionos", "web_embedded", "android_vr"], "cookies": False, "proxy": True},
]

_RESOLVE_ATTEMPT_TIMEOUT = 15.0   # tối đa cho 1 lần extract — tránh 1 attempt treo ăn hết thời gian
_RESOLVE_TOTAL_BUDGET    = 25.0   # tổng thời gian tối đa cho mọi chiến lược của 1 bài
_MEM_PRESSURE_HEAL       = 0.75   # cgroup dùng ≥ 75% giới hạn → coi là áp lực bộ nhớ
_RESOLVE_CACHE_TTL       = 300.0
_RESOLVE_CACHE_MAX       = 64


def _relieve_memory() -> str:
    """Chạy trong executor: trả RAM rảnh của Python + restart bgutil-pot (V8 nhúng, hay ngốn RAM)."""
    _mem.relieve()
    _pot.restart_now("áp lực bộ nhớ")
    return "mem-relieved"


class _YtHealth:
    """
    Sức khoẻ YouTube nói chung. Log 21/11: cả 7 chiến lược fail ~20s/bài rồi SoundCloud mới cứu
    (1.5s). Sau DEAD_AFTER bài liên tiếp mà mọi chiến lược YouTube đều fail, bài kế tiếp đi
    SoundCloud TRƯỚC (~2s); nếu SC không có thì mới chạy chuỗi YouTube. Cứ PROBE_EVERY giây lại
    thử YouTube trước 1 lần để biết khi nào nó sống lại (1 thành công → reset).
    """
    DEAD_AFTER  = 2
    PROBE_EVERY = 600.0

    def __init__(self) -> None:
        self.fails = 0
        self.probe_at = 0.0

    def ok(self) -> None:
        self.fails = 0

    def fail(self) -> None:
        self.fails += 1
        self.probe_at = time.monotonic()

    def sc_first(self) -> bool:
        if self.fails < self.DEAD_AFTER:
            return False
        if time.monotonic() - self.probe_at >= self.PROBE_EVERY:
            self.probe_at = time.monotonic()   # lượt thăm dò YouTube
            return False
        return True


_yt_health = _YtHealth()
_last_trim = 0.0


async def _trim_memory_soon(loop: asyncio.AbstractEventLoop) -> None:
    """gc + malloc_trim sau resolve (yt-dlp để lại nhiều object; python 99→142MB sau 5 bài). Tối đa 1 lần/60s."""
    global _last_trim
    if time.monotonic() - _last_trim < 60.0:
        return
    _last_trim = time.monotonic()
    try:
        await loop.run_in_executor(None, _mem.relieve)
    except Exception:  # noqa: BLE001
        pass


class _StrategyBreaker:
    TRIP_AFTER = 3
    COOLDOWN   = 600.0

    def __init__(self) -> None:
        self._fails: dict[str, int]   = {}
        self._last_vid: dict[str, str] = {}
        self._until: dict[str, float] = {}
        self._ok_at: dict[str, float] = {}

    def ok(self, name: str) -> None:
        self._fails[name] = 0
        self._until.pop(name, None)
        self._ok_at[name] = time.monotonic()

    def fail(self, name: str, video_id: str) -> None:
        # Cùng 1 video fail nhiều lần (DRM, giới hạn tuổi...) không phải lỗi của
        # chiến lược → chỉ đếm khi là video KHÁC với lần fail trước.
        if self._last_vid.get(name) == video_id:
            return
        self._last_vid[name] = video_id
        n = self._fails.get(name, 0) + 1
        self._fails[name] = n
        if n >= self.TRIP_AFTER:
            self._until[name] = time.monotonic() + self.COOLDOWN
            logger.warning("BREAKER | chiến lược '%s' fail %d video liên tiếp → bỏ qua %.0f phút",
                           name, n, self.COOLDOWN / 60)

    def tripped(self, name: str) -> bool:
        return self._until.get(name, 0.0) > time.monotonic()

    def direct_dead(self) -> bool:
        directs = [s["name"] for s in _YT_STRATEGIES if not s["proxy"]]
        return bool(directs) and all(self.tripped(n) for n in directs)

    def plan(self) -> list[dict[str, Any]]:
        directs = [s for s in _YT_STRATEGIES if not s["proxy"]]
        proxies = [s for s in _YT_STRATEGIES if s["proxy"]]
        alive = [s for s in directs if not self.tripped(s["name"])]
        dead  = [s for s in directs if self.tripped(s["name"])]
        # Giữ thứ tự tĩnh (xếp theo hiệu quả thực tế). Không xếp theo "thắng gần nhất" nữa: lỗi
        # "Requested format..."/"page needs to be reloaded" là theo TỪNG video, 1 video lạ thắng
        # bằng embed_vr không có nghĩa web hỏng. Chỉ đẩy chiến lược dùng cookies xuống cuối khi
        # cookies.txt không hợp lệ (gửi cookies rác vô ích).
        cookies_ok = _cookies_valid()
        now = time.monotonic()
        order = {s["name"]: i for i, s in enumerate(_YT_STRATEGIES)}
        def _key(s: dict[str, Any]) -> tuple[int, int, float, int]:
            no_ck = 1 if (s["cookies"] and not cookies_ok) else 0
            # Chiến lược vừa thắng trong 20 phút qua → thử TRƯỚC (thí nghiệm SABR của YouTube theo
            # session/tài khoản, không theo video: thắng gần đây = khả năng cao thắng tiếp).
            ok_at = self._ok_at.get(s["name"], 0.0)
            recent = 0 if (ok_at and now - ok_at < 1200.0) else 1
            return (no_ck, recent, -ok_at if recent == 0 else 0.0, order[s["name"]])
        alive.sort(key=_key)
        # Proxy datacenter hầu như luôn dính bot-check → chỉ thử 1 lần/bài, và bỏ qua khi breaker đã khoá.
        live_proxies = [s for s in proxies if not self.tripped(s["name"])]
        dead += [s for s in proxies if self.tripped(s["name"])]
        main = [s for s in alive if not s.get("late")]
        late = [s for s in alive if s.get("late")]
        return main + live_proxies + late + dead


_breaker = _StrategyBreaker()
_resolve_cache: dict[str, tuple[float, dict[str, Any], bool, str | None]] = {}
_search_cache: dict[str, tuple[float, str]] = {}


def _resolve_cache_get(video_url: str) -> tuple[dict[str, Any], bool, str | None] | None:
    hit = _resolve_cache.get(video_url)
    if hit and (time.monotonic() - hit[0]) < _RESOLVE_CACHE_TTL:
        return hit[1], hit[2], hit[3]
    _resolve_cache.pop(video_url, None)
    return None


_SLIM_KEYS = ("title", "url", "webpage_url", "thumbnail", "uploader", "channel", "duration", "id", "http_headers")


def _resolve_cache_put(video_url: str, data: dict[str, Any], via_proxy: bool, proxy: str | None) -> None:
    # Chỉ giữ các trường Track thực sự dùng. Dict gốc của yt-dlp có cả danh sách format/phụ đề/
    # caption (hàng trăm KB mỗi bài) — giữ 64 bản trong RAM suốt đời process là lãng phí khi
    # container bị giới hạn bộ nhớ và Deno cần chỗ để giải n-challenge.
    now = time.monotonic()
    for k in [k for k, v in _resolve_cache.items() if now - v[0] >= _RESOLVE_CACHE_TTL]:
        _resolve_cache.pop(k, None)
    if len(_resolve_cache) >= _RESOLVE_CACHE_MAX:
        _resolve_cache.pop(min(_resolve_cache, key=lambda k: _resolve_cache[k][0]), None)
    _resolve_cache[video_url] = (now, {k: data[k] for k in _SLIM_KEYS if k in data}, via_proxy, proxy)


_UNAVAILABLE_MARKERS = (
    "video is unavailable", "video unavailable", "this video is private",
    "video has been removed", "account associated with this video",
    "private video", "video is no longer available",
)


# Client mà URL googlevideo CẦN PO-Token (yt-dlp: GVS_PO_TOKEN_POLICY required=True). Log 21/11 (75Tqdnwitgw):
# qua proxy yt-dlp chọn itag 18 của ANDROID_VR nhưng URL KHÔNG có "pot=" → ffmpeg 403 ngay → bot refresh 6 lần
# bằng chính URL chết đó (32s rồi bỏ bài). Giờ: URL kiểu này phải qua 1 request thử trước khi nhận.
_POT_CLIENT_IDS = ("android_vr", "ios", "android", "web", "mweb")
_POT_URL_CLIENTS = tuple(c.upper() for c in _POT_CLIENT_IDS)


def _url_client(url: str) -> str:
    import urllib.parse as _up
    return (_up.parse_qs(_up.urlparse(url or "").query).get("c") or [""])[0].upper()


def _needs_probe(data: dict[str, Any]) -> bool:
    u = str(data.get("url") or "")
    return bool(u) and _url_client(u) in _POT_URL_CLIENTS and "pot=" not in u


def _probe_sync(data: dict[str, Any], proxy: str | None) -> bool:
    """GET Range 0-1 với đúng http_headers của yt-dlp (+ đúng proxy đã resolve). False CHỈ khi 401/403/404/410;
    lỗi mạng/timeout → True (không phạt oan, ffmpeg còn cơ chế retry riêng)."""
    import httpx
    url = str(data.get("url") or "")
    hdrs = {str(k): str(v) for k, v in (data.get("http_headers") or {}).items() if k and v}
    hdrs["Range"] = "bytes=0-1"
    kw: dict[str, Any] = {"verify": False, "timeout": 6.0, "follow_redirects": True, "headers": hdrs}
    try:
        try:
            cli = httpx.Client(proxy=proxy, **kw) if proxy else httpx.Client(**kw)
        except TypeError:                       # httpx cũ: tham số tên "proxies"
            cli = httpx.Client(proxies=proxy, **kw) if proxy else httpx.Client(**kw)
        with cli as c, c.stream("GET", url) as r:
            code = r.status_code
    except Exception:  # noqa: BLE001
        return True
    if code in (401, 403, 404, 410):
        logger.warning("PROBE | HTTP %d cho client=%s (%s)", code, _url_client(url) or "?", "proxy" if proxy else "direct")
        return False
    return True


async def _extract_verified(
    opts: dict[str, Any], video_url: str, loop: asyncio.AbstractEventLoop,
) -> dict[str, Any]:
    """extract + (chỉ khi client cần PO-Token mà URL thiếu pot=) kiểm tra URL; 403 → thử lại 1 lần với
    các client KHÔNG đòi PO-Token (tv_samsung/visionos/web_embedded...) trên cùng proxy; vẫn hỏng → raise."""
    import copy
    data = await loop.run_in_executor(None, _extract_sync, opts, video_url)
    if not _needs_probe(data):
        return data
    px = opts.get("proxy")
    if await loop.run_in_executor(None, _probe_sync, data, px):
        return data
    clients = list((opts.get("extractor_args", {}).get("youtube", {}) or {}).get("player_client") or [])
    safe = [c for c in clients if c not in _POT_CLIENT_IDS]
    if not safe or safe == clients:
        raise RuntimeError("stream probe 403 (client thiếu PO-Token)")
    logger.info("PROBE | URL %s 403 (thiếu pot) → thử lại không dùng %s", _url_client(data["url"]),
                [c for c in clients if c in _POT_CLIENT_IDS])
    o2 = copy.deepcopy(opts)
    o2["extractor_args"]["youtube"]["player_client"] = safe
    data = await loop.run_in_executor(None, _extract_sync, o2, video_url)
    if _needs_probe(data) and not await loop.run_in_executor(None, _probe_sync, data, px):
        raise RuntimeError("stream probe 403 (client thiếu PO-Token)")
    return data


def _win_info(data: dict[str, Any]) -> str:
    """Client thắng không nằm trong info dict → suy từ format đã chọn (c= trong URL googlevideo)."""
    import urllib.parse as _up
    u = str(data.get("url") or "")
    c = (_up.parse_qs(_up.urlparse(u).query).get("c") or ["?"])[0]
    return f"client={c} fmt={data.get('format_id', '?')} proto={data.get('protocol', '?')}"


_PROXY_RACE_WIDTH  = 2      # số proxy chạy song song mỗi vòng
_PROXY_RACE_ROUNDS = 2      # tối đa 2 vòng (4 IP khác nhau) nếu còn ngân sách thời gian
_PROXY_ROUND2_MAX_ELAPSED = 12.0


async def _proxy_race(
    opts: dict[str, Any], video_url: str, loop: asyncio.AbstractEventLoop, t_start: float,
) -> dict[str, Any]:
    """
    Chiến lược proxy: IP datacenter của Webshare có IP bị bot-check, có IP không (log 1HaK6z2l2Lw:
    1 proxy dính bot-check là mất cả bài). Đua _PROXY_RACE_WIDTH proxy KHÁC NHAU song song, ai xong
    trước thắng; cả vòng fail thì thêm 1 vòng IP mới. Thành công → opts["proxy"] = proxy thắng.
    RAM: chỉ đua song song khi cgroup < 60% (mỗi extract thêm ~30MB python).
    Raise lỗi cuối cùng nếu hết cách (hoặc ngay nếu video thật sự không tồn tại).
    """
    import copy
    # Lần đầu sau khi khởi động, danh sách Webshare (API) có thể chưa về → đừng để proxy free công cộng
    # đứng đầu (log 21/11: thắng bằng 34.43.46.91:80 — proxy free, URL sau đó 403). Chờ tối đa 3s.
    for _ in range(30):
        if not _proxy._fetching:
            break
        await asyncio.sleep(0.1)
    tried: set[str] = set()
    last: Exception | None = None
    for rnd in range(_PROXY_RACE_ROUNDS):
        if rnd and time.monotonic() - t_start > _PROXY_ROUND2_MAX_ELAPSED:
            break
        press = _mem.pressure()
        width = _PROXY_RACE_WIDTH if (press is None or press < 0.60) else 1
        proxies = _proxy.pick(width, exclude=tried)
        if not proxies:
            break
        tried.update(proxies)
        tasks: dict[asyncio.Future, str] = {}
        for px in proxies:
            o = copy.deepcopy(opts)
            _set_proxy(o, px)
            tasks[asyncio.ensure_future(asyncio.wait_for(
                _extract_verified(o, video_url, loop),
                timeout=_RESOLVE_ATTEMPT_TIMEOUT))] = px
        pending = set(tasks)
        try:
            while pending:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for fut in done:
                    px = tasks[fut]
                    try:
                        data = fut.result()
                    except asyncio.TimeoutError:
                        last = RuntimeError(f"yt-dlp timeout sau {_RESOLVE_ATTEMPT_TIMEOUT:.0f}s (proxy)")
                        _proxy.mark_dead(px, temporary=True)
                        continue
                    except Exception as e:  # noqa: BLE001
                        last = e
                        low = str(e).lower()
                        if any(x in low for x in _UNAVAILABLE_MARKERS) and "format" not in low:
                            raise
                        if "stream probe" not in low:      # proxy vẫn tốt, chỉ là client thiếu PO-Token
                            _proxy.mark_dead(px, temporary=not ("407" in low or "proxy authentication" in low))
                        logger.info("PROXY RACE | ...%s fail: %s", px[-15:], str(e)[:60].replace("\n", " "))
                        continue
                    opts["proxy"] = px
                    _set_proxy(opts, px)
                    logger.info("PROXY RACE | thắng ...%s (vòng %d, %d proxy)", px[-15:], rnd + 1, len(proxies))
                    return data
        finally:
            for fut in pending:       # thread yt-dlp thua cuộc tự chạy nốt, kết quả bị bỏ
                fut.cancel()
    raise last or RuntimeError("không có proxy khả dụng")


async def _resolve_stream(
    video_url: str, loop: asyncio.AbstractEventLoop,
) -> tuple[dict[str, Any] | None, str | None, Exception | None]:
    """
    Thử lần lượt các chiến lược theo _breaker.plan().
    Trả về (data, proxy_đã_dùng, None) nếu thành công, (None, None, last_err) nếu hết cách.
    Raise ngay nếu video THẬT SỰ không tồn tại/private.
    """
    vm = re.search(r"(?:v=|/)([\w-]{11})(?:[&?/]|$)", video_url)
    vid = vm.group(1) if vm else video_url
    plan = _breaker.plan()
    last_err: Exception | None = None
    t_start = time.monotonic()
    logger.info("MEM | %s", _mem.snapshot())
    # Chỉ "phạt" chiến lược trong breaker khi 1 chiến lược KHÁC cuối cùng thắng ở video đó.
    # Nếu mọi chiến lược đều fail thì lỗi nằm ở VIDEO (giới hạn tuổi, hội viên, bị gỡ...) chứ
    # không phải chiến lược hỏng — phạt lúc đó sẽ khoá oan cả 'web' đang chạy tốt với video khác.
    pending_fails: list[str] = []
    skip_names: set[str] = set()
    healed = False      # chỉ tự hồi phục PO-Token provider 1 lần cho mỗi bài
    escalated = False   # chỉ nhảy sang proxy 1 lần/bài khi gặp bot-check
    bot_err: Exception | None = None   # lỗi bot-check đầu tiên — là NGUYÊN NHÂN THẬT, đừng để lỗi phụ ("page needs to be reloaded") che mất

    i = -1
    while i + 1 < len(plan):
        i += 1
        strat = plan[i]
        if strat["name"] in skip_names:
            continue
        if strat.get("heavy") and (_mem.pressure() or 0.0) >= 0.65:
            try:   # cuộc đua proxy + yt-dlp để lại nhiều RAM (log: cgroup 48% → 80%) → trả bớt rồi đo lại
                await asyncio.wait_for(loop.run_in_executor(None, _mem.relieve), timeout=5.0)
            except Exception:  # noqa: BLE001
                pass
            if (_mem.pressure() or 0.0) >= 0.65:
                logger.info("RESOLVE | bỏ qua [%s]: RAM cgroup ≥ 65%%", strat["name"])
                continue
        if i and time.monotonic() - t_start > _RESOLVE_TOTAL_BUDGET:
            logger.warning("RESOLVE | quá %.0fs, dừng thử thêm chiến lược", _RESOLVE_TOTAL_BUDGET)
            break
        opts = _ytdl_opts(strat["cookies"], use_proxy=strat["proxy"])
        opts["extractor_args"]["youtube"] = {
            "player_client": strat["clients"],
            "skip": ["translated_subs", "comments"],
        }
        logger.info("Client rotation | attempt %d/%d [%s] → %s (proxy=%s)", i + 1, len(plan),
                    strat["name"], strat["clients"], "yes" if opts.get("proxy") else "no")
        t0 = time.monotonic()
        oom0 = (_mem.cgroup().get("oom_kill") or 0)
        try:
            if strat["proxy"]:
                opts.pop("proxy", None)
                data = await _proxy_race(opts, video_url, loop, t_start)
            else:
                data = await asyncio.wait_for(
                    _extract_verified(opts, video_url, loop),
                    timeout=_RESOLVE_ATTEMPT_TIMEOUT,
                )
            _breaker.ok(strat["name"])
            for _n in pending_fails:
                _breaker.fail(_n, vid)
            logger.info("RESOLVE OK | [%s] %.1fs | %s%s", strat["name"], time.monotonic() - t0,
                        _win_info(data), " | proxy=..." + opts["proxy"][-15:] if opts.get("proxy") else "")
            return data, opts.get("proxy"), None
        except asyncio.TimeoutError:
            last_err = RuntimeError(f"yt-dlp timeout sau {_RESOLVE_ATTEMPT_TIMEOUT:.0f}s ({strat['name']})")
            err_str = str(last_err)
        except Exception as e:  # noqa: BLE001
            last_err = e
            err_str = str(e)
            # Không bắt nhầm "Requested format is not available" (lỗi format, retry được).
            # So khớp KHÔNG phân biệt hoa/thường: yt-dlp báo "Video unavailable" (V hoa) nên
            # bản cũ so sánh trực tiếp với chuỗi thường không bao giờ khớp → video chết thật
            # vẫn bị thử hết mọi chiến lược trước khi báo lỗi.
            _low = err_str.lower()
            if any(x in _low for x in _UNAVAILABLE_MARKERS) and "format" not in _low:
                raise

        # Lỗi kiểu "Requested format is not available"/"page needs to be reloaded" ở client web có
        # cookies. Log thực tế (pot=ok, warn: 'Error running deno process (returncode: -9)') cho thấy
        # nguyên nhân hay gặp là Deno — bộ giải n-challenge — bị SIGKILL từ ngoài (nghi OOM của
        # container), KHÔNG phải PO-Token. Xử lý:
        #   • RAM cgroup ≥ 75% → giải phóng (gc + malloc_trim + restart bgutil-pot) rồi thử lại web;
        #   • còn lại → heal provider (restart nếu chết/không ping); xoá cache token KHÔNG thử lại
        #     (log thực tế: xoá cache xong thử lại vẫn fail, chỉ tốn thêm ~5s).
        _el = err_str.lower()
        # Chỉ 'web' phụ thuộc PO-Token provider (bgutil-pot); các client TV không cần PO-Token nên
        # restart provider khi chúng fail là vô ích (log 21/11: pot=ok, oom_kill=0, lỗi do SABR).
        if (not healed and strat["name"] == "web"
                and ("requested format is not available" in _el or "page needs to be reloaded" in _el)):
            healed = True
            press = _mem.pressure()
            snap = _mem.snapshot()
            # Đo lúc lỗi thì Deno đã chết nên cgroup trông "rảnh"; dấu hiệu chắc chắn là oom_kill tăng.
            oom_hit = (_mem.cgroup().get("oom_kill") or 0) > oom0
            try:
                if oom_hit or (press is not None and press >= _MEM_PRESSURE_HEAL):
                    action = await asyncio.wait_for(loop.run_in_executor(None, _relieve_memory), timeout=25.0)
                else:
                    action = await asyncio.wait_for(
                        loop.run_in_executor(None, _pot.heal, f"{strat['name']}: {err_str[:50]}"), timeout=20.0)
            except Exception as _he:  # noqa: BLE001
                action = f"lỗi: {_he}"
            logger.warning("HEAL | [%s] lỗi '%s' → %s | oom_hit=%s | pot=%s | MEM %s", strat["name"],
                           err_str[:50].replace("\n", " "), action, oom_hit, _pot.status(), snap)
            if action in ("restarted", "mem-relieved"):
                plan.insert(i + 1, strat)   # thử lại đúng chiến lược này; chưa tính là "fail" của nó
                continue

        pending_fails.append(strat["name"])
        if bot_err is None and "sign in to confirm" in _el:
            bot_err = last_err
        # Bot-check ("Sign in to confirm...") khi đi thẳng từ IP Railway: log 5GG_MkQYQq0 & qe7stKL5qco
        # cho thấy mọi chiến lược trực tiếp còn lại đều fail y hệt (~13s) rồi proxy mới thắng → nhảy
        # thẳng sang proxy (1 lần/bài), các chiến lược trực tiếp còn lại thử sau nếu proxy cũng hỏng.
        if (not strat["proxy"] and not escalated and "sign in to confirm" in err_str.lower()
                and "sign in to confirm" in _el):
            for _j in range(i + 2, len(plan)):
                if plan[_j]["proxy"] and plan[_j]["name"] not in skip_names:
                    plan.insert(i + 1, plan.pop(_j))
                    escalated = True
                    logger.info("RESOLVE | bot-check ở [%s] → nhảy thẳng sang proxy", strat["name"])
                    break
        if strat["proxy"] and "sign in to confirm" in err_str.lower():
            skip_names.add(strat["name"])   # đã đua nhiều IP proxy khác nhau trong _proxy_race → khỏi thử lại
            if escalated:
                # Bot-check theo IP: vision + proxy đều dính → embed_vr/web_nock (không cookies) và web
                # (SABR, +50MB RAM, ~5s) gần như chắc chắn fail y hệt (log 1HaK6z2l2Lw: tốn ~12s vô ích).
                # Giữ lại samsung_ck/tv_dg_ck (có cookies, rẻ 1-3s) cho video cần đăng nhập.
                skip_names.update({"embed_vr", "web", "web_nock"})
                logger.info("RESOLVE | bot-check cả direct lẫn proxy → bỏ embed_vr/web/web_nock (còn tv_ck/samsung_ck/tv_dg_ck/safari_ck dùng cookies)")
        current = opts.get("proxy", "")
        if current and not strat["proxy"]:
            if "407" in err_str or "Proxy Authentication" in err_str:
                _proxy.mark_dead(current, temporary=False)
                if current == _proxy.PROXY_US:
                    logger.warning("PROXY_US bị 407 — credentials sai/hết hạn! Cần update Secrets.")
            else:
                _proxy.mark_dead(current, temporary=True)
        logger.warning("yt-dlp attempt %d [%s] failed (%s) %.1fs (proxy=%s)", i + 1, strat["name"],
                       err_str[:80].replace("\n", " "), time.monotonic() - t0,
                       current[-15:] if current else "none")
        if i < len(plan) - 1:
            await asyncio.sleep(0.3)

    return None, None, (bot_err or last_err)


class _WarnCapture:
    """Logger giả cho yt-dlp: gom warning/error để đưa vào log chẩn đoán (opts mặc định tắt hết)."""
    _DBG_KEYS = ("pot", "po token", "n challenge", "jsc", "sabr", "cookie", "login", "skipping",
                 "client", "player", "signature", "deno", "visitor", "logged")

    def __init__(self) -> None:
        self.msgs: list[str] = []
        self.dbg: list[str] = []
    def debug(self, msg: str) -> None:
        m = str(msg)
        if len(self.dbg) < 14 and any(k in m.lower() for k in self._DBG_KEYS):
            self.dbg.append(m[:140])
    def info(self, msg: str) -> None: pass
    def warning(self, msg: str) -> None: self.msgs.append(str(msg))
    def error(self, msg: str) -> None: self.msgs.append(str(msg))


async def _diagnose_failure(video_url: str, loop: asyncio.AbstractEventLoop) -> tuple[str, str]:
    """
    Khi MỌI chiến lược đều fail: hỏi yt-dlp metadata (không đòi format) để biết vì sao.
    Trả về (tóm tắt để log, gợi ý tiếng Việt cho người dùng — rỗng nếu không rõ).
    Tóm tắt có cả cảnh báo của yt-dlp (vd "formats were skipped as they require a GVS PO Token")
    và trạng thái bgutil-pot — để lần sau biết lỗi là do video hay do PO-Token provider.
    """
    cap = _WarnCapture()
    pot = _pot.status()
    try:
        opts = _ytdl_opts(True, use_proxy=False)
        opts["extractor_args"]["youtube"] = {"player_client": ["web"], "skip": ["translated_subs", "comments"]}
        opts["ignore_no_formats_error"] = True
        opts["no_warnings"] = False
        opts["verbose"] = True   # để logger nhận cả dòng debug (PO token/jsc/cookies) — chỉ dùng cho chẩn đoán
        opts["logger"] = cap
        info = await asyncio.wait_for(loop.run_in_executor(None, _extract_sync, opts, video_url), timeout=10.0)
    except Exception as exc:  # noqa: BLE001
        return f"chẩn đoán lỗi: {str(exc)[:120]} | pot={pot} | warn={' / '.join(cap.msgs)[:200]}", ""
    _diag_meta[video_url] = {"title": info.get("title") or "", "duration": info.get("duration"),
                             "uploader": info.get("uploader") or info.get("channel") or ""}
    if len(_diag_meta) > 64:
        _diag_meta.pop(next(iter(_diag_meta)), None)
    fmts = info.get("formats") or []
    n_url = sum(1 for f in fmts if f.get("url"))
    n_audio = sum(1 for f in fmts if f.get("url") and f.get("acodec") not in (None, "none"))
    avail, age, live = info.get("availability"), info.get("age_limit"), info.get("live_status")
    warn = " / ".join(m.replace("\n", " ")[:140] for m in cap.msgs[:3])
    summary = (f"availability={avail} age_limit={age} live_status={live} formats={len(fmts)} có_url={n_url} "
               f"có_audio={n_audio} | pot={pot} | warn={warn or '-'} | MEM {_mem.snapshot()}"
               f" | dbg={' ‖ '.join(cap.dbg) or '-'}")
    deno_failed = any("n challenge solving failed" in m or "returncode: -9" in m for m in cap.msgs)
    hint = ""
    if deno_failed and n_audio == 0:
        hint = "Bộ giải mã chữ ký của bot (Deno) bị dừng giữa chừng (nghi thiếu RAM) — thử lại sau vài giây."
    elif (age or 0) >= 18 or avail == "needs_auth":
        hint = "Video giới hạn độ tuổi/cần đăng nhập — tài khoản dùng cho cookies phải là tài khoản đã xác minh 18+."
    elif avail in ("subscriber_only", "premium_only"):
        hint = "Video chỉ dành cho hội viên kênh/YouTube Premium."
    elif live in ("is_upcoming", "post_live"):
        hint = "Video là buổi phát trực tiếp chưa/vừa kết thúc nên chưa lấy được stream."
    elif fmts and n_audio == 0:
        hint = "YouTube không đưa link stream âm thanh trực tiếp cho video này — thử bản upload khác."
    return summary, hint


class _BufferedPCM(discord.AudioSource):
    """
    Bộ đệm PCM: 1 thread nền đọc ffmpeg liên tục vào hàng đợi, player của discord.py
    chỉ lấy từ hàng đợi.

    Vì sao: ffmpeg mở stream qua proxy mất vài giây rồi đầu bài đọc chậm hơn thời
    gian thực → player đói frame → nghe giật ở đầu bài, sau đó ổn. Ở đây ta CHỜ có
    sẵn ~2-3s đệm rồi mới phát, và nếu giữa bài đệm cạn thì phát im lặng cho tới
    khi đệm lại ≥0.5s (rebuffer) thay vì giật từng frame.
    """
    FRAME = 3840          # 20ms @ 48kHz s16le stereo
    FPS   = 50

    def __init__(self, inner: "discord.FFmpegPCMAudio", target_s: float = 2.0,
                 max_s: float = 12.0, rebuffer_s: float = 0.5) -> None:
        self._inner = inner
        self._frames: collections.deque[bytes] = collections.deque()
        self._cond = threading.Condition()
        self._target = int(target_s * self.FPS)
        self._max    = int(max_s * self.FPS)
        self._rebuf  = max(1, int(rebuffer_s * self.FPS))
        self._eof = False
        self._stopped = False
        self._rebuffering = False
        self._silence = b"\x00" * self.FRAME
        self.underruns = 0
        self._err_file = getattr(inner, "_err_file", None)  # để _log_ffmpeg_stderr vẫn đọc được
        threading.Thread(target=self._pump, daemon=True, name="pcm-prebuffer").start()

    def _pump(self) -> None:
        try:
            while True:
                with self._cond:
                    while len(self._frames) >= self._max and not self._stopped:
                        self._cond.wait(0.5)
                    if self._stopped:
                        return
                data = self._inner.read()
                with self._cond:
                    if not data:
                        self._eof = True
                        self._cond.notify_all()
                        return
                    self._frames.append(data)
                    self._cond.notify_all()
        except Exception:  # ffmpeg bị kill/cleanup giữa chừng → coi như hết stream
            with self._cond:
                self._eof = True
                self._cond.notify_all()

    @property
    def buffered_s(self) -> float:
        return len(self._frames) / self.FPS

    async def wait_ready(self, timeout: float) -> bool:
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            with self._cond:
                if len(self._frames) >= self._target or self._eof:
                    return True
            await asyncio.sleep(0.05)
        return False

    def read(self) -> bytes:
        with self._cond:
            n = len(self._frames)
            if self._rebuffering and (n >= self._rebuf or self._eof):
                self._rebuffering = False
            if not self._rebuffering and n:
                data = self._frames.popleft()
                self._cond.notify_all()
                return data
            if n == 0 and self._eof:
                return b""
            if not self._rebuffering:
                self._rebuffering = True
                self.underruns += 1
            return self._silence

    def cleanup(self) -> None:
        with self._cond:
            self._stopped = True
            self._cond.notify_all()
        try:
            self._inner.cleanup()
        except Exception:
            pass


_PREBUFFER_TIMEOUT = 12.0
PIPED_FIRST = os.environ.get("PIPED_FIRST", "0") == "1"


# ── Fallback cuối: SoundCloud ──────────────────────────────────────────────────
# Khi MỌI chiến lược YouTube đều fail (IP datacenter bị bot-check / tài khoản dính thí nghiệm SABR),
# tìm đúng bài trên SoundCloud (không có bot-check kiểu YouTube, yt-dlp hỗ trợ sẵn). Chỉ nhận kết quả
# khi tên bài khớp VÀ độ dài lệch ít (tránh nhầm bản remix/preview 30s). Tắt bằng SC_FALLBACK=0.
_SC_ENABLED = os.environ.get("SC_FALLBACK", "1").strip() != "0"
_SC_STOPWORDS = frozenset({
    "official", "video", "audio", "lyrics", "lyric", "mv", "hd", "4k", "hq", "visualizer",
    "topic", "vevo", "ft", "feat", "featuring", "music", "the", "a", "of", "and", "full", "song",
})
_diag_meta: dict[str, dict[str, Any]] = {}   # video_url → {title, duration, uploader} (từ _diagnose_failure)


def _title_tokens(text: str) -> list[str]:
    return [t for t in re.findall(r"\w+", (text or "").lower()) if len(t) > 1 and t not in _SC_STOPWORDS]


def _sc_pick(entries: list[dict[str, Any]], title: str, duration: int | float | None) -> dict[str, Any] | None:
    """Chọn kết quả SoundCloud khớp nhất; None nếu không đủ tin cậy."""
    want = set(_title_tokens(title))
    if not want:
        return None
    best: tuple[float, dict[str, Any]] | None = None
    for e in entries:
        if not e or not e.get("url"):
            continue
        have = set(_title_tokens(e.get("title", "")) + _title_tokens(e.get("uploader", "")))
        overlap = len(want & have) / len(want)
        if overlap < 0.6:
            continue
        cd = e.get("duration")
        bonus = 0.0
        if duration and cd:
            diff = abs(float(duration) - float(cd))
            if diff > max(8.0, 0.06 * float(duration)):
                continue            # khác độ dài đáng kể → bản khác/preview
            bonus = 0.3 * (1.0 - min(diff / 10.0, 1.0))
        elif cd and cd < 60:
            continue                # không biết độ dài YouTube: loại bản preview ngắn
        score = overlap + bonus
        if best is None or score > best[0]:
            best = (score, e)
    return best[1] if best else None


async def _yt_oembed_title(video_url: str) -> str:
    """Tên video qua oEmbed (không cần đăng nhập, ít bị bot-check) — dùng khi không có metadata nào khác."""
    try:
        async with _httpx.AsyncClient(timeout=4.0) as client:
            r = await client.get(
                "https://www.youtube.com/oembed", params={"url": video_url, "format": "json"})
        if r.status_code == 200:
            return str(r.json().get("title") or "")
    except Exception:  # noqa: BLE001
        pass
    return ""


_ALT_UPLOAD_ENABLED = os.environ.get("ALT_UPLOAD", "1") != "0"


async def _alt_upload_resolve(
    title: str, duration: int | float | None, bad_id: str, loop: asyncio.AbstractEventLoop,
) -> dict[str, Any] | None:
    """
    Khi MỌI chiến lược YouTube fail với 1 video: bot-check ở đây có vẻ theo TỪNG video (log 21/11: visionos
    thắng ~1.1s ở hầu hết video, chỉ vài bản "1 HOUR" bị chặn ở mọi IP/proxy/cookies). Tìm bản upload KHÁC của
    cùng bài (cùng tiêu đề + độ dài ±6%) rồi thử riêng visionos (~1s, không cookies/proxy) tối đa 2 ứng viên.
    Rẻ hơn và đúng bài hơn SoundCloud. Tắt bằng biến môi trường ALT_UPLOAD=0.
    """
    if not _ALT_UPLOAD_ENABLED or not title:
        return None
    t0 = time.monotonic()
    try:
        q = " ".join(_title_tokens(title))[:80] or title[:80]
        s_opts = {"quiet": True, "no_warnings": True, "extract_flat": True, "socket_timeout": 8,
                  "default_search": "ytsearch"}
        info = await asyncio.wait_for(
            loop.run_in_executor(None, _extract_sync, s_opts, f"ytsearch8:{q}"), timeout=8.0)
        entries = [e for e in (info.get("entries") or []) if e and e.get("id") and e.get("id") != bad_id]
        for e in entries:
            e["url"] = f"https://www.youtube.com/watch?v={e['id']}"
        tried = 0
        while entries and tried < 2 and time.monotonic() - t0 < 10.0:
            pick = _sc_pick(entries, title, duration)
            if not pick:
                break
            entries = [e for e in entries if e is not pick]
            tried += 1
            opts = _ytdl_opts(False, use_proxy=False)
            opts["extractor_args"]["youtube"] = {"player_client": ["visionos"], "skip": ["translated_subs", "comments"]}
            try:
                data = await asyncio.wait_for(_extract_verified(opts, pick["url"], loop), timeout=8.0)
            except Exception as exc:  # noqa: BLE001
                logger.info("ALT UPLOAD | %s fail: %s", pick["id"], str(exc)[:60].replace("\n", " "))
                continue
            logger.info("ALT UPLOAD OK | %s → '%s' (%s) %.1fs", bad_id, str(data.get("title"))[:50],
                        pick["id"], time.monotonic() - t0)
            return data
        logger.info("ALT UPLOAD | không có bản thay thế khớp cho '%s'", title[:60])
    except Exception as exc:  # noqa: BLE001
        logger.warning("ALT UPLOAD lỗi: %s", str(exc)[:100])
    return None


async def _soundcloud_resolve(
    title: str, duration: int | float | None, loop: asyncio.AbstractEventLoop,
) -> dict[str, Any] | None:
    if not _SC_ENABLED or not title:
        return None
    t0 = time.monotonic()
    try:
        q = " ".join(_title_tokens(title))[:80] or title[:80]
        s_opts = {"quiet": True, "no_warnings": True, "extract_flat": True, "socket_timeout": 8}
        info = await asyncio.wait_for(
            loop.run_in_executor(None, _extract_sync, s_opts, f"scsearch8:{q}"), timeout=10.0)
        pick = _sc_pick([e for e in (info.get("entries") or []) if e], title, duration)
        if not pick:
            logger.info("SC FALLBACK | không có kết quả khớp cho '%s'", title[:60])
            return None
        # yt-dlp xếp opus > mp3 nên "bestaudio" trên SoundCloud chọn hls_opus (.m3u8, segment Ogg) —
        # ffmpeg đọc xong playlist (22806 B ≈ playlist HLS của bài 252s) rồi KHÔNG ra PCM nào (log 21/11:
        # peak=0, đệm=0.0s → im lặng). Ép progressive http(s) mp3 trước; HLS chỉ là phương án chót.
        d_opts = {"quiet": True, "no_warnings": True, "noplaylist": True, "socket_timeout": 8,
                  "format": ("bestaudio[protocol^=http][ext=mp3]/http_mp3_128/"
                             "bestaudio[protocol^=http]/bestaudio/best")}
        data = await asyncio.wait_for(
            loop.run_in_executor(None, _extract_sync, d_opts, pick["url"]), timeout=12.0)
        if not data.get("url"):
            return None
        if ".m3u8" in str(data.get("url")) or "m3u8" in str(data.get("protocol") or ""):
            logger.warning("SC FALLBACK | chỉ có HLS (%s) — ffmpeg dễ không ra tiếng", data.get("format_id"))
        data["uploader"] = f"{data.get('uploader') or data.get('channel') or '?'} · SoundCloud"
        data["_source"] = "soundcloud"
        logger.info("SC FALLBACK OK | '%s' → '%s' (%.1fs)", title[:50], str(data.get("title"))[:50],
                    time.monotonic() - t0)
        return data
    except Exception as exc:  # noqa: BLE001
        logger.warning("SC FALLBACK lỗi: %s", str(exc)[:120])
        return None


class Track:
    """Metadata + streaming URL for one song, resolved by yt-dlp."""

    def __init__(
        self,
        data: dict[str, Any],
        requester: discord.Member | discord.User,
        via_proxy: bool = False,
        proxy_used: str | None = None,
    ) -> None:
        self.title: str            = data.get("title", "Unknown Title")
        self.url: str              = data.get("url", "")
        self.webpage_url: str      = data.get("webpage_url", self.url)
        self.thumbnail: str | None = data.get("thumbnail")
        self.uploader: str         = data.get("uploader") or data.get("channel", "Unknown")
        self.duration: int         = int(data.get("duration") or 0)
        self.requester             = requester
        self.video_id: str         = data.get("id", "")
        # QUAN TRỌNG: 1 số client extraction (android_vr, android, tv_embedded)
        # trả về URL stream mà CDN googlevideo.com CHỈ chấp nhận nếu request
        # thật sự gửi kèm đúng User-Agent/header mà yt-dlp đã dùng lúc thương
        # lượng URL đó — ffmpeg mở URL trực tiếp KHÔNG tự biết header này, mặc
        # định dùng User-Agent generic của ffmpeg → CDN trả 403 dù URL còn hạn.
        # Lưu lại để truyền vào ffmpeg qua -headers ở make_source().
        self.http_headers: dict[str, str] = dict(data.get("http_headers") or {})
        self._url_fetched_at: float = time.monotonic()
        # QUAN TRỌNG: URL stream (googlevideo.com) bị khoá theo IP đã request nó.
        # Nếu resolve qua proxy nhưng ffmpeg lại stream trực tiếp (hoặc ngược lại)
        # → 403 giữa chừng → phải refresh liên tục → nghe lag/giật ở giây đầu.
        self._url_via_proxy: bool  = via_proxy
        # Lưu ĐÚNG chuỗi proxy đã dùng để resolve (không gọi lại _proxy.get() ở
        # make_source vì proxy manager có thể trả proxy KHÁC nếu state đã đổi
        # — dùng sai proxy = IP mismatch = 403 ngay từ request đầu).
        self._proxy_used: str | None = proxy_used

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
        # Nếu là URL YouTube → extract video ID để tránh bị chặn, và QUAN TRỌNG:
        # bỏ qua hẳn bước "search" phía dưới — ta đã có video ID rồi, không cần
        # tìm kiếm gì nữa. Trước đây dù đã có video ID, code vẫn chạy thêm 1 (thực
        # ra là 2, bị lặp) bước search bằng yt-dlp KHÔNG có PO-Token/cookie/client
        # rotation nào cả — nếu bước thừa đó dính bot-check, exception văng thẳng
        # ra ngoài, bỏ qua toàn bộ vòng lặp client rotation đáng tin cậy phía sau.
        # Đây chính là lý do /play bằng link trực tiếp có lúc báo "Sign in to
        # confirm..." ngay lập tức mà log không hề thấy dòng "Client rotation".
        yt_id_re = re.compile(r"(?:v=|youtu\.be/|/shorts/|/embed/)([A-Za-z0-9_-]{11})")
        yt_match = yt_id_re.search(query)
        yt_meta: dict[str, Any] = {}   # title/duration của kết quả search (nếu có) — cho SC fallback

        if yt_match:
            video_url = f"https://www.youtube.com/watch?v={yt_match.group(1)}"
        else:
            # Text search — cần 1 bước tìm kiếm nhẹ để ra video_id trước khi
            # resolve đầy đủ. Bọc try/except: nếu bước tìm kiếm này lỗi (hiếm,
            # nhưng có thể do bot-check), KHÔNG để nó chặn toàn bộ quá trình —
            # thử lại chính bằng "ytsearch1:<query>" trong vòng client rotation
            # phía dưới luôn, để vẫn có cơ hội qua được nhờ PO-Token/client tốt.
            resolved = query if _URL_RE.match(query) else f"ytsearch1:{query}"
            _sc = _search_cache.get(resolved)
            if _sc and (time.monotonic() - _sc[0]) < 600:
                video_url = _sc[1]   # đã tìm query này gần đây → khỏi gọi search lần nữa
                yt_meta = {"title": query}
            else:
                try:
                    search_opts = {
                        "default_search" : "ytsearch",
                        "noplaylist"     : False,
                        "quiet"          : True,
                        "no_warnings"    : True,
                        "extract_flat"   : True,
                        "socket_timeout" : 8,
                    }
                    with yt_dlp.YoutubeDL(search_opts) as ytdl:
                        partial = functools.partial(ytdl.extract_info, resolved, download=False)
                        search_data: dict[str, Any] = await loop.run_in_executor(None, partial)
                    if "entries" in search_data:
                        entries = [e for e in search_data["entries"] if e]
                        if not entries:
                            raise ValueError(f"No results found for: {query}")
                        entry = entries[0]
                        yt_meta = {"title": entry.get("title") or query, "duration": entry.get("duration")}
                        video_url = (entry.get("url") or entry.get("webpage_url")
                                     or f"https://www.youtube.com/watch?v={entry['id']}")
                    else:
                        video_url = resolved
                    if resolved.startswith("ytsearch1:"):
                        if len(_search_cache) > 128:
                            _search_cache.clear()
                        _search_cache[resolved] = (time.monotonic(), video_url)
                except Exception as _search_exc:
                    logger.warning("Bước search sơ bộ lỗi (%s) — thử lại trực tiếp qua client rotation", _search_exc)
                    video_url = resolved  # để vòng client rotation tự xử lý (kể cả dạng ytsearch1:)

        # Bước 2: resolve stream URL. Cache ngắn hạn → /play lại cùng bài gần như
        # tức thì; còn lại đi qua _resolve_stream() (breaker + thứ tự chiến lược).
        cached = _resolve_cache_get(video_url)
        if cached is not None:
            c_data, c_via_proxy, c_proxy = cached
            logger.info("RESOLVE CACHE HIT | '%s'", c_data.get("title", "?"))
            return cls(c_data, requester, via_proxy=c_via_proxy, proxy_used=c_proxy)

        # YouTube đang chết liên tiếp → thử SoundCloud trước (nhanh ~2s thay vì ~20s).
        if _SC_ENABLED and _yt_health.sc_first():
            _t = yt_meta.get("title") or _diag_meta.get(video_url, {}).get("title") or ""
            if _t:
                sc_fast = await _soundcloud_resolve(_t, yt_meta.get("duration"), loop)
                if sc_fast:
                    logger.info("SC-FIRST | YouTube fail %d bài liên tiếp → dùng SoundCloud ngay", _yt_health.fails)
                    _resolve_cache_put(video_url, sc_fast, False, None)
                    return cls(sc_fast, requester, via_proxy=False)

        data, resolved_proxy_str, last_err = await _resolve_stream(video_url, loop)
        resolved_via_proxy = bool(resolved_proxy_str)
        if data is not None:
            _yt_health.ok()
            asyncio.ensure_future(_trim_memory_soon(loop))

        if data is None:
            _yt_health.fail()
            # ── Mọi chiến lược YouTube đều fail ──
            # (1) Chẩn đoán (cũng lấy title/duration cho bước SoundCloud).
            summary, hint = await _diagnose_failure(video_url, loop)
            logger.warning("DIAG | %s | %s", video_url, summary)
            # (2) SoundCloud: đúng bài + đúng độ dài thì phát luôn, người dùng khỏi phải thử lại.
            meta = {**_diag_meta.pop(video_url, {}), **{k: v for k, v in yt_meta.items() if v}}
            sc_title = meta.get("title") or await _yt_oembed_title(video_url)
            _bad = re.search(r"(?:v=|/)([\w-]{11})(?:[&?/]|$)", video_url)
            alt_data = await _alt_upload_resolve(sc_title, meta.get("duration"), _bad.group(1) if _bad else "", loop)
            if alt_data:
                _resolve_cache_put(video_url, alt_data, False, None)
                return cls(alt_data, requester, via_proxy=False)
            sc_data = await _soundcloud_resolve(sc_title, meta.get("duration"), loop)
            if sc_data:
                _resolve_cache_put(video_url, sc_data, False, None)
                return cls(sc_data, requester, via_proxy=False)
            # (3) Piped/Invidious công khai gần như đã chết hoàn toàn (log 21/11: 404/500/502) →
            #     mặc định bỏ qua để khỏi tốn thêm ~8s; bật lại bằng PIPED_FALLBACK=1.
            vid_match = re.search(r"(?:v=|/)([\w-]{11})(?:[&?/]|$)", video_url)
            if vid_match and os.environ.get("PIPED_FALLBACK", "0") == "1":
                try:
                    piped_data = await asyncio.wait_for(_piped_fallback(vid_match.group(1), loop), timeout=8.0)
                except asyncio.TimeoutError:
                    piped_data = None
                if piped_data:
                    return cls(piped_data, requester, via_proxy=False)
            if not hint and last_err is not None and "sign in to confirm" in str(last_err).lower():
                hint = ("YouTube đang chặn IP máy chủ bot (bot-check) với video này — thử lại sau ít phút, "
                        "hoặc dùng bản upload khác của bài.")
            asyncio.ensure_future(_trim_memory_soon(loop))
            if last_err is not None:
                last_err._diag_hint = hint  # type: ignore[attr-defined]
            # Để lỗi gốc của yt-dlp hiện ra (chính xác hơn việc tự đoán nguyên nhân).
            raise last_err  # type: ignore

        # gcr=<country> trong URL KHÔNG có nghĩa là phải re-fetch qua proxy. Log 11:42: URL
        # có ip=152.55.181.204 (IP Mỹ — chính IP egress của Railway) nên gcr=us là đúng nước của
        # server; URL direct phát bình thường. Bản trước re-fetch qua proxy Mỹ thừa (tốn ~2s và
        # fail "page needs to be reloaded"), còn bước kiểm tra Range bằng httpx thì bị 403 oan
        # vì không gửi đúng header của yt-dlp. Giờ bỏ hẳn: URL direct cứ phát; nếu ffmpeg gặp
        # 403 thì nhánh STREAM DROP trong player loop tự refresh (direct trước, proxy sau).
        gcr_match = re.search(r"gcr=([a-z]{2})\b", data.get("url", ""))
        if gcr_match:
            logger.info("gcr=%s trong URL (%s) — phát luôn, 403 sẽ tự refresh",
                        gcr_match.group(1).upper(), "qua proxy" if resolved_via_proxy else "direct")

        _resolve_cache_put(video_url, data, resolved_via_proxy, resolved_proxy_str)
        return cls(data, requester, via_proxy=resolved_via_proxy,
                   proxy_used=resolved_proxy_str)

    @property
    def duration_str(self) -> str:
        h, r   = divmod(self.duration, 3600)
        m, sec = divmod(r, 60)
        return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"

    @property
    def url_is_fresh(self) -> bool:
        """URL stream còn hạn không (YouTube expire sau ~6 tiếng)."""
        return (time.monotonic() - self._url_fetched_at) < 240  # 4 phút

    async def refresh_url(self, loop: asyncio.AbstractEventLoop, force: bool = False,
                          force_proxy: bool = False, force_itag18: bool = False) -> None:
        """
        Refresh stream URL.
        force=True      → re-resolve từ video ID, không dùng cache URL cũ.
        force_proxy=True→ bỏ qua direct, chỉ dùng proxy (dùng khi gcr=us geo-lock).
        force_itag18=True → ép dùng itag 18 (progressive mp4 360p, có kèm
            video) thay vì để yt-dlp tự chọn "bestaudio". itag 18 là định
            dạng DUY NHẤT được xác nhận MIỄN PO-Token hoàn toàn (theo tracker
            yt-dlp) — bestaudio hầu như luôn trỏ về itag 251/140 (cần
            PO-Token, ngày càng bị YouTube siết chặt). Đánh đổi: tải dư phần
            video (ffmpeg tự bỏ qua bằng -vn khi phát), chất lượng audio thấp
            hơn (~96kbps AAC so với ~160kbps opus) — chấp nhận được khi mục
            tiêu là "phát được" thay vì "phát hay nhất". Dùng làm phương án
            thoát hiểm khi đã retry nhiều lần vẫn dính đúng 1 kiểu 403.
        """
        if not force and self.url_is_fresh:
            return
        last_exc: Exception | None = None
        # force_proxy=True → chỉ thử proxy (skip direct hoàn toàn)
        if force_proxy:
            proxy_order = (True,)
        elif self._url_via_proxy:
            proxy_order = (True, False)   # URL cũ gắn với IP proxy; direct từ Railway thường bot-check (log 21/11: tốn ~3s/lần)
        else:
            proxy_order = (False, True)
        for use_proxy in proxy_order:
            try:
                # QUAN TRỌNG: ép dùng client tv_embedded/tv trước (HLS, chưa cần
                # PO-Token) — KHÔNG dùng default của _ytdl_opts(), vì mặc định
                # ưu tiên web/android/ios khi có cookies.txt, 2 client này
                # thường xuyên fail "Requested format is not available". Nếu
                # bài vừa fail vì 403/PO-Token (như android_vr itag 251), thử
                # lại đúng client cũ chỉ fail y hệt lần nữa — đổi sang
                # tv_embedded trước mới có cơ hội thoát khỏi vòng lặp 403.
                _ck = _cookies_valid() and not use_proxy
                opts = _ytdl_opts(cookies=_ck, use_proxy=use_proxy)
                opts["extractor_args"] = {
                    "youtube": {"player_client": (["tv_samsung", "web_embedded"] if _ck
                                                  else ["tv_samsung", "visionos", "web_embedded", "android_vr"]),
                                "skip": ["translated_subs", "comments"]},
                    "youtubepot-bgutilhttp": {"base_url": ["http://127.0.0.1:4416"]},
                }
                if force_itag18:
                    opts["format"] = "18"
                if use_proxy:
                    opts.pop("proxy", None)
                    data = await _proxy_race(opts, self.webpage_url, loop, time.monotonic())
                else:
                    data = await asyncio.wait_for(
                        _extract_verified(opts, self.webpage_url, loop), timeout=_RESOLVE_ATTEMPT_TIMEOUT)
                new_url = data.get("url", "")
                if new_url and "youtube.com/watch" not in new_url:
                    self.url = new_url
                    self.http_headers = dict(data.get("http_headers") or {})
                    self._url_fetched_at = time.monotonic()
                    self._url_via_proxy = use_proxy
                    self._proxy_used = opts.get("proxy")
                    logger.info("URL refreshed (%s%s) for '%s'",
                                "proxy" if use_proxy else "direct",
                                ", itag18" if force_itag18 else "", self.title)
                    return
            except Exception as exc:
                last_exc = exc
                if not use_proxy:
                    continue
        # Trước đây im lặng quay lại với URL CŨ đã 403 → bot lặp 6 lần (~32s) rồi bỏ bài. Giờ báo lỗi để vòng
        # phát dừng ngay và báo người dùng.
        if last_exc:
            logger.warning("Failed to refresh URL for '%s': %s", self.title, last_exc)
        if force:
            raise last_exc or RuntimeError("refresh không ra URL mới")

    async def make_source(self, volume: float, seek: float = 0) -> discord.PCMVolumeTransformer:
        """
        Tạo audio source để phát.

        Dùng FFmpegPCMAudio + PCMVolumeTransformer thay vì FFmpegOpusAudio —
        cho phép chỉnh volume LIVE khi đang phát (không cần restart ffmpeg).
        FFmpegOpusAudio bake volume vào ffmpeg filter lúc tạo source nên nút
        Up/Down chỉ áp dụng cho bài TIẾP THEO, bài đang phát không đổi gì —
        đây là bug đã gặp thực tế, PCMVolumeTransformer sửa triệt để.

        Đánh đổi: pipe ffmpeg→discord.py giờ truyền PCM thô (~1.4Mbps) thay
        vì Opus đã nén (~128kbps), CPU tăng nhẹ do discord.py tự encode Opus
        (qua libopus/PyNaCl, không phải Python thuần) — không đáng kể cho 1
        stream duy nhất.
        """
        filters = []
        if getattr(self, '_bassboost', False):
            filters.append("bass=g=10:f=110:w=0.3")
        if getattr(self, '_nightcore', False):
            filters.append("asetrate=44100*1.25,aresample=44100")
        filter_str = ",".join(filters) if filters else "anull"
        opts = f'-vn -filter:a "{filter_str}"'

        safe_vol = max(0.01, min(2.0, volume))

        # Nếu phải qua proxy VÀ có video_id: thử Piped NHANH (cap cứng 5s)
        # trước khi stream qua proxy — server Piped tự fetch từ YouTube phía
        # họ (không qua proxy của mình) nên thường ổn định hơn với các video
        # cần proxy (geo-lock). Nếu không có kết quả trong 5s, bỏ qua ngay.
        #
        # MẶC ĐỊNH TẮT (PIPED_FIRST=1 để bật lại): log thực tế không có lần nào
        # "Dùng Piped stream" — nghĩa là mỗi bài qua proxy đều NGỒI CHỜ trọn 5s
        # timeout rồi mới khởi động ffmpeg, cộng thẳng vào độ trễ đầu bài. Piped
        # vẫn còn là phương án cuối trong from_query() khi yt-dlp fail hoàn toàn.
        if PIPED_FIRST and self._url_via_proxy and seek == 0 and self.video_id:
            try:
                piped_data = await asyncio.wait_for(
                    _piped_fallback(self.video_id, asyncio.get_running_loop()), timeout=5.0,
                )
            except asyncio.TimeoutError:
                piped_data = None
            if piped_data and piped_data.get("url"):
                logger.info("Dùng Piped stream cho '%s' — tránh proxy jitter", self.title)
                pcm = _new_pcm(
                    piped_data["url"],
                    _ffmpeg_before(no_proxy=True, hls=_is_hls(piped_data["url"])),
                    opts,
                )
                return await self._wrap_source(pcm, safe_vol, via_proxy=False)

        pcm = _new_pcm(
            self.url,
            _ffmpeg_before(seek=seek, no_proxy=not self._url_via_proxy,
                           proxy_override=self._proxy_used,
                           headers=self.http_headers, hls=_is_hls(self.url)),
            opts,
        )
        return await self._wrap_source(pcm, safe_vol, via_proxy=self._url_via_proxy)

    # ── Prewarm: chạy ffmpeg + đệm trong lúc bot đang nối voice (~2-3s) ─────────────
    def start_prewarm(self, volume: float) -> None:
        if getattr(self, "_pre_task", None) is not None:
            return
        self._pre_task = asyncio.ensure_future(self.make_source(volume))
        asyncio.get_running_loop().call_later(60.0, self.discard_prewarm)   # không ai lấy → dọn, khỏi rò ffmpeg

    def discard_prewarm(self) -> None:
        t = getattr(self, "_pre_task", None)
        self._pre_task = None
        if t is None:
            return
        if t.done():
            if not t.cancelled() and t.exception() is None:
                try:
                    t.result().cleanup()
                except Exception:  # noqa: BLE001
                    pass
        else:
            t.cancel()

    async def take_source(self, volume: float) -> "discord.PCMVolumeTransformer":
        t = getattr(self, "_pre_task", None)
        self._pre_task = None
        if t is None:
            return await self.make_source(volume)
        src = await t                      # lỗi của prewarm (vd SC im lặng) vẫn nổi lên như make_source
        src.volume = volume                # volume có thể đã đổi trong lúc chờ
        return src

    async def _wrap_source(self, pcm: "discord.FFmpegPCMAudio", volume: float,
                           via_proxy: bool) -> "_CountingSource":
        """
        Bọc PCM trong bộ đệm rồi CHỜ có sẵn ~2s (3s nếu qua proxy) trước khi trả
        về cho player — để đầu bài không bị giật khi ffmpeg khởi động chậm.
        Volume vẫn live vì _CountingSource (PCMVolumeTransformer) nằm trên bộ đệm.
        """
        buf = _BufferedPCM(pcm, target_s=3.0 if via_proxy else 2.0)
        src = _CountingSource(buf, volume=volume)
        t0 = time.monotonic()
        try:
            ready = await buf.wait_ready(_PREBUFFER_TIMEOUT)
        except BaseException:      # bị cancel (/stop) trong lúc đợi → dọn ffmpeg, không để rò process
            src.cleanup()
            raise
        logger.info("PREBUFFER | '%s' %s sau %.1fs (đệm=%.1fs, proxy=%s)", self.title,
                    "sẵn sàng" if ready else "HẾT GIỜ CHỜ — phát luôn", time.monotonic() - t0,
                    buf.buffered_s, "yes" if via_proxy else "no")
        # ffmpeg không ra nổi 1 frame PCM sau cả timeout (vd SoundCloud HLS-opus: đọc xong playlist
        # rồi đứng) → phát chỉ là im lặng suốt cả bài. Báo lỗi rõ ràng để player bỏ qua/thử bài khác.
        if not ready and buf.buffered_s == 0.0 and "sndcdn.com" in (self.url or ""):
            _log_ffmpeg_stderr(src)
            src.cleanup()
            raise RuntimeError("Nguồn SoundCloud không giải mã được (ffmpeg không ra âm thanh)")
        return src


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
    flags = []
    if loop:    flags.append("🔁 Loop ON")
    if shuffle: flags.append("🔀 Shuffle ON")
    if flags:
        e.add_field(name="Mode", value="  ".join(flags), inline=False)
    if track.thumbnail:
        e.set_thumbnail(url=track.thumbnail)
    return e


def _e_queued(track: Track, pos: int) -> discord.Embed:
    e = discord.Embed(
        title       = f"🎵  Song Added to Queue #{pos}",
        description = f"[{track.title}]({track.webpage_url}) `[ {track.duration_str} ]`",
        colour      = COLOUR_QUEUE,
    )
    return e


# ── DJ role permission ──────────────────────────────────────────────────────
#
# Mặc định (KHÔNG cần cấu hình gì): người đã /play bài ĐANG PHÁT là người duy
# nhất (ngoài Admin/chủ server) được điều khiển panel/slash-command của bài
# đó — pause, resume, skip, stop, volume, seek, loop, shuffle, autoplay,
# back, bassboost, nightcore, 24/7. Người khác vẫn THẤY panel bình thường,
# chỉ là bấm nút không có tác dụng (báo "không đủ quyền").
#
# /djrole cho phép admin đặt THÊM 1 role được điều khiển MỌI bài (không chỉ
# bài họ tự request) — dùng khi muốn có vài người "quản lý nhạc" cố định
# thay vì giới hạn tuyệt đối theo người request.

_DJ_ROLES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "dj_roles.json")


def _load_dj_roles() -> dict[int, int]:
    try:
        with open(_DJ_ROLES_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)
        return {int(k): int(v) for k, v in raw.items()}
    except (FileNotFoundError, json.JSONDecodeError, ValueError, TypeError):
        return {}


def _save_dj_roles() -> None:
    try:
        os.makedirs(os.path.dirname(_DJ_ROLES_FILE), exist_ok=True)
        with open(_DJ_ROLES_FILE, "w", encoding="utf-8") as f:
            json.dump({str(k): v for k, v in _DJ_ROLES.items()}, f)
    except OSError as exc:
        logger.warning("Không lưu được dj_roles.json: %s", exc)


_DJ_ROLES: dict[int, int] = _load_dj_roles()


def _can_control(member: discord.Member, player: "GuildPlayer | None") -> bool:
    """
    True nếu member được phép điều khiển nhạc ngay bây giờ. Thứ tự ưu tiên:
      1. Chủ server / Admin / Manage Server — luôn được (tránh tự khoá tay).
      2. Có DJ role (nếu admin đã /djrole set) — điều khiển được MỌI bài.
      3. Là người đã /play ra bài ĐANG PHÁT hiện tại — chỉ điều khiển được
         bài của chính mình, người khác thì không.
    Nếu cả 3 đều không thoả → False (vẫn xem panel được, chỉ không bấm được).
    """
    guild = member.guild
    if member.id == guild.owner_id:
        return True
    perms = member.guild_permissions
    if perms.administrator or perms.manage_guild:
        return True

    dj_role_id = _DJ_ROLES.get(guild.id)
    if dj_role_id is not None and any(r.id == dj_role_id for r in member.roles):
        return True

    if player is not None and player.current is not None:
        requester = getattr(player.current, "requester", None)
        if requester is not None and requester.id == member.id:
            return True

    return False


def _e_dj_denied(guild: discord.Guild, player: "GuildPlayer | None" = None) -> discord.Embed:
    dj_role_id  = _DJ_ROLES.get(guild.id)
    requester   = getattr(player.current, "requester", None) if (player and player.current) else None
    bits: list[str] = []
    if requester:
        bits.append(f"người đã `/play` bài này ({requester.mention})")
    if dj_role_id:
        bits.append(f"<@&{dj_role_id}>")
    bits.append("Admin/chủ server")
    return discord.Embed(
        title       = "🚫 Không đủ quyền",
        description = f"Chỉ {', '.join(bits)} mới được điều khiển bài này.",
        colour      = COLOUR_STOP,
    )


async def _require_dj(interaction: discord.Interaction, player: "GuildPlayer | None" = None) -> bool:
    """Dùng đầu mỗi slash command bị giới hạn. True = được phép; False = đã tự
    gửi phản hồi từ chối, caller chỉ cần `return`."""
    member = interaction.user
    if not isinstance(member, discord.Member) or interaction.guild is None:
        return True
    if _can_control(member, player):
        return True
    embed = _e_dj_denied(interaction.guild, player)
    try:
        if not interaction.response.is_done():
            await interaction.response.send_message(embed=embed, ephemeral=True)
        else:
            await interaction.followup.send(embed=embed, ephemeral=True)
    except discord.HTTPException:
        pass
    return False


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
        self._cooldowns: dict[str, float] = {}  # custom_id → last used
        self._cd_secs = 3.0
        self._sync()

    async def _check_cd(self, interaction: discord.Interaction, cid: str) -> bool:
        """Return True nếu được phép, False nếu còn cooldown (auto-reply ephemeral)."""
        now  = time.monotonic()
        left = self._cd_secs - (now - self._cooldowns.get(cid, 0.0))
        if left > 0:
            try:
                await interaction.response.send_message(
                    f"⏳ Chờ **{left:.1f}s** trước khi dùng lại!", ephemeral=True
                )
            except discord.HTTPException:
                pass
            return False
        self._cooldowns[cid] = now
        return True

    async def _check_dj(self, interaction: discord.Interaction) -> bool:
        """Return True nếu member được phép điều khiển nhạc (auto-reply ephemeral nếu không)."""
        member = interaction.user
        if not isinstance(member, discord.Member) or interaction.guild is None:
            return True
        if _can_control(member, self.player):
            return True
        try:
            await interaction.response.send_message(
                embed=_e_dj_denied(interaction.guild, self.player), ephemeral=True
            )
        except discord.HTTPException:
            pass
        return False

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
        if not await self._check_dj(interaction): return
        if not await self._check_cd(interaction, "music_vdn"): return
        new_pct = max(0, round(self.player.volume * 100) - 10)
        self.player.set_volume(new_pct)
        logger.info("BUTTON vol − → %d%% [guild %d]", new_pct, self.player.vc.guild.id)
        await self._quick_edit(interaction)

    @discord.ui.button(emoji="⏮", label="Back", style=discord.ButtonStyle.secondary, custom_id="music_back", row=0)
    async def btn_back(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not await self._check_dj(interaction): return
        if not await self._check_cd(interaction, "music_back"): return
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
        if not await self._check_dj(interaction): return
        if not await self._check_cd(interaction, "music_pp"): return
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
        if not await self._check_dj(interaction): return
        if not await self._check_cd(interaction, "music_skip"): return
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
        if not await self._check_dj(interaction): return
        if not await self._check_cd(interaction, "music_vup"): return
        new_pct = min(100, round(self.player.volume * 100) + 10)
        self.player.set_volume(new_pct)
        logger.info("BUTTON vol + → %d%% [guild %d]", new_pct, self.player.vc.guild.id)
        await self._quick_edit(interaction)

    # ── Row 2 ──────────────────────────────────────────────────────────────────

    @discord.ui.button(emoji="🔀", label="Shuffle", style=discord.ButtonStyle.secondary, custom_id="music_shuffle", row=2)
    async def btn_shuffle(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not await self._check_dj(interaction): return
        if not await self._check_cd(interaction, "music_shuffle"): return
        p = self.player
        p.shuffle = not p.shuffle
        logger.info("BUTTON shuffle → %s [guild %d]", p.shuffle, p.vc.guild.id)
        await self._quick_edit(interaction)

    @discord.ui.button(emoji="🔁", label="Loop", style=discord.ButtonStyle.secondary, custom_id="music_loop", row=2)
    async def btn_loop(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not await self._check_dj(interaction): return
        if not await self._check_cd(interaction, "music_loop"): return
        p = self.player
        p.loop = not p.loop
        logger.info("BUTTON loop → %s [guild %d]", p.loop, p.vc.guild.id)
        await self._quick_edit(interaction)

    @discord.ui.button(emoji="⏹", label="Stop", style=discord.ButtonStyle.danger, custom_id="music_stop", row=2)
    async def btn_stop(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not await self._check_dj(interaction): return
        if not await self._check_cd(interaction, "music_stop"): return
        p = self.player
        gid = p.vc.guild.id

        # Ngay lập tức: xóa buttons, hiện embed Stopped với thumbnail
        self._disable_all()
        track = p.current
        stopped_embed = discord.Embed(
            description = f"⏹ Stopping *{track.title}*…" if track else "⏹ Stopping…",
            colour      = COLOUR_STOP,
        )
        await interaction.response.edit_message(embed=stopped_embed, view=None)

        # Đánh dấu NP message đã được xử lý (tránh _expire_np_message edit lại)
        p._np_msg  = None
        p._np_view = None

        await p.stop()

        # Edit lại thành style Lara: ✅ @Rimuru Stopped *tên bài* (italic, không có @@ )
        try:
            final_embed = discord.Embed(
                description = f"✅ {interaction.user.mention} Stopped"
                              + (f" *{track.title}*" if track else ""),
                colour      = COLOUR_SUCCESS,
            )
            await interaction.edit_original_response(embed=final_embed, view=None)
        except discord.HTTPException:
            pass
        logger.info("BUTTON stop [guild %d]", gid)

    # ── Row 3 ──────────────────────────────────────────────────────────────────

    @discord.ui.button(emoji="▶️", label="AutoPlay", style=discord.ButtonStyle.secondary, custom_id="music_autoplay", row=3)
    async def btn_autoplay(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not await self._check_dj(interaction): return
        if not await self._check_cd(interaction, "music_autoplay"): return
        p = self.player
        p.autoplay = not p.autoplay
        logger.info("BUTTON autoplay → %s [guild %d]", p.autoplay, p.vc.guild.id)
        await self._quick_edit(interaction)

    @discord.ui.button(emoji="📋", label="Playlist", style=discord.ButtonStyle.secondary, custom_id="music_playlist", row=3)
    async def btn_playlist(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not await self._check_cd(interaction, "music_playlist"): return
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
        self._current_source: discord.PCMVolumeTransformer | None = None  # cho volume live
        # Đếm số lệnh /play đang resolve (yt-dlp) nhưng CHƯA kịp vào queue.
        # Idle-timeout phải chờ những resolve này xong, không được đếm giờ
        # trong lúc đó — nếu không sẽ auto-disconnect giữa chừng khi bài đang
        # phát bị lỗi (queue tạm rỗng) và bài tiếp theo vẫn đang tải.
        self.pending_resolves: int = 0
        self._start: float | None  = None
        self._attempt_start: float | None = None  # thời điểm play/resume gần nhất — dùng cho heuristic phát hiện fail-sớm-câm-lặng
        self.loop: bool            = False
        self.shuffle: bool         = False
        self.autoplay: bool        = False
        self._history: list[Track] = []
        self._preloaded: Track | None = None      # pre-load bài tiếp
        self._old_np_msgs: list[discord.Message] = []  # NP cũ để xóa
        self._fetch_msg: discord.Message | None = None  # "🔎 Fetched" tạm — xoá khi panel thật lên
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
        Set volume — áp dụng LIVE cho bài đang phát (PCMVolumeTransformer),
        không cần đợi bài tiếp theo mới có tác dụng như trước.
        """
        self.volume = max(0.01, min(2.0, pct / 100))
        if self._current_source is not None:
            self._current_source.volume = self.volume

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

                # ── Idle wait — CHỈ khi queue trống, không phát nhạc, VÀ không
                # có lệnh /play nào đang resolve (tránh auto-disconnect giữa
                # chừng khi bài đang tải xong bài trước lỗi) ────────────────
                if (not self._queue and not self.vc.is_playing() and not self.vc.is_paused()
                        and self.pending_resolves == 0):
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
                            ),
                            delete_after=8,
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
                self._attempt_start = time.monotonic()
                logger.info(
                    "PLAY | '%s' (%.0fs) vol=%.0f%% queue=%d [guild %d]",
                    track.title, track.duration, self.volume * 100,
                    len(self._queue), self.vc.guild.id,
                )

                # ── Create audio source ────────────────────────────────────────
                try:
                    source = await track.take_source(self.volume)
                    self._current_source = source  # cho set_volume() chỉnh live
                    # make_source() có thể mất vài giây (chờ prebuffer) → tính giờ bài
                    # từ lúc THỰC SỰ bắt đầu phát, nếu không progress bar chạy lố.
                    self._start = time.monotonic()
                    self._attempt_start = time.monotonic()
                except Exception as exc:
                    logger.error(
                        "SOURCE | failed for '%s': %s [guild %d]",
                        track.title, exc, self.vc.guild.id,
                    )
                    await self.text_ch.send(
                        embed=_e_err(
                            "Playback Error",
                            f"Could not load **{track.title}**.\n`{exc}`\nSkipping…",
                        ),
                        delete_after=15,
                    )
                    self.current = None
                    self._start  = None
                    self._attempt_start = None
                    self._next.set()
                    continue

                # ── Closure-safe _after callback ───────────────────────────────
                # Capture loop-local references at definition time so the
                # closure remains valid even if self._next is replaced.
                _loop = self._loop
                _ev   = self._next
                _403_flag = [False, False, False]  # [is_403, is_geo_blocked, is_eof]
                _403_retries = [0]
                _cur_src = [source]  # nguồn đang phát (cập nhật khi resume) — để log stderr ffmpeg đúng nguồn

                def _after(err: Exception | None, _l: asyncio.AbstractEventLoop = _loop, _e: asyncio.Event = _ev) -> None:
                    _tail = _log_ffmpeg_stderr(_cur_src[0])
                    if _tail and ("403" in _tail or "Forbidden" in _tail):
                        _403_flag[0] = True
                    if err:
                        err_s = str(err)
                        logger.error("VC after-error: %s", err_s)
                        if "403" in err_s or "Forbidden" in err_s:
                            _403_flag[0] = True
                            if "gcr=" in track.url:
                                _403_flag[1] = True
                        elif any(x in err_s for x in ("End of file", "Input/output", "I/O error", "Connection reset")):
                            _403_flag[2] = True
                    _l.call_soon_threadsafe(_e.set)

                # QUAN TRỌNG: clear _next TRƯỚC vc.play() để tránh race condition
                # (_after có thể fire ngay lập tức nếu URL lỗi)
                self._next.clear()

                # Voice có thể đã bị ngắt (idle-disconnect, force-kick...) trong
                # lúc bài này đang resolve — kiểm tra trước khi play() để tránh
                # crash "Not connected to voice" làm chết cả player loop.
                if not self.vc.is_connected():
                    logger.warning(
                        "Voice không còn kết nối khi chuẩn bị phát '%s' [guild %d], bỏ qua bài này.",
                        track.title, self.vc.guild.id,
                    )
                    try:
                        await self.text_ch.send(
                            embed=_e_err(
                                "⚠️ Mất kết nối voice",
                                f"Bot bị ngắt khỏi voice trước khi phát được **{track.title}**.\n"
                                "Dùng `/play` lại để bot vào lại voice nhé!",
                            ),
                            delete_after=15,
                        )
                    except discord.HTTPException:
                        pass
                    self.current = None
                    self._start  = None
                    self._attempt_start = None
                    return  # thoát player loop — sẽ được tạo lại ở lần /play tiếp theo

                if not await _wait_dave_ready(self.vc, 6.0):
                    logger.warning("DAVE chưa sẵn sàng sau 6s — vẫn phát nhưng có thể không ai nghe được. %s",
                                   _dave_state(self.vc))
                self.vc.play(source, after=_after)
                asyncio.ensure_future(self._audio_watchdog(source, track.title))

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

                # Panel thật đã lên → xoá tin nhắn "🔎 Fetched" tạm (nếu có)
                if self._fetch_msg is not None:
                    try:
                        await self._fetch_msg.delete()
                    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                        pass
                    self._fetch_msg = None

                # ── Pre-load bài tiếp theo trong background ────────────────────
                if self._queue and not self._preloaded:
                    asyncio.ensure_future(self._preload_next(self._queue[0]))

                # ── Wait for track to finish — auto-refresh nếu stream đứt ────
                # Giới hạn retry SCALE THEO ĐỘ DÀI bài — cap cố định (3-5 lần)
                # quá ít cho video dài (1h+): URL YouTube tự hết hạn sau vài giờ
                # (param expire=), và xác suất chập chờn mạng cũng cao hơn nhiều
                # lần trong 1 phiên phát dài. +1 lượt retry mỗi 20 phút, trần 40.
                _duration_bonus  = track.duration // 1200  # +1 mỗi 20 phút
                _MAX_403_RETRY   = min(40, 3 + _duration_bonus)
                _MAX_EOF_RETRY   = min(40, 5 + _duration_bonus)
                while True:
                    await self._next.wait()
                    self._next.clear()

                    elapsed = int(time.monotonic() - self._start) if self._start else 0

                    if self.current is None or self.current != track:
                        break
                    if not self.vc.is_playing() and not self.vc.is_paused():
                        is_403 = _403_flag[0]
                        is_eof = _403_flag[2]
                        # QUAN TRỌNG: discord.py chỉ gọi after(err) với err KHÔNG
                        # None nếu chính discord.py raise exception khi ĐỌC dữ liệu
                        # từ ffmpeg. Khi ffmpeg fail NGAY LÚC MỞ URL (403, DNS lỗi,
                        # v.v.) và thoát với các return code khác — ffmpeg đơn giản
                        # không sinh ra byte nào, discord.py đọc được b'' (EOF) và
                        # coi đó là "hết bài bình thường", gọi after(None). Nghĩa
                        # là _403_flag KHÔNG BAO GIỜ được set cho đúng trường hợp
                        # gây lỗi thật (như log 403 Forbidden ngay khi PLAY), khiến
                        # bot "chết lặng" — không retry, không báo lỗi, chỉ ngồi
                        # yên tới lúc auto-disconnect. Bù lại bằng heuristic: nếu
                        # LẦN THỬ HIỆN TẠI (không phải cả bài) chết sớm bất thường
                        # → gần như chắc chắn là lỗi mở stream câm lặng.
                        #
                        # BUG ĐÃ SỬA (18/08/2026): heuristic cũ dùng `elapsed`
                        # (vị trí TUYỆT ĐỐI trong bài) — sau khi resume ở giây 20s
                        # chẳng hạn, nếu ffmpeg chết ngay lập tức LẦN NỮA thì
                        # `elapsed` vẫn ~20s (không gần 0) → heuristic không nhận
                        # ra đây cũng là fail-sớm-câm-lặng, bỏ cuộc luôn không
                        # retry tiếp dù còn nguyên hạn mức retry — khiến bài
                        # "chết lặng" ngay sau lần resume đầu tiên. Giờ dùng
                        # `attempt_elapsed` (thời gian kể từ LẦN PLAY/RESUME GẦN
                        # NHẤT, luôn reset về gần 0 mỗi lần retry) thay vì
                        # `elapsed` (vị trí tuyệt đối, chỉ tăng dần suốt cả bài).
                        attempt_elapsed = (
                            time.monotonic() - self._attempt_start
                            if self._attempt_start else elapsed
                        )
                        ended_too_early = (
                            track.duration > 10
                            and not is_403 and not is_eof
                            and attempt_elapsed < 5
                        )
                        should_retry = is_403 or is_eof or ended_too_early
                        if track.duration > 0 and elapsed < track.duration - 5 and should_retry:
                            retries   = _403_retries[0]
                            max_retry = _MAX_EOF_RETRY if is_eof else _MAX_403_RETRY
                            if retries >= max_retry:
                                logger.warning("Stream retry limit (%d) for '%s', skipping.", retries, track.title)
                                break
                            reason = "EOF/URL-expire" if is_eof else ("403" if is_403 else "im lặng (ffmpeg fail sớm, không báo lỗi)")
                            logger.warning(
                                "STREAM DROP %s (attempt %d/%d) at %ds/%ds for '%s', refreshing…",
                                reason, retries + 1, max_retry, elapsed, track.duration, track.title,
                            )
                            try:
                                is_geo = "gcr=" in track.url
                                is_geo_us = "gcr=us" in track.url
                                if is_geo_us:
                                    _proxy.force_us()
                                # Sau 2 lần retry vẫn dính 403/EOF y hệt → gần như chắc
                                # chắn cứ resolve lại cũng ra ĐÚNG itag 251/140 qua
                                # android_vr (cần PO-Token) mỗi lần, lặp vô ích. Từ lần
                                # thứ 3 trở đi, ép hẳn itag 18 (progressive mp4, MIỄN
                                # PO-Token hoàn toàn) — chất lượng thấp hơn nhưng thoát
                                # được vòng lặp 403 chắc chắn hơn nhiều.
                                force_18 = (not is_eof) and retries >= 2
                                await track.refresh_url(
                                    self._loop, force=True, force_proxy=False,  # direct trước, proxy sau (server ở Mỹ nên gcr=us hợp lệ)
                                    force_itag18=force_18,
                                )
                                _403_flag[0] = False
                                _403_flag[2] = False
                                _403_retries[0] += 1
                                # Dùng elapsed CHÍNH XÁC (có phần thập phân, không làm
                                # tròn xuống cả giây như biến `elapsed` dùng cho log/
                                # retry-limit ở trên) và chỉ lùi lại 0.3s — đủ để tránh
                                # rơi giữa 1 audio frame, nhưng KHÔNG đủ dài để tai
                                # người nhận ra là "phát lặp lại 1 đoạn" (trước dùng
                                # nguyên 1 giây → nghe rõ như bị nhại lại 1 đoạn nhạc
                                # vừa nghe, phản tác dụng so với mục đích ban đầu).
                                precise_elapsed = (time.monotonic() - self._start) if self._start else 0.0
                                seek_pos = max(0.0, precise_elapsed - 0.3)
                                new_src = await track.make_source(self.volume, seek=seek_pos)
                                self._current_source = new_src
                                _cur_src[0] = new_src
                                self._next.clear()
                                self.vc.play(new_src, after=_after)
                                # QUAN TRỌNG: reset lại self._start theo đúng vị trí
                                # đang resume — quá trình refresh URL tốn vài giây
                                # "chết" (không có tiếng) nhưng đồng hồ monotonic vẫn
                                # chạy suốt; nếu không reset, elapsed sẽ tính lố dần
                                # theo mỗi lần refresh, gây sai lệch cho lần retry sau
                                # (vd kiểm tra "gần hết bài chưa" bị sai) và cả progress
                                # bar hiển thị lệch so với audio thực tế đang nghe.
                                self._start = time.monotonic() - seek_pos
                                self._attempt_start = time.monotonic()
                                logger.info("STREAM RESUMED (%s attempt %d) at %.1fs for '%s'",
                                            reason, retries + 1, seek_pos, track.title)
                                continue
                            except Exception as exc:
                                logger.error("STREAM REFRESH FAILED: %s", exc)
                                _403_retries[0] += 1
                        break
                    break

                # Notify nếu hết retry 403
                elapsed = int(time.monotonic() - self._start) if self._start else 0
                if _403_retries[0] >= _MAX_403_RETRY or _403_flag[0]:
                    try:
                        await self.text_ch.send(
                            embed=_e_err(
                                "⚠️ Không thể phát",
                                f"**{track.title}** bị YouTube chặn (403) sau {_403_retries[0]} lần thử.\nThử bài khác nhé!",
                            ),
                            delete_after=15,
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
                self._attempt_start = None
                self.current = None

                # ── Thông báo hết nhạc nếu queue trống — xoá hẳn MUSIC PANEL cũ,
                #    chỉ để lại 1 thông báo gọn giống Lara (không để cả 2 cùng
                #    hiện: panel cũ với nút đã disable + thông báo hết nhạc mới).
                if not self._queue and not self.loop:
                    if self._old_np_msgs:
                        last_msg = self._old_np_msgs[-1]
                        try:
                            await last_msg.delete()
                            self._old_np_msgs.remove(last_msg)
                        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                            pass
                    try:
                        e = discord.Embed(
                            description = f"✅ Hết nhạc trong hàng đợi — bài cuối *{track.title}*",
                            colour      = COLOUR_QUEUE,
                        )
                        e.set_footer(text=f"⏱ Bot tự rời sau {self.IDLE_TIMEOUT}s nếu không có bài mới")
                        if track.thumbnail:
                            e.set_thumbnail(url=track.thumbnail)
                        await self.text_ch.send(embed=e, delete_after=8)
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

    async def _audio_watchdog(self, source: Any, title: str) -> None:
        """Log chẩn đoán lúc 3s / 8s / 20s sau khi phát: byte ffmpeg, biên độ, quyền Speak, trạng thái DAVE."""
        for delay in (3, 5, 12):
            await asyncio.sleep(delay)
            try:
                n = getattr(source, "bytes_read", -1)
                me = self.vc.guild.me
                perms = self.vc.channel.permissions_for(me)
                vs = me.voice
                logger.warning(
                    "AUDIO DIAG '%s': ffmpeg_bytes=%d peak=%s underruns=%s buf=%.1fs | playing=%s | can_speak=%s mute=%s suppress=%s | %s",
                    title, n, getattr(source, "peak", "?"), getattr(source, "underruns", "?"),
                    getattr(source, "buffered_s", 0.0), self.vc.is_playing(),
                    perms.speak, getattr(vs, "mute", None), getattr(vs, "suppress", None),
                    _dave_state(self.vc),
                )
            except Exception as exc:
                logger.warning("AUDIO DIAG lỗi: %s", exc)
                return

    async def _expire_np_message(self, stopped: bool = False) -> None:
        """
        Khi bài kết thúc hoặc stop:
        - stopped=False (hết bài tự nhiên): disable tất cả buttons
        - stopped=True  (user bấm stop):    xóa hết buttons, chỉ giữ thumbnail + "Stopped"
        """
        if self._np_msg and self._np_view:
            try:
                self._np_view._disable_all()
                if stopped and self._np_msg:
                    # Lấy track info trước khi clear
                    track = self.current
                    stopped_embed = discord.Embed(
                        title       = "⏹  Stopped",
                        description = f"**@{getattr(self._np_msg.guild, 'me', None) and ''}** Stopped"
                                      + (f" *{track.title}*" if track else ""),
                        colour      = COLOUR_STOP,
                    )
                    # Gửi view=None để xóa hoàn toàn tất cả buttons
                    await self._np_msg.edit(embed=stopped_embed, view=None)
                else:
                    await self._np_msg.edit(view=self._np_view)
                self._old_np_msgs.append(self._np_msg)
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

    async def cog_app_command_error(
        self, interaction: discord.Interaction, error: app_commands.AppCommandError
    ) -> None:
        """Bắt lỗi cooldown / lỗi chung ở mức Cog để tránh traceback vô ích."""
        if isinstance(error, app_commands.CommandOnCooldown):
            msg = f"⏳ Chờ **{error.retry_after:.1f}s** rồi dùng lại lệnh này nhé!"
            try:
                if interaction.response.is_done():
                    await interaction.followup.send(embed=_e_err("Từ từ đã", msg), ephemeral=True)
                else:
                    await interaction.response.send_message(embed=_e_err("Từ từ đã", msg), ephemeral=True)
            except discord.HTTPException:
                pass
            return
        logger.exception("Unhandled app command error", exc_info=error)

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
    @app_commands.checks.cooldown(1, 3.0, key=lambda i: (i.guild_id, i.user.id))
    async def play(self, interaction: discord.Interaction, query: str) -> None:
        # ── 1. Defer NGAY LẬP TỨC (phải trong 3s) ────────────────────────────
        if not interaction.response.is_done():
            try:
                await interaction.response.defer()
            except (discord.errors.NotFound, discord.errors.HTTPException):
                return

        # ── 2. Voice checks (sau defer, dùng followup) ────────────────────────
        err = self._voice_precheck(interaction)
        if err:
            await interaction.followup.send(
                embed=_e_err("Cannot Play", err), ephemeral=True
            )
            return

        user  = interaction.user
        guild = interaction.guild
        assert isinstance(user, discord.Member) and guild is not None

        # ── 3. Resolve và phát/thêm queue luôn — không hiện dropdown chọn nữa ──
        await self._resolve_and_enqueue(interaction, query, user)

    async def _ensure_voice(self, interaction: discord.Interaction) -> discord.VoiceClient | None:
        """
        Kết nối voice nếu chưa có, gửi lỗi qua followup nếu fail. Trả về None nếu fail.
        Tự retry 1 lần với cleanup nếu bị "Timed out connecting to voice" — lỗi này
        thường do voice_client cũ (stale) còn kẹt lại từ session trước.
        """
        user  = interaction.user
        guild = interaction.guild
        assert isinstance(user, discord.Member) and guild is not None

        existing_vc = guild.voice_client
        if isinstance(existing_vc, discord.VoiceClient) and existing_vc.is_connected():
            return existing_vc

        # Dọn voice_client cũ bị kẹt (is_connected() == False nhưng vẫn còn attach)
        if isinstance(existing_vc, discord.VoiceClient):
            try:
                await existing_vc.disconnect(force=True)
            except Exception:
                pass
            await asyncio.sleep(0.5)

        channel = user.voice.channel  # type: ignore[union-attr]
        _MAX_VOICE_RETRY = 3
        for attempt in range(_MAX_VOICE_RETRY):
            try:
                # Tăng dần timeout mỗi lần thử — UDP handshake có thể cần thêm thời gian
                # nếu host đang bị nghẽn mạng tạm thời.
                timeout_s = 15.0 + attempt * 5.0
                voice_client = await channel.connect(self_deaf=True, timeout=timeout_s, reconnect=False)
                logger.info(
                    "CONNECT | joined #%s [guild %d]%s",
                    channel.name, guild.id, f" (attempt {attempt + 1})" if attempt else "",
                )
                _perms = channel.permissions_for(guild.me)
                if not _perms.speak:
                    logger.error("THIẾU QUYỀN SPEAK trong #%s — bot sẽ vào được nhưng KHÔNG có tiếng!", channel.name)
                    try:
                        await interaction.followup.send(
                            embed=_e_err(
                                "Bot thiếu quyền Speak",
                                f"Bot không có quyền **Speak (Nói)** trong **#{channel.name}** nên sẽ không có tiếng.\n"
                                "Vào *Edit Channel → Permissions* (hoặc Server Settings → Roles) "
                                "và bật **Connect + Speak** cho role của bot.",
                            ),
                            ephemeral=True,
                        )
                    except discord.HTTPException:
                        pass
                return voice_client
            except asyncio.TimeoutError:
                logger.warning(
                    "Voice connect timeout (attempt %d/%d, %.0fs) — retrying…",
                    attempt + 1, _MAX_VOICE_RETRY, timeout_s,
                )
                # Dọn sạch trạng thái treo trước khi thử lại
                stale = guild.voice_client
                if isinstance(stale, discord.VoiceClient):
                    try:
                        await stale.disconnect(force=True)
                    except Exception:
                        pass
                await asyncio.sleep(2.0)
                continue
            except discord.ClientException as exc:
                await interaction.followup.send(
                    embed=_e_err("Connection Failed", f"`{exc}`"), ephemeral=True,
                )
                return None
            except Exception as exc:
                logger.error("Voice connect failed: %s", exc)
                await interaction.followup.send(
                    embed=_e_err(
                        "Connection Failed",
                        f"Could not join your voice channel.\n`{exc}`",
                    ),
                    ephemeral=True,
                )
                return None

        # Hết các lần thử đều timeout
        await interaction.followup.send(
            embed=_e_err(
                "Connection Timeout",
                "Không thể kết nối voice sau 3 lần thử (15s/20s/25s).\n"
                "Nếu lỗi này lặp lại thường xuyên, có thể do host đang chặn/nghẽn UDP outbound "
                "(voice Discord cần UDP) — nên kiểm tra Network settings của host hoặc thử lại sau.",
            ),
            ephemeral=True,
        )
        return None

    async def _resolve_and_enqueue(
        self, interaction: discord.Interaction, query: str, user: discord.Member
    ) -> None:
        """
        Resolve 1 track (URL hoặc text search) rồi enqueue + reply.
        Dùng cho /play — phát/thêm queue thẳng, không qua bước chọn nào khác.
        Yêu cầu: interaction.response đã done (defer hoặc edit_message trước đó).
        """
        guild = interaction.guild
        assert guild is not None

        # Nếu player đã tồn tại (đang phát bài khác), báo cho nó biết có 1 lệnh
        # /play đang resolve — tránh idle-timeout đá bot ra giữa lúc bài đang
        # tải nếu bài trước đó vừa lỗi (queue tạm rỗng).
        _existing_player = self._get_player(interaction.guild_id)
        if _existing_player:
            _existing_player.pending_resolves += 1

        try:
            track = await Track.from_query(query, user, self.bot.loop)
        except yt_dlp.utils.DownloadError as exc:
            logger.warning("yt-dlp DownloadError for '%s': %s", query, exc)
            exc_str = str(exc)
            if "cookies" in exc_str.lower() or "sign in to confirm" in exc_str.lower():
                await interaction.followup.send(
                    embed=_e_err(
                        "YouTube yêu cầu xác thực",
                        "YouTube đang chặn bot ở bài này (bot-check/PO-Token) dù đã thử nhiều "
                        "client/proxy khác nhau. Thử lại sau ít phút hoặc thử bài/link khác."
                        + (f"\n\n💡 {getattr(exc, '_diag_hint', '')}" if getattr(exc, "_diag_hint", "") else ""),
                    ),
                    ephemeral=True,
                )
            else:
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
        finally:
            if _existing_player:
                _existing_player.pending_resolves = max(0, _existing_player.pending_resolves - 1)

        # Resolve thành công → khởi động ffmpeg + đệm NGAY (song song với nối voice ~2-3s) nếu bot đang
        # rảnh; bài sẽ phát gần như tức thì khi voice xong. Không ai lấy trong 60s thì tự dọn.
        _idle_now = (_existing_player is None) or not (
            _existing_player.vc.is_playing() or _existing_player.vc.is_paused() or _existing_player.queue)
        if _idle_now:
            track.start_prewarm(_existing_player.volume if _existing_player else config.DEFAULT_VOLUME / 100)

        # Resolve thành công → giờ mới kết nối voice.
        voice_client = await self._ensure_voice(interaction)
        if voice_client is None:
            track.discard_prewarm()
            return

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

        # QUAN TRỌNG: xác định trạng thái "đang rảnh hay không" TRƯỚC khi
        # enqueue() — vì enqueue() có thể đánh thức player loop gần như ngay
        # lập tức, và nếu loop resolve xong quá nhanh (thường gặp với link
        # trực tiếp), nó có thể chạy tới bước xoá _fetch_msg TRƯỚC KHI dòng
        # gán player._fetch_msg bên dưới kịp chạy — dẫn đến xoá trúng None,
        # bỏ lỡ luôn, để lại tin nhắn "Fetched" tồn tại vĩnh viễn trên chat.
        was_idle = not (player.vc.is_playing() or player.vc.is_paused())
        if was_idle:
            fetch_msg = await interaction.followup.send(
                embed=discord.Embed(
                    title       = "🔎  Fetched",
                    description = f"Loading **[{track.title}]({track.webpage_url})**…",
                    colour      = COLOUR_SUCCESS,
                )
            )
            # Gán NGAY trước khi enqueue() — đảm bảo player loop luôn thấy
            # được reference này trước khi nó có cơ hội chạy tới bước xoá.
            player._fetch_msg = fetch_msg

        position = player.enqueue(track)
        logger.info(
            "ENQUEUE | '%s' pos=%d queue=%d [guild %d] by %s",
            track.title, position, len(player.queue), guild_id, user,
        )

        if position == -1:
            track.discard_prewarm()
            await interaction.followup.send(
                embed=discord.Embed(
                    title       = "⚠️ Trùng bài",
                    description = f"**{track.title}** đã có trong queue rồi!",
                    colour      = COLOUR_PAUSE,
                ),
                ephemeral=True,
            )
        elif not was_idle:
            await interaction.followup.send(embed=_e_queued(track, position))

    # ── /pause ─────────────────────────────────────────────────────────────────

    @app_commands.command(name="pause", description="Pause the current song.")
    @app_commands.guild_only()
    async def pause(self, interaction: discord.Interaction) -> None:
        p = self._get_player(interaction.guild_id)  # type: ignore[arg-type]
        if not await _require_dj(interaction, p): return
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
        if not await _require_dj(interaction, p): return
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
        if not await _require_dj(interaction, p): return
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
        guild = interaction.guild
        assert guild is not None

        vc = guild.voice_client
        # Lấy player HOẶC tìm bot đang trong voice
        p = self._get_player(gid)
        if not await _require_dj(interaction, p): return

        # Nếu không có player nhưng bot vẫn trong voice → disconnect luôn
        if not p:
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
        if not await _require_dj(interaction, p): return
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

        queries = result if isinstance(result, list) else [result]
        guild_id = interaction.guild_id
        assert guild_id is not None

        if len(queries) == 1:
            # 1 bài: resolve trước, rồi mới quyết định gửi "Fetched" + enqueue —
            # tránh race condition giống /play (nếu gán player._fetch_msg SAU
            # enqueue(), player loop có thể đã chạy xong bước xoá trước đó rồi).
            try:
                track = await Track.from_query(queries[0], user, self.bot.loop)
            except Exception as exc:
                logger.warning("yt-dlp lỗi cho Spotify query '%s': %s", queries[0], exc)
                await interaction.followup.send(
                    embed=_e_err("Not Found", "Không tìm thấy bài nào!"), ephemeral=True,
                )
                return

            # Resolve thành công → giờ mới connect voice.
            voice_client = guild.voice_client
            if not isinstance(voice_client, discord.VoiceClient) or not voice_client.is_connected():
                try:
                    voice_client = await user.voice.channel.connect()  # type: ignore
                except Exception as exc:
                    await interaction.followup.send(
                        embed=_e_err("Connection Failed", f"Không thể kết nối voice channel.\n`{exc}`"),
                        ephemeral=True,
                    )
                    return

            player = self._get_player(guild_id)
            if player is None:
                player = GuildPlayer(
                    vc      = voice_client,
                    text_ch = interaction.channel,  # type: ignore[arg-type]
                    loop    = self.bot.loop,
                    volume  = config.DEFAULT_VOLUME / 100,
                )
                self._players[guild_id] = player

            was_idle = not (player.vc.is_playing() or player.vc.is_paused())
            if was_idle:
                fetch_msg = await interaction.followup.send(
                    embed=discord.Embed(
                        title       = "🎵 Spotify",
                        description = f"Loading **[{track.title}]({track.webpage_url})**…",
                        colour      = 0x1DB954,
                    )
                )
                player._fetch_msg = fetch_msg

            pos = player.enqueue(track)
            if pos == -1:
                await interaction.followup.send(
                    embed=discord.Embed(
                        title       = "⚠️ Trùng bài",
                        description = f"**{track.title}** đã có trong queue rồi!",
                        colour      = COLOUR_PAUSE,
                    ),
                    ephemeral=True,
                )
            elif not was_idle:
                await interaction.followup.send(embed=_e_queued(track, pos))
            return

        # Album/Playlist — resolve nhiều bài liên tiếp. Voice chỉ được connect
        # LẦN ĐẦU khi có 1 bài resolve thành công (lazy).
        added = 0
        first_track: Any = None
        player: "GuildPlayer | None" = self._get_player(guild_id)

        for q in queries:
            try:
                track = await Track.from_query(q, user, self.bot.loop)
                if player is None:
                    voice_client = guild.voice_client
                    if not isinstance(voice_client, discord.VoiceClient) or not voice_client.is_connected():
                        voice_client = await user.voice.channel.connect()  # type: ignore
                    player = GuildPlayer(
                        vc      = voice_client,
                        text_ch = interaction.channel,  # type: ignore[arg-type]
                        loop    = self.bot.loop,
                        volume  = config.DEFAULT_VOLUME / 100,
                    )
                    self._players[guild_id] = player
                pos = player.enqueue(track)
                if pos != -1:
                    added += 1
                    if first_track is None:
                        first_track = track
            except Exception:
                logger.warning("yt-dlp lỗi cho playlist item '%s' — bỏ qua", q)
                continue

        if not first_track:
            await interaction.followup.send(
                embed=_e_err("Not Found", "Không tìm thấy bài nào!"),
                ephemeral=True,
            )
            return

        thumb = getattr(first_track, "thumbnail", None) or ""
        await interaction.followup.send(
            embed=discord.Embed(
                title       = "🎵 Spotify Playlist",
                description = f"Đã thêm **{added}** bài vào queue!\nĐang phát: **{first_track.title}**",
                colour      = 0x1DB954,
            ).set_thumbnail(url=thumb)
        )

    async def _resolve_spotify(self, url: str) -> list[str] | str | None:
        """Convert Spotify URL → search queries cho YouTube."""
        spotify_re = re.compile(
            r"https?://open\.spotify\.com/(?:intl-[a-z]{2}/)?(track|album|playlist)/([A-Za-z0-9]+)"
        )
        m = spotify_re.search(url.strip())

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
                # Lỗi phổ biến hiện tại: Spotify Web API trả 403 "Active premium
                # subscription required for the owner of the app" — đây là
                # chính sách MỚI của Spotify (từ 02/2026, có tạm hoãn rồi lại áp
                # dụng thất thường): tài khoản Spotify SỞ HỮU app (gắn với
                # SPOTIFY_CLIENT_ID/SECRET) phải có Premium mới gọi API được,
                # không liên quan gì tới tài khoản người dùng bot. Không tự sửa
                # được bằng code — cần nâng cấp Premium cho tài khoản đó (và đôi
                # khi phải rotate lại Client Secret sau khi nâng cấp mới hết).
                logger.warning("Spotify API error (%s): %s — dùng fallback không cần API", stype, exc)

        # Không tìm được / API lỗi → thử fallback KHÔNG cần Spotify API:
        # scrape thẳng trang track để lấy tên bài + nghệ sĩ từ thẻ meta (không
        # cần Premium, không cần Client ID/Secret). Chỉ hoạt động tốt cho
        # single track — album/playlist vẫn cần API ở trên.
        if m and m.group(1) == "track":
            query = await self._scrape_spotify_track_title(url.strip())
            if query:
                return query

        # Fallback oembed (title đơn giản, không có tên nghệ sĩ tách riêng)
        if m:
            try:
                clean_url = url.strip().split("?")[0]  # bỏ tracking param ?si=...
                async with _httpx.AsyncClient(timeout=8.0, follow_redirects=True) as client:
                    r = await client.get(
                        "https://open.spotify.com/oembed",
                        params={"url": clean_url},
                        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"},
                    )
                if r.status_code == 200:
                    title = r.json().get("title", "").strip()
                    if title:
                        return title
            except Exception as exc:
                logger.warning("Spotify oembed fallback lỗi: %s", exc)

        # KHÔNG bao giờ trả raw open.spotify.com URL ra làm query — yt-dlp sẽ cố
        # tự trích xuất URL đó và luôn fail với "[DRM] ... known to use DRM
        # protection" (Spotify mã hoá DRM, không extract được). Trả None để
        # báo lỗi rõ ràng thay vì query rác gây nhầm lẫn.
        if m:
            return None
        if url.strip():
            return url.strip()
        return None

    async def _scrape_spotify_track_title(self, track_url: str) -> str | None:
        """
        Lấy 'Tên bài + nghệ sĩ' bằng cách đọc thẻ <meta> trong HTML của trang
        Spotify công khai — không cần Spotify API/Client ID/Premium. Dùng khi
        Spotify Web API bị lỗi 403 (chính sách Premium-required hiện tại).
        """
        try:
            async with _httpx.AsyncClient(timeout=8.0, follow_redirects=True) as client:
                resp = await client.get(
                    track_url,
                    headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"},
                )
            if resp.status_code != 200:
                return None
            html = resp.text
            title_m = re.search(r'<meta property="og:title" content="([^"]+)"', html)
            desc_m  = re.search(r'<meta property="og:description" content="([^"]+)"', html)
            if not title_m:
                return None
            title = title_m.group(1).strip()
            artist = ""
            if desc_m:
                # og:description thường có dạng "Song · Artist · Song · Year"
                artist = desc_m.group(1).split("·")[0].strip()
                if artist.lower() == title.lower():
                    artist = ""
            query = f"{title} {artist}".strip()
            return query or None
        except Exception as exc:
            logger.warning("Scrape Spotify track title lỗi: %s", exc)
            return None

    # ── /seek ──────────────────────────────────────────────────────────────────

    @app_commands.command(name="seek", description="Tua đến vị trí bất kỳ. Ví dụ: 1:30 hoặc 90")
    @app_commands.describe(position="Thời gian (vd: 1:30 hoặc 90)")
    @app_commands.guild_only()
    async def seek(self, interaction: discord.Interaction, position: str) -> None:
        p = self._get_player(interaction.guild_id)  # type: ignore[arg-type]
        if not await _require_dj(interaction, p): return
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
            pcm = discord.FFmpegPCMAudio(
                track.url,
                before_options=_ffmpeg_before(seek=secs, no_proxy=not getattr(track, "_url_via_proxy", False),
                                               proxy_override=getattr(track, "_proxy_used", None),
                                               headers=getattr(track, "http_headers", None),
                                               hls=_is_hls(track.url)),
                options="-vn",
            )
            source = discord.PCMVolumeTransformer(pcm, volume=safe_vol)
            p.vc.stop()
            await asyncio.sleep(0.3)
            p._start = time.monotonic() - secs
            p._current_source = source
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
        if not await _require_dj(interaction, p): return
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
        if not await _require_dj(interaction, p): return
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
        if not await _require_dj(interaction, p): return
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

    # ── /djrole ────────────────────────────────────────────────────────────────

    @app_commands.command(
        name="djrole",
        description="Đặt/xem/xoá 1 role được điều khiển MỌI bài (ngoài người tự /play bài đó).",
    )
    @app_commands.describe(
        role  = "Role được điều khiển mọi bài hát. Để trống = xem cấu hình hiện tại.",
        clear = "True để bỏ role này (vẫn còn quy tắc mặc định: ai /play bài nào tự điều khiển bài đó).",
    )
    @app_commands.guild_only()
    @app_commands.default_permissions(manage_guild=True)
    async def djrole(
        self,
        interaction: discord.Interaction,
        role: discord.Role | None = None,
        clear: bool = False,
    ) -> None:
        guild = interaction.guild
        assert guild is not None
        member = interaction.user
        assert isinstance(member, discord.Member)

        # Phòng khi admin server đã tự đổi permission override cho lệnh này ở
        # Integrations settings — vẫn chặn lại ở runtime cho chắc.
        if not (member.guild_permissions.manage_guild or member.id == guild.owner_id):
            await interaction.response.send_message(
                embed=_e_err("Không đủ quyền", "Cần quyền **Manage Server** để dùng lệnh này."),
                ephemeral=True,
            )
            return

        if clear:
            had = _DJ_ROLES.pop(guild.id, None) is not None
            _save_dj_roles()
            await interaction.response.send_message(
                embed=discord.Embed(
                    title       = "🔓 Đã bỏ DJ role",
                    description = (
                        "Không còn role nào điều khiển MỌI bài nữa.\n"
                        "Quy tắc mặc định vẫn giữ nguyên: **ai `/play` bài nào thì tự điều khiển bài đó** "
                        "(+ Admin/chủ server luôn được)."
                    ) if had else "Server này chưa từng đặt DJ role.",
                    colour      = COLOUR_SUCCESS,
                ),
                ephemeral=True,
            )
            return

        if role is None:
            current_id = _DJ_ROLES.get(guild.id)
            base = (
                "**Mặc định (luôn áp dụng):** ai `/play` ra bài đang phát thì chỉ người đó "
                "(+ Admin/chủ server) điều khiển được panel/lệnh của bài đó — người khác thấy "
                "panel nhưng bấm không có tác dụng."
            )
            extra = f"\n\n**DJ role hiện tại:** <@&{current_id}> — role này điều khiển được **mọi bài**, không riêng gì bài tự request." \
                if current_id else "\n\n**DJ role:** chưa đặt."
            await interaction.response.send_message(
                embed=discord.Embed(title="🎚️ Cấu hình điều khiển nhạc", description=base + extra, colour=COLOUR_QUEUE),
                ephemeral=True,
            )
            return

        _DJ_ROLES[guild.id] = role.id
        _save_dj_roles()
        await interaction.response.send_message(
            embed=discord.Embed(
                title       = "🎚️ Đã đặt DJ Role",
                description = (
                    f"{role.mention} giờ điều khiển được **mọi bài hát** (không riêng bài họ tự request), "
                    "cộng thêm quy tắc mặc định là ai `/play` bài nào vẫn tự điều khiển được bài đó, "
                    "và Admin/chủ server luôn được.\n"
                    "-# Dùng `/djrole clear:True` để bỏ role này (giữ lại quy tắc mặc định)."
                ),
                colour      = COLOUR_SUCCESS,
            ),
            ephemeral=True,
        )
        logger.info("DJROLE | set role=%d by %s [guild %d]", role.id, member, guild.id)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Music(bot))
