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

logger = get_logger(__name__)

# ── Colours ────────────────────────────────────────────────────────────────────
COLOUR_PLAY    = 0x1DB954   # Spotify green
COLOUR_PAUSE   = 0xF5A623   # amber
COLOUR_STOP    = 0xED4245   # red
COLOUR_QUEUE   = 0x5865F2   # blurple
COLOUR_SUCCESS = 0x57F287   # mint

# ── Webshare Proxy Manager ─────────────────────────────────────────────────────

class WebshareProxyManager:
    """Proxy manager: Bright Data residential (ưu tiên cao nhất) → US → Japan → free datacenter."""

    # US proxy để bypass video chỉ cho Mỹ xem. Set env var PROXY_US=http://user:pass@host:port
    PROXY_US = os.environ.get("PROXY_US", "")

    # Bright Data residential (freemium) — IP nhà dân thật, không nằm trong blocklist
    # công khai như datacenter/free proxy. Ưu tiên CAO NHẤT vì chất lượng tốt nhất.
    #
    # QUAN TRỌNG: gói "freemium" có vẻ KHÔNG cho phép thêm hậu tố tùy chỉnh nào
    # (kể cả -session-, -ip-, -country-) — mọi hậu tố đều bị từ chối 403. Dùng
    # ĐÚNG Y HỆT credentials cơ bản như dashboard Bright Data hiển thị, không
    # thêm gì cả. IP residential sẽ được họ tự động xoay vòng ngẫu nhiên.
    _BRD_CRED = "c7593mkpjov5"
    _BRD_CUSTOMER = "brd-customer-hl_3fb760f0-zone-freemium"
    BRIGHTDATA_RESIDENTIAL = [
        f"http://{_BRD_CUSTOMER}:{_BRD_CRED}@brd.superproxy.io:33335",
    ]

    # Webshare auth proxies — 10 proxy cố định của gói free, đã TEST THẬT bằng
    # curl + ip-api.com ngày 16/08/2026, xác nhận còn sống + đúng quốc gia ghi
    # chú (không phải đoán mò như phần free public bên dưới).
    _WS_CRED = "fywznozi:gv94cmc9t7qs"
    WEBSHARE_10 = [
        f"http://{_WS_CRED}@31.56.127.193:7684",    # US — Seattle, WA
        f"http://{_WS_CRED}@198.23.243.226:6361",   # US — Los Angeles, CA
        f"http://{_WS_CRED}@38.154.185.97:6370",    # US — Piscataway, NJ
        f"http://{_WS_CRED}@191.96.254.138:6185",   # US — Los Angeles, CA
        f"http://{_WS_CRED}@142.111.67.146:5611",   # JP — Ueda, Nagano
        f"http://{_WS_CRED}@31.59.20.176:6754",     # GB — London
        f"http://{_WS_CRED}@45.38.107.97:6014",     # GB — London
        f"http://{_WS_CRED}@198.105.121.200:6462",  # GB — Canary Wharf
        f"http://{_WS_CRED}@64.137.96.74:6641",     # ES — Madrid
        f"http://{_WS_CRED}@84.247.60.125:6095",    # PL — Warsaw
    ]

    # Public free proxies (fallback cuối — không auth, dễ chết, CHƯA kiểm
    # chứng như WEBSHARE_10 ở trên, chỉ dùng khi mọi proxy có auth đều fail)
    PRIORITY = [
        # BRIGHTDATA_RESIDENTIAL rút khỏi vòng xoay LẦN 2 — đã test lại, vẫn 403
        # dù Playground báo OK + IP đã whitelist. Xác nhận lỗi hạ tầng phía họ.
        # Cần liên hệ support Bright Data xác nhận zone đã fix trước khi bật lại.
        # *BRIGHTDATA_RESIDENTIAL,
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
                    for p in r.json().get("results", []):
                        country = p.get("country_code", "US").upper()
                        if country not in self.ALLOWED_COUNTRIES:
                            continue
                        url = f"http://{p['username']}:{p['password']}@{p['proxy_address']}:{p['port']}"
                        if url not in self._proxies:
                            self._proxies.append(url)
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

    def force_us(self) -> None:
        """Ép dùng proxy US cho lần yt-dlp tiếp theo (bypass gcr=us geo-lock)."""
        if self.PROXY_US:
            self._force_us = True
            logger.info("Forcing US proxy: %s", self.PROXY_US)
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

# ── yt-dlp options ─────────────────────────────────────────────────────────────

def _ytdl_opts(cookies: bool = True, use_proxy: bool = True) -> dict[str, Any]:
    # tv_embedded/android_vr được thiết kế để BYPASS auth — nếu gửi kèm cookies,
    # YouTube trả về format list rỗng/hạn chế → "Requested format is not available".
    # Khi có cookies hợp lệ → ưu tiên client hỗ trợ auth (web, android).
    # Thêm "tv" — hiện là 1 trong các client MẶC ĐỊNH của chính yt-dlp (cùng
    # ios/web) từ giữa 2025, khá bền với PO-Token so với mweb/web đơn thuần.
    cookie_file_exists = cookies and _cookies_valid()
    player_clients = ["tv", "web", "android", "ios"] if cookie_file_exists else ["tv", "tv_embedded", "ios", "android_vr"]

    opts: dict[str, Any] = {
        "format"          : "bestaudio/best/18/140/251",  # 18=progressive mp4, 140=m4a, 251=opus — fallback tường minh khi bestaudio selector không match được (client trả format thiếu tag acodec/vcodec)
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
        "sleep_interval_requests": 1,
        "sleep_interval"     : 1,
        "max_sleep_interval" : 3,
        "ratelimit"          : 3_000_000,  # 3MB/s cap — tránh spike bị flag bot
    }
    if use_proxy:
        proxy = _proxy.get()
        if proxy:
            # Đảm bảo format đúng cho yt-dlp
            opts["proxy"] = proxy
            # Thêm header auth riêng để tránh 407
            import urllib.parse
            parsed = urllib.parse.urlparse(proxy)
            if parsed.username and parsed.password:
                opts["http_headers"] = opts.get("http_headers", {})
                creds = base64.b64encode(f"{parsed.username}:{parsed.password}".encode()).decode()
                opts["http_headers"]["Proxy-Authorization"] = f"Basic {creds}"
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
) -> str:
    """
    FFmpeg input options cho YouTube stream.
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
        "-reconnect_streamed 1 "
        "-reconnect_delay_max 2 "         # giảm từ 5s→2s: rút ngắn thời gian "đứng
                                           # hình" tối đa mỗi lần ffmpeg tự dò kết nối
                                           # lại gần cuối bài (reconnect_at_eof)
        "-reconnect_at_eof 1 "
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
            resp = await client.get(f"{base}/api/v1/videos/{video_id}", timeout=6.0)
            if resp.status_code != 200:
                return None
            data = resp.json()
            formats = (data.get("adaptiveFormats") or []) + (data.get("formatStreams") or [])
            audio_only = [f for f in formats if "audio" in f.get("type", "")] or formats
            if not audio_only:
                return None
            best = max(audio_only, key=lambda f: int(f.get("bitrate", 0) or 0))
            if not best.get("url"):
                return None
            logger.info("Invidious fallback OK via %s for video_id=%s", base, video_id)
            return {
                "id"          : video_id,
                "title"       : data.get("title", "Unknown Title"),
                "duration"    : data.get("lengthSeconds", 0),
                "url"         : best["url"],
                "thumbnail"   : (data.get("videoThumbnails") or [{}])[0].get("url", ""),
                "uploader"    : data.get("author", "Unknown"),
                "webpage_url" : f"https://www.youtube.com/watch?v={video_id}",
            }
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
    if not b64:
        # QUAN TRỌNG: trước đây return im lặng ở đây — không cách nào biết
        # được là do thiếu biến môi trường hay do lỗi khác. Giờ log rõ ràng
        # để không còn debug mù nữa.
        logger.warning(
            "Không tìm thấy YTDLP_COOKIES_B64 hoặc YTDLP_COOKIES_B64_1 nào trong "
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

        if yt_match:
            video_url = f"https://www.youtube.com/watch?v={yt_match.group(1)}"
        else:
            # Text search — cần 1 bước tìm kiếm nhẹ để ra video_id trước khi
            # resolve đầy đủ. Bọc try/except: nếu bước tìm kiếm này lỗi (hiếm,
            # nhưng có thể do bot-check), KHÔNG để nó chặn toàn bộ quá trình —
            # thử lại chính bằng "ytsearch1:<query>" trong vòng client rotation
            # phía dưới luôn, để vẫn có cơ hội qua được nhờ PO-Token/client tốt.
            resolved = query if _URL_RE.match(query) else f"ytsearch1:{query}"
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
                    video_url = (entry.get("url") or entry.get("webpage_url")
                                 or f"https://www.youtube.com/watch?v={entry['id']}")
                else:
                    video_url = resolved
            except Exception as _search_exc:
                logger.warning("Bước search sơ bộ lỗi (%s) — thử lại trực tiếp qua client rotation", _search_exc)
                video_url = resolved  # để vòng client rotation tự xử lý (kể cả dạng ytsearch1:)

        # Bước 2: Thử không proxy trước → bị chặn thì dùng proxy
        last_err: Exception | None = None

        for attempt in range(4):
            # Bright Data/proxy datacenter thường bị Google tự động gắn cờ bot, và
            # còn gây JITTER khi stream thật (âm thanh giật) — ưu tiên tối đa việc
            # KHÔNG dùng proxy. Chỉ dùng proxy ở lần thử CUỐI CÙNG (attempt 3).
            #
            # THỨ TỰ CLIENT (đã đảo lại dựa trên thực tế quan sát được):
            #   attempt 0: android/tv_embedded/android_vr, KHÔNG proxy — đây là
            #     tổ hợp DUY NHẤT luôn thành công trong thực tế (chưa từng thử
            #     không proxy trước đây, luôn ép proxy oan uổng). tv_embedded/
            #     android_vr được YouTube thiết kế bypass-auth nên vẫn còn trả
            #     URL stream trực tiếp, không bị khoá SABR-only như web/ios.
            #   attempt 1: web + cookies + PO-Token, KHÔNG proxy — vẫn giữ thử
            #     (đề phòng 1 số video legacy chưa bị SABR khoá), nhưng KHÔNG
            #     còn ưu tiên vì thực tế gần như luôn "Requested format is not
            #     available" — đây là do YouTube đã chuyển web/ios sang
            #     SABR-only streaming (không lộ URL trực tiếp nữa), KHÔNG phải
            #     lỗi cookie/PO-Token — không có cách nào fix từ phía yt-dlp.
            #   attempt 2: ios, KHÔNG proxy — tương tự, giữ làm phương án phụ.
            #   attempt 3: android/tv_embedded/android_vr/tv, CÓ proxy — chỉ
            #     khi cả 3 lần trước đều fail (hiếm, có thể do IP server bị chặn).
            use_proxy = (attempt == 3)

            if attempt == 0:
                # ĐỔI THỨ TỰ (17/08/2026): tv_embedded/tv lên ĐẦU, android/
                # android_vr bị đẩy xuống attempt cuối. Lý do: android/
                # android_vr giờ yêu cầu GVS PO-Token cho MỌI itag TRỪ 18 —
                # itag 251 (audio-only, cái bot luôn chọn) LUÔN cần token,
                # trong khi tv_embedded trả về định dạng HLS (m3u8) mà theo
                # tài liệu yt-dlp hiện tại CHƯA cần PO-Token. Quan trọng hơn:
                # trước đây gộp chung android_vr + tv_embedded trong CÙNG 1
                # lần extract khiến yt-dlp merge format rồi ưu tiên chọn
                # format của android_vr (bitrate cao hơn) thay vì HLS của
                # tv_embedded — dù tv_embedded có mặt cũng vô nghĩa. Giờ tách
                # hẳn ra 1 attempt riêng, không còn ai "giành" format nữa.
                opts = _ytdl_opts(False, use_proxy=use_proxy)
                opts["extractor_args"]["youtube"] = {"player_client": ["tv_embedded", "tv"],
                                                       "skip": ["translated_subs", "comments"]}
            elif attempt == 1:
                # web + cookies + PO-Token — combo đúng nếu video chưa bị SABR khoá.
                # Thêm "mweb" (mobile web) — LƯU Ý: mweb từng KHÔNG cần PO-Token
                # nhưng yt-dlp đã đổi default sang "tv" vì mweb giờ cũng hay cần
                # PO-Token rồi (không còn bền như trước) — vẫn giữ làm phương án
                # phụ, không kỳ vọng nhiều.
                opts = _ytdl_opts(True, use_proxy=use_proxy)
                opts["extractor_args"]["youtube"] = {"player_client": ["web", "mweb"],
                                                       "skip": ["translated_subs", "comments"]}
            elif attempt == 2:
                # ios KHÔNG dùng cookies — nếu không yt-dlp sẽ tự skip client này
                opts = _ytdl_opts(False, use_proxy=use_proxy)
                opts["extractor_args"]["youtube"] = {"player_client": ["ios"],
                                                       "skip": ["translated_subs", "comments"]}
            else:  # attempt 3 — phương án cuối, CÓ proxy — gộp lại full combo kể
                   # cả android/android_vr (đôi khi vẫn hoạt động, không loại hẳn)
                opts = _ytdl_opts(False, use_proxy=use_proxy)
                opts["extractor_args"]["youtube"] = {"player_client": ["tv_embedded", "tv", "android", "android_vr"],
                                                       "skip": ["translated_subs", "comments"]}
            logger.info("Client rotation | attempt %d → %s (proxy=%s)", attempt + 1,
                        opts["extractor_args"]["youtube"]["player_client"],
                        "yes" if use_proxy else "no")

            try:
                data: dict[str, Any] = await loop.run_in_executor(None, _extract_sync, opts, video_url)
                resolved_via_proxy = bool(opts.get("proxy"))
                resolved_proxy_str = opts.get("proxy")
                break
            except Exception as e:
                last_err = e
                err_str = str(e)

                # Chỉ raise ngay nếu video THẬT SỰ không tồn tại/private — không bắt nhầm
                # "Requested format is not available" (lỗi format, có thể retry được).
                is_truly_unavailable = any(x in err_str for x in [
                    "video is unavailable", "video unavailable", "this video is private",
                    "video has been removed", "account associated with this video",
                    "private video", "video is no longer available",
                ]) and "format" not in err_str.lower()
                if is_truly_unavailable:
                    raise last_err

                # QUAN TRỌNG (18/08/2026): ĐẢO NGƯỢC logic — trước đây dùng
                # allowlist (chỉ retry nếu lỗi khớp 1 trong vài câu quen thuộc:
                # "Requested format", "403", "Sign in to confirm"...). Cách này
                # LUÔN đuổi theo sau, vì YouTube liên tục đổi câu thông báo mới
                # ("DRM protected", "The page needs to be reloaded.", và chắc
                # chắn còn nhiều câu khác nữa trong tương lai) — mỗi câu lạ
                # không nằm trong allowlist khiến bot bỏ cuộc oan sau ĐÚNG 1
                # lần thử (tv_embedded/tv), chưa từng thử qua android/ios/web dù
                # 3 client đó có thể phát bình thường cho ĐÚNG video đó. Giờ mặc
                # định LUÔN retry hết cả 4 attempt — chỉ dừng sớm ở nhánh
                # is_truly_unavailable phía trên (video removed/private thật).
                is_407 = "407" in err_str or "Proxy Authentication" in err_str

                if attempt < 3:
                    current = opts.get("proxy", "")
                    # 407 = proxy auth fail → mark dead permanent
                    if is_407:
                        if current:
                            _proxy.mark_dead(current, temporary=False)
                            # Nếu proxy US bị 407 (auth fail) → đừng force lại nó
                            if current == _proxy.PROXY_US:
                                logger.warning("PROXY_US bị 407 — credentials sai/hết hạn! Cần update Secrets.")
                    elif current:
                        _proxy.mark_dead(current, temporary=True)

                    # Backoff ngắn (1.5s cố định) — KHÔNG dùng exponential nữa vì lỗi
                    # "Requested format" trên web/ios thường do YouTube khoá SABR-only,
                    # KHÔNG phải rate-limit → chờ lâu hơn cũng không giúp gì, chỉ tốn
                    # thời gian user chờ nhạc phát.
                    delay = 0.6
                    logger.warning(
                        "yt-dlp attempt %d failed (%s), waiting %.1fs (proxy=%s)",
                        attempt + 1, err_str[:80].replace("\n", " "), delay,
                        current[-15:] if current else "none",
                    )
                    await asyncio.sleep(delay)
                    continue
                # Hết attempt cuối cùng mà vẫn fail → KHÔNG raise ngay ở đây,
                # để code phía dưới có cơ hội thử Piped fallback trước khi bỏ cuộc.
                break

        if last_err is not None and 'data' not in dir():
            # ── Mọi client/proxy/AI đều fail → thử Piped API làm phương án cuối ──
            vid_match = re.search(r"(?:v=|/)([\w-]{11})(?:[&?/]|$)", video_url)
            if vid_match:
                logger.warning("yt-dlp exhausted, trying Piped fallback for %s…", vid_match.group(1))
                piped_data = await _piped_fallback(vid_match.group(1), loop)
                if piped_data:
                    # Piped/Invidious tự phục vụ URL của họ, không qua proxy Bright Data/Webshare
                    return cls(piped_data, requester, via_proxy=False)

            err_str = str(last_err)
            if ("Sign in to confirm" in err_str or "not a bot" in err_str) and not _cookies_valid():
                logger.error(
                    "Bot-check thất bại vĩnh viễn — cookies.txt thiếu/rỗng! "
                    "Cần export cookies YouTube hợp lệ và đặt tại cookies.txt (thư mục gốc bot)."
                )
                raise yt_dlp.utils.DownloadError(
                    "YouTube yêu cầu xác thực (bot-check) và bot chưa có cookies hợp lệ. "
                    "Cần cập nhật file cookies.txt."
                )
            raise last_err  # type: ignore

        # ── Nếu URL có gcr=<country> → re-fetch qua proxy để tránh 403 khi stream ──
        # TRƯỚC ĐÂY chỉ check "gcr=us" — video geo-lock nước khác (vd gcr=nl,
        # gcr=de...) bị bỏ qua hoàn toàn, phát thẳng URL geo-mismatch → 403
        # ngay khi ffmpeg mở stream. Giờ bắt MỌI mã quốc gia.
        stream_url = data.get("url", "")
        gcr_match = re.search(r"gcr=([a-z]{2})\b", stream_url)
        if gcr_match:
            gcr_country = gcr_match.group(1).upper()
            use_forced_us = gcr_country == "US" and bool(WebshareProxyManager.PROXY_US)
            logger.info(
                "gcr=%s detected, re-fetching via %s proxy for '%s'",
                gcr_country, "US (forced)" if use_forced_us else "rotating pool",
                data.get("title", "?"),
            )
            if use_forced_us:
                _proxy.force_us()
            try:
                # QUAN TRỌNG: phải chỉ định LẠI đúng player_client (không dùng
                # default của _ytdl_opts() vì mặc định có "web", dễ dính SABR/
                # "Requested format is not available"). tv_embedded/tv lên đầu
                # (HLS, chưa cần PO-Token) — android/android_vr chỉ làm phương
                # án phụ vì itag 251 chúng trả về giờ cần PO-Token mà bgutil-pot
                # tự thừa nhận "không còn bypass được đa số trường hợp" (17/08/2026).
                proxy_opts = _ytdl_opts(True, use_proxy=True)
                proxy_opts["extractor_args"]["youtube"] = {
                    "player_client": ["tv_embedded", "tv", "android", "android_vr"],
                    "skip": ["translated_subs", "comments"],
                }
                data = await loop.run_in_executor(None, _extract_sync, proxy_opts, video_url)
                resolved_via_proxy = True
                resolved_proxy_str = proxy_opts.get("proxy")
                logger.info("gcr=%s re-fetch OK via proxy for '%s'", gcr_country, data.get("title", "?"))
            except Exception as _e:
                logger.warning("gcr=%s proxy re-fetch failed: %s — will retry on stream drop", gcr_country, _e)

        return cls(data, requester, via_proxy=resolved_via_proxy,  # type: ignore[possibly-undefined]
                   proxy_used=resolved_proxy_str)  # type: ignore[possibly-undefined]

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
        proxy_order = (True,) if force_proxy else (False, True)
        for use_proxy in proxy_order:
            try:
                # QUAN TRỌNG: ép dùng client tv_embedded/tv trước (HLS, chưa cần
                # PO-Token) — KHÔNG dùng default của _ytdl_opts(), vì mặc định
                # ưu tiên web/android/ios khi có cookies.txt, 2 client này
                # thường xuyên fail "Requested format is not available". Nếu
                # bài vừa fail vì 403/PO-Token (như android_vr itag 251), thử
                # lại đúng client cũ chỉ fail y hệt lần nữa — đổi sang
                # tv_embedded trước mới có cơ hội thoát khỏi vòng lặp 403.
                opts = _ytdl_opts(cookies=False, use_proxy=use_proxy)
                opts["extractor_args"] = {
                    "youtube": {"player_client": ["tv_embedded", "tv", "android", "android_vr"],
                                "skip": ["translated_subs", "comments"]},
                    "youtubepot-bgutilhttp": {"base_url": ["http://127.0.0.1:4416"]},
                }
                if force_itag18:
                    opts["format"] = "18"
                data = await loop.run_in_executor(None, _extract_sync, opts, self.webpage_url)
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
        if last_exc:
            logger.warning("Failed to refresh URL for '%s': %s", self.title, last_exc)

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
        if self._url_via_proxy and seek == 0 and self.video_id:
            try:
                piped_data = await asyncio.wait_for(
                    _piped_fallback(self.video_id, asyncio.get_running_loop()), timeout=5.0,
                )
            except asyncio.TimeoutError:
                piped_data = None
            if piped_data and piped_data.get("url"):
                logger.info("Dùng Piped stream cho '%s' — tránh proxy jitter", self.title)
                pcm = discord.FFmpegPCMAudio(
                    piped_data["url"],
                    before_options=_ffmpeg_before(no_proxy=True),
                    options=opts,
                )
                return discord.PCMVolumeTransformer(pcm, volume=safe_vol)

        pcm = discord.FFmpegPCMAudio(
            self.url,
            before_options=_ffmpeg_before(seek=seek, no_proxy=not self._url_via_proxy,
                                           proxy_override=self._proxy_used,
                                           headers=self.http_headers),
            options=opts,
        )
        return discord.PCMVolumeTransformer(pcm, volume=safe_vol)


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
                    source = await track.make_source(self.volume)
                    self._current_source = source  # cho set_volume() chỉnh live
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

                def _after(err: Exception | None, _l: asyncio.AbstractEventLoop = _loop, _e: asyncio.Event = _ev) -> None:
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
                                    self._loop, force=True, force_proxy=is_geo,
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
            if "cookies" in exc_str.lower() or "bot-check" in exc_str.lower():
                await interaction.followup.send(
                    embed=_e_err(
                        "YouTube yêu cầu xác thực",
                        "Bot đang bị YouTube nghi ngờ là bot và cần cookies hợp lệ để vượt qua.\n"
                        "Admin cần cập nhật file `cookies.txt` (export cookies YouTube từ browser đã đăng nhập).",
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

        # Resolve thành công → giờ mới kết nối voice.
        voice_client = await self._ensure_voice(interaction)
        if voice_client is None:
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
                                               headers=getattr(track, "http_headers", None)),
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
