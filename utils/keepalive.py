"""
HTTP keep-alive server chạy NGAY TRÊN event loop của bot (aiohttp — đã là dependency của discord.py,
không thêm thư viện nào, không cần thread riêng, không block vòng lặp chính vì toàn bộ là async).

Dành cho host kiểu Render Free Web Service: dịch vụ bị ngủ sau 15 phút không có HTTP request tới và
phải mở cổng $PORT trên 0.0.0.0 thì mới được coi là "live". Gồm:
  • Web server: GET / → 200 "Bot is alive!"  (+ /health, /ping, /status dạng JSON)
  • Self-ping: nếu có RENDER_EXTERNAL_URL (Render tự set) hoặc KEEPALIVE_URL, cứ ~10 phút bot tự gọi
    URL công khai của chính nó. Request đi vòng qua proxy của Render nên được tính là traffic vào,
    giữ cho dịch vụ không ngủ. (Nên thêm UptimeRobot/cron-job.org ping mỗi 5 phút làm dự phòng.)
"""
from __future__ import annotations

import asyncio
import logging
import os
import random
import time
from typing import Any, Callable

import aiohttp
from aiohttp import web

log = logging.getLogger("keepalive")

_START = time.time()
_PING_INTERVAL = float(os.environ.get("KEEPALIVE_INTERVAL", "600"))   # giây; phải < 900 (15 phút)


def _make_app(get_bot: Callable[[], Any]) -> web.Application:
    def _ready() -> Any:
        b = get_bot()
        return b if (b is not None and b.is_ready()) else None

    async def index(_: web.Request) -> web.Response:
        return web.Response(text="Bot is alive!", status=200)

    async def health(_: web.Request) -> web.Response:
        up = int(time.time() - _START)
        h, rem = divmod(up, 3600)
        m, s = divmod(rem, 60)
        return web.json_response({"status": "online", "bot": "ready" if _ready() else "starting",
                                  "uptime": f"{h}h {m}m {s}s"})

    async def ping(_: web.Request) -> web.Response:
        b = _ready()
        return web.json_response({"pong": True, "latency_ms": round(b.latency * 1000, 2) if b else 0})

    async def status(_: web.Request) -> web.Response:
        b = _ready()
        return web.json_response({"status": "online", "bot_ready": bool(b),
                                  "guild_count": len(b.guilds) if b else 0,
                                  "latency_ms": round(b.latency * 1000, 2) if b else 0,
                                  "uptime_s": int(time.time() - _START)})

    app = web.Application()
    app.add_routes([web.get("/", index), web.get("/health", health),
                    web.get("/ping", ping), web.get("/status", status)])
    return app


async def _self_ping(url: str) -> None:
    """Gọi URL công khai của chính mình định kỳ để Render không cho ngủ. Không bao giờ raise."""
    await asyncio.sleep(30)   # đợi web server + bot lên hẳn
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as sess:
        while True:
            try:
                async with sess.get(url) as r:
                    log.info("keep-alive self-ping %s → HTTP %s", url, r.status)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("keep-alive self-ping lỗi: %s", exc)
            await asyncio.sleep(_PING_INTERVAL + random.uniform(-30, 30))


async def start(get_bot: Callable[[], Any]) -> web.AppRunner | None:
    """
    Mở server trên 0.0.0.0:$PORT (mặc định 8080) và bật self-ping nếu có URL công khai.
    Lỗi ở đây (vd cổng bận) chỉ log cảnh báo — KHÔNG làm bot ngừng chạy.
    """
    port = int(os.environ.get("PORT", 8080))
    try:
        runner = web.AppRunner(_make_app(get_bot), access_log=None)
        await runner.setup()
        await web.TCPSite(runner, "0.0.0.0", port).start()
    except Exception as exc:  # noqa: BLE001
        log.warning("Không mở được keep-alive server trên cổng %s: %s", port, exc)
        return None
    log.info("Keep-alive HTTP server đang nghe trên 0.0.0.0:%s", port)

    url = (os.environ.get("KEEPALIVE_URL") or os.environ.get("RENDER_EXTERNAL_URL") or "").strip()
    if url:
        asyncio.create_task(_self_ping(url), name="keepalive-selfping")
        log.info("Self-ping bật: %s mỗi ~%.0fs", url, _PING_INTERVAL)
    else:
        log.info("Chưa có RENDER_EXTERNAL_URL/KEEPALIVE_URL → không self-ping (hãy dùng UptimeRobot ping URL của bạn).")
    return runner
