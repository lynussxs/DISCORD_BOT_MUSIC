"""
Tự xoá MỌI thông báo lỗi của bot sau vài giây (mặc định 10s, đổi bằng AUTO_DELETE_ERRORS_SECONDS; 0 = tắt).

Thay vì sửa ~40 chỗ gửi lỗi rải rác, module này bọc 4 đường gửi tin của discord.py:
  • InteractionResponse.send_message  (phản hồi đầu tiên của slash command)
  • Webhook.send                      (interaction.followup.send)
  • Interaction.edit_original_response (sửa tin "đang xử lý..." thành lỗi)
  • abc.Messageable.send              (channel.send)
Tin nào có embed tiêu đề bắt đầu bằng "❌" (hoặc nội dung bắt đầu bằng "❌") được coi là lỗi và sẽ bị xoá.
Mọi lỗi khi xoá (tin đã bị xoá/ephemeral hết hạn/thiếu quyền) đều bị nuốt — không bao giờ ảnh hưởng bot.
"""
from __future__ import annotations

import asyncio
import functools
import logging
import os
from typing import Any, Awaitable, Callable

import discord

log = logging.getLogger("autodelete")

DELAY = float(os.environ.get("AUTO_DELETE_ERRORS_SECONDS", "10") or 0)
_MARK = "❌"
_pending: dict[Any, "asyncio.Task[None]"] = {}
_installed = False


def is_error(content: Any = None, embed: Any = None, embeds: Any = None) -> bool:
    if isinstance(content, str) and content.lstrip().startswith(_MARK):
        return True
    items = []
    if embed is not None and embed is not discord.utils.MISSING:
        items.append(embed)
    if embeds is not None and embeds is not discord.utils.MISSING:
        items.extend(embeds)
    return any(isinstance(e, discord.Embed) and (e.title or "").lstrip().startswith(_MARK) for e in items)


def _schedule(key: Any, delete: Callable[[], Awaitable[Any]]) -> None:
    if DELAY <= 0:
        return
    old = _pending.pop(key, None)
    if old:
        old.cancel()

    async def _run() -> None:
        try:
            await asyncio.sleep(DELAY)
            await delete()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 — tin đã mất/hết hạn/thiếu quyền: bỏ qua
            pass
        finally:
            if _pending.get(key) is asyncio.current_task():
                _pending.pop(key, None)

    try:
        _pending[key] = asyncio.get_running_loop().create_task(_run())
    except RuntimeError:
        pass


def _cancel(key: Any) -> None:
    t = _pending.pop(key, None)
    if t:
        t.cancel()


def _wrap_send_message(orig: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    @functools.wraps(orig)
    async def send_message(self: Any, content: Any = None, *args: Any, **kw: Any) -> Any:
        res = await orig(self, content, *args, **kw)
        if is_error(content, kw.get("embed"), kw.get("embeds")):
            inter = self._parent
            _schedule(("orig", inter.id), inter.delete_original_response)
        return res
    return send_message


def _wrap_webhook_send(orig: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    @functools.wraps(orig)
    async def send(self: Any, content: Any = discord.utils.MISSING, *args: Any, **kw: Any) -> Any:
        msg = await orig(self, content, *args, **kw)   # Webhook.send mặc định content=MISSING (≠ None)
        if msg is not None and is_error(content, kw.get("embed"), kw.get("embeds")):
            _schedule(("msg", msg.id), msg.delete)
        return msg
    return send


def _wrap_edit_original(orig: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    @functools.wraps(orig)
    async def edit_original_response(self: Any, **kw: Any) -> Any:
        msg = await orig(self, **kw)
        if is_error(kw.get("content"), kw.get("embed"), kw.get("embeds")):
            _schedule(("orig", self.id), self.delete_original_response)
        elif any(k in kw for k in ("content", "embed", "embeds")):
            _cancel(("orig", self.id))   # tin đã được sửa thành nội dung bình thường → đừng xoá nhầm
        return msg
    return edit_original_response


def _wrap_channel_send(orig: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    @functools.wraps(orig)
    async def send(self: Any, content: Any = None, *args: Any, **kw: Any) -> Any:
        msg = await orig(self, content, *args, **kw)
        if msg is not None and is_error(content, kw.get("embed"), kw.get("embeds")):
            _schedule(("msg", msg.id), msg.delete)
        return msg
    return send


def install() -> None:
    global _installed
    if _installed or DELAY <= 0:
        return
    _installed = True
    discord.InteractionResponse.send_message = _wrap_send_message(discord.InteractionResponse.send_message)  # type: ignore[method-assign]
    discord.Webhook.send = _wrap_webhook_send(discord.Webhook.send)  # type: ignore[method-assign]
    discord.Interaction.edit_original_response = _wrap_edit_original(discord.Interaction.edit_original_response)  # type: ignore[method-assign]
    discord.abc.Messageable.send = _wrap_channel_send(discord.abc.Messageable.send)  # type: ignore[method-assign]
    log.info("Tự xoá thông báo lỗi sau %.0fs đã bật.", DELAY)
