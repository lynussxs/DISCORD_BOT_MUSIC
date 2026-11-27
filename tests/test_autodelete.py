import asyncio
import discord
from utils import autodelete as ad


def run(c): return asyncio.run(c)

ERR = discord.Embed(title="❌  Error", description="x")
OK = discord.Embed(title="🎵  Now Playing")


def test_is_error():
    assert ad.is_error(embed=ERR) and ad.is_error(embeds=[OK, ERR]) and ad.is_error("❌ oops")
    assert not ad.is_error(embed=OK) and not ad.is_error("hello") and not ad.is_error()


class Msg:
    def __init__(self, i): self.id = i; self.deleted = False
    async def delete(self): self.deleted = True


class Inter:
    id = 7
    def __init__(self): self.deleted = False; self.edits = []
    async def delete_original_response(self): self.deleted = True
    async def edit_original_response(self, **kw): self.edits.append(kw); return Msg(1)


def test_send_message_followup_channel_and_edit(monkeypatch):
    monkeypatch.setattr(ad, "DELAY", 0.05)
    async def go():
        inter = Inter()
        class Resp:
            _parent = inter
            async def send_message(self, content=None, **kw): return "r"
        r = ad._wrap_send_message(Resp.send_message)
        await r(Resp(), embed=ERR, ephemeral=True)
        await r(Resp(), embed=OK)
        await asyncio.sleep(0.15)
        assert inter.deleted
        # followup / channel: xoá đúng tin lỗi, giữ tin thường
        sent = []
        async def wsend(self, content=discord.utils.MISSING, **kw): m = Msg(len(sent) + 10); sent.append(m); return m
        w = ad._wrap_webhook_send(wsend)
        await w(None, embed=ERR); await w(None, embed=OK); await w(None, "❌ text")
        await asyncio.sleep(0.15)
        assert [m.deleted for m in sent] == [True, False, True]
        # edit_original_response: lỗi → xoá; sau đó sửa thành nội dung thường → huỷ xoá
        i2 = Inter(); i2.id = 99
        e = ad._wrap_edit_original(Inter.edit_original_response)
        await e(i2, embed=ERR)
        await e(i2, embed=OK)
        await asyncio.sleep(0.15)
        assert not i2.deleted
        i3 = Inter(); i3.id = 100
        await e(i3, embed=ERR)
        await asyncio.sleep(0.15)
        assert i3.deleted
    run(go())


def test_delete_errors_swallowed(monkeypatch):
    monkeypatch.setattr(ad, "DELAY", 0.02)
    async def go():
        class Bad(Msg):
            async def delete(self): raise discord.NotFound(type("R", (), {"status": 404, "reason": ""})(), "gone")
        async def send(self, content=None, **kw): return Bad(5)
        s = ad._wrap_channel_send(send)
        await s(None, embed=ERR)
        await asyncio.sleep(0.1)
    run(go())
