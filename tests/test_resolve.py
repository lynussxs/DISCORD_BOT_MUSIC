"""
Test offline (không cần mạng/YouTube): thứ tự chiến lược, breaker, chọn bài SoundCloud,
luồng fallback của Track.from_query. Chạy:  python -m pytest tests -q
"""
import asyncio
import os
import sys
import time
import types

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.environ.setdefault("BOT_TOKEN", "x")
os.environ.setdefault("DEV_GUILD_ID", "1")
os.environ.pop("WEBSHARE_CRED", None)

from cogs import music as m  # noqa: E402


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    m._breaker.__init__()
    m._resolve_cache.clear()
    m._search_cache.clear()
    m._diag_meta.clear()
    monkeypatch.setattr(m, "_cookies_valid", lambda: True)
    yield


# ── Không còn credentials/ client chết trong source ─────────────────────────────
def test_no_hardcoded_credentials_or_dead_clients():
    src = open(os.path.join(ROOT, "cogs", "music.py"), encoding="utf-8").read()
    for secret in ("fywznozi", "gv94cmc9t7qs", "c7593mkpjov5", "brd-customer"):
        assert secret not in src
    for st in m._YT_STRATEGIES:
        assert "tv_embedded" not in st["clients"] and "android" not in st["clients"]
    assert m.WebshareProxyManager.WEBSHARE_10 == []     # không có env → không có proxy cố định


def test_no_cookie_incompatible_mix():
    # client không hỗ trợ cookies không được trộn với strategy dùng cookies (yt-dlp sẽ bỏ qua chúng)
    no_cookie_clients = {"visionos", "android_vr", "ios", "android", "tv_simply"}
    for st in m._YT_STRATEGIES:
        if st["cookies"]:
            assert not (set(st["clients"]) & no_cookie_clients), st


def test_all_client_names_exist_in_installed_ytdlp():
    from yt_dlp.extractor.youtube._base import INNERTUBE_CLIENTS
    for st in m._YT_STRATEGIES:
        for c in st["clients"]:
            assert c in INNERTUBE_CLIENTS, f"{c} không tồn tại trong yt-dlp đang cài"


# ── Thứ tự chiến lược / breaker ────────────────────────────────────────────────
def test_plan_default_order_and_single_proxy_attempt():
    names = [s["name"] for s in m._breaker.plan()]
    assert names[0] == "samsung_ck"
    assert names.count("proxy") == 1 and names[-1] == "proxy"


def test_plan_promotes_recent_winner():
    m._breaker.ok("vision")
    assert m._breaker.plan()[0]["name"] == "vision"


def test_plan_demotes_cookie_strategies_when_cookies_invalid(monkeypatch):
    monkeypatch.setattr(m, "_cookies_valid", lambda: False)
    plan = m._breaker.plan()
    firsts = [s for s in plan[:3]]
    assert all(not s["cookies"] for s in firsts)


def test_breaker_trips_after_three_distinct_videos_and_skips():
    for vid in ("a" * 11, "b" * 11, "c" * 11):
        m._breaker.fail("web", vid)
    assert m._breaker.tripped("web")
    names = [s["name"] for s in m._breaker.plan()]
    assert names.index("web") > names.index("embed_vr")      # bị đẩy xuống cuối nhóm direct


# ── Chọn bài SoundCloud ────────────────────────────────────────────────────────
def E(title, dur, user="x", url="https://soundcloud.com/x/y"):
    return {"title": title, "duration": dur, "uploader": user, "url": url}


def test_sc_pick_matches_title_and_duration():
    es = [E("Charlie Puth - Attention (Official)", 208, "Charlie Puth"),
          E("Attention", 31, "random"),                       # preview 30s
          E("Charlie Puth Attention Remix", 300, "dj")]
    got = m._sc_pick(es, "Charlie Puth - Attention (Official Audio)", 208)
    assert got is es[0]


def test_sc_pick_rejects_wrong_length_and_low_overlap():
    es = [E("Charlie Puth - Attention", 400)]
    assert m._sc_pick(es, "Charlie Puth - Attention", 208) is None      # lệch 190s
    assert m._sc_pick([E("Totally different song", 208)], "Charlie Puth - Attention", 208) is None


def test_sc_pick_unknown_duration_skips_short_previews():
    es = [E("Attention Charlie Puth", 30), E("Attention Charlie Puth", 207)]
    assert m._sc_pick(es, "attention charlie puth", None) is es[1]


def test_sc_pick_empty_title():
    assert m._sc_pick([E("a b", 100)], "", 100) is None


# ── resolve: thứ tự thử + fallback ─────────────────────────────────────────────
class FakeData(dict):
    pass


def run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def test_resolve_stops_at_first_success(monkeypatch):
    calls = []

    def fake_extract(opts, url):
        calls.append(opts["extractor_args"]["youtube"]["player_client"])
        if len(calls) < 3:
            raise RuntimeError("ERROR: [youtube] x: Requested format is not available")
        return {"url": "https://rr.googlevideo.com/x", "title": "T", "id": "x" * 11}

    monkeypatch.setattr(m, "_extract_sync", fake_extract)
    monkeypatch.setattr(m._pot, "heal", lambda *a, **k: "ok")
    async def go():
        return await m._resolve_stream("https://www.youtube.com/watch?v=" + "x" * 11, asyncio.get_running_loop())
    data, proxy, err = run(go())
    assert data and err is None and proxy is None
    assert len(calls) == 3
    assert calls[0] == ["tv_samsung"]                 # TV client (không cần PO-Token) thử đầu tiên
    assert m._breaker.plan()[0]["name"] == [s["name"] for s in m._YT_STRATEGIES][2]  # người thắng được ưu tiên


def test_resolve_marks_unavailable_without_trying_everything(monkeypatch):
    n = []
    def fake_extract(opts, url):
        n.append(1)
        raise RuntimeError("ERROR: [youtube] x: Video unavailable")
    monkeypatch.setattr(m, "_extract_sync", fake_extract)
    async def go():
        return await m._resolve_stream("https://www.youtube.com/watch?v=" + "y" * 11, asyncio.get_running_loop())
    with pytest.raises(RuntimeError):
        run(go())
    assert len(n) == 1


def _patch_all_yt_fail(monkeypatch, sc_entries=None, sc_stream=None, diag=("sum", "")):
    seen = {"sc": []}

    def fake_extract(opts, url):
        if url.startswith("scsearch"):
            seen["sc"].append(url)
            return {"entries": sc_entries or []}
        if "soundcloud.com" in url:
            if sc_stream is None:
                raise RuntimeError("sc boom")
            return dict(sc_stream)
        raise RuntimeError("ERROR: [youtube] x: Sign in to confirm you’re not a bot")

    async def fake_diag(video_url, loop):
        m._diag_meta[video_url] = {"title": "Charlie Puth - Attention", "duration": 208, "uploader": "Charlie Puth"}
        return diag

    monkeypatch.setattr(m, "_extract_sync", fake_extract)
    monkeypatch.setattr(m, "_diagnose_failure", fake_diag)
    monkeypatch.setattr(m._pot, "heal", lambda *a, **k: "ok")
    monkeypatch.setattr(m, "_RESOLVE_TOTAL_BUDGET", 0.0)       # chỉ thử 1 chiến lược cho nhanh
    return seen


class FakeUser:
    id = 1
    display_name = "u"


def test_from_query_falls_back_to_soundcloud(monkeypatch):
    sc = {"url": "https://cf-media.sndcdn.com/a.mp3", "title": "Attention", "duration": 208,
          "uploader": "Charlie Puth", "webpage_url": "https://soundcloud.com/cp/attention", "id": "1"}
    seen = _patch_all_yt_fail(monkeypatch, sc_entries=[E("Charlie Puth - Attention", 208, "Charlie Puth")], sc_stream=sc)
    async def go():
        return await m.Track.from_query("https://youtu.be/" + "z" * 11, FakeUser(), asyncio.get_running_loop())
    t = run(go())
    assert t.url.endswith("a.mp3") and t.uploader.endswith("SoundCloud") and not t._url_via_proxy
    assert seen["sc"] and seen["sc"][0].startswith("scsearch8:")


def test_from_query_raises_original_error_when_no_match(monkeypatch):
    _patch_all_yt_fail(monkeypatch, sc_entries=[E("Unrelated", 50)])
    async def go():
        return await m.Track.from_query("https://youtu.be/" + "w" * 11, FakeUser(), asyncio.get_running_loop())
    with pytest.raises(RuntimeError, match="Sign in to confirm"):
        run(go())


def test_from_query_sc_disabled(monkeypatch):
    seen = _patch_all_yt_fail(monkeypatch, sc_entries=[E("Charlie Puth - Attention", 208)])
    monkeypatch.setattr(m, "_SC_ENABLED", False)
    async def go():
        return await m.Track.from_query("https://youtu.be/" + "q" * 11, FakeUser(), asyncio.get_running_loop())
    with pytest.raises(RuntimeError):
        run(go())
    assert seen["sc"] == []


def test_format_selector_never_prefers_big_muxed_video():
    assert m._YT_FORMAT.startswith("bestaudio/18/")
    assert "best[height<=480]" in m._YT_FORMAT
