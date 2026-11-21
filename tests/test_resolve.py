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
    m._breaker.__init__(); m._yt_health.__init__()
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
    assert names[0] == "vision"          # log 21/11: chỉ visionos thắng (không cookies/PO-Token/Deno)
    assert names.index("web") > names.index("samsung_ck")   # web (SABR, tốn RAM) không đứng đầu
    assert names.count("proxy") == 1
    # web/web_nock ("late") chỉ chạy SAU proxy: chưa thắng lần nào, lại làm RAM vọt
    assert names.index("proxy") < names.index("web") and names.index("proxy") < names.index("web_nock")


def test_plan_promotes_recent_winner():
    m._breaker.ok("vision")
    assert m._breaker.plan()[0]["name"] == "vision"


def test_plan_demotes_cookie_strategies_when_cookies_invalid(monkeypatch):
    monkeypatch.setattr(m, "_cookies_valid", lambda: False)
    plan = m._breaker.plan()
    firsts = [s for s in plan[:2]]
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
    assert calls[0] == ["visionos"]                   # vision thử đầu tiên
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


# ── SoundCloud: phải ép progressive mp3, không để yt-dlp chọn hls_opus ─────────
def test_soundcloud_requests_progressive_mp3(monkeypatch):
    seen = {}
    def fake_extract(opts, url):
        if url.startswith("scsearch"):
            return {"entries": [E("Charlie Puth - Attention", 208, "Charlie Puth")]}
        seen["format"] = opts["format"]
        return {"url": "https://cf-media.sndcdn.com/a.mp3", "title": "Attention", "duration": 208}
    monkeypatch.setattr(m, "_extract_sync", fake_extract)
    async def go():
        return await m._soundcloud_resolve("Charlie Puth - Attention", 208, asyncio.get_running_loop())
    assert run(go())
    fmt = seen["format"]
    assert fmt.index("http_mp3_128") < fmt.index("bestaudio/best")
    assert fmt.startswith("bestaudio[protocol^=http]")


# ── YouTube chết liên tiếp → SoundCloud trước, thăm dò YouTube định kỳ ─────────
def test_yt_health_sc_first_and_probe():
    h = m._YtHealth()
    assert not h.sc_first()
    h.fail(); assert not h.sc_first()
    h.fail(); assert h.sc_first()
    h.probe_at -= m._YtHealth.PROBE_EVERY + 1
    assert not h.sc_first()          # đến hạn thăm dò → thử YouTube
    assert h.sc_first()              # xong lượt thăm dò → lại SC trước
    h.ok(); assert not h.sc_first()


def test_from_query_sc_first_skips_youtube_chain(monkeypatch):
    m._yt_health.__init__()
    m._yt_health.fails = 2
    m._yt_health.probe_at = time.monotonic()
    yt_calls = []
    def fake_extract(opts, url):
        if url.startswith("scsearch"):
            return {"entries": [E("Charlie Puth - Attention", 208, "Charlie Puth")]}
        if "soundcloud.com" in url:
            return {"url": "https://cf-media.sndcdn.com/a.mp3", "title": "Attention", "duration": 208}
        yt_calls.append(url)
        raise RuntimeError("ERROR: [youtube] x: Sign in to confirm you’re not a bot")
    monkeypatch.setattr(m, "_extract_sync", fake_extract)
    async def fake_diag(v, l):
        return ("s", "")
    monkeypatch.setattr(m, "_diagnose_failure", fake_diag)
    async def go():
        q = "Charlie Puth - Attention"
        m._search_cache["ytsearch1:" + q] = (time.monotonic(), "https://www.youtube.com/watch?v=" + "a" * 11)
        return await m.Track.from_query(q, FakeUser(), asyncio.get_running_loop())
    t = run(go())
    assert t.url.endswith("a.mp3") and yt_calls == []
    m._yt_health.__init__()


# ── HLS: không dùng reconnect_at_eof/streamed (stall ~3s lúc mở playlist, đo bằng ffmpeg thật) ──
def test_hls_drops_eof_reconnect_but_progressive_keeps_it():
    hls = m._ffmpeg_before(no_proxy=True, hls=True)
    prog = m._ffmpeg_before(no_proxy=True)
    assert "reconnect_at_eof" not in hls and "reconnect_streamed" not in hls
    assert "-reconnect 1" in hls and "reconnect_on_network_error" in hls
    assert "-reconnect_at_eof 1" in prog and "-reconnect_streamed 1" in prog


def test_is_hls():
    assert m._is_hls("https://manifest.googlevideo.com/api/manifest/hls_playlist/id/x/index.m3u8")
    assert not m._is_hls("https://rr1---sn.googlevideo.com/videoplayback?id=1")
    assert not m._is_hls(None)


# ── Prewarm: ffmpeg khởi động trước, take_source dùng lại; discard dọn sạch ─────
def test_prewarm_is_reused_and_discarded(monkeypatch):
    cleaned = []
    class Src:
        volume = 1.0
        def cleanup(self): cleaned.append(1)
    made = []
    async def fake_make(self, volume, seek=0):
        made.append(volume); return Src()
    monkeypatch.setattr(m.Track, "make_source", fake_make)
    t = m.Track.__new__(m.Track)
    async def go():
        t.start_prewarm(0.5); t.start_prewarm(0.5)        # gọi 2 lần chỉ tạo 1 nguồn
        await asyncio.sleep(0)
        src = await t.take_source(0.8)
        assert src.volume == 0.8 and made == [0.5]
        assert await t.take_source(0.3) is not None and made == [0.5, 0.3]   # không prewarm → tạo mới
        t.start_prewarm(0.5); await asyncio.sleep(0.01)
        t.discard_prewarm()
        assert cleaned == [1]                               # nguồn dựng sẵn bị dọn, không rò ffmpeg
    run(go())


# ── Bot-check ở chiến lược trực tiếp → nhảy thẳng sang proxy (log 5GG_MkQYQq0 / qe7stKL5qco) ──
def test_botcheck_on_direct_jumps_straight_to_proxy(monkeypatch):
    calls = []
    def fake_extract(opts, url):
        clients = opts["extractor_args"]["youtube"]["player_client"]
        calls.append(clients)
        if len(clients) == 4:                      # chiến lược proxy
            return {"url": "https://x/a", "title": "t", "duration": 100}
        raise RuntimeError("ERROR: [youtube] x: Sign in to confirm you’re not a bot.")
    monkeypatch.setattr(m, "_extract_sync", fake_extract)
    async def go():
        return await m._resolve_stream("https://www.youtube.com/watch?v=" + "y" * 11, asyncio.get_running_loop())
    data, proxy, err = run(go())
    assert data and err is None
    assert calls[0] == ["visionos"]
    assert 1 <= len(calls) - 1 <= m._PROXY_RACE_WIDTH            # chỉ chiến lược proxy (đua ≤2 IP), không 7 lần
    assert all(len(c) == 4 for c in calls[1:])


def test_botcheck_then_proxy_fail_continues_with_remaining(monkeypatch):
    calls = []
    def fake_extract(opts, url):
        clients = opts["extractor_args"]["youtube"]["player_client"]
        calls.append(clients)
        if clients == ["tv_samsung"]:
            return {"url": "https://x/a", "title": "t", "duration": 100}
        raise RuntimeError("ERROR: [youtube] x: Sign in to confirm you’re not a bot.")
    monkeypatch.setattr(m, "_extract_sync", fake_extract)
    async def go():
        return await m._resolve_stream("https://www.youtube.com/watch?v=" + "z" * 11, asyncio.get_running_loop())
    data, proxy, err = run(go())
    assert data                                    # proxy hỏng → vẫn thử tiếp samsung_ck và thắng
    assert calls[1] and len(calls[1]) == 4         # nhảy proxy ngay sau vision
    assert ["tv_samsung"] in calls


# ── Đua nhiều proxy + bỏ chiến lược vô ích khi bot-check theo IP ───────────────────────────────
def test_proxy_pick_returns_distinct_and_rotates():
    pm = m.WebshareProxyManager()
    pm._proxies = ["http://a:1", "http://b:2", "http://c:3"]
    first = pm.pick(2)
    assert len(set(first)) == 2
    assert pm.pick(2, exclude=set(first)) == [u for u in pm._proxies if u not in first]   # loại IP đã thử
    pm._dead_until[first[0]] = time.monotonic() + 60
    assert first[0] not in pm.pick(3)


def test_proxy_race_second_ip_wins_when_first_is_botchecked(monkeypatch):
    seen = []
    def fake_extract(opts, url):
        px = opts["proxy"]; seen.append(px)
        if px in ("http://bad1:1", "http://bad2:2"):
            raise RuntimeError("ERROR: [youtube] x: Sign in to confirm you’re not a bot.")
        return {"url": "https://rr.googlevideo.com/x?c=TVHTML5", "title": "t", "format_id": "251"}
    monkeypatch.setattr(m, "_extract_sync", fake_extract)
    monkeypatch.setattr(m._proxy, "_proxies", ["http://bad1:1", "http://bad2:2", "http://ok:3", "http://ok:4"])
    monkeypatch.setattr(m._proxy, "_idx", 0); monkeypatch.setattr(m._proxy, "_fetch_api", lambda: None)
    m._proxy._dead.clear(); m._proxy._dead_until.clear()
    async def go():
        opts = {"extractor_args": {"youtube": {}}}
        d = await m._proxy_race(opts, "https://www.youtube.com/watch?v=" + "q" * 11,
                                asyncio.get_running_loop(), time.monotonic())
        return d, opts
    d, opts = run(go())
    assert opts["proxy"].startswith("http://ok:") and d["format_id"] == "251"     # vòng 2 (IP mới) thắng
    assert "bad1" in " ".join(seen) and m._proxy._dead_until.get("http://bad1:1")  # IP dính bot-check bị tạm khoá


def test_botcheck_everywhere_skips_useless_direct_strategies(monkeypatch):
    calls = []
    def fake_extract(opts, url):
        calls.append(opts["extractor_args"]["youtube"]["player_client"])
        raise RuntimeError("ERROR: [youtube] x: Sign in to confirm you’re not a bot.")
    monkeypatch.setattr(m, "_extract_sync", fake_extract)
    async def go():
        return await m._resolve_stream("https://www.youtube.com/watch?v=" + "w" * 11, asyncio.get_running_loop())
    data, proxy, err = run(go())
    assert data is None and err is not None
    flat = [c for cl in calls for c in cl]
    assert "web" not in flat and "mweb" not in flat and "android_vr" not in [c for cl in calls if len(cl) == 2 for c in cl]


def test_win_info_reads_client_from_url():
    assert "client=IOS" in m._win_info({"url": "https://rr.googlevideo.com/v?c=IOS&x=1", "format_id": "18"})
    assert "client=?" in m._win_info({"url": "https://a/b.m3u8"})


# ── Probe URL thiếu PO-Token (log 75Tqdnwitgw: ANDROID_VR itag 18 qua proxy → ffmpeg 403 ×6) ──────
def test_android_vr_url_without_pot_403_retries_with_safe_clients(monkeypatch):
    calls = []
    def fake_extract(opts, url):
        cl = opts["extractor_args"]["youtube"]["player_client"]
        calls.append(list(cl))
        c = "ANDROID_VR" if "android_vr" in cl else "TVHTML5"
        return {"url": f"https://rr.googlevideo.com/x?c={c}", "title": "t", "format_id": "18"}
    probes = []
    monkeypatch.setattr(m, "_extract_sync", fake_extract)
    monkeypatch.setattr(m, "_probe_sync", lambda d, px: (probes.append(d["url"]), "ANDROID_VR" not in d["url"])[1])
    async def go():
        o = {"extractor_args": {"youtube": {"player_client": ["tv_samsung", "visionos", "web_embedded", "android_vr"]}}}
        return await m._extract_verified(o, "https://www.youtube.com/watch?v=" + "p" * 11, asyncio.get_running_loop())
    d = run(go())
    assert "TVHTML5" in d["url"] and calls[1] == ["tv_samsung", "visionos", "web_embedded"]


def test_url_with_pot_or_safe_client_is_not_probed():
    assert not m._needs_probe({"url": "https://a/b?c=ANDROID_VR&pot=abc"})
    assert not m._needs_probe({"url": "https://a/b?c=TVHTML5"})
    assert not m._needs_probe({"url": "https://a/b.m3u8"})
    assert m._needs_probe({"url": "https://a/b?c=ANDROID_VR"})


def test_refresh_url_raises_instead_of_reusing_dead_url(monkeypatch):
    t = m.Track({"url": "https://old/403", "title": "t", "webpage_url": "https://www.youtube.com/watch?v=" + "r" * 11,
                 "duration": 100}, None, via_proxy=True)
    def fake_extract(opts, url):
        raise RuntimeError("ERROR: [youtube] x: Sign in to confirm you’re not a bot.")
    monkeypatch.setattr(m, "_extract_sync", fake_extract)
    async def go():
        await t.refresh_url(asyncio.get_running_loop(), force=True)
    with pytest.raises(Exception):
        run(go())
    assert t.url == "https://old/403"


# ── Client mới + chọn lỗi gốc ───────────────────────────────────────────────────────────────
def test_new_strategies_use_real_clients_and_order():
    names = [s["name"] for s in m._breaker.plan()]
    assert names.index("tv_ck") > names.index("samsung_ck") and names.index("tv_ck") < names.index("proxy")
    assert names.index("safari_ck") > names.index("proxy")
    st = {s["name"]: s for s in m._YT_STRATEGIES}
    assert st["tv_ck"]["clients"] == ["tv"] and st["safari_ck"]["clients"] == ["web_safari"] and st["safari_ck"]["heavy"]


def test_final_error_is_the_botcheck_not_the_secondary_error(monkeypatch):
    def fake_extract(opts, url):
        cl = opts["extractor_args"]["youtube"]["player_client"]
        if cl == ["visionos"] or len(cl) == 4:
            raise RuntimeError("ERROR: [youtube] x: Sign in to confirm you’re not a bot.")
        raise RuntimeError("ERROR: [youtube] x: The page needs to be reloaded.")
    monkeypatch.setattr(m, "_extract_sync", fake_extract)
    monkeypatch.setattr(m._mem, "pressure", lambda: 0.1)
    async def go():
        return await m._resolve_stream("https://www.youtube.com/watch?v=" + "b" * 11, asyncio.get_running_loop())
    data, proxy, err = run(go())
    assert data is None and "sign in to confirm" in str(err).lower()


def test_heavy_strategy_skipped_under_memory_pressure(monkeypatch):
    seen = []
    def fake_extract(opts, url):
        seen.append(opts["extractor_args"]["youtube"]["player_client"])
        raise RuntimeError("ERROR: [youtube] x: Requested format is not available")
    monkeypatch.setattr(m, "_extract_sync", fake_extract)
    monkeypatch.setattr(m._mem, "pressure", lambda: 0.9)
    monkeypatch.setattr(m._pot, "heal", lambda *a, **k: "ok")
    async def go():
        return await m._resolve_stream("https://www.youtube.com/watch?v=" + "h" * 11, asyncio.get_running_loop())
    run(go())
    assert ["web_safari"] not in seen and ["tv"] in seen


# ── Bản upload thay thế khi video bị bot-check ở mọi chiến lược ────────────────────────────────
def test_alt_upload_skips_bad_id_and_tries_next_candidate(monkeypatch):
    tried = []
    def fake_extract(opts, url):
        if url.startswith("ytsearch8:"):
            return {"entries": [
                {"id": "BAD" + "0" * 8, "title": "X-COOL (1HOUR) tienanh109", "duration": 3600, "uploader": "a"},
                {"id": "A" * 11, "title": "X-COOL 1HOUR tienanh109 hailong140", "duration": 3610, "uploader": "b"},
                {"id": "B" * 11, "title": "X-COOL 1HOUR tienanh109 hailong140 HDN", "duration": 3598, "uploader": "c"},
                {"id": "C" * 11, "title": "Another thing entirely", "duration": 3600, "uploader": "d"},
            ]}
        tried.append((url[-11:], opts["extractor_args"]["youtube"]["player_client"]))
        if url.endswith("A" * 11):
            raise RuntimeError("ERROR: [youtube] x: Sign in to confirm you’re not a bot.")
        return {"url": "https://x/hls.m3u8", "title": "ok", "id": "B" * 11}
    monkeypatch.setattr(m, "_extract_sync", fake_extract)
    async def go():
        return await m._alt_upload_resolve("X-COOL (1HOUR) - tienanh109,hailong140,HDN", 3600, "BAD" + "0" * 8,
                                           asyncio.get_running_loop())
    d = run(go())
    assert d and d["id"] == "B" * 11
    assert all(c == ["visionos"] for _, c in tried) and not any(u.startswith("BAD") for u, _ in tried)


def test_alt_upload_none_when_nothing_matches_or_disabled(monkeypatch):
    monkeypatch.setattr(m, "_extract_sync", lambda o, u: {"entries": [{"id": "D" * 11, "title": "zzz qqq", "duration": 50}]})
    async def go(): return await m._alt_upload_resolve("Charlie Puth Attention", 208, "x" * 11, asyncio.get_running_loop())
    assert run(go()) is None
    monkeypatch.setattr(m, "_ALT_UPLOAD_ENABLED", False)
    assert run(go()) is None


def test_from_query_uses_alt_upload_before_soundcloud(monkeypatch):
    seen = _patch_all_yt_fail(monkeypatch, sc_entries=[E("Charlie Puth - Attention", 208, "Charlie Puth")],
                              sc_stream={"url": "https://x/a.mp3", "title": "t", "duration": 208, "id": "1"})
    async def fake_alt(title, dur, bad, loop):
        return {"url": "https://x/alt.m3u8", "title": "Charlie Puth - Attention", "duration": 208, "id": "ALTALTALTAL"}
    monkeypatch.setattr(m, "_alt_upload_resolve", fake_alt)
    async def go():
        return await m.Track.from_query("https://youtu.be/" + "z" * 11, FakeUser(), asyncio.get_running_loop())
    t = run(go())
    assert t.url.endswith("alt.m3u8") and not seen["sc"]       # chưa cần tới SoundCloud


# ── Nhớ proxy vừa thắng + working-set RAM (không tính page cache) ───────────────────────────────
def test_pick_puts_recent_winner_first_and_forgets_it_when_dead():
    pm = m.WebshareProxyManager()
    pm._proxies = ["http://a:1", "http://b:2", "http://c:3"]
    pm._fetch_api = lambda: None
    pm.note_good("http://c:3")
    assert pm.pick(2)[0] == "http://c:3"
    pm.mark_dead("http://c:3", temporary=True)
    assert "http://c:3" not in pm.pick(3) and pm._last_good is None
    pm._dead_until.clear(); pm._idx = 1
    pm.note_good("http://a:1"); pm._last_good = ("http://a:1", time.monotonic() - 4000)   # hết hạn 30 phút
    assert pm.pick(1) == ["http://b:2"]                    # không ưu tiên a nữa, đi theo thứ tự xoay thường


def test_memstat_pressure_excludes_reclaimable_page_cache(monkeypatch):
    from utils import memstat
    files = {
        "/sys/fs/cgroup/memory.current": "497000000\n",       # ≈474MB (có cache)
        "/sys/fs/cgroup/memory.max": "511705088\n",           # 488MB
        "/sys/fs/cgroup/memory.events": "oom_kill 0\n",
        "/sys/fs/cgroup/memory.stat": "anon 200000000\ninactive_file 290000000\n",
    }
    monkeypatch.setattr(memstat, "_read", lambda path: files.get(path))
    assert memstat.cgroup()["used"] == 497000000
    assert 0.38 < memstat.pressure() < 0.42                # ~207MB/488MB, không phải 97%
    assert "ws=" in memstat.snapshot()


# ── Bỏ qua direct khi bị bot-check liên tiếp ───────────────────────────────────
def test_skip_direct_after_streak_and_reprobe(monkeypatch):
    b = m._breaker
    assert not b.skip_direct()
    for _ in range(3):
        b.note_direct(True)
    assert b.skip_direct()
    b._direct_bc_at -= b.DIRECT_REPROBE + 1          # quá hạn → thăm dò lại direct
    assert not b.skip_direct()
    b.note_direct(True)
    assert b.skip_direct()
    b.note_direct(False)                              # direct thắng → reset
    assert not b.skip_direct()


def test_resolve_goes_straight_to_proxy_when_direct_blocked(monkeypatch):
    for _ in range(3):
        m._breaker.note_direct(True)
    order = []

    async def fake_race(opts, url, loop, t0):
        order.append("proxy")
        return {"url": "https://x/a", "title": "T", "id": "x" * 11}

    def fake_extract(opts, url):
        order.append("direct")
        raise RuntimeError("never")

    monkeypatch.setattr(m, "_proxy_race", fake_race)
    monkeypatch.setattr(m, "_extract_sync", fake_extract)
    async def go():
        return await m._resolve_stream("https://www.youtube.com/watch?v=" + "k" * 11, asyncio.get_running_loop())
    data, _, err = run(go())
    assert data and err is None and order == ["proxy"]
