"""
Test _ChunkedHTTP với ffmpeg THẬT + server giả giống googlevideo ở điểm quan trọng: chỉ nhận Range
CÓ GIỚI HẠN (như log 21/11: ffmpeg mở "bytes=0-" → 403, yt-dlp tải theo khúc → 206).
"""
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.environ.setdefault("BOT_TOKEN", "x")
os.environ.setdefault("DEV_GUILD_ID", "1")

from cogs import music as m  # noqa: E402

pytestmark = pytest.mark.skipif(shutil.which("ffmpeg") is None, reason="cần ffmpeg")
SECONDS = 20


@pytest.fixture(scope="module")
def mp4(tmp_path_factory):
    out = str(tmp_path_factory.mktemp("media") / "t.mp4")
    r = subprocess.run(
        ["ffmpeg", "-v", "error", "-f", "lavfi", "-i", f"sine=frequency=440:duration={SECONDS}",
         "-f", "lavfi", "-i", f"testsrc=duration={SECONDS}:size=480x360:rate=25",
         "-c:a", "aac", "-b:a", "96k", "-c:v", "mpeg4", "-b:v", "400k", "-movflags", "+faststart", "-y", out],
        capture_output=True)
    if r.returncode != 0:
        pytest.skip("ffmpeg không tạo được mp4 thử: " + r.stderr.decode()[-200:])
    return out


class _Srv:
    def __init__(self, path, mode="strict"):
        self.data = open(path, "rb").read()
        self.log = []          # (range_header, status)
        outer = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_GET(self):
                rng = self.headers.get("Range")
                n = len(outer.data)
                status, body, extra = 200, b"", {}
                if mode == "deny":
                    status = 403
                else:
                    mt = re.fullmatch(r"bytes=(\d+)-(\d*)", rng or "")
                    if not mt or not mt.group(2) or int(mt.group(2)) - int(mt.group(1)) + 1 > 10 << 20:
                        status = 403            # thiếu Range / mở đầu "bytes=0-" / khúc quá lớn
                    else:
                        a, b = int(mt.group(1)), min(int(mt.group(2)), n - 1)
                        if a >= n:
                            status = 416
                        else:
                            status, body = 206, outer.data[a:b + 1]
                            extra = {"Content-Range": f"bytes {a}-{b}/{n}"}
                outer.log.append((rng, status))
                self.send_response(status)
                self.send_header("Content-Length", str(len(body)))
                for k, v in extra.items():
                    self.send_header(k, v)
                self.end_headers()
                self.wfile.write(body)

        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.url = f"http://127.0.0.1:{self.srv.server_port}/v.mp4"
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    def close(self):
        self.srv.shutdown()


def _read_pcm(pcm, seconds):
    got = 0
    while got < seconds * 192000:
        b = pcm.read()
        if not b:
            break
        got += len(b)
    return got


def test_ffmpeg_direct_gets_403_on_bounded_range_only_server(mp4):
    s = _Srv(mp4)
    try:
        r = subprocess.run(["ffmpeg", "-v", "warning", "-i", s.url, "-vn", "-f", "null", "-"],
                           capture_output=True, timeout=30)
        assert r.returncode != 0 and b"403" in r.stderr          # tái hiện đúng triệu chứng trong log
    finally:
        s.close()


def test_chunked_reader_plays_through_ffmpeg(mp4):
    s = _Srv(mp4)
    errf = tempfile.TemporaryFile()
    try:
        feeder = m._ChunkedHTTP(s.url, {"User-Agent": "x"}, None, errf)
        pcm = m._new_pcm_chunked(feeder, "-analyzeduration 1000000 -probesize 96k", "-vn", errf)
        got = _read_pcm(pcm, SECONDS)
        pcm.cleanup()
        assert got >= 0.85 * SECONDS * 192000, got                # giải mã gần đủ độ dài
        assert s.log and all(st in (206, 416) for _, st in s.log), s.log
        assert all(re.fullmatch(r"bytes=\d+-\d+", r) for r, _ in s.log)
        assert not feeder.failed
    finally:
        s.close()


def test_chunked_reader_seek(mp4):
    s = _Srv(mp4)
    errf = tempfile.TemporaryFile()
    try:
        feeder = m._ChunkedHTTP(s.url, None, None, errf)
        pcm = m._new_pcm_chunked(feeder, "-ss 10 -analyzeduration 1000000 -probesize 96k", "-vn", errf)
        got = _read_pcm(pcm, SECONDS)
        pcm.cleanup()
        assert 0.4 * 10 * 192000 < got < 1.2 * 10 * 192000, got   # ~10s còn lại
    finally:
        s.close()


def test_chunked_403_is_reported_like_ffmpeg(mp4):
    s = _Srv(mp4, mode="deny")
    errf = tempfile.TemporaryFile()
    try:
        feeder = m._ChunkedHTTP(s.url, None, None, errf)
        assert feeder.read(8192) == b"" and feeder.failed
        errf.seek(0)
        assert b"403 Forbidden" in errf.read()                   # nhánh STREAM DROP 403 nhận ra
    finally:
        feeder.close()
        s.close()


def _track(url, protocol="https"):
    return m.Track({"title": "t", "url": url, "id": "x" * 11, "protocol": protocol}, requester=None)


def test_needs_chunked_only_for_risky_urls(monkeypatch):
    base = "https://rr1---sn-x.googlevideo.com/videoplayback?itag=18&c=ANDROID_VR&ip=1.2.3.4"
    assert _track(base).needs_chunked
    assert not _track(base + "&pot=abc").needs_chunked                    # đã có PO-Token
    assert not _track(base, "m3u8_native").needs_chunked                  # HLS: ffmpeg tự đọc theo segment
    assert not _track(base.replace("ANDROID_VR", "TVHTML5")).needs_chunked  # client không cần pot
    monkeypatch.setenv("CHUNKED_READ", "0")
    assert not _track(base).needs_chunked
