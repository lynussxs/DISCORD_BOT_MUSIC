"""
Giám sát + tự hồi phục cho bgutil-pot (server sinh PO-Token cho yt-dlp).

Vì sao cần: trước đây bot.py chỉ Popen() bgutil-pot MỘT lần lúc khởi động, đổ
stdout/stderr vào /dev/null và không bao giờ kiểm tra lại. Nếu process chết/treo
(hoặc cache integrity-token bị hỏng) thì client `web` mất PO-Token → mọi video
báo "Requested format is not available" cho tới khi cả bot được khởi động lại —
đúng hiện tượng "bài 2-3 lỗi, restart xong bài 4 lại được".

Module cung cấp:
  • start()  — chạy bgutil-pot, ghi stderr ra file (để biết vì sao nó chết), bật watchdog.
  • heal()   — gọi từ luồng resolve khi nghi lỗi PO-Token: process chết/không ping được
               → restart; còn sống → xoá cache token. Có throttle để không lạm dụng.
  • status() — "ok" / "dead" / "n/a" cho log chẩn đoán.
"""
from __future__ import annotations

import logging
import os
import subprocess
import threading
import time
import urllib.error
import urllib.request

log = logging.getLogger("bgutil")

_LOG_MAX_BYTES = 2 * 1024 * 1024


class PotProvider:
    def __init__(self, bin_path: str, cwd: str, port: int = 4416,
                 watch_interval: float = 30.0, heal_interval: float = 60.0) -> None:
        self.bin_path = bin_path
        self.cwd = cwd
        self.port = port
        self.base = f"http://127.0.0.1:{port}"
        self.watch_interval = watch_interval
        self.heal_interval = heal_interval
        self.log_path = os.path.join(cwd, "bgutil-pot.log")
        self._proc: subprocess.Popen | None = None
        self._lock = threading.RLock()
        self._last_heal = 0.0
        self._watch_started = False
        self.restarts = 0

    # ── process ───────────────────────────────────────────────────────────────
    def _spawn(self) -> None:
        # 'ab' (O_APPEND) để có thể truncate() file khi quá lớn mà process vẫn ghi đúng cuối file.
        logf = open(self.log_path, "ab")
        env = dict(os.environ)
        env.setdefault("RUST_LOG", "warn")
        self._proc = subprocess.Popen(
            [self.bin_path, "server", "--host", "127.0.0.1", "--port", str(self.port)],
            cwd=self.cwd, stdout=logf, stderr=subprocess.STDOUT, env=env,
        )
        logf.close()  # child giữ fd riêng

    def alive(self) -> bool:
        p = self._proc
        return p is not None and p.poll() is None

    def pid(self) -> int | None:
        return self._proc.pid if self._proc else None

    def _tail(self, n: int = 5) -> str:
        try:
            with open(self.log_path, "rb") as f:
                f.seek(0, os.SEEK_END)
                f.seek(max(0, f.tell() - 4096))
                lines = f.read().decode("utf-8", "ignore").strip().splitlines()
            return " | ".join(l.strip()[:200] for l in lines[-n:]) or "(trống)"
        except OSError:
            return "(không đọc được log)"

    def _trim_log(self) -> None:
        try:
            if os.path.getsize(self.log_path) > _LOG_MAX_BYTES:
                os.truncate(self.log_path, 0)
        except OSError:
            pass

    # ── HTTP ──────────────────────────────────────────────────────────────────
    def ping(self, timeout: float = 2.0) -> bool:
        try:
            with urllib.request.urlopen(f"{self.base}/ping", timeout=timeout) as r:
                return 200 <= r.status < 300
        except Exception:
            return False

    def _post(self, path: str, timeout: float = 3.0) -> bool:
        for method in ("POST", "GET"):   # tài liệu chỉ nêu tên endpoint → thử POST rồi GET nếu 405
            try:
                req = urllib.request.Request(f"{self.base}{path}", data=b"{}" if method == "POST" else None,
                                             method=method, headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=timeout) as r:
                    if 200 <= r.status < 300:
                        return True
            except urllib.error.HTTPError as e:
                if e.code == 405:
                    continue
                return False
            except Exception:
                return False
        return False

    def invalidate(self) -> bool:
        a = self._post("/invalidate_caches")
        b = self._post("/invalidate_it")
        return a or b

    # ── lifecycle ─────────────────────────────────────────────────────────────
    def _wait_ready(self, timeout: float) -> bool:
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if not self.alive():
                return False
            if self.ping(1.0):
                return True
            time.sleep(0.3)
        return False

    def restart(self, reason: str) -> bool:
        with self._lock:
            log.warning("POT | restart bgutil-pot (%s). stderr cuối: %s", reason, self._tail())
            p = self._proc
            if p is not None and p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    p.kill()
                    try:
                        p.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        pass
            try:
                self._spawn()
            except Exception as exc:  # noqa: BLE001
                log.warning("POT | không khởi động lại được bgutil-pot: %s", exc)
                return False
            self.restarts += 1
            ok = self._wait_ready(10.0)
            log.info("POT | bgutil-pot PID %s %s (lần restart thứ %d)", self.pid(),
                     "sẵn sàng" if ok else "CHƯA ping được sau 10s", self.restarts)
            return ok

    def start(self) -> None:
        with self._lock:
            self._spawn()
            ok = self._wait_ready(10.0)
            log.info("PO-Token provider (bgutil-pot, Rust) started — PID %s, %s%s",
                     self.pid(), self.base, "" if ok else " (chưa ping được — watchdog sẽ theo dõi)")
        if not self._watch_started:
            self._watch_started = True
            threading.Thread(target=self._watchdog, daemon=True, name="pot-watchdog").start()

    def _watchdog(self) -> None:
        fails = 0
        while True:
            time.sleep(self.watch_interval)
            try:
                if not self.alive():
                    code = self._proc.returncode if self._proc else None
                    self.restart(f"process đã thoát, code={code}")
                    fails = 0
                    continue
                if self.ping(3.0):
                    fails = 0
                else:
                    fails += 1
                    if fails >= 3:
                        self.restart("3 lần ping liên tiếp không trả lời")
                        fails = 0
                self._trim_log()
            except Exception:  # noqa: BLE001
                log.exception("POT watchdog lỗi")

    # ── dùng từ luồng resolve ─────────────────────────────────────────────────
    def heal(self, reason: str) -> str:
        """
        Blocking (gọi qua run_in_executor). Trả về:
          "throttled"      — vừa heal <heal_interval giây trước, không làm gì
          "restarted"      — process chết/không ping được và đã khởi động lại thành công
          "restart-failed" — khởi động lại nhưng chưa ping được
          "cache-cleared"  — process khoẻ, đã xoá cache token/integrity token
          "ping-ok"        — process khoẻ nhưng endpoint xoá cache không phản hồi
        """
        with self._lock:
            now = time.monotonic()
            if now - self._last_heal < self.heal_interval:
                return "throttled"
            self._last_heal = now
        if not self.alive() or not self.ping(3.0):
            return "restarted" if self.restart(f"heal: {reason}") else "restart-failed"
        return "cache-cleared" if self.invalidate() else "ping-ok"

    def status(self) -> str:
        if not self.alive():
            return "dead"
        return "ok" if self.ping(1.5) else "no-ping"


provider: PotProvider | None = None


def start(bin_path: str, cwd: str, port: int = 4416) -> PotProvider:
    global provider
    provider = PotProvider(bin_path, cwd, port)
    provider.start()
    return provider


def heal(reason: str) -> str:
    return provider.heal(reason) if provider else "n/a"


def status() -> str:
    return provider.status() if provider else "n/a"
