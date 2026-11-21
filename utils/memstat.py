"""
Đo bộ nhớ của container (cgroup) và từng process — để biết Deno/ffmpeg có bị OOM-kill không.

Bối cảnh: log DIAG cho thấy yt-dlp báo `Error running deno process (returncode: -9)` →
Deno (bộ giải n-challenge) bị SIGKILL từ bên ngoài, khiến client `web` mất hết format
("Requested format is not available"). SIGKILL từ ngoài thường là OOM-killer của container.
Module này ghi lại: mức dùng/giới hạn cgroup, số lần oom_kill, RSS theo tên process.
"""
from __future__ import annotations

import ctypes
import gc
import os

_MB = 1024 * 1024


def _read(path: str) -> str | None:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read().strip()
    except OSError:
        return None


def _int(s: str | None) -> int | None:
    try:
        return int(s) if s is not None else None
    except ValueError:
        return None


def _inactive_file() -> int | None:
    """Page cache không dùng tới (thu hồi được trước khi OOM). cgroup v2: memory.stat 'inactive_file'; v1: 'total_inactive_file'."""
    for path, key in (("/sys/fs/cgroup/memory.stat", "inactive_file"),
                      ("/sys/fs/cgroup/memory/memory.stat", "total_inactive_file")):
        txt = _read(path)
        if not txt:
            continue
        for line in txt.splitlines():
            parts = line.split()
            if len(parts) == 2 and parts[0] == key:
                return _int(parts[1])
    return None


def cgroup() -> dict[str, int | None]:
    """{'used','limit','oom_kill'} tính bằng byte (None nếu không có). Hỗ trợ cgroup v2 rồi v1."""
    used = _int(_read("/sys/fs/cgroup/memory.current"))
    limit_raw = _read("/sys/fs/cgroup/memory.max")
    oom = None
    ev = _read("/sys/fs/cgroup/memory.events")
    if used is not None:
        limit = None if limit_raw in (None, "max") else _int(limit_raw)
        for line in (ev or "").splitlines():
            if line.startswith("oom_kill "):
                oom = _int(line.split()[1])
        return {"used": used, "limit": limit, "oom_kill": oom}
    # cgroup v1
    used = _int(_read("/sys/fs/cgroup/memory/memory.usage_in_bytes"))
    limit = _int(_read("/sys/fs/cgroup/memory/memory.limit_in_bytes"))
    if limit is not None and limit >= (1 << 60):   # v1 dùng số rất lớn nghĩa là "không giới hạn"
        limit = None
    for line in (_read("/sys/fs/cgroup/memory/memory.oom_control") or "").splitlines():
        if line.startswith("oom_kill "):
            oom = _int(line.split()[1])
    return {"used": used, "limit": limit, "oom_kill": oom}


def procs() -> list[tuple[str, int]]:
    """[(tên process, tổng RSS MB)] giảm dần — gộp theo tên (nhiều ffmpeg/python → cộng lại)."""
    agg: dict[str, int] = {}
    try:
        pids = [p for p in os.listdir("/proc") if p.isdigit()]
    except OSError:
        return []
    for pid in pids:
        st = _read(f"/proc/{pid}/status")
        if not st:
            continue
        name, rss_kb = None, 0
        for line in st.splitlines():
            if line.startswith("Name:"):
                name = line.split(None, 1)[1].strip()
            elif line.startswith("VmRSS:"):
                rss_kb = _int(line.split()[1]) or 0
        if name and rss_kb:
            agg[name] = agg.get(name, 0) + rss_kb * 1024
    return sorted(((n, v // _MB) for n, v in agg.items()), key=lambda x: -x[1])


def working_set() -> int | None:
    """Bộ nhớ THẬT SỰ đang bị giữ = memory.current − page cache thu hồi được (cách kubelet/cAdvisor tính)."""
    cg = cgroup()
    if cg["used"] is None:
        return None
    return max(0, cg["used"] - (_inactive_file() or 0))


def pressure() -> float | None:
    """Tỉ lệ working-set/giới hạn của cgroup (0..1), None nếu không biết giới hạn.
    Log 21/11: memory.current báo 97% trong khi python+bgutil chỉ ~190MB (phần còn lại là page cache) → bot
    tưởng sắp OOM, giảm đua proxy 2→1 và bỏ qua chiến lược nặng một cách oan."""
    cg = cgroup()
    ws = working_set()
    if ws is None or not cg["limit"]:
        return None
    return ws / cg["limit"]


def snapshot(top: int = 6) -> str:
    cg = cgroup()
    if cg["used"] is None:
        head = "cgroup=?"
    else:
        lim = f"{cg['limit'] // _MB}MB" if cg["limit"] else "không giới hạn"
        pct = f" ({cg['used'] * 100 // cg['limit']}%)" if cg["limit"] else ""
        head = f"cgroup={cg['used'] // _MB}/{lim}{pct}"
    ws = working_set()
    if ws is not None and cg["used"] is not None and ws != cg["used"]:
        head += f" ws={ws // _MB}MB"
    if cg["oom_kill"] is not None:
        head += f" oom_kill={cg['oom_kill']}"
    return head + " | " + " ".join(f"{n}={mb}MB" for n, mb in procs()[:top])


def relieve() -> None:
    """Trả bộ nhớ rảnh của Python về hệ điều hành (gc + malloc_trim của glibc)."""
    gc.collect()
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        pass


def limit_malloc_arenas() -> None:
    """
    Giới hạn số arena của glibc malloc = 2 (mallopt M_ARENA_MAX=-8). Python + thread pool
    (run_in_executor) sinh nhiều arena → RSS phình dần dù không rò rỉ. Phải gọi trước khi
    tạo thread.
    """
    try:
        ctypes.CDLL("libc.so.6").mallopt(-8, 2)
    except Exception:
        pass
