"""
bot.py — Entry point.

Run with: python3 bot.py

Command sync strategy
─────────────────────
Discord maintains two separate command registries per bot:
  • Global commands   — visible in every server (propagation: up to 1 hour)
  • Guild commands    — visible only in one server (propagation: instant)

The duplicate-command problem occurs when BOTH registries contain the same
command names — Discord's client renders them side by side.

This bot uses guild-only sync for all environments:
  1. Copy the in-memory command tree to the dev guild scope.
  2. Sync to the guild  → registers/updates commands in that guild only.
  3. Clear the global scope locally, then sync it → pushes an EMPTY global
     tree to Discord, removing any stale global commands from previous runs.

DEV_GUILD_ID is required. The bot will refuse to start without it so that
a misconfigured .env can never cause a silent global-sync and duplicates.
"""

import asyncio
import logging
import os
import subprocess
import sys
import time
from threading import Thread

import discord
from flask import Flask, jsonify
from discord import app_commands
from discord.ext import commands

import config
from utils.logger import setup_logging, get_logger

# ── Silence Flask/Werkzeug startup noise ────────────────────────────────────
logging.getLogger("werkzeug").setLevel(logging.ERROR)

# ── Keep-alive web server ────────────────────────────────────────────────────
# Flask serves health-check routes on the port Replit assigns via $PORT.
# Cloud Run (autoscale) and Reserved VM both inject PORT and health-check it.
# Using a single deterministic port (never a fallback loop) is required so
# the startup probe gets a response before the promote-step timeout.

_flask_app  = Flask(__name__)
_start_time = time.time()

# $PORT is set by Replit's deployment runtime (Cloud Run / VM).
# Fall back to 8080 for local dev where PORT is not set.
_PORT = int(os.environ.get("PORT", 8080))

# Global bot reference — set in main() so health endpoints can inspect it.
_bot_ref: "MusicBot | None" = None


@_flask_app.route("/")
def _index():
    """Root route — must return 200 for Cloud Run startup probe to pass."""
    return "Bot is alive!", 200


@_flask_app.route("/health")
def _health():
    bot_ready = _bot_ref is not None and _bot_ref.is_ready()
    uptime_s  = int(time.time() - _start_time)
    h, rem    = divmod(uptime_s, 3600)
    m, s      = divmod(rem, 60)
    return jsonify({
        "status": "online",
        "bot":    "ready" if bot_ready else "starting",
        "uptime": f"{h}h {m}m {s}s",
    })


@_flask_app.route("/ping")
def _ping():
    latency = round(_bot_ref.latency * 1000, 2) if (_bot_ref and _bot_ref.is_ready()) else 0
    return jsonify({"pong": True, "latency_ms": latency})


@_flask_app.route("/status")
def _status():
    bot_ready  = _bot_ref is not None and _bot_ref.is_ready()
    uptime_s   = int(time.time() - _start_time)
    guilds     = len(_bot_ref.guilds)      if bot_ready else 0
    latency_ms = round(_bot_ref.latency * 1000, 2) if bot_ready else 0
    return jsonify({
        "status":      "online",
        "bot_ready":   bot_ready,
        "guild_count": guilds,
        "latency_ms":  latency_ms,
        "uptime_s":    uptime_s,
    })


def _run_flask() -> None:
    """Bind Flask to 0.0.0.0:$PORT — single port, no fallback loop."""
    _flask_app.run(host="0.0.0.0", port=_PORT, use_reloader=False)


def _log_outbound_ip() -> None:
    """
    Log IP outbound thật của server — cần cái này để thêm vào IP Allowlist của
    Bright Data (proxy residential yêu cầu whitelist IP trước khi cho kết nối).
    Chạy 1 lần lúc khởi động, không lặp lại.
    """
    import urllib.request
    log = logging.getLogger("bgutil")
    try:
        with urllib.request.urlopen("https://api.ipify.org", timeout=10) as resp:
            ip = resp.read().decode().strip()
        log.info(
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "  🌐 IP OUTBOUND CỦA SERVER NÀY: %s\n"
            "  → Copy IP này vào Bright Data → IP Allowlist → Add allowed IP\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            ip,
        )
    except Exception as exc:
        log.warning("Không lấy được IP outbound: %s", exc)


def keep_alive() -> None:
    """Start the Flask keep-alive server on a background daemon thread."""
    Thread(target=_run_flask, daemon=True).start()


# ── PO-Token provider (bgutil) — chạy ngầm để yt-dlp vượt bot-check YouTube ──
_bgutil_proc: "subprocess.Popen | None" = None


def _download_deno() -> None:
    """
    Tải binary Deno (JS runtime) — YouTube từ 11/2025 bắt buộc yt-dlp phải giải
    n-signature/sig challenge bằng JS runtime thật, KHÔNG chỉ PO-Token. Thiếu
    Deno → hàng loạt format bị loại bỏ ("n challenge solving failed"), gây lỗi
    "Requested format is not available" dù cookies/PO-Token đều đúng.
    Deno là 1 file binary duy nhất — tải + chmod +x là dùng được, không cần cài.
    """
    import io
    import urllib.request
    import zipfile

    log  = logging.getLogger("bgutil")
    root = os.path.dirname(os.path.abspath(__file__))
    bin_path = os.path.join(root, "deno")

    if os.path.exists(bin_path):
        os.environ["PATH"] = root + os.pathsep + os.environ.get("PATH", "")
        try:
            result = subprocess.run([bin_path, "--version"], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                log.info("Deno đã có sẵn, hoạt động OK: %s", result.stdout.strip().splitlines()[0])
        except Exception:
            pass
        return

    url = "https://github.com/denoland/deno/releases/latest/download/deno-x86_64-unknown-linux-gnu.zip"
    try:
        log.info("Downloading Deno (JS runtime cho n-signature challenge của YouTube)…")
        with urllib.request.urlopen(url, timeout=60) as resp:
            data = resp.read()
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            with z.open("deno") as src, open(bin_path, "wb") as dst:
                dst.write(src.read())
        os.chmod(bin_path, 0o755)
        # Đảm bảo PATH có thư mục này (phòng khi bgutil-pot lỗi trước đó chưa set PATH)
        os.environ["PATH"] = root + os.pathsep + os.environ.get("PATH", "")
        log.info("Deno downloaded OK.")

        # Kiểm tra binary CHẠY ĐƯỢC thật (không chỉ tải về) — log rõ version
        # để xác nhận yt-dlp sẽ tìm thấy và dùng được Deno.
        try:
            result = subprocess.run(
                [bin_path, "--version"], capture_output=True, text=True, timeout=10
            )
            if result.returncode == 0:
                log.info("Deno hoạt động OK: %s", result.stdout.strip().splitlines()[0])
            else:
                log.warning("Deno chạy nhưng lỗi (code %d): %s", result.returncode, result.stderr.strip())
        except Exception as exc:
            log.warning("Không chạy thử được Deno: %s", exc)
    except Exception as exc:
        log.warning("Không tải được Deno: %s — yt-dlp có thể thiếu 1 số format do JS challenge.", exc)


def _bgutil_setup_and_run() -> None:
    """
    Chạy trong thread nền: tải binary Rust bgutil-pot (không cần Node.js/npm —
    host này không có sẵn npm) rồi khởi động server. KHÔNG chặn Discord bot —
    nếu bước nào lỗi, yt-dlp vẫn hoạt động bình thường (chỉ mất khả năng vượt
    PO-Token).
    """
    global _bgutil_proc
    import urllib.request

    log  = logging.getLogger("bgutil")
    root = os.path.dirname(os.path.abspath(__file__))
    bin_path = os.path.join(root, "bgutil-pot")

    if not os.path.exists(bin_path):
        url = (
            "https://github.com/jim60105/bgutil-ytdlp-pot-provider-rs/"
            "releases/latest/download/bgutil-pot-linux-x86_64"
        )
        try:
            log.info("Downloading bgutil-pot (Rust PO-Token provider, no Node.js needed)…")
            urllib.request.urlretrieve(url, bin_path)
            os.chmod(bin_path, 0o755)
            log.info("bgutil-pot downloaded OK.")
        except Exception as exc:
            log.warning("Không tải được bgutil-pot: %s — bỏ qua PO-Token provider.", exc)
            return

    # yt-dlp's CLI-based POT provider (getpot_bgutil_cli.py) gọi lệnh 'bgutil-pot'
    # trần (không có đường dẫn đầy đủ) qua PATH. Vì binary nằm ở thư mục riêng của
    # bot (không nằm trong PATH mặc định), thêm thư mục này vào PATH để tránh
    # FileNotFoundError khi yt-dlp kiểm tra provider CLI.
    os.environ["PATH"] = root + os.pathsep + os.environ.get("PATH", "")

    try:
        # Chạy im lặng — log verbose của bgutil-pot chỉ cần khi debug thủ công.
        _bgutil_proc = subprocess.Popen(
            [bin_path, "server", "--host", "127.0.0.1", "--port", "4416"],
            cwd=root,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        log.info(
            "PO-Token provider (bgutil-pot, Rust) started — PID %d, http://127.0.0.1:4416",
            _bgutil_proc.pid,
        )
    except Exception as exc:
        log.warning("Không khởi động được bgutil-pot: %s", exc)


def _sync_bgutil_plugin() -> None:
    """
    Đồng bộ plugin Python với server Rust (cùng project jim60105, cùng version).
    pip install bgutil-ytdlp-pot-provider (Brainicism) cài NHẦM plugin của project
    khác (TypeScript-based) — không tương thích version với server Rust đang chạy.
    Fix: tải plugin ZIP từ đúng project (jim60105/bgutil-ytdlp-pot-provider-rs) và
    ghi đè lên đúng 3 file trong thư mục yt_dlp_plugins/extractor/ đã tồn tại.
    """
    import io
    import urllib.request
    import zipfile

    log = logging.getLogger("bgutil")
    try:
        # Suy ra site-packages từ yt_dlp (chắc chắn import được, đang dùng để chạy bot)
        # thay vì import trực tiếp yt_dlp_plugins (namespace package, hay lỗi import).
        import yt_dlp
        site_packages = os.path.dirname(os.path.dirname(os.path.abspath(yt_dlp.__file__)))
        plugin_dir = os.path.join(site_packages, "yt_dlp_plugins", "extractor")
        os.makedirs(plugin_dir, exist_ok=True)
    except Exception as exc:
        log.warning("Không xác định được thư mục plugin: %s — bỏ qua sync.", exc)
        return

    zip_url = (
        "https://github.com/jim60105/bgutil-ytdlp-pot-provider-rs/"
        "releases/latest/download/bgutil-ytdlp-pot-provider-rs.zip"
    )
    try:
        with urllib.request.urlopen(zip_url, timeout=30) as resp:
            data = resp.read()
        zip_basenames: set[str] = set()
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            for name in z.namelist():
                base = os.path.basename(name)
                if base.startswith("getpot_bgutil") and base.endswith(".py"):
                    zip_basenames.add(base)
                    target = os.path.join(plugin_dir, base)
                    with z.open(name) as src, open(target, "wb") as dst:
                        dst.write(src.read())

        # XÓA file thừa từ package CŨ (Brainicism) không có trong zip mới —
        # vd getpot_bgutil_script.py chỉ tồn tại ở bản cũ (script-based, dùng Node),
        # bản mới (jim60105, HTTP-based) không có file này. Để sót lại sẽ crash
        # AttributeError vì class cũ không tương thích API yt-dlp hiện tại.
        removed = 0
        if os.path.isdir(plugin_dir):
            for fname in os.listdir(plugin_dir):
                if fname.startswith("getpot_bgutil") and fname.endswith(".py") and fname not in zip_basenames:
                    try:
                        os.remove(os.path.join(plugin_dir, fname))
                        removed += 1
                    except OSError:
                        pass

        log.info(
            "Đồng bộ plugin bgutil-rs xong — %d file cập nhật, %d file cũ đã xóa.",
            len(zip_basenames), removed,
        )
    except Exception as exc:
        log.warning("Không sync được plugin bgutil-rs: %s — có thể vẫn báo version mismatch.", exc)


def start_bgutil_provider() -> None:
    """Chạy toàn bộ setup + start bgutil trong thread nền, không chặn bot chính."""
    def _run():
        _bgutil_setup_and_run()
        _sync_bgutil_plugin()
        _download_deno()
    Thread(target=_run, daemon=True).start()


# ── Logging + constants ──────────────────────────────────────────────────────

setup_logging()
logger = get_logger(__name__)

EXTENSIONS: list[str] = ["cogs.music"]

EXPECTED_COMMANDS: frozenset[str] = frozenset({
    "play", "pause", "resume", "skip",
    "stop", "queue", "nowplaying", "volume",
})


# ── Bot class ────────────────────────────────────────────────────────────────

class MusicBot(commands.Bot):

    def __init__(self) -> None:
        intents = discord.Intents.default()
        intents.message_content = True

        super().__init__(
            command_prefix="\x00",  # null-byte → prefix never matches
            intents=intents,
            help_command=None,
        )

    # ── setup_hook ─────────────────────────────────────────────────────────────

    async def setup_hook(self) -> None:
        if not config.DEV_GUILD_ID:
            logger.critical(
                "DEV_GUILD_ID is not set. "
                "This bot uses guild-only command sync. "
                "Set DEV_GUILD_ID to your server's ID and restart."
            )
            await self.close()
            sys.exit(1)

        for ext in EXTENSIONS:
            try:
                await self.load_extension(ext)
                logger.info("Loaded extension: %s", ext)
            except Exception as exc:
                logger.exception("Failed to load extension '%s': %s", ext, exc)
                sys.exit(1)

        registered = {cmd.name for cmd in self.tree.get_commands()}
        missing    = EXPECTED_COMMANDS - registered
        extra      = registered - EXPECTED_COMMANDS

        if missing:
            logger.error("Commands missing from tree: %s", ", ".join(sorted(missing)))
        if extra:
            logger.warning("Unexpected commands in tree: %s", ", ".join(sorted(extra)))
        if not missing and not extra:
            logger.info(
                "Command tree OK — %d commands: %s",
                len(registered),
                ", ".join(f"/{c}" for c in sorted(registered)),
            )

        await self._sync_guild_only(config.DEV_GUILD_ID)

    async def _sync_guild_only(self, guild_id: int) -> None:
        guild_obj = discord.Object(id=guild_id)

        self.tree.copy_global_to(guild=guild_obj)
        try:
            guild_cmds = await self.tree.sync(guild=guild_obj)
        except discord.HTTPException as exc:
            logger.error("Guild sync failed: %s", exc)
            return

        logger.info(
            "Guild sync complete [guild %d] — %d command(s) registered:",
            guild_id, len(guild_cmds),
        )
        for cmd in sorted(guild_cmds, key=lambda c: c.name):
            logger.info("  /%s — %s", cmd.name, cmd.description)

        self.tree.clear_commands(guild=None)
        try:
            removed = await self.tree.sync()
            logger.info(
                "Global command purge complete — %d global command(s) remaining (should be 0).",
                len(removed),
            )
        except discord.HTTPException as exc:
            logger.warning("Global purge failed (non-fatal): %s", exc)

    # ── on_ready ───────────────────────────────────────────────────────────────

    async def on_ready(self) -> None:
        logger.info(
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )
        logger.info("  ✅  Bot online: %s (ID: %d)", self.user, self.user.id)  # type: ignore[union-attr]
        logger.info("  📡  Guilds    : %d", len(self.guilds))
        logger.info("  🏓  Latency   : %.1f ms", self.latency * 1000)
        logger.info(
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )
        await self.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.listening,
                name="/play",
            )
        )

    # ── Auto-reconnect ─────────────────────────────────────────────────────────

    async def on_disconnect(self) -> None:
        logger.warning("Bot disconnected from Discord — will reconnect automatically.")

    async def on_resumed(self) -> None:
        logger.info("Session resumed successfully.")

    # ── Error handlers ─────────────────────────────────────────────────────────

    async def on_error(self, event_method: str, *args, **kwargs) -> None:
        logger.exception("Unhandled exception in event '%s'", event_method)

    async def on_app_command_error(
        self,
        interaction: discord.Interaction,
        error: app_commands.AppCommandError,
    ) -> None:
        if isinstance(error, app_commands.CommandInvokeError):
            error = error.original  # type: ignore[assignment]

        logger.error(
            "App command error in /%s: %s",
            interaction.command.name if interaction.command else "?",
            error,
            exc_info=True,
        )

        msg = "An unexpected error occurred. Please try again."
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message(msg, ephemeral=True)
            else:
                await interaction.followup.send(msg, ephemeral=True)
        except discord.HTTPException:
            pass


# ── Entry point ──────────────────────────────────────────────────────────────

async def main() -> None:
    global _bot_ref
    bot     = MusicBot()
    _bot_ref = bot          # expose to Flask health endpoints

    try:
        await bot.start(config.BOT_TOKEN)
    except discord.LoginFailure:
        logger.critical(
            "Invalid BOT_TOKEN. Check your environment variables — "
            "get the token from https://discord.com/developers/applications"
        )
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Shutdown requested — closing.")
        await bot.close()


if __name__ == "__main__":
    keep_alive()
    start_bgutil_provider()
    Thread(target=_log_outbound_ip, daemon=True).start()
    asyncio.run(main())
