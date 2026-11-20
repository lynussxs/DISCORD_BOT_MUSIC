# Dockerfile tuỳ chỉnh — Railway sẽ dùng file này thay vì tự sinh qua Nixpacks
# khi thấy Dockerfile tồn tại ở root repo.
#
# QUAN TRỌNG: KHÔNG khai báo bất kỳ ARG/ENV nào chứa secret (BOT_TOKEN,
# OPENROUTER_API_KEY, SPOTIFY_CLIENT_SECRET, WEBSHARE_API_KEY, PROXY_US...)
# ở đây. Bot Python không cần các giá trị này lúc BUILD (chỉ cần lúc pip
# install), chỉ cần lúc RUNTIME khi bot.py chạy và tự đọc qua os.getenv().
# Railway tự động inject toàn bộ biến trong Dashboard → Variables vào
# container lúc chạy — không cần khai báo gì trong Dockerfile cho việc đó.

FROM python:3.11-slim

# ffmpeg cần cho việc phát nhạc (bot dùng discord.py + ffmpeg pipe audio)
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    rm -rf /var/lib/apt/lists/*

# Deno — JS runtime BẮT BUỘC để yt-dlp giải mã chữ ký/n-challenge của YouTube.
# Thiếu nó, yt-dlp chỉ thấy format rất hạn chế hoặc báo bot-check/"Requested
# format is not available". yt-dlp tự tìm `deno` trong PATH.
COPY --from=denoland/deno:bin /deno /usr/local/bin/deno

WORKDIR /app

# Copy trước requirements.txt riêng để tận dụng Docker layer cache — chỉ
# chạy lại pip install khi requirements.txt thực sự thay đổi, không phải
# mỗi lần đổi 1 dòng code nhỏ trong bot.py.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ code sau cùng
COPY . .

# Luôn lấy yt-dlp MỚI NHẤT ở mỗi lần build. Layer `pip install -r requirements.txt`
# ở trên được cache miễn requirements.txt không đổi → Railway có thể giữ mãi 1 bản
# yt-dlp cũ dù requirements ghi ">=". Đặt SAU `COPY . .` nên chạy lại mỗi lần
# đổi code; YouTube đổi cách trả stream gần như hàng tuần và yt-dlp vá theo.
RUN pip install --no-cache-dir -U "yt-dlp[default]"

# Không set ENV nào chứa secret ở đây — Railway tự inject lúc container start
CMD ["python3", "bot.py"]
