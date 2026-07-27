# utils/cookie_sync.py
import os
import requests
import logging
import asyncio
from threading import Thread

logger = logging.getLogger(__name__)

COOKIE_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cookies.txt")

# URL của service cookie-sync (sẽ deploy sau)
COOKIE_SYNC_URL = os.environ.get("COOKIE_SYNC_URL", "")

def update_cookie_from_service():
    """Gọi cookie-sync service để cập nhật cookie"""
    if not COOKIE_SYNC_URL:
        logger.warning("COOKIE_SYNC_URL chưa được cấu hình")
        return False
    
    try:
        logger.info("Đang cập nhật cookie từ service...")
        response = requests.post(COOKIE_SYNC_URL, timeout=60)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('status') == 'success':
                logger.info(f"Cập nhật cookie thành công: {data.get('cookies_count')} cookies")
                return True
            else:
                logger.error(f"Cập nhật cookie thất bại: {data.get('message')}")
                return False
        else:
            logger.error(f"Cookie sync service trả về mã {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"Lỗi khi gọi cookie sync service: {e}")
        return False

async def scheduled_cookie_update(interval_hours=2):
    """Chạy cập nhật cookie định kỳ"""
    while True:
        await asyncio.sleep(interval_hours * 3600)
        update_cookie_from_service()
