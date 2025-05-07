import os

# 基本路徑
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Flask 設定（示意用，實際機密請在 local_settings.py 覆寫）
FLASK_SECRET_KEY = '<key>'

# 基本 URL 與 OAuth 相關
BASE_URI = '127.0.0.1:5000'

# LINE Bot 設定（示意用，請在 local_settings.py 中覆寫）
CHANNEL_SECRET = 'your_line_channel_secret'
CHANNEL_ACCESS_TOKEN = 'your_line_channel_access_token'

# Google OAuth 設定
SCOPES = 'https://www.googleapis.com/auth/drive.file'
# OAuth 授權用的 client_secrets 檔案路徑（請自行提供）
CLIENT_SECRETS_FILE = os.path.join(BASE_DIR, 'client_secrets.json')

# 資料庫設定（示意用，請在 local_settings.py 中覆寫真正機密資訊）
DATABASE = {
    'HOST': 'localhost',
    'PORT': 3306,
    'USER': 'host',
    'PASSWORD': '<password>',
    'DB': 'drivetoken',
}

# 靜態檔案路徑設定
STATIC_TMP_PATH = os.path.join(BASE_DIR, 'static', 'tmp')
STATIC_LOGS_PATH = os.path.join(BASE_DIR, 'static', 'logs')

# Celery 設定
CELERY_BROKER_URL = 'redis://127.0.0.1:6379/0'
CELERY_BACKEND_URL = 'redis://127.0.0.1:6379/0'

# 匯入 local_settings.py 中的覆寫設定（若存在的話）
try:
    from local_settings import *
except ImportError:
    pass