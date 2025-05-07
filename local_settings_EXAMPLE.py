# local_settings.py (機密設定，請勿納入版本控制)
# 建議將此檔案加到 .gitignore 中

# Flask 機密金鑰：請更換為真正的隨機字串
FLASK_SECRET_KEY = '你自己的超安全隨機字串'

# LINE Bot 設定：請設定你申請到的正確資訊
CHANNEL_SECRET = '你的 LINE Channel Secret'
CHANNEL_ACCESS_TOKEN = '你的 LINE Channel Access Token'

# 資料庫設定：請填入正確的資料庫連線資訊
DATABASE = {
    'HOST': 'your_db_host',
    'PORT': 3306,
    'USER': 'your_db_user',
    'PASSWORD': 'your_db_password',
    'DB': 'drivetoken',
}

# 若有其他機密設定，也可在此處覆寫 