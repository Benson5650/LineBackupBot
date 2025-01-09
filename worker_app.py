# worker_app.py
import os
import json
import datetime
import logging
import requests
from celery import Celery
from celery.exceptions import SoftTimeLimitExceeded
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import portalocker

# 讀取 config.json
with open('config.json', 'r') as config_file:
    config = json.load(config_file)

UPLOAD_FOLDER = config['UPLOAD_FOLDER']
channel_secret = config['channel_secret']
channel_access_token = config['channel_access_token']

SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = 'google.json'

# Celery 連線設定 (以 Redis 為 broker)
CELERY_BROKER_URL = 'redis://127.0.0.1:6379/0'  
CELERY_BACKEND_URL = 'redis://127.0.0.1:6379/0'

# 建立 Celery 物件
celery = Celery('worker_app', broker=CELERY_BROKER_URL, backend=CELERY_BACKEND_URL)

# 讀取 Google Drive 認證
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)

logging.basicConfig(level=logging.INFO)

def get_source_name(source_type, source_id):
    if source_type == 'group':
        url = f'https://api.line.me/v2/bot/group/{source_id}/summary'
        headers = {'Authorization': f'Bearer {channel_access_token}'}
    elif source_type == 'user':
        url = f'https://api.line.me/v2/bot/profile/{source_id}'
        headers = {'Authorization': f'Bearer {channel_access_token}'}

    get = requests.get(url, headers=headers)
    if get.status_code == 200:
        data = get.json()
        if source_type == 'group':
            return data['groupName']
        elif source_type == 'user':
            return data['displayName']
    else:
        return None

def get_or_create_folder_for_source_id(source_type, source_id, drive_service, logger):
    """
    傳入 source_type, source_id, 
    若對應的雲端資料夾不存在，則新建並更新 folder_map.json。
    若已存在，則直接回傳對應的 folder_id。
    """
    folder_map_file = 'folder_map.json'  # 用於紀錄每個 source_id 對應的雲端資料夾 ID

    # 使用 portalocker.Lock 來鎖定檔案，避免多個 Worker 併發讀寫
    with portalocker.Lock(folder_map_file, mode='a+', timeout=10) as locked_file:
        # 先移動檔案指標到開頭，以便讀取現有內容
        locked_file.seek(0)

        try:
            # 嘗試讀取 JSON
            folder_map = json.load(locked_file)
        except json.JSONDecodeError:
            folder_map = {}

        # 若已有 folder_id, 直接回傳
        if source_id in folder_map:
            return folder_map[source_id]

        # 若無，建立新資料夾
        folder_name = get_source_name(source_type, source_id)
        if not folder_name:
            # 若取不到名稱，可用 source_id 當作備用名稱
            folder_name = source_id

        folder_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [UPLOAD_FOLDER]
        }
        try:
            folder = drive_service.files().create(
                body=folder_metadata, 
                fields='id'
            ).execute()
            folder_id = folder.get('id')
            logger.info(f"Created new folder for source_id={source_id}, folder_id={folder_id}")

            # 更新資料
            folder_map[source_id] = folder_id

            # 回到檔案開頭並清空，再寫入更新後內容
            locked_file.seek(0)
            locked_file.truncate()
            json.dump(folder_map, locked_file, ensure_ascii=False, indent=2)

            return folder_id

        except Exception as e:
            logger.error(f"Failed to create folder for source_id={source_id}: {e}")
            return None

@celery.task(
    time_limit=300,                        # 300秒(5分鐘)硬超時
    soft_time_limit=270,                   # 270秒時觸發軟超時
    autoretry_for=(ConnectionError, ),     # 遇到 ConnectionError 時自動重試
    retry_kwargs={'max_retries': 3, 'countdown': 10},  # 最多重試3次, 每次等10秒
    retry_backoff=True                     # 指數回退(每次失敗等待時間加倍)
)
def upload_file_to_drive_task(dist_path, dist_name, source_type, source_id):
    """
    上傳檔案到 Google Drive 的任務。
    dist_path : 檔案在本地的實際路徑
    dist_name : 要上傳到 Google Drive 時的檔名
    source_type: 'group' or 'user' (用於取得名稱)
    source_id : 依照 source_id 建立或使用已存在的雲端子資料夾
    """
    logger = logging.getLogger('celery')
    logger.info(f"接收到上傳任務，source_type={source_type}, source_id={source_id}")

    try:
        service = build('drive', 'v3', credentials=creds)

        # 取得或建立專屬該 source_id 的雲端資料夾
        folder_id = get_or_create_folder_for_source_id(source_type, source_id, service, logger)
        if not folder_id:
            logger.error(f"無法取得或建立資料夾，任務中止: {source_id}")
            return

        # 執行檔案上傳
        media = MediaFileUpload(dist_path, chunksize=1024*1024, resumable=True)
        file_metadata = {
            'name': dist_name,
            'parents': [folder_id]
        }
        file = service.files().create(
            body=file_metadata,
            media_body=media
        ).execute()
        file_id = file.get('id')

        logger.info(f'File uploaded to Google Drive with ID: {file_id} (source_id={source_id})')

    except SoftTimeLimitExceeded:
        # 270 秒軟超時到時，會觸發這個異常
        logger.error(f'Upload task soft-time-limit exceeded (source_id={source_id}).')
        # 在此可做清理工作，或記錄失敗原因
        raise  # 將異常往上拋，Celery 可記錄該任務失敗或觸發重試

    except ConnectionError as exc:
        # 在 autoretry_for=(ConnectionError,) 的情況下，
        # 遇到此錯誤會自動重試，不需手動raise self.retry()。
        logger.error(f'Upload connection failed (source_id={source_id}): {exc}')
        raise  # 讓 Celery 自動進行重試

    except Exception as e:
        logger.error(f'Upload failed (source_id={source_id}): {e}')

    finally:
        # 關閉檔案串流
        if 'media' in locals():
            media.stream().close()

        # 移除本地暫存檔
        try:
            os.remove(dist_path)
            logger.info(f'已刪除暫存檔案: {dist_path}')
        except OSError as e:
            logger.error(f'刪除暫存檔失敗: {e}')
