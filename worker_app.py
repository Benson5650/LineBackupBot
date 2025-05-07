# worker_app.py

# === 基本設定 ===
from settings import *

import os
import json
import logging
import tempfile
import datetime
from celery import Celery, chord, group
from celery.exceptions import SoftTimeLimitExceeded
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
import pymysql
from linebot.v3.messaging import (
    ReplyMessageRequest,
    Configuration,
    ApiClient,
    MessagingApi,
    TextMessage,
    ButtonsTemplate,
    TemplateMessage,
    MessageAction,
    MessagingApiBlob,
    ShowLoadingAnimationRequest,
    URIAction,
    CarouselColumn,
    CarouselTemplate
)
from dbutils.pooled_db import PooledDB  # 改用 PooledDB
import time


configuration = Configuration(
    access_token=CHANNEL_ACCESS_TOKEN,
)

celery = Celery('worker_app', broker=CELERY_BROKER_URL, backend=CELERY_BACKEND_URL)

# 設定時區
celery.conf.timezone = 'UTC'

# 設定定期任務
celery.conf.beat_schedule = {
    'clean-old-upload-logs-every-day': {
        'task': 'worker_app.clean_old_upload_logs',
        'schedule': 86400.0,  # 每86400秒（即每天執行一次）
        'args': (7,)  # 傳遞參數，這裡是保留7天的日誌
    },
    'clean-tmp-logs-every-10-minutes': {
        'task': 'worker_app.clean_tmp_logs',
        'schedule': 600.0,  # 每600秒（即10分鐘執行一次）
        'args': (30,)  # 傳遞參數，這裡是保留30分鐘的日誌
    },
}

class UserCredentialsError(Exception):
    """使用者憑證錯誤"""
    pass

# 日誌設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# 建立資料庫連線池
db_pool = PooledDB(
    creator=pymysql,
    maxconnections=6,      # 連線池允許的最大連線數
    mincached=2,           # 初始化時，連線池中至少建立的空閒連線數量
    maxcached=4,           # 連線池中允許的最大空閒連線數量
    maxshared=2,           # 共享連線數量
    blocking=True,         # 連線池滿時是否等待
    host=DATABASE["HOST"],
    user=DATABASE["USER"],
    password=DATABASE["PASSWORD"],
    database=DATABASE["DB"],
    charset='utf8mb4',
    autocommit=False,
)
def reply_message(reply_token, messages):
    with ApiClient(configuration) as api_client:
        line_bot_api = MessagingApi(api_client)
        line_bot_api.reply_message_with_http_info(
            ReplyMessageRequest(
                reply_token=reply_token,
                messages=messages
            )
        )

def reply_loading_animation(chat_id, seconds=5):
    with ApiClient(configuration) as api_client:
        line_bot_api = MessagingApi(api_client)
        # 顯示載入動畫
        request = ShowLoadingAnimationRequest(chatId=chat_id, loadingSeconds=seconds)
        line_bot_api.show_loading_animation(request)


def get_db_connection():
    """從連線池取得資料庫連線"""
    return db_pool.connection()

def get_user_credentials(user_id):
    """從資料庫取得使用者的 Google OAuth 憑證
    
    參數:
        user_id (str): LINE 使用者 ID
    
    回傳:
        Credentials: Google OAuth 憑證物件，若未找到則回傳 None
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute('SELECT token FROM user_tokens WHERE user_id = %s', (user_id,))
            result = cursor.fetchone()
            if result:
                token_json = json.loads(result[0])
                creds = Credentials.from_authorized_user_info(token_json)
                return creds
    finally:
        conn.close()
    return None

def delete_folder_map(source_id, user_id):
    """從資料庫中刪除指定的資料夾映射關係
    
    參數:
        source_id (str): 來源 ID（群組或使用者）
        user_id (str): LINE 使用者 ID
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = "DELETE FROM folder_map WHERE source_id = %s AND user_id = %s"
            cursor.execute(sql, (source_id, user_id))
        conn.commit()
    finally:
        conn.close()

def get_source_name(source_type, source_id):
    """更新或建立來源名稱記錄
    
    參數:
        source_type (str): 來源類型（'group' 或 'user'）
        source_id (str): 來源 ID
    
    回傳:
        str: 來源名稱，若取得失敗則回傳 None
    """
    logger = logging.getLogger('celery')
    
    try:
        # 從 LINE API 取得最新名稱
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            
            if source_type == 'group':
                group_summary = line_bot_api.get_group_summary(group_id=source_id)
                name = group_summary.group_name
            elif source_type == 'user':
                profile = line_bot_api.get_profile(user_id=source_id)
                name = profile.display_name
            else:
                return None
            logger.info("[LINE] Got source name: type=%s, id=%s, name=%s", 
                       source_type, source_id, name)
            return name
    except Exception as e:
        logger.error("[LINE] Failed to get source name: %s", str(e))
        return None

def get_or_create_folder_by_name(drive_service, folder_name, parent_id=None):
    """
    在指定 parent_id (或根目錄) 下，尋找或建立名為 folder_name 的資料夾，並回傳該資料夾的 ID。
    """
    logger = logging.getLogger('celery')
    if parent_id:
        query = (
            f"mimeType='application/vnd.google-apps.folder' "
            f"and name='{folder_name}' "
            f"and '{parent_id}' in parents "
            f"and trashed=false"
        )
        logger.info("[DRIVE] Searching in parent_id=%s for folder_name=%s", parent_id, folder_name)
    else:
        query = (
            f"mimeType='application/vnd.google-apps.folder' "
            f"and name='{folder_name}' "
            f"and 'root' in parents "
            f"and trashed=false"
        )
        logger.info("[DRIVE] Searching in 'root' for folder_name=%s", folder_name)

    response = drive_service.files().list(
        q=query,
        spaces='drive'
    ).execute()

    folders = response.get('files', [])
    
    logger.info("[DRIVE] Found folders:")
    for f in folders:
        logger.info(" - %s | %s", f.get('id'), f.get('name'))

    if folders:
        folder_id = folders[0]['id']
        logger.info("[DRIVE] Using existing folder: %s", folder_id)
        return folder_id

    # 若沒有，則建立新的資料夾
    metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder',
    }
    if parent_id:
        metadata['parents'] = [parent_id]

    folder = drive_service.files().create(body=metadata, fields='id').execute()
    return folder.get('id')

def get_or_create_folder_for_source_id(source_type, source_id, target_user_id, drive_service, logger):
    """
    在 `folder_map(source_id, user_id)` 裡找/建此 (群組, 使用者) 的專屬資料夾。
    如果不存在，就在該使用者雲端中「LineBot/群組名稱」底下建立並回存 DB。

    回傳資料夾 ID (str) 或 None 表示失敗。
    """
    # 取得群組或使用者名稱（若無法取得，就用 source_id）
    folder_name = get_source_name(source_type, source_id) or source_id

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 手動開始交易
            cursor.execute("START TRANSACTION")

            # 1) 嘗試從 folder_map 查詢 (source_id, target_user_id) 是否已存在
            select_sql = """
                SELECT folder_id 
                FROM folder_map
                WHERE source_id = %s
                  AND user_id = %s
                FOR UPDATE
            """
            cursor.execute(select_sql, (source_id, target_user_id))
            row = cursor.fetchone()

            if row:
                # 已有資料 → 直接用現有的 folder_id
                folder_id = row[0]
                logger.info(f"[RowLock] Folder exists in DB: {folder_id} (source_id={source_id}, user={target_user_id})")
            else:
                # 沒有資料 → 建新的資料夾

                # 先在該使用者雲端下，找/建 "LineBot" 資料夾
                linebot_folder_id = get_or_create_folder_by_name(drive_service, 'LineBot', parent_id=None)
                # 再在 "LineBot" 裡建群組資料夾
                folder_id = get_or_create_folder_by_name(drive_service, folder_name, parent_id=linebot_folder_id)

                logger.info(f"[RowLock] Created new group folder in user {target_user_id} drive: {folder_id}")

                # 寫入 DB
                insert_sql = """
                    INSERT INTO folder_map (source_id, user_id, folder_id)
                    VALUES (%s, %s, %s)
                """
                cursor.execute(insert_sql, (source_id, target_user_id, folder_id))
            
            # 提交交易，釋放鎖
            conn.commit()
            return folder_id

    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to create/get folder (source_id={source_id}, user={target_user_id}): {e}")
        return None

    finally:
        conn.close()

@celery.task(
    time_limit=300,
    soft_time_limit=270,
    autoretry_for=(ConnectionError, SoftTimeLimitExceeded, HttpError, ),
    retry_kwargs={'max_retries': 3, 'countdown': 10},
    retry_backoff=True,
    bind=True
)
def upload_file_to_drive_task(self, dist_path, dist_name, source_type, source_id, target_user_id, reply_token=None,retry=False):
    """上傳檔案到使用者的 Google Drive
    
    參數:
        self: Celery task 物件
        dist_path (str): 暫存檔案路徑
        dist_name (str): 目標檔案名稱
        source_type (str): 來源類型（'group' 或 'user'）
        source_id (str): 來源 ID
        target_user_id (str): 目標使用者 ID
        reply_token (str): 回應 token
    """
    current_retry = self.request.retries  # 這次進到 except block 前的重試計數
    max_retry = self.max_retries
    logger = logging.getLogger('celery')
    logger.info("[TASK] Starting upload task: source_type=%s, source_id=%s, target_user_id=%s",
                source_type, source_id, target_user_id)

    uploaded_successfully = False
    try:
        current_name = get_source_name(source_type, source_id)
    except Exception as e:
        logger.error("[TASK] Failed to get source name: %s", str(e))
        current_name = None

    try:
        # 1. 取得該使用者的 Google OAuth Credentials
        user_creds = get_user_credentials(target_user_id)
        if not user_creds:
            logger.error("[AUTH] Failed to get credentials for user_id=%s", target_user_id)
            raise UserCredentialsError

        # 2. 建立 Drive Service
        service = build('drive', 'v3', credentials=user_creds)

        # 3. 取得(或建立)針對「(群組, 該使用者)」的專屬資料夾
        folder_id = get_or_create_folder_for_source_id(source_type, source_id, target_user_id, service, logger)
        if not folder_id:
            logger.error("[DRIVE] Failed to get/create folder for source_id=%s, user_id=%s",
                        source_id, target_user_id)
            raise Exception

        # 在上傳前檢查並更新資料夾名稱
        
        if current_name:
            try:
                # 先取得現有資料夾資訊
                folder_metadata = service.files().get(fileId=folder_id, fields='name').execute()
                existing_name = folder_metadata.get('name')
                
                # 只在名稱不同時才更新
                if existing_name != current_name:
                    service.files().update(
                        fileId=folder_id,
                        body={'name': current_name}
                    ).execute()
                    logger.info(f"Updated folder name: {existing_name} -> {current_name}")
                else:
                    logger.debug(f"Folder name unchanged: {existing_name}")
            except Exception as e:
                logger.warning(f"Failed to update folder name: {str(e)}")

        # 4. 上傳檔案
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

        # 記錄上傳日誌
        if not retry:
            log_upload(target_user_id, dist_name, source_type, source_id, current_name, "success")

        logger.info("[DRIVE] Upload successful: file_id=%s, user_id=%s", file_id, target_user_id)
        if reply_token:
            reply_message(reply_token, [TextMessage(text=f"測試圖片已成功上傳到您的 Google Drive\n連結: https://drive.google.com/file/d/{file_id}/view?usp=sharing")])
        
        uploaded_successfully = True

    except SoftTimeLimitExceeded:
        logger.error("[TASK] Soft time limit exceeded for user_id=%s", target_user_id)
        if not retry:
            log_upload(target_user_id, dist_name, source_type, source_id, current_name, f"timeout{current_retry}/{max_retry}")
        raise

    except ConnectionError as exc:
        logger.error("[NETWORK] Upload failed for user_id=%s: %s", target_user_id, str(exc))
        if not retry:
            log_upload(target_user_id, dist_name, source_type, source_id, current_name, f"connection_error{current_retry}/{max_retry}")
        raise

    except HttpError as e:
        if e.resp.status == 404:
            logger.warning("[DRIVE] 404 error: Folder might be deleted. Removing folder_map: %s", str(e))
            delete_folder_map(source_id, target_user_id)
        logger.error("[DRIVE] Upload failed for user_id=%s: %s", target_user_id, str(e))
        if not retry:
            log_upload(target_user_id, dist_name, source_type, source_id, current_name, f"http_error{current_retry}/{max_retry}")
        raise

    except UserCredentialsError:
        # 記錄上傳失敗日誌
        if not retry:
            log_upload(target_user_id, dist_name, source_type, source_id, current_name, f"user_credentials_error")
        if reply_token:
            reply_message(reply_token, [TextMessage(text="Google 帳號認證失敗，請重新綁定")])
        raise

    except Exception as e:
        logger.error("[TASK] Unexpected error for user_id=%s: %s", target_user_id, str(e))
        if reply_token:
            reply_message(reply_token, [TextMessage(text="上傳失敗，請稍後再試")])
        
        # 記錄上傳失敗日誌
        if not retry:
            log_upload(target_user_id, dist_name, source_type, source_id, current_name, f"unknown_error")
        raise

    finally:
        if uploaded_successfully:
            logger.info("[TASK] Upload completed, keeping temp file: %s", dist_path)
        else:
            logger.info("[TASK] Keeping temp file (upload failed): %s", dist_path)

def log_upload(user_id, file_name, source_type, source_id, source_name, status):
    """記錄上傳日誌"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = "INSERT INTO upload_logs (user_id, file_name, source_type, source_id, source_name, status) VALUES (%s, %s, %s, %s, %s, %s)"
            cursor.execute(sql, (user_id, file_name, source_type, source_id, source_name, status))
        conn.commit()
    finally:
        conn.close()

@celery.task
def clean_temp_file(results, dist_path):
    """清理暫存檔案（Celery chord 的回調函數）
    
    參數:
        results: chord group 的執行結果
        dist_path (str): 要刪除的暫存檔案路徑
    """
    logger = logging.getLogger('celery')
    # (可選) logger.info(f"chord group results: {results}")

    if os.path.exists(dist_path):
        try:
            os.remove(dist_path)
            logger.info("[CLEANUP] File deleted: %s", dist_path)
        except OSError as e:
            logger.error("[CLEANUP] Failed to delete file: %s", str(e))
    else:
        logger.info("[CLEANUP] File does not exist: %s", dist_path)

@celery.task
def check_google_status(reply_token, line_user_id):
    logger = logging.getLogger('celery')
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 檢查是否有綁定記錄
            cursor.execute('SELECT id FROM user_tokens WHERE user_id = %s', (line_user_id,))
            token_row = cursor.fetchone()
            
            if token_row:
                template_message = TemplateMessage(
                    alt_text='Google 帳號狀態',
                    template=ButtonsTemplate(
                        title='Google 帳號狀態',
                        text='✅ 您的 Google 帳號綁定狀態正常',
                        actions=[
                            MessageAction(
                                label='解除綁定',
                                text='!unbindgoogle'
                            )
                        ]
                    )
                )
                reply_message(reply_token, [template_message])
            else:
                template_message = TemplateMessage(
                    alt_text='Google 帳號狀態',
                    template=ButtonsTemplate(
                        title='Google 帳號狀態',
                        text='❗ 您尚未綁定 Google 帳號',
                        actions=[
                            MessageAction(
                                label='立即綁定',
                                text='!bindgoogle'
                            )
                        ]
                    )
                )
                reply_message(reply_token, [template_message])
    except Exception as e:
        logger.error(f"檢查 Google 狀態時發生錯誤: {str(e)}")
        template_message = TemplateMessage(
            alt_text='Google 帳號狀態',
            template=ButtonsTemplate(
                title='系統錯誤',
                text='檢查 Google 帳號狀態時發生錯誤，請稍後再試',
                actions=[
                    MessageAction(
                        label='重試',
                        text='!checkgoogle'
                    )
                ]
            )
        )
        reply_message(reply_token, [template_message])
    finally:
        conn.close()

@celery.task(bind=True, max_retries=3, default_retry_delay=5)
def download_line_file_task(self, message_id, ext, filename=None):
    """
    從 LINE Messaging API 下載檔案，並保存到暫存路徑。
    若下載或寫檔過程出現任何例外，會自動重試 (最多 3 次)。
    """
    try:
        with ApiClient(configuration) as api_client:
            line_bot_blob_api = MessagingApiBlob(api_client)
            message_content = line_bot_blob_api.get_message_content(message_id=message_id)
            if not message_content:
                raise ValueError("無法取得檔案內容，message_content 為 None。")

            # 若需要 .read() 就自行加上： content_data = message_content.read()
            content_data = message_content

            with tempfile.NamedTemporaryFile(dir=STATIC_TMP_PATH, prefix=f'{ext}-', delete=False) as tf:
                tf.write(content_data)
                tempfile_path = tf.name

        # 生成時間戳
        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

        # 從臨時檔案路徑獲取基本檔名（不含路徑）
        temp_basename = os.path.basename(tempfile_path)

        # 根據檔案類型處理
        if ext == 'file':
            if filename:
                # 如果有提供檔名，使用它
                dist_path = f"{STATIC_TMP_PATH}/{timestamp}_{filename}"
            else:
                # 如果沒有提供檔名，直接使用臨時檔名
                dist_path = f"{STATIC_TMP_PATH}/{timestamp}_{temp_basename}"
        else:
            # 對於媒體檔案，在臨時檔名基礎上加上副檔名
            dist_path = f"{STATIC_TMP_PATH}/{timestamp}_{temp_basename}.{ext}"

        # 重命名臨時檔案
        os.rename(tempfile_path, dist_path)

        # 直接從 dist_path 提取檔名
        dist_name = os.path.basename(dist_path)

        return dist_path, dist_name

    except Exception as e:
        raise self.retry(exc=e, countdown=5)
    
@celery.task
def handle_upload_task(download_result, event_data):
    """
    取代原本在主程式的 handle_upload() 函式：
    1) 根據 download_result 拿 dist_path, dist_name
    2) 判斷來源是 user / group，進行上傳
    3) 上傳完後 chord callback -> clean_temp_file
    """
    dist_path, dist_name = download_result
    source_type = event_data['source_type']
    source_id = event_data['source_id']
    reply_token = event_data.get('reply_token', None)

    # log 可改為你的 logger
    print(f"[TASK] Creating upload tasks for {source_type}: id={source_id}")

    if source_type == 'user':
        # 查使用者 debug_mode (範例)
        debug_mode = False
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute('SELECT debug_mode FROM user_status WHERE user_id = %s', (source_id,))
                result = cursor.fetchone()
                debug_mode = (result[0] if result else False)

        # 若要在 worker 端用 reply_token 回應, 要注意 token 30秒有效; 建議改成 push_message
        if debug_mode:
            reply_loading_animation(source_id, 15)
        else:
            reply_token = None

        # 單一任務 + clean_temp_file
        task = upload_file_to_drive_task.s(dist_path, dist_name, source_type, source_id, source_id, reply_token)
        callback = clean_temp_file.s(dist_path)
        return chord(task)(callback)

    else:  # group
        bound_users = []
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute('SELECT user_id FROM group_users WHERE group_id = %s', (source_id,))
                bound_users = cursor.fetchall()

        if bound_users:
            task_list = [
                upload_file_to_drive_task.s(dist_path, dist_name, source_type, source_id, row[0])
                for row in bound_users
            ]
            callback = clean_temp_file.s(dist_path)
            return chord(group(task_list))(callback)
        else:
            # 無綁定使用者 -> 直接刪掉暫存檔
            if os.path.exists(dist_path):
                os.remove(dist_path)
            print(f"[CLEANUP] No bound users, deleted temp file: {dist_path}")
            return "OK"

@celery.task
def clean_old_upload_logs(days=7):
    """清理超過指定天數的上傳日誌"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute('DELETE FROM upload_logs WHERE upload_time < NOW() - INTERVAL %s DAY', (days,))
        conn.commit()
    finally:
        conn.close()

@celery.task
def clean_tmp_logs(minutes=30):
    """清理暫存日誌檔案"""
    now = time.time()
    cutoff = now - (minutes * 60)
    
    for filename in os.listdir(STATIC_LOGS_PATH):
        file_path = os.path.join(STATIC_LOGS_PATH, filename)
        if os.path.isfile(file_path):
            file_mtime = os.path.getmtime(file_path)
            if file_mtime < cutoff:
                os.remove(file_path)
                print(f"Deleted {file_path}")
    
@celery.task
def showlog_task(reply_token, user_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute('SELECT file_name, upload_time, source_type, source_name, status FROM upload_logs WHERE user_id = %s ORDER BY upload_time DESC', (user_id,))
            logs = cursor.fetchall()
            
            if logs:
                full_logs = []
                for log in logs:
                    file_name, upload_time, source_type, source_name, status = log
                    full_logs.append(f"{file_name}\n時間: {upload_time}\n來源: {source_type} ({source_name})\n狀態: {status}\n\n")
                full_text = "".join(full_logs)
                
                if len(full_text) > 1000:
                    reply_text = "日誌過長，請點擊查看完整紀錄(有效期30分鐘)\n\n" + full_text[:20] + "..."
                    with tempfile.NamedTemporaryFile(delete=False, dir=STATIC_LOGS_PATH, suffix=".txt") as temp_file:
                        temp_file.write(full_text.encode('utf-8'))
                        temp_file_name = os.path.basename(temp_file.name)
                    
                    # 假設您有一個伺服器可以提供這個檔案
                    file_url = f"https://{BASE_URI}/linebot/logs/{temp_file_name}"
                    button_template = TemplateMessage(
                        alt_text="上傳日誌",
                        template=ButtonsTemplate(
                            title="上傳日誌",
                            text=reply_text,
                            actions=[
                                URIAction(
                                    label="查看完整紀錄",
                                    uri=file_url
                                )
                            ]
                        )
                    )
                    reply_message(reply_token, [button_template])
                else:
                    reply_message(reply_token, [TextMessage(text=full_text)])


            else:
                reply_message(reply_token, [TextMessage(text="您尚未上傳任何檔案。")])
    finally:
        conn.close()

@celery.task
def handle_list_group_task(reply_token, user_id):
    logger = logging.getLogger('celery')
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 先查詢個人資料夾
            logger.info("[LISTGROUP] Querying personal folder")
            cursor.execute('''
                SELECT 
                    'personal' as type,
                    fm.source_id,
                    fm.folder_id
                FROM folder_map fm
                WHERE fm.source_id = %s 
                    AND fm.user_id = %s
                UNION ALL
                SELECT 
                    'group' as type,
                    gu.group_id,
                    fm2.folder_id
                FROM group_users gu 
                LEFT JOIN folder_map fm2
                    ON fm2.source_id = gu.group_id 
                    AND fm2.user_id = gu.user_id 
                WHERE gu.user_id = %s
            ''', (user_id, user_id, user_id))
            rows = cursor.fetchall()
            
        if not rows:
            logger.info("[LISTGROUP] No folders found for user_id=%s", user_id)
            reply_text = "您尚未建立任何資料夾。"
            reply_message(reply_token, [TextMessage(text=reply_text)])
        
        columns = []
        
        # 處理查詢結果
        for i, row in enumerate(rows):
            row_type, source_id, folder_id = row
            if row_type == 'personal':
                display_name = "個人資料夾"
            else:
                display_name = get_source_name('group', source_id) or source_id

            logger.info("[LISTGROUP] Processing group %d: id=%s, name=%s, folder_id=%s", 
                        i, source_id, display_name, folder_id)
            
            # 建立 Carousel 欄位
            if i < 9 or (i == 9 and len(rows) <= 10):  # 前9個或最後一個（如果總數<=10）
                actions = []
                if folder_id:
                    actions.append(URIAction(
                        label='開啟資料夾',
                        uri=f"https://drive.google.com/drive/folders/{folder_id}?authuser=0&openExternalBrowser=1"
                    ))
                else:
                    actions.append(MessageAction(
                        label='尚未建立資料夾',
                        text='尚未建立對應的 Google Drive 資料夾'
                    ))
                
                columns.append(CarouselColumn(
                    title=display_name[:40],  # LINE限制標題長度
                    text=f"{source_id}"[:60],  # LINE限制內文長度
                    actions=actions
                ))
            elif i == 9:  # 第10個且總數>10
                # 新增「顯示更多」按鈕，加入必要的 label 參數
                columns.append(CarouselColumn(
                    title="還有更多群組",
                    text=f"還有 {len(rows) - 9} 個群組",
                    actions=[
                        MessageAction(
                            label="顯示完整清單",  # 加入必要的 label 參數
                            text='!showcompletegroup'
                        )
                    ]
                ))
                break
            
        carousel_template = CarouselTemplate(columns=columns)
        template_message = TemplateMessage(
            alt_text='您的群組列表',
            template=carousel_template
        )
        logger.info("[LISTGROUP] Sending carousel template with %d columns", len(columns))

        reply_message(reply_token, [template_message])
    except Exception as e:
        logger.error(f"[LISTGROUP] Error processing listgroup command: {str(e)}")
        raise
    finally:
        conn.close()

@celery.task
def handle_update_folder_task(reply_token, user_id):
    """更新使用者綁定群組的資料夾名稱
    
    此任務會檢查使用者綁定的所有群組資料夾，並確保資料夾名稱與群組名稱一致。
    具體檢查內容:
    1. 檢查資料夾是否存在 (404錯誤表示資料夾不存在)
    2. 檢查資料夾是否有效 (是否可以訪問或已被刪除)
    3. 檢查資料夾名稱是否與群組名稱一致
    
    參數:
        reply_token (str): LINE 回覆令牌
        user_id (str): LINE 使用者 ID
    """
    logger = logging.getLogger('celery')
    logger.info("[UPDATEFOLDER] 開始檢查使用者 %s 的群組資料夾", user_id)
    
    # 取得使用者的 Google OAuth 憑證
    user_creds = get_user_credentials(user_id)
    if not user_creds:
        logger.error("[UPDATEFOLDER] 無法獲取使用者 %s 的 Google 憑證", user_id)
        reply_message(reply_token, [TextMessage(text="您尚未綁定 Google 帳號，請先輸入 !bindgoogle")])
        return
    
    # 建立 Drive Service
    service = build('drive', 'v3', credentials=user_creds)
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 查詢使用者綁定的所有群組與對應資料夾
            logger.info("[UPDATEFOLDER] 查詢使用者 %s 的所有群組與資料夾映射", user_id)
            cursor.execute('''
                SELECT gu.group_id, fm.folder_id, gi.name 
                FROM group_users gu 
                LEFT JOIN folder_map fm 
                    ON fm.source_id = gu.group_id 
                    AND fm.user_id = gu.user_id 
                LEFT JOIN group_info gi
                    ON gi.group_id = gu.group_id
                WHERE gu.user_id = %s AND fm.folder_id IS NOT NULL
                ''', (user_id,))
                
            groups = cursor.fetchall()
            
            if not groups:
                reply_message(reply_token, [TextMessage(text="您尚未綁定任何群組或尚未建立群組資料夾。")])
                return
            
            updated_count = 0        # 已更新的資料夾數量
            not_found_count = 0      # 找不到的資料夾數量
            no_change_count = 0      # 無需更新的資料夾數量
            error_count = 0          # 發生錯誤的資料夾數量
            access_error_count = 0   # 訪問權限錯誤的資料夾數量
            
            update_results = []
            
            # 檢查每個群組的資料夾
            for group_id, folder_id, stored_name in groups:
                try:
                    # 1. 檢查資料夾是否存在與是否有效 - 透過嘗試獲取資料夾元數據
                    try:
                        # 嘗試獲取資料夾詳細資訊，包括資料夾名稱、權限等
                        folder_metadata = service.files().get(
                            fileId=folder_id, 
                            fields='name,capabilities,trashed'
                        ).execute()
                        
                        folder_name = folder_metadata.get('name')
                        is_trashed = folder_metadata.get('trashed', False)
                        can_edit = folder_metadata.get('capabilities', {}).get('canEdit', False)
                        
                        # 檢查資料夾是否已被移到垃圾桶
                        if is_trashed:
                            logger.warning("[UPDATEFOLDER] 資料夾已被移到垃圾桶 (folder_id=%s, group_id=%s)",
                                        folder_id, group_id)
                            update_results.append(f"🗑️ 資料夾已被移到垃圾桶: {stored_name or group_id}")
                            not_found_count += 1
                            continue
                            
                        # 檢查是否有編輯權限
                        if not can_edit:
                            logger.warning("[UPDATEFOLDER] 沒有資料夾編輯權限 (folder_id=%s, group_id=%s)",
                                        folder_id, group_id)
                            update_results.append(f"🔒 沒有編輯權限: {stored_name or group_id}")
                            access_error_count += 1
                            continue
                            
                        # 2. 獲取最新的群組名稱（用於檢查資料夾名稱是否需要更新）
                        current_name = get_source_name('group', group_id)
                        
                        if not current_name:
                            logger.warning("[UPDATEFOLDER] 無法獲取群組 %s 的名稱，使用資料庫中的名稱", group_id)
                            current_name = stored_name or group_id
                        
                        # 3. 檢查資料夾名稱是否與群組名稱一致
                        if folder_name != current_name:
                            # 更新資料夾名稱
                            service.files().update(
                                fileId=folder_id,
                                body={'name': current_name}
                            ).execute()
                            logger.info("[UPDATEFOLDER] 已更新資料夾名稱: %s -> %s (folder_id=%s, group_id=%s)",
                                    folder_name, current_name, folder_id, group_id)
                            update_results.append(f"✅ 已更新: {folder_name} → {current_name}")
                            updated_count += 1
                        else:
                            logger.info("[UPDATEFOLDER] 資料夾名稱無需更新: %s (folder_id=%s, group_id=%s)",
                                    folder_name, folder_id, group_id)
                            no_change_count += 1
                            
                    except HttpError as e:
                        # 處理不同類型的 API 錯誤
                        if e.resp.status == 404:
                            # 資料夾不存在
                            logger.warning("[UPDATEFOLDER] 找不到資料夾 (folder_id=%s, group_id=%s): %s",
                                        folder_id, group_id, str(e))
                            update_results.append(f"⚠️ 找不到資料夾: {stored_name or group_id}")
                            not_found_count += 1
                            
                            # 可以選擇從映射表中刪除此記錄
                            delete_folder_map(group_id, user_id)
                            logger.info("[UPDATEFOLDER] 已從資料庫中刪除不存在的資料夾映射記錄 (folder_id=%s)", folder_id)
                            
                        elif e.resp.status == 403:
                            # 沒有權限訪問
                            logger.warning("[UPDATEFOLDER] 沒有權限訪問資料夾 (folder_id=%s, group_id=%s): %s",
                                        folder_id, group_id, str(e))
                            update_results.append(f"🔒 沒有訪問權限: {stored_name or group_id}")
                            access_error_count += 1
                        else:
                            # 其他 API 錯誤
                            logger.error("[UPDATEFOLDER] 更新資料夾時發生 API 錯誤 (folder_id=%s, group_id=%s): %s",
                                        folder_id, group_id, str(e))
                            update_results.append(f"❌ 更新失敗: {stored_name or group_id}")
                            error_count += 1
                            
                except Exception as e:
                    # 處理其他未預期的錯誤
                    logger.error("[UPDATEFOLDER] 處理群組 %s 資料夾時發生錯誤: %s", group_id, str(e))
                    update_results.append(f"❌ 處理錯誤: {stored_name or group_id}")
                    error_count += 1
            
            # 生成回覆訊息
            total = len(groups)
            summary = f"資料夾檢查完成 ({total} 個群組):\n"
            summary += f"✅ {updated_count} 個已更新\n"
            summary += f"✓ {no_change_count} 個無需更新\n"
            
            if not_found_count > 0:
                summary += f"⚠️ {not_found_count} 個找不到或已刪除\n"
            if access_error_count > 0:
                summary += f"🔒 {access_error_count} 個權限錯誤\n"
            if error_count > 0:
                summary += f"❌ {error_count} 個發生錯誤\n"
                
            if update_results:
                details = "\n".join(update_results[:10])  # 限制最多顯示10個詳細結果
                if len(update_results) > 10:
                    details += f"\n(還有 {len(update_results) - 10} 個結果未顯示)"
                reply_text = f"{summary}\n\n詳細結果:\n{details}"
            else:
                reply_text = summary
                
            reply_message(reply_token, [TextMessage(text=reply_text)])
            
    except Exception as e:
        logger.error("[UPDATEFOLDER] 更新資料夾時發生錯誤: %s", str(e))
        reply_message(reply_token, [TextMessage(text="檢查群組資料夾時發生錯誤，請稍後再試。")])
    finally:
        conn.close()

@celery.task
def retry_failed_uploads_task(reply_token, user_id, hours_ago=24):
    """重試指定使用者在指定時間內失敗的上傳
    
    參數:
        reply_token (str): LINE 回覆令牌
        user_id (str): LINE 使用者 ID
        hours_ago (int): 檢索多少小時前的失敗記錄
    """
    logger = logging.getLogger('celery')
    logger.info(f"[RETRY] 開始處理使用者 {user_id} 的失敗上傳（{hours_ago}小時內）...")
    
    # 重試結果統計
    results = {
        "找到需重試記錄": 0,
        "檔案不存在": 0,
        "重試成功": 0,
        "重試失敗": 0,
        "無法處理": 0
    }
    
    # 從資料庫獲取該使用者的失敗記錄
    conn = get_db_connection()
    failed_uploads = []
    try:
        with conn.cursor() as cursor:
            # 查詢特定使用者的失敗上傳記錄
            cursor.execute("""
                SELECT id, file_name, source_type, source_id, status 
                FROM upload_logs 
                WHERE user_id = %s 
                  AND upload_time > DATE_SUB(NOW(), INTERVAL %s HOUR)
                  AND status NOT LIKE 'success'
                  AND status NOT LIKE '重試成功'
                ORDER BY upload_time DESC
            """, (user_id, hours_ago))
            failed_uploads = cursor.fetchall()
            results["找到需重試記錄"] = len(failed_uploads)
            logger.info(f"[RETRY] 找到 {len(failed_uploads)} 筆失敗記錄")
    
        # 處理每一筆失敗記錄
        for record in failed_uploads:
            record_id, file_name, source_type, source_id, status = record
            logger.info(f"[RETRY] 處理記錄 ID={record_id}, 檔案={file_name}")
            
            # 檢查檔案是否存在
            dist_path = os.path.join(STATIC_TMP_PATH, file_name)
            if not os.path.exists(dist_path):
                logger.warning(f"[RETRY] 檔案不存在: {dist_path}")
                results["檔案不存在"] += 1
                
                # 嘗試查找只有基本名稱匹配的檔案
                base_name = os.path.basename(file_name)
                found = False
                for tmp_file in os.listdir(STATIC_TMP_PATH):
                    if base_name in tmp_file:
                        dist_path = os.path.join(STATIC_TMP_PATH, tmp_file)
                        logger.info(f"[RETRY] 找到可能匹配的檔案: {tmp_file}")
                        found = True
                        break
                
                if not found:
                    # 更新日誌狀態
                    with conn.cursor() as cursor:
                        cursor.execute(
                            "UPDATE upload_logs SET status = %s WHERE id = %s",
                            (f"檔案不存在 (重試於 {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')})", record_id)
                        )
                    conn.commit()
                    continue
            
            # 重試上傳
            try:
                logger.info(f"[RETRY] 嘗試重新上傳: {file_name}, 來源: {source_type}/{source_id}")
                
                
                # 執行上傳任務
                upload_file_to_drive_task(dist_path, file_name, source_type, source_id, user_id)
                
                # 更新日誌狀態
                with conn.cursor() as cursor:
                    cursor.execute(
                        "UPDATE upload_logs SET status = %s WHERE id = %s",
                        ("重試成功", record_id)
                    )
                conn.commit()
                results["重試成功"] += 1
                logger.info(f"[RETRY] 重試上傳成功: {file_name}")
                
            except Exception as e:
                logger.error(f"[RETRY] 重試上傳失敗: {str(e)}")
                results["重試失敗"] += 1
                
                # 更新日誌狀態
                with conn.cursor() as cursor:
                    cursor.execute(
                        "UPDATE upload_logs SET status = %s WHERE id = %s",
                        ("重試失敗", record_id)
                    )
                conn.commit()
    
    except Exception as e:
        logger.error(f"[RETRY] 執行補上傳任務時發生錯誤: {str(e)}")
    finally:
        conn.close()
    
    # 回報結果
    reply_text = f"您的補上傳結果：\n"
    reply_text += f"✓ 找到失敗記錄：{results['找到需重試記錄']}筆\n"
    if results["找到需重試記錄"] > 0:
        reply_text += f"✓ 成功補上傳：{results['重試成功']}筆\n"
        if results["檔案不存在"] > 0:
            reply_text += f"✗ 檔案已不存在：{results['檔案不存在']}筆\n"
        if results["重試失敗"] > 0:
            reply_text += f"✗ 重試失敗：{results['重試失敗']}筆\n"

    
    if reply_token:
        reply_message(reply_token, [TextMessage(text=reply_text)])
    return results

@celery.task
def list_failed_uploads_task(reply_token, user_id, hours_ago=24):
    """列出指定使用者在指定時間內的失敗上傳記錄
    
    參數:
        reply_token (str): LINE 回覆令牌
        user_id (str): LINE 使用者 ID
        hours_ago (int): 檢索多少小時前的失敗記錄
    """
    logger = logging.getLogger('celery')
    logger.info(f"[LIST_FAILED] 開始查詢使用者 {user_id} 的失敗上傳（{hours_ago}小時內）...")
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            logger.info("[LIST_FAILED] 查詢使用者的失敗上傳記錄")
            # 查詢特定使用者的失敗上傳記錄
            cursor.execute("""
                SELECT id, file_name, upload_time, source_type, source_id, status 
                FROM upload_logs 
                WHERE user_id = %s 
                  AND upload_time > DATE_SUB(NOW(), INTERVAL %s HOUR)
                  AND status NOT LIKE 'success'
                  AND status NOT LIKE '重試成功'
                ORDER BY upload_time DESC
            """, (user_id, hours_ago))
            logger.info("[LIST_FAILED] 取得失敗記錄")
            failed_logs = cursor.fetchall()
            logger.info("[LIST_FAILED] 取得失敗記錄至變數")
            
            
            # 準備回覆訊息
            if failed_logs is None or len(failed_logs) == 0:
                reply_text = f"過去 {hours_ago} 小時內沒有失敗的上傳紀錄。"
                reply_message(reply_token, [TextMessage(text=reply_text)])
                logger.info("[LIST_FAILED] 無失敗記錄，返回空訊息")
                return
            
            # 計算統計資訊
            count_by_source_type = {}  # 依來源類型統計
            count_by_status = {}       # 依狀態統計
            
            # 準備詳細列表
            logs_text = []
            for i, (record_id, file_name, upload_time, source_type, source_name, status) in enumerate(failed_logs, 1):
                # 更新統計
                count_by_source_type[source_type] = count_by_source_type.get(source_type, 0) + 1
                
                # 統計各種錯誤類型
                error_type = "其他錯誤"
                if "connection_error" in status:
                    error_type = "連線錯誤"
                elif "timeout" in status:
                    error_type = "超時錯誤"
                elif "user_credentials_error" in status:
                    error_type = "認證錯誤"
                elif "http_error" in status:
                    error_type = "API錯誤"
                elif "檔案不存在" in status:
                    error_type = "檔案不存在"
                
                count_by_status[error_type] = count_by_status.get(error_type, 0) + 1
                
                # 組合詳細記錄
                logs_text.append(
                    f"{i}. 檔案：{file_name}\n"
                    f"   時間：{upload_time}\n"
                    f"   來源：{source_type} ({source_name})\n"
                    f"   狀態：{status}\n"
                )
            
            # 組合摘要與詳細記錄
            summary = f"過去 {hours_ago} 小時內失敗上傳記錄摘要：\n"
            summary += f"• 總計失敗記錄：{len(failed_logs)}筆\n"
            
            # 來源類型摘要
            summary += "\n【依來源類型】\n"
            for source_type, count in count_by_source_type.items():
                summary += f"• {source_type}: {count}筆\n"
            
            # 錯誤類型摘要
            summary += "\n【依錯誤類型】\n"
            for error_type, count in count_by_status.items():
                summary += f"• {error_type}: {count}筆\n"
            
            summary += "\n要重試這些失敗的上傳，請使用 !retryupload 指令。\n"
            
            # 合併訊息，如果太長，就建立暫存檔並提供連結
            full_text = summary + "\n詳細記錄：\n\n" + "\n".join(logs_text)
            
            if len(full_text) > 1000:
                with tempfile.NamedTemporaryFile(delete=False, dir=STATIC_LOGS_PATH, suffix=".txt") as temp_file:
                    temp_file.write(full_text.encode('utf-8'))
                    temp_file_name = os.path.basename(temp_file.name)
                
                # 產生檔案 URL
                file_url = f"https://{BASE_URI}/linebot/logs/{temp_file_name}"
                
                # 使用 ButtonsTemplate 讓用戶可以查看完整記錄
                button_template = TemplateMessage(
                    alt_text="失敗上傳記錄",
                    template=ButtonsTemplate(
                        title="失敗上傳記錄",
                        text="詳細記錄請點下方按鈕查看完整內容",
                        actions=[
                            URIAction(
                                label="查看完整記錄",
                                uri=file_url
                            ),
                            MessageAction(
                                label="重試上傳",
                                text=f"!retryupload {hours_ago}"
                            )
                        ]
                    )
                )
                reply_message(reply_token, [button_template])
            else:
                # 直接回覆完整訊息
                reply_message(reply_token, [TextMessage(text=full_text)])
                
    except Exception as e:
        logger.error(f"[LIST_FAILED] 處理失敗上傳記錄時發生錯誤: {str(e)}")
        reply_message(reply_token, [TextMessage(text="查詢失敗上傳記錄時發生錯誤，請稍後再試。")])
    finally:
        conn.close()

@celery.task
def handle_show_complete_group_task(reply_token, user_id):
    logger = logging.getLogger('celery')
            
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 查詢使用者綁定的所有群組
            logger.info("[SHOWCOMPLETEGROUP] Querying all groups for user_id=%s", user_id)
            cursor.execute('''
                SELECT gu.group_id, fm.folder_id, gi.name 
                FROM group_users gu 
                LEFT JOIN folder_map fm 

                    ON fm.source_id = gu.group_id 
                    AND fm.user_id = gu.user_id 
                LEFT JOIN group_info gi
                    ON gi.group_id = gu.group_id
                WHERE gu.user_id = %s
                ORDER BY gi.name, gu.group_id
                ''', (user_id,))
                

            groups = cursor.fetchall()
            
            if not groups:
                reply_text = "您尚未綁定任何群組。"
            else:
                all_groups_text = []
                for i, (group_id, folder_id, group_name) in enumerate(groups, 1):
                    group_text = f"{i}. {group_name or '未命名群組'}\n"
                    group_text += f"ID: {group_id}\n"
                    if folder_id:
                        folder_url = f"https://drive.google.com/drive/folders/{folder_id}?authuser=0&openExternalBrowser=1"
                        group_text += f"資料夾連結: {folder_url}"
                    else:
                        group_text += "尚未建立對應的 Google Drive 資料夾"
                    all_groups_text.append(group_text)
                
                reply_text = "以下是完整的群組清單：\n\n" + '\n\n'.join(all_groups_text)

            logger.info("[SHOWCOMPLETEGROUP] 找到 %d 個群組", len(groups))
            
        reply_message(reply_token, [TextMessage(text=reply_text)])
    except Exception as e:
        logger.error("[SHOWCOMPLETEGROUP] 處理群組列表時發生錯誤: %s", str(e))
        reply_text = "取得群組列表時發生錯誤，請稍後再試。"
        reply_message(reply_token, [TextMessage(text=reply_text)])
    finally:
        conn.close()
