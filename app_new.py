# app_new.py

# === 基本設定 ===
from settings import *

import datetime
import logging
import os
import secrets

from google_auth_oauthlib.flow import Flow


from flask import Flask, request, abort, jsonify, redirect, url_for, session, render_template, send_from_directory
from werkzeug.middleware.proxy_fix import ProxyFix

from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.webhooks import (
    MessageEvent,
    TextMessageContent,
    ImageMessageContent,
    VideoMessageContent,
    AudioMessageContent,
    FileMessageContent,
    JoinEvent,
    LeaveEvent
)
from linebot.v3.messaging import (
    TextMessage,
    Configuration,
    ApiClient,
    MessagingApi,
    ReplyMessageRequest,
    TemplateMessage,
    ButtonsTemplate,
    URIAction,
    MessageAction,
    ShowLoadingAnimationRequest
)

import pymysql
from celery import group, chord, chain
from dbutils.pooled_db import PooledDB

# 載入 Celery worker_app
from worker_app import (
    celery,
    upload_file_to_drive_task,
    get_source_name,
    check_google_status,
    download_line_file_task,
    handle_upload_task,
    showlog_task,
    handle_list_group_task,
    handle_show_complete_group_task,
    handle_update_folder_task,
    retry_failed_uploads_task,
    list_failed_uploads_task
)
from redis import Redis
from datetime import timedelta


app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_host=1, x_proto=1)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
app.logger.setLevel(logging.INFO)

app.secret_key = FLASK_SECRET_KEY

handler = WebhookHandler(CHANNEL_SECRET)
configuration = Configuration(
    access_token=CHANNEL_ACCESS_TOKEN,
)

# 建立資料庫連線池
db_pool = PooledDB(
    creator=pymysql,
    maxconnections=10,     # Web 應用通常需要更多連線
    mincached=2,           # 初始化時的最小空閒連線
    maxcached=5,           # 最大空閒連線
    maxshared=3,           # 共享連線數量
    blocking=True,         # 連線池滿時等待
    host=DATABASE["HOST"],
    user=DATABASE["USER"],
    password=DATABASE["PASSWORD"],
    database=DATABASE["DB"],
    charset='utf8mb4',
    autocommit=False,
    ping=1
)

# 初始化 Redis 連線
redis_client = Redis(host='localhost', port=6379, db=1, decode_responses=True)

def get_db_connection():
    """從連線池取得資料庫連線"""
    return db_pool.connection()

def init_db():
    """初始化資料庫表格"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 建立 user_tokens 表格（儲存使用者的 Google OAuth token）
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_tokens (
                    user_id VARCHAR(255) PRIMARY KEY,
                    token TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
                        ON UPDATE CURRENT_TIMESTAMP
                )
            ''')
            
            # 建立 verification_codes 表格（用於 OAuth 流程驗證）
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS verification_codes (
                    code VARCHAR(255) PRIMARY KEY,
                    user_id VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 建立 group_users 表格（記錄群組與使用者的綁定關係）
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS group_users (
                    group_id VARCHAR(255),
                    user_id VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (group_id, user_id)
                )
            ''')
            
            # 建立 folder_map 表格（記錄群組/使用者與 Google Drive 資料夾的對應）
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS folder_map (
                    source_id VARCHAR(255),
                    user_id VARCHAR(255),
                    folder_id VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (source_id, user_id)
                )
            ''')
            
            # 建立 group_info 表格（記錄群組資訊）
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS group_info (
                    group_id VARCHAR(255) PRIMARY KEY,
                    name VARCHAR(255),
                    type VARCHAR(50),
                    status VARCHAR(20) DEFAULT 'active',
                    left_at TIMESTAMP NULL,
                    joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
                        ON UPDATE CURRENT_TIMESTAMP
                )
            ''')
            
            # 建立 user_status 表格（記錄使用者狀態設定）
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_status (
                    user_id VARCHAR(255) PRIMARY KEY,
                    debug_mode BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
                        ON UPDATE CURRENT_TIMESTAMP
                )
            ''')

            # 建立 upload_logs 表格（記錄上傳紀錄）
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS upload_logs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id VARCHAR(255),
                file_name VARCHAR(255),
                upload_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source_type VARCHAR(50),
                source_id VARCHAR(255),
                source_name VARCHAR(255),
                status VARCHAR(255)
                )
            ''')
            
        conn.commit()
    finally:
        conn.close()

def reply_loading_animation(chat_id, seconds=5):
    with ApiClient(configuration) as api_client:
        line_bot_api = MessagingApi(api_client)
        # 顯示載入動畫
        request = ShowLoadingAnimationRequest(chatId=chat_id, loadingSeconds=seconds)
        line_bot_api.show_loading_animation(request)

# 建立暫存資料夾
def make_static_tmp_dir():
    """建立暫存檔案目錄"""
    try:
        os.makedirs(STATIC_TMP_PATH, exist_ok=True)
        os.makedirs(STATIC_LOGS_PATH, exist_ok=True)
    except Exception as e:
        app.logger.error("[SYSTEM] Failed to create temp directory: %s", str(e))

def reply_message(reply_token, messages):
    """傳送回覆訊息
    
    參數:
        reply_token (str): LINE 回覆令牌
        messages (list): 要發送的訊息列表
    """
    with ApiClient(configuration) as api_client:
        line_bot_api = MessagingApi(api_client)
        line_bot_api.reply_message_with_http_info(
            ReplyMessageRequest(
                reply_token=reply_token,
                messages=messages
            )
        )

@app.route("/linebot/callback_LineBot", methods=['POST'])
def callback():
    try:
        signature = request.headers['X-Line-Signature']
        body = request.get_data(as_text=True)
        app.logger.info("[LINE] Received webhook request: %s", body)
        handler.handle(body, signature)
    except InvalidSignatureError:
        app.logger.error("[LINE] Invalid signature detected")
        abort(400)
    except Exception as e:
        app.logger.error("[LINE] Unexpected error occurred: %s", str(e))
        return jsonify({'error': 'Unexpected error occurred'}), 500

    return 'OK'

def parse_time_str(time_str):
    """解析 MMDDHHMM 格式的時間字串
    
    參數:
        time_str (str): MMDDHHMM 格式的時間字串
    
    回傳:
        datetime 或 None: 解析成功返回 datetime 物件，失敗返回 None
    """
    try:
        if len(time_str) != 8:
            return None
            
        month = int(time_str[0:2])
        day = int(time_str[2:4])
        hour = int(time_str[4:6])
        minute = int(time_str[6:8])
        
        # 使用當前年份
        current_year = datetime.datetime.now().year
        
        return datetime.datetime(current_year, month, day, hour, minute)
    except (ValueError, IndexError):
        return None

def check_and_update_name_verify_attempts(user_id):
    """檢查並更新使用者使用名稱驗證的嘗試次數（bind 和 unbind 共用）
    
    參數:
        user_id (str): LINE 使用者 ID
    
    回傳:
        tuple: (是否允許嘗試, 剩餘次數, 錯誤訊息)
    """
    key = f"name_verify_attempts:{user_id}"
    
    # 取得目前嘗試次數
    attempts = redis_client.get(key)
    if attempts is None:
        # 初始化嘗試次數（每小時5次）
        redis_client.setex(key, timedelta(hours=1), 5)
        return True, 5, None
    
    attempts = int(attempts)
    if attempts <= 0:
        ttl = redis_client.ttl(key)
        minutes = (ttl + 59) // 60  # 無條件進位到分鐘
        error_msg = f"您已超過使用名稱驗證的嘗試次數上限，請等待約 {minutes} 分鐘後再試，或使用群組 ID 進行驗證。"
        return False, 0, error_msg
    
    # 更新嘗試次數
    redis_client.decr(key)
    return True, attempts - 1, None

def reset_name_verify_attempts(user_id):
    """重置使用者的名稱驗證嘗試次數"""
    key = f"name_verify_attempts:{user_id}"
    redis_client.delete(key)

def handle_group_command(event, is_bind=True):
    """處理群組綁定/解除綁定指令
    
    參數:
        event: LINE 訊息事件
        is_bind (bool): True 為綁定，False 為解除綁定
    """
    text = event.message.text.strip()
    line_user_id = event.source.user_id
    command = "綁定" if is_bind else "解除綁定"
    parts = text.split()
    
    if len(parts) not in [2, 3]:
        return (
            f"請使用以下其中一種格式來{command}群組：\n"
            f"1. !{command.lower()}group <GROUP_ID>\n"
            f"2. !{command.lower()}group <群組名稱> <加入時間>\n"
            "時間格式：MMDDHHMM（月日時分）"
        )

    if len(parts) == 2:
        # 使用 ID 驗證 - 不限制嘗試次數
        line_group_id = parts[1]
        # 檢查群組是否存在
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    'SELECT 1 FROM group_info WHERE group_id = %s',
                    (line_group_id,)
                )
                if not cursor.fetchone():
                    return "找不到此群組。"
        finally:
            conn.close()
    else:
        # 使用群組名稱和時間驗證 - 限制嘗試次數
        is_allowed, remaining_attempts, error_msg = check_and_update_name_verify_attempts(line_user_id)
        if not is_allowed:
            return error_msg

        group_name = parts[1]
        time_str = parts[2]
        
        parsed_time = parse_time_str(time_str)
        if not parsed_time:
            return f"時間格式錯誤（剩餘 {remaining_attempts} 次嘗試機會）：請使用 MMDDHHMM 格式（例：01221935 表示 1月22日19:35）"
        
        # 查詢符合條件的群組
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    '''SELECT group_id FROM group_info 
                       WHERE name = %s 
                       AND ABS(TIMESTAMPDIFF(SECOND, joined_at, %s)) <= 60''',
                    (group_name, parsed_time)
                )
                result = cursor.fetchone()
                if not result:
                    return f"找不到符合的群組（剩餘 {remaining_attempts} 次嘗試機會）：請確認群組名稱和加入時間是否正確。"
                
                line_group_id = result[0]
                if not line_group_id.startswith('C'):
                    return f"找到的群組 ID 格式不正確（剩餘 {remaining_attempts} 次嘗試機會）。"
                
                # 驗證成功，重置嘗試次數
                reset_name_verify_attempts(line_user_id)
        finally:
            conn.close()

    # 執行綁定/解除綁定操作
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            if is_bind:
                # 檢查 Google 帳號綁定
                cursor.execute('SELECT id FROM user_tokens WHERE user_id = %s', (line_user_id,))
                if not cursor.fetchone():
                    return "您尚未綁定 Google 帳號，請先輸入 !bindgoogle。"
                
                # 執行綁定
                try:
                    cursor.execute(
                        'INSERT INTO group_users (group_id, user_id) VALUES (%s, %s)',
                        (line_group_id, line_user_id)
                    )
                    conn.commit()
                    return "已將您與群組綁定成功！\n未來該群組收到的檔案都會同步上傳到您的 Google Drive。"
                except pymysql.err.IntegrityError:
                    return "您已經與此群組綁定過了。"
            else:
                # 執行解除綁定
                cursor.execute(
                    'SELECT 1 FROM group_users WHERE group_id = %s AND user_id = %s',
                    (line_group_id, line_user_id)
                )
                if cursor.fetchone():
                    cursor.execute(
                        'DELETE FROM group_users WHERE group_id = %s AND user_id = %s',
                        (line_group_id, line_user_id)
                    )
                    conn.commit()
                    return "已解除綁定此群組。"
                else:
                    return "您尚未綁定此群組。"
    finally:
        conn.close()

def handle_unbind_all_group(event):
    app.logger.info("[test] unbind all group")
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 先檢查使用者綁定的群組數量
            cursor.execute(
                'SELECT COUNT(*) FROM group_users WHERE user_id = %s',
                (event.source.user_id,)
            )
            group_count = cursor.fetchone()[0]
            
            if group_count == 0:
                reply_text = "您目前沒有綁定任何群組。"
            else:
                # 刪除所有群組綁定
                cursor.execute(
                    'DELETE FROM group_users WHERE user_id = %s',
                    (event.source.user_id,)
                )
                conn.commit()
                reply_text = f"已成功解除綁定所有群組（共 {group_count} 個）。"
                app.logger.info(
                    "[UNBIND_ALL] User %s unbound from %d groups",
                    event.source.user_id, group_count
                )
    except Exception as e:
        app.logger.error("[UNBIND_ALL] Error: %s", str(e))
        reply_text = "解除綁定過程發生錯誤，請稍後再試。"
    finally:
        conn.close()
    return reply_text

def handle_bind_google(event):
    app.logger.info("[test] bind google")
    line_user_id = event.source.user_id
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute('SELECT id FROM user_tokens WHERE user_id = %s', (line_user_id,))
            row = cursor.fetchone()
            
            if row:
                messages = [TextMessage(text="您已綁定過 Google 帳號。")]
            else:
                verification_code = secrets.token_urlsafe(24)  # 產生32個字元的安全隨機字串
                cursor.execute('DELETE FROM verification_codes WHERE user_id = %s', (line_user_id,))
                cursor.execute(
                    'INSERT INTO verification_codes (code, user_id) VALUES (%s, %s)',
                    (verification_code, line_user_id)
                )
                conn.commit()
                
                authorize_url = (
                    f"https://{BASE_URI}/linebot/authorize"
                    f"?line_user_id={line_user_id}"
                    f"&verification_code={verification_code}"
                    "&openExternalBrowser=1"
                )
                messages = [
                    TemplateMessage(
                        alt_text="Google 帳號綁定",
                        template=ButtonsTemplate(
                            title="Google 帳號綁定",
                            text="請點擊下方按鈕進行 Google 帳號綁定",
                            actions=[
                                URIAction(
                                    label="綁定 Google 帳號",
                                    uri=authorize_url
                                )
                            ]
                        )
                    )
                ]
    finally:
        conn.close()
    reply_message(event.reply_token, messages)
    return

def handle_unbind_google(event):
    app.logger.info("[test] unbind google")
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute('DELETE FROM user_tokens WHERE user_id = %s', (event.source.user_id,))
        conn.commit()
        reply_text = "已解除綁定 Google 帳號。"
    except Exception as e:
        app.logger.error("[UNBIND_GOOGLE] Error: %s", str(e))
        reply_text = "解除綁定過程發生錯誤，請稍後再試。"
    finally:
        conn.close()
    return reply_text

def handle_google_action(event):
    app.logger.info("[test] google action")
    reply_text = [
        TemplateMessage(
            alt_text="Google 帳號操作",
            template=ButtonsTemplate(
                title="Google 帳號操作",
                text="請選擇您要進行的操作",
                actions=[
                    MessageAction(
                        label="綁定 Google 帳號",
                        text="!bindgoogle"
                    ),
                    MessageAction(
                        label="解綁 Google 帳號",
                        text="!unbindgoogle"
                    ),
                    MessageAction(
                        label="檢查 Google 狀態",
                        text="!checkgoogle"
                    ),
                    MessageAction(
                        label="測試上傳",
                        text="!testupload"
                    )
                ]
            )
        )
    ]
    reply_message(event.reply_token, reply_text)
    return None

def handle_check_google(event):
    app.logger.info("[test] check google")
    line_user_id = event.source.user_id
    reply_loading_animation(line_user_id, 10)
    check_google_status.delay(event.reply_token, line_user_id)
    return None

def handle_switch_debug(event):
    app.logger.info("[test] switch debug")
    line_user_id = event.source.user_id
    app.logger.info("[SWITCHDEBUG] Processing debug mode switch for user: %s", line_user_id)
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # 檢查用戶是否存在
                cursor.execute('SELECT debug_mode FROM user_status WHERE user_id = %s', (line_user_id,))
                result = cursor.fetchone()
                
                if result is None:
                    # 用戶不存在，創建新記錄
                    cursor.execute('INSERT INTO user_status (user_id, debug_mode) VALUES (%s, %s)',
                                    (line_user_id, True))
                    debug_mode = True
                    reply_text = "已創建用戶設定並開啟 debug 模式。"
                else:
                    # 切換現有用戶的 debug 模式
                    debug_mode = not result[0]
                    cursor.execute('UPDATE user_status SET debug_mode = %s WHERE user_id = %s',
                                    (debug_mode, line_user_id))
                    reply_text = "已開啟 debug 模式。" if debug_mode else "已關閉 debug 模式。"
                
                conn.commit()
                app.logger.info("[SWITCHDEBUG] Debug mode for user %s set to: %s",
                                line_user_id, debug_mode)
                
    except Exception as e:
        app.logger.error("[SWITCHDEBUG] Error occurred: %s", str(e))
        reply_text = "設定 debug 模式時發生錯誤，請稍後再試。"
    
    return reply_text

def handle_show_log(event):
    app.logger.info("[test] show log")
    line_user_id = event.source.user_id
    reply_loading_animation(line_user_id, 20)
    showlog_task.delay(event.reply_token, line_user_id)
    return None

def handle_group_action(event):
    app.logger.info("[test] group action")
    reply_text = [
        TemplateMessage(
            alt_text="群組操作",
            template=ButtonsTemplate(
                title="群組操作",
                text="請選擇您要進行的操作，在私訊中管理群組時，請依指示操作",
                actions=[
                    MessageAction(
                        label="綁定群組",
                        text="!bindgroup"
                    ),
                    MessageAction(
                        label="解綁群組",
                        text="!unbindgroup"
                    ),
                    MessageAction(
                        label="查看群組列表",
                        text="!listgroup"
                    )
                ]
            )
        )
    ]
    reply_message(event.reply_token, reply_text)
    return None

def handle_list_group(event):
    app.logger.info("[test] list group")
    line_user_id = event.source.user_id
    reply_loading_animation(line_user_id, 10)
    handle_list_group_task.delay(event.reply_token, line_user_id)

    return None

def handle_show_complete_group(event):
    app.logger.info("[test] show complete group")
    line_user_id = event.source.user_id
    reply_loading_animation(line_user_id, 10)
    handle_show_complete_group_task.delay(event.reply_token, line_user_id)
    return None

def handle_test_upload(event):
    app.logger.info("[test] test upload")
    line_user_id = event.source.user_id
    dist_path = "static/testimage.jpg"
    dist_name = "testimage.jpg"
    source_type = "user"
    source_id = line_user_id
    
    upload_file_to_drive_task.delay(dist_path, dist_name, source_type, source_id, source_id, event.reply_token)
    reply_loading_animation(line_user_id, 15)
    return None

def handle_update_folder(event):
    app.logger.info("[test] update folder")
    line_user_id = event.source.user_id
    reply_loading_animation(line_user_id, 15)
    handle_update_folder_task.delay(event.reply_token, line_user_id)
    return None

def handle_list_failed_uploads(event):
    """處理列出失敗上傳檔案指令"""
    app.logger.info("[test] list failed uploads")
    line_user_id = event.source.user_id
    
    # 分析指令參數
    text = event.message.text.strip()
    parts = text.split()
    hours = 24  # 預設檢查24小時內的失敗
    
    if len(parts) > 1:
        try:
            hours = int(parts[1])
        except ValueError:
            return "時間參數必須是整數小時"
    
    # 啟動查詢任務
    reply_loading_animation(line_user_id, 10)
    list_failed_uploads_task.delay(event.reply_token, line_user_id, hours)
    return None

def handle_retry_upload(event):
    """處理手動觸發補上傳失敗檔案的指令"""
    app.logger.info("[test] retry upload")
    line_user_id = event.source.user_id
    
    # 分析指令參數
    text = event.message.text.strip()
    parts = text.split()
    hours = 24  # 預設檢查24小時內的失敗
    
    if len(parts) > 1:
        try:
            hours = int(parts[1])
        except ValueError:
            return "時間參數必須是整數小時"
    
    # 啟動補上傳任務
    reply_loading_animation(line_user_id, 20)
    retry_failed_uploads_task.delay(event.reply_token, line_user_id, hours)
    return None

def handle_help(event):
    app.logger.info("[test] help")
    help_message = (
            "📋 指令說明：\n"
            "1️⃣ 綁定 Google 帳號\n"
            "!bindgoogle - 取得 Google 帳號綁定連結\n\n"
            "!unbindgoogle - 解除 Google 帳號綁定\n\n"
            "2️⃣ 綁定群組\n"
            "方式一：在群組中輸入 !bindgroup\n"
            "方式二：使用群組 ID\n"
            "!bindgroup <GROUP_ID>\n"
            "方式三：使用群組名稱和加入時間\n"
            "!bindgroup <群組名稱> <MMDDHHMM>\n"
            "（例：!bindgroup 測試群組 01221935）\n\n"
            "3️⃣ 解除群組綁定\n"
            "方式一：在群組中輸入 !unbindgroup\n"
            "方式二：使用群組 ID\n"
            "!unbindgroup <GROUP_ID>\n"
            "方式三：使用群組名稱和加入時間\n"
            "!unbindgroup <群組名稱> <MMDDHHMM>\n"
            "方式四：解除所有群組綁定\n"
            "!unbindgroup all\n\n"
            "⚠️ 注意事項：\n"
            "• 使用名稱驗證方式每小時限制 5 次嘗試\n"
            "• 時間格式為 MMDDHHMM（月日時分24小時制）\n"
            "• 時間誤差允許正負 1 分鐘\n"
            "• 超過嘗試次數限制可改用群組 ID 方式\n\n"
            "4️⃣ 其他指令\n"
            "!listgroup - 查看已綁定的群組\n"
            "!updatefolder - 更新資料夾名稱與群組同步\n"
            "!listfailed [小時數] - 顯示失敗上傳記錄（預設24小時內）\n"
            "!retryupload [小時數] - 重試失敗的檔案上傳（預設24小時內）\n"
            "!help - 顯示此說明"
        )
    return help_message

def handle_bind_group(event):
    app.logger.info("[test] bind group")
    line_user_id = event.source.user_id
    if event.source.type == 'group':
        line_group_id = event.source.group_id
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # 檢查 user 是否綁定 Google
                cursor.execute('SELECT id FROM user_tokens WHERE user_id = %s', (line_user_id,))
                user_token_row = cursor.fetchone()
                if not user_token_row:
                    reply_text = "您尚未綁定 Google 帳號，請先輸入 !bindgoogle。"
                else:
                    # 新增 group_users
                    try:
                        cursor.execute(
                            'INSERT INTO group_users (group_id, user_id) VALUES (%s, %s)',
                            (line_group_id, line_user_id)
                        )
                        conn.commit()
                        reply_text = (
                            "已將您與此群組綁定！\n"
                            "未來此群組的檔案都會同步上傳到您的 Google Drive。"
                        )
                    except pymysql.err.IntegrityError:
                        reply_text = "您已經與此群組綁定過了。"
        finally:
            conn.close()
        reply_message(event.reply_token, [TextMessage(text=reply_text)])
    else:
        reply_text = [
                    TemplateMessage(
                        alt_text="私訊綁定提示",
                        template=ButtonsTemplate(
                            title="請依指示輸入訊息",
                            text="請點擊下方按鈕然後輸入<Group id>\n或是輸入<群組名稱> <加入時間>\n",
                            actions=[
                                URIAction(
                                    label="綁定群組",
                                    uri="https://line.me/R/oaMessage/%40765hxobx/?%21bindgroup%20"
                                )
                            ]
                        )
                    )
                ]
        reply_message(event.reply_token, reply_text)
    return None

def handle_unbind_group(event):
    app.logger.info("[test] unbind group")
    line_user_id = event.source.user_id
    if event.source.type == 'group':
        line_group_id = event.source.group_id
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    'SELECT * FROM group_users WHERE group_id = %s AND user_id = %s',
                    (line_group_id, line_user_id)
                )
                row = cursor.fetchone()
                if row:
                    cursor.execute(
                        'DELETE FROM group_users WHERE group_id = %s AND user_id = %s',
                        (line_group_id, line_user_id)
                    )
                    conn.commit()
                    reply_text = "已解除綁定此群組。"
                else:
                    reply_text = "您尚未綁定此群組。"
        finally:
            conn.close()
        reply_message(event.reply_token, [TextMessage(text=reply_text)])
    else:
        reply_text = [TemplateMessage(
            alt_text="私訊解除綁定提示",
            template=ButtonsTemplate(
                title="依指示輸入訊息",
                text="請點擊下方按鈕然後輸入 Group id\n或是輸入<群組名稱> <加入時間>",
                actions=[
                    URIAction(
                        label="解除綁定群組",
                        uri="https://line.me/R/oaMessage/%40765hxobx/?%21unbindgroup%20"
                    )
                ]
            )
        )
        ]
        reply_message(event.reply_token, reply_text)
    return None


@handler.add(MessageEvent, message=TextMessageContent)
def handle_text_command_message(event):
    text = event.message.text.strip()
    source_type = event.source.type  # "user" 表示私訊，"group" 表示群組
    line_user_id = event.source.user_id

###################################################################
    # ─── 先處理特殊帶參數的指令 ──────────────────────────────
    # 只處理特殊情況：群組綁定/解除綁定帶參數情形
    if source_type == "user":
        if text.startswith("!bindgroup ") or (text == "!bindgroup"):
            # 處理群組綁定邏輯，只有帶參數的情況才需要特殊處理
            if text.startswith("!bindgroup "):
                reply_text = handle_group_command(event, is_bind=True)
                reply_message(event.reply_token, [TextMessage(text=reply_text)])
                return
        elif text.startswith("!unbindgroup "):
            if text.strip() == "!unbindgroup all":
                reply_text = handle_unbind_all_group(event)
            else:
                reply_text = handle_group_command(event, is_bind=False)
            reply_message(event.reply_token, [TextMessage(text=reply_text)])
            return

    # ─── 定義指令對應字典 ──────────────────────────────
    # 每個項目包含：
    #   - aliases：該功能可觸發的字串（指令語法或自然語言格式）
    #   - handler：實際處理函式
    #   - allowed_context：允許的來源類型（例如：只允許 "user" 表示私訊、"group" 表示群組；或同時允許）
    COMMANDS = {
        "bind_google": {
            "aliases": ["!bindgoogle", "請幫我綁定google帳號"],
            "handler": handle_bind_google,
            "allowed_context": ["user"],  # 私訊才能使用
        },
        "unbind_google": {
            "aliases": ["!unbindgoogle", "請幫我解除google帳號"],
            "handler": handle_unbind_google,
            "allowed_context": ["user"],
        },
        "google_action": {
            "aliases": ["!googleaction"],
            "handler": handle_google_action,
            "allowed_context": ["user"],
        },
        "check_google": {
            "aliases": ["!checkgoogle"],
            "handler": handle_check_google,
            "allowed_context": ["user"],
        },
        "switch_debug": {
            "aliases": ["!switchdebug"],
            "handler": handle_switch_debug,
            "allowed_context": ["user"],
        },
        "show_log": {
            "aliases": ["!showlog"],
            "handler": handle_show_log,
            "allowed_context": ["user"],
        },
        "group_action": {
            "aliases": ["!groupaction"],
            "handler": handle_group_action,  # 顯示群組相關操作的選單
            "allowed_context": ["user"],
        },
        "list_group": {
            "aliases": ["!listgroup"],
            "handler": handle_list_group,
            "allowed_context": ["user"],
        },
        "show_complete_group": {
            "aliases": ["!showcompletegroup"],
            "handler": handle_show_complete_group,
            "allowed_context": ["user"],
        },
        "test_upload": {
            "aliases": ["!testupload"],
            "handler": handle_test_upload,
            "allowed_context": ["user"],
        },
        "bind_group": {
            "aliases": ["!bindgroup", "請幫我綁定群組"],
            "handler": handle_bind_group,  # 處理群組綁定（群組中執行）
            "allowed_context": ["group", "user"],
        },
        "unbind_group": {
            "aliases": ["!unbindgroup", "請幫我解除群組"],
            "handler": handle_unbind_group,
            "allowed_context": ["group", "user"],
        },
        "help": {
            "aliases": ["!help"],
            "handler": handle_help,
            "allowed_context": ["user", "group"],
        },
        "update_folder": {
            "aliases": ["!updatefolder"],
            "handler": handle_update_folder,
            "allowed_context": ["user"],
        },
        "retry_upload": {
            "aliases": ["!retryupload"],
            "handler": handle_retry_upload,
            "allowed_context": ["user"],
        },
        "list_failed_uploads": {
            "aliases": ["!listfailed"],
            "handler": handle_list_failed_uploads,
            "allowed_context": ["user"],
        },
    }


    # ─── 根據 COMMANDS 逐一比對使用者輸入 ──────────────────────────────
    matched_handler = None
    for cmd_info in COMMANDS.values():
        # 檢查是否有任何別名是輸入文字的前綴
        matched_alias = None
        for alias in cmd_info["aliases"]:
            if text == alias or text.startswith(alias + " "):
                matched_alias = alias
                break
                
        if matched_alias:
            # 檢查指令允許的來源
            if source_type not in cmd_info["allowed_context"]:
                # 若來源不符，給予相應警告
                if "user" in cmd_info["allowed_context"]:
                    warning = "此指令僅限私訊使用，請透過私訊輸入。"
                elif "group" in cmd_info["allowed_context"]:
                    warning = "此指令僅限群組中使用，請在群組中輸入。"
                else:
                    warning = "此指令不允許在此使用。"
                reply_message(event.reply_token, [TextMessage(text=warning)])
                return
            matched_handler = cmd_info["handler"]
            break

    # ─── 執行對應處理函式或回覆錯誤訊息 ──────────────────────────────
    if matched_handler:
        reply_text = matched_handler(event)
        if reply_text:
            reply_message(event.reply_token, [TextMessage(text=reply_text)])

@handler.add(MessageEvent, message=(ImageMessageContent, VideoMessageContent, AudioMessageContent))
def handle_content_message(event):
    # 1) 判斷副檔名
    ext_map = {
        ImageMessageContent: 'jpg',
        VideoMessageContent: 'mp4',
        AudioMessageContent: 'm4a'
    }
    ext = ext_map.get(type(event.message), None)
    
    if not ext:
        return 'OK'

    # 2) 收集事件資料
    source_type = event.source.type  # "user" or "group"
    source_id = event.source.user_id if source_type == 'user' else event.source.group_id
    event_data = {
        'source_type': source_type,
        'source_id': source_id,
        'reply_token': event.reply_token
    }

    message_id = event.message.id

    # 3) 建立 Celery chain: 先下載 -> 再上傳
    workflow = chain(
        download_line_file_task.s(message_id, ext),
        handle_upload_task.s(event_data)
    )
    workflow.apply_async()

    # 4) 立即回 OK, 不阻塞
    return 'OK'

@handler.add(MessageEvent, message=FileMessageContent)
def handle_file_message(event):
    # 假設檔案副檔名為 'file'，或可從 event.message.file_name 動態擷取
    ext = 'file'

    source_type = event.source.type
    source_id = event.source.user_id if source_type == 'user' else event.source.group_id
    event_data = {
        'source_type': source_type,
        'source_id': source_id,
        'reply_token': event.reply_token
    }

    message_id = event.message.id

    workflow = chain(
        download_line_file_task.s(message_id, ext, event.message.file_name),
        handle_upload_task.s(event_data)
    )
    workflow.apply_async()

    return 'OK'

@handler.add(JoinEvent)
def handle_join(event):
    """處理加入群組事件
    
    參數:
        event (JoinEvent): LINE 加入事件物件
    """
    source_type = event.source.type
    source_id = event.source.group_id if source_type == 'group' else event.source.room_id
    source_name = get_source_name(source_type, source_id)
    
    # 記錄或更新群組資訊到資料庫
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 檢查群組是否已存在
            cursor.execute(
                'SELECT status FROM group_info WHERE group_id = %s',
                (source_id,)
            )
            existing_group = cursor.fetchone()
            
            if existing_group:
                # 群組已存在，更新狀態和加入時間
                cursor.execute(
                    '''UPDATE group_info 
                       SET status = 'active',
                           joined_at = CURRENT_TIMESTAMP,
                           name = %s,
                           left_at = NULL
                       WHERE group_id = %s''',
                    (source_name, source_id)
                )
                app.logger.info(f"[JOIN] Updated {source_type} info: {source_id} ({source_name})")
            else:
                # 新群組，插入記錄
                cursor.execute(
                    'INSERT INTO group_info (group_id, name, type, status) VALUES (%s, %s, %s, %s)',
                    (source_id, source_name, source_type, 'active')
                )
                app.logger.info(f"[JOIN] Recorded new {source_type}: {source_id} ({source_name})")
            
        conn.commit()
    except Exception as e:
        app.logger.error(f"[JOIN] Failed to record/update {source_type}: {str(e)}")
        conn.rollback()
    finally:
        conn.close()

@handler.add(LeaveEvent)
def handle_leave(event):
    """處理離開群組事件
    
    參數:
        event (LeaveEvent): LINE 離開事件物件
    """
    source_type = event.source.type
    source_id = event.source.group_id if source_type == 'group' else event.source.room_id
    
    app.logger.info("[LEAVE] Bot left %s: %s", source_type, source_id)
    
    # 更新資料庫記錄
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 刪除群組使用者綁定關係
            cursor.execute('DELETE FROM group_users WHERE group_id = %s', (source_id,))
            
            # 刪除群組資料夾映射
            cursor.execute('DELETE FROM folder_map WHERE source_id = %s', (source_id,))
            
            # 更新群組資訊狀態
            cursor.execute(
                '''UPDATE group_info 
                   SET left_at = CURRENT_TIMESTAMP,
                       status = 'left'
                   WHERE group_id = %s''',
                (source_id,)
            )
        conn.commit()
        app.logger.info("[LEAVE] Successfully updated database records for %s: %s", 
                       source_type, source_id)
    except Exception as e:
        app.logger.error("[LEAVE] Failed to update database records: %s", str(e))
        conn.rollback()
    finally:
        conn.close()

@app.route('/linebot/')
def index():
    """首頁路由"""
    return render_template('index.html')

@app.route('/linebot/testimage')
def testimage():
    return send_from_directory('static', 'testimage.jpg', mimetype='image/jpeg')

@app.route('/linebot/logs/<path:filename>')
def serve_logs(filename):
    # 確保文件存在
    file_path = os.path.join(STATIC_LOGS_PATH, filename)
    if not os.path.isfile(file_path):
        abort(404, description="檔案已過期或不存在")
    
    return send_from_directory(STATIC_LOGS_PATH, filename)

@app.route('/linebot/authorize')
def authorize():
    """處理 Google OAuth 授權請求
    
    回傳:
        Response: 重導向至 Google OAuth 授權頁面或錯誤頁面
    """
    # 取出參數
    line_user_id = request.args.get('line_user_id', '')
    verification_code = request.args.get('verification_code', '')
    
    # 驗證參數
    if not line_user_id or not verification_code:
        app.logger.error("[AUTH] Missing required parameters")
        return render_template('oauth_error.html', error="缺少必要參數")
    
    # 從資料庫驗證碼
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 檢查驗證碼
            cursor.execute(
                'SELECT user_id FROM verification_codes WHERE code = %s AND user_id = %s',
                (verification_code, line_user_id)
            )
            row = cursor.fetchone()
            
            if not row:
                app.logger.error("[AUTH] Invalid or expired verification code for user_id=%s", line_user_id)
                return render_template('oauth_error.html', error="驗證碼無效或已過期")
            
            app.logger.info("[AUTH] Verification successful for user_id=%s", line_user_id)
            # 驗證成功後刪除驗證碼
            cursor.execute('DELETE FROM verification_codes WHERE code = %s', (verification_code,))
            conn.commit()
    finally:
        conn.close()

    # 記錄在 session 中供 callback 使用
    session['line_user_id'] = line_user_id
    
    # 建立 OAuth flow
    flow = Flow.from_client_secrets_file(
        'client_secrets.json',
        scopes=SCOPES,
        redirect_uri='https://benson.tcirc.tw/linebot/oauth2callback'
    )
    
    # 產生授權 URL
    authorization_url, state = flow.authorization_url(
        access_type='offline',
        include_granted_scopes='true',
        prompt='consent'
    )
    
    session['state'] = state
    return redirect(authorization_url)

@app.route('/linebot/oauth2callback')
def oauth2callback():
    state = session.get('state')
    line_user_id = session.get('line_user_id', '')
    
    # 檢查 line_user_id 是否存在且有效
    if not line_user_id:
        app.logger.error("[AUTH] Missing LINE user ID in session")
        return render_template('oauth_error.html', error="授權過程發生錯誤：找不到使用者資訊")
    
    app.logger.info("[AUTH] Processing OAuth callback for user_id=%s", line_user_id)
    
    # 從 client_secrets.json 建立 Flow 物件
    flow = Flow.from_client_secrets_file(
        'client_secrets.json',
        scopes=SCOPES,
        state=state,
        redirect_uri='https://benson.tcirc.tw/linebot/oauth2callback'
    )
    
    try:
        # 以授權完成後的回傳 URL 交換 token
        flow.fetch_token(authorization_response=request.url)
        
        # 取得 Credentials 物件
        credentials = flow.credentials
        
        # 轉為 JSON 字串，準備寫入資料庫
        token_json = credentials.to_json()
        
        # 寫入資料庫
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute('DELETE FROM user_tokens WHERE user_id = %s', (line_user_id,))
                cursor.execute('INSERT INTO user_tokens (user_id, token) VALUES (%s, %s)', 
                             (line_user_id, token_json))
            conn.commit()
            app.logger.info("[AUTH] Successfully stored token for user_id=%s", line_user_id)
        finally:
            conn.close()
        
        return render_template('oauth_success.html')
        
    except Exception as e:
        app.logger.error("[AUTH] OAuth callback failed for user_id=%s: %s", line_user_id, str(e))
        return render_template('oauth_error.html', error="授權過程發生錯誤")


make_static_tmp_dir()
init_db()
if __name__ == "__main__":
    #make_static_tmp_dir()
    #init_db()
    app.run(debug=True, port=5000)
