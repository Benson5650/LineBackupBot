import datetime
import json
import logging
import os
import tempfile
import errno

from flask import Flask, request, abort, jsonify
from werkzeug.middleware.proxy_fix import ProxyFix

from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.webhooks import (
    MessageEvent,
    ImageMessageContent,
    VideoMessageContent,
    AudioMessageContent,
    FileMessageContent
)
from linebot.v3.messaging import (
    Configuration,
    ApiClient,
    MessagingApiBlob
)

# 載入 Celery worker_app


app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_host=1, x_proto=1)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
app.logger.setLevel(logging.INFO)

with open('config.json', 'r') as config_file:
    config = json.load(config_file)

enable_drive = config['enable_cloud_sync']
UPLOAD_FOLDER = config['UPLOAD_FOLDER']
channel_secret = config['channel_secret']
channel_access_token = config['channel_access_token']

handler = WebhookHandler(channel_secret)

static_tmp_path = os.path.join(os.path.dirname(__file__), 'static', 'tmp')

configuration = Configuration(
    access_token=channel_access_token
)

if enable_drive:
    from worker_app import celery, upload_file_to_drive_task

# 建立暫存資料夾
def make_static_tmp_dir():
    try:
        os.makedirs(static_tmp_path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(static_tmp_path):
            pass
        else:
            raise

@app.route("/callback_LineBot", methods=['POST'])
def callback():
    try:
        signature = request.headers['X-Line-Signature']
        body = request.get_data(as_text=True)
        app.logger.info("Request body: " + body)
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    except Exception as e:
        app.logger.error(f"Unexpected error: {e}")
        return jsonify({'error': 'Unexpected error occurred'}), 500

    return 'OK'

@handler.add(MessageEvent, message=(ImageMessageContent, VideoMessageContent, AudioMessageContent))
def handle_content_message(event):
    # 判斷副檔名
    if isinstance(event.message, ImageMessageContent):
        ext = 'jpg'
    elif isinstance(event.message, VideoMessageContent):
        ext = 'mp4'
    elif isinstance(event.message, AudioMessageContent):
        ext = 'm4a'
    else:
        return 'OK'

    with ApiClient(configuration) as api_client:
        line_bot_blob_api = MessagingApiBlob(api_client)
        message_content = line_bot_blob_api.get_message_content(message_id=event.message.id)
        with tempfile.NamedTemporaryFile(dir=static_tmp_path, prefix=ext + '-', delete=False) as tf:
            tf.write(message_content)
            tempfile_path = tf.name

    timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    dist_path = tempfile_path + '.' + ext
    dist_name = f"{timestamp}_{os.path.basename(dist_path)}"
    os.rename(tempfile_path, dist_path)
    source_type = event.source.type
    if source_type == 'user':
        source_id = event.source.user_id
    elif source_type == 'group':
        source_id = event.source.group_id
    else:
        source_id = None

    if source_id:
        if enable_drive:
            upload_file_to_drive_task.delay(dist_path, dist_name, source_type, source_id)

    # 不阻塞，直接回應
    app.logger.info("已將檔案上傳任務提交到 Celery Worker。")
    return 'OK'


@handler.add(MessageEvent, message=FileMessageContent)
def handle_file_message(event):
    with ApiClient(configuration) as api_client:
        line_bot_blob_api = MessagingApiBlob(api_client)
        message_content = line_bot_blob_api.get_message_content(message_id=event.message.id)
        with tempfile.NamedTemporaryFile(dir=static_tmp_path, prefix='file-', delete=False) as tf:
            tf.write(message_content)
            tempfile_path = tf.name

    timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    dist_name = f"{timestamp}_{event.message.file_name}"
    dist_path = os.path.join(static_tmp_path, dist_name)
    os.rename(tempfile_path, dist_path)

    source_type = event.source.type
    if source_type == 'user':
        source_id = event.source.user_id
    elif source_type == 'group':
        source_id = event.source.group_id
    else:
        source_id = None

    if source_id:
        if enable_drive:
            upload_file_to_drive_task.delay(dist_path, dist_name, source_type, source_id)

    app.logger.info("已將檔案上傳任務提交到 Celery Worker。")
    return 'OK'


if __name__ == "__main__":
    make_static_tmp_dir()
    app.run(debug=True, port=5000)
