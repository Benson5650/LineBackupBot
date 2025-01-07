# LineBackupBot 賴群檔案備份機器人

這個專案使用 **Flask** 和 **Line Messaging API (v3)** 打造了一個 Line Bot，能夠備份群組或是對話圖片、影音和檔案等內容。你還可以啟用 Google Drive 雲端同步的功能，讓檔案管理變得更輕鬆方便。

## 致謝
本專案參考 [line-bot-tutorial](https://github.com/yaoandy107/line-bot-tutorial)、[透過Python Flask網站程式上傳影像檔到Google Drive雲端硬碟](https://swf.com.tw/?p=1776) 以及 [LINE機器人自動備份群組照片](https://www.hanksvba.com/posts/3782846762/)的教學進行開發



## 專案結構說明

```
.
├── app.py                # Flask 應用主程式，處理 Line Bot 回呼與消息事件
├── worker_app.py         # Celery 相關設定 (載入後可進行背景任務上傳)
├── config.json           # 專案主要設定檔
├── requirements.txt      # Python 依賴函式庫 (可自行建立或更新)
└── README.md             # 專案說明文件
```

**app.py**  

- 使用 `Flask` 作為 Web Framework。
- 定義了 `/callback_LineBot` 路由，負責處理 Line Bot 的 Webhook 回呼。  
- 透過 `WebhookHandler` 來接收並處理訊息事件（目前針對 `ImageMessageContent`、`VideoMessageContent`、`AudioMessageContent`、`FileMessageContent` 進行檔案儲存）。  
- 若 `config.json` 中的 `enable_cloud_sync` 設為 `true`，則會在收到檔案後呼叫 Celery background task 上傳檔案至雲端 (Google Drive)。  
- 預設執行時使用 `debug=True, port=5000`，可依需求修改。

**worker_app.py**  

- Celery Worker 設定位於 worker_app.py，並包含非同步任務 upload_file_to_drive_task 用於上傳檔案到 Google Drive。
- 若 enable_cloud_sync 為 true，收到檔案後，Flask 即會透過 upload_file_to_drive_task.delay(dist_path, dist_name, source_type, source_id) 呼叫背景工作。

**config.json**  
```
{
    "enable_cloud_sync": true,
    "UPLOAD_FOLDER": "the parent folder id of google drive to upload files",
    "channel_secret": "your channel secret",
    "channel_access_token": "your channel access token"
}
```

- `enable_cloud_sync`: 布林值，若為 `true`，接收檔案時會自動呼叫背景任務將檔案上傳至 Google Drive。  
- `UPLOAD_FOLDER`: Google Drive 的上層資料夾 ID，用於決定將檔案上傳到哪個資料夾下。  
- `channel_secret`: Line Bot 的 Channel Secret。  
- `channel_access_token`: Line Bot 的 Channel Access Token。



## 使用前準備

1. **Line 開發者帳號**  
   - [建立並設定 Line Messaging API](https://developers.line.biz/en/)  

   - 取得 `channel_secret` 與 `channel_access_token`。[參考教學](https://github.com/yaoandy107/line-bot-tutorial)

2. **Google Cloud Platform (GCP) 與 Google Drive API**（可選）  
   - 若要上傳檔案到 Google Drive，需要先於 [Google Cloud Console](https://console.cloud.google.com/) 建立專案並啟用 Drive API。[參考教學](https://swf.com.tw/?p=1776)

   - 產生服務帳號憑證並覆蓋 `google.json`

   - `UPLOAD_FOLDER` 則需設定為 Google Drive 中所欲放置檔案的資料夾 ID。

   ![alt text](image/image.png)



## 安裝與設定

1. **安裝依賴**  
   
   ```bash
   pip install -r requirements.txt
   ```
   依賴函式庫例如：
   ```txt
   line-bot-sdk
   flask
   google-api-python-client
   celery // 如不需要雲端同步可以不安裝
   redis // 如不需要雲端同步可以不安裝
   portalocker // 如不需要雲端同步可以不安裝
   ...
   ```

2. **安裝Redis**
    ```bash
    # Windows
    https://github.com/MicrosoftArchive/redis

    # Linux (Ubuntu)
    sudo apt-get install redis-server
    ```

3. **設定 config.json**  
   - 在專案目錄下，編輯 `config.json`，填入對應的值：
     ```json
     {
         "enable_cloud_sync": true,
         "UPLOAD_FOLDER": "the parent folder id of google drive to upload files",
         "channel_secret": "your channel secret",
         "channel_access_token": "your channel access token"
     }
     ```
   - `enable_cloud_sync` 若為 `false`，則不會呼叫上傳至雲端的 Celery 任務。

## 執行專案

1. **啟動 Celery Worker 與 Redis**  
   - 在專案根目錄下執行：
     ```bash
     celery -A worker_app.celery worker --loglevel=info
     ```
   - 如果在Windows環境下執行：
     ```
     celery -A worker_app.celery worker --pool=solo --loglevel=info
     ```

2. **啟動 Flask**  
   - 測試執行：
     ```bash
     python app.py
     ```
   - 預設監聽在 `http://127.0.0.1:5000`。

   - 正式使用建議搭配 [uwsgi](https://hackmd.io/@luluxiu/By2ZsccgT)

3. **取得公開的https連線** 
   - 若沒有公網 ip 和 ssl 證書，可以使用 [ngrok](https://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=web&cd=&cad=rja&uact=8&ved=2ahUKEwjywb6B-uOKAxWaka8BHViqLX0QFnoECAwQAQ&url=https%3A%2F%2Fngrok.com%2F&usg=AOvVaw0qg9kSksx3M4uUIoIqmJI3&opi=89978449)。

   - [教學](https://ithelp.ithome.com.tw/m/articles/10295654)

4. **設定 Line Messaging API Webhook**  
   - 到 Line 開發者後台，將 Webhook URL 設定為：
     ```
     https://your-domain.com/callback_LineBot
     ```
   - 測試確認連線並啟用 Webhook。



## 功能說明

1. **接收圖片、影音、音訊**  
   - `/callback_LineBot` 會接收 `ImageMessageContent`, `VideoMessageContent`, `AudioMessageContent`，並根據檔案類型存檔到暫存目錄 `static/tmp`。  
   - 若 `enable_cloud_sync = true`，則同時會將上傳任務提交給 Celery。

2. **接收檔案**  
   - 收到 `FileMessageContent` 時，也會同樣存至暫存資料夾並（若啟用）發送上傳任務。

3. **非同步背景上傳**  
   - Celery Worker 內部的 `upload_file_to_drive_task` 會負責與 Google Drive API 溝通，將檔案上傳到指定的雲端資料夾。  

---

若有任何問題或建議，歡迎提出 Issue 或 Pull Request。祝開發順利！