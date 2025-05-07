# LINE Bot Google Drive 同步工具

一個 LINE Bot，旨在自動將 LINE 聊天（私人訊息和群組聊天）中的檔案（圖片、影片、音訊和其他文件）同步到使用者的 Google Drive。它可以幫助使用者備份 LINE 上分享的重要檔案，無需手動操作。

# 機器人官方網站 
Host版 無須設定即可啟用 活多久算多久
點[這裡](https://benson.tcirc.tw/linebot/)然後加入好友

## 功能特性

*   **Google 帳號整合**：使用 OAuth 2.0 安全地連結您的 Google 帳號。
*   **自動檔案同步**：
    *   將指定 LINE 群組中的檔案同步到您的 Google Drive。
    *   儲存您在私訊中直接傳送給 Bot 的檔案。
*   **選擇性群組綁定**：選擇您想要同步的 LINE 群組。
    *   直接在群組內綁定/解除綁定。
    *   透過私訊使用群組 ID 或群組名稱 + 加入時間來綁定/解除綁定。
*   **有組織的儲存**：檔案儲存在您 Google Drive 的 "LineBot" 資料夾中，並為每個同步的群組或您的個人上傳建立子資料夾。
*   **完整的指令集**：
    *   管理 Google 帳號連結 (`!bindgoogle`, `!unbindgoogle`, `!checkgoogle`)。
    *   管理群組綁定 (`!bindgroup`, `!unbindgroup`, `!listgroup`)。
    *   檢視上傳歷史和狀態 (`!showlog`, `!listfailed`)。
    *   重試失敗的上傳 (`!retryupload`)。
    *   確保資料夾名稱保持最新 (`!updatefolder`)。
    *   獲取幫助 (`!help`)。
*   **背景處理**：利用 Celery 進行可靠的非同步檔案下載和上傳，確保 Bot 保持回應。
*   **錯誤處理與重試**：包含處理上傳失敗並重試的機制。
*   **日誌記錄**：追蹤上傳活動和錯誤。

## 安裝與設定

### 1. 先決條件
*   Python 3.x
*   Redis
*   MySQL (或相容的 MariaDB)
*   一個可公開存取的伺服器或使用 ngrok 進行本機測試 (用於 LINE Webhook)。

### 2. 複製儲存庫
```bash
git clone https://github.com/Benson5650/LineBackupBot.git
cd LineBackupBot
```

### 3. 安裝依賴套件
```bash
pip install -r requirements.txt
```

### 4. 設定
*   **LINE Bot 設定**：
    *   在 [LINE Developers Console](https://developers.line.biz/) 建立一個 Messaging API 頻道。
    *   記下您的 **Channel Secret** 和 **Channel Access Token**。
    *   設定 Webhook URL 指向 `https://<您的公開網域>/linebot/callback_LineBot`。

*   **Google Cloud Platform 設定**：
    *   前往 [Google Cloud Console](https://console.cloud.google.com/)。
    *   建立新專案或選取現有專案。
    *   啟用 **Google Drive API**。
    *   建立 OAuth 2.0 憑證 (選擇 "Web 應用程式")。
    *   在「已授權的重新導向 URI」中新增 `https://<您的公開網域>/linebot/oauth2callback`。
        *   **重要**: `<您的公開網域>` 必須與您在下面 `settings.py` 中設定的 `BASE_URI` 以及 `app_new.py` 中 Google OAuth 流程的 `redirect_uri` 相符。目前 `app_new.py` 中硬式編碼為 `https://benson.tcirc.tw/linebot/oauth2callback`。如果您使用不同的網域，則需要修改 `app_new.py` 中的 `redirect_uri`。
    *   下載憑證為 `client_secrets.json` 並將其放置在專案的根目錄。

*   **應用程式設定**：
    *   複製 `local_settings_EXAMPLE.py` 為 `local_settings.py`。
        ```bash
        cp local_settings_EXAMPLE.py local_settings.py
        ```
    *   編輯 `local_settings.py` 並填入：
        *   `FLASK_SECRET_KEY`：一個強固的隨機字串。
        *   `CHANNEL_SECRET`：您的 LINE Channel Secret。
        *   `CHANNEL_ACCESS_TOKEN`：您的 LINE Channel Access Token。
        *   `DATABASE`：您的 MySQL 連線詳細資訊。
    *   編輯 `settings.py`：
        *   更新 `BASE_URI` 為您的公開網域 (例如：`'your_domain.com'` 或 `benson.tcirc.tw` 如果您使用預設的 OAuth redirect URI)。此設定用於產生 OAuth 授權 URL 和日誌檔案連結。
        *   `CLIENT_SECRETS_FILE` 預設指向 `client_secrets.json`。

*   **資料庫設定**：
    *   確保您的 MySQL 伺服器正在執行，並建立 `local_settings.py` 中指定的資料庫 (預設為 `drivetoken`)。
    *   應用程式將在首次執行時自動建立必要的資料表。

### 5. 執行應用程式
*   **初始化資料庫與暫存資料夾**：
    Flask 應用程式會在啟動時嘗試建立資料表和暫存目錄 (定義於 `STATIC_TMP_PATH` 和 `STATIC_LOGS_PATH` 在 `settings.py` 中)。

*   **啟動 Celery Worker**：
    ```bash
    celery -A worker_app.celery worker -l info
    ```

*   **啟動 Celery Beat (用於排程任務，如日誌清理)**：
    ```bash
    celery -A worker_app.celery beat -l info
    ```

*   **啟動 Flask 應用程式**：
    開發環境：
    ```bash
    python app_new.py
    ```
    生產環境建議使用 WSGI 伺服器，例如 Gunicorn：
    ```bash
    gunicorn -w 4 -b 0.0.0.0:5000 app_new:app
    ```

## 使用方式

1.  **新增 Bot**：將 LINE Bot 加入您的 LINE 帳號 (例如，掃描其 QR code 或使用其 LINE ID)。QR code 和「加入好友」按鈕可在 Bot 的首頁 (`https://<您的公開網域>/linebot/`) 找到。

2.  **初始設定**：
    *   在私訊中傳送 `!bindgoogle` 給 Bot，並依照連結指示授權 Google Drive 存取。

3.  **指令**：主要在私訊中使用以下指令與 Bot 互動 (除非特別註明)：

    *   **Google 帳號**：
        *   `!bindgoogle`：開始 Google 帳號連結流程。
        *   `!unbindgoogle`：解除您的 Google 帳號連結。
        *   `!checkgoogle`：檢查 Google 帳號綁定狀態。
        *   `!testupload`：上傳一個測試圖片到您的 Google Drive (來自 Bot 的靜態檔案)。

    *   **群組管理**：
        *   `!bindgroup`：
            *   在群組中：將您與該群組綁定。
            *   在私訊中：提示輸入群組 ID 或群組名稱 + 加入時間。
            *   範例 (私訊)：`!bindgroup C1234567890abcdef1234567890abcdef`
            *   範例 (私訊)：`!bindgroup "我的群組" 08151430` (代表 8月15日 14:30)
        *   `!unbindgroup`：
            *   在群組中：解除您與該群組的綁定。
            *   在私訊中：提示輸入群組 ID 或群組名稱 + 加入時間。
            *   範例 (私訊)：`!unbindgroup C1234567890abcdef1234567890abcdef`
            *   範例 (私訊)：`!unbindgroup all` (解除所有群組的綁定)
        *   `!listgroup`：列出您已綁定的所有群組。
        *   `!showcompletegroup`：如果 `!listgroup` 的結果被截斷，顯示完整的群組列表。
        *   `!updatefolder`：為您所有綁定的群組，檢查並更新 Google Drive 資料夾名稱以符合目前的 LINE 群組名稱。

    *   **檔案管理與日誌**：
        *   `!showlog`：顯示您最近的上傳日誌。
        *   `!listfailed [小時數]`：列出過去 `[小時數]` 內失敗的上傳 (預設 24 小時)。
        *   `!retryupload [小時數]`：嘗試重新上傳過去 `[小時數]` 內失敗的檔案 (預設 24 小時)。

    *   **其他**：
        *   `!help`：顯示包含所有指令的幫助訊息。
        *   `!switchdebug`：切換偵錯模式 (僅限私訊)。啟用時，對於私訊上傳，Bot 會顯示載入動畫並在上傳成功後回覆 Google Drive 連結。

## 技術棧

*   **後端框架**：Flask
*   **任務隊列**：Celery
*   **訊息代理/Celery 後端**：Redis
*   **資料庫**：MySQL (使用 PyMySQL, DBUtils)
*   **APIs**：
    *   LINE Messaging API (透過 `line-bot-sdk`)
    *   Google Drive API (透過 `google-api-python-client`, `google-auth-oauthlib`)

## 檔案結構
```
.
├── app_new.py              # 主要 Flask 應用程式，處理 webhook 和指令
├── worker_app.py           # Celery worker 定義，用於背景任務
├── settings.py             # 一般應用程式設定
├── local_settings_EXAMPLE.py # 本機 (機密) 設定範例
├── requirements.txt        # Python 依賴套件
├── templates/              # HTML 樣板
│   ├── index.html
│   ├── oauth_success.html
│   └── oauth_error.html
├── static/                 # 靜態檔案 (例如圖片、暫存檔)
│   ├── QRcode.png
│   ├── testimage.jpg
│   └── tmp/                # 下載檔案的暫存區
│   └── logs/               # 透過 URL 提供的日誌檔案暫存區
└── client_secrets.json     # Google OAuth 憑證 (注意：此檔案應被 .gitignore 排除)
```

## 注意事項
*   `client_secrets.json` 包含敏感資訊，請確保已將其加入 `.gitignore` 檔案中，不要提交到版本控制。
*   `local_settings.py` 也應被 `.gitignore` 排除。
*   定期備份您的資料庫。
*   確保您的 Redis 和 MySQL 服務穩定執行。
```// filepath: README.md

