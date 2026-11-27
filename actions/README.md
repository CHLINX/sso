# SSO F04 H0 GitHub Actions pilot

本分支先以 GitHub Actions 驗證雲端執行環境。原版 F04 v2.0.1 的選股、09:00 快照凍結、09:01/09:05 進場觀察、H0 出場、成本與缺漏資料口徑均未變更。

## GitHub Actions Secrets

在 GitHub repository 的 **Settings → Secrets and variables → Actions** 建立下列 Repository secrets：

| Secret 名稱 | 內容 |
|---|---|
| `GOOGLE_DRIVE_CREDENTIALS_JSON` | Google Cloud service account JSON 全文 |
| `SSO_BASE_FILE_ID` | 固定 v2.31 gzip CSV 的 Google Drive file ID |
| `SSO_INCREMENT_FOLDER_ID` | `SSO_Market_Data_Continuation_TWSE_TPEX_v1` 專用根資料夾 ID |
| `FUGLE_API` | Fugle Market Data API Key |

`GITHUB_TOKEN` 不需要自行建立；Workflow 以 GitHub Actions 的 `github.token` 注入，權限限於 repository contents。程式另會核對 `main` SHA 必須仍是 `aaabc2433fb5df2bb68af06461178305d6bd30d1`，並且只推送新的 `audit/sso-fugle-f04-h0-shadow-v2_*` 分支。

## Google Drive 唯讀授權

1. 在 Google Cloud 專案啟用 Google Drive API，建立 service account，下載 JSON key。
2. 將固定 v2.31 基礎 CSV 與 SSO 專用增量資料夾，以 **Viewer** 權限分享給 service account 的 `client_email`。
3. 將 JSON 全文放入 `GOOGLE_DRIVE_CREDENTIALS_JSON`；檔案 ID 與資料夾 ID 分別放入對應 Secret。
4. `actions/drive_sync.py` 僅列目錄與下載檔案，使用 `drive.readonly` scope；不會寫入、搬移或刪除任何 Drive 資料。下載後還會嚴格驗證 v2.31 固定 SHA256。

## 執行方式與時間

- Push 到此 audit 分支：只跑原版 self-test 與 Drive/憑證 preflight，不呼叫 Fugle、不凍結 Top10。
- Workflow dispatch：可選 `self-test`、`freeze`、`close`。正式 freeze/close 必須先通過 Drive 與憑證 preflight。
- 預定盤前：台灣時間 08:35（UTC 00:35）；先同步 Drive 資料，再啟動原版程式。原版會先載入歷史／狀態，等待 09:00:05 才抓 TSE/OTC 快照。
- 預定盤後：台灣時間 16:40（UTC 08:40）；只讀取已凍結 Top10 並做 H0 分鐘 K 觀察。
- 若盤前工作流程延遲，導致程式啟動超過 09:00:58，原版會拒絕當日凍結；不會事後補排名或改用收盤資料。
- Snapshot Quotes 的 Fugle 方案權限仍須符合原規格（Developer plan 或以上）。

## 重要限制

GitHub 文件明確規定，`schedule` 事件只會在 workflow 已存在於預設分支時觸發；排程也可能延遲或漏掉。因此本分支的 schedule 是待審查草案，未合併至預設分支前不會自動排程。為遵守 SSO 的 main 保護規則，此試驗不直接寫入 main。只有在審查並合併 PR 後，排程才會生效。

這是前瞻影子驗證，不會下單，也不代表選股策略已證明獲利。
