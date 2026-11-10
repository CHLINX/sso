# SSO Market Data Continuation v1.5.0

## 目的

核對 v1.4.3 普通股官方行情增量與歷史行情來源的重疊資料。

## v1.4.3 來源

Branch: `audit/sso-marketdata-v143-ordinary-increment-20261111_0040`

CSV: `audit_readout/v143_corrected_ordinary_increment_20261111/10月27日至11月10日_已確認普通股官方行情增量.csv`

Git blob SHA: `d6a3483d5c1a25a7972d115b016e23087d095463`

SHA256: `fcb07ca8ce26c22c431750393cb4340ea70eca61fb9eaff7dd5ff42f79dcc9e7`

解析普通股列數：21,434

預期列數：21,434

日期：2026-10-27 至 2026-11-10

## 重疊日期

2026-10-27, 2026-10-28

## v2.8 判定

v2.8 尚未取得完整逐筆 OHLCV 一致證據，不得將新行情併入該價格序列。

v2.8 最大日期為 2026-10-28；若重疊核驗通過，2026-10-29 起才屬於待銜接的新日期。

## v4.84.9.8 判定

尚未取得可辨識的 v4.84.9.8 歷史行情日期範圍。

## 安全界線

本程式只讀取來源及產生稽核報告。
未修改 Drive 原始檔。
未更新正式資料庫。
未寫入 shadow OHLC。
未合併行情。
未重訓、未回測、未選股。
GitHub 僅建立獨立 audit branch，不修改 main。
