# v1.3.9｜2026-11-10 普通股範圍配對與行情分類

目標行情日期：2026-11-10
執行時間：2026-11-10T22:47:20.463188+08:00
狀態：PASS_WITH_PRODUCT_CLASSIFICATION_LIMITS

## 輸入來源

- v1.3.8 雙市場行情 ZIP：SHA256 `97fb3ab0bc6bc6c508f800f68d14c2bd47baa5d963fb2452a40eabda3c62c1c8`
- v1.3.3 官方普通股範圍來源 ZIP：SHA256 `48fbd3a78edcde0efbb4b39f7dc24197d55047fbac00b3b7f9a41f586d32480d`
- TWSE 官方上市公司基本資料：`https://openapi.twse.com.tw/v1/opendata/t187ap03_L`
- TPEx 官方上櫃公司基本資料：`https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O`

## 行情配對結果

- 雙市場行情列數：2,409
- 官方公司主檔匹配個股候選：1,982
- 官方商品名錄配對或 TDR 候選：10
- 未匹配、商品種類待確認：417
- 重複代號列：0

## 分類原則

公司主檔匹配僅代表行情代號命中該市場官方公司基本資料，列為「個股候選」，不過度宣稱逐商品普通股法律分類。只有取得並解析官方 ETF/ETN/TDR 商品名錄且代號吻合，才標成明確商品類別。代號或名稱的 TDR 提示只列為候選，未取得名錄時不宣稱確證。未匹配資料不猜測商品種類。

## 主要輸出

- `normalized/公司主檔匹配個股候選行情.csv`
- `reports/逐筆證券分類與主檔配對.csv`
- `reports/未匹配商品種類待確認.csv`
- `reports/官方商品名錄配對與TDR候選.csv`
- `reports/重複代號檢查.csv`
- `reports/官方當前公司主檔.csv`
- `reports/普通股範圍主檔交叉比對摘要.json`

## 資料安全

原始資料唯讀；本版只建立新封存，不修改 v1.3.8、v1.3.3、正式資料庫或 main；不重訓、不回測、不選股。零成交量與缺OHLC資料不刪除、不補值。
