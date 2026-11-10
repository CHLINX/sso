# v1.4.1｜官方 ISIN 商品種類核驗與普通股行情輸出

目標行情日期：2026-11-10
執行時間：2026-11-10T23:08:01.098867+08:00
狀態：PASS_CONFIRMED_ORDINARY_STOCKS

## 輸入來源
- v1.3.8 雙市場行情 ZIP：SHA256 `97fb3ab0bc6bc6c508f800f68d14c2bd47baa5d963fb2452a40eabda3c62c1c8`
- v1.3.3 官方歷史主檔 ZIP：SHA256 `48fbd3a78edcde0efbb4b39f7dc24197d55047fbac00b3b7f9a41f586d32480d`
- TWSE 官方 ISIN 綜合證券名錄：`https://isin.twse.com.tw/isin/e_single_main.jsp`
- TWSE 官方上市公司基本資料：`https://openapi.twse.com.tw/v1/opendata/t187ap03_L`
- TPEx 官方上櫃公司基本資料：`https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O`

## 分類結果
- 原始行情列：2,409
- 已確認普通股：1,951
- ETF：363
- ETN：20
- TDR：10
- 其他非普通股：34
- 未匹配／待確認：31
- ISIN 衝突或隔離列：0
- 行情重複代號列：0
- ISIN 名錄重複代號列：0

## 分類原則
只以官方 ISIN 名錄的 Market、Type of security、CFICode 分類。CFI 前兩碼 ES 代表普通／一般股；其他商品類別依 Type of security 明確排除。公司主檔配對僅供交叉參考，不單獨作普通股證明。資料來源不足或衝突時，保留待確認，不猜測。

## 資料安全
原始 v1.3.8 / v1.3.3 唯讀；不修改正式資料庫、不重訓、不回測、不選股、不修改 main。零成交量及 OHLC 缺漏均保留原值，不自行補值。
