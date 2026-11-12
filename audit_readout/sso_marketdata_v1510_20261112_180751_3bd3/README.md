# SSO_Market_Data_Continuation_TWSE_TPEX_v1.5.10

## 執行結論

NO_NEW_ROWS

## 行情查詢門檻

- 台灣時間：2026-11-12T18:07:51.274711+08:00
- 當日查詢門檻：17:00
- 本次查詢截止日：2026-11-12

## 2026-11-11 雙市場修正承接

- TWSE：1,390 列
- TWSE canonical SHA256：
  b366ce6fc4910caba953e3bff3d3cb2ed479b12ee33cf462e927f4931b22533f

- TPEx：1,020 列
- TPEx 查詢類型：EW
- TPEx canonical SHA256：
  eb4bd7a5e16831f74f6cc700c8076163d0ba4c5b782023f3858951035a31e627

兩市場均採完整日期替代，不可與舊版同日資料相加。

## 本次新行情

- 前瞻新增行情：0 列
- TWSE 新增：0
- TPEx 新增：0
- 新增交易日：[]

## 官方查詢

- TWSE：MI_INDEX / ALLBUT0999
- TPEx：afterTrading/otc / EW
- 原始 API 回應均保存為 gzip。
- 官方尚無資料的日期保留待重查，不推定休市。

## 資料接續

- 讀取既有 v1.5.x 標準化增量。
- 2026-11-11 雙市場資料以 v1.5.9 修正層替代。
- 新日期資料寫入全市場標準化增量。
- 新增日期與修正日期寫入逐股 overlay。
- 不修改任何舊版 Drive 資料。

## SSO 下游預檢

- downstream_preflight_ready：
  True
- 既有資料衝突鍵值：
  0
- 待重查日期：
  0

本版本只完成行情資料接續及資料品質檢查。
不執行模型訓練、Q5 回測或選股分數計算。

## 原始資料保護

- 不修改 v1.5.9
- 不修改 v1.5.8
- 不修改 v1.5.7
- 不修改 v1.5.6
- 不刪除歷史資料
- 不修改 GitHub main
