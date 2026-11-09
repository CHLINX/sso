# v4.87.5.116.4

## 本版目標

只追查上市、上櫃個股，不把非普通股商品納入個股研究。

## 承接 v116.2 已核驗成果

- v4.68.1 完整來源：4,193,745 rows。
- v4.68.1 SHA256：a5151cdebe26233389f55bf6189f65d21e1f5df05f025b653963bafc483deb4d
- 官方獨有目標證券：844 檔。
- v4.68.1 找得到：539 檔。
- v4.68.1 找不到：305 檔。
- 缺席目標資料：1,179 筆。
- 42,819 筆前後45日鄰近行情已核對完成。
- 舊倍率4,522組與新版7,022筆在10位小數下已核對完成。

本版不重掃原始4,193,745筆資料，不重做鄰近行情與倍率核驗。

## 本版檢查的歷史個股資料

- v4.24.1 完整證券清單與股票池範圍核驗。
- v4.16 官方行情完整證券代號重建。
- v4.42 官方個股日行情與來源缺漏核驗。
- v4.43 TPEx新站個股歷史日行情核驗。
- v4.56 SSO股票池與原始行情日期核對。
- v4.87.5.32 個股候選來源盤點。
- v4.87.5.34 個股完整母體來源追查。
- v4.87.5.35 個股候選來源擴充核驗。
- v231 股票池逐筆樣本與欄位資料。

## 分類原則

1. 有明確普通股類型欄位：列為已確認普通股。
2. 在既有個股或股票池資料中出現，但類型欄位不足：列為個股來源候選，仍須確認。
3. 有明確非普通股類型欄位：不納入個股研究。
4. 無法確認者：保留待確認，不猜測。

## 分類結果

{
  "total_absent_securities": 305,
  "target_rows": 1179,
  "confirmed_common_stock": 0,
  "confirmed_common_stock_target_rows": 0,
  "stockpool_or_individual_source_candidates": 257,
  "stockpool_or_individual_source_target_rows": 1000,
  "explicit_nonstock_excluded": 48,
  "explicit_nonstock_target_rows": 179,
  "classification_unknown_or_conflict": 0,
  "classification_counts": {
    "曾出現在SSO個股／股票池來源": 257,
    "明確非普通股": 48
  }
}

## 未解項目

[]

## 安全限制

- Google Drive唯讀。
- 不呼叫官方API。
- 不修改原始來源。
- 不更新資料庫。
- 不重訓、不回測、不選股。
- 不刪除資料。
- GitHub只新增獨立audit branch，不修改main。
