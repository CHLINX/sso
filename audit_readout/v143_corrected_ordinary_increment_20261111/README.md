# v1.4.3｜修正臨時休市日並產生官方普通股行情增量

狀態：PASS_COVERAGE_COMPLETE_ORDINARY_CLASSIFIED_INCREMENT
資料區間：2026-10-23 至 2026-11-10
實際交易日：2026-10-27 至 2026-11-10，共 11 日
官方休市：2026-10-23 薔琵颱風臨時休市；2026-10-26 臺灣光復暨金門古寧頭大捷紀念日補假。

v1.4.2 source rows：26477
Official confirmed ordinary universe：1951
Ordinary increment rows：21434
Unmatched/unconfirmed rows：5043

原始 v1.4.2 / v1.4.1 ZIP 唯讀；本報告只新增獨立增量，不修改正式歷史資料庫。
不將未確認代號猜成普通股；不把原始收盤價直接併入 shadow OHLC。
