# SSO_H5_848_Official_Price_Backfill_v1.0.2

狀態：PARTIAL_BLOCKED

訊號日：2026-11-06；入場：2026-11-09 開盤；出場：2026-11-13 收盤。

候選：848 檔；官方市場日：12/12；行情列：14,452。

本資料夾是獨立 H5 行情快照，不會自動併入既有市場資料版本目錄，避免重複自然鍵。

848 檔配對表提供價格層報酬初算；正式交易有效性仍須由 v2.31 公司行動／signal_entry_mismatch adapter 檢核。
多日期檔案以 (trade_date, market, instrument_code) 自然鍵驗證，不以跨日期重複代號誤判。
全市場標準化檔沿用 v1.5.10 record_to_target 寫入器，確保歷史中文欄名不會寫成空值。
原始官方回應來源：/content/drive/MyDrive/看股最速適/H5_848官方行情補抓_20261117_024522_8dd5。
