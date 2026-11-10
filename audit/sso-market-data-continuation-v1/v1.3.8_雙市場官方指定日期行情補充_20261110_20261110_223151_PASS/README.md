# v1.3.8｜雙市場官方指定日期行情補充

目標資料日期：2026-11-10
執行時間：2026-11-10T22:31:51.114940+08:00
狀態：PASS

## 官方指定日期來源
TWSE：https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX
查詢參數：{
  "date": "20261110",
  "type": "ALLBUT0999",
  "response": "json"
}
TPEx：https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php
查詢參數：{
  "l": "zh-tw",
  "o": "json",
  "d": "115/11/10",
  "se": "EW",
  "s": "0,asc,0"
}

## 日期核驗結果
- TWSE：passed=True；rows=1389；response_dates=['2026-11-10']；error=None
- TPEX：passed=True；rows=1020；response_dates=['2026-11-10']；error=None

## 原始資料保存原則
- 原始 HTTP 回應以 gzip 保存，另保存 URL、參數、HTTP 狀態、回應標頭及 SHA256。
- 不改寫官方原始欄位、不覆蓋既有資料、不更新正式資料庫。
- 若狀態為 WAITING_DATE_VERIFICATION，本資料夾僅為診斷證據，不得併入正式行情資料。

## 正式行情輸出
- 兩市場均通過指定日期驗證及最低筆數檢核。
- 已產生逐市場與合併標準化 CSV。
- 品質檢查只標示異常，不自行修補成交量或 OHLC。
