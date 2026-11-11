# SSO_Market_Data_Continuation_TWSE_TPEX_v1.5.2

- 執行日期：2026-11-11T23:52:26.954334+08:00
- TWSE 前次最後行情日：2026-11-10；本次自 2026-11-11 起
- TPEx 前次最後行情日：2026-11-10；本次自 2026-11-11 起
- 查詢截止：2026-11-11
- 新增行情列數：15,142
- 新增個股／市場檔：15,142

## 更新方式

只讀取既有 v1.5.x 增量行情的日期與代表性欄位格式；
不掃描十年歷史底稿、不向前檢查舊行情。

各市場從最後一個有行情日期的下一個日曆日起，
逐日呼叫官方日期端點。
週末也會查詢，以免漏掉補班交易日。

本次資料只寫入新的 append-only overlay：
`overlay/官方歷史行情/`。

格式沿用既有 v1.5.1 覆蓋檔欄位；
全市場標準化增量沿用既有標準化 CSV 欄位。

原始官方回應另以 gzip 保存。

零成交量、OHLC 空值及官方重複列均保留，
不刪除、不補值、不篩除 ETF、ETN、TDR、權證等官方商品。

既有 v2.8 ZIP、v1.4.2 ZIP、v1.5.1 ZIP 及舊增量均不修改。

## 官方來源

- TWSE：`https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX`
- TPEx：`https://www.tpex.org.tw/www/zh-tw/afterTrading/otc`，`type=AL`

## 使用方式

分析資料以 v2.8 歷史底稿 + 既有 v1.5.1 overlay
+ 本次 v1.5.2 overlay，依日期、代號及市場銜接。
