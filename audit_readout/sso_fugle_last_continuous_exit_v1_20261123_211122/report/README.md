# SSO_FUGLE_1M_LAST_CONTINUOUS_EXIT_ROBUSTNESS_v1.0.1

This follow-up reuses the completed Fugle pilot and does not call the API again.
Pilot source: GITHUB_AUDIT_ARCHIVE; restored=True.

## Fixed comparison
- Frozen pilot Top10 selections: F02 / F04 / OVERNIGHT_PAIR, no refit or weight changes.
- Entry: existing first available 1-minute bar OPEN at/after 09:01 / 09:05.
- Entry eligibility: maximum observed bar-start delay <= 5 minutes.
- Exit A: 13:30 closing-auction candle CLOSE.
- Exit B: CLOSE of the latest observed minute candle starting 13:20-13:24, the final five-minute continuous-trading window before the 13:25 closing call auction.
- For Exit B, the report records the selected bar timestamp and its start-time gap to 13:25 (1-5 minutes).
- Horizons: [0, 1, 3, 5, 10].

OHLCV prices are proxies, not guaranteed order fills. This is a small diagnostic only.

## Counts
```json
{
  "pilot_selected_rows": 600,
  "pilot_trade_rows": 6000,
  "minute_rows": 322570,
  "symbols": 144,
  "bar_dates": 30,
  "request_status": {
    "OK": 144
  },
  "max_entry_delay_min": 5,
  "entry_eligible_unique_rows": 585,
  "unique_entry_rows": 1200,
  "exit_methods": [
    "AUCTION_1330_CLOSE",
    "LAST_CONTINUOUS_BAR_CLOSE_1320_1324"
  ],
  "status_counts": {
    "ENTRY_NOT_ELIGIBLE_WITHIN_5M": 6150,
    "OK": 4759,
    "NO_LAST_CONTINUOUS_BAR_1320_1324": 639,
    "NO_1330_AUCTION_BAR": 378,
    "NO_BARS": 74
  }
}
```
