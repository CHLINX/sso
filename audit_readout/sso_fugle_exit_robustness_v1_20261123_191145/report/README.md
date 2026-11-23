# SSO_FUGLE_1M_EXIT_PRICE_ROBUSTNESS_v1.0

This follow-up reuses the completed Fugle pilot and does not call the API again.

## Fixed comparison
- Frozen pilot Top10 selections: F02 / F04 / OVERNIGHT_PAIR, no refit or weight changes.
- Entry: existing first available 1-minute bar OPEN at/after 09:01 / 09:05.
- Entry eligibility: maximum observed bar-start delay <= 5 minutes.
- Exit A: 13:30 closing-auction candle CLOSE.
- Exit B: first observed 1-minute candle starting 13:25-13:29; use its OPEN.
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
    "PRE_CLOSE_FIRST_BAR_OPEN"
  ],
  "status_counts": {
    "ENTRY_NOT_ELIGIBLE_WITHIN_5M": 6150,
    "NO_PRE_CLOSE_BAR_1325_1329": 2888,
    "OK": 2510,
    "NO_1330_AUCTION_BAR": 378,
    "NO_BARS": 74
  }
}
```
