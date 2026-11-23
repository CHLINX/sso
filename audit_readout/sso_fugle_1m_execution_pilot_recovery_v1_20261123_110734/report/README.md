# SSO_FUGLE_1M_EXECUTION_PRICE_PILOT_v1.0.1

- Fugle is a non-official independent minute-price validation layer; the SSO source data remains read-only.
- Sample signal dates: 2026-09-21 through 2026-10-21 (20 sessions); minute data downloaded through 2026-11-06 to preserve H10 exits.
- Selection: F02 HIGH, F04 HIGH, OVERNIGHT_PAIR = 50/50 mean cross-sectional percentile ranks; daily Top10.
- Entry: first 1-minute bar at or after 09:01 / 09:05, using bar OPEN as a proxy. Exit: explicit 13:30 closing-auction bar CLOSE after H=0/1/3/5/10 further market sessions.
- OHLCV bar prices do not guarantee actual execution. No spread/depth/queue/partial fill/market-impact simulation.
- Historical 1-minute API is limited to 60 historical requests per minute on the published plans; this run throttles below that.
- This is a short sample pilot, not a full OOS validation or trading approval.

## Key counts

{
  "minute_rows": 322570,
  "symbols_requested": 144,
  "symbols_ok": 144,
  "symbols_404": 0,
  "symbols_other_failed": 0,
  "symbol_days": 4008,
  "symbol_days_with_1330": 2848,
  "symbol_days_with_0900": 2137,
  "duplicate_minute_timestamps": 0,
  "invalid_ohlc_rows": 0,
  "comparison_rows": 600,
  "open_match_1pct_ratio": 1.0,
  "high_match_1pct_ratio": 1.0,
  "low_match_1pct_ratio": 1.0,
  "close_match_1pct_ratio": 0.6916666666666667,
  "trade_rows": 6000,
  "valid_trade_rows": 2650
}
