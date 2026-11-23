# SSO_FUGLE_1M_COVERAGE_AND_FILL_DIAGNOSTIC_v1.0

- This follow-up is read-only: it does not call Fugle, re-download minute bars, or read/write Drive.
- Reuses the completed pilot at `/content/SSO_Fugle_1m_Execution_Pilot_v1_20261123_090619`.
- Previous audit record: https://github.com/CHLINX/sso/tree/audit/sso-fugle-1m-recovery-v1_20261123_110734/audit_readout/sso_fugle_1m_execution_pilot_recovery_v1_20261123_110734
- It decomposes trade coverage into missing entry bars, missing forward dates, and missing 13:30 exit bars; reports actual entry delay; joins entry-bar volume; and recomputes returns under maximum entry-delay cutoffs of 0/1/2/5/10 minutes.
- The first qualifying minute-bar OPEN is only a price proxy, not a guaranteed fill.
- 13:30 bar absence is described as missing observed bar; the reason is not inferred.
- No liquidity filter was applied in the original pilot. This diagnostic quantifies that limitation rather than changing the selected universe.
- Not a trading approval.

## Core counts

```json
{
  "symbol_days": 4008,
  "symbols": 144,
  "bar_dates": 30,
  "symbol_days_has_0900": 2137,
  "symbol_days_has_1330": 2848,
  "symbol_days_no_1330": 1160,
  "has_0900_ratio": 0.533183632734531,
  "has_1330_ratio": 0.7105788423153693,
  "duplicate_minute_timestamps": 0,
  "invalid_ohlc_rows": 0,
  "bars_per_symbol_day_mean": 80.48153692614771,
  "bars_per_symbol_day_p10": 2.0,
  "bars_per_symbol_day_median": 28.0,
  "bars_per_symbol_day_p90": 249.30000000000018,
  "daily_compare_rows": 600,
  "open_match_1pct_count": 600,
  "high_match_1pct_count": 600,
  "low_match_1pct_count": 600,
  "close_auction_rows_in_compare": 415,
  "close_no_1330_rows_in_compare": 185,
  "close_match_1pct_when_auction_bar_present": 1.0,
  "api_request_status_counts": {
    "OK": 144
  },
  "trade_status_counts": {
    "NO_ENTRY_BAR_AFTER_TIME": 2820,
    "OK": 2650,
    "NO_1330_AUCTION_BAR": 478,
    "NO_BARS": 52
  },
  "entry_delay_mean_min_all_rows": 1.3066037735849056
}
```
