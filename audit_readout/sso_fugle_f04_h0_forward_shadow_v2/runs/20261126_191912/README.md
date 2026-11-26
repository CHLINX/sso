# SSO_F04_H0_FORWARD_SHADOW_VALIDATION_v2.0

- Phase: `close`
- Status: `WAITING_FOR_PREOPEN_FREEZE`
- Target date: `2026-11-26`
- OOS start: `2026-11-24`
- Run once from 08:30:00 (recommended 08:45-08:50): the program preloads history/state, waits until 09:00:05, and freezes F04 HIGH Top10 from TSE/OTC COMMONSTOCK snapshots before 09:01.
- Run again at/after 16:35: the program loads the already-frozen list and computes H0 execution proxies.
- The 16:35+ close phase only loads the already-frozen list and never recomputes the ranking.
- Snapshot open-price coverage and valid F04 history-intersection coverage each must be at least 90-95% before ranking.
- Missing prices remain NA; the 10-stock basket metric requires 10/10 valid prices.
- FUGLE Snapshot Quotes requires Developer plan or above; lack of access blocks the prospective run.
- Minute K prices are proxies, not confirmed fills.
