# Dual Table Fast Mode Design

## Goal

Make both dashboard tables return the primary ranking and primary PnL fields as fast as possible, while secondary explanatory fields are filled asynchronously later. The frontend must show fast-path latency and detail-path latency in real time.

## Scope

- Funding table:
  - Fast path: ranking, symbol, strategy, estimated net/day, funding pnl/day, round-trip cost
  - Detail path: breakeven days, VAR/Lighter bps breakdown, indicative basis
- Price table:
  - Fast path: ranking, symbol, direction, diff bps, gross profit, round-trip cost, net round-trip
  - Detail path: VAR spread, L_taker, VAR mid, L_ref, real Lighter bid/ask-derived fields

## Approach

1. Keep snapshot-driven serving in `server.py`.
2. Split each table into two snapshot tiers:
   - `fast`: refreshed aggressively and optimized for minimal upstream work
   - `full`: refreshed more slowly and used only for async enrichment
3. Default API responses return the `fast` tier.
4. Frontend first renders the fast tier, then fetches the full tier and merges detail columns by symbol without re-sorting the table.

## Data Semantics

- Ranking is always based on fast-path fields.
- Full/detail updates never block fast-path rendering.
- Full/detail updates should not reshuffle the table order in the browser.
- Each API response must expose:
  - `source`
  - `detail_level`
  - `snapshot_age_ms`
  - `fetch_ms`

## Performance Targets

- Funding fast snapshot age: around `<= 1s`
- Price fast snapshot age: around `<= 1-2s` if upstream permits
- Default table HTTP response: low milliseconds on localhost
- Full/detail path may be slower or older than fast path, but should remain visible in UI metadata

## UI Requirements

- Add explicit latency badges for each panel:
  - fast path age / response source
  - detail path age / detail readiness
- If a detail field is not available yet, show placeholder text rather than blocking row rendering.

## Risk

- Upstream rate limits may constrain the price fast-path cadence.
- Full/detail fields can be temporarily inconsistent with the fast ranking because they arrive later.
- The UI must clearly distinguish fast primary data from lagging detail data to avoid misleading interpretation.
