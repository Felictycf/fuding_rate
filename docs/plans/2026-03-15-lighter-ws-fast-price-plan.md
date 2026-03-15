# Lighter WebSocket Fast Price Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a persistent Lighter WebSocket order book cache so `/api/price?detail_level=fast` reads local top-of-book data instead of issuing per-market `orderBookDetails` requests on the hot path.

**Architecture:** Keep the existing fast/full snapshot architecture. Add a background WebSocket cache for Lighter order books, use it in `price fast`, reuse it for funding detail enrichment where available, and fall back to REST per symbol only when the local cache is missing.

**Tech Stack:** Python stdlib, `websockets`, existing ThreadingHTTPServer, unittest, frontend vanilla JS.

---

### Task 1: Document the target behavior

**Files:**
- Create: `docs/plans/2026-03-15-lighter-ws-fast-price-design.md`
- Create: `docs/plans/2026-03-15-lighter-ws-fast-price-plan.md`

**Step 1: Capture the approved design**

- Describe the WS cache, fast-path integration, fallback behavior, and latency targets.

**Step 2: Save the plan**

- Include explicit TDD steps for backend helpers, WS integration, and runtime verification.

### Task 2: Add a failing test for fast-path quote reuse

**Files:**
- Modify: `tests/test_history_helpers.py`
- Modify: `server.py`

**Step 1: Write the failing test**

- Add a test proving `build_price_payload_fast()` uses a WS quote for a covered symbol and does not call `orderBookDetails` for that symbol.

**Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_history_helpers.SnapshotHelperTests.test_build_price_payload_fast_prefers_ws_quotes`

Expected: FAIL because no WS quote provider exists yet.

**Step 3: Write minimal implementation**

- Introduce a helper that exposes WS quote snapshots to `build_price_payload_fast()`.

**Step 4: Run test to verify it passes**

Run the same unittest command and confirm PASS.

### Task 3: Add a failing test for REST fallback

**Files:**
- Modify: `tests/test_history_helpers.py`
- Modify: `server.py`

**Step 1: Write the failing test**

- Add a test proving only uncovered symbols trigger `orderBookDetails` REST fallback.

**Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_history_helpers.SnapshotHelperTests.test_build_price_payload_fast_falls_back_only_for_missing_ws_quotes`

Expected: FAIL because the current implementation always hits per-market details.

**Step 3: Write minimal implementation**

- Split fast-path quote collection into “WS-covered” and “REST-missing”.

**Step 4: Run test to verify it passes**

Run the same unittest command and confirm PASS.

### Task 4: Add a failing test for order-book state updates

**Files:**
- Modify: `tests/test_history_helpers.py`
- Modify: `server.py`

**Step 1: Write the failing test**

- Add a test proving an initial subscribed snapshot plus incremental updates produce the correct best bid/ask after zero-size deletions.

**Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_history_helpers.SnapshotHelperTests.test_lighter_order_book_state_updates_best_levels`

Expected: FAIL because no order-book state helper exists yet.

**Step 3: Write minimal implementation**

- Add a pure helper / small class to ingest asks+bids and maintain top-of-book.

**Step 4: Run test to verify it passes**

Run the same unittest command and confirm PASS.

### Task 5: Implement the persistent WebSocket cache

**Files:**
- Modify: `server.py`

**Step 1: Add the background cache**

- Implement a thread-owned asyncio loop that:
  - refreshes the market map,
  - connects to Lighter WS,
  - subscribes to `order_book/{market_id}`,
  - applies snapshots and deltas to local state,
  - reconnects on failure.

**Step 2: Integrate with fast price payload**

- Make `build_price_payload_fast()` consume local quotes first and annotate WS coverage metadata.

**Step 3: Reuse where safe in funding**

- Use the local quote cache for funding detail enrichment when available, without changing funding fast ranking semantics.

### Task 6: Surface WS latency on the frontend

**Files:**
- Modify: `web/app.js`
- Modify: `web/index.html`

**Step 1: Update status rendering**

- Show WS coverage / WS age alongside existing fast/full timing.

**Step 2: Keep UX stable**

- Preserve current fast-first rendering and note formatting.

### Task 7: Verify end-to-end

**Files:**
- Modify: `server.py`
- Modify: `tests/test_history_helpers.py`

**Step 1: Run targeted tests**

Run:
- `python3 -m unittest tests.test_history_helpers.SnapshotHelperTests.test_build_price_payload_fast_prefers_ws_quotes`
- `python3 -m unittest tests.test_history_helpers.SnapshotHelperTests.test_build_price_payload_fast_falls_back_only_for_missing_ws_quotes`
- `python3 -m unittest tests.test_history_helpers.SnapshotHelperTests.test_lighter_order_book_state_updates_best_levels`

**Step 2: Run full test suite**

Run: `python3 -m unittest discover -s tests -p 'test_*.py'`

Expected: all tests pass.

**Step 3: Run syntax verification**

Run: `python3 -m py_compile server.py arb_rank.py price_arb_cn.py http_json.py`

Expected: exit 0.

**Step 4: Run runtime latency measurements**

- Start the server.
- Measure `/api/price?detail_level=fast` several times.
- Record:
  - HTTP return time
  - `fetch_ms`
  - `snapshot_age_ms`
  - `lighter_ws_age_ms`
  - `lighter_ws_covered`
  - fallback count
