# Realtime Feed Cache Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a persistent in-memory feed cache for fast funding and price payloads so requests read warm market data instead of fetching upstream HTTP on the hot path.

**Architecture:** Keep the existing snapshot and WS order-book system. Add background refreshers for raw market feeds, make fast builders consume cached raw feeds first, and expose cache-age metadata to the frontend while preserving the current fast/full HTTP UI.

**Tech Stack:** Python stdlib threads, existing HTTP/WS helpers, unittest, vanilla JS frontend.

---

### Task 1: Add failing tests for cached fast builders

**Files:**
- Modify: `tests/test_history_helpers.py`
- Modify: `server.py`

**Step 1: Write the failing test**

- Add a test proving `build_funding_payload_fast()` uses cached feeds and skips live HTTP when cache is populated.

**Step 2: Run test to verify it fails**

Run: `python3 -m unittest discover -s tests -p 'test_history_helpers.py' -v`
Expected: FAIL on the new funding-cache test.

**Step 3: Write minimal implementation**

- Add feed-cache getters/setters and make funding fast builder read cache first.

**Step 4: Run test to verify it passes**

Run the same command and confirm PASS.

### Task 2: Add failing tests for price fast cached feeds

**Files:**
- Modify: `tests/test_history_helpers.py`
- Modify: `server.py`

**Step 1: Write the failing test**

- Add a test proving `build_price_payload_fast()` uses cached feeds for Variational/Lighter metadata and only uses per-symbol fallback when needed.

**Step 2: Run test to verify it fails**

Run: `python3 -m unittest discover -s tests -p 'test_history_helpers.py' -v`
Expected: FAIL on the new price-cache test.

**Step 3: Write minimal implementation**

- Reuse the feed cache in price fast builder.

**Step 4: Run test to verify it passes**

Run the same command and confirm PASS.

### Task 3: Add background refreshers

**Files:**
- Modify: `server.py`

**Step 1: Write the failing test**

- Add a test for feed-cache metadata updates if needed, or verify behavior indirectly through cache readers.

**Step 2: Run test to verify it fails**

- Use the focused unittest command and confirm the failure is about missing refresh support.

**Step 3: Write minimal implementation**

- Add background refresh loops for:
  - `var_stats`
  - `lighter_orderbooks`
  - `lighter_funding`

**Step 4: Run test to verify it passes**

- Re-run the targeted tests.

### Task 4: Surface feed age metadata on the frontend

**Files:**
- Modify: `web/app.js`

**Step 1: Write/update tests if frontend has coverage**

- If no frontend tests exist, keep this change minimal and validate via runtime checks.

**Step 2: Write minimal implementation**

- Show feed cache age in the existing latency note/badge when present.

**Step 3: Verify manually**

- Confirm the page renders existing notes plus feed age info.

### Task 5: Verify end-to-end

**Files:**
- Modify: `server.py`
- Modify: `tests/test_history_helpers.py`
- Modify: `web/app.js`

**Step 1: Run targeted tests**

Run: `python3 -m unittest discover -s tests -p 'test_history_helpers.py' -v`

**Step 2: Run full test suite**

Run: `python3 -m unittest discover -s tests -p 'test_*.py'`

**Step 3: Run syntax verification**

Run: `python3 -m py_compile server.py arb_rank.py price_arb_cn.py http_json.py`

**Step 4: Measure runtime latency**

- Start the server
- Sample `funding fast` and `price fast`
- Record:
  - HTTP return time
  - `fetch_ms`
  - `snapshot_age_ms`
  - feed cache age
  - WS coverage / fallback count where relevant
