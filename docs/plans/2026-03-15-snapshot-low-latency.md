# Snapshot Low Latency Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Serve funding and price tables from continuously refreshed in-memory snapshots so dashboard data stays within a few seconds of upstream while requests return in milliseconds.

**Architecture:** `server.py` will maintain per-query snapshot entries for funding and price requests, refresh active entries in background worker threads, and prefer fresh snapshots for `/api/funding` and `/api/price` unless `force=1` is requested. `web/app.js` will stop relying on a 300-second API cache and will surface snapshot age in the UI.

**Tech Stack:** Python 3.12 stdlib, `http.server`, background threads, existing browser JS frontend, `unittest`

---

### Task 1: Add snapshot behavior tests

**Files:**
- Modify: `tests/test_history_helpers.py`
- Test: `tests/test_history_helpers.py`

**Step 1: Write the failing test**

```python
def test_resolve_snapshot_prefers_recent_snapshot_over_live():
    ...
```

**Step 2: Run test to verify it fails**

Run: `python3 -m unittest discover -s tests -p 'test_history_helpers.py'`
Expected: FAIL because snapshot helpers do not exist yet.

**Step 3: Write minimal implementation**

Add snapshot state helpers in `server.py` for:
- seeding default dashboard queries
- resolving snapshot vs live response
- tracking snapshot age metadata

**Step 4: Run test to verify it passes**

Run: `python3 -m unittest discover -s tests -p 'test_history_helpers.py'`
Expected: PASS

### Task 2: Add background snapshot refreshers

**Files:**
- Modify: `server.py`
- Test: `tests/test_history_helpers.py`

**Step 1: Write the failing test**

```python
def test_default_snapshot_entries_seed_funding_and_price_queries():
    ...
```

**Step 2: Run test to verify it fails**

Run: `python3 -m unittest discover -s tests -p 'test_history_helpers.py'`
Expected: FAIL because default snapshot registry is missing.

**Step 3: Write minimal implementation**

Add:
- default funding/price snapshot params
- active snapshot registry
- refresher threads started from `main()`

**Step 4: Run test to verify it passes**

Run: `python3 -m unittest discover -s tests -p 'test_history_helpers.py'`
Expected: PASS

### Task 3: Serve API from snapshots and keep live fallback

**Files:**
- Modify: `server.py`
- Test: `tests/test_history_helpers.py`

**Step 1: Write the failing test**

```python
def test_force_live_bypasses_snapshot():
    ...
```

**Step 2: Run test to verify it fails**

Run: `python3 -m unittest discover -s tests -p 'test_history_helpers.py'`
Expected: FAIL because `force=1` behavior is not wired into snapshot resolution yet.

**Step 3: Write minimal implementation**

Refactor current `/api/funding` and `/api/price` live code into reusable builders, then route requests through snapshot resolution before falling back to live computation.

**Step 4: Run test to verify it passes**

Run: `python3 -m unittest discover -s tests -p 'test_history_helpers.py'`
Expected: PASS

### Task 4: Update UI for fresh snapshots

**Files:**
- Modify: `web/app.js`

**Step 1: Write the failing test**

No JS test harness exists. Verify this task through server/runtime checks only after Tasks 1-3 are green.

**Step 2: Write minimal implementation**

Update dashboard requests to stop depending on `cache_s=300`, and render snapshot source plus snapshot age alongside existing fetch timing.

**Step 3: Run verification**

Run:
- `python3 -m unittest discover -s tests -p 'test_*.py'`
- hit `/api/funding` and `/api/price` twice and verify second response is `source=snapshot` with low `snapshot_age_ms`

### Task 5: Verify end-to-end latency

**Files:**
- Modify: `server.py`
- Modify: `web/app.js`

**Step 1: Run fresh verification**

Run:
- `python3 -m unittest discover -s tests -p 'test_*.py'`
- compare `force=1` vs default endpoint timings from localhost

**Step 2: Confirm target behavior**

Expect:
- default requests return snapshot responses in milliseconds
- snapshot age stays within the configured refresh interval
- `force=1` still returns live data path for debugging
