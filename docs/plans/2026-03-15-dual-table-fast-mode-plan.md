# Dual Table Fast Mode Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make funding and price tables render primary ranking fields from low-latency snapshots first, then asynchronously enrich secondary columns while showing live latency on the frontend.

**Architecture:** `server.py` will maintain `fast` and `full` snapshots per table, with different refresh cadences and builders. `web/app.js` will fetch and render `fast` first, then merge `full` data by symbol and expose latency/readiness badges in the UI.

**Tech Stack:** Python stdlib threads and HTTP server, existing CLI modules, vanilla JS frontend, `unittest`

---

### Task 1: Add failing tests for fast/full snapshot registration and preference

**Files:**
- Modify: `tests/test_history_helpers.py`
- Modify: `server.py`

**Step 1: Write the failing test**

Add tests that require:
- default snapshot seeding registers both `fast` and `full` funding/price entries
- fast funding params disable Lighter price enrichment
- snapshot resolution returns `detail_level`

**Step 2: Run test to verify it fails**

Run: `python3 -m unittest discover -s tests -p 'test_history_helpers.py'`
Expected: FAIL because fast/full snapshot support is missing.

**Step 3: Write minimal implementation**

Add snapshot tier metadata and detail-level-aware snapshot keys/builders.

**Step 4: Run test to verify it passes**

Run: `python3 -m unittest discover -s tests -p 'test_history_helpers.py'`
Expected: PASS

### Task 2: Implement fast price builder

**Files:**
- Modify: `price_arb_cn.py`
- Modify: `server.py`
- Test: `tests/test_history_helpers.py`

**Step 1: Write the failing test**

Add a test for fast price params or fast builder behavior so the implementation must distinguish fast and full modes.

**Step 2: Run test to verify it fails**

Run: `python3 -m unittest discover -s tests -p 'test_history_helpers.py'`

**Step 3: Write minimal implementation**

Create a fast price path that avoids `orderBookOrders` and uses lighter last-trade plus assumed spread for primary ranking.

**Step 4: Run test to verify it passes**

Run: `python3 -m unittest discover -s tests -p 'test_history_helpers.py'`

### Task 3: Serve fast by default and full on async detail requests

**Files:**
- Modify: `server.py`
- Modify: `web/app.js`
- Modify: `web/index.html`
- Modify: `web/styles.css`

**Step 1: Write the failing test**

Add a test that snapshot payloads include `detail_level` and preserve fast-path snapshot preference.

**Step 2: Run test to verify it fails**

Run: `python3 -m unittest discover -s tests -p 'test_history_helpers.py'`

**Step 3: Write minimal implementation**

Expose:
- default table requests => `fast`
- async enrichment requests => `full`
- frontend merge-by-symbol rendering
- latency badges in each panel

**Step 4: Run test to verify it passes**

Run: `python3 -m unittest discover -s tests -p 'test_history_helpers.py'`

### Task 4: Verify latency and freshness

**Files:**
- Modify: `server.py`
- Modify: `web/app.js`

**Step 1: Run full verification**

Run:
- `python3 -m unittest discover -s tests -p 'test_*.py'`
- `python3 -m py_compile server.py arb_rank.py price_arb_cn.py http_json.py`
- localhost timing script for funding/price fast/full requests

**Step 2: Confirm results**

Expect:
- default fast requests return in low milliseconds
- funding fast snapshot age is about 1 second
- price fast snapshot age is as low as upstream allows
- frontend exposes both fast and detail latency/readiness
