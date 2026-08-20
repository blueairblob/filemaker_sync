# Dev Worksheet

> Living journal of development sessions. Maintained as a tech blog — decisions, commands, outcomes, and open threads.

---

## Session 1 — 2026-08-18 — Wire the DML loader into the GUI

**Focus:** Resolve whether the GUI hid a newer migration codebase, then close the gap it exposed — get the second-stage normaliser running behind the GUI so the full two-stage pipeline needs no terminal. Ship it as one verified patch.
**Status:** `completed`

---

### Context

This follows a longer recovery arc (predating this worksheet): reconstructing the missing second stage `db_dml_loader.py`, rotating a leaked Supabase DB password, untangling a 1.47 GB large-file push and a PowerShell↔WSL2 line-ending divergence, and a three-way reconciliation of the live `rat` schema against the hand-written DDL and the loader. Coming back after ~a year, a nagging question remained: did the tkinter GUI wrap an *updated* migration engine, or was it just a shell around the two original CLI scripts? This session answered that and then wired in the half that was missing.

---

### Decisions

| Decision | Rationale | Alternatives Considered |
|---|---|---|
| Confirmed GUI is an **extract-only wrapper**; the refactored extract is behaviourally identical to the original | Grep showed `gui_operations.py` shells out to `filemaker_extract_refactored.py` only; escaping + `df_to_sql_bulk_insert` (incl. the `ratcatalogue` picture→image_no swap) match the original byte-for-byte | Assuming the GUI held newer logic and patching against it (would have been wrong) |
| Wire the loader into the GUI via `subprocess`, mirroring the extract path | Leaves the crown-jewel loader untouched; reuses the existing `run_python_command` mechanism | Importing the loader as a module (tighter coupling, risk); rewriting the loader (unnecessary) |
| `Load to Target` runs `dml_files` mode, gated on **target connection only** | The file route carries the loader's full sanitisation/quarantine; the loader needs Supabase, not FileMaker | `migration_schema`/staging mode (stale-files risk); gating on both connections (over-strict — the loader never touches FileMaker) |
| Read `export.path` from `config.toml` at dispatch time | `OperationManager` holds no config; avoids threading config through GUI constructors | Hardcoding a path (brittle); adding config plumbing to `OperationManager` (larger, riskier patch) |
| Ship all changes as **one** `git apply` patch, verified against a pristine `HEAD` | Nothing from the chat had been applied yet; one reviewable, reversible artifact | Multiple separate diffs/files (harder to apply cleanly and in the right order) |

---

### Commands

```bash
# [APPLIED] Confirm the GUI wraps the extract, not the loader — nothing imports db_dml_loader
grep -rnE "db_dml_loader|migrate_catalog|batch_upsert" gui/     # -> no matches
grep -nE "filemaker_extract_refactored" gui/gui_operations.py    # -> subprocess target
```

```bash
# [APPLIED] Apply the GUI wiring with strict single-anchor matching, then syntax-check
python3 /tmp/patcher.py            # 9 edits across 3 files, fails loudly if any anchor != 1 match
python3 -m py_compile gui/gui_operations.py gui/gui_widgets.py gui/filemaker_gui.py scripts/db_dml_loader.py
```

```bash
# [APPLIED] Build the combined patch (loader fix + constraints SQL + GUI wiring) and verify it applies to a clean HEAD
git add -N supabase/schema/fix_rat_constraints.sql
git diff HEAD > picaloco_pipeline.patch
git archive HEAD | tar -x -C /tmp/verify && (cd /tmp/verify && git init -q && git add -A && git commit -qm base \
  && git apply --check --verbose ../picaloco_pipeline.patch)   # -> APPLIES CLEANLY
```

```bash
# [APPLIED — by user] Apply and push
git apply picaloco_pipeline.patch      # (use --3way on Windows/WSL2 if CRLF trips it)
git add -A && git commit -m "Wire db_dml_loader into GUI; fix catalog_metadata/usage upsert keys; add constraint prereqs"
git push
```

```sql
-- [RECOMMENDED — not yet run] Prerequisite DDL. Must run against the live `rat` schema BEFORE the next loader run.
-- File shipped by the patch at supabase/schema/fix_rat_constraints.sql
-- Adds UNIQUE (catalog_id) on catalog_metadata (required by the loader's new ON CONFLICT),
-- drops the duplicate picture_metadata FK, and includes a duplicate-catalog_id pre-check.
```

---

### Outcome

The tkinter GUI now exposes a **Load to Target** button that runs `db_dml_loader.py --mode dml_files` against the export directory from `config.toml`. The full FileMaker → normalised `rat.*` pipeline is now reachable end-to-end from the GUI (**Export to Files → Load to Target**), with no command line required — the original goal behind adding a GUI in the first place. Everything was delivered as `picaloco_pipeline.patch` (five files: the loader upsert-key fix, the new `fix_rat_constraints.sql`, and the three GUI files), verified to apply cleanly against a pristine `HEAD` and to compile afterwards. The patch is now applied and pushed.

The side investigation also settled the "does the GUI hide newer code?" question for good: the GUI's extract is a faithful, byte-equivalent refactor of the original — no divergent migration logic, no drift risk switching CLI↔GUI. The only real asymmetry was the missing second stage, which this session fixed.

---

### Gotchas & Notes

- **Output doesn't stream.** `run_python_command` uses `subprocess.run(capture_output=True)`, so during an hour-long load the GUI shows nothing until it finishes — a client will think it's hung. `Popen` line-streaming is the real fix (next session).
- **Stale-files footgun.** `Load to Target` reads whatever `.sql` files are on disk (`dml_files` mode). Running **Full Sync** (the `--db-exp` staging route) and *then* `Load to Target` would point the loader at possibly-stale files. Intended flow is Export to Files → Load to Target. A second mode-aware button (or wiring Full Sync to `migration_schema` mode) removes this.
- **Order matters:** `fix_rat_constraints.sql` must run against live `rat` **before** the loader, or the new `ON CONFLICT (catalog_id)` errors on the missing unique constraint. Run its dedupe pre-check first.
- **Patch line endings:** generated on Linux (LF). On the Windows/WSL2 checkout, `git apply --3way` sidesteps CRLF mismatches (it merges against the normalised index blob).
- **Requirements smell:** `requirements.txt` lists `sqlalchemy`/`psycopg2-binary` but not `supabase` (supabase-py), while the loader references `supabase.table(...)` in places. Worth confirming which client path is actually live — a leftover from the mixed-client state noted earlier.

---

### Open Threads

- [ ] Run `fix_rat_constraints.sql` against the live `rat` schema, then re-run a small test load to confirm the upsert-key fix works end-to-end.
- [ ] Check live row counts on `catalog_metadata` / `usage` — determine whether those two tables ever loaded under the old (broken) `id = image_no` code.
- [ ] Add `Popen` line-streaming so GUI operations show live progress (makes the loader run feel client-ready).
- [ ] Add a second mode-aware button, or wire Full Sync's staging route to the loader's `migration_schema` mode, to eliminate the stale-files risk.
- [ ] Extend the idempotency fix to the remaining five `batch_upsert` calls still conflicting on the auto-UUID `id`: `collection`, `photographer`, `builder` (→ conflict on `name`), `picture_metadata` (→ `catalog_id`), and `catalog_builder` (needs a composite-uniqueness decision).
- [ ] Confirm the live supabase-py vs SQLAlchemy client path in the loader; prune the dead one.
- [ ] Rotate the anon JWT once the frontend is ready to receive a new key (deliberately parked).
- [ ] Consolidate `filemaker_sync` + `picaloco_rest` into `PicaLocoBackend`; rebrand `rat`→`picaloco` only after consolidation is stable.

---

## Session 2 — 2026-08-19/20 — Incremental-sync change detection (probe → validated engine)

**Focus:** Make the GUI a slick "extract all / extract latest, stage or push" tool. The headline feature — "latest" — forced the real question: how to detect *changed* records (not just new ones) in a 100k+-row FileMaker source whose data entry is error-prone. This session settled the detection strategy, proved it against the live file, and delivered + validated the change-detection engine end to end.
**Status:** `completed` (Increment 1); Increment 2 scoped, not built.

---

### Context

"Extract the latest data" sounds simple, but the existing `--start-from <image_no>` is append-only — it skips to an archive id and inserts what follows, so it **misses edits to existing rows**. For a volunteer-fed archive where *corrections are the norm*, that's the worst failure mode: a fixed typo never propagates and the browsable copy silently drifts from the source. The source has no modification-timestamp field (only `entry_date`, which doesn't move on edits), so a proper change key had to come from elsewhere. A deep dive into FileMaker's hidden metadata found it: the built-in **`ROWMODID`** pseudo-column (per-record commit counter, = `Get(RecordModificationCount)`), reachable over ODBC with no change to the source app.

---

### Decisions

| Decision | Rationale | Alternatives Considered |
|---|---|---|
| Detect change via **`ROWMODID`** in a **two-pass** design (skinny scan of `image_no, ROWID, ROWMODID` for all rows → full extract of the delta only) | `ROWMODID` moves on edits *and* inserts, needs no source-app change, and the skinny scan is seconds not minutes at 141k rows | Full refresh each run (heavy, needs perf work); hashing every row (still requires a full extract every run) |
| **Row-hash relegated to a secondary backstop**, not the primary signal | FileMaker bumps `ROWMODID` on *import* even for unchanged rows; a hash on just the delta filters those false positives | Hash as primary (defeats the point of the cheap scan) |
| **Polish tkinter**, don't rewrite the GUI | Fast, low-risk; the perceived "slickness" comes from the *workflow* (preview + staged review + streaming), not the widget toolkit | Move to a web UI (slicker ceiling, much more work; a native desktop tool that shells to Python is better staying native) |
| Manifest in **Supabase `rat_migration.sync_manifest`, keyed on `image_no`** (not `ROWID`) | `image_no` is the durable natural key; `ROWID` resets on a file rebuild → keying on it would corrupt the mapping. A rebuild then just looks like "everything changed" → a safe full reconcile | Keying on `ROWID` (fragile); a local file manifest (doesn't survive across machines) |
| Manifest `fm_rowmodid` advances **only on a successful load**; **deletes flag-for-review, never auto-apply**; **duplicate `image_no` pulled out of clean classification into a quarantine bucket** | Rejects retry next run instead of being marked done; an authoritative archive shouldn't drop records on a guess; `new + changed` must equal "rows we'll actually load" | Advancing on scan (would skip failed rows); auto-deleting; letting duplicates corrupt the diff |

---

### Commands

```bash
# [DONE] Read-only probe — characterise the live file (run from native Windows Python)
python.exe fm_metadata_probe.py --dsn rat --user train --pwd "" --json fm_probe.json
#   -> ROWMODID present on ALL 141,262 rows (0 NULL); rowVerColumns advertises ROWMODID; 273 never-edited, max 3864
# [DONE] Behaviour test: edit one record in FileMaker, then compare
python.exe fm_metadata_probe.py --dsn rat --user train --pwd "" --compare LATEST
#   -> EDITED=1  (bpuk2237, rowmodid 1435 -> 1436), NEW=0, MISSING=0   ✓ ROWMODID tracks real edits
```

```bash
# [DONE] Change-detection engine — offline validation first (no DB)
python.exe db_sync_manifest.py --selftest                                  # pure logic: PASS
python.exe db_sync_manifest.py --diff-snapshots prev.json curr.json        # snapshot vs snapshot
# [DONE] Live preview against a snapshot baseline (FileMaker scan, no PG yet)
python.exe db_sync_manifest.py --preview --manifest-snapshot <baseline>.json --dsn rat --user train --pwd ""
#   -> CHANGED=1 (bpuk2237), NEW=0, DELETE=0, UNKEYED=3, DUPLICATE image_no=13
```

```bash
# [DONE] Manifest on live Supabase (session pooler :5432)
python.exe -m pip install psycopg2-binary
python.exe db_sync_manifest.py --init                       # created rat_migration.sync_manifest
python.exe db_sync_manifest.py --baseline                   # dry run: 141,244 keyed / 3 unkeyed / 13 dup
python.exe db_sync_manifest.py --baseline --yes             # wrote 141,244 rows marked 'loaded'
python.exe db_sync_manifest.py --preview                    # live DB read: NEW 0 / CHANGED 0 / DELETE 0  ✓
```

---

### Outcome

Delivered two standalone, self-testing tools (both compile-checked and exercised offline before touching anything live):

- **`fm_metadata_probe.py`** — read-only FileMaker ODBC diagnostic. Proved the design is viable: `ROWMODID` is exposed and readable on every one of 141,262 rows (zero NULLs — the documented ODBC-NULL caveat did not bite, because `ratcatalogue` is a native table not an ESS shadow), the driver formally advertises it via `SQLSpecialColumns`/`rowVerColumns`, and it moved by exactly 1 on a real edit. Also dumped the schema catalog (`FileMaker_Tables`, `FileMaker_BaseTableFields`) and revealed the source's `RATbuilders`/`Plants` triple-occurrences that corroborate the 3-builder junction design.

- **`db_sync_manifest.py`** (Increment 1) — the change-detection engine: manifest DDL, skinny-scan reader, and a diff that sorts rows into new / changed / unchanged / candidate-delete plus two quarantine buckets (unkeyed, duplicate-key). Reuses the probe's snapshot format so real captures diff offline. **Now proven end-to-end on live Supabase:** `--init` created the manifest, `--baseline --yes` wrote 141,244 rows, and `--preview` against the live manifest returned a clean 0/0/0 — silent when idle, exact when something moves.

The full FileMaker → detect-delta loop works against production infrastructure. An idle sync now costs a few-second skinny scan and touches nothing; a real update pulls only the delta.

---

### Findings & Gotchas

- **First full pass found real data-quality issues** (surfaced, not fixed): **13 duplicate `image_no`** (12 × `br…`, plus `lwp8181`) — since `catalog.image_no` is UNIQUE, the target may be silently missing up to 13 photos; and **3 NULL `image_no`** rows (ROWIDs 42279, 47343, 145873) that can't be keyed. 16 records total need a human decision before a clean sync. The engine quarantined all 16 rather than corrupting the diff.
- **Stale target password.** `config.toml`'s `[database.target.supabase].pwd` was the *pre-rotation* value → `--init` failed auth (`password authentication failed for user "postgres"`). Passing the current password worked. **The loader reads the same section**, so this likely also blocks live loader writes — update it.
- **FileMaker ODBC is Windows-only.** Must run from native Windows Python (`python.exe`), not WSL — WSL has no FileMaker driver (`libodbc.so.2` / DSN-not-found) and can't see the Windows System DSN.
- **Blank FileMaker password.** The `train` account has no password; the probe was fixed to require only DSN + user and allow an empty `pwd`.
- **Split target config.** Connection details span two sections: `host` in `[database.target]`, `user`/`pwd`/`port` in `[database.target.supabase]`. The resolver must merge them (child wins). Supabase dbname is always `postgres` (not the `db`/`name` labels). Pooler user is `postgres.<project-ref>`; the `.` splits role from ref, so an auth error names the role `postgres`.
- **`FileMaker_ValueLists` is not a real system table** — pull controlled vocabularies from a DDR or the `Prompts` base table instead.
- **`.fmpur`** = FileMaker 12 runtime solution file (holds the real data). Needs *full* FileMaker Pro to open + ODBC-share (the runtime engine can't share); a 45-day Claris trial covers it. Also the best route to a **DDR** for de-opaquing the forms.
- Minor probe bug: audit-field detection via `cursor.columns()` hit `pyodbc.Row has no attribute 'column_name'` and didn't run — low stakes now that ROWMODID won.

---

### Open Threads

- [ ] **Increment 2:** feed `new + changed` into `db_dml_loader.py`; advance the manifest only on *verified* successful loads; add the row-hash backstop so a FileMaker import can't masquerade as ~140k edits.
- [ ] **Resolve the 16 flagged records** — inspect the 13 duplicate `image_no` and 3 NULL-key rows in FileMaker; assign ids or exclude.
- [ ] **Update `config.toml` target password** to the current (post-rotation) value — likely unblocks the loader too. (Don't commit it.)
- [ ] Add `pyodbc` and `psycopg2-binary` to `requirements.txt` (both were installed by hand into Windows Python this session).
- [ ] Generate a **DDR** from the `.fmpur` copy (schema, fields, value lists) using the FileMaker trial — de-opaque the forms and capture controlled vocabularies.
- [ ] Probe niceties: make `--compare` also write a timestamped snapshot (so a history accumulates); fix the `cursor.columns()` audit-field detection.
- [ ] Drop the new tools into `scripts/` and commit; consider a short `scripts/CLAUDE.md` as the sync engine grows.
- [ ] *(Carried from Session 1)* run `fix_rat_constraints.sql` + test load; check `catalog_metadata`/`usage` row counts; finish the five non-idempotent upserts; confirm supabase-py vs SQLAlchemy path; JWT rotation; consolidation/rebrand.

---
