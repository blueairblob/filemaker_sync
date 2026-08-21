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

## Session 3 — 2026-08-20 — Secrets externalised + loader idempotency (with a key-design save)

**Focus:** Move DB passwords out of `config.toml` into a shared env/.env mechanism, then verify the live migration and make the loader safe to re-run (the precondition for incremental sync). A row-count diagnostic on the way turned up a genuine `catalog_builder` key-design error and prevented silent data loss.
**Status:** `completed` (4 of 5 upserts made idempotent; `picture_metadata` deferred, now understood).

---

### Context

Externalising secrets was the immediate trigger, but it had a knock-on effect: the loader's target password in `config.toml` was the *pre-rotation* value, so `db_dml_loader.py` had silently been unable to write to Supabase. Fixing secrets unblocked the loader — which made "verify the loader actually works and is safe to re-run" the right next move before building Increment 2 on top of it.

---

### Decisions

| Decision | Rationale | Alternatives Considered |
|---|---|---|
| One shared `env_secrets.py` (`resolve_secret` + `url_quote`), imported by every password reader (config_manager, database_connections, filemaker_extract, db_dml_loader, db_sync_manifest) | Single mechanism, same env var names + precedence everywhere; `config.toml` can be secret-free and committable | Copy-paste env logic per file (drift); gitignore config.toml forever (loses committable structure) |
| Precedence: CLI arg > env/.env > config.toml > default; **URL-encode** all `postgresql://` passwords | .env passwords may contain URL-special chars that would corrupt the connection string (a latent bug the move would have exposed) | Raw interpolation (breaks on `@`/`:`/`/` in passwords) |
| Commit a **secret-free `config.toml`**; gitignore only `.env` | Non-secret structure (DSNs, hosts, paths) is useful to commit and documents the pipeline | Gitignore config.toml + ship config.toml.example (redundant once secret-free) |
| Row-count diagnostic **before** any Increment 2 work | Cheapest question, highest payoff: tests whether the migration is whole and the manifest baseline honest | Assume the loader/baseline are fine and build on them |
| Loader idempotency: conflict on **real keys** — `name` (collection/photographer/builder), `(catalog_id, builder_id, builder_order)` (catalog_builder); `picture_metadata` **deferred** | Auto-UUID `id` never collides → every re-run duplicated. The 3-col catalog_builder key was proven by data (see below) | 2-col `(catalog_id, builder_id)` — **WRONG**, would have destroyed real records; full-payload key — 162 collisions, loses `builder_order` info |
| **Never blind-DELETE** the 5,927 catalog_builder "duplicates" | Inspection showed all 5,927 had *differing payloads* — genuinely distinct build records, not copies | Dedupe on `(catalog_id, builder_id)` — would have deleted 6,302 real archival rows |

---

### Commands

```sql
-- [DONE] Row-count diagnostic (Supabase SQL editor). n_live_tup reads 0 (stale stats) — ignore;
-- trust exact count(*). catalog / catalog_metadata / usage all = 141,243 -> spine fully loaded.
-- catalog_missing_metadata = 0, catalog_missing_usage = 0  -> old id=image_no bug did NOT leave them empty.
-- Lookups populated: builder 516, photographer 270, collection 65, country 117, route 2863,
--   location 14176, organisation 1519, catalog_builder 116530, picture_metadata 141420, manifest 141244.
```

```sql
-- [DONE] Idempotency pre-check found the trap the constraint later hit:
--   collection/photographer/builder name dupes = 0  (clean)
--   catalog_builder (catalog_id,builder_id) dupes = 5927   <-- constraint would fail
--   picture_metadata: 0 dupe catalog_id, 0 orphans, 354 NULL catalog_id
-- Drill-down: all 5927 pairs have DIFFERING payloads (12,229 rows) -> distinct records, not copies.
-- Key test: dupes_on (catalog_id,builder_id,builder_order) = 0, null builder_order = 0  -> THE key.
```

```sql
-- [DONE] Constraints added (three name keys individually — the all-in-one DO block
-- rolled back when the wrong 2-col catalog_builder key failed), then the correct 3-col key:
ALTER TABLE rat.collection   ADD CONSTRAINT collection_name_key   UNIQUE (name);
ALTER TABLE rat.photographer ADD CONSTRAINT photographer_name_key UNIQUE (name);
ALTER TABLE rat.builder      ADD CONSTRAINT builder_name_key      UNIQUE (name);
ALTER TABLE rat.catalog_builder
  ADD CONSTRAINT catalog_builder_catalog_id_builder_id_builder_order_key
  UNIQUE (catalog_id, builder_id, builder_order);
-- Verified: all four present in pg_constraint.
```

```powershell
# [DONE — pushed] Secrets bundle + loader idempotency patch
git apply picaloco_secrets.patch           # env_secrets.py + 4 readers
git apply fix_rat_idempotency_loader.patch # batch_upsert composite support + 4 conflict keys
# committed: secrets mechanism, secret-free config, .env ignore, idempotency loader
```

---

### Outcome

Secrets now resolve through one shared `env_secrets.py` (env/.env first, config fallback, URL-encoded), so `config.toml` is committed secret-free and only `.env` is ignored. This also fixed the stale loader password.

The row-count diagnostic **verified the migration is whole**: the catalog spine (141,243) and both children fully loaded — overturning the standing fear that the old code left `catalog_metadata`/`usage` empty. The manifest baseline is honest bar one row (catalog 141,243 vs manifest 141,244).

The loader is now **idempotent on four of five tables**, conflicting on real keys. The fifth, `catalog_builder`, drove the session's main save: its constraint failed on the assumed `(catalog_id, builder_id)` key, and investigation showed all 5,927 "duplicate" pairs were **distinct build records with different payloads** (works number / plant / year), correctly identified by `builder_order`. The right key is `(catalog_id, builder_id, builder_order)` — proven unique with zero NULLs. A naive dedupe would have destroyed 6,302 real archival records; quarantine-over-coercion (the constraint refusing bad coercion) prevented it.

---

### Findings & Gotchas

- **`pg_stat_user_tables.n_live_tup` reads 0 on never-analyzed bulk-loaded tables** — stale estimate, not empty. Trust exact `count(*)`; `ANALYZE` refreshes the estimate (cosmetic).
- **A failing statement inside a single `DO $$ ... $$` block rolls back the WHOLE block** — the three clean `name` constraints didn't get added until run separately from the failing `catalog_builder` one.
- **`catalog_builder` is not a pure junction** — it carries payload (`builder_order`, `plant_code`, `works_number`, `year_built`). "Duplicate" `(catalog_id, builder_id)` pairs are legitimate multi-build records. `builder_order` is a better key component than the full payload (payload alone has 162 collisions).
- **The 5,927 duplicates are the historical fingerprint of the bug being fixed** — prior loader runs conflicting on the never-colliding UUID `id` re-inserted links every run.
- `picture_metadata` is cleaner than feared: 0 duplicate `catalog_id`, 0 orphans, 354 NULL `catalog_id`. `UNIQUE(catalog_id)` would add fine (Postgres allows many NULLs) — its idempotency fix is a small follow-up, deferred only to avoid widening scope mid-fix.
- **URL-encode DB passwords** in every `postgresql://` string — moving secrets to `.env` can introduce special chars that silently corrupt raw-interpolated URLs.

---

### Open Threads

- [ ] **`picture_metadata` idempotency** — add `UNIQUE(catalog_id)` and flip its `batch_upsert` to `id_column='catalog_id'` (clean; ~10 min, mirrors this session).
- [ ] **The single missing catalog row** (141,243 vs manifest 141,244) — one row the baseline asserts as loaded but isn't in `catalog`; Increment 2's verified loads will expose/fix it.
- [ ] **Increment 2** — feed the sync engine's `new + changed` delta into the now-idempotent loader; advance the manifest only on verified loads; add the row-hash backstop for import inflation.
- [ ] **The 16 flagged source records** (13 duplicate `image_no`, 3 null keys) — FileMaker-side cleanup while the trial is live.
- [ ] Pin `pyodbc`, `psycopg2-binary`, `python-dotenv` in `requirements.txt`.
- [ ] Commit `fix_rat_idempotency.sql` into `supabase/schema/` (DDL provenance, beside `fix_rat_constraints.sql`).
- [ ] *(Carried)* run `fix_rat_constraints.sql` (catalog_metadata/usage) if not yet applied; DDR from the `.fmpur` copy; supabase-py vs SQLAlchemy prune; JWT rotation; consolidation/rebrand.

---
