# CLAUDE.md

Operational guide for working in this repo with Claude Code. **Read this first every session.**
For the running history and open threads, read `devlog/worksheet.md` (Sessions 1–13).

---

## What this project is (and why it's shaped this way)

The **Restoration and Archive Trust (RAT)** is a volunteer charity digitising a large railway-photograph
archive. Volunteers hand-enter each photo's details into a bespoke **FileMaker Pro** app that has no
formal support and an opaque form design. The brief: export that data into **Supabase (PostgreSQL)**.

**This is a transient, per-engagement migration tool, not a permanent service** (clarified directly by the
user, Session 13, correcting an assumption that cron/Task Scheduler automation was a natural next step).
The typical use: a client (RAT) provides desktop access to the machine where FileMaker Pro lives, the
scripts get installed there, and the migration runs during that on-site engagement — there's no persistent
server context for this to live in as a background job. Delta Sync's value in that context is catching late
edits during a multi-day engagement without repeating a full multi-hour sync, not "keep two systems in sync
forever." The **real end goal**, per the user's own words: "getting the RAT people off old s/w onto the
newer (Postgres/React Form/REST API/iOS, Android app)." This repo's job is narrower than that whole
program — get FileMaker's data reliably into Postgres, with a GUI reliable enough to run during a transient
on-site engagement. The REST API/React frontend/mobile apps RAT will actually use afterward are a separate,
not-yet-scoped piece of work (not started in this repo as of Session 13) — don't assume it belongs here.

**Update (Session 18): the "transient tool" framing above is deliberately being widened, not
replaced.** The real deployment situation only became explicit this session: one desktop, one
FileMaker database, at a remote site, operated by RAT volunteers — the user does not want to be
physically present (or remoted in) for routine care like reopening FileMaker after a reboot. Agreed
direction: evolve the current tkinter GUI into a small-footprint installable **agent** (not a classic
"thin client" — Stage 1 extraction needs native FileMaker ODBC on Windows and can never move off that
desktop) that runs as a background service, connects **outbound** over a persistent websocket to a
relay hosted on `oci` (the desktop itself stays unreachable from outside, no inbound holes needed),
and is monitored/controlled from a browser-based admin page reachable from anywhere. This is a real,
deliberate scope expansion — the first server-side component beyond Supabase itself — agreed with the
user, not backed into. Nothing beyond a small first step is built yet; see `devlog/worksheet.md`
Session 18 for the full discussion and the agreed "foot in the door" first step (a `rat.sync_status`
table + a GUI link-out button, proving the read path before any of the agent/relay/auth work).

**Internalise this — it drives every decision:** the source data is error-prone (hand-entered) and the
source schema is partly opaque. We can't fully trust the source. This is a *data rescue*, not a clean
sync. That's why the loader **quarantines** dubious rows instead of coercing them, why the **live DB**
is treated as ground truth over any hand-written DDL, and why **`image_no`** (the archive id, e.g.
`arc00001`, `bpuk2237` — note: multiple prefixes, not a uniform format) is the one anchor we hold onto.

> **Naming:** repo is `filemaker_sync`, target schema is `rat`. Plan is to consolidate into
> `PicaLocoBackend` and rebrand `rat`→`picaloco` — but only after the pipeline is stable. Don't rebrand yet.

---

## Architecture in one glance

Two separate stages. Do not conflate them.

```
FileMaker Pro
  └─ Stage 1  EXTRACT   scripts/filemaker_extract.py   (ODBC, Windows only)
       emits flat DML  ->  .sql files   OR   flat load into rat_migration (staging)
  └─ Stage 2  LOAD      scripts/db_dml_loader.py        <- the crown jewel
       parse -> quarantine -> resolve FKs via lookup caches -> upsert into rat.*
  Incremental change detection (Session 2):
       scripts/fm_metadata_probe.py   - read-only FileMaker metadata diagnostic
       scripts/db_sync_manifest.py    - skinny-scan + manifest diff (new/changed/etc.)
```

The tkinter GUI (`gui/`) is a `subprocess` wrapper around the CLIs. It contains **no** migration logic.

### GUI status: reliable and demo-ready (Sessions 9–13)

Every layer was broken or stale at some point across Sessions 9–13 and is now fixed and live-verified —
not just code-reviewed, but confirmed against the real running app, several times catching a fix that
looked right but wasn't. Full session-by-session blow-by-blow (each bug's exact reproduction and fix) lives
in `devlog/worksheet.md` Sessions 9–13; this is the current-state summary.

- **Script wiring & config resolution** (Session 9, 11): path resolution was resolving scripts as bare
  filenames relative to launch directory instead of `scripts/`; `scripts/config_manager.py`'s
  `_parse_config()` still read a pre-profile `config.toml` shape that stopped existing at Session 5
  (confirmed live crash: `Missing required configuration key: 'db'`) — this, not staleness, was the real
  reason Test Connections/Migration Status were broken. Data-moving operations
  (`full_sync`/`incremental_sync`/`export_files`/`export_images`) now route to the real, maintained
  `scripts/filemaker_extract.py`; `test_connections`/`migration_status` stay on
  `filemaker_extract_refactored.py` (its class-based `--json` diagnostic layer has no equivalent in the
  real script). "Load to Target" switched from the broken `--mode dml_files` to the working
  `--mode migration_schema`. The dead `gui/config_manager.py` + `database_connections.py` +
  `filemaker_extract_refactored.py` trio (confirmed unreachable — nothing imports them, subprocess
  `sys.path[0]` always resolves to `scripts/`) was deleted outright.
- **Live output streaming** (Session 12): operations used to buffer all subprocess output until
  completion (a multi-minute Full Sync showed nothing but a progress bar). Now streams line-by-line via
  `Popen` with merged stdout/stderr. Found a second, independent bug along the way: three scripts'
  console log handlers only ever activated in `--debug` mode (which the GUI never passes), so even a live
  stream had nothing to show — fixed to always attach, `debug_mode` now only affects logger *level*.
  Side effect fixed for free: `OperationManager._current_process` was declared but never assigned, so Stop
  Action could never actually kill a running subprocess.
- **Tabbed layout + live panel** (Session 13): window split into an Actions tab (Quick Actions buttons)
  and a Status tab (Migration Overview + new `LiveStatusPanel` — a color-coded, level-filterable, always-live
  view fed by the same `LogManager` callback the streaming fix produces). Starting any action
  auto-switches to the Status tab. Does **not** replace the separate `LogViewerWindow` popup
  (search/filter/sort) — that's still available via "View Logs", the new tab is a simpler at-a-glance view.
- **A real deadlock, found and fixed** (Session 13): `LiveStatusPanel` was the first `LogManager` callback
  ever invoked concurrently from two background threads — exposed a latent bug where
  `_add_log_entry()` called `_notify_callbacks()` **while holding** a non-reentrant `_log_lock`, and the
  except-handler's own error logging re-entered the same method trying to reacquire it. Froze the whole
  app on the first concurrent trigger. Reproduced mechanically (a standalone test hung at the exact
  expected point on the old code, ran clean on the fix) before trusting it.
- **A real app freeze, found and fixed** (Session 13): `ConnectionTester.test_all_connections()` has
  always launched two fully redundant subprocesses (one `--info-only --json` call already reports both
  statuses). Harmless before, but the new startup auto-connection-test (below) made it fire at every
  launch, and an immediate operation click could pile a third concurrent FileMaker-touching subprocess on
  top — FileMaker Pro's ODBC driver doesn't reliably handle concurrent access from one desktop file, and
  the resulting hang stalled the GUI's own process too. Fixed at the root: `OperationManager` gained
  `self._subprocess_lock`, serializing **every** subprocess launch (connection tests, status refreshes,
  real operations — everything funnels through `run_python_command()`) regardless of caller, so this class
  of bug can't recur; always acquired from a background thread, so it can never freeze the GUI itself. Also
  fixed the redundancy: `test_all_connections()` now makes exactly one subprocess call.
- **Migration Overview reflects reality, not a misleading percentage**: its "Target"/"Completion %"
  always meant "current `rat_migration` staging row count vs FileMaker" — a fine proxy when only Full Sync
  populated staging, actively misleading once Delta Sync deliberately narrows staging to just the delta.
  Relabeled honestly (`'Target'` → `'Staging'`, `'Completion %'` → `'Staging Match %'`, with a caption);
  no longer auto-refreshes with staging numbers after Delta Sync/Incremental Sync at all (leaves whatever
  was last shown, which is more honest than a number that looks broken); and now shows a genuinely useful
  **delta-aware breakdown** instead (`MigrationOverview.show_delta_result()`) — built entirely from Delta
  Sync's own JSON summary, no new query: `ratcatalogue` shows `"N update(s)"`/`"✓ Success"`, the four
  always-refreshed reference tables show `"Refreshed (full)"`, the two untouched tables are labeled as such.
- **Delta Sync's own result is surfaced**: `run_incremental_sync.py` now emits a JSON summary
  (new/changed/verified/manifest-advanced) on every exit path — the GUI's summary label shows it directly
  ("✓ Delta Sync completed in 37.5s — 1 changed, 1 verified") instead of just a duration.
- **Polish**: hover tooltips on every Quick Action button, written specifically to clear up "Incremental
  Sync" (not delta-driven despite the name) vs "Delta Sync" (the real one) confusion; connection cards
  self-test ~500ms after launch instead of staying "Not tested" until a manual click or config save;
  `LiveStatusPanel` gained a level filter (`All`/`Info+`/`Warning+`/`Errors only`, default `Info+`) that
  filters the *view* only, `LogManager`'s own history is untouched.

**GUI target-profile picker added (Session 23)** — a header combobox (`FileMakerSyncGUI`'s
`_discover_target_profiles()`/`_resolve_default_profile()`) now lets the GUI pick any
`[database.target.<profile>]` from `config.toml` (`supabase`/`oci`) instead of always running
against the file's `active_profile`. Session-only, doesn't write back to `config.toml` — a restart
still defaults to whatever the file says, same as any CLI run without `--target-profile`.
`OperationManager.target_profile` is the single hook: `run_python_command()` appends
`--target-profile <profile>` to every subprocess call from there, so connection tests, status
refreshes, and every real operation all picked it up without touching each call site. Live-tested:
launched the real GUI and confirmed the startup connection-test's own log line showed
`--target-profile oci` reaching the real subprocess and connecting for real — widget rendering
itself not visually confirmed (no way to see the Windows desktop from that session).

**Known, accepted limitations** (deliberate choices, not bugs):
- Stop Action only cancels Full/Incremental/Delta Sync, Load to Target, and Export operations — Test
  Connections/Update Dashboard use a separate code path it doesn't touch.
- Migration Overview's numbers reflect `rat_migration` staging, not the final `rat.*` schema — no clean
  1:1 table mapping exists for `ratcopyright`/`ratlabels`/`prompts` to do a proper `rat.*`-comparison
  redesign; parked as a separate, bigger piece of work.

**`config.toml`'s `active_profile` is now `oci`** (was `supabase`). The old cloud project's pooler no
longer resolves the tenant (`FATAL: tenant/user postgres.kmoehqdowgdupzdxtbei not found` — found live in
Session 9, likely paused/rotated on Supabase's side, unrelated to this repo); `oci` is the actively-loaded,
live-tested target since Session 5. Every script/GUI operation that doesn't pass `--target-profile`
explicitly now defaults to `oci`.

---

## Loader status — the real production loader is adopted

`scripts/db_dml_loader.py` is now the **real production loader** (~1495 lines): schema-driven column
handling, in-memory **lookup caches** (no N+1 per-row SELECTs — genuinely true everywhere as of Session 6,
see below), the full **sanitise/quarantine/reject** pipeline, and correct natural-key conflict targets. It
has `env_secrets` wired into `get_db_engine`.
(It replaced an earlier stale 781-line reconstruction — if you see references to that, they're historical.)

> **⚠ Major limitation found AND FIXED, Session 23 — `batch_upsert()` now genuinely upserts.**
> `batch_upsert()` — the single function every `migrate_*` call in this pipeline goes through, for
> every table — used to always do `ON CONFLICT (...) DO NOTHING`, never `DO UPDATE`. "Correct
> natural-key conflict targets" above means the conflict *target* was always right; the conflict
> *action* silently skipped instead of updating. **Once a row existed for a given natural key, its
> stored field values could never be corrected by any future sync** — Full Sync, Delta Sync, or a
> manual Stage 2 re-run all funneled through this same insert-or-skip logic. Confirmed live: re-ran
> Stage 2 against freshly-corrected staging specifically to fix 4 known-corrupted rows, and it made
> zero difference — had to fix those 4 with a direct, targeted `UPDATE` instead, which is how this
> was discovered. Compounded with `run_incremental_sync.py`'s Delta Sync "verify" step
> (`fetch_verified()`), which only checks image_no *presence* in `rat.catalog`, not content — so an
> already-existing "changed" row verified successfully even when its real update was silently
> dropped, and the sync manifest then advanced as if it were correctly synced, permanently masking
> the failure. This meant "Delta Sync catches late edits" was **only true for genuinely new
> records**, not edits to existing ones.
>
> **Fixed, same session:** `_apply_upsert_conflict()` now does a real `ON CONFLICT DO UPDATE`,
> refreshing every inserted column except the conflict target and the *creation* audit columns
> (`created_by`/`created_date` are preserved; `modified_by`/`modified_date` correctly move). Two
> real risks handled, not just the naive fix: (1) Postgres refuses `DO UPDATE` if one batch's own
> `VALUES` list repeats a conflict key (`"ON CONFLICT DO UPDATE command cannot affect row a second
> time"`) — a real, not hypothetical, risk given the 13 known duplicate `image_no` rows (see
> "Verified facts" below) resolving to the same `catalog_id` for `catalog_metadata`/`usage`/
> `picture_metadata` — fixed with a same-batch dedup guard (last-value-wins, matching
> `migrate_catalog()`'s own `OrderedDict` pattern). (2) A `WHERE ... IS DISTINCT FROM ...` guard so
> re-syncing unchanged data doesn't rewrite `modified_date` on all 141k catalog rows every Full Sync
> (real MVCC bloat/write amplification at this scale, and it would make `modified_date` meaningless
> as "last real edit"). Validated first against real Postgres via throwaway scratch tables (never
> touching `rat.*`), covering all of the above plus audit-column correctness, **then validated live
> against real `oci`**: a full Stage 2 run left every table's row count exactly unchanged, 20
> randomly sampled untouched rows kept their exact original `modified_date` (the no-op guard holding
> at real scale, not just synthetic), and — the actual point — the 9 rows the backtick audit had
> found genuinely drifted (see below) are now **correctly corrected**, re-confirmed via a second
> audit run showing **0 confirmed hits and 0 remaining drift**. See `devlog/worksheet.md` Session 23
> for the full discovery-and-fix trace.

**Every lookup goes through `lookup_caches` now (Session 6, ~30–1,000× faster).** `migrate_route`/
`migrate_builder`/`migrate_organisation`/`migrate_location` used to call `get_location_id()`/
`get_country_id()` — always a live, unindexed `SELECT` per row — because the shared cache was only built
once, *after* `migrate_catalog()`. Fixed by building `country`/`location` caches right after their own
`migrate_*` (via the new `cache_lookup_table(name, column)` helper, which updates one key instead of
replacing the whole `lookup_caches` dict) and running `create_lookup_indexes()` first thing in `main()`
instead of buried inside the old single cache-build call. `get_location_id()`/`get_country_id()` now check
the cache first and fall through to the *unchanged* live-SELECT + auto-add path only on a genuine miss —
zero behaviour change, just short-circuits the common case. `get_builder_id()` is confirmed dead code
(nothing calls it) — left alone.

**Increment 2 is done (Sessions 5–8).** Sub-task 1 (Session 5): `migrate_catalog_builder` now upserts on its
real key `(catalog_id, builder_id, builder_order)` — the DB constraint already existed
(`catalog_builder_catalog_id_builder_id_builder_order_key`), so this was a conflict-target change plus
deleting the now-dead `TRUNCATE` branch (traced: nothing downstream depended on the truncate having
happened). **Never dedupe on `(catalog_id, builder_id)` alone** — 5,927 legitimate multi-build pairs
differ only in `builder_order`/payload. Sub-tasks 2–4 (Session 8): a real delta-driven sync,
`scripts/run_incremental_sync.py` — see "Delta-driven incremental sync" below.

**`main()` now runs the full population**, not just `catalog_metadata` — every `migrate_*` call was
restored (Session 5; they'd been commented out, so the live cloud data was never produced by a run of
that committed code). Current order (Session 6 added the two early cache-build steps, Session 7 added
`ensure_unknown_builder()`, both marked `*`):
`create_lookup_indexes()* → country → cache 'country'* → organisation → location → cache 'location'* →
route → collection → photographer → builder → ensure_unknown_builder()* → catalog →
create_lookup_index_cache() (full rebuild) → catalog_metadata → catalog_builder → usage →
picture_metadata`.

**Fixed (Session 7): `catalog_builder`/`catalog_metadata` no longer duplicate on `NULL`-key re-runs.**
SQL never treats `NULL = NULL`, so a row whose `builder_id`/`catalog_id` couldn't be resolved never
conflicted with its own earlier insert, and every re-run added a fresh copy. Two different fixes for two
different situations: `catalog_builder` rows with unresolved `builder_id` but real payload
(`plant_code`/`works_number`/`year_built`) now fall back to a real sentinel `'UNK'` builder row (same
convention already used for `location`/`country`'s `'unknown'`) — see `ensure_unknown_builder()`.
`catalog_metadata` rows whose `catalog_id` never resolved are skipped outright (the row is unlinkable —
its own `catalog` insert already failed, no sentinel makes sense for the spine table). Proven idempotent
by running Stage 2 twice and confirming zero growth; 58,326 + 9 accumulated legacy duplicates cleaned up
once. See `devlog/worksheet.md` Session 7.

**Fixed a silent one-row-per-table loss (Session 5):** `read_data_from_migration_schema()` had a dead
debug line (`first_row = result.fetchone()`) that consumed a row off the cursor before the real read
loop. This plausibly explains the long-standing "catalog is 1 short of the manifest" mystery below —
removed.

**Delta-driven incremental sync — real, runnable, live-proven (Session 8):** `scripts/run_incremental_sync.py`
ties `db_sync_manifest.py`'s scan/diff engine, `filemaker_extract.py`, and `db_dml_loader.py` together into
an on-demand "sync now": skinny-scan → diff against the manifest → extract exactly the `new`/`changed`
`image_no`s (new `filemaker_extract.py --image-nos-file` flag, parameterised `WHERE image_no IN (...)`) plus
a full refresh of the 4 small reference tables → load via the loader's unmodified `--mode migration_schema`
entry point → verify against `rat.catalog` directly (DB truth, not the loader's exit code) → compare each
verified row's freshly-extracted `row_hash()` (new in `db_sync_manifest.py`, `sha256` over the row's JSON)
against the manifest's stored hash to separate real edits from FileMaker import-inflation (ROWMODID bumped,
content unchanged) → `PgManifest.mark_loaded()` (new) advances the manifest **only** for the verified set.
Run with `python.exe scripts/run_incremental_sync.py --target-profile oci` (add `--dry-run` to stop after
the scan/diff). **Two real bugs found and fixed getting this to run live** (see `devlog/worksheet.md`
Session 8 for full detail — both worth knowing about elsewhere in this codebase):
1. `filemaker_extract.py`'s `--ddl`/`--dml` flags default to `False` and must be passed explicitly for
   `get_table_data()` to actually fetch/insert data — passing `--db-exp` alone silently only runs the
   row-count action, no error, staging table ends up dropped-and-never-refilled. `run_incremental_sync.py`
   now passes `--ddl --dml` on every `filemaker_extract.py` call it makes.
2. `stripy()` (`scripts/db_dml_loader.py`) now guards against pandas' `NaN` (blank source cell), not just
   `None` — `migrate_builder()`'s `row['Location'] != None` check let a `NaN` float through because
   `NaN != None` is `True` in Python, crashing on `NaN.strip()`. Fixed both the one call site
   (`pd.notna(...)`) and `stripy()` itself, since it's reused across a dozen call sites reading raw
   DataFrame cells equally exposed to the same pattern.

Cron/Task Scheduler wiring is deliberately out of scope — this makes the sync correct and runnable on
demand, not automatic.

**Sync is now target-aware, not just source-aware (Session 19).** Every Check Sync/Sync also checks
(a) `repaired` — is anything the manifest believes is loaded actually missing from `rat.catalog` right
now (target-side data loss, independent of whether FileMaker's `ROWMODID` moved) — and (b)
`missing_images` — the same Postgres-vs-Storage gap check `upload_images_oci.py --list-missing` uses,
run unconditionally so a record whose catalog row is stable but whose photo never uploaded doesn't
silently stop being reported the moment it falls out of that week's delta. Both run every time, not
gated on there being a source-side delta at all. See `devlog/worksheet.md` Session 19.

**Two real, previously-unknown data-corruption bugs found and fixed (Session 19) — both "our own
pipeline silently diverging from the source value," not bad source data:**
1. `adjust_sql_syntax()` did a blind, whole-SQL-string backtick→doublequote replace meant to convert
   MySQL-style column-identifier quoting, but `df_to_sql_bulk_insert()` already emits correct
   Postgres identifiers by the time this ran — so it only ever found *data*, and silently mangled any
   literal backtick in any text field of any table, for this pipeline's entire history. Removed from
   both DML call sites (kept for DDL, where it may still be legitimate). **Blast radius quantified
   AND fixed (Session 23):** `scripts/audit_backtick_corruption.py` retrospectively confirmed
   exactly **4** already-loaded `rat.catalog` rows still carried the mangled value (all in
   `description`) — small, not the wider damage the "lots of junk in FileMaker" framing that
   prompted the audit might have suggested. **Attempting to fix via a normal Stage 2 re-run against
   fresh staging did NOT work** — see the loader's own `ON CONFLICT DO NOTHING` limitation flagged
   above, discovered as a direct result of this fix attempt failing. Fixed instead with a targeted,
   direct `UPDATE` per row; verified 0 confirmed hits remain. `rat_migration.reject_log` entries
   marked `resolved=true, resolution='fixed'`, with the literal backtick itself flagged in each
   note for a RAT volunteer to review/correct in FileMaker (a genuine source-side typo, not
   something to auto-fix) — exported to `rejects_backtick_audit_20260913.xlsx` as the handoff
   artifact (gitignored, not committed).
2. `export_images()` stripped spaces from the local filename before writing it, so any
   space-containing `image_no` (a real, common pattern — `"Class 1400 (11)"`, `"Porto Tram"`, etc.)
   extracted its photo correctly every run but under a filename that could never match any
   `--image-nos` list built from the real value — permanently invisible, no error anywhere. Fixed by
   dropping just the space-strip (`\n`/`\r` kept). **Recovered ~130 previously-invisible real photos
   in one re-run** once fixed.

Both confirmed with direct byte-level (hex dump) or FileMaker-screenshot evidence before touching
anything — see Session 19 for the full trace. Worth knowing if a "corrupted" or "missing" record ever
turns up again: check whether it's genuinely bad source data, or this pipeline's own code silently
changing a value in transit, before assuming either.

## Golden rules (invariants - do not violate without explicit sign-off)

1. **Quarantine over silent coercion.** Bad/ambiguous rows go to a `.reject` file with a reason. Never
   "fix" questionable source data by guessing. This is the project's most valuable behaviour.
2. **The live DB is ground truth.** `rat_schema_original.sql` is a pull of the real `rat` schema; the
   hand-written DDL in `supabase/schema/` has known drift. Reconcile against the live pull. Capture fresh
   before any restructuring: `supabase db dump --linked --schema rat`.
3. **`image_no` is the natural key.** `catalog.image_no` is `VARCHAR UNIQUE`. Joins back to source go
   through it. UUID surrogate PKs are internal; resolve them from `image_no` via lookup caches.
4. **Validate loader/sanitisation changes against `test/` fixtures - never iterate against the live DB.**
   `test/test.sql`, `test/test.sql.bad_data_examples`, `test/test.sql.reject` exist for exactly this.
5. **No secrets in commits.** Passwords live only in `.env` (see Secrets below). `config.toml` is
   secret-free and committed. A DB password leaked here once and was rotated - don't repeat it.
6. **Sequence: stabilise & document -> restructure -> rebrand.** Don't rename `rat`->`picaloco` or reshuffle
   into `PicaLocoBackend` until the pipeline is stable.

---

## Verified facts (from live diagnostics - trust these)

- **Migration is whole.** Freshest live counts on `oci` (Session 13, 2026-09-04, post-cleanup — supersedes
  older per-session figures below, kept for historical context): `catalog` / `catalog_metadata` / `usage` =
  **141,244**; `builder` 518 (517 real + `'UNK'` sentinel); `catalog_builder` 116,538; `organisation` 1,520;
  `location` 14,178; `route` 2,874; `photographer` 270; `collection` 66; `picture_metadata` 0 (needs local
  image files, none present in any session so far). `rat_migration.*` staging tables now exactly match the
  live FileMaker source row-for-row (no accumulated duplication — see "GUI status" above for the bug that
  caused and then fixed that). Older snapshot (Sessions 3–5, `catalog` **141,243**, `builder` 516, `route`
  2863, `location` 14176, `organisation` 1519, `catalog_builder` 116530, `picture_metadata` 141420 — that
  last figure was from the *original* `supabase` cloud target, not `oci`, and hasn't been reconciled).
  (Ignore `pg_stat_user_tables.n_live_tup` - reads 0 on never-analyzed tables; use exact `count(*)`.)
- **Builder's natural key is `code`, NOT `name`.** `builder.code` is `NOT NULL UNIQUE` (already enforced);
  `name` is nullable. Config confirms: `ratbuilders = ['"Builder code"']`.
- **`catalog_builder` carries payload** (`builder_order`, `works_number`, `plant_code`, `year_built`) and
  is **not** a pure junction. Its real key is **`(catalog_id, builder_id, builder_order)`**. There are
  5,927 legitimate multi-build pairs with differing payloads - **NEVER dedupe on `(catalog_id, builder_id)`**;
  that would destroy 6,302 real records.
- **Change detection uses FileMaker `ROWMODID`** (per-record commit counter, validated: present on all
  141,262 rows, 0 NULL, moves on edit). Two-pass: skinny scan `SELECT image_no, ROWID, ROWMODID` -> extract
  only the delta. Manifest = `rat_migration.sync_manifest`, keyed on `image_no`, storing last-loaded ROWMODID.
- **Known data-quality debris (surfaced, mostly report-only):** 13 duplicate `image_no` (12 `br...`, plus
  `lwp8181`); 3 NULL `image_no` (ROWIDs 42279, 47343, 145873); 354 `picture_metadata` rows with NULL
  `catalog_id`. **The "catalog is 1 short of the manifest" mystery is solved (Session 5):** root cause was
  `read_data_from_migration_schema()`'s dead `first_row = result.fetchone()` line silently consuming a row
  off the cursor. Fixed; a fresh full load on the `oci` target landed `catalog` at exactly 141,244, matching
  the manifest. (The cloud target itself wasn't re-loaded this session, so its `catalog` may still read
  141,243 until it's re-run.)
- **The `oci` target profile is live and fully populated (Session 5):** same source, same counts as above
  (allowing for organic growth since the cloud target's original load — e.g. `location` 14178 vs 14176,
  `route` 2874 vs 2863). `rat_migration.sync_manifest` is baselined; `--preview --target-profile oci` comes
  back clean and flags the identical known debris above. See `devlog/worksheet.md` Session 5 for exact
  counts and the six `filemaker_extract.py` bugs found getting a real live run working.
- **Stage 1/Stage 2 are fast now (Session 6):** the full `ratcatalogue` extract dropped from ~2h to
  ~3m46s (~31×); `migrate_route` dropped from ~35min to ~2s (~1,000×) — see "Loader status" above for the
  root cause and "Gotchas" below for a real bug this exposed.
- **`catalog_builder`'s real count is 116,538, not 116,530** — that older figure (Session 3, cloud target)
  may itself have been inflated by the same `NULL`-`builder_id`-never-conflicts bug Session 6 found and
  Session 7 fixed on `oci`; not reconciled against the cloud target. 57,187 of those rows resolve to the
  `'UNK'` sentinel builder (real data — many trains genuinely have no resolvable builder code, but do carry
  real `plant_code`/`works_number`/`year_built`); this is now stable across re-runs, not growing.
- **The real FileMaker data-entry layouts, from the user directly (Session 23), resolve several
  previously-unclear questions:**
  - **The mysterious `S`-prefixed `ratcatalogue` columns** (`Scountry`, `Sorganisation`, `Simage_no`,
    `Sgauge`, `Sactive_area`, `Sfacility`, `Scorporate_body`, `Scollection`, `Sbuilder code`,
    `Sworks number`, `Sroute`, `Slocation`) **are FileMaker's own "Search" layout criteria-storage
    fields, not real catalog data** — confirmed against the actual Search layout screenshot, which
    shows fields like `Simage_no`/`active_area` mapped 1:1 to these. Already correctly excluded from
    `rat.catalog`'s column list; nothing to fix, just previously undocumented.
  - **`Works number`/`Year built` are entered on the "Common carrier" and "Industrial" `RATcatalogue`
    layouts** (not the main "Data entry" one, and not the separate `RATbuilders` layout, which is
    only the builder *master list* editor, not a per-photo entry point) — on both, `Works no.` and
    `Year built` sit immediately adjacent on the same row. Plausible (not confirmed) explanation for
    the `cmuk0089` runaway-paste case (Session 23, "Loader status" above): a paste meant for
    `Description` (further down the same screen) landing in `Year built` instead. The same
    adjacency appears on both layouts, so it's a systemic form-design pattern, not a one-off.
  - **Two real scope gaps, found by comparing the `RATcollections`/`RATroutes` layouts against what
    actually lands in `rat.*` — since fixed.** `rat_migration.ratcollections` already carried
    `photographer`, `print_sales`, `internet_use`, `publications_use`, `contact`, `remarks`,
    `accession_number`; `rat_migration.ratroutes` already carried `organisation` ("Owning
    organisation"), `country`, `remarks` — both genuinely extracted by Stage 1, neither ever
    migrated by Stage 2. Initially documented only (user's call, given the PII angle below), then
    picked back up the same session: `rat.route` gained `organisation_id`/`country_id` (FKs,
    matching the existing `location_id`/`country_id` convention, not raw duplicated text) and
    `remarks`; `rat.collection` gained `photographer_id` (FK, same reasoning), `print_sales`/
    `internet_use`/`publications_use` (booleans), `accession_number`, `contact`, `remarks`. DDL:
    `supabase/schema/add_collection_route_columns.sql`, applied live and folded into
    `bootstrap_rat_schema.sql`. `migrate_route()`/`migrate_collection()` now populate all of it —
    see `devlog/worksheet.md` Session 23 for the full trace, including two real bugs found live
    (a `lookup_caches` staleness issue for the new FKs, and a whitespace-only `accession_number`
    value that `pd.notna()` alone didn't catch).
    **Confirmed live: `contact` (real donor/collector names and at least one full postal address —
    e.g. "[real name and full postal address - redacted]") is now public** — `anon` already had full
    `SELECT` on `rat.collection` (same as the already-public `owner`/`donor`), and the user chose to
    add `contact` with no column-level restriction, explicitly, after being shown this exact fact.
    Not an oversight — if this ever needs revisiting, it's a deliberate decision to reverse, not a
    bug to fix.
  - Full layout screenshots referenced above live in
    `FileMakerPro_source_details/RAT_Original_App_Images/` (`rat_form__common_carrier.png`,
    `rat_form__industrial.png`, `rat_form__ratcollections.png`, `rat_form__ratroutes.png`,
    `rat_form__search.png` — the last four saved to disk this session, weren't there before) plus
    `rat_orig_app_details.txt` (the main
    "Data entry" layout's documented tab order). See `devlog/worksheet.md` Session 23 for the
    conversation that produced these.
  - **Same staging-vs-target check run against `organisation`/`location`/`builder` — one real hit.**
    `organisation` (`type`/`country_id`, 1522/1522 populated) and `location` (`country_id`,
    14178/14178 populated) have no gap — both fully populated, and their FileMaker lookup screens
    (`rat_lookup__org_area.png`/`rat_lookup__location.png`) confirm they're plain name-pick value
    lists, nothing richer to capture. **`builder` had the exact same dead-column bug as
    `migrate_catalog()`'s columns above:** `rat.builder.plant_code`/`builder_plant`/`remarks` were
    0/518 populated despite `rat_migration.ratbuilders` already carrying the matching data — fixed
    the same session, dry-run validated (0 flags across 352 real values) then backfilled live:
    29/290/32 rows populated. No PII concern here (plant codes and manufacturer remarks), so unlike
    the collection/route gaps above, this one was fixed immediately rather than just documented.

---

## Constraints state (rat schema)

Added and correct: `collection_name_key` UNIQUE(name), `photographer_name_key` UNIQUE(name),
`catalog_builder_catalog_id_builder_id_builder_order_key` UNIQUE(catalog_id, builder_id, builder_order),
plus `catalog_id` keys on `catalog_metadata`/`usage` and the schema's own `picture_metadata.catalog_id UNIQUE`
and `builder.code UNIQUE`.

**Reconciled:** `builder_name_key` UNIQUE(name) was dropped (see `supabase/schema/fix_rat_builder_key.sql`) —
builder's real key is `code` (`builder.code NOT NULL UNIQUE`), which the adopted loader conflicts on.

---

## Secrets

One shared mechanism: `scripts/env_secrets.py` (`resolve_secret` + `url_quote`). Every password reader
imports it. Precedence: **CLI arg > env/.env > config.toml > default**. Env vars:

- `RAT_SOURCE_PWD` - FileMaker source password (the `train` account has **none** - leave blank)
- `RAT_TARGET_PWD` - password for the `supabase` target profile (legacy name, kept for back-compat)
- `RAT_TARGET_PWD_<PROFILE>` - password for a named target profile, e.g. `RAT_TARGET_PWD_OCI`. Resolved via
  `resolve_target_pwd(profile, ...)`. **No fallback to `RAT_TARGET_PWD`** for any profile except `supabase`
  itself — switching profiles can never silently reuse the wrong target's password.
- `RAT_TARGET_PROFILE` - which `[database.target.<profile>]` is active (also `--target-profile` CLI flag on
  `db_dml_loader.py`/`db_sync_manifest.py`/`filemaker_extract.py`); defaults to `supabase`.

`.env` lives at repo root, is gitignored, and is auto-loaded (python-dotenv). `config.toml` is secret-free
and committed. All `postgresql://` URLs must wrap the password in `url_quote()`.

---

## How to run (environment matters a lot)

- **Run from the repo root** - scripts read `config.toml` and `.env` from the working directory.
- **FileMaker (Stage 1, the probe, the sync scan) MUST run under native Windows Python**, invoked as
  `python.exe` from WSL, or from PowerShell. WSL's own Python has **no FileMaker ODBC driver** and can't
  see the Windows DSN (`rat` — a **User** DSN under `HKCU`, confirmed live Session 18; despite this
  doc previously saying "System DSN", the real one on the test machine is not). **Postgres-only work
  runs fine from WSL directly, confirmed live
  (Session 5)** - the loader/manifest/probe's target-DB side needs no Windows detour, only the
  FileMaker-facing side does.
- Two target profiles exist in `config.toml`: `supabase` (the original cloud project — unreachable as of
  Session 9, pooler no longer resolves the tenant) and `oci` (self-hosted, Tailscale — **the default** since
  Session 13, and the live-tested target since Session 5). Pass `--target-profile supabase` (or set
  `RAT_TARGET_PROFILE=supabase`) to point any of the three scripts at the old cloud project if it's ever
  revived. See Secrets above for each profile's password env var.
- **WSL's `python.exe` interop genuinely works for a full live run** (Session 5, confirmed against the real
  FileMaker file and a live OCI load, not just theory) — no separate Windows session needed, this session's
  agent ran Stage 1 + Stage 2 directly via `python.exe scripts/...` from WSL.
- Windows Python tooling: `pyodbc`, `psycopg2-binary`, `python-dotenv` were already installed; Session 5
  added `pandas`, `SQLAlchemy`, `tomli`, `pillow`, `tqdm` **unpinned** (that Windows Python is 3.13;
  `requirements.txt`'s `pandas==2.1.4` has no 3.13 wheel and fails building from source — pin needs
  loosening, outstanding).

```bash
# incremental sync preview (dry-run; the "what would change" view)
python.exe scripts/db_sync_manifest.py --preview
# same, against the OCI target
python.exe scripts/db_sync_manifest.py --preview --target-profile oci
# read-only FileMaker metadata probe
python.exe scripts/fm_metadata_probe.py --json fm_probe.json
# offline self-tests (no DB)
python.exe scripts/db_sync_manifest.py --selftest
python.exe scripts/fm_metadata_probe.py --selftest
```

---

## Gotchas that waste time

- **WSL can't do FileMaker** - `libodbc.so.2` / "data source name not found" means you're in WSL; use `python.exe`.
- **`config.toml` target creds live per-profile** - `host`/`user`/`pwd`/`port`/`dbname` are all inside
  `[database.target.<profile>]` (`supabase` or `oci`); only `schema`/`mig_schema`/`tgt_schema`/
  `default_migration_user` are shared at the parent `[database.target]` level. dbname is always
  `postgres`. Cloud pooler user is `postgres.<project-ref>`; self-hosted (Supavisor) is
  `postgres.<tenant>` (this box's tenant is `default`). Port `5432` = session pooler (use this - the
  loader holds explicit multi-statement transactions), `6543` = transaction pooler.
- **A failing statement inside a single `DO $$ ... $$` block rolls the WHOLE block back.** Add constraints
  individually if one might fail.
- **`FileMaker_ValueLists` is not a real system table** - pull controlled vocabularies from a DDR or the
  `Prompts` base table.
- **Line endings:** Windows checkout is CRLF; `.gitattributes` normalises. Patches from Linux are LF - use
  `git apply --3way` if one won't apply.
- `scripts/scripts.old/` and other `*.old` paths are cruft - don't build on them.
- **`db_dml_loader.py --mode dml_files` cannot parse a realistic FileMaker export** (confirmed against
  `test/test.sql`: mixed quoting, `Timestamp('...')`-style values, embedded punctuation all break its
  `pd.read_csv`-based parser). Use `--mode migration_schema` instead (reads `rat_migration.*` staging
  tables populated by `filemaker_extract.py --db-exp`) until the parser gets a real rewrite.

---

## Where things live

| Need | Look at |
|---|---|
| Stage-2 loader (the real, adopted one) | `scripts/db_dml_loader.py` |
| Stage-1 extract (target-connection path fixed Session 5; see Gotchas re: `--mode dml_files`) | `scripts/filemaker_extract.py` |
| Incremental sync engine (scan/diff/manifest) | `scripts/db_sync_manifest.py` |
| Incremental sync orchestrator (the actual periodic sync) | `scripts/run_incremental_sync.py` |
| Persisted reject history (writer + admin CLI reader) — see "Current focus" Session 20 | `scripts/reject_log.py` |
| FileMaker metadata probe | `scripts/fm_metadata_probe.py` |
| Shared secret resolution | `scripts/env_secrets.py` |
| Ground-truth schema (reference only, not re-appliable) | `rat_schema_original.sql` |
| Fresh-target bootstrap DDL (idempotent, corrections baked in, incl. `mobile_catalog_view`/`anon` grants) | `supabase/schema/bootstrap_rat_schema.sql` |
| **Disaster recovery** — full rebuild of `oci` from FileMaker Pro, verified live, a primary use case | `devlog/worksheet.md`'s "Reference: Disaster Recovery" section, right after the intro |
| Constraint DDL (already-applied patches, cloud target only) | `supabase/schema/fix_rat_constraints.sql`, `fix_rat_idempotency.sql` |
| `rat.collection`/`rat.route` column additions (already-applied, also folded into bootstrap) | `supabase/schema/add_collection_route_columns.sql` |
| Sanitisation fixtures | `test/` |
| Source field meanings / valid values | `FileMakerPro_source_details/` |
| History + open threads | `devlog/worksheet.md` |

---

## Current focus

**The pipeline is stable, fast, idempotent, and delta-sync-capable (Sessions 5–8) — and the GUI is now
genuinely reliable on top of it (Sessions 9–13).** `oci` is the live, fully-loaded target. Both pipeline
stages run in minutes, not hours. Re-running Stage 2 never accumulates duplicates on unresolved
natural-key lookups. `scripts/run_incremental_sync.py` is a real, live-proven "sync now" (see
"Delta-driven incremental sync" above). The GUI's every layer — script wiring, config resolution, output
streaming, layout, and two genuinely serious bugs (a deadlock, a concurrent-ODBC-access freeze) — has been
found and fixed against the real running app, not just reviewed; see "GUI status" above for the current
state and `devlog/worksheet.md` Sessions 9–13 for the full history. As of Session 13 this has all been
confirmed working live by the user, including the delta-aware Migration Overview breakdown.

**Scope reminder (Session 13): this repo is a transient, per-engagement migration tool**, not a permanent
service — see "What this project is" above. Don't propose making it more "always-on" (cron, background
services); that solves a problem this project doesn't have.

**Update (Session 14): the app-layer question is answered, built, and shipped — in a sibling repo, not
here.** The client's actual ask turned out to be much simpler than the existing `trainpixelfolio` mobile
app (over-specced, unfinished): a public web search/browse tool for the archive, keyed on `image_no`. A new
sibling repo, **`picaloco_web`**, is **live at https://picaloco-web.vercel.app** (Vite + React + TS +
Tailwind + react-router + `@supabase/supabase-js` + React Query), querying `oci`'s `rat` schema through a
recreated `rat.mobile_catalog_view` (it didn't exist on `oci`; recovered from the old cloud project's
dashboard and recreated — see `picaloco_web/DEVOPS.md` for the exact SQL if it ever needs rebuilding). Two
things worth knowing if you're touching `oci` from *this* repo's side: (1) that view now exists, additive,
doesn't affect the migration pipeline; (2) `anon`'s grants on `rat` were **tightened this session** — it now
only has `SELECT` on `mobile_catalog_view` + the small lookup tables, *not* raw `rat.catalog` (whose
`valuation`/`owners_ref` columns must never be public) — if a future `filemaker_sync` change needs `anon`
to read a different `rat` table directly, that's a deliberate grant now, not an accidental default. `oci`
is also now publicly reachable (Tailscale Funnel), not Tailscale-only — factor that in if `oci`'s exposure
surface ever matters to this repo's own work. Full detail in `devlog/worksheet.md`
Session 14. `picaloco_rest` (a third sibling repo) is not a custom backend, just a Vercel-hosted Swagger UI
mirror — not directly relevant to any of this.

**Update (Session 18): remote-agent direction agreed, first step in progress — see "What this project
is" above.** `run_incremental_sync.py` now writes each run's summary to `rat.sync_status` (DDL not yet
applied to `oci`) and the GUI has a Tools-menu link-out to `picaloco-web`'s (not-yet-built) `/admin`
page. Full agent/relay/websocket build not started — this is scoping/foot-in-the-door only. See
`devlog/worksheet.md` Session 18 for the full discussion and open threads.

**Update (Session 19): the biggest chunk of this session was live-testing `picaloco_agent` for the
first time and finding two real, previously-unknown data-corruption bugs in code that's been running
since this pipeline's earliest days** (backtick→doublequote mangling in DML text; space-stripped
local image filenames silently orphaning ~130 real photos) — both now fixed, see "Loader status"
above and `devlog/worksheet.md` Session 19 for the full trace. Sync itself also became target-aware,
not just source-aware (the `repaired`/`missing_images` checks, also in "Loader status" above) — a
real gap this session's testing exposed: a record whose target row/photo silently went missing could
previously go unreported forever. Separately, `picaloco_agent`'s activation-gate work (floated in
Session 18) was built for the DB-password half: a revocable **registration key**, checked against a
new Vercel serverless function in `picaloco_web` (`api/agent-auth.ts` + `rat.agent_licenses`), means
the agent no longer needs a baked-in DB password at all — full design/build/debugging detail in
`devlog/worksheet.md` Session 19 and `picaloco_web/DEVOPS.md` §11.

**Update (later, same day as Session 19 — Storage relay completed):** the Storage `service_role`
half of the activation gate, called "not yet built" at the time, is now done too — same
registration-key pattern, via a new opt-in `--registration-key` mode on `upload_images_oci.py`
(routes through `picaloco_web`'s `api/agent-storage-upload.ts`/`agent-storage-list.ts` relay
instead of talking to Storage directly). Default direct-service_role path (and `--init`) is
completely unchanged. `picaloco_agent`'s `target_config.has_storage_access()` replaces the old
`has_service_key()` gate on Upload Images — that gate previously blocked Upload Images entirely on
any real distributed install regardless of registration key, the actual gap this closed. Full
detail: `picaloco_web/DEVOPS.md` §11, `picaloco_agent/README.md`. `picaloco_agent` also picked up
its own public GitHub remote this session (`github.com/blueairblob/picaloco_agent`, previously
local-only) — history checked clean of secrets first (`.env`/`src/_secret.py` were always
gitignored, confirmed via full-history search before publishing).

**Update (Session 20): every reject in this pipeline used to only ever exist as a one-off local
file or a log line — invisible on a remote picaloco_agent install unless someone found and
forwarded it.** New `scripts/reject_log.py` persists loader/upload rejects into
`rat_migration.reject_log` (writer: `log_reject()`/`log_crash()`; admin reader:
`--list`/`--export`/`--resolve` CLI) — additive, not a replacement for the existing
`rejects_*.xlsx` reports. Wired into all three producers: `upload_images_oci.py`'s
`InvalidKeyError` path (severity `"reject"` — fully classified) and its unexpected-failure path
(`"ambiguous"`); `run_incremental_sync.py`'s `write_quarantine_report()` (`"ambiguous"` — its
existing "didn't verify in rat.catalog" reason is a catch-all, not db_dml_loader.py's actual
per-row cause); `db_dml_loader.py`'s `migrate_catalog()`/`migrate_catalog_metadata()`/
`migrate_catalog_builder()` per-row exception/skip branches. Every calling script's `__main__`
block also gained a top-level crash handler (`log_crash()`) so an unhandled exception still gets
one diagnostic row before the process exits non-zero. Data ref is deliberately three separate
nullable columns (`image_no`/`fm_rowid`/`catalog_id`), not one — this codebase already has real
cases where `image_no` itself is the broken thing (NULL/duplicate rows, see "Verified facts"
above). Table lives in `rat_migration`, not `rat` — same reasoning as `sync_manifest`, direct
Postgres access only, never PostgREST. **`--init` run against live `oci` and confirmed live
end-to-end (2026-09-12)**: the real `upload_images_oci.py` InvalidKeyError path was exercised
against two genuine backtick image_nos (not a synthetic call) and landed correctly in
`rat_migration.reject_log` with the right `catalog_id`/`run_id`/reason; `log_crash()` was also
confirmed against a real exception. See `devlog/worksheet.md` Session 20 for the full trace.

**Update (Session 23): the backtick data audit found its own answer, then found something much
bigger while trying to apply the fix.** The client had admitted a lot of junk exists in the
FileMaker source and wants it corrected; the user connected this to quantifying the historical
backtick bug's real damage plus a step toward ongoing export-time validation. Scoped down (via a
clarifying question) to just the retrospective scan first. New `scripts/audit_backtick_corruption.py`
diffed `rat.catalog` against a fresh FileMaker extract using the bug's own exact signature
(`` fresh_value.replace('`', '"') == stored_value ``, not just "these differ") — found **4** confirmed
hits, all in `catalog.description`. (A first raw-diff pass claimed 112,475 differences; investigated
before trusting it — almost entirely the loader's own whitespace-stripping the script hadn't
replicated, not real corruption. Real drift once fixed: 9 rows.) **Trying to apply the obvious
fix — re-running Stage 2 against the now-fresh staging — silently did nothing**, which is how the
`ON CONFLICT DO NOTHING` limitation flagged at the top of "Loader status" was actually discovered.
Fixed the 4 rows directly instead (targeted `UPDATE`, verified 0 hits remain). Extended
`reject_log.py` with a structured `resolution` field (`'fixed'` vs `'flagged_for_source_fix'` vs
`'dismissed'`, not just a bare boolean) and a `--source-script` filter, per the user's explicit ask
to track fixes in the reject table itself as a proper audit tool. **Then fixed the upsert bug
itself, same session**, once the user picked it back up as the next task — see the blockquote atop
"Loader status" for the full fix-and-validation trace (isolated scratch-table tests, then a live
Stage 2 run against real `oci`: row counts unchanged, the no-op guard holding at scale, and the 9
previously-drifted rows now genuinely corrected). **Then fixed the dead `rat.catalog` columns too**
(`works_number`/`year_built`/`plant_code`/`bw_image_no`, all 0/141,244 populated — `migrate_catalog()`
only copied staging columns whose names matched `rat.catalog`'s exactly, and these four never got
renamed from FileMaker's raw naming): new `CATALOG_COLUMN_RENAME` maps each to its real staging
name. Backfilling live surfaced a genuine, previously-invisible data-entry error — `image_no`
`cmuk0089`'s `Year built` held a 31,008-character runaway paste (the same phrase repeated ~350
times) — quarantined (left `NULL`, logged with `resolution='flagged_for_source_fix'`) rather than
loaded verbatim, since these four columns are unbounded `varchar` and nothing else in the pipeline
would have caught it. Backfilled 79,942/105,694/25,002/162 rows correctly (the gap from staging's
raw non-empty counts is whitespace-only placeholders, correctly stripped).

**Then built the general export-time validation pass the client actually asked for.**
`migrate_catalog()` now checks *every* text column it writes (not just the 4 renamed ones) for two
independent "this looks like a FileMaker data-entry accident" signals: a generous absolute length
backstop (5,000 chars — `description`'s own legitimate max is 2,294) and compression-ratio
repetition detection (`_looks_like_runaway_repetition()` — catches a short phrase pasted hundreds
of times, the actually-confirmed real failure mode, independent of what "normal length" means for
any given column). **Validated with a dry run against all ~1M non-empty text values in live staging
before writing anything**: the first pass found 16 flags, 15 of which were false positives (a
genuine short value like `"5634"` padded with ~200 chars of trailing whitespace, which compresses
well raw but is fine once stripped) — fixed by checking the stripped value, matching what actually
gets inserted; re-ran clean at exactly 1 flag, the one already-confirmed `cmuk0089` case. Ran live
against real `oci`: same result, row count unchanged. See `devlog/worksheet.md` Session 23 for the
complete discovery-to-fix narrative across all three pieces of work (upsert fix, dead columns,
general validation).

**Update (Session 23, continued): `rat.collection`/`rat.route`'s scope gaps fixed, then the
thumbnail size gap closed.** The collection/route gap documented just above was picked back up the
same session, PII question resolved directly (`anon` confirmed to already have full `SELECT` on
both tables; `rat.collection.contact` confirmed to hold real people's details; user's explicit call
to add it anyway, no restriction) rather than left parked — see `devlog/worksheet.md` for the full
trace, including two real bugs found live (a `lookup_caches` ordering issue for the new FKs, and a
whitespace-only value that slipped past the first fix attempt). Then: `export_images()`
(`filemaker_extract.py`) now actually generates a `webp_mobile/` thumbnail — previously **nothing
in this codebase ever did**, the historical thumbnail folder was a one-time external process, and
`run_incremental_sync.py`'s own image step had been silently working around the gap by uploading
the full-size `webp` instead. 320px fixed width (confirmed exactly against real historical files);
file-size quality (35) is an empirically-tuned approximation, not an exact match — the original
tool/settings are unknown, single knob left in place to retune. Not validated end-to-end through a
live FileMaker extraction (needs native Windows Python + ODBC) — worth a live Export Images/Delta
Sync run to confirm.

Smaller open threads: Migration Overview's full `rat.*`-comparison redesign
(parked, no clean table mapping); `picture_metadata` untested against real images (no local files); the 16
flagged source records (FileMaker-side); `--mode dml_files` parser rewrite (low priority,
`migration_schema` mode works); `requirements.txt`'s `pandas==2.1.4` pin (no Python 3.13 wheel);
`PicaLocoBackend`/`picaloco` rebrand (explicitly gated until stable — arguably close now, still not done);
`supabase-edge-functions` crash-loop fix handed to user, not yet confirmed run; the general
validation pass covers `rat.catalog`/`rat.builder`/`rat.route`/`rat.collection` now, not
`organisation`/`location` (checked, deliberately not wired up — see "Verified facts" above); the
new thumbnail generation not yet confirmed via a live FileMaker run; `picaloco_agent` builds, installs, and runs
(Session 22) but real distribution to a RAT volunteer is blocked on an unsigned-exe AV
false-positive (Norton quarantines every build regardless of `--onefile`/`--onedir`) — code signing
is the only mitigation that would actually generalize, deliberately deferred as a cost decision
while `picaloco_agent` is still pre-distribution dev work, not a live blocker.
Full detail in `devlog/worksheet.md`.
