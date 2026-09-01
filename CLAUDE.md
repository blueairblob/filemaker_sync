# CLAUDE.md

Operational guide for working in this repo with Claude Code. **Read this first every session.**
For the running history and open threads, read `devlog/worksheet.md` (Sessions 1–3).
For the task currently in flight, read `devlog/HANDOFF_increment2.md`.

---

## What this project is (and why it's shaped this way)

The **Railway Archive Trust (RAT)** is a volunteer charity digitising a large railway-photograph
archive. Volunteers hand-enter each photo's details into a bespoke **FileMaker Pro** app that has no
formal support and an opaque form design. The brief: export that data into **Supabase (PostgreSQL)**
and provide a way to browse it.

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

---

## Loader status — the real production loader is adopted

`scripts/db_dml_loader.py` is now the **real production loader** (~1495 lines): schema-driven column
handling, in-memory **lookup caches** (no N+1 per-row SELECTs — genuinely true everywhere as of Session 6,
see below), the full **sanitise/quarantine/reject** pipeline, and correct natural-key conflict targets. It
has `env_secrets` wired into `get_db_engine`.
(It replaced an earlier stale 781-line reconstruction — if you see references to that, they're historical.)

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

**Increment 2's first sub-task is done (Session 5):** `migrate_catalog_builder` now upserts on its real
key `(catalog_id, builder_id, builder_order)` — the DB constraint already existed
(`catalog_builder_catalog_id_builder_id_builder_order_key`), so this was a conflict-target change plus
deleting the now-dead `TRUNCATE` branch (traced: nothing downstream depended on the truncate having
happened). **Never dedupe on `(catalog_id, builder_id)` alone** — 5,927 legitimate multi-build pairs
differ only in `builder_order`/payload. Remaining Increment 2 sub-tasks (drive the loader from the sync
manifest's delta, advance the manifest only on verified loads, row-hash backstop against FileMaker
import-inflation) are still ahead.

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

- **Migration is whole.** Exact counts: `catalog` / `catalog_metadata` / `usage` = **141,243**;
  `builder` 516, `photographer` 270, `collection` 65, `country` 117, `route` 2863, `location` 14176,
  `organisation` 1519, `catalog_builder` 116530, `picture_metadata` 141420, `rat_migration.sync_manifest` 141244.
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
  see the Windows System DSN (`rat`). **Postgres-only work runs fine from WSL directly, confirmed live
  (Session 5)** - the loader/manifest/probe's target-DB side needs no Windows detour, only the
  FileMaker-facing side does.
- Two target profiles exist in `config.toml`: `supabase` (the original cloud project, default) and `oci`
  (self-hosted, Tailscale). Pass `--target-profile oci` (or set `RAT_TARGET_PROFILE=oci`) to point any of
  the three scripts at it. See Secrets above for the profile's password env var.
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
| Incremental sync engine | `scripts/db_sync_manifest.py` |
| FileMaker metadata probe | `scripts/fm_metadata_probe.py` |
| Shared secret resolution | `scripts/env_secrets.py` |
| Ground-truth schema (reference only, not re-appliable) | `rat_schema_original.sql` |
| Fresh-target bootstrap DDL (idempotent, corrections baked in) | `supabase/schema/bootstrap_rat_schema.sql` |
| Constraint DDL (already-applied patches, cloud target only) | `supabase/schema/fix_rat_constraints.sql`, `fix_rat_idempotency.sql` |
| Sanitisation fixtures | `test/` |
| Source field meanings / valid values | `FileMakerPro_source_details/` |
| History + open threads | `devlog/worksheet.md` |
| Current task (Increment 2) | `devlog/HANDOFF_increment2.md` |

---

## Current focus

**The `oci` target bring-up is done (Session 5), fast (Session 6), and genuinely idempotent (Session 7).**
Full archive loaded live, sync manifest baselined, `--preview` clean, both pipeline stages run in minutes
instead of hours, and re-running Stage 2 no longer accumulates duplicates on unresolved natural-key
lookups (see "Loader status" above). `python.exe` interop works directly from this WSL environment — no
separate Windows session needed to drive Stage 1 against the real FileMaker file.

**Increment 2** (the real loader is adopted): its first sub-task, converting `catalog_builder` to an
incremental upsert, is **done** (Session 5) and now actually idempotent end-to-end (Session 7 closed the
`NULL`-key gap). Remaining: feed the sync engine's `new + changed` delta into the loader, advance the
manifest only on *verified* loads, and add a row-hash backstop so a FileMaker import can't masquerade as
~140k edits — unblocked, since the manifest, the full data, and now idempotent re-runs are all live on
`oci`. Smaller threads: `picture_metadata` untested against real images (no local files); the 16 flagged
source records (FileMaker-side, re-confirmed via the `oci` manifest baseline). Detail in
`devlog/worksheet.md`.
