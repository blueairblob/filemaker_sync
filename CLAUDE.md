# CLAUDE.md

Operational guide for working in this repo with Claude Code. **Read this first every session.**
For the running history and open threads, read `devlog/worksheet.md` (Sessions 1–3).
For the task currently in flight, read `devlog/HANDOFF_loader_adoption.md`.

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

## /!\ Loader status - READ BEFORE TOUCHING db_dml_loader.py

There are **two** loaders and they are not the same:

- The **committed `scripts/db_dml_loader.py`** is a **stale partial reconstruction**. Several of its
  migrate functions can't load the live uuid schema (e.g. `migrate_catalog` sets `id = image_no` into a
  uuid PK; `migrate_picture_metadata` never resolves `catalog_id`; uses `color_space` - column is
  `colour_space`). **Do not trust it as the source of truth.**
- The **real production loader** (the code that actually loaded the live data) lives at
  `C:\dev\RAT_Trains_Project\Migration\scripts\db_dml_loader.py` (~1476 lines): schema-driven column
  handling, in-memory **lookup caches** (no N+1), the full **sanitise/quarantine/reject** pipeline, and
  correct conflict keys throughout. **This is the one to adopt.**

The loader adoption is **in progress** - see `devlog/HANDOFF_loader_adoption.md` for exactly where it
stands and the remaining steps. Until it's done, treat loader behaviour as under active reconstruction.

---

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
  `catalog_id`; and catalog is 1 short of the manifest (141,243 vs 141,244 - the manifest baseline asserts
  one row it didn't verify).

---

## Constraints state (rat schema)

Added and correct: `collection_name_key` UNIQUE(name), `photographer_name_key` UNIQUE(name),
`catalog_builder_catalog_id_builder_id_builder_order_key` UNIQUE(catalog_id, builder_id, builder_order),
plus `catalog_id` keys on `catalog_metadata`/`usage` and the schema's own `picture_metadata.catalog_id UNIQUE`
and `builder.code UNIQUE`.

**Wrong - must be dropped:** `builder_name_key` UNIQUE(name) was added on a mistaken assumption; builder's
real key is `code`. `DROP CONSTRAINT builder_name_key` is part of the loader-adoption reconciliation.

---

## Secrets

One shared mechanism: `scripts/env_secrets.py` (`resolve_secret` + `url_quote`). Every password reader
imports it. Precedence: **CLI arg > env/.env > config.toml > default**. Env vars:

- `RAT_SOURCE_PWD` - FileMaker source password (the `train` account has **none** - leave blank)
- `RAT_TARGET_PWD` - Supabase/Postgres DB password (current, post-rotation)

`.env` lives at repo root, is gitignored, and is auto-loaded (python-dotenv). `config.toml` is secret-free
and committed. All `postgresql://` URLs must wrap the password in `url_quote()`.

---

## How to run (environment matters a lot)

- **Run from the repo root** - scripts read `config.toml` and `.env` from the working directory.
- **FileMaker (Stage 1, the probe, the sync scan) MUST run under native Windows Python**, invoked as
  `python.exe` from WSL, or from PowerShell. WSL's own Python has **no FileMaker ODBC driver** and can't
  see the Windows System DSN (`rat`). Postgres-only work (manifest, loader target) runs fine anywhere.
- Tooling installed by hand into Windows Python this session: `pyodbc`, `psycopg2-binary`, `python-dotenv`
  (pin these in `requirements.txt` - outstanding).

```bash
# incremental sync preview (dry-run; the "what would change" view)
python.exe scripts/db_sync_manifest.py --preview
# read-only FileMaker metadata probe
python.exe scripts/fm_metadata_probe.py --json fm_probe.json
# offline self-tests (no DB)
python.exe scripts/db_sync_manifest.py --selftest
python.exe scripts/fm_metadata_probe.py --selftest
```

---

## Gotchas that waste time

- **WSL can't do FileMaker** - `libodbc.so.2` / "data source name not found" means you're in WSL; use `python.exe`.
- **`config.toml` target creds are split** - `host` in `[database.target]`, `user`/`pwd`/`port` in
  `[database.target.supabase]`; merge them (child wins). Supabase dbname is always `postgres`. Pooler user
  is `postgres.<project-ref>`; port `5432` = session pooler (use this), `6543` = transaction pooler.
- **A failing statement inside a single `DO $$ ... $$` block rolls the WHOLE block back.** Add constraints
  individually if one might fail.
- **`FileMaker_ValueLists` is not a real system table** - pull controlled vocabularies from a DDR or the
  `Prompts` base table.
- **Line endings:** Windows checkout is CRLF; `.gitattributes` normalises. Patches from Linux are LF - use
  `git apply --3way` if one won't apply.
- `scripts/scripts.old/` and other `*.old` paths are cruft - don't build on them.

---

## Where things live

| Need | Look at |
|---|---|
| Real migration logic (adopt this) | the real loader from `Migration\scripts\db_dml_loader.py` |
| Stale committed loader (don't trust) | `scripts/db_dml_loader.py` |
| Stage-1 extract (already correct) | `scripts/filemaker_extract.py` |
| Incremental sync engine | `scripts/db_sync_manifest.py` |
| FileMaker metadata probe | `scripts/fm_metadata_probe.py` |
| Shared secret resolution | `scripts/env_secrets.py` |
| Ground-truth schema | `rat_schema_original.sql` |
| Constraint DDL | `supabase/schema/fix_rat_constraints.sql`, `fix_rat_idempotency.sql` |
| Sanitisation fixtures | `test/` |
| Source field meanings / valid values | `FileMakerPro_source_details/` |
| History + open threads | `devlog/worksheet.md` |
| Current task | `devlog/HANDOFF_loader_adoption.md` |

---

## Current focus

**Adopt the real loader** (see the handoff doc), then **Increment 2**: feed the sync engine's `new + changed`
delta into the now-idempotent loader, advance the manifest only on *verified* loads, and add a row-hash
backstop so a FileMaker import can't masquerade as ~140k edits. Smaller open threads: `picture_metadata`
idempotency, the 1 missing catalog row, the 16 flagged source records (FileMaker-side), and the
`requirements.txt` pins. All are listed with detail in `devlog/worksheet.md`.
