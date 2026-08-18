# filemaker_sync — RAT / PicaLoco migration backend

Migration tooling that moves the **Railway Archive Trust (RAT)** train-photograph catalogue from **FileMaker Pro** into a normalised **Supabase / PostgreSQL** schema. This is internal tooling for a volunteer archival project, not a distributable product.

The primary metadata key throughout the system is the **archive id** (`image_no`, e.g. `arc00001`) — it is the natural key that ties the flat source rows to the normalised target.

> **Naming:** the repo is currently `filemaker_sync` and the target schema is `rat`. The plan is to consolidate this (with `picaloco_rest`) into **PicaLocoBackend** and rebrand `rat` → `picaloco` — but only once the pipeline is stable. Until then, "RAT" and "PicaLoco" refer to the same project.

---

## The pipeline

Migration is **two stages**. This matters: the extract and the loader are separate programs, and for a long time only the first was obvious.

```
FileMaker Pro
   │
   │  Stage 1 — EXTRACT  (filemaker_extract.py / _refactored.py)
   │  Pulls source "as-is" and emits flat DML.
   ▼
 .sql DML files          ── OR ──   flat load into  rat_migration  (staging)
   │                                        │
   │  Stage 2 — LOAD / NORMALISE  (db_dml_loader.py)
   │  Parse → quarantine bad rows → explode flat→relational → resolve FKs → upsert
   ▼                                        ▼
        Supabase  rat.*  (normalised, ~12 tables)
```

Stage 2 has two input modes matching the two Stage-1 routes:

- `--mode dml_files` — reads the `.sql` files written by `--fn-exp` (the **file route**)
- `--mode migration_schema` — reads the flat `rat_migration` staging tables written by `--db-exp` (the **direct route**)

**The loader (`db_dml_loader.py`) is the crown jewel.** It is the *only* place the flat 70-column `ratcatalogue` is exploded into the relational `rat.*` tables, and the only place the sanitisation/quarantine rules live. Stage 1 just gets the data out of FileMaker.

---

## Repository layout

```
filemaker_sync/
├── scripts/                        # the engines (CLI)
│   ├── filemaker_extract.py            # Stage 1 — original procedural extract (ran production)
│   ├── filemaker_extract_refactored.py # Stage 1 — class-based refactor (FileMakerMigrationManager)
│   ├── data_exporter.py                # DataExporter — SQL formatting/escaping used by the refactor
│   ├── db_dml_loader.py                # Stage 2 — flat→relational normaliser + quarantine  ← crown jewel
│   ├── config_manager.py, database_connections.py
│   ├── build_exe.py, deploy.py, version_info.py   # packaging helpers
│   └── scripts.old/                    # legacy copies — cruft, scheduled for pruning
├── gui/                            # tkinter desktop GUI (so clients never touch a terminal)
│   ├── filemaker_gui.py                # main window + action bindings
│   ├── gui_operations.py               # operation map + subprocess dispatch to the engines
│   ├── gui_widgets.py                  # action buttons, status cards
│   ├── gui_logging.py, gui_logviewer.py
│   └── config_manager.py, database_connections.py
├── supabase/
│   ├── schema/                         # hand-written DDL + fix_rat_constraints.sql (prereq patch)
│   └── migrations/                     # pull snapshots (auth/storage only — rat not captured here)
├── test/                           # sanitisation fixtures (test.sql, .bad_data_examples, .reject)
├── FileMakerPro_source_details/    # source schema.json, PDFs, form/lookup screenshots (controlled vocab)
├── rat_schema_original.sql         # live rat schema pull — ground truth for reconciling
├── RAT_schema.docx, RAT_mindmap_v1.PNG   # original DB design docs
├── backend_scripts_rat_log.md      # dev log / notes from the original build
├── config.toml                     # connection + export config (see below)
└── requirements.txt
```

The GUI is a **wrapper around the CLI engines** — `gui_operations.py` shells out via `subprocess`; it does not reimplement any migration logic.

---

## Data model

- **Source:** one flat, wide `ratcatalogue` table (plus a few lookups), discovered via FileMaker's `FileMaker_BaseTableFields` metadata.
- **Target (`rat` schema):** ~12 normalised tables with UUID surrogate PKs and real foreign keys — `catalog`, `catalog_metadata`, `usage`, `picture_metadata`, `collection`, `photographer`, `builder`, `catalog_builder`, `country`, `organisation`, `location`, `route`.
- `catalog.image_no VARCHAR UNIQUE` preserves the **archive id** as the natural key across the migration.
- Imprecise dates are handled with a real `DATE date_taken` plus separate `circa` / `imprecise_date` text fields, so "circa 1935" survives without corrupting the date column.

---

## Sanitisation & quarantine

Volunteer-entered archive data is inconsistent, so the loader **quarantines rather than silently coercing** — bad rows go to a `.reject` file with a reason, instead of being force-fitted. The recovered rules:

- Quote-placeholder protection before CSV parsing (handles mixed quoting and commas inside descriptions)
- Column-count mismatch → reject the row
- Parse failure → reject the statement, with the exception message
- Missing foreign key → `'unknown'` sentinel
- Boolean coercion from `'yes'` strings
- Audit columns (`created_by` / `modified_by` / timestamps) auto-stamped per batch

`test/test.sql.bad_data_examples` and `test/test.sql.reject` are the fixtures for this behaviour. Treat these rules as the most fragile, most valuable part of the codebase — they were invented on the fly during the original migration.

---

## Setup

### Prerequisites

- Python 3.9+
- FileMaker Pro with the ODBC driver, ODBC/JDBC sharing enabled, and a **System** DSN
- A Supabase project (PostgreSQL)
- Windows is the current dev/target OS for the GUI (tkinter); the CLI is cross-platform

### Install

```bash
python -m venv py3
py3\Scripts\activate          # Windows;  source py3/bin/activate on Linux/WSL
pip install -r requirements.txt
```

### Configure `config.toml`

The engines and the loader both read `config.toml` from the working directory. Sections:

```toml
[database.source]              # FileMaker ODBC
dsn  = 'your_filemaker_dsn'
user = 'filemaker_user'
pwd  = '...'

[database.target.supabase]     # Supabase / PostgreSQL
user = 'postgres.<project-ref>'
pwd  = '...'
host = 'aws-0-<region>.pooler.supabase.com'   # session pooler
port = '5432'                                 # 5432 session pooler, NOT 6543 transaction pooler

[export]
path   = '/path/to/export/directory'   # where --fn-exp writes .sql and where the loader reads them
prefix = 'rat'

[debug]
# ...
```

> **Do not commit real credentials.** History hygiene has bitten this repo before (a leaked DB password was rotated; the anon JWT is still parked pending a frontend-coordinated rotation). Keep secrets out of tracked files.

---

## Running it

### GUI (recommended — no terminal needed)

```bash
python gui/filemaker_gui.py
```

Test connections, then use the action buttons. The client flow for a full migration is:

1. **Export to Files** — Stage 1, writes DML `.sql` to the export path
2. **Load to Target** — Stage 2, runs `db_dml_loader.py` in `dml_files` mode against those files

Other buttons: **Full Sync** (`--db-exp --ddl --dml`, flat load to staging), **Incremental Sync**, **Export Images**, **Test Connections**, **Update Dashboard**.

### CLI

```bash
# Stage 1 — extract to DML files
python scripts/filemaker_extract.py --fn-exp --ddl --dml

# Stage 1 — flat load straight into the staging schema
python scripts/filemaker_extract.py --db-exp --ddl --dml

# Stage 2 — normalise into rat.* (file route)
python scripts/db_dml_loader.py --mode dml_files --export-path <dir> --user-id you

# Stage 2 — normalise from staging (direct route)
python scripts/db_dml_loader.py --mode migration_schema --export-path <dir> --user-id you

# Useful flags
python scripts/filemaker_extract.py --get-images          # extract container images
python scripts/filemaker_extract.py --migration-status --json
python scripts/filemaker_extract.py --info-only --debug   # connection test, verbose
```

Stage-1 options: `--db-exp`, `--fn-exp`, `--ddl`, `--dml`, `--get-images`, `--max-rows N`, `--start-from ID`, `--debug`.
Stage-2 options: `--mode {dml_files|migration_schema}` (required), `--export-path` (required), `--batch-size N`, `--user-id`, `--debug`.

---

## Current state & known issues

**Working:** the full two-stage pipeline is recovered, documented, and runnable from both CLI and GUI. The GUI now wraps *both* stages (the loader was recently wired in).

**Before the next migration run:** apply `supabase/schema/fix_rat_constraints.sql` to the live `rat` schema — it adds the `UNIQUE (catalog_id)` that the loader's `catalog_metadata` / `usage` upserts now depend on, and drops a duplicate FK. Run its dedupe pre-check first.

**Rough edges (tracked):**

- GUI operations don't stream output — long loads look frozen until they finish (`subprocess.run(capture_output=True)`; a `Popen` streaming version is planned).
- `Load to Target` runs `dml_files` mode, so run **Export to Files** first — pointing it at stale `.sql` after a `--db-exp` run would load stale data.
- Five `batch_upsert` calls still conflict on the auto-generated `id`, so re-runs can duplicate rows for `collection` / `photographer` / `builder` / `picture_metadata` / `catalog_builder`. Fix in progress.
- The loader mixes supabase-py and SQLAlchemy client calls; one path is dead and should be pruned.
- `rat_schema_original.sql` came from the dashboard viewer, not `pg_dump`, so it reads correctly but isn't cleanly re-appliable. For a canonical dump use `supabase db dump --linked --schema rat`.

---

## Picking this up in Claude Code

Fast orientation for an agent or a returning human:

- **Start with the loader.** `scripts/db_dml_loader.py` is where the real logic is — the `migrate_*` functions, FK resolution, and quarantine. Everything else feeds it.
- **Ground truth is the live DB**, captured in `rat_schema_original.sql`, not the hand-written DDL in `supabase/schema/` (which has known drift). When in doubt, reconcile against the live schema.
- **The dev log** `backend_scripts_rat_log.md` and **the worksheet** `devlog/worksheet.md` carry the "why" behind decisions and the running list of open threads — read them before changing the loader or the schema.
- **Source vocabularies** (valid values for controlled fields) are in `FileMakerPro_source_details/` — useful when validating or extending sanitisation.
- **Capture live schema before restructuring** (`supabase db dump --linked --schema rat`) and **run `git status` per repo** before any move/rename. Sequencing principle for this project: stabilise & document → restructure → rebrand.

---

## Related repos

- [`trainpixelfolio`](https://github.com/blueairblob/trainpixelfolio) — Expo / React Native browse-only frontend (to be renamed `PicaLocoFrontend`)
- [`picaloco_rest`](https://github.com/blueairblob/picaloco_rest) — PostgREST/Swagger API docs (low priority; Supabase provides PostgREST natively)
- `PicaLocoBackend` — future consolidation target for this repo + `picaloco_rest`
