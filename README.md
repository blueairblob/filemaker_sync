# filemaker_sync — RAT / PicaLoco migration backend

Migration tooling that moves the **Restoration and Archive Trust (RAT)** train-photograph catalogue from **FileMaker Pro** into a normalised **Supabase / PostgreSQL** schema. This is internal tooling for a volunteer archival project, not a distributable product.

The primary metadata key throughout the system is the **archive id** (`image_no`, e.g. `arc00001`) — it is the natural key that ties the flat source rows to the normalised target.

> **Naming:** the repo is currently `filemaker_sync` and the target schema is `rat`. The plan is to consolidate this (with `picaloco_rest`) into **PicaLocoBackend** and rebrand `rat` → `picaloco` — but only once the pipeline is stable. Until then, "RAT" and "PicaLoco" refer to the same project. Don't rebrand without explicit sign-off — see `CLAUDE.md`'s Golden Rules.

**This README is a fast orientation, not the source of truth.** `CLAUDE.md` (read every session) and `devlog/worksheet.md` (the full running history, Session 1 onward) are kept current continuously and carry the real detail — when in doubt, trust those over this file.

---

## The pipeline

Migration is **two stages**, plus a delta-sync layer on top. The extract and the loader are separate programs — don't conflate them.

```
FileMaker Pro
   │
   │  Stage 1 — EXTRACT  (filemaker_extract.py, Windows-only ODBC)
   │  Pulls source "as-is" and emits flat DML.
   ▼
 .sql DML files          ── OR ──   flat load into  rat_migration  (staging)
   │                                        │
   │  Stage 2 — LOAD / NORMALISE  (db_dml_loader.py)
   │  Parse → quarantine bad rows → explode flat→relational → resolve FKs → upsert
   ▼                                        ▼
        Supabase  rat.*  (normalised, 12 tables)

Incremental change detection, on top of both stages:
   db_sync_manifest.py    — skinny scan (image_no/ROWID/ROWMODID) + manifest diff
   run_incremental_sync.py — ties scan → targeted extract → load → verify into one "sync now"
```

Stage 2 has two input modes matching the two Stage-1 routes:

- `--mode migration_schema` — reads the flat `rat_migration` staging tables written by `--db-exp` (the **direct route** — this is the real, live-tested production path)
- `--mode dml_files` — reads the `.sql` files written by `--fn-exp` (the **file route** — a working fallback, not the primary path; its parser was rewritten to handle a realistic FileMaker export, see `CLAUDE.md`)

**The loader (`db_dml_loader.py`) is the crown jewel.** It is the *only* place the flat ~70-column `ratcatalogue` is exploded into the relational `rat.*` tables, and the only place the sanitisation/quarantine rules live. Stage 1 just gets the data out of FileMaker.

---

## Repository layout

```
filemaker_sync/
├── scripts/                        # the engines (CLI)
│   ├── filemaker_extract.py            # Stage 1 — the real, maintained extract (Windows-only ODBC)
│   ├── filemaker_extract_refactored.py # Stage 1 — class-based diagnostic layer (test-connections, migration-status --json)
│   ├── db_dml_loader.py                # Stage 2 — flat→relational normaliser + quarantine  ← crown jewel
│   ├── db_sync_manifest.py             # incremental sync — skinny scan + manifest diff
│   ├── run_incremental_sync.py         # incremental sync — the real "sync now" orchestrator
│   ├── reject_log.py                   # persisted loader/upload reject history (rat_migration.reject_log)
│   ├── audit_backtick_corruption.py    # retrospective audit for a historical DML-corruption bug (see CLAUDE.md)
│   ├── audit_key_debris.py             # turns known duplicate/unkeyed image_no debris into a volunteer hand-off
│   ├── upload_images_oci.py            # Storage upload (oci) for extracted images
│   ├── env_secrets.py                  # shared secret resolution (CLI > env/.env > config.toml)
│   ├── paths.py                        # Windows/WSL export-path translation
│   ├── build_exe.py, deploy.py, version_info.py   # packaging helpers
│   └── scripts.old/                    # legacy copies — cruft, not built on
├── gui/                             # tkinter desktop GUI (so clients never touch a terminal)
│   ├── filemaker_gui.py                # main window, tabbed layout (Actions / Status), target-profile picker
│   ├── gui_operations.py               # operation map + subprocess dispatch to the engines (no migration logic)
│   ├── gui_widgets.py                  # action buttons, Migration Overview, LiveStatusPanel
│   └── gui_logging.py, gui_logviewer.py
├── supabase/
│   └── schema/                         # idempotent DDL — bootstrap_rat_schema.sql is the fresh-target rebuild path
├── test/                           # sanitisation fixtures (test.sql, .bad_data_examples, .reject) — validate against these, never the live DB
├── FileMakerPro_source_details/    # source schema/form/lookup screenshots — controlled vocab reference
├── rat_schema_original.sql         # live rat schema pull — ground truth for reconciling hand-written DDL
├── devlog/worksheet.md             # full running history, Session 1 onward — read before changing the loader/schema
├── config.toml                     # connection + export config, secret-free (see below)
└── requirements.txt
```

The GUI is a **wrapper around the CLI engines** — `gui_operations.py` shells out via `subprocess`; it does not reimplement any migration logic.

---

## Data model

- **Source:** one flat, wide `ratcatalogue` table (plus `ratbuilders`/`ratroutes`/`ratcollections`/a few small lookups), discovered via FileMaker's `FileMaker_BaseTableFields` metadata.
- **Target (`rat` schema):** 12 normalised tables with UUID surrogate PKs and real foreign keys — `catalog`, `catalog_metadata`, `usage`, `picture_metadata`, `collection`, `photographer`, `builder`, `catalog_builder`, `country`, `organisation`, `location`, `route`.
- `catalog.image_no VARCHAR UNIQUE` preserves the **archive id** as the natural key across the migration.
- Imprecise dates are handled with a real `DATE date_taken` plus separate `circa` / `imprecise_date` text fields, so "circa 1935" survives without corrupting the date column.

---

## Sanitisation & quarantine

Volunteer-entered archive data is inconsistent and the source schema is partly opaque, so the loader **quarantines rather than silently coercing** — bad rows go to a `.reject` file (and, since Session 20, into `rat_migration.reject_log` for cross-run visibility) with a reason, instead of being force-fitted. This is the project's single most valuable behaviour. The recovered rules include:

- Column-count mismatch → reject the row
- Parse failure → reject the statement, with the exception message
- Missing foreign key → `'unknown'` sentinel row
- Runaway-paste / oversized text detection (compression-ratio heuristic, independent of a hardcoded length) → quarantine rather than load verbatim
- Boolean coercion from `'yes'` strings
- Audit columns (`created_by` / `modified_by` / timestamps) auto-stamped per batch, with a real `ON CONFLICT DO UPDATE` so a later re-sync can actually correct a previously-loaded row (not just insert-or-skip)

`test/test.sql`, `test/test.sql.bad_data_examples`, and `test/test.sql.reject` are the fixtures for this behaviour — validate sanitisation changes against these, never against the live DB.

---

## Setup

### Prerequisites

- Python 3.9+ (the real Windows install this pipeline runs on is 3.13 — `requirements.txt`'s pins are expressed as floors, not exact versions, for exactly this reason)
- FileMaker Pro, open and running, with the ODBC driver and a **User** DSN (`HKCU`, not a System DSN) configured
- A Supabase project — either the cloud original or a self-hosted instance (`oci`)
- Windows is required for anything touching FileMaker ODBC (Stage 1, the sync scan, the metadata probe) — invoke as `python.exe` from WSL, or from PowerShell directly. Postgres-only work (the loader's target side, `db_sync_manifest.py`'s manifest side) runs fine from plain WSL/Linux Python.

### Install

```bash
python -m venv py3
py3\Scripts\activate          # Windows;  source py3/bin/activate on Linux/WSL
pip install -r requirements.txt
```

### Configure `config.toml`

Every script reads `config.toml` (and `.env` for secrets) from the **working directory** — always run from the repo root. `config.toml` itself is secret-free and committed; real passwords live only in `.env` (gitignored) via `scripts/env_secrets.py`.

```toml
[database.source]              # FileMaker ODBC
dsn  = 'rat'
user = 'your_filemaker_user'
pwd  = ''

[database.target]
schema = ["rat_migration", "rat"]
active_profile = "oci"         # which [database.target.<profile>] below is used by default

[database.target.supabase]     # original cloud project
host = 'aws-0-<region>.pooler.supabase.com'
user = 'postgres.<project-ref>'
port = '5432'                  # session pooler — NOT 6543, the loader holds explicit multi-statement transactions

[database.target.oci]          # self-hosted instance
host = 'your-tailnet-hostname.ts.net'
user = 'postgres.default'
port = '5432'

[export]
path   = 'C:/path/to/export/directory'   # Windows-native form; scripts/paths.py translates for WSL
prefix = 'rat'
```

Passwords: `RAT_SOURCE_PWD` (FileMaker), `RAT_TARGET_PWD_<PROFILE>` (e.g. `RAT_TARGET_PWD_OCI`) in `.env`. Pass `--target-profile <name>` on any script (or set `RAT_TARGET_PROFILE`) to override `active_profile`.

> **Do not commit real credentials.** A DB password leaked here once and was rotated — keep secrets out of tracked files.

---

## Running it

### GUI (recommended — no terminal needed)

```bash
python gui/filemaker_gui.py
```

A tabbed window (Actions / Status) with a header target-profile picker, live-streaming output, and a Migration Overview that compares FileMaker directly against the real `rat.*` schema. Quick Actions: **Full Sync**, **Delta Sync** (the real incremental sync — not to be confused with the older "Incremental Sync" button), **Load to Target**, **Export Images**, **Test Connections**, **Update Dashboard**.

### CLI

```bash
# Stage 1 — flat load straight into the staging schema (the real production route)
python.exe scripts/filemaker_extract.py --db-exp --ddl --dml --target-profile oci

# Stage 2 — normalise from staging into rat.*
python scripts/db_dml_loader.py --mode migration_schema --export-path <dir> --target-profile oci

# Delta sync — scan, diff, extract only what changed, load, verify — one command
python.exe scripts/run_incremental_sync.py --target-profile oci
python.exe scripts/db_sync_manifest.py --preview --target-profile oci   # dry-run preview only

# Extract images (writes local jpg/webp/webp_mobile), then upload what's new to Storage
python.exe scripts/filemaker_extract.py --get-images --target-profile oci
python scripts/upload_images_oci.py --target-profile oci

# Connection/status diagnostics
python.exe scripts/filemaker_extract_refactored.py --info-only --json
python.exe scripts/filemaker_extract_refactored.py --migration-status --json
```

See `CLAUDE.md`'s "How to run" for the full flag reference and gotchas (WSL-vs-`python.exe`, pooler port, DSN registry location, etc).

---

## Current state & known issues

**The pipeline is stable, fast, idempotent, and delta-sync-capable, and the GUI is reliable on top of it.** `oci` (self-hosted) is the live, fully-loaded default target; the original cloud project's pooler is unreachable and kept only for reference. Both stages run in minutes, not hours. Re-running Stage 2 never accumulates duplicate rows, and a real `ON CONFLICT DO UPDATE` means a later sync can actually correct a previously-loaded row, not just skip it.

**Open threads, current as of the last session** (see `CLAUDE.md`'s "Current focus" for the live list — this section will drift faster than that one):

- Migration Overview's widget rendering not visually confirmed in the real GUI (correctness rests on code review + live JSON validation)
- `picture_metadata` untested against real images (none present in any dev session so far)
- The 16 known duplicate/unkeyed `image_no` records are a persisted hand-off (`rat_migration.reject_log` + an exportable `.xlsx`) awaiting a RAT volunteer with FileMaker access to fix them — not something this pipeline can resolve itself
- `PicaLocoBackend`/`picaloco` rebrand explicitly gated on stability — not started
- `picaloco_agent` (see below) builds and runs but real distribution is blocked on an unsigned-exe AV false positive — deferred, not urgent while it's still pre-distribution dev work
- The loader still mixes supabase-py and SQLAlchemy client calls in places; one path is dead and could be pruned

---

## Picking this up in Claude Code

Fast orientation for an agent or a returning human:

- **Read `CLAUDE.md` first, every session.** It is the maintained operational guide — architecture, golden rules, verified live facts, and the current-focus summary — and is kept accurate continuously, not just at milestones.
- **Then `devlog/worksheet.md`** for the full session-by-session history behind any non-obvious decision, including the exact reproduction of every real bug found and fixed.
- **Start with the loader.** `scripts/db_dml_loader.py` is where the real logic is — the `migrate_*` functions, FK resolution via `lookup_caches`, and the quarantine rules. Everything else feeds it.
- **Ground truth is the live DB**, captured in `rat_schema_original.sql`, not the hand-written DDL in `supabase/schema/` (which has known drift) — reconcile against the live schema before restructuring anything.
- **Source vocabularies and real data-entry layouts** (screenshots of the actual FileMaker forms) are in `FileMakerPro_source_details/` — useful when validating or extending sanitisation.
- **Validate loader/sanitisation changes against `test/` fixtures, never against the live DB.**
- Sequencing principle for this project: **stabilise & document → restructure → rebrand.** Don't rename `rat`→`picaloco` or start the `PicaLocoBackend` consolidation without explicit sign-off.

---

## Related repos

- [`picaloco_web`](https://picaloco-web.vercel.app) — **live**, public web search/browse tool for the archive (Vite/React/TS + Supabase), the actual client-facing product this backend feeds. Its `DEVOPS.md` has the disaster-recovery steps from that repo's own side.
- `picaloco_agent` — an installable background agent that wraps this pipeline for a remote, volunteer-operated FileMaker desktop (outbound-only, no inbound network exposure needed); active development, not yet distributed.
- [`trainpixelfolio`](https://github.com/blueairblob/trainpixelfolio) — an earlier, over-specced mobile app effort, superseded by `picaloco_web` for the actual client ask; not under active development.
- [`picaloco_rest`](https://github.com/blueairblob/picaloco_rest) — a Vercel-hosted Swagger UI mirror, not a custom backend; low priority, dormant.
- `PicaLocoBackend` — future consolidation target for this repo + `picaloco_rest`, once stable.
