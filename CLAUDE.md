# CLAUDE.md

Operational guide for working in this repo with Claude Code. Read this first each session. For human-facing setup and full detail, see `README.md`; for the running history and current open threads, see `devlog/worksheet.md`.

---

## What this project is (and why it's shaped this way)

The **Railway Archive Trust (RAT)** is a volunteer charity that collects railway photo archives and memorabilia for museums. Volunteers, on a best-endeavours basis, work through boxes of photographs and type each photo's details into a bespoke **FileMaker Pro** thick-client forms app. That app has no formal support, and its form design is opaque — it's hard to know exactly what the source *intends*.

**The brief:** export that data into **Supabase (PostgreSQL)**, and from there provide a way to browse it.

**The core difficulty — internalise this, it drives every design decision:** the source data is **error-prone** (hand-entered by volunteers) and the source schema is **partially opaque** (FileMaker forms hide their real structure). So we cannot fully trust the source. The whole migration is a *data rescue*, not a clean database sync. That is why the loader **quarantines** dubious rows instead of coercing them, why the **live target DB** is treated as ground truth over any hand-written DDL, and why the **archive id** is the one anchor we hold onto.

The anchor: **`image_no`** (the archive id, e.g. `arc00001`) is the natural key that ties a flat source row to its normalised target rows. When in doubt, key on `image_no`.

---

## Architecture in one glance

Two separate stages. Do not conflate them.

```
FileMaker Pro
  └─ Stage 1  EXTRACT   scripts/filemaker_extract.py (+ _refactored.py)
       emits flat DML  →  .sql files   OR   flat load into rat_migration (staging)
  └─ Stage 2  LOAD      scripts/db_dml_loader.py        ← THE CROWN JEWEL
       parse → quarantine → explode flat→relational → resolve FKs → upsert
       →  Supabase  rat.*  (~12 normalised tables)
```

- Stage 2 has two modes: `--mode dml_files` (reads Stage-1 files) and `--mode migration_schema` (reads the staging tables).
- The **GUI** (`gui/`) is a `subprocess` wrapper around the Stage-1 and Stage-2 CLIs. It contains **no migration logic** — don't look for business logic there, and don't add any; put logic in the engines and wire the GUI to call it.

---

## Golden rules (invariants — do not violate without explicit sign-off)

1. **Quarantine over silent coercion.** Bad/ambiguous rows go to a `.reject` file with a reason. Never "fix" questionable source data by guessing. If you find yourself adding a fallback that hides a data problem, stop — reject it instead. This is the project's most valuable behaviour.
2. **The live DB is ground truth.** `rat_schema_original.sql` is a pull of the real `rat` schema. The hand-written DDL in `supabase/schema/` has **known drift** — reconcile against the live pull, not the DDL. Before any schema restructuring, capture fresh: `supabase db dump --linked --schema rat`.
3. **`image_no` is the natural key.** Preserve it. `catalog.image_no` is `VARCHAR UNIQUE` on purpose. Surrogate PKs are UUIDs; joins back to source go through `image_no`.
4. **The loader is fragile and precious.** Its sanitisation rules were invented on the fly during the original migration and are the hardest thing to reconstruct. Treat every change to `db_dml_loader.py` with extra care and test it (see below) before running it against anything live.
5. **Never commit secrets.** A DB password leaked here once and had to be rotated. The anon JWT is deliberately parked (rotating it invalidates all keys at once — coordinate with the frontend first). Keep credentials out of tracked files; `config.toml` holds real creds locally and must not carry them into a commit.
6. **Sequence: stabilise & document → restructure → rebrand.** Don't rename `rat`→`picaloco` or reshuffle into `PicaLocoBackend` until the pipeline is stable. Don't reorganise before the live schema is captured and `git status` is clean per repo.

---

## Before you run the loader

`supabase/schema/fix_rat_constraints.sql` **must be applied to the live `rat` schema first.** It adds `UNIQUE (catalog_id)` on `catalog_metadata` (the loader's `catalog_metadata`/`usage` upserts now `ON CONFLICT (catalog_id)` and will error without it) and drops a duplicate FK. Run its dedupe pre-check before applying.

---

## How to work here

**Run scripts from the repo root.** Both engines and the loader read `config.toml` from the current working directory (`os.getcwd()`). Running from elsewhere silently uses the wrong (or no) config.

**Validate loader / sanitisation changes against fixtures, not the live DB.** `test/test.sql`, `test/test.sql.bad_data_examples`, and `test/test.sql.reject` exist precisely so you can exercise quarantine behaviour offline. Never iterate on the loader by repeatedly hitting Supabase.

**Quick checks:**
```bash
# syntax-check anything you touch
python -m py_compile scripts/db_dml_loader.py gui/gui_operations.py

# connection / dry test (Stage 1)
python scripts/filemaker_extract.py --info-only --debug
python scripts/filemaker_extract.py --db-exp --ddl --dml --max-rows 10   # small end-to-end

# Stage 2 (file route) — requires --mode and --export-path
python scripts/db_dml_loader.py --mode dml_files --export-path <dir> --user-id you --debug
```

**Environment:** Python venv named `py3` (`py3\Scripts\activate` on Windows, `source py3/bin/activate` on WSL/Linux); deps in `requirements.txt`.

**Windows + WSL2:** this repo is edited from both. `.gitattributes` (`* text=auto`) is committed to keep line endings sane. Patches generated on Linux are LF — if `git apply` fails on CRLF, use `git apply --3way`.

**Commits:** conventional style preferred (`feat:`, `fix:`, `docs:`, `chore:`). Keep whitespace/line-ending-only commits isolated from substantive changes.

---

## Gotchas that will waste your time

- **The GUI doesn't stream output** (`subprocess.run(capture_output=True)`), so a long loader run looks frozen. It isn't. (A `Popen` streaming version is a known open thread.)
- **`Load to Target` uses `dml_files` mode**, so it reads whatever `.sql` is on disk. The intended flow is **Export to Files → Load to Target**; running `--db-exp` (staging) then `Load to Target` would load stale files.
- **Five `batch_upsert` calls still conflict on the auto-UUID `id`** (`collection`, `photographer`, `builder`, `picture_metadata`, `catalog_builder`) → re-runs can duplicate rows. Fixing these is active work; `catalog_builder` needs a composite-uniqueness decision.
- **The loader mixes two DB clients** (supabase-py `.table(...)` and SQLAlchemy `.execute(...)`). One path is dead; confirm which is live before extending, and prune the other.
- **`rat_schema_original.sql` is a dashboard export, not `pg_dump`** — accurate to read, but not cleanly re-appliable (e.g. bare `tags ARRAY`). For a canonical artifact use `supabase db dump`.
- **`scripts/scripts.old/` and other `*.old` paths are cruft** scheduled for pruning — don't build on them.

---

## Where things live

| Need | Look at |
|---|---|
| The real migration logic | `scripts/db_dml_loader.py` |
| Stage-1 extract | `scripts/filemaker_extract.py`, `scripts/filemaker_extract_refactored.py`, `scripts/data_exporter.py` |
| GUI wiring | `gui/gui_operations.py` (dispatch), `gui/gui_widgets.py` (buttons), `gui/filemaker_gui.py` (bindings) |
| Ground-truth schema | `rat_schema_original.sql` |
| Constraint prereqs | `supabase/schema/fix_rat_constraints.sql` |
| Sanitisation fixtures | `test/` |
| Source field meanings / valid values | `FileMakerPro_source_details/` |
| Why decisions were made / open threads | `backend_scripts_rat_log.md`, `devlog/worksheet.md` |

---

## Current focus

The two-stage pipeline is recovered and now runs end-to-end from both CLI and GUI (the loader was recently wired into the GUI as **Load to Target**). Immediate next steps live in `devlog/worksheet.md` under *Open Threads* — at time of writing: apply the constraint SQL and re-run a test load, check whether `catalog_metadata`/`usage` ever populated, add GUI output streaming, and finish the idempotency fixes. Consolidation into `PicaLocoBackend` and the `rat`→`picaloco` rebrand come **after** the pipeline is stable.
