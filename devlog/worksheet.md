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

## Session 4 — 2026-08-30 — Adopt the real production loader; make the repo Claude-Code-ready

**Focus:** Recover and adopt the real production `db_dml_loader.py` (the committed one was a stale partial reconstruction), reconcile the constraints and secrets to match it, and verify the whole repo state against live GitHub so a fresh Claude Code session starts from truth.
**Status:** `completed` — loader adopted. **Increment 2 is now the next build.**

---

### Context

Auditing the committed loader against the live uuid schema showed ~half its migrate functions couldn't load it (`migrate_catalog` writes `image_no` into a uuid PK; `migrate_picture_metadata` never resolves `catalog_id`; `color_space` vs `colour_space`). The live data was clearly loaded by a different, correct loader — recovered from `Migration\scripts\db_dml_loader.py` (~1476 lines): schema-driven, in-memory lookup caches (no N+1), full sanitise/quarantine/reject pipeline, correct keys.

---

### Decisions

| Decision | Rationale | Alternatives Considered |
|---|---|---|
| Adopt the real loader as-is + re-apply `env_secrets`, replacing the committed patchwork | It's the code that actually produced the live data; correct by construction | Reconstruct the committed loader function-by-function (more work, error-prone) |
| **Leave `catalog_builder`'s truncate-reload untouched; defer the incremental conversion to Increment 2** | Discovered it achieves idempotency by `TRUNCATE` when `uniq_columns == ['id']`, with a downstream "tables after this must also be deleted" assumption — changing it is a load-strategy change, not a key swap | Swap `uniq_columns` to the 3-col key now (would silently drop the truncate and touch downstream ordering) |
| Drop `builder_name_key`; builder's real key is `code` | Triple-confirmed: config `ratbuilders = ['"Builder code"']`, schema `builder.code NOT NULL UNIQUE`, real loader conflicts on `code`; `name` is nullable | Keep the Session-3 `name` constraint (wrong) |

---

### Findings

- **Repo audit vs live GitHub is clean:** `env_secrets` in all four readers, both constraint SQL files, the idempotency conflict-key fixes, and a secret-free `config.toml` are all correctly committed.
- **Security scare, resolved:** `scripts/scripts.old/.env` contains a `SUPABASE_SERVICE_ROLE_KEY` in a public repo — but it decodes to `iss: supabase-demo`, the **public local-dev demo constant**, not the cloud key. Harmless; no rotation needed. (The parked cloud anon JWT remains a separate, deferred, RLS-gated item.)
- **`catalog_builder` truncate behaviour** (above) — the key discovery that reshaped the plan.
- **Housekeeping surfaced:** `.gitignore` is UTF-16 (PowerShell artifact; git tolerates it, `.env` is ignored — normalise to UTF-8); two diverged worksheets (consolidated); `requirements.txt` missing `python-dotenv` and carrying a redundant bare `psycopg2`; stray `*.patch` files tracked at root.

---

### Outcome

Adopted loader (real + `env_secrets`, ~1495 lines) delivered as the new `scripts/db_dml_loader.py`; `fix_rat_builder_key.sql` drops the wrong constraint; `requirements.txt` corrected; docs refreshed (`CLAUDE.md` reframed to "loader adopted", `HANDOFF_increment2.md` replaces the loader-adoption handoff). The loader-adoption thread is **complete** — the repo now carries the real production loader.

---

### Open Threads

- [ ] **Increment 2** — feed the sync engine's `new + changed` delta into the adopted loader; advance the manifest only on *verified* loads; add the row-hash backstop. **First sub-task: convert `catalog_builder` from truncate-reload to incremental natural-key upsert** on `(catalog_id, builder_id, builder_order)`, tracing the "tables after this must also be deleted" assumption first.
- [ ] `picture_metadata` idempotency (constraint already exists; flip its `batch_upsert` to `catalog_id` — clean).
- [ ] The 1 missing catalog row (141,243 vs manifest 141,244).
- [ ] The 16 flagged source records (13 duplicate `image_no`, 3 null keys) — FileMaker-side.
- [ ] Run `fix_rat_builder_key.sql` against live `rat`; confirm the adopted loader's deps (incl. Pillow) are in Windows Python.
- [ ] *(Carried)* DDR from the `.fmpur` copy; supabase-py vs SQLAlchemy prune; anon-JWT rotation when frontend ready; consolidation into `PicaLocoBackend` + rebrand once stable.

---

## Session 5 — 2026-09-01 — Bring up the OCI self-hosted target; restore full Stage 2; found the missing-row bug

**Focus:** Stand up a second Postgres target (self-hosted Supabase on an OCI host, reached over Tailscale) alongside the existing Supabase.com cloud project, without breaking the cloud path; get Stage 2 back to populating the whole `rat.*` schema instead of just `catalog_metadata`; do Increment 2's first sub-task (`catalog_builder`'s incremental key); and validate all of it end to end before touching real FileMaker data.
**Status:** `completed` — **the full ~141k-row archive is now live on the OCI target**, sync manifest baselined, `--preview` clean. What started as a config change turned into fixing thirteen distinct pre-existing bugs across `db_dml_loader.py` and `filemaker_extract.py` (the latter previously documented as "already correct").

---

### Context

The user is bringing up `huey.taila2eeb2.ts.net`, a self-hosted Supabase instance on Tailscale, as a new staging/production target. It started **fresh** — no `rat`/`rat_migration` schema at all. Getting a real run working against it exposed that the pipeline had only ever pointed at one target (host/user/port were single values in `config.toml`, not per-target), that `db_dml_loader.py`'s committed `main()` only actually ran `create_lookup_index_cache()` + `migrate_catalog_metadata()` — every other `migrate_*` call was commented out, so the live cloud data was never produced by a run of today's code as committed — and that `filemaker_extract.py`'s target-connection path had a latent variable-naming bug independent of any of this.

---

### Decisions

| Decision | Rationale | Alternatives Considered |
|---|---|---|
| **Config: named profiles under `[database.target.<name>]`**, `host`/`user`/`pwd`/`port`/`dbname` all per-profile; `active_profile` selector resolved CLI `--target-profile` > env `RAT_TARGET_PROFILE` > config default, via `resolve_secret()` reused as-is | User wants both targets usable, not a hard cutover; `resolve_secret`'s existing CLI>env>config precedence is exactly the right shape for a profile *name* too — no new mechanism needed | A `--target-profile`-only flag with no config default (breaks scripted/cron use); separate config files per target (more moving parts, drifts) |
| **New `resolve_target_pwd(profile, ...)` in `env_secrets.py`**: profile-scoped env var `RAT_TARGET_PWD_<PROFILE>`, with the bare legacy `RAT_TARGET_PWD` as a fallback **only** for the `supabase` profile | Existing `.env` must keep working unmodified; a new profile must never silently inherit another target's password | One shared `RAT_TARGET_PWD` for all profiles (the exact footgun this avoids) |
| Pooler port **5432 (session), not 6543 (transaction)**, for both profiles | The loader holds explicit multi-statement transactions per batch (`batch_upsert`'s commit/rollback, `PgManifest`'s `autocommit=False`); transaction pooling doesn't guarantee that session affinity. Matches the project's own existing guidance for the cloud target | 6543 (would risk silent session-state issues under load) |
| `PgManifest`'s hardcoded `sslmode="require"` → `"prefer"` | The self-hosted OCI Postgres doesn't terminate TLS (Tailscale already encrypts the transport) — `require` hard-fails the connection; `prefer` negotiates SSL where available (cloud target unaffected) and falls back where not | Making `sslmode` a new per-profile config key (more surface than needed for one hardcoded default) |
| Restore **all** `migrate_*` calls in `main()`, in the order already written | That order already respects the FK dependency chain (`location` before `route`/`builder`, `catalog` before the lookup-cache build); just needed uncommenting | Reordering (unnecessary — the original order was already correct, just disabled) |
| `catalog_builder`: `uniq_columns` **`['id']` → `['catalog_id','builder_id','builder_order']`**, truncate branch deleted entirely | Traced the "tables after this must also be deleted" assumption (Session 4's deferred question): nothing downstream reads from `catalog_builder` having been truncated (`usage`/`picture_metadata` key off `catalog`, not `catalog_builder`) — safe to convert | Keeping truncate as a dead branch (misleading; would never fire again) |
| Bootstrap DDL as **one new file**, `supabase/schema/bootstrap_rat_schema.sql`, with the three existing `fix_*.sql` corrections baked in inline, instead of applying the patches after a raw `rat_schema_original.sql` run | `rat_schema_original.sql` is explicitly "reference only, not meant to be run"; a fresh target should never need to reproduce-then-fix the same mistakes (`builder` name-key, duplicate `picture_metadata` FK) | Running `rat_schema_original.sql` as-is then layering the three fix files (extra steps, briefly re-introduces since-corrected mistakes) |

---

### Findings

- **`filemaker_extract.py`'s `db_type`/`dbt_type` split was a real, confirmed bug — but not the one it first looked like.** `get_args()` injects every CLI arg into module globals via `globals().update(vars(args))`, so `db_type` (from `--db-type`, default `'supabase'`) is genuinely available everywhere from the start of `__main__`. `dbt_type`, a *different* name, was only ever assigned inside the `db_exp`/`get_images` branches — so any code path using `dbt_type` (`format_value`, `df_to_sql_bulk_insert`, the dialect branches in `get_table_data`/`export_data`) would `NameError` on a **`--fn-exp`-only** run, the primary documented Stage-1 flow. First attempt at this fix went the wrong way (renamed the *working* `db_type` sites to `dbt_type`) — caught before committing anything, reverted, and fixed in the correct direction: dialect branches consolidated onto `db_type` (deleting the two stray `dbt_type = ...` assignments), and the separate credential/label lookups (`db[db_type]['user']` etc.) switched to the new `active_profile` instead, since a dialect ('mysql'/'supabase') and a target profile ('supabase'/'oci') are no longer the same thing now that two profiles share the 'supabase' dialect.
- **`read_data_from_migration_schema()` silently dropped the first row of every table it read.** A leftover debug line, `first_row = result.fetchone()`, consumed a row off the forward-only SQLAlchemy cursor before the real `for row in result:` loop ran — `first_row` itself was never used (its only consumer was a commented-out print). Confirmed on the live OCI target: a 5-row fixture insert came back as 4 via this path. **Confirmed root cause of the multi-session open item "catalog is 1 short of the manifest (141,243 vs 141,244)"**: after fixing it and running the full live load, `rat.catalog` landed at **exactly 141,244** rows — precisely the count the manifest baseline had always asserted but `catalog` never matched. Mystery closed.
- **`--mode dml_files`'s hand-rolled SQL parser (`parse_insert_statement`, via `pd.read_csv` over regex-preprocessed VALUES text) cannot parse a realistic FileMaker export.** Tested directly against `test/test.sql` (deliberately includes mixed quoting, `Timestamp('...')`-style values, embedded punctuation): 0 of 6 rows parsed, `pd.read_csv` raised `EOF inside string`. **Decision (user, mid-session): stop trying to fix this parser and use `--mode migration_schema` instead** — validated as the working path end-to-end (see Outcome). The `dml_files` parser is a known-broken code path for realistic data; flagged here rather than fixed, since fixing it properly means writing a real tokenizer, not patching `pd.read_csv` usage.
- `main()`'s existence-check gate had two bugs of its own, both fixed: `mig_tables` listed `'user'` but the actual table/query is `'users'` (mismatch — would always warn-and-continue past a real problem); and `if not (check_tables(tgt) or check_tables(mig)):` used `or`, so it only aborted if **both** checks failed rather than either.
- `sync_config.json` (repo root) carries a real plaintext Supabase password — but it's untracked and already `.gitignore`d, and unreferenced by any code (confirmed via repo-wide grep). Not a leak, just dead. Safe to delete whenever, not urgent.
- Confirmed (Plan-agent research + code trace) this pipeline never uses the Supabase REST/client API, only raw `postgresql://` via SQLAlchemy/psycopg2 — the OCI host's `SUPABASE_URL`/anon/service_role keys the user provided aren't needed anywhere in this repo.

**Second half of the session: WSL had `python.exe` interop enabled**, so Stage 1 could run directly against the live FileMaker file from this session (no separate Windows hands-on-keyboard step needed) — connected via the existing `rat` DSN, confirmed 141,262 live `ratcatalogue` rows. `requirements.txt`'s `pandas==2.1.4` pin has no Python 3.13 wheel (that Windows install is 3.13) and fails building from source (`_PyLong_AsByteArray` C-API mismatch) — installed unpinned versions instead; worth loosening the pin properly later. Running Stage 1 for real surfaced **six more confirmed, pre-existing bugs in `filemaker_extract.py`**, none related to the profile work, all fixed:
- `config.toml` was resolved relative to the *script's own directory* (`Path(__file__).parent`), not the working directory — contradicted `CLAUDE.md`'s documented "run from repo root" behaviour and `db_dml_loader.py`'s own `os.getcwd()` pattern. Fixed to match.
- `--max-rows` combined with `--ddl` built an invalid doubled `... FETCH FIRST 20 ROWS ONLY FETCH FIRST 100 ROWS ONLY` clause (the base query already got one FETCH clause for `--max-rows`; the DDL-sampling code unconditionally appended a second). Only fires when both flags are set together — plausibly never exercised before since prior runs used the `--max-rows` default (`'all'`).
- For `ratcatalogue` specifically, `ORDER BY image_no ASC` was appended *after* an existing `FETCH FIRST` clause — invalid clause order (FETCH FIRST must follow ORDER BY). Same "only fires with `--max-rows` set" shape as above.
- `get_table_data()` assumed a per-table duplicate-error dict (`ins_err[tab]`) was always populated — true only when a database target is involved; a `--fn-exp`-only run left it empty, `KeyError` on any table with zero duplicates (i.e. every table, every time).
- The same `ins_err` dict's own init logic (`try: ins_err[name] == {} / except: ...`) used `==` instead of an assignment — a no-op that never actually set the key, and its "recovery" branch replaced the *entire* global dict rather than adding to it, discarding any other table's accumulated errors.
- `create_pk()` and the DML insert loop both unconditionally required `dbt[mig_schema]['pk'][name]` — a config structure (`[database.target.<schema>.pk.<table>]`) that has never existed anywhere in the repo, for an explicit-named-PK feature nothing downstream needs (`rat_migration` staging tables are flat; `db_dml_loader.py` just `SELECT *`s them). This crashed (`sys.exit()`) the whole `--db-exp` run on the very first table. Both now skip gracefully (no PK / plain insert) when that config isn't present, rather than requiring config that was never real.

Every one of these six was confirmed by live reproduction against the real FileMaker file (isolated repro queries, full tracebacks via the script's own `--debug` path), not guessed.

---

### Outcome

Delivered and **verified live** against the fresh OCI target (`huey.taila2eeb2.ts.net`, profile `oci`):
- `config.toml` restructured to named target profiles (`supabase` = existing cloud project, unchanged values; `oci` = new); `[database.target].user` renamed to `default_migration_user` (it was never a connection user — it's the audit-username fallback, and collided in meaning with the per-profile `user`).
- `scripts/env_secrets.py`: new `resolve_target_pwd()`.
- `scripts/db_dml_loader.py`, `scripts/db_sync_manifest.py`, `scripts/filemaker_extract.py`: `--target-profile` flag + `resolve_active_profile()` in each; all target-connection code paths updated to read the active profile's sub-table. Plus: the `read_data_from_migration_schema` off-by-one fix, the `mig_tables`/gate-logic fixes, full `main()` population restored (all `migrate_*` uncommented, dependency order unchanged), `catalog_builder`'s incremental key, `PgManifest`'s `sslmode` fix.
- `supabase/schema/bootstrap_rat_schema.sql` (new): one idempotent script, all 12 `rat.*` tables + `rat_migration.users`/`migration_log` (the latter two have no discoverable DDL anywhere in the repo — reconstructed from code usage, flagged as such, not a live capture). **Applied to the OCI target and verified**: all tables present, constraint names match exactly what `fix_rat_constraints.sql`/`fix_rat_idempotency.sql` expect (confirms Postgres's default auto-naming), `builder_name_key` correctly absent.
- End-to-end proof on the live OCI target, via `test/test.sql` loaded into a real (throwaway) `rat_migration.ratcatalogue` staging table and run through `--mode migration_schema --target-profile oci`: every `rat.*` table populated correctly in one pass (`country`, `organisation`, `location` — including the auto-add-missing-location fallback path — `catalog`, `catalog_metadata`, `catalog_builder` at its new 3-column key, `usage`), and a second identical run produced **zero duplicates** on every table except `catalog_builder`, whose 5,927-legitimate-pairs key correctly does *not* fire `ON CONFLICT` when `builder_id IS NULL` (standard SQL — NULL is never "equal" to NULL for uniqueness) — a fixture artifact (no real builder data in this minimal test), not a code defect; real data always resolves `builder_id` to a real UUID or explicitly skips the row. Regression-checked: `--target-profile supabase` (or no flag) still resolves to the unchanged cloud target throughout.
- Test artifacts cleaned up afterward: `rat.*` truncated back to empty, throwaway staging tables dropped, `rat_migration.sync_manifest`/`users` reset — the OCI target is back to a clean, bootstrapped, empty state, ready for a real load.

**Then the real thing, live, twice** (a 20-row `--max-rows` smoke test, then the full archive), both via `python.exe` interop from this same WSL session — no separate Windows hands-on-keyboard step ended up being needed:

- Smoke test (20 rows): `filemaker_extract.py --db-exp --ddl --dml --max-rows 20 --target-profile oci` → `db_dml_loader.py --mode migration_schema --target-profile oci`. Surfaced and fixed the six `filemaker_extract.py` bugs above. Correctly quarantined one row with `NULL image_no` (logged, skipped) rather than crashing — the project's core "quarantine, don't coerce" behaviour working against real data for the first time this session.
- **Full load** (no `--max-rows`): Stage 1 took ~2 hours (`ratcatalogue`'s 141,262 rows dominate — per-row insert overhead in `export_data`'s staging-table path, ~7,064s of the total). Stage 2 took under 6 minutes end to end. Final `rat.*` counts on the OCI target: `country` 117, `location` 14178, `route` 2874, `organisation` 1519, `collection` 66, `photographer` 270, `builder` 517, **`catalog` 141244**, `catalog_metadata` 141247 (3 with `catalog_id IS NULL` — the rows that failed `catalog`'s `NOT NULL image_no`), `catalog_builder` 116542, `usage` 141244, `picture_metadata` 0 (no local image files in this environment). All either match or slightly exceed the historical cloud-target baseline — consistent with organic growth, and `catalog`/`usage` landing exactly at 141,244 (not 141,243) directly confirms the `read_data_from_migration_schema` off-by-one fix above.
- **Manifest baselined**: `db_sync_manifest.py --baseline --yes --target-profile oci` → 141,244 keyed rows baselined, 3 skipped as unkeyed, 13 duplicate `image_no` — then `--preview --target-profile oci` came back **completely clean** (0 new, 0 changed, 141,231 unchanged) and flagged the *exact same* known data-quality debris `CLAUDE.md` already documents (same 13 duplicate `image_no` values, same 3 unkeyed ROWIDs: 42279, 47343, 145873). Syncing against the OCI target is now real, working, and internally consistent with everything previously known about this source data.

---

### Open Threads

- [ ] **`--mode dml_files`'s parser is confirmed broken for realistic data** — needs a real rewrite (proper tokenizer, not `pd.read_csv`) if this mode is ever needed again; `--mode migration_schema` is the path to use meanwhile.
- [ ] `scripts/config_manager.py`, `scripts/database_connections.py`, and their `gui/` duplicates still read `[database.target]` the old (pre-profile) way — deliberately deferred (GUI-only path, out of scope this session), will `KeyError` if exercised against the new config shape.
- [ ] Increment 2 sub-tasks 2–4 (drive the loader from the sync-manifest delta, advance the manifest only on verified loads, row-hash backstop against FileMaker import-inflation) — still ahead, unchanged from Session 4. The manifest and full data are now both live on the OCI target, so this is unblocked.
- [ ] `picture_metadata` untested against real images this session (needs local image files, none present in the WSL environment) — 0 entries migrated, expected given no local files, but the code path itself wasn't exercised against real photos.
- [ ] `requirements.txt`'s `pandas==2.1.4` pin has no Python 3.13 wheel and fails building from source — worth loosening the pin (this session installed unpinned `pandas`/`sqlalchemy`/`tomli`/`pillow`/`tqdm` into the Windows Python instead).
- [x] ~~`ratcatalogue`'s Stage-1 export dominates runtime~~ — **fixed Session 6**, see below.
- [ ] *(Carried)* the 16 flagged source records (13 duplicate `image_no`, 3 null keys) — now precisely re-confirmed via the OCI manifest baseline, still FileMaker-side cleanup; DDR from the `.fmpur` copy; supabase-py vs SQLAlchemy prune; anon-JWT rotation when frontend ready; consolidation into `PicaLocoBackend` + rebrand once stable; delete the dead `sync_config.json`.

---

## Session 6 — 2026-09-01 — Fix the loader's and extract's row-by-row performance bottlenecks

**Focus:** Session 5's full live load worked but was slow — Stage 1 took ~2 hours for 141k rows, and Stage 2's `migrate_route` alone took ~35 minutes for 2,874 rows. Find and fix the root cause in both stages, verify against real data, measure the actual improvement.
**Status:** `completed`. Also surfaced (and cleaned up) a real, pre-existing data-integrity bug that testing this fix exposed at scale.

---

### Context

Both bottlenecks traced to the same pattern: one network round trip per row where the codebase already
had (or nearly had) a batched/cached alternative, discovered via `grep`/direct code trace, not guesswork
— **Stage 2** (`db_dml_loader.py`): the project's own `lookup_caches` in-memory cache (built once via
`create_lookup_index_cache()`, used correctly by `migrate_catalog_metadata`/`migrate_catalog_builder` at
500–2,500+ rows/sec) was built too late — *after* `migrate_catalog()` — for `migrate_route`,
`migrate_builder`, `migrate_organisation`, and `migrate_location`, which instead called
`get_location_id()`/`get_country_id()`: always a live `SELECT`, unindexed until the same late point.
**Stage 1** (`filemaker_extract.py`, `export_data()`): `batch_insert = False` was hardcoded (next to a
literal `#do check integrity instead?` comment) — every row got its own `INSERT`+`commit()` despite
`df_to_sql_bulk_insert()` already building one multi-row statement per chunk. A dead `else:` branch did
attempt a real bulk insert, but with no per-row fallback — a single conflicting row in a 100-row chunk
would have rolled back all 100, which is presumably why it was never turned on.

---

### Decisions

| Decision | Rationale | Alternatives Considered |
|---|---|---|
| Build `country`/`location` caches early (right after their own `migrate_*`), keep the existing full rebuild after `migrate_catalog()` | `cache_lookup_table()` updates one key in `lookup_caches` rather than replacing the dict, so early partial builds survive; the later full rebuild is cheap (one bulk SELECT/table) and picks up anything `add_location()` inserted in between | Building every cache eagerly at the very start (impossible — location/builder caches need their own tables populated first, which is circular) |
| `get_location_id()`/`get_country_id()`: cache-first fast path, falling through to the *unchanged* live-SELECT + auto-add path on a miss; update the cache in-memory when `add_location()` inserts a new row | Zero behaviour change on a miss — purely additive; the in-memory update means a second reference to the same new name later in the same run also hits the fast path | Making the cache the *only* path (would silently break the auto-add-missing-location quarantine behaviour on any staleness) |
| Stage 1: try the whole chunk as one bulk statement, catch `IntegrityError`, fall back to the *existing* per-row loop for just that chunk | Mirrors `db_dml_loader.py`'s own proven `batch_upsert()` pattern; a single multi-row `INSERT` is atomic in Postgres, so a rolled-back bulk attempt leaves nothing to clean up before the per-row retry | Reusing the dead `else:` branch as-is (no fallback, would lose a whole chunk on one conflict) |
| Bulk path keyed off "does this chunk's own text already start with `INSERT INTO`" (same check the per-row loop already uses), not the `mode` variable | Caught live: `mode` is only ever assigned inside the *file*-export branch — a `--db-exp`-only run (no `--fn-exp`) hit `UnboundLocalError` the first time this path actually ran, since `mode` was referenced but never assigned in that call path | Guarding with `getattr`/try-except around `mode` (papers over the same fragility instead of removing it) |

---

### Findings

- **Confirmed by direct code trace, not guesswork**: `get_builder_id()` is dead code (nothing calls it — `migrate_builder` resolves its own `location`, not a builder lookup; `catalog_builder`'s builder resolution already goes through `lookup_caches['builder']` directly). Left untouched, out of scope.
- **A live reproduction caught a bug in my own first version of the Stage-1 fix**: mirroring the dead bulk branch's `if mode == 'a': i = insert_header` line inherited its latent flaw — `mode` is local to `export_data()` as a whole (Python's whole-function static scoping) but only ever *assigned* inside the file-export branch, so a `--db-exp`-only run left it unbound. Fixed by using the same "does the text already start with INSERT INTO" check the per-row loop already relies on.
- **A real, pre-existing data-integrity bug, exposed at scale by testing this fix**: `rat.catalog_builder`'s natural key is `(catalog_id, builder_id, builder_order)`, but SQL treats `NULL ≠ NULL` for uniqueness — so every row where `builder_id` couldn't be resolved (no conflict target match) gets a fresh, never-deduplicated copy on every re-run. Re-running Stage 2 twice this session (to measure the fix) accumulated **58,326 duplicate rows** — 115,513 `NULL`-`builder_id` rows where only 57,187 distinct `(catalog_id, builder_order)` pairs should exist. Non-`NULL` `builder_id` rows were unaffected (verified zero duplicates there — the natural-key fix from Session 5 works correctly for resolvable data). Cleaned up via a `DISTINCT ON (catalog_id, builder_order)` delete, verified before committing (57,187 + 59,351 = 116,538, matching exactly). **Not fixed at the code level** — that needs a real design decision (partial index? sentinel value for unresolved `builder_id`?) and is out of scope for a performance pass; flagged as a new open thread. A much smaller version of the same issue showed up in `catalog_metadata` too (9 `NULL`-`catalog_id` rows vs. the original 3) — too small and keyless to safely dedupe the same way, documented rather than touched.

---

### Outcome

**Verified live, before/after, against the real FileMaker source and the `oci` target:**

| Step | Rows | Before | After | Speedup |
|---|---|---|---|---|
| Stage 1 `ratcatalogue` extract | 141,262 | ~7,064s (~2h) | 225.71s (3m46s) | **~31×** |
| Stage 2 `migrate_route` | 2,874–2,883 | ~2,100s (35m) | 1.8–2.1s | **~1,000×** |
| Stage 2 `migrate_location` | 11,947 | ~200s (3.3m) | 5.5s | **~36×** |
| Stage 2 `migrate_builder` | 517 | ~75s | 1.3–1.35s | **~57×** |
| Stage 2 whole run | 141k rows, 8 tables | ~46m | ~7.6m | **~6×** |

Isolated write-side tests against a scratch table (no FileMaker needed) proved both the clean-bulk path and
the conflict-triggers-fallback path before touching real data: a clean 20-row batch went through bulk with
zero fallback; a batch overlapping 16 already-existing ids correctly rolled back atomically and fell
through to per-row, inserting exactly the 4 genuinely-new rows and quarantining exactly the 16 duplicates
with individual `ins_err` records — no data loss either way.

Final `rat.*` state after the full re-run + `catalog_builder` cleanup: `country` 117, `location` 14178,
`route` 2874, `organisation` 1519, `collection` 66, `photographer` 270, `builder` 517, `catalog` 141244,
`catalog_metadata` 141253, `catalog_builder` 116538, `usage` 141244 — all stable/idempotent across the
performance-optimized code paths except the `catalog_builder`/`catalog_metadata` `NULL`-key duplication
noted above, which is pre-existing and now cleaned up (not code-fixed).

---

### Open Threads

- [x] ~~`catalog_builder`/`catalog_metadata`'s `NULL`-key duplication~~ — **fixed Session 7**, see below.
- [ ] *(Carried)* everything from Session 5's Open Threads not resolved above: `--mode dml_files` parser rewrite; `config_manager.py`/`database_connections.py`/GUI profile support; Increment 2 sub-tasks 2–4; `picture_metadata` untested against real images; `requirements.txt`'s `pandas==2.1.4` pin; the 16 flagged source records; DDR; supabase-py/SQLAlchemy prune; anon-JWT rotation; `PicaLocoBackend` consolidation/rebrand; delete `sync_config.json`.

---

## Session 7 — 2026-09-01 — Fix the catalog_builder/catalog_metadata NULL-key duplication bug

**Focus:** Session 6's performance testing exposed and accumulated a real bug: rows whose natural key includes an unresolved (`NULL`) lookup never conflict with themselves on re-run, since SQL treats `NULL ≠ NULL`. Fix it properly instead of just cleaning up the symptom again.
**Status:** `completed`. Fix proven idempotent by running Stage 2 twice in a row and confirming zero growth, then the accumulated legacy duplicates were cleaned up once, for good this time.

---

### Context

Two tables, two different root causes, two different fixes — treating them as one bug would have been wrong:

- **`catalog_builder`** (`builder_id` unresolved): the row still carries real, worth-keeping payload
  (`plant_code`/`works_number`/`year_built`) even without a matched builder — the existing skip-check
  (`if not any([builder_id, plant_code, works_number, year_built])`) already proves that's the intended
  design. `location`/`country` resolution already had a working answer to exactly this problem — fall back
  to a real `'unknown'` sentinel row instead of leaving the FK `NULL` — but `catalog_builder`'s
  `builder_id` resolution never had the equivalent, plausibly because it uses the fast in-memory cache
  lookup directly rather than the slower `get_builder_id()` (dead code, confirmed unused, but its own
  logic already anticipated an `'UNK'` fallback — the intended design just wasn't wired up).
- **`catalog_metadata`** (`catalog_id` unresolved): means this source row's own `catalog` insert already
  failed (e.g. `NULL image_no`) — there's no sentinel that makes sense here (a fake "unknown catalog" row
  would corrupt `catalog` itself, unlike builder where `'unknown'` is a real, meaningful entity). The row
  is genuinely unlinkable. Golden rule 1 (quarantine, don't silently keep broken data) points at skipping
  it, not inventing a workaround.

---

### Decisions

| Decision | Rationale | Alternatives Considered |
|---|---|---|
| `catalog_builder`: add a real sentinel `'UNK'` builder row (`ensure_unknown_builder()`, called once in `main()` before the full cache rebuild), fall back to its id when `builder_id` doesn't resolve **but the row has other real payload** | Reuses the exact convention already shipped for `location`/`country`; makes `builder_id` always a real, stable, non-`NULL` value, so the existing `(catalog_id, builder_id, builder_order)` constraint works as designed — no schema change needed | A `COALESCE`-based expression unique index (works, but needs the conflict target expressed as a matching expression, doesn't fit `batch_upsert()`'s simple `index_elements=uniq_columns` API without rework, and doesn't match any existing pattern in this codebase) |
| `catalog_builder`: the skip-check (`any([builder_id, ...])`) must run **before** the sentinel substitution, using the real pre-substitution value | Substituting first would make `builder_id` always truthy (the sentinel's id), silently keeping every row regardless of whether it has any real payload — a behaviour change nobody asked for | — |
| `catalog_metadata`: skip inserting when `catalog_id` is unresolved, rather than inventing a sentinel | No sentinel is semantically safe for the primary spine table; the row is unlinkable either way — skipping is honest about that instead of leaving synthetic-looking orphan rows | A `COALESCE` index limiting orphans to exactly one total (arbitrary — why keep one and silently drop the rest?) |

---

### Findings

- Verified precisely before touching anything: 115,513 of the pre-fix `catalog_builder` rows had `builder_id IS NULL`, collapsing to only 57,187 distinct `(catalog_id, builder_order)` pairs — **58,326 duplicates**, all from Session 6's repeated test re-runs. Zero duplicates existed among the resolved (`NOT NULL`) rows, confirming the `(catalog_id, builder_id, builder_order)` key itself works correctly — only the `NULL` case was broken.
- First attempt at `ensure_unknown_builder()` failed live (`NotNullViolation` on `builder.created_by`) — hand-rolling a `pg_insert(...).on_conflict_do_nothing(...)` skipped the audit columns `batch_upsert()` normally adds automatically. Fixed by just calling `batch_upsert()` itself instead of reinventing it.
- Proved forward idempotency directly, not by inference: ran the full Stage 2 load twice back-to-back after the fix, confirmed `catalog_builder`'s total row count and its `UNK`-builder-id row count were bit-for-bit identical both times (173,725 → 173,725, both runs; the growth on the *first* post-fix run was 100% legacy `NULL` rows colliding with nothing — those are a different value on the conflict target than the new `UNK` rows, so of course they coexisted rather than deduping. Expected, not a fix failure).
- Before deleting the legacy `NULL` rows, verified every single one had an exact-payload-match `UNK` row already covering it (`catalog_id`, `builder_order`, `plant_code`, `works_number`, `year_built` all `IS NOT DISTINCT FROM` matching) — zero were orphaned/unmatched, so nothing was lost in the cleanup.

---

### Outcome

Code changes (`scripts/db_dml_loader.py`): new `ensure_unknown_builder()`, called once in `main()` right
after `migrate_builder()`; `migrate_catalog_builder()`'s per-row loop now skips outright when `catalog_id`
is unresolved (same reasoning as `catalog_metadata`) and falls back to the `'UNK'` sentinel for
`builder_id` only after the existing skip-check has already decided the row is worth keeping;
`migrate_catalog_metadata()` now skips (with a per-run count logged) rows whose `catalog_id` never
resolved instead of inserting an orphan.

Live on `oci`, after the code fix, the idempotency proof, and cleaning up both tables' accumulated legacy
duplicates: `catalog` 141,244, `catalog_metadata` 141,244 (was 141,253 with 9 `NULL`-`catalog_id` orphans —
those 9 were pre-existing from before this fix and are now gone for good), `catalog_builder` 116,538 (was
up to 174,864 mid-testing; zero `NULL` `builder_id` rows remain, all real `UNK`-sentinel rows verified
duplicate-free), `usage` 141,244. `builder` is 518 (517 real + the new `'UNK'` sentinel).

---

### Open Threads

- [ ] *(Carried, unchanged)* `--mode dml_files` parser rewrite; `config_manager.py`/`database_connections.py`/GUI profile support; `picture_metadata` untested against real images; `requirements.txt`'s `pandas==2.1.4` pin; the 16 flagged source records; DDR; supabase-py/SQLAlchemy prune; anon-JWT rotation; `PicaLocoBackend` consolidation/rebrand; delete `sync_config.json`.

---

## Session 8 — 2026-09-01 — Increment 2 sub-tasks 2–4: delta-driven incremental sync, live-proven

**Focus:** Wire `db_sync_manifest.py`'s already-working change-detection engine into an actual periodic sync — extract and load only `new`/`changed` rows instead of a full re-extract, advance the manifest only for loads verified against `rat.catalog` itself (never from the scan/diff step), and add a row-hash backstop against FileMaker import-inflation (ROWMODID bumped, content unchanged).
**Status:** `completed`. Proven against a live, hand-edited FileMaker record on `oci`: exactly one `image_no` detected, extracted, loaded, verified, and its manifest entry advanced — everything else (140,243 other rows) untouched. Two real bugs found and fixed along the way (see Findings).

---

### Context

Sub-task 1 (`catalog_builder` incremental key, Session 5) and sub-task 5 (`picture_metadata` idempotency,
already fine) were done. This session closed sub-tasks 2–4 — the actual point of Increment 2 — now
buildable because Sessions 5–7 made the `oci` target fully loaded, fast, and genuinely idempotent on
re-run. Three code changes, one new orchestrator:

- **Schema**: `rat_migration.sync_manifest` gained a `row_hash text` column (guarded `ALTER ... ADD COLUMN
  IF NOT EXISTS`, applied live via the existing `--init` path).
- **`filemaker_extract.py`**: new `--image-nos-file <path>` flag — when set, `get_table_data_set()` scopes
  `ratcatalogue` to exactly those `image_no`s via a parameterised `WHERE image_no IN (?,?,...)` (pyodbc
  positional params, not string interpolation), instead of the full-table extract. Every other table keeps
  its normal full extract — they're small and cheap even every run, and aren't `image_no`-scoped data.
  `--max-rows` is ignored (with a warning) when combined, since the delta list *is* the row selection.
- **`db_sync_manifest.py`**: new `row_hash(record) -> str` (`sha256` over `json.dumps(record,
  sort_keys=True, default=str)`) and `PgManifest.mark_loaded(by_image)` — an upsert that advances
  `fm_rowmodid`/`row_hash`/`load_status='loaded'` for exactly the `image_no`s passed in. `read_all()` now
  also returns the stored `row_hash` so a caller can compare.
- **New `scripts/run_incremental_sync.py`**: the orchestrator. Imports `db_sync_manifest`'s scan/diff/
  `PgManifest` directly (already clean, side-effect-scoped functions); invokes `filemaker_extract.py` and
  `db_dml_loader.py` as **subprocesses**, matching `gui/gui_operations.py`'s existing pattern — both rely
  on `globals().update(vars(args))` and dozens of implicit globals set up through their own `__main__`
  blocks, confirmed fragile when hand-unit-testing one function in isolation this session; subprocess
  isolation sidesteps that entirely. Flow: scan+diff (`--dry-run` stops here) → extract the delta +
  refresh the 4 small reference tables in full → load via the loader's unmodified `--mode migration_schema`
  entry point → verify by querying `rat.catalog` directly for the attempted `image_no`s (DB truth, not the
  loader's exit code) → for each verified row, compare its freshly-extracted `row_hash` against the
  manifest's stored one (a match means ROWMODID moved but content didn't — still safe to have upserted,
  but reported separately, not counted as a real change) → `mark_loaded()` for exactly the verified set.

---

### Findings — two real bugs, both pre-existing, both surfaced by finally exercising this code path live

1. **`filemaker_extract.py`'s `--ddl`/`--dml` flags default to `False`.** `run_incremental_sync.py`'s two
   `filemaker_extract.py` subprocess calls passed `--db-exp --tables-to-export ... --del-data` but never
   `--ddl --dml` — so `get_table_data()`'s action loop only ever ran the row-count action, silently
   skipping the actual DDL-fetch/DML-fetch-and-insert steps entirely. No error, no exception — the log just
   quietly jumped from "Has N rows" straight to "Finished". Both delta and reference-table extracts left
   their `rat_migration.*` staging tables **dropped and never refilled** (`--del-data` did drop them, since
   that step doesn't depend on the missing flags). Caught only because the next stage, `db_dml_loader.py`,
   crashed on `catalog_df['country']` — `pd.DataFrame([])` from an empty read has no columns at all. Fixed
   by adding `--ddl --dml` to both subprocess calls. **`rat.catalog` and the rest of the production `rat`
   schema were never touched** — the crash happened at the very first line of `main()`'s migration order,
   before any write — confirmed live before doing anything further (`rat.catalog` held steady at 141,244
   throughout).
2. **`migrate_builder()`'s NaN-vs-None check silently let bad data through the intended safety net.**
   `if row['Location'] != None:` was meant to route builders with a blank `Location` to the `'unknown'`
   sentinel (the `else` branch beneath it already existed for exactly this) — but pandas represents a blank
   source cell as a float `NaN`, and `NaN != None` evaluates `True` in plain Python (NaN compares unequal
   to everything, including `None`), so the check let it through to `get_location_id()` → `stripy()`, which
   crashed on `NaN.strip()`. Fixed the one call site with `pd.notna(row['Location'])`, **and** hardened
   `stripy()` itself (`isinstance(txt, str)` guard, returns `None` for anything else including `NaN`) since
   it's reused across a dozen call sites reading raw DataFrame cells, several of which (`organisation`,
   `route`, `collection`, `photographer`, `plant_code`, `works_number`, `year_built`) are exactly as exposed
   to the same NaN-from-blank-cell pattern — a one-line root-cause fix instead of hunting each call site
   down by crash.

Both bugs are latent in code paths that simply hadn't been exercised this way before: `run_incremental_sync.py`
is new, so nothing had called `filemaker_extract.py` without explicit `--ddl --dml` before; and this was the
first `migrate_builder()` run against a builder whose `Location` cell happened to be genuinely blank in the
live extract.

---

### Verification (in order run)

1. **Fixture test** — a disposable scratch-rows test (`__synctest_a`/`__synctest_b` image_nos, cleaned up in
   a `finally`) against the *live* `oci` manifest table, proving `row_hash()` correctly tells a real content
   change apart from a ROWMODID-only bump, and `mark_loaded()` upserts both correctly. PASS, zero leftover
   rows confirmed after.
2. **Live `--dry-run` against `oci`, no source changes**: `new: 0, changed: 0`, clean exit, no manifest
   writes.
3. **Live hand-edit test**: asked the user to edit one real FileMaker record. `--dry-run` correctly showed
   `changed: 1`. Real run hit the two bugs above; after fixing both, re-ran clean: `verified: 1, false
   positives: 0, real changes: 1`, manifest advanced for exactly 1 row. `rat.catalog` count unchanged at
   141,244 (in-place update, not a duplicate) confirmed by direct query. Follow-up `--dry-run` came back to
   `new: 0, changed: 0` — settled.

---

### Outcome

All four plan phases are code-complete and live-verified. `scripts/run_incremental_sync.py` is a real,
runnable "sync now" — cron/Task Scheduler wiring remains deliberately out of scope (this made the sync
itself correct and runnable, not automatic). Increment 2 is now fully done (sub-tasks 1 and 5 were already
closed; 2–4 close this session).

---

### Open Threads

- [ ] *(Carried, unchanged)* `--mode dml_files` parser rewrite; `config_manager.py`/`database_connections.py`/GUI profile support; `picture_metadata` untested against real images; `requirements.txt`'s `pandas==2.1.4` pin; the 16 flagged source records; DDR; supabase-py/SQLAlchemy prune; anon-JWT rotation; `PicaLocoBackend` consolidation/rebrand; delete `sync_config.json`; cron/Task Scheduler wiring for `run_incremental_sync.py` (deliberately out of scope this session).

---

## Session 9 — 2026-09-02 — Wire the delta sync into the GUI; fix a long-broken script-path bug

**Focus:** Add a GUI button for Session 8's delta-driven sync. Investigating how to do that safely surfaced a
bigger, pre-existing problem: the GUI's subprocess dispatch has been broken since before this worksheet
started.
**Status:** `completed`, scoped deliberately narrow per the user's explicit choice (see Decisions) — smoke
tested live via `python.exe`, not just compiled.

---

### Context

`gui/gui_operations.py`'s `run_python_command()` resolves the script it's about to run as a **bare
filename** (e.g. `'db_dml_loader.py'`) relative to whatever directory the GUI process happens to be
launched from — not a path into `scripts/`, where every pipeline script actually lives. Git history pins
the cause exactly: the commit that wired `db_dml_loader.py` into the GUI (`0d3c812`) predates the `scripts/`
subdirectory existing at all — a later commit (`7281663 Recover migration files`) moved everything into
`scripts/` without updating the GUI's assumptions. Net effect: **every existing GUI operation** (Full Sync,
the old "Incremental Sync", Load to Target, Export Files/Images, Test Connections, Migration Status) has
been silently broken — `"{script} not found"` — for as long as `scripts/` has existed, with nothing in the
GUI's own code or any launcher setting `cwd` to make the bare filename resolve. Separately (found, not
fixed — see Decisions): `gui/filemaker_extract_refactored.py` is a stale June-2025 fork of the extract
script, missing every Session 5–8 fix, and is itself a duplicate of `scripts/filemaker_extract_refactored.py`.

---

### Decisions

| Decision | Rationale | Alternatives Considered |
|---|---|---|
| Fix `run_python_command()`'s path resolution (`Path.cwd() / 'scripts' / script`) as part of this change, rather than working around it for just the new button | The new button needs correct resolution anyway; the fix is small, mechanical, and repairs every other GUI operation for free | Route only the new operation around the bug locally (leaves the other seven operations broken, doesn't fix the actual defect) |
| Leave `gui/filemaker_extract_refactored.py` (the stale duplicate extract fork) untouched | Explicitly out of scope — user chose the narrower option when asked; retiring it or repointing the other GUI operations at the real `scripts/filemaker_extract.py` is a bigger, separate call | Full cleanup: also repoint `full_sync`/`export_files`/etc. at `scripts/filemaker_extract.py` (offered, user declined for this pass) |
| New button labelled **"Delta Sync"**, a distinct operation key (`delta_sync`) from the pre-existing **"Incremental Sync"** button | The existing "Incremental Sync" button (`operation_commands['incremental_sync']`) is just a plain full-table extract without DDL regen — not delta-driven at all, despite the name. Reusing the name or the key would have silently changed what an existing button does | Renaming/repurposing the old "Incremental Sync" button (bigger behavioural change than asked for; leaves users' muscle memory pointing at something different) |

---

### Findings

- Confirmed live (`python.exe`, headless smoke test calling `run_python_command()` directly with the same
  args a button click sends): the path fix correctly resolves and launches `run_incremental_sync.py` from
  `scripts/`, with `cwd` still the repo root so `config.toml`/`.env` load exactly as they do outside the
  GUI. First attempt legitimately failed — not a wiring bug — because `run_incremental_sync.py` defaults to
  `config.toml`'s `active_profile = "supabase"` (the old cloud project), which is no longer reachable
  (`psycopg2.OperationalError: ... tenant/user postgres.kmoehqdowgdupzdxtbei not found`, i.e. that project's
  pooler doesn't recognise the tenant anymore — likely paused/rotated, unrelated to anything in this repo).
  Passing `--target-profile oci` (exactly as every live test since Session 5 has) succeeded: `new: 0,
  changed: 0, Nothing to do.` — correct, since nothing had changed on `oci` since Session 8's test edit.
- The GUI still has **no target-profile picker at all** (`grep` across `gui/*.py` for
  `target_profile`/`active_profile` returns nothing) — every GUI operation, including the new button, runs
  against whatever `config.toml`'s `active_profile` says (currently `supabase`, the now-unreachable one).
  This was already a known open thread (`config_manager.py`/`database_connections.py`/GUI profile support)
  and is unchanged by this session — flagging again since it now also blocks the new button's default
  usability, not just the pre-existing ones.

---

### Outcome

`gui/gui_operations.py`: `run_python_command()` resolves scripts under `scripts/` instead of a bare
filename; new `operation_commands['delta_sync'] = []`; `operation_scripts` dict maps `delta_sync` →
`run_incremental_sync.py` and `load_to_target` → `db_dml_loader.py` (replacing the old two-way ternary,
same behaviour, easier to extend); 10-minute timeout for `delta_sync`. `gui/gui_widgets.py`: new "Delta
Sync" button (`both_required` connection gate). `gui/filemaker_gui.py`: button wired to
`safe_run_operation('delta_sync')`. `py_compile`/pyright clean on both platforms (the 2 remaining pyright
errors in `gui/` are pre-existing, confirmed via `git stash` diff, unrelated to this change).

---

### Open Threads

- [ ] *(Carried, unchanged)* `--mode dml_files` parser rewrite; GUI target-profile picker (now blocks the
  new Delta Sync button's default usability too, not just the older operations — config.toml's
  `active_profile` currently points at the unreachable `supabase` project); `gui/filemaker_extract_refactored.py`
  stale-fork cleanup (found this session, deliberately left untouched); `picture_metadata` untested against
  real images; `requirements.txt`'s `pandas==2.1.4` pin; the 16 flagged source records; DDR;
  supabase-py/SQLAlchemy prune; anon-JWT rotation; `PicaLocoBackend` consolidation/rebrand; delete
  `sync_config.json`; cron/Task Scheduler wiring for `run_incremental_sync.py`.

---

## Session 10 — 2026-09-02 — Flip `config.toml`'s default target profile to `oci`

**Focus:** Quick fix for the usability gap Session 9 surfaced — the GUI (and any script run without
`--target-profile`) defaulted to the now-unreachable `supabase` cloud project. Flip the default instead of
building a full profile picker.
**Status:** `completed`. One-line config change, verified live.

---

### Context

`config.toml`'s `[database.target].active_profile` was still `"supabase"` — the original cloud project,
whose pooler no longer resolves the tenant (`FATAL: tenant/user postgres.kmoehqdowgdupzdxtbei not found`,
confirmed live in Session 9). `oci` has been the actively-loaded, live-tested target since Session 5, but
every script and every GUI operation that doesn't pass `--target-profile` explicitly was still silently
defaulting to the dead one.

### Outcome

`config.toml`: `active_profile = "oci"`. Verified live: `python.exe scripts/run_incremental_sync.py
--dry-run` (no `--target-profile`) now correctly connects to `postgres.default@huey.taila2eeb2.ts.net` and
reports `new: 0, changed: 0`; `python.exe scripts/db_sync_manifest.py --preview` likewise resolves `oci` by
default and comes back clean (`141,244` manifest rows, `0` new/changed). The GUI's Delta Sync button (and
every other GUI operation) now works without any code change, since none of them pass `--target-profile`
either.

A real in-GUI profile picker (to switch back to `supabase` if it's ever revived, or add future profiles)
remains an open thread — this was the cheap fix, not that one.

### Open Threads

- [ ] *(Carried, unchanged)* `--mode dml_files` parser rewrite; GUI target-profile picker (no longer
  urgent now the default is live, but still the only way to reach `supabase` from the GUI is by hand-editing
  `config.toml`); `gui/filemaker_extract_refactored.py` stale-fork cleanup; `picture_metadata` untested
  against real images; `requirements.txt`'s `pandas==2.1.4` pin; the 16 flagged source records; DDR;
  supabase-py/SQLAlchemy prune; anon-JWT rotation; `PicaLocoBackend` consolidation/rebrand; delete
  `sync_config.json`; cron/Task Scheduler wiring for `run_incremental_sync.py`.

---

## Session 11 — 2026-09-02 — Make every GUI operation actually work (client-demo readiness)

**Focus:** User wants the pipeline's "nuts and bolts" solid, then the GUI demo-ready for clients.
Investigating what "fix all GUI operations" required (the user's explicit scope choice) surfaced a genuine
runtime bug that would have crashed several buttons in front of a client — not just staleness.
**Status:** `completed` for the backend/code side. Every operation smoke-tested live via `python.exe`,
exercising `run_python_command()` exactly as a button click would. Visual/click-through testing on Windows
is explicitly deferred to the user, per their choice this session (code-only review, not live GUI testing).

---

### Context

Three separate problems, found in order while tracing why "repoint the GUI at real scripts" wasn't a clean
swap:

1. **`scripts/config_manager.py`'s `_parse_config()` was flatly broken against the current `config.toml`.**
   It still read the pre-profile shape (`target_config['host']`, `target_config['db']`,
   `target_config[db_type]['user']`, `target_config['dsn']`) that stopped existing when Session 5
   introduced `[database.target.<profile>]` sub-tables. **Confirmed live**: running the file's own `__main__`
   demo against the real config threw `Missing required configuration key: 'db'` and crashed outright.
   Every consumer was broken by this — `filemaker_extract_refactored.py`'s diagnostic layer (which backs
   the GUI's Test Connections and migration-status dashboard), `data_exporter.py`, and the standalone
   `usage_example.py` demo. A *second*, separate bug in the same function: `target_config['dsn']` doesn't
   exist either — that key never moved into the per-profile sub-tables; profiles store `dbname` instead
   (used as the connection URL's path segment despite the dataclass field still being called `dsn` —
   pre-existing naming quirk, left alone, just pointed at the right source key).
2. **`scripts/filemaker_extract.py` (the real, actively-maintained script) has no `--json`/`--migration-status`
   support at all** — that reporting layer (`run_info_only()`, `run_migration_status()`, structured
   `connection_status` dicts) only exists in `filemaker_extract_refactored.py`'s class-based design.
   "Repoint everything at the real script" would have silently broken Test Connections/Migration Status
   rather than fixed them.
3. **`gui/` held a second, more-stale, fully independent copy** of `config_manager.py`/
   `database_connections.py`/`filemaker_extract_refactored.py` — confirmed dead code: nothing in `gui/`
   (`filemaker_gui.py`, `gui_operations.py`, etc.) imports any of the three directly, and Python's
   `sys.path[0]` for any subprocess-invoked script under `scripts/` (Session 9's fix) always resolves
   sibling imports to `scripts/`, never `gui/`. A silently-diverging duplicate nobody could reach was exactly
   the kind of trap that caused this session's own confusion tracing the bug.

Separately, `CLAUDE.md` already documented "Load to Target" (`db_dml_loader.py --mode dml_files`) as unable
to parse a realistic FileMaker export — a real, demo-breaking bug, not theoretical.

---

### Decisions

| Decision | Rationale | Alternatives Considered |
|---|---|---|
| Fix `config_manager.py`'s profile resolution (reuse `resolve_active_profile()`/`resolve_target_pwd()`, same pattern as the three main scripts) rather than rebuild the diagnostic layer into `scripts/filemaker_extract.py` | Root-cause fix, small and well-understood; the diagnostic layer's logic was never actually wrong, just fed a config shape that stopped existing two sessions ago | Port `--json`/`--migration-status` into the real script (much bigger, duplicates working logic, not needed just to make the GUI reliable) |
| Split GUI operations by kind: data-moving ops (`full_sync`/`incremental_sync`/`export_files`/`export_images`) → `scripts/filemaker_extract.py`; diagnostic ops (`test_connections`/`migration_status`) stay on `filemaker_extract_refactored.py` (now fixed) | Both scripts support the data-moving flags identically (confirmed via a direct `add_argument` diff); only the refactored script has working diagnostics | Force everything onto one script (would have required building new diagnostic functionality for no reliability benefit) |
| Delete the dead `gui/` duplicate trio outright, not just leave it | Confirmed genuinely unreachable at runtime; a stale, drifting duplicate is itself a reliability risk for future debugging, matching the session's own "nuts and bolts" goal | Leave it in place as inert (rejected — it's exactly what caused this session's own confusion) |
| Fix "Load to Target" by switching to `--mode migration_schema` (drop the broken `dml_files` mode entirely), removing the now-dead `_load_export_path()` helper | `migration_schema` mode is what every live test since Session 5 has actually used and is proven idempotent (Session 7); `--export-path` stays required by argparse but is genuinely unused in this mode — `'unused'` matches how `run_incremental_sync.py` already calls it | Fix the `dml_files` parser instead (bigger, separate, already-known-hard task per `CLAUDE.md`'s own Gotchas) |

---

### Findings

- `scripts/config_manager.py`'s bug was **confirmed live before any fix was written**: `venv/bin/python -c
  "..."` instantiating `ConfigManager` against the real `config.toml` reproduced `Missing required
  configuration key: 'db'` exactly. After the fix, the same call correctly resolved `oci`'s real host
  (`huey.taila2eeb2.ts.net`), user, dbname, and password with `validate_config()` returning `True`.
- **`gui/config_manager.py` had an additional, independent problem** even setting aside staleness: it read
  `target_config[db_type]['pwd']` directly from `config.toml`, with no `resolve_secret`/`env_secrets`
  involvement at all — a real secrets-hygiene gap for whichever copy the GUI happened to import (moot now
  it's deleted, but worth noting: `config.toml`'s per-profile `pwd` fields are just placeholder text, not
  real passwords, so this was never an actual leak — just wrong hygiene, not a live incident).
- Live smoke test (`python.exe`, calling `run_python_command()` directly, same shape a button click sends)
  after all fixes: **Test Connections** now correctly reports both FileMaker (`Found 102.0 base table
  fields`) and target (`PostgreSQL 17.6`, `oci`) connected; **Migration Status** correctly reports live
  per-table row counts (`ratcatalogue: source 141,262 / target 1` — accurately reflecting that staging
  currently only holds Session 8's single test-delta row, not stale/cached numbers); **Load to Target**
  (new `migration_schema` command) ran the loader successfully against that same live staging data.
- No regressions: `py_compile`/pyright clean on both platforms for every touched file; the pyright errors
  that remain in `gui/`/`scripts/config_manager.py`/`filemaker_extract_refactored.py` are byte-for-byte the
  same set as before this session's changes (confirmed via `git stash` diff), just line-shifted.

---

### Outcome

`scripts/config_manager.py`: `_parse_config()` resolves the active profile via
`resolve_active_profile()`/`resolve_target_pwd()` instead of a config shape that stopped existing at Session
5; `ConfigManager.__init__` gained `target_profile`/`db_type` params. `scripts/filemaker_extract_refactored.py`:
new `--target-profile` flag threaded into its `ConfigManager` construction. `gui/gui_operations.py`:
`operation_scripts` now routes data-moving ops to `scripts/filemaker_extract.py` and diagnostic ops to
`scripts/filemaker_extract_refactored.py` explicitly; `load_to_target`'s command switched to
`--mode migration_schema`; dead `_load_export_path()` removed. `gui/filemaker_gui.py`: the "Run
Diagnostics" file-existence check pointed at the real, correctly-resolvable path. Deleted:
`gui/config_manager.py`, `gui/database_connections.py`, `gui/filemaker_extract_refactored.py` (confirmed
dead code).

This round was explicitly **code-only** — no live tkinter click-through (the user's choice this session).
Every operation's *backend* wiring is now proven live; visual/UX testing (does it look right, is it
pleasant to click through for a client) is the user's next step on Windows.

---

### Open Threads

- [ ] *(Carried, unchanged)* `--mode dml_files` parser rewrite; GUI target-profile picker;
  `picture_metadata` untested against real images; `requirements.txt`'s `pandas==2.1.4` pin; the 16 flagged
  source records; DDR; supabase-py/SQLAlchemy prune; anon-JWT rotation; `PicaLocoBackend`
  consolidation/rebrand; delete `sync_config.json`; cron/Task Scheduler wiring for
  `run_incremental_sync.py`; `gui/install_gui_fixed.py`/`gui/setup_gui.py` still reference the deleted
  `gui/`-local trio by name (standalone installer scripts, not imported by the live app — left untouched,
  out of this session's scope, but will report those files "missing" if ever run).

---

## Session 12 — 2026-09-02 — Live output streaming for the GUI (the user's first real click-through)

**Focus:** The user ran the actual GUI on Windows for the first time (following Sessions 9 & 11's backend
fixes) — a real Full Sync — and reported the only sign of activity was the indeterminate progress bar
strobing; no log output appeared anywhere, even in the already-built "View Logs" viewer.
**Status:** `completed`. Root cause traced through two layers, not one; both fixed; live-proven via a
headless smoke test that asserts log entries arrive *during* a real subprocess call, not only after it
returns.

---

### Context

Two separate, stacked problems — fixing only the first would still have shown nothing:

1. **`gui/gui_operations.py`'s `run_python_command()`** — the one method every GUI operation goes through —
   used a single blocking `subprocess.run(capture_output=True, ...)`. All output is buffered by the OS pipe
   and only reaches Python after the subprocess exits; `_process_command_result()` (the only place that
   called `log_subprocess_output()`) only ran at that point. For a multi-minute Full Sync, nothing new
   reached `LogManager` between "Starting: ..." and the final "✓ Completed" — there was nothing for the
   already-working, already-auto-refreshing `LogViewerWindow` to show, no matter when it was opened.
2. **Even after fixing (1), a smoke test still showed zero live entries.** Traced to a second, independent
   bug one layer down: `scripts/filemaker_extract.py`, `scripts/db_dml_loader.py`, and
   `scripts/filemaker_extract_refactored.py`'s `setup_logging()` functions all gated their console
   `StreamHandler` behind `if debug_mode:` — so without `--debug` (which the GUI never passes, and which
   also bumps the logger to a much noisier DEBUG level), every `logger.info()` progress line
   ("Getting a query count", "Has N rows", "Inserted N rows", etc.) went **only** to the log file, never to
   stdout. Confirmed live outside the GUI entirely: piping `filemaker_extract.py`'s own output without
   `--debug` showed nothing but tqdm's progress bar. Streaming an empty pipe live is still an empty pipe —
   fixing (1) alone would have "worked" but shown almost nothing.

A third, connected bug found while reading the cancel path: **`OperationManager._current_process` was
declared in `__init__` but never actually assigned anywhere** — `cancel_current_operation()` ("Stop
Action") checks `if self._current_process: self._current_process.terminate()`, but that was always `None`.
Stop Action could never actually kill a running subprocess; it only reset GUI state to idle while the real
process kept running in the background. Switching to `Popen` (needed for streaming anyway) meant holding a
real process handle, so setting `self._current_process` was a natural, essentially-free part of the same
change.

---

### Decisions

| Decision | Rationale | Alternatives Considered |
|---|---|---|
| Merge stderr into stdout (`stderr=subprocess.STDOUT`) rather than reading two pipes | Order doesn't matter for a live feed (chronological interleaving is more useful than separated streams); avoids a two-pipe reader-thread deadlock; and it's what makes a *fourth*, connected bug fixable for free — the old code unconditionally logged every stderr line as `ERROR`, but `tqdm`'s progress bar defaults to stderr and isn't an error. `log_subprocess_output()` already sniffs real severity from content, so routing everything through it (instead of forcing stderr=ERROR) fixes both problems at once | Two separate reader threads for stdout/stderr (more complexity, no real benefit for a log feed where order-of-arrival matters more than source) |
| Fix the console-handler gating in all three `setup_logging()` functions (always attach, `debug_mode` only controls logger *level*) | Root cause of "nothing to stream" even after fixing the GUI side; also fixes plain CLI usability — running these scripts directly from a terminal without `--debug` showed almost nothing either, which `CLAUDE.md`'s own "How to run" examples never account for | Have the GUI always pass `--debug` (rejected — conflates "show console output" with "verbose DEBUG-level logging," would make a demo's log view far noisier than needed) |
| Rely on Python's default universal-newline translation (`text=True` implies `newline=None`) to handle `tqdm`'s `\r`-based progress updates as ordinary lines | Verified this is standard `io`/`subprocess` behavior, not an assumption — `\r` maps to `\n` on read in text mode, so a plain `for line in process.stdout:` loop naturally yields each tqdm tick as its own line with zero special-casing | Suppressing tqdm output entirely for GUI-invoked runs (would lose real, wanted progress detail; and there's no clean env-var lever for it without touching every `tqdm()` call site) |

---

### Findings

- The bug surfaced through direct live user testing, not code review — confirms the value of the "user
  runs it on Windows and reports back" split from Session 11.
- My own first smoke test attempt (checking `entry.component == "Command-Output"`) produced a false
  negative — `LogManager.log()` bakes the `component` argument into the *message* text
  (`f"[{component}] {message}"`); `LogEntry.component` actually holds the underlying Python logger's name
  (`'FileMakerSync'`, from `record.name` in `LogCaptureHandler.emit()`). Caught by isolating the two halves
  (`log_subprocess_output()` alone vs. a bare `Popen` read loop alone) before concluding the *fix* was
  broken — it wasn't; the *test* was checking the wrong field. Fixed the test to check
  `"[Command-Output]" in entry.message` instead.
- Final live smoke test (`python.exe`, real `scripts/filemaker_extract.py` extract of a small table,
  registering a `LogManager` callback and timestamping every entry): 16 log entries received, **all 16
  arrived before the call returned** — proving the stream is genuinely live, not a fast post-hoc dump.
  Timeout handling re-verified unchanged (`"Command timed out after {timeout}s: {description}"`, same
  shape as before). `_current_process` confirmed `None` again after clean completion (lifecycle correct).

---

### Outcome

`gui/gui_operations.py`: `run_python_command()`'s execution now goes through new `_run_streaming()` —
`subprocess.Popen` with merged stdout/stderr, line-by-line live `log_subprocess_output()` calls, a
`threading.Timer` watchdog reproducing the original timeout behavior, and `self._current_process` properly
set/cleared (fixing Stop Action as a side effect). `_process_command_result()` simplified — output logging
moved to the streaming path, so its old post-hoc stdout/stderr loops are gone; JSON-extraction and
success/failure logic unchanged. `scripts/filemaker_extract.py`, `scripts/db_dml_loader.py`,
`scripts/filemaker_extract_refactored.py`: console log handler now always attached (`debug_mode` only
affects logger level, as it should). No regressions: `py_compile`/pyright clean both platforms; pyright
error set byte-for-byte identical to before this session (confirmed via `git stash` diff).

Still outstanding: the user's next real-world check is the same live Full Sync in the actual GUI, watching
for live-updating log entries this time — that's the true end-to-end proof, not the headless smoke test.

---

### Open Threads

- [ ] *(Carried, unchanged)* `--mode dml_files` parser rewrite; GUI target-profile picker;
  `picture_metadata` untested against real images; `requirements.txt`'s `pandas==2.1.4` pin; the 16 flagged
  source records; DDR; supabase-py/SQLAlchemy prune; anon-JWT rotation; `PicaLocoBackend`
  consolidation/rebrand; delete `sync_config.json`; cron/Task Scheduler wiring for `run_incremental_sync.py`;
  `gui/install_gui_fixed.py`/`gui/setup_gui.py` stale references.

---

## Session 13 — 2026-09-03 to 2026-09-04 — Nine rounds of live GUI testing with the user, real-time

**Focus:** One continuous live-testing session with the user actually clicking through the real Windows
GUI after every fix, reporting back immediately — nine rounds total, each building directly on the user's
own feedback rather than a prescribed plan. Started from a live Unicode crash and a tab-layout request;
along the way surfaced and fixed two genuinely serious bugs (a reentrant-lock deadlock, a concurrent
FileMaker-ODBC-access freeze — both reproduced mechanically before trusting the fix, not just reasoned
about), a real data bug (`full_sync` silently inflating staging 3–4×), and iterated Migration Overview
through three attempts before it actually read well to the user. Round-by-round detail below; short version:
**Round 1** Unicode crash fix + tabbed Actions/Status layout. **Round 2** live output streaming was
incomplete without it — same-day gap fix. **Round 3–4** the tab redesign's `LiveStatusPanel` exposed a real
deadlock in `LogManager`, found, reproduced, fixed. **Round 5** the Unicode fix itself had a gap in one of
three files — found via the user's very next click. **Round 6** the user's direct question after a real
FileMaker edit + Delta Sync ("should I have been told 1 update found?") led to surfacing Delta Sync's own
JSON summary, a startup connection check, button tooltips. **Round 7–8** Migration Overview relabeled, then
fixed at the root (stop refreshing with misleading data), then given a genuinely useful delta-aware
breakdown — three iterations because the first two weren't enough on their own. **Round 9** a real app
freeze from concurrent FileMaker ODBC access, root-caused via the child processes' own log files and fixed
by serializing every subprocess launch through one lock.
**Status:** `completed`. All nine rounds fixed and confirmed live by the user, including the final
delta-aware Migration Overview breakdown. The deadlock was reproduced and disproven mechanically (not just
argued from code reading) — see Findings. The tab layout's actual feel is still the user's to confirm on
Windows.

---

### Context

**The Unicode bug.** Session 12's live re-test showed a "⚠ Minor Issues, 3 errors" status badge during Full
Sync. Traced via the log file (`logs/filemaker_sync_20260903.log`): repeated `UnicodeEncodeError: 'charmap'
codec can't encode character '✓'` — the ✓ checkmark. Root cause: `gui/gui_logging.py`'s own
`LogManager.setup_logging_system()` attaches a console `StreamHandler(sys.stdout)` when `config.toml`'s
`[debug] console_logging = true` (which it is), with no UTF-8 wrapping — unlike the fix already applied to
the three CLI scripts' `setup_logging()` in Session 12. The character comes from `gui/gui_operations.py`'s
own native log calls (`f"✓ Completed: {description}"` etc.), not subprocess output — this was always a
latent bug, just newly exercised at volume once Session 12 started pushing far more log entries through the
system (streamed subprocess lines) than before.

**The layout request.** User's own words, watching a live Full Sync: the Migration Overview grid "shuold be
expandable or on a tab with the funtions buttons on another tab so when the user selects a function they
can just flip to the status / stats / messages screen." Clarified with the user: add a new, simple
always-live status tab; leave the existing, more elaborate `LogViewerWindow` popup (~1400 lines,
search/filter/sort) untouched rather than refactor it into the tab — lower risk, faster to ship. Worth
noting for future reference: the empty Migration Overview grid itself was never going to solve this even
before the tab redesign — it's a static per-table summary populated only by "Update Dashboard", not a log
feed; the real gap was the *absence* of any embedded live feed, not the grid's emptiness specifically.

---

### Decisions

| Decision | Rationale | Alternatives Considered |
|---|---|---|
| Fix the Unicode bug the same way as Session 12's three CLI-script fixes (`codecs.getwriter('utf-8')(sys.stdout.buffer)` before creating the console handler) | Consistent, already-proven pattern in this exact codebase from the same day; small and contained | Disable `console_logging` in config.toml instead (would silence a legitimately useful debug channel just to dodge a fixable bug) |
| New `LiveStatusPanel` widget (`gui/gui_widgets.py`) reusing `LogManager.get_recent_logs()`/`add_callback()` — the same primitives `LogViewerWindow` already uses — rather than inventing a new log-delivery mechanism | No new plumbing needed; Session 12's streaming fix already pushes everything through these; keeps the popup and the new tab reading from one source of truth | A separate polling mechanism (redundant, more moving parts) |
| Keep `LogViewerWindow` popup untouched, ship a deliberately simpler tab (no search/filter/sort) | User's explicit choice when asked; the popup's ~1400 lines are working, tested-by-use code — refactoring it into an embeddable frame was assessed as real risk for this pass | Retire the popup and move all its functionality into the tab (offered, user declined) |
| Auto-switch to the Status tab in `safe_run_operation()`'s confirm callback, not as a separate manual step | Matches the user's own stated goal ("so when the user selects a function they can just flip to the status screen") — made automatic rather than requiring an extra click | Leave switching manual (doesn't fulfil what was actually asked) |

---

### Findings

- Caught a real editing mistake before it shipped: the first attempt at adding `LiveStatusPanel` to
  `gui/gui_widgets.py` didn't account for `StatusBar.update_health()`'s actual last two lines (an "Update
  timestamp" statement) — the `Read` used to locate the insertion point stopped just short of the file's
  true end, so the new class got spliced into the middle of that method, orphaning its trailing statement
  at the end of the file where it silently became part of `LiveStatusPanel._append()`'s body instead
  (`self.last_update_label.configure(...)` — an attribute that doesn't exist on that class). Caught by
  pyright (`Cannot access attribute "last_update_label" for class "LiveStatusPanel*"`) before any runtime
  test ran — not by the runtime smoke test itself. Fixed by restoring the timestamp line to
  `update_health()` and removing the orphaned duplicate.
- Live headless smoke test built a *real* (offscreen, `root.withdraw()`) Tk instance rather than only
  import-checking — necessary here since the previous two sessions' `run_python_command()`-level tests
  couldn't have caught a Tk widget-tree/layout bug like the one above; this one specifically constructed
  `FileMakerSyncGUI(root)` end-to-end and asserted `self.notebook.tabs()` has exactly 2 tabs named "Actions"
  and "Status", plus pushed real log entries through a real `LiveStatusPanel` and confirmed the widget's
  text content updated and the line cap enforces correctly.

---

### The deadlock — found immediately after shipping the tab redesign

Right after relaunching with the new tabs, the user clicked "Test Connections" and the window went "Not
Responding". Root-caused via `gui/gui_logging.py`, not guessed at:

`LogManager._add_log_entry()` calls `_notify_callbacks(entry)` **while still holding** `self._log_lock` (a
plain, non-reentrant `threading.Lock`). `_notify_callbacks`'s own except-handler used to call
`self.logger.error(...)` when a callback raised — which re-enters the standard `logging` module's handler
chain, which calls back into `LogCaptureHandler.emit()` → `self.log_manager._add_log_entry()` → tries to
reacquire the *same* lock, from the *same* thread, that the outer call still holds. A plain `Lock` isn't
reentrant, so that thread blocks forever — and since it's holding Python's global `logging` module lock
partway through, every *other* thread's logging calls (including the Tk main thread's) block behind it too.
That's what "Not Responding" actually was.

This bug was latent in `gui_logging.py` before today, but nothing had ever triggered it: no registered
callback had ever raised. `LiveStatusPanel`'s new callback (`_on_new_entry`) was the first one invoked
*concurrently from two background threads simultaneously* — `ConnectionTester.test_all_connections()`
deliberately runs the FileMaker and target checks concurrently — combined with `self.after(0, ...)` being
called from a background thread under that concurrent load being the first realistic way to actually
trigger an exception in a callback.

**Reproduced mechanically, not just reasoned about:** wrote a standalone test that registers a callback set
to raise every 3rd call, fires two threads each logging 200 entries concurrently, with a 15s watchdog.
Run against the pre-fix code: hung, confirmed dead at exactly `callback calls: 3` (the exact call that first
raised) — the watchdog's timeout is what let the test fail cleanly instead of hanging the test run itself.
Run against the fix: 0.03s, all 400 calls completed, zero deadlock.

**Fix:** moved `_notify_callbacks(entry)` outside the `with self._log_lock:` block in `_add_log_entry()` —
the lock only needs to protect the `memory_logs` list mutation, not arbitrary callback execution. Changed
`_notify_callbacks`'s except-handler from `self.logger.error(...)` to a direct `print(..., file=sys.stderr)`
— eliminates the recursion risk entirely rather than just narrowing the window for it. Also widened
`LiveStatusPanel._on_new_entry`'s except clause from `tk.TclError` specifically to a bare `except Exception`,
defensively — a `LogManager` callback must never let anything escape back into that chain, regardless of
type.

Separately, the user also flagged the Actions tab having "lots of empty space" now that Migration Overview
moved to the Status tab — the Quick Actions box was still only `pack(fill='x')` (no vertical expand), so it
sat as a small block above a large dead void. Changed to `fill='both', expand=True` so the bordered box
itself grows to fill the tab.

---

### Outcome

`gui/gui_logging.py`: `LogManager.setup_logging_system()`'s console handler now UTF-8-wrapped on Windows
(same fix as the three scripts); `_add_log_entry()`/`_notify_callbacks()` no longer call callbacks while
holding `_log_lock`, and never re-enter the logging chain from an error handler. `gui/gui_widgets.py`: new
`LiveStatusPanel` class (`ScrolledText`, capped at 500 lines, same color palette as `LogViewerWindow`'s
tree tags), its callback now catches broadly; `QuickActions`'s frame now fills its tab. `gui/filemaker_gui.py`:
`create_main_content()` rebuilt around a `ttk.Notebook` (Actions tab: Quick Actions buttons + progress bar;
Status tab: Migration Overview grid + the new `LiveStatusPanel`); `safe_run_operation()` now calls
`self.notebook.select(self.status_tab)` on confirm. No regressions: `py_compile`/pyright clean both
platforms (pyright error set unchanged from before this session, confirmed via `git stash` diff each round).

Still outstanding: the user's own re-test on Windows — does the tab flip feel right, is the live panel
legible and useful during a real Full Sync, and (most importantly this round) does Test Connections now
actually complete instead of freezing.

---

### Open Threads

- [ ] *(Carried, unchanged)* `--mode dml_files` parser rewrite; GUI target-profile picker;
  `picture_metadata` untested against real images; `requirements.txt`'s `pandas==2.1.4` pin; the 16 flagged
  source records; DDR; supabase-py/SQLAlchemy prune; anon-JWT rotation; `PicaLocoBackend`
  consolidation/rebrand; delete `sync_config.json`; cron/Task Scheduler wiring for `run_incremental_sync.py`;
  `gui/install_gui_fixed.py`/`gui/setup_gui.py` stale references; **user's live re-test on Windows** (tab
  layout feel, live panel usefulness, and specifically that Test Connections no longer hangs).

---

### Round 4 (same session, same day) — staging duplication found; level filter + duration summary added

After the deadlock fix, the user's live re-test surfaced two more things:

**`rat_migration` staging tables were 3–4x inflated.** The Status tab's Migration Overview showed
"Completion %: 302%" — not a display bug. Checked directly against `oci`: `ratbuilders` 520→2,080 (4x),
`ratroutes` 2,892→11,568 (4x), `ratcatalogue` 141,262→423,787 (~3x), etc. — exact multiples, all seven
tables. Root cause: `gui/gui_operations.py`'s `operation_commands['full_sync']` never included `--del-data`,
so every repeated Full Sync from the GUI today just appended onto existing staging rows instead of
replacing them. **Verified the actual production data was never at risk**: `rat.catalog`/`catalog_metadata`/
`usage`/`builder`/`catalog_builder` all matched the documented-correct baseline exactly (`catalog` 141,244,
`catalog_builder` 116,538, etc.) — the final schema's upsert-on-natural-key logic dedupes regardless of how
much staging duplication feeds into it; only the intermediate staging tables (and the GUI's own read of
their row counts) were affected. `picture_metadata = 0` is separately expected and pre-existing (documented
since Session 5 — needs local image files, none present in this environment). Fixed: added `--del-data` to
`full_sync`'s command. **Cleanup done, with the user's explicit go-ahead**: ran
`python.exe scripts/filemaker_extract.py --db-exp --ddl --dml --del-data --target-profile oci` directly
(~4m22s). Verified after: every `rat_migration.*` staging count now matches the true FileMaker source
exactly (`ratcatalogue` 141,262, `ratbuilders` 520, `ratroutes` 2,892, `ratcollections` 66, `ratcopyright`
24, `ratlabels` 21, `prompts` 3 — no inflation), and `rat.*` (`catalog` 141,244, `builder` 518,
`catalog_builder` 116,538) unchanged, confirming the final schema was never at risk either before or after.

**Level filter + duration summary.** User: "lots of logging" (confirmed the color-coding is working, wants
volume control) and "how long etc. did it take stats." `gui/gui_widgets.py`'s `LiveStatusPanel` gained a
header row: a level-filter combobox (`All`/`Info+`/`Warning+`/`Errors only`, defaulting to `Info+` to hide
DEBUG-level chatter) that re-renders from `LogManager.get_recent_logs()` on change — filters the *view*
only, `LogManager`'s own history is untouched — and a duration/result summary label, fed by a new
`'duration'` field threaded through `gui/gui_operations.py`'s existing `run_operation_async` →
`_notify_callbacks_safe('complete', ...)` payload (`time.time()` at start, computed in the `finally` block
so it's correct even on failure/exception). Deliberately did not attempt to parse/condense the underlying
subprocess log lines into per-table summaries — bigger, riskier text-parsing scope; the filter and duration
summary directly address what was actually asked.

Caught a real bug in my own first attempt: `on_operation_status_safe`'s nested `update_operation_ui()`
closure did `result = result or {}`, which — because of Python's closure scoping rules (any assignment to a
name inside a nested function makes that name local to the *whole* function, not just from that line
onward) — shadowed the outer `result` parameter and raised `UnboundLocalError` at the read on the same
line. Caught by pyright (`"result" is unbound`) before runtime. Fixed by renaming the local to `op_result`.

Live headless smoke test (real offscreen Tk `LiveStatusPanel`): default filter correctly hides DEBUG,
switching to "All" reveals it, switching to "Errors only" hides INFO/WARNING and shows a subsequent ERROR;
duration formatting correct for both sub-minute and multi-minute cases; `update_summary()`/`set_running()`
produce the expected label text. (One test-only false alarm along the way: my test's own bare `LogManager()`
call defaulted to `log_level='INFO'`, silently dropping the DEBUG entry before it ever reached the panel —
not a real bug, just not matching `config.toml`'s actual `log_level = "DEBUG"`; fixed the test's setup, not
the code.)

`py_compile`/pyright clean both platforms, no regressions (confirmed via `git stash` diff).

---

### Round 5 (same session, same day) — the Unicode fix had a gap; found via the user's very next click

User re-tested with the new filter/summary and immediately reported: "not sure the filtering is working"
plus a wall of `UnicodeEncodeError` on ✓ again, visible under both "Warning+" and "Errors only" filters.

**The filter itself was working correctly** — "Warning+"/"Errors only" both legitimately include `ERROR`
level, so a genuine flood of ERROR-tagged crash entries was always going to show under either. Not a filter
bug; just completely obscured by real errors flooding in.

**The real bug: my earlier Unicode fix to `scripts/filemaker_extract_refactored.py` was incomplete.** Round
1 today fixed three scripts' console-handler gating (`if debug_mode:` → always attached) and separately
added UTF-8 stdout/stderr wrapping to two of them (`filemaker_extract.py`, `db_dml_loader.py`) — but
`filemaker_extract_refactored.py`'s own fix only removed the debug-mode gate, **without** also adding the
UTF-8 wrapping that made that safe elsewhere. Net effect: that fix made things *worse*, not better — the
handler went from "off unless `--debug`" (never actually triggered by the GUI, which never passes
`--debug`) to "always on, but still crashes on `✓`" — and this script is the one behind Test
Connections/Migration Status/Update Dashboard, which log `f"✓ FileMaker: {message}"`/`f"✓ Target:
{message}"` on every successful connection check. Confirmed by reproducing directly: `python.exe
scripts/filemaker_extract_refactored.py --migration-status --json --target-profile oci` threw the exact
same `UnicodeEncodeError: ...character '✓' in position 71` repeatedly. Fixed: added the same
`codecs.getwriter('utf-8')(sys.stdout.buffer)` wrapping this file was missing, matching the other two
scripts exactly.

**Swept for other unfixed spots rather than wait for another report:** grepped every `StreamHandler(sys.stdout)`
call site across `scripts/` and `gui/` (4 total). Found a 4th, in `gui/gui_logging.py`'s
`toggle_console_logging()` — confirmed dead code (grepped, never called anywhere in `gui/`), but the exact
same latent bug shape; fixed it too rather than leave a landmine for whenever it does get wired up.

Verified the fix two ways: direct reproduction (`filemaker_extract_refactored.py --migration-status --json`
now produces zero `Logging error` lines, previously produced dozens per run), and a live smoke test through
the actual GUI code path (`run_python_command()`, watching for any `Logging error`/`UnicodeEncodeError`
text in the streamed output — zero found, `result['success']` `True`).

**Lesson for future Unicode-safety fixes in this codebase:** when the same fix needs applying to multiple
files, verify each one got the *complete* pattern (both halves — degate AND encode-wrap), not just that the
edit compiled. A partial fix that "looks the same" can be worse than no fix at all if it changes when the
broken code path actually runs.

`py_compile`/pyright clean both platforms (confirmed via `git stash` diff), no regressions.

---

### Round 6 (same session, same day) — the Delta Sync question, tooltips, startup connection check

After confirming the staging cleanup, the user's next round of feedback, all from watching a real Delta
Sync run: connection indicators only went green "at the very end" (not, as it turns out, a timing quirk —
**confirmed nothing calls a connection test at startup at all**, only a config save or a manual click ever
had); Stop Action "always disabled" (checked mechanically first rather than assumed — `show_progress()`/
`hide_progress()` correctly cycle disabled→normal→disabled in isolation, so the mechanism itself is fine;
the real gap is Test Connections/Update Dashboard never touch it, a separate code path bypassing the
operation state machine entirely — per the user's choice, documented as a known limitation rather than
fixed this round); a request for hover tooltips on the Quick Action buttons; and the substantive one — after
hand-editing one FileMaker record and running Delta Sync, "should I have been told something like 1 update
found and updated to the target database, or is Delta Sync not for this?"

**The honest answer: yes, and the information already existed** — `run_incremental_sync.py`'s own `Report`
already computes new/changed/verified/manifest-advanced counts, it just prints them as plain text with
nothing for the GUI's already-built JSON extraction (`_extract_json_from_output()`, used by Test
Connections/Migration Status) to find. Investigating this also surfaced why Migration Overview showed
"302%... Partial" reading right after a clean, successful Delta Sync: its "Target" column has always meant
"rows currently in `rat_migration` staging," which was a reasonable proxy back when only a full re-extract
ever populated staging (staging ≈ full mirror) — but Delta Sync deliberately narrows staging to just the
delta by design, so the same metric that made sense for Full Sync becomes actively misleading for Delta
Sync. No clean fix exists this round (`ratcopyright`/`ratlabels`/`prompts` have no obvious 1:1 final-schema
table to compare against instead) — asked the user, who chose the honest-relabel option over a deeper
redesign.

**Fixes:**
- `gui/filemaker_gui.py`: `self.root.after(500, self.safe_test_all_connections)` added to `__init__`,
  same pattern as the existing `start_auto_refresh` delay — connection cards now show real status within a
  second of the window opening.
- `gui/gui_widgets.py`: new `Tooltip` class (tkinter has none built in — standard `<Enter>`/`<Leave>` +
  borderless `Toplevel` pattern) wired onto all 10 Quick Action buttons via a `QuickActions.BUTTON_TOOLTIPS`
  dict, written specifically to clear up the "Incremental Sync" vs "Delta Sync" naming confusion ("...NOT
  delta-driven despite the name -- see Delta Sync for that" / "...the recommended way to pick up recent
  edits") and to correctly describe Load to Target's current (Session 11) behavior.
- `scripts/run_incremental_sync.py`: added `import json`; every exit path (nothing-to-do, `--dry-run`, and
  full success) now also prints a JSON summary line at the end — reusing the GUI's existing
  `_extract_json_from_output()` scanning as-is, no new CLI flag or `gui_operations.py` parsing changes
  needed, since that scanner already looks for a `{`-starting line anywhere in stdout.
- `gui/gui_operations.py`: completion notification (`run_operation_thread()`) now also carries
  `'data': command_result.get('data')`; `gui/filemaker_gui.py` passes it through to
  `LiveStatusPanel.update_summary()`, which now (`gui/gui_widgets.py`) accepts an optional `data` param and
  appends a detail clause (`" — 1 changed, 1 verified"` / `" — nothing to do"`) when it recognizes the
  delta-sync summary shape (`'manifest_advanced' in data`) — a cheap, specific-enough check that leaves
  every other operation's plain duration-only summary untouched.
- `gui/gui_widgets.py`, `MigrationOverview`: `'Completion %'` stat relabeled `'Staging Match %'`, the
  `'Target'` column's *displayed* heading (not its internal id — nothing else needed touching) relabeled
  `'Staging'`, and a small persistent gray caption added below the table explaining what it actually shows
  and pointing at the Delta Sync summary above it for the real answer.

Verified: direct reproduction (`run_incremental_sync.py --target-profile oci --dry-run` now ends with a
parseable JSON line); full headless smoke test covering all five pieces together, including the complete
`run_python_command()` → `run_incremental_sync.py --dry-run` → parsed-`data` path (not just the unit-level
pieces in isolation) — all passed. `py_compile`/pyright clean both platforms, no regressions (`git stash`
diff empty).

---

### Round 7 (same session, same day) — the relabel wasn't enough; stop the misleading refresh instead

The rich Delta Sync summary line from Round 6 worked exactly as intended live ("✓ Delta Sync completed in
37.5s — 1 changed, 1 verified"). But the user pushed back on Round 6's Migration Overview fix: even
relabeled and captioned, a large bold "2%" next to "⚠ Partial" is alarming at a glance regardless of what
the fine print underneath says — people pattern-match the headline number, not the caption. Fair, and a
real gap in Round 6's fix, not a new problem: relabeling explains *what* the number means but doesn't stop
a wrong-looking number from appearing in the first place.

**Root cause of the number appearing at all**: `on_operation_status_safe()`'s `'complete'` handler
unconditionally scheduled `safe_refresh_migration_status()` after *every* operation, including Delta Sync —
so the very act of a successful Delta Sync immediately overwrote whatever good, Full-Sync-representative
numbers were on screen with delta-scoped staging counts. Fix: skip the auto-refresh specifically for
`delta_sync` and the older `incremental_sync` (neither leaves staging holding a full mirror of FileMaker,
by design — `incremental_sync` doesn't clear staging first either, so it has the same problem in kind).
Leaves whatever was last shown (a real Full Sync's numbers, or the unset "not yet run" state) untouched —
more honest than replacing it with a number that looks broken. "Update Dashboard" (manual) is unaffected —
a user who explicitly asks for a staging snapshot still gets one, with Round 6's caption still explaining
it.

Verified with a targeted smoke test (spy on `root.after`, watching specifically for
`safe_refresh_migration_status` calls) across all three operation kinds — confirmed `delta_sync`/
`incremental_sync` correctly skip scheduling the refresh, `full_sync` still schedules it. One test-harness
gotcha along the way: the first attempt asserted immediately after a single `root.update()` call and failed
for `full_sync` even though the fix is correct — `on_operation_status_safe` runs through
`schedule_gui_update()`'s async queue, processed by a periodic ~100ms `root.after` poll, so a single
synchronous `update()` right after the call doesn't give that poll cycle time to fire. Fixed the test (not
the code) with a short poll loop.

`py_compile`/pyright clean both platforms, no new regressions (only the same pre-existing
`ConfigurationWindow` import error, unrelated).

---

### Round 8 (same session, same day) — a delta-aware table breakdown instead of nothing

Round 7's fix (skip the misleading auto-refresh after Delta Sync) worked as intended, but left Migration
Overview showing "0/0, 0%" and an empty grid after a fresh app launch + Delta Sync — technically honest (no
longer wrong), but the user pointed out it's now just less useful. Their own specific ask: show per-table
stats, and for the table that actually changed, put something like "1 update" under Status and a
success indicator under Progress — instead of either a misleading percentage or nothing.

Built entirely from data already available — no new live queries, no changes to `run_incremental_sync.py`'s
output. Delta Sync's own behavior is fixed and known: it always touches the same set of tables the same
way (`ratcatalogue` gets the actual delta; `ratbuilders`/`ratroutes`/`ratcollections`/`prompts` get a full
refresh every single run regardless of what changed; `ratcopyright`/`ratlabels` aren't touched at all) — so
the already-parsed JSON summary (`new`/`changed`/`verified`/`manifest_advanced`) is enough to build an
accurate, delta-specific table view without querying the database again.

`gui/gui_widgets.py`, `MigrationOverview`: new `show_delta_result(data)` method — repurposes the two
headline stat boxes ("Tables Migrated" → "Tables Touched", showing e.g. "5/7"; "Staging Match %" → "Rows
Changed", showing the actual count) and populates the grid: `ratcatalogue` gets `Status="N update(s)"`,
`Progress="✓ Success"` (or "⚠ Check log" if verified count doesn't match); the four always-refreshed
reference tables get `Status="Refreshed (full)"`, `Progress="✓ Success"`; the two untouched tables get
`Status="Not touched by Delta Sync"`. Needed to keep references to the stat `LabelFrame`s (not just their
value labels) to relabel them — added `self.stat_frames` alongside the existing `self.stat_boxes`.
`update_overview()` (the real Full-Sync-driven refresh) now resets both labels back to their normal text
first, in case a delta view was showing.

`gui/filemaker_gui.py`, `on_operation_status_safe()`: for `delta_sync` specifically, when `data` is present
and shaped like the delta summary (`'manifest_advanced' in data` — same check `update_summary()` already
uses), calls `show_delta_result()` instead of just skipping the refresh outright. `incremental_sync` (the
older, non-delta operation) still just skips the refresh with no special view — it has no equivalent JSON
summary to build one from.

Verified with a headless smoke test: pushed a synthetic delta-sync-shaped `data` dict through
`show_delta_result()` directly, confirmed every row and both relabeled headline stats match expectations;
then called `update_overview()` with a normal full-sync-shaped payload and confirmed both labels reset back
to "Tables Migrated"/"Staging Match %" correctly (proving the two views don't leak into each other).
`py_compile`/pyright clean both platforms, no regressions (`git stash` diff empty).

---

### Round 9 (same session, same day) — a real app freeze: concurrent FileMaker ODBC access

The app froze ("Not Responding") right after the user launched it and immediately clicked Delta Sync.
Root-caused via the log files rather than guessed at (the child processes' own log files kept writing even
while the GUI itself was frozen, since they're separate OS processes) — `logs/filemaker_extract_20260904.log`
showed **two near-simultaneous, independent processes** both running `filemaker_extract_refactored.py
--info-only --json`'s full connection-test sequence, timestamps only 5–70ms apart. Traced to a genuinely
pre-existing bug in `ConnectionTester.test_all_connections()`: it has always called
`test_filemaker_connection()` **and** `test_target_connection()` concurrently, each independently launching
its own subprocess — even though a single `--info-only --json` response already reports *both* statuses in
one payload. Wasteful but apparently harmless before today, since nothing made it fire reliably at exactly
the moment a user might also start a real operation. Round 6's new startup auto-connection-test changed
that: it now **guarantees** two concurrent FileMaker-touching subprocesses at every launch, and if the user
acts quickly (exactly what happened — "launched, started demo"), Delta Sync's own FileMaker connection
piles a third one on top. FileMaker Pro's ODBC driver does not reliably handle concurrent connections from
a single desktop file, and apparently blocked badly enough to also stall the GUI's own Python process (a
plausible mechanism: a native blocking ODBC call not releasing the GIL promptly during a real driver-level
hang would starve the Tk main thread of GIL time too, not just the background thread that issued it).

**Two fixes, addressing both the trigger and the root cause:**
1. `gui/gui_operations.py`, `OperationManager`: new `self._subprocess_lock` — every `run_python_command()`
   call now serializes through it before launching, regardless of caller (connection tests, status
   refreshes, real sync operations all funnel through this one method). Always acquired from a background
   thread (confirmed true for every current caller), so blocking here can never freeze the GUI itself — it
   just makes a launch wait its turn instead of racing. This is the actual fix: no code path, present or
   future, can trigger concurrent FileMaker/target subprocess access again.
2. `ConnectionTester.test_all_connections()` rewritten to make exactly **one** `--info-only --json` call and
   process both connection types from its single response (reusing the already-correctly-shaped
   `_process_connection_result()`, which already reads `connection_status.filemaker`/`.target` from one
   payload) — halves the FileMaker/target touch-time for every "Test Connections" click and the startup
   auto-test, and removes a second, independent source of the same class of risk. `test_filemaker_connection()`/
   `test_target_connection()` (used by the individual per-card "Test" buttons) are unchanged.

Verified two ways: a live serialization test (two concurrent `run_python_command()` calls against a
deliberately slow fake script, confirmed their execution windows never overlap — one fully finishes before
the other starts) and a live call-count test (`test_all_connections()` now makes exactly 1 subprocess call,
not 2, and both connection statuses still come back correctly from it). `py_compile`/pyright clean both
platforms, no regressions.

---

### Session close — scope clarified; docs consolidated; committed

Asked what the next big feature should be. First guess (cron/Task Scheduler automation for
`run_incremental_sync.py`) was wrong — the user corrected it directly: **this repo is a transient,
per-engagement migration tool**, not a permanent service. The real pattern: a client (RAT) provides desktop
access to the FileMaker machine, the scripts get installed there, the migration runs during that on-site
engagement. No persistent server context exists for automation to live in. The actual end goal is bigger
than this repo: "getting the RAT people off old s/w onto the newer (Postgres/React Form/REST API/iOS,
Android app)" — `filemaker_sync`'s job is the narrower "get the data in reliably" piece of that program.
Whether/where the REST API/frontend work has started is genuinely unknown as of this session's close — asked
the user, not yet answered. Saved to memory (`project_use_case_and_scope.md`) so this doesn't need
re-explaining next session.

Asked to commit everything and bring the docs current for both human and AI readers. Did three things:

1. **Deleted two fully-stale handoff docs** (`HANDOFF_increment2.md`, `HANDOFF_loader_adoption.md`,
   untracked, predating this worksheet) — both described work completed and superseded by Sessions 4–8;
   keeping them around was exactly the kind of stale-duplicate clutter this project has already been bitten
   by more than once this week.
2. **Consolidated `CLAUDE.md`'s GUI section.** It had grown into nine near-duplicate "Session 13, same
   day" paragraphs (one per round, chronologically appended) — accurate but unusable as a *current-state*
   reference; a fresh reader had to read the whole chronological pile to find out where things actually
   stand today. Rewrote it as a single "GUI status" summary organized by *topic* (script wiring, streaming,
   layout, the two serious bugs, Migration Overview, polish, known limitations) instead of by session,
   keeping the full blow-by-blow only in this file. Also refreshed the "Verified facts" row counts to
   today's freshest live numbers (previously stale from Sessions 3–5) and fixed a couple of small
   contradictions the day's changes had introduced (`config.toml`'s default profile description still said
   `supabase` after Session 10 flipped it to `oci`).
3. **This entry** — closing out Session 13's own header/focus to reflect all nine rounds, not just the
   first three (which is what it said before this pass).

No code changes in this entry — documentation and cleanup only. Committed and pushed at the user's request;
see the commit message for the exact file list.

---

## Session 14 — 2026-09-06 — App-layer kickoff: `picaloco_web` planned; `mobile_catalog_view` and the missing images resolved

**Focus:** Session 13 closed with the app-layer question open ("whether/where the REST API/frontend work has
started... not yet answered"). This session answered it: the client wants a much simpler public web
search/browse tool than the over-specced, unfinished `trainpixelfolio` mobile app, keyed on `image_no`. Planned
and began Phase-0-verifying a new sibling repo, `picaloco_web`, to build it. **Note for future sessions:** an
earlier attempt at this same conversation was lost to a desktop power outage before anything was written down
— this entry, the updated plan file, and new memory files exist specifically so that doesn't happen again.
**Status:** planning complete and user-approved; Phase 0 spike (verify the data/image story before writing UI
code) mostly done, live DDL (recreating `mobile_catalog_view` on `oci`) approved but not yet executed as of
this entry.

---

### Context

Three sibling repos make up the whole picture, confirmed this session (not all previously documented here,
since they live outside this repo):
- `filemaker_sync` (this repo) — the migration pipeline, stable, not to be touched by this work.
- `trainpixelfolio` — a React Native/Expo mobile app ("PicaLoco"), over-specced (cart, favourites, auth,
  admin back-office) and unfinished; already has `ENABLE_AUTHENTICATION/CART/ADMIN: false` feature flags,
  i.e. the mobile team independently reached the same "strip this down" conclusion.
- `picaloco_rest` — not a custom backend, just a Vercel-hosted Swagger UI mirror of Supabase's OpenAPI spec.

The client's actual ask, per the user: a simple public browser-based search/browse tool for the photo
archive, keyed on `image_no`, styled cleanly ("akin to good Apple design policies"). Recommended building
this as a new, much smaller web app rather than finishing the mobile app first — directly matches what was
asked, sidesteps fixing cart/auth/admin scope nobody wants yet, and can reuse `trainpixelfolio`'s existing
Supabase query logic as a reference.

---

### Decisions

| Decision | Rationale | Alternatives Considered |
|---|---|---|
| New sibling repo `picaloco_web` (user-confirmed), not a subfolder of `trainpixelfolio` or a full replacement of it | Different tooling (web bundler vs Expo/RN metro); leaves the mobile app untouched in case it's revisited | Subfolder in trainpixelfolio (mixes RN/web deps); replace trainpixelfolio entirely (loses it as reference/fallback) |
| Stack: Vite + React + TS + Tailwind + shadcn/ui + react-router + `@supabase/supabase-js` + `@tanstack/react-query` | Two routes, one data source, no SSR/SEO need that would justify Next.js; Vite deploys to Vercel same as `picaloco_rest` already does | Next.js (only worth it if indexable per-photo SEO pages become a real requirement later) |
| Audience is **public** internet, not just RAT staff on Tailscale (user-confirmed) | Real infra consequence: `oci`'s backend is Tailscale-only today | Tailscale-only tool (would have been simpler, but isn't what was asked) |
| Recreate `mobile_catalog_view` on `oci` from the **recovered original SQL** (found on the old cloud project's dashboard), not reconstructed from guesswork | User pulled the exact `pg_get_viewdef` output from the old `dev` schema on Supabase.com — every column matches `rat.*` 1:1 except `search_vector` (never migrated) | Guessing the join shape from `FilterModal.tsx`'s query usage alone (would have been close but not exact) |
| Images: **split sources** — metadata/search stays on `oci`/`rat`; images load from a separate, already-public Supabase project's Storage, no file copying | That project (`tvucfqzldbcghtxddtmq`, found in `trainpixelfolio` git history) already has 1,003 real thumbnails, publicly fetchable with zero auth, right now | Copying files into `oci`'s (currently empty) Storage bucket first (unnecessary extra work for no benefit) |

---

### Findings

- **`mobile_catalog_view` genuinely does not exist on `oci`** (confirmed live: zero views in the `rat`
  schema). `trainpixelfolio` was built against a view that was apparently only ever created on the old cloud
  project, never carried over when the data migrated to `oci`.
- **The old cloud project (`kmoehqdowgdupzdxtbei`, `filemaker_sync`'s documented "supabase" profile) is
  confirmed dead, not just paused** — direct-connect host `db.kmoehqdowgdupzdxtbei.supabase.co` doesn't even
  resolve in DNS (stronger signal than the previously-known pooler "tenant not found" error). Its dashboard
  is still reachable via browser login, though (different subsystem than direct Postgres).
- **A third, previously-undocumented Supabase project exists**: `tvucfqzldbcghtxddtmq.supabase.co`, found via
  `git log -p -- trainpixelfolio/.env` (commit "7. Add pictures working"). It's alive, has a `dev` schema
  with the same catalog data (treat as a frozen early-development snapshot, not a live source), and — the
  useful part — a public `picaloco` storage bucket with **1,003 real thumbnail `.webp` files** under
  `images/`, fetchable with zero auth. Coverage: `arc`(591) `alb`(113) `ab`(128) `aj`(43) `ae`(30) `agb`(52)
  `adgp`(28) `albsa`(18) — ~0.7% of the 141,244 catalog rows have a real image today; the rest need a
  placeholder state, not a broken image.
- **Concrete (not just theoretical) security finding on `oci`**: the `anon` Postgres role already has schema
  `USAGE` on `rat` and table `SELECT` (read-only — no write grants, better than initially feared) on every
  `rat` table, with **zero RLS anywhere**. `rat.catalog` includes `valuation` (numeric) and `owners_ref`
  (varchar) columns that must never be exposed publicly. The recovered `mobile_catalog_view` SQL already
  excludes both, confirming the original design was privacy-conscious — the fix is to expose only that view
  publicly (via Tailscale Funnel, once stood up) and never grant `anon` direct access to raw `rat.catalog`.
- `trainpixelfolio/.env`'s CRLF line endings broke naive `curl`/shell env-var use the same way this repo's
  own docs already warn about elsewhere — strip `\r` before use.
- A real, harmless pre-existing bug in `trainpixelfolio/src/components/FilterModal.tsx`: its builder/works-
  number filter does a jsonb containment check on `builder_id`, but the view's `builders` array only ever
  carries `builder_name`/`builder_code` — that filter could never have matched anything. Not fixed (out of
  scope, mobile app untouched), just noted so `picaloco_web`'s port doesn't repeat it (filter on
  `builder_code` instead).

---

### Outcome

A full implementation plan for `picaloco_web` was designed (two Explore agents + one Plan agent), reviewed,
and approved by the user; saved at `/home/trevour/.claude/plans/linked-puzzling-phoenix.md` (outside this
repo — a Claude Code plan file, not a repo artifact) and kept updated as Phase 0 findings came in, so it's
the authoritative next-step reference regardless of chat continuity. Two of Phase 0's four items are
resolved (the view's real SQL recovered; the image story solved via the third project); the `CREATE VIEW`
against live `oci` is written and user-approved but not yet run as of this entry (next action). Tailscale
Funnel for public reachability and the anon-grant/RLS hardening remain outstanding. No code exists yet in a
`picaloco_web` repo — that repo hasn't been created on disk yet.

---

### Open Threads

- [ ] Run the recreated `mobile_catalog_view` + `GRANT SELECT ... TO anon` against live `oci`, then verify.
- [ ] Stand up Tailscale Funnel on the `oci` host for public reachability; confirm Vercel's edge can actually
  reach it once live (don't assume).
- [ ] Harden `anon`'s grants so the public path only ever reaches `mobile_catalog_view`, not raw `rat.catalog`
  (the raw-table `SELECT` grant already exists today and is currently only "protected" by Tailscale).
- [ ] Confirm the `tvucfqzldbcghtxddtmq` project's plan won't auto-pause it from inactivity (Supabase free
  tier does this) — check its dashboard.
- [x] ~~Scaffold the actual `picaloco_web` repo~~ — **done, same day.** See update below.

**Update, same session, continued:**
- Tailscale Funnel stood up on `huey` (`tailscale funnel --bg http://100.124.0.62:8000`, run by the
  user after the tailnet admin enabled Funnel at `login.tailscale.com/f/funnel`) — confirmed publicly
  reachable via a fetch from outside the tailnet entirely (Anthropic's own infra got a real `401`,
  not a connection failure).
- `anon`'s grants hardened: `REVOKE SELECT ON rat.catalog, rat.catalog_metadata, rat.catalog_builder,
  rat.usage, rat.picture_metadata FROM anon` (run by the user via Supabase Studio's SQL editor,
  `http://huey.taila2eeb2.ts.net:3000` — confirmed running as a container on the same host). Verified
  live: `anon` now only has `SELECT` on `mobile_catalog_view` + the small lookup tables.
- `picaloco_web` scaffolded at `work_picaloco_dev/picaloco_web/` (Vite + React + TS + Tailwind 4 +
  react-router + supabase-js + react-query) — not git-initialized yet, not deployed yet. Full
  search/browse/detail flow built and type-checks/builds clean; **not yet visually verified in a
  browser** — this sandbox has no usable Chrome for the Playwright MCP tool (needs `sudo` to install
  at a fixed path), so the user needs to click through it themselves (`npm run dev`). Two
  infra-changing commands (the `tailscale funnel` call and the `REVOKE`) were blocked by Claude
  Code's auto-mode safety classifier when attempted directly (SSH / direct DB write) — the user ran
  both manually instead, which is now the established pattern for this kind of change here.
- Note for future sessions: `REVOKE`/`GRANT`/`CREATE VIEW` DDL against live `oci` and any command
  that changes public network exposure (Funnel) reliably get blocked when attempted directly from a
  Claude Code session — plan to hand the exact command to the user rather than expect to run it.
- **Shipped and user-confirmed working live**, same day: git-initialized, pushed to
  `github.com/blueairblob/picaloco_web` (public), deployed to Vercel
  (`https://picaloco-web.vercel.app`, token-auth'd CLI deploy — GitHub auto-connect during `vercel
  link` failed silently, so it's a one-off deploy, not yet auto-deploying on push). User's own
  screenshot of the live site: searching `ab000` correctly returned 6 real archive photos with real
  thumbnails rendering (`ab0002` "Kingham", `ab0006` "Ruabon 1962-09-29", etc.) — the first real
  end-to-end proof, not just isolated API checks. `picaloco_web` Session 1 (its own devlog, if it
  gets one) starts from here.
- [ ] *(Carried, unchanged, `filemaker_sync`-side)* everything already open as of Session 13: `--mode
  dml_files` parser rewrite; GUI target-profile picker; `picture_metadata` untested against real images;
  `requirements.txt`'s `pandas==2.1.4` pin; the 16 flagged source records; `PicaLocoBackend`/`picaloco`
  rebrand (still gated on stability, unaffected by this session's app-layer work).

**Final update, same session — real-browser verification, one more bug found and fixed, and a
DevOps manual written:** once a working Chrome was available for browser automation (`npx
playwright install chrome`, run by the user — not `sudo npx ...`, which breaks nvm's PATH under
`sudo`'s restricted `secure_path`), the live site was click-tested directly rather than just via
curl. Found and fixed one real, plan-flagged bug: direct deep-links (e.g. sharing a link to
`/photo/ab0002`, a fresh load rather than in-app navigation) 404'd on Vercel — a Vite SPA needs an
explicit rewrite-to-`index.html` rule that wasn't present; added `picaloco_web/vercel.json`, pushed,
reverified fixed. Found and **deferred** (not blocking, search unaffected): the `Location`/
`Organisation`/`Route` facet-filter dropdowns are silently capped at ~1,000 options each by
PostgREST's default row limit, against real counts of ~14,178/~1,520/~2,874 — `Category`/`Country`/
`Collection`/`Photographer` are all small enough to be unaffected. Also wrote `picaloco_web/DEVOPS.md`,
a comprehensive from-scratch rebuild/redeploy manual (system map across all three Supabase projects,
the exact view/grants/Funnel SQL and commands, Vercel deploy gotchas actually hit, a troubleshooting
table) — written deliberately secret-free since that repo is public.

**`picaloco_web` is now live, git-connected for auto-deploy, and user-verified working**:
https://picaloco-web.vercel.app. `filemaker_sync` itself had zero code changes this session — all
work was on the shared `oci` database (additive view + tightened grants) and in the new sibling
repo.

---
