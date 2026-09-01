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
- [ ] `ratcatalogue`'s Stage-1 export dominates runtime (~2 hours for 141k rows, individual-insert overhead in `export_data`'s staging-table DML path) — worth a batched-insert pass if `--db-exp` becomes a routine/frequent operation rather than a one-off bootstrap.
- [ ] *(Carried)* the 16 flagged source records (13 duplicate `image_no`, 3 null keys) — now precisely re-confirmed via the OCI manifest baseline, still FileMaker-side cleanup; DDR from the `.fmpur` copy; supabase-py vs SQLAlchemy prune; anon-JWT rotation when frontend ready; consolidation into `PicaLocoBackend` + rebrand once stable; delete the dead `sync_config.json`.

---
