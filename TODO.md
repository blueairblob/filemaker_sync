# TODO

Parked work, with enough context to pick it up cold. Newest first.

---

## Loco number: extract it into its own field (extracted live 2026-11-14; web admin in progress)

**Status (2026-11-14):** Postgres side built and live. `add_data_fix_tables.sql` is applied on
`oci`, and `extract_loco_fields.py --apply` filled `rat.data_fix_value` with 282,488 rows: 141,244
photos × 2 fixes. A re-run changed nothing. The preservation baseline is `{"reviewed": 0}`.

| Fix | High | Med | Low | None |
|---|---|---|---|---|
| `loco_number` | 91,329 | 15,537 | 2,141 | 32,237 |
| `loco_name` | 14,594 | 4,918 | 1,324 | 120,408 |

Low quoted names (e.g. "Cambrian Coast Express") are kept by the user's decision. Remaining:
`picaloco_web` search on the new fields, the admin sign-in, the review queue and bulk confirm.
The plan is `picaloco_web/docs/superpowers/plans/2026-11-14-data-fixes-loco-number-name.md`
(Tasks 8–13). The history below is kept for context.

**Client response, 2026-11-14 (initial):**
- **They agree:** the data is messy, and a dedicated number field is the right fix.
- **Many numbering formats are valid.** Different railways used different sequences, so
  validation must not force a single format. Their format instructions may need to vary by
  railway.
- **Works numbers are in two places.** One person typed the works info into `description` as well
  as the works number field; this is the `ms*` collections' `built in … by …, Works No. N` pattern.
  The client wants it held in one place only. Open: does that mean removing it from
  `description`? Edits to `description` in Postgres would be overwritten by the next FileMaker
  sync, because the upsert is now `DO UPDATE`. They would have to be made in FileMaker, or held as
  an overlay in Postgres.
- **New: a "Loco Name" filter**, also extracted from `description` (e.g. `60103 Flying Scotsman`,
  `673 Maude`, `0-6-0T Doll`). This becomes Data Fix #2.
- **Sequence:** fix the data **before** the search is launched, i.e. before the link from
  ratrust.org's menu goes live. The stopgap filter stays in place until then.
- **The client wants to work through the extracts personally** to weed out invalid data. That
  answers who reviews, at least at first. Open: spreadsheets now, or wait for the Admin page? And
  does "weed out" mean every record, or only the anomaly buckets?

**Stopgap in place:** `picaloco_web`'s Loco number filter (commit `e4cd49d`, live) searches
`description` for the typed value as a whole token: a PostgREST `imatch` with
`(^|[^[:alnum:]])<value>([^[:alnum:]]|$)`, about 0.25s on the live view. Known false positives:
years, train times (`3.20`), and works numbers that share the digits. About 24% of descriptions
carry no loco number, so those photos can't be found this way.

**Source-side plan (suggested to the client 2026-11-14):** RAT add a "Loco number(s)" field to
the FileMaker entry form. Not confirmed yet. When it lands:
- Stage 1 picks the new column up automatically into `rat_migration.ratcatalogue`. Check its
  exact FileMaker name.
- Stage 2 won't copy it until `rat.catalog` has a matching column, or `CATALOG_COLUMN_RENAME`
  maps it. That's the same dead-column trap `works_number`/`year_built` fell into in Session 23.
- A photo can show several locos, and one loco can carry several identities. Decide whether the
  field holds a list, and how volunteers separate entries.
- `mobile_catalog_view` needs the column added, and `anon` grants re-checked.
- Switch the web filter from the description search to the real field. Possibly keep the
  description search as a fallback for records nobody has backfilled yet.

**Direction changed 2026-11-14: Postgres only, fixed through the web app.** The corrected loco
numbers will live in Supabase/Postgres, not FileMaker. The FileMaker entry form will not change; the
client will issue format instructions instead. Volunteers will review and correct the numbers on an
Admin → "Data Fixes" page in `picaloco_web`, and Loco number is the first fix on that page. Not
every photo will have a number: a volunteer has to identify it from the photo, and sometimes that
can't be done. Because of this, the data must live in its own table, never in `rat.catalog`, so
the FileMaker → Postgres upsert can never overwrite it. Design written (awaiting approval):
`picaloco_web/docs/superpowers/specs/2026-11-14-data-fixes-loco-number-name-design.md`.

**Dry run, 2026-11-14 (read-only; all 141,244 `mobile_catalog_view` rows; scratch CSV only):**

| Bucket | Rows | % |
|---|---|---|
| High: one marked number | 83,676 | 59.2 |
| High, but a `Works No.` is also present (excluded from the candidate) | 7,282 | 5.2 |
| Review: several locos (`numbers 1413 and 1412`) | 7,242 | 5.1 |
| Review: renumbering (`number 130, Urquiza no. 290`) | 7,366 | 5.2 |
| Review: odd format (`51339/51382`) | 41 | 0.0 |
| Suggest: unmarked number found (`Garratt 4041`) | 3,287 | 2.3 |
| Photo: loco mentioned, no number | 14,986 | 10.6 |
| Photo: no loco word (stations, scenery, plus some locos the regex missed) | 17,251 | 12.2 |
| Empty description | 113 | 0.1 |

Parser gaps found in the high buckets, all fixable before anything is built:
- **2,693 space-separated numbers are cut short.** Examples: `No. 99 7238` is read as `99`, and
  `number 73 129` as `73`. This is the European style (DR/CSD/OBB/CFR).
- **About 979 photo-bucket rows use a marker the regex missed.** Examples: `number. 2683`, `No, E113`,
  and a letter prefix after a space, as in `number DT 601` or `KDB 951455`.
- **Scope question for the client:** about 5,600 high rows are trams, cars, railcars, units or wagons
  (`Car number 19`). Does "loco number" include these?
- Year-like numbers (849 rows, e.g. `number 1882`) checked: they are real loco numbers, so not a
  problem.

Collection matters a lot. `arc` and `gwr` are about 50–59% photo-inspection, while
`jdmn`/`lwp`/`pr` are 80%+ high.

**Backfill from descriptions:** extract existing numbers to pre-fill the new field, or a derived
table. Findings from a read-only scan of all 141,131 non-empty `rat.catalog.description` values
(2026-11-14):
- **Main pattern:** 65% use "number X" (e.g. `Ex LMS class 4 2-6-4T number 42616 stands…`).
  76% carry some marker: `number`, `numbers`, `No.`, `no.` or `nos.`.
- **Style varies by collection** (`image_no` prefix): `msus`/`mscu` use "No." about 90% of the
  time. `arc`, `gwr` and `bpuk` have no marker in 50–60% of rows.
- **Several per description:** 12,732 rows (9%) name two or more locos (`numbers 1414 and 1409`,
  `D1679 & D1670 … number 3675`).
- **Renumbering:** one loco can have two identities (`number 46, Fecosa no. 59`,
  `number 4424, SNCF no. 141TB.424`). Lowercase "no." usually marks the later identity.
- **Works numbers, not loco numbers:** `Works No. X` in 9,796 rows (6.9%), and `(2247/1948)` in
  about 507. Exclude both.
- **Token formats vary:** `42616`, `E113`, `52.7034`, `141TB.424`, `Ty2 953`, `HF191`, `W31`,
  `251-86`.
- **Loco mentioned but no number:** 14,481 rows. These are named locos (`0-6-0T Doll`), generic
  ones (`A class 15F`) and multiple units (`class 120 DMU`).
- **Typos exist:** e.g. `483991` for a 5-digit LMS number. Quarantine doubtful values for
  review; never guess (Golden Rule #1).
- **Design direction so far:** derived and regenerable, never rewriting `description`. Store one
  row per number, plus the marker it came from and the source phrase. Send ambiguous cases to
  `rat_migration.reject_log`.

---

## Builder code / Works number web filters need a view change

Removed from `picaloco_web`'s filter picker (commit `e4cd49d`). They always returned HTTP 400
and were never on screen. Cause: `mobile_catalog_view.builders` is `jsonb[]`
(`array_agg(jsonb_build_object(...))`), and PostgREST can't partially match elements inside it.
Fix: add flat columns to the view, e.g. `builder_codes text[]` and `works_numbers text[]`, then
filter with `cs.{…}`. DDL on `oci` gets auto-blocked, so hand the SQL to the user. Do this
alongside the loco-number work above, since "loco number" and "works number" are easy to confuse.
