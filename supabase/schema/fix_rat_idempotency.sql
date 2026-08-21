-- ============================================================================
-- fix_rat_idempotency.sql
-- Prerequisite constraints for the loader idempotency patch
-- (fix_rat_idempotency_loader.patch). Adds the UNIQUE keys the loader's new
-- ON CONFLICT targets need, so re-running the loader SKIPS existing rows instead
-- of inserting duplicates.
--
-- ORDER OF OPERATIONS:
--   1. Run STEP 1 (read-only). Confirm the duplicate counts are all 0.
--   2. If clean, run STEP 2 to add the constraints.
--   3. THEN apply the loader patch and re-run.
--   Do NOT run STEP 2 for any table STEP 1 flags with duplicates — send me the
--   numbers instead (a duplicate may be referenced by a foreign key, so it can't
--   just be deleted).
--
-- Scope: collection / photographer / builder (UNIQUE name) and catalog_builder
-- (UNIQUE catalog_id, builder_id, builder_order). picture_metadata is intentionally EXCLUDED —
-- it has ~354 extra rows (duplicate/orphan catalog_id); STEP 1 reports them but
-- this script does NOT constrain or delete it (report-only, per decision).
-- ============================================================================


-- STEP 1 — PRE-CHECK (read-only). Every "duplicate_groups" should be 0 for the
-- four tables we're about to constrain. The picture_metadata rows are reported
-- for information only (that table is not touched here).
SELECT 'collection.name (dupe groups)'              AS check_item,
       count(*) AS n FROM (SELECT name FROM rat.collection   GROUP BY name HAVING count(*) > 1) d
UNION ALL
SELECT 'photographer.name (dupe groups)',
       count(*) FROM (SELECT name FROM rat.photographer      GROUP BY name HAVING count(*) > 1) d
UNION ALL
SELECT 'builder.name (dupe groups)',
       count(*) FROM (SELECT name FROM rat.builder           GROUP BY name HAVING count(*) > 1) d
UNION ALL
SELECT 'catalog_builder.(catalog_id,builder_id,builder_order) (dupe groups)',
       count(*) FROM (SELECT catalog_id, builder_id, builder_order FROM rat.catalog_builder
                      GROUP BY catalog_id, builder_id, builder_order HAVING count(*) > 1) d
UNION ALL
SELECT 'picture_metadata.catalog_id (dupe groups) [report only]',
       count(*) FROM (SELECT catalog_id FROM rat.picture_metadata
                      WHERE catalog_id IS NOT NULL
                      GROUP BY catalog_id HAVING count(*) > 1) d
UNION ALL
SELECT 'picture_metadata orphan catalog_id (no catalog) [report only]',
       count(*) FROM rat.picture_metadata p
       WHERE p.catalog_id IS NOT NULL
         AND NOT EXISTS (SELECT 1 FROM rat.catalog c WHERE c.id = p.catalog_id)
UNION ALL
SELECT 'picture_metadata NULL catalog_id [report only]',
       count(*) FROM rat.picture_metadata WHERE catalog_id IS NULL
ORDER BY check_item;


-- STEP 2 — ADD CONSTRAINTS (run only after STEP 1 confirms 0 duplicates for
-- these four). Guarded and idempotent: safe to re-run.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'collection_name_key') THEN
        ALTER TABLE rat.collection   ADD CONSTRAINT collection_name_key   UNIQUE (name);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'photographer_name_key') THEN
        ALTER TABLE rat.photographer ADD CONSTRAINT photographer_name_key UNIQUE (name);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'builder_name_key') THEN
        ALTER TABLE rat.builder      ADD CONSTRAINT builder_name_key      UNIQUE (name);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'catalog_builder_catalog_id_builder_id_builder_order_key') THEN
        ALTER TABLE rat.catalog_builder
            ADD CONSTRAINT catalog_builder_catalog_id_builder_id_builder_order_key
            UNIQUE (catalog_id, builder_id, builder_order);
    END IF;
END $$;


-- STEP 3 — VERIFY (read-only). Confirm the four constraints now exist.
SELECT conname, conrelid::regclass AS table_name
FROM pg_constraint
WHERE conname IN ('collection_name_key', 'photographer_name_key',
                  'builder_name_key', 'catalog_builder_catalog_id_builder_id_builder_order_key')
ORDER BY conname;
