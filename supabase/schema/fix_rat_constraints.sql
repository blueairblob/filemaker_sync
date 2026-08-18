-- fix_rat_constraints.sql
-- Prerequisite DDL for the db_dml_loader.py catalog_metadata / usage fixes.
-- Run against the live `rat` schema BEFORE re-running the loader.
-- Safe to run more than once (guarded / IF [NOT] EXISTS).

-- ---------------------------------------------------------------------------
-- Fix 1: make catalog_metadata idempotent (1:1 with catalog)
-- The loader now upserts with ON CONFLICT (catalog_id). That conflict target
-- only works if catalog_id carries a UNIQUE constraint. usage.catalog_id and
-- picture_metadata.catalog_id already have one; catalog_metadata does not.
--
-- PRE-CHECK — if this returns any rows, dedupe them first or the ALTER fails:
--   SELECT catalog_id, count(*)
--   FROM rat.catalog_metadata
--   GROUP BY catalog_id
--   HAVING count(*) > 1;
-- ---------------------------------------------------------------------------
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'catalog_metadata_catalog_id_key'
      AND conrelid = 'rat.catalog_metadata'::regclass
  ) THEN
    ALTER TABLE rat.catalog_metadata
      ADD CONSTRAINT catalog_metadata_catalog_id_key UNIQUE (catalog_id);
  END IF;
END $$;

-- ---------------------------------------------------------------------------
-- Fix 2: remove the duplicate foreign key on picture_metadata.
-- The live schema has two identical FKs on catalog_id:
--   picture_metadata_catalog_id_fkey  and  picture_metadata_catalog_id_fkey1
-- Keep the first, drop the accidental duplicate.
-- ---------------------------------------------------------------------------
ALTER TABLE rat.picture_metadata
  DROP CONSTRAINT IF EXISTS picture_metadata_catalog_id_fkey1;
