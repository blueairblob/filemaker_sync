-- ============================================================================
-- fix_rat_builder_key.sql  — reconcile builder's unique key with the real loader
--
-- The real production loader conflicts `builder` on `code` (its natural key —
-- `builder.code` is already NOT NULL UNIQUE in the schema). The `builder_name_key`
-- constraint added in Session 3 was based on a wrong assumption (name as key) and
-- must be removed: `name` is nullable and is NOT the builder's identity.
--
-- Safe and idempotent. Run once in the Supabase SQL editor.
-- ============================================================================

-- Drop the mistaken UNIQUE(name) constraint (no-op if already gone).
ALTER TABLE rat.builder DROP CONSTRAINT IF EXISTS builder_name_key;

-- Verify: builder's real key (code) is present; the wrong one (name) is gone.
SELECT conname, pg_get_constraintdef(oid) AS definition
FROM pg_constraint
WHERE conrelid = 'rat.builder'::regclass
  AND contype IN ('u', 'p')
ORDER BY conname;
-- Expect a UNIQUE (code) constraint and NO builder_name_key.
