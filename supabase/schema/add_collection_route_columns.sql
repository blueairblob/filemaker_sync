-- add_collection_route_columns.sql
--
-- Adds the rat.collection/rat.route columns found missing Session 23: Stage 1
-- already extracts these from FileMaker (rat_migration.ratcollections/
-- ratroutes), but the target schema never had columns for them, so Stage 2
-- never had anywhere to put them. See devlog/worksheet.md Session 23.
--
-- Idempotent (ADD COLUMN IF NOT EXISTS, guarded ADD CONSTRAINT) -- safe to
-- re-run, matches fix_rat_idempotency.sql's own convention.
--
-- rat.collection.contact holds real people's names/contact details (donors/
-- collectors) -- confirmed live, e.g. "[real name and full postal
-- address - redacted]". anon already has full SELECT on rat.collection today (same
-- as owner/donor, already public), so this makes contact public too.
-- Deliberate: user's explicit call, made with that fact in front of them --
-- see devlog/worksheet.md Session 23, not an oversight.
--
-- Run once against oci (psql or the Supabase SQL editor):
--     psql "$OCI_CONNECTION_STRING" -f supabase/schema/add_collection_route_columns.sql
-- Also folded into bootstrap_rat_schema.sql for fresh-install parity.

ALTER TABLE rat.route ADD COLUMN IF NOT EXISTS organisation_id uuid;
ALTER TABLE rat.route ADD COLUMN IF NOT EXISTS country_id uuid;
ALTER TABLE rat.route ADD COLUMN IF NOT EXISTS remarks text;

ALTER TABLE rat.collection ADD COLUMN IF NOT EXISTS photographer_id uuid;
ALTER TABLE rat.collection ADD COLUMN IF NOT EXISTS print_sales boolean;
ALTER TABLE rat.collection ADD COLUMN IF NOT EXISTS internet_use boolean;
ALTER TABLE rat.collection ADD COLUMN IF NOT EXISTS publications_use boolean;
ALTER TABLE rat.collection ADD COLUMN IF NOT EXISTS accession_number numeric;
ALTER TABLE rat.collection ADD COLUMN IF NOT EXISTS contact text;
ALTER TABLE rat.collection ADD COLUMN IF NOT EXISTS remarks text;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'route_organisation_id_fkey') THEN
        ALTER TABLE rat.route ADD CONSTRAINT route_organisation_id_fkey
            FOREIGN KEY (organisation_id) REFERENCES rat.organisation(id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'route_country_id_fkey') THEN
        ALTER TABLE rat.route ADD CONSTRAINT route_country_id_fkey
            FOREIGN KEY (country_id) REFERENCES rat.country(id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'collection_photographer_id_fkey') THEN
        ALTER TABLE rat.collection ADD CONSTRAINT collection_photographer_id_fkey
            FOREIGN KEY (photographer_id) REFERENCES rat.photographer(id);
    END IF;
END $$;
