-- Disable triggers to prevent any unexpected side effects
SET session_replication_role = 'replica';

-- Drop all tables in the RAT schema
DROP TABLE IF EXISTS 
    "rat.Country",
    "rat.Organisation",
    "rat.Location",
    "rat.Route",
    "rat.Collection",
    "rat.Photographer",
    "rat.Builder",
    "rat.Catalog",
    "rat.CatalogMetadata",
    "rat.Usage",
    "rat.CatalogBuilder",
    "rat.PictureMetadata"
CASCADE;

-- Drop any views in the RAT schema
DROP VIEW IF EXISTS rat.rat_view_1, rat.rat_view_2; -- Add any view names here

-- Drop any functions in the RAT schema
DROP FUNCTION IF EXISTS rat.update_modified_column() CASCADE;

-- Drop any custom types in the RAT schema
DROP TYPE IF EXISTS rat.custom_type_1, rat.custom_type_2; -- Add any custom type names here

-- Drop any sequences in the RAT schema
DROP SEQUENCE IF EXISTS rat.rat_sequence_1, rat.rat_sequence_2; -- Add any sequence names here

-- Drop the schema itself
DROP SCHEMA IF EXISTS public CASCADE;

-- Recreate the public schema
CREATE SCHEMA public;

-- Reset default privileges
ALTER DEFAULT PRIVILEGES REVOKE ALL ON TABLES FROM public;

-- Restore default privileges if needed
-- GRANT ALL ON SCHEMA public TO postgres;
-- GRANT ALL ON SCHEMA public TO public;

-- Re-enable triggers
SET session_replication_role = 'origin';

-- Notify of completion
DO $$
BEGIN
    RAISE NOTICE 'RAT schema and all its objects have been dropped and recreated.';
END $$;
