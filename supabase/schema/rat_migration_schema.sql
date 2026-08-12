-- Enable UUID extension if not already enabled
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create rat_migration schema
CREATE SCHEMA IF NOT EXISTS rat_migration;

-- Create users table
CREATE TABLE rat_migration.users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    username VARCHAR(255) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP WITH TIME ZONE
);


-- Create migration_log table
CREATE TABLE rat_migration.migration_log (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_name VARCHAR(255) NOT NULL UNIQUE,
    last_migrated_id VARCHAR(255),
    rows_migrated INTEGER,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE,
    created_by UUID REFERENCES rat_migration.users(id),
    status VARCHAR(50) CHECK (status IN ('started', 'completed', 'failed'))
);

-- Create a function to update the completed_at timestamp
CREATE OR REPLACE FUNCTION rat_migration.update_migration_completed_at()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.status = 'completed' AND OLD.status != 'completed' THEN
        NEW.completed_at = CURRENT_TIMESTAMP;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create a trigger to automatically update completed_at
CREATE TRIGGER update_migration_completed_at_trigger
BEFORE UPDATE ON rat_migration.migration_log
FOR EACH ROW
EXECUTE FUNCTION rat_migration.update_migration_completed_at();

-- Create a view to show the latest migration status for each table
CREATE OR REPLACE VIEW rat_migration.latest_migration_status AS
SELECT DISTINCT ON (table_name)
    table_name,
    last_migrated_id,
    rows_migrated,
    started_at,
    completed_at,
    created_by,
    status
FROM rat_migration.migration_log
ORDER BY table_name, started_at DESC;

-- Grant necessary permissions (adjust as needed based on your Supabase setup)
GRANT USAGE ON SCHEMA rat_migration TO authenticated, service_role;
GRANT ALL ON ALL TABLES IN SCHEMA rat_migration TO authenticated, service_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA rat_migration TO authenticated, service_role;
GRANT ALL ON ALL FUNCTIONS IN SCHEMA rat_migration TO authenticated, service_role;

-- Insert a default user (replace with actual user details)
INSERT INTO rat_migration.users (username, email)
VALUES ('migration_user', 'migration@example.com')
ON CONFLICT (username) DO NOTHING;


-- SELECT image_no COUNT(*)
-- FROM rat_migration.ratcatalogue
-- GROUP BY image_no
-- HAVING COUNT(*) > 1;

-- -- Check for dupes
-- select * from rat_migration.ratcatalogue where image_no in (
-- SELECT image_no FROM rat_migration.ratcatalogue GROUP BY image_no HAVING COUNT(*) > 1);