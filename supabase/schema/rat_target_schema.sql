-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create rat_migration schema
CREATE SCHEMA IF NOT EXISTS rat;

-- A function for updating the modified_date and modified_by columns
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.modified_date = CURRENT_TIMESTAMP;
    NEW.modified_by = auth.uid();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Create country table
CREATE TABLE rat.country (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(100) NOT NULL UNIQUE,
    created_by UUID NOT NULL,
    created_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_by UUID,
    modified_date TIMESTAMP WITH TIME ZONE
);

-- Create Organisation table
CREATE TABLE rat.organisation (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(100),
    type VARCHAR(50),
    country_id UUID,
    created_by UUID NOT NULL,
    created_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_by UUID,
    modified_date TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (country_id) REFERENCES rat.country(id)
);

-- Create location table
--ALTER TABLE rat.location ADD CONSTRAINT location_name_key UNIQUE (name);
CREATE TABLE rat.location (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(100) UNIQUE NOT NULL,
    country_id UUID,
    created_by UUID NOT NULL,
    created_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_by UUID,
    modified_date TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (country_id) REFERENCES rat.country(id)
);

-- Create Route table
CREATE TABLE rat.route (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(100) NOT NULL,
    start_location_id UUID,
    end_location_id UUID,
    created_by UUID NOT NULL,
    created_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_by UUID,
    modified_date TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (start_location_id) REFERENCES rat.location(id),
    FOREIGN KEY (end_location_id) REFERENCES rat.location(id)
);

-- Create collection table
CREATE TABLE rat.collection (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(100) NOT NULL,
    owner VARCHAR(100),
    donor VARCHAR(100),
    storage_location VARCHAR(100),
    created_by UUID NOT NULL,
    created_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_by UUID,
    modified_date TIMESTAMP WITH TIME ZONE
);

-- Create Photographer table
CREATE TABLE rat.photographer (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(100) NOT NULL,
    created_by UUID NOT NULL,
    created_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_by UUID,
    modified_date TIMESTAMP WITH TIME ZONE
);

-- Create Builder table
CREATE TABLE rat.builder (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    code VARCHAR(20) NOT NULL,
    name VARCHAR(100) NOT NULL,
    location VARCHAR(100),
    created_by UUID NOT NULL,
    created_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_by UUID,
    modified_date TIMESTAMP WITH TIME ZONE
);

-- Create Catalog table
CREATE TABLE rat.catalog (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    image_no VARCHAR(50) UNIQUE,
    accession_no DECIMAL(10,2),
    category VARCHAR(50),
    date_taken DATE,
    circa VARCHAR(50),
    imprecise_date VARCHAR(50),
    description TEXT,
    condition VARCHAR(50),
    valuation DECIMAL(10,2),
    entry_date DATE,
    owners_ref VARCHAR(100),
    cd_no VARCHAR(50),
    cd_no_hr VARCHAR(50),
    BW_image_no VARCHAR(50),
    bw_cd_no VARCHAR(50),
    gauge VARCHAR(20),
    works_number VARCHAR(50),
    year_built VARCHAR(20),
    plant_code VARCHAR(20),
    picture VARCHAR(255),
    active_area VARCHAR(100),
    corporate_body VARCHAR(100),
    facility VARCHAR(100),
    parent_folder VARCHAR(255),
    imgref_stem VARCHAR(255),
    website VARCHAR(255),
    created_by UUID NOT NULL,
    created_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_by UUID,
    modified_date TIMESTAMP WITH TIME ZONE
);

-- Create CatalogMetadata table
CREATE TABLE rat.catalog_metadata (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    catalog_id UUID,
    organisation_id UUID,
    location_id UUID,
    route_id UUID,
    collection_id UUID,
    photographer_id UUID,
    created_by UUID NOT NULL,
    created_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_by UUID,
    modified_date TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (catalog_id) REFERENCES rat.catalog(id),
    FOREIGN KEY (organisation_id) REFERENCES rat.organisation(id),
    FOREIGN KEY (location_id) REFERENCES rat.location(id),
    FOREIGN KEY (route_id) REFERENCES rat.route(id),
    FOREIGN KEY (collection_id) REFERENCES rat.collection(id),
    FOREIGN KEY (photographer_id) REFERENCES rat.photographer(id)
);

-- Create Usage table
CREATE TABLE rat.usage (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    catalog_id UUID,
    prints_allowed BOOLEAN,
    internet_use BOOLEAN,
    publications_use BOOLEAN,
    created_by UUID NOT NULL,
    created_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_by UUID,
    modified_date TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (catalog_id) REFERENCES rat.catalog(id)
);

-- Create CatalogBuilder table
CREATE TABLE rat.catalog_builder (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    catalog_id UUID,
    builder_id UUID,
    builder_order INT,
    created_by UUID NOT NULL,
    created_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_by UUID,
    modified_date TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (catalog_id) REFERENCES rat.catalog(id),
    FOREIGN KEY (builder_id) REFERENCES rat.builder(id)
);

-- Create PictureMetadata table
CREATE TABLE rat.picture_metadata (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    image_no VARCHAR(50) UNIQUE NOT NULL,
    file_location VARCHAR(255),
    file_type VARCHAR(50),
    file_size BIGINT,
    width INTEGER,
    height INTEGER,
    resolution VARCHAR(50),
    color_space VARCHAR(50),
    ai_description TEXT,
    tags TEXT[],
    created_by UUID NOT NULL,
    created_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_by UUID,
    modified_date TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (image_no) REFERENCES rat.catalog(image_no)
);

-- Add constraints
ALTER TABLE rat.catalog ADD CONSTRAINT check_valuation CHECK (valuation >= 0);
ALTER TABLE rat.catalog ADD CONSTRAINT check_entry_date CHECK (entry_date <= CURRENT_DATE);

-- Create triggers for updating modified columns
CREATE TRIGGER update_country_modtime BEFORE UPDATE ON rat.country FOR EACH ROW EXECUTE FUNCTION update_modified_column();
CREATE TRIGGER update_organisation_modtime BEFORE UPDATE ON rat.organisation FOR EACH ROW EXECUTE FUNCTION update_modified_column();
CREATE TRIGGER update_location_modtime BEFORE UPDATE ON rat.location FOR EACH ROW EXECUTE FUNCTION update_modified_column();
CREATE TRIGGER update_route_modtime BEFORE UPDATE ON rat.route FOR EACH ROW EXECUTE FUNCTION update_modified_column();
CREATE TRIGGER update_collection_modtime BEFORE UPDATE ON rat.collection FOR EACH ROW EXECUTE FUNCTION update_modified_column();
CREATE TRIGGER update_photographer_modtime BEFORE UPDATE ON rat.photographer FOR EACH ROW EXECUTE FUNCTION update_modified_column();
CREATE TRIGGER update_builder_modtime BEFORE UPDATE ON rat.builder FOR EACH ROW EXECUTE FUNCTION update_modified_column();
CREATE TRIGGER update_catalog_modtime BEFORE UPDATE ON rat.catalog FOR EACH ROW EXECUTE FUNCTION update_modified_column();
CREATE TRIGGER update_catalog_metadata_modtime BEFORE UPDATE ON rat.catalog_metadata FOR EACH ROW EXECUTE FUNCTION update_modified_column();
CREATE TRIGGER update_usage_modtime BEFORE UPDATE ON rat.usage FOR EACH ROW EXECUTE FUNCTION update_modified_column();
CREATE TRIGGER update_catalog_builder_modtime BEFORE UPDATE ON rat.catalog_builder FOR EACH ROW EXECUTE FUNCTION update_modified_column();
CREATE TRIGGER update_picture_metadata_modtime BEFORE UPDATE ON rat.picture_metadata FOR EACH ROW EXECUTE FUNCTION update_modified_column();

-- Enable Row Level Security (RLS) on all tables
ALTER TABLE rat.country ENABLE ROW LEVEL SECURITY;
ALTER TABLE rat.organisation ENABLE ROW LEVEL SECURITY;
ALTER TABLE rat.location ENABLE ROW LEVEL SECURITY;
ALTER TABLE rat.route ENABLE ROW LEVEL SECURITY;
ALTER TABLE rat.collection ENABLE ROW LEVEL SECURITY;
ALTER TABLE rat.photographer ENABLE ROW LEVEL SECURITY;
ALTER TABLE rat.builder ENABLE ROW LEVEL SECURITY;
ALTER TABLE rat.catalog ENABLE ROW LEVEL SECURITY;
ALTER TABLE rat.catalog_metadata ENABLE ROW LEVEL SECURITY;
ALTER TABLE rat.usage ENABLE ROW LEVEL SECURITY;
ALTER TABLE rat.catalog_builder ENABLE ROW LEVEL SECURITY;
ALTER TABLE rat.picture_metadata ENABLE ROW LEVEL SECURITY;

-- Create basic RLS policies (you may want to adjust these based on your specific requirements)
CREATE POLICY "Users can view all records" ON rat.country FOR SELECT USING (true);
CREATE POLICY "Users can view all records" ON rat.organisation FOR SELECT USING (true);
CREATE POLICY "Users can view all records" ON rat.location FOR SELECT USING (true);
CREATE POLICY "Users can view all records" ON rat.route FOR SELECT USING (true);
CREATE POLICY "Users can view all records" ON rat.collection FOR SELECT USING (true);
CREATE POLICY "Users can view all records" ON rat.photographer FOR SELECT USING (true);
CREATE POLICY "Users can view all records" ON rat.builder FOR SELECT USING (true);
CREATE POLICY "Users can view all records" ON rat.catalog FOR SELECT USING (true);
CREATE POLICY "Users can view all records" ON rat.catalog_metadata FOR SELECT USING (true);
CREATE POLICY "Users can view all records" ON rat.usage FOR SELECT USING (true);
CREATE POLICY "Users can view all records" ON rat.catalog_builder FOR SELECT USING (true);
CREATE POLICY "Users can view all records" ON rat.picture_metadata FOR SELECT USING (true);

-- Example of a more restrictive policy (uncomment and modify as needed)
-- CREATE POLICY "Users can update their own records" ON catalog FOR UPDATE USING (auth.uid() = created_by);