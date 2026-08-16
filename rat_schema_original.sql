-- WARNING: This schema is for context only and is not meant to be run.
-- Table order and constraints may not be valid for execution.

CREATE TABLE rat.builder (
  id uuid NOT NULL DEFAULT uuid_generate_v4(),
  code character varying NOT NULL UNIQUE,
  name character varying,
  location_id uuid,
  plant_code character varying,
  builder_plant character varying,
  remarks character varying,
  created_by uuid NOT NULL,
  created_date timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_by uuid,
  modified_date timestamp with time zone,
  CONSTRAINT builder_pkey PRIMARY KEY (id),
  CONSTRAINT builder_location_id_fkey FOREIGN KEY (location_id) REFERENCES rat.location(id)
);
CREATE TABLE rat.catalog (
  id uuid NOT NULL DEFAULT uuid_generate_v4(),
  image_no character varying NOT NULL UNIQUE,
  accession_no numeric,
  category character varying,
  date_taken date,
  circa character varying,
  imprecise_date character varying,
  description text,
  condition character varying,
  valuation numeric CHECK (valuation >= 0::numeric),
  entry_date date CHECK (entry_date <= CURRENT_DATE),
  owners_ref character varying,
  cd_no character varying,
  cd_no_hr character varying,
  bw_image_no character varying,
  bw_cd_no character varying,
  gauge character varying,
  works_number character varying,
  year_built character varying,
  plant_code character varying,
  picture character varying,
  active_area character varying,
  corporate_body character varying,
  facility character varying,
  parent_folder character varying,
  imgref_stem character varying,
  website character varying,
  created_by uuid NOT NULL,
  created_date timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_by uuid,
  modified_date timestamp with time zone,
  CONSTRAINT catalog_pkey PRIMARY KEY (id)
);
CREATE TABLE rat.catalog_builder (
  id uuid NOT NULL DEFAULT uuid_generate_v4(),
  catalog_id uuid,
  builder_id uuid,
  builder_order integer NOT NULL,
  plant_code character varying,
  works_number character varying,
  year_built character varying,
  created_by uuid NOT NULL,
  created_date timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_by uuid,
  modified_date timestamp with time zone,
  CONSTRAINT catalog_builder_pkey PRIMARY KEY (id),
  CONSTRAINT catalog_builder_builder_id_fkey FOREIGN KEY (builder_id) REFERENCES rat.builder(id),
  CONSTRAINT catalog_builder_catalog_id_fkey FOREIGN KEY (catalog_id) REFERENCES rat.catalog(id)
);
CREATE TABLE rat.catalog_metadata (
  id uuid NOT NULL DEFAULT uuid_generate_v4(),
  catalog_id uuid,
  organisation_id uuid,
  location_id uuid,
  route_id uuid,
  collection_id uuid,
  photographer_id uuid,
  created_by uuid NOT NULL,
  created_date timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_by uuid,
  modified_date timestamp with time zone,
  CONSTRAINT catalog_metadata_pkey PRIMARY KEY (id),
  CONSTRAINT catalog_metadata_route_id_fkey FOREIGN KEY (route_id) REFERENCES rat.route(id),
  CONSTRAINT catalog_metadata_catalog_id_fkey FOREIGN KEY (catalog_id) REFERENCES rat.catalog(id),
  CONSTRAINT catalog_metadata_collection_id_fkey FOREIGN KEY (collection_id) REFERENCES rat.collection(id),
  CONSTRAINT catalog_metadata_location_id_fkey FOREIGN KEY (location_id) REFERENCES rat.location(id),
  CONSTRAINT catalog_metadata_organisation_id_fkey FOREIGN KEY (organisation_id) REFERENCES rat.organisation(id),
  CONSTRAINT catalog_metadata_photographer_id_fkey FOREIGN KEY (photographer_id) REFERENCES rat.photographer(id)
);
CREATE TABLE rat.collection (
  id uuid NOT NULL DEFAULT uuid_generate_v4(),
  name character varying NOT NULL UNIQUE,
  owner character varying,
  donor character varying,
  storage_location character varying,
  created_by uuid NOT NULL,
  created_date timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_by uuid,
  modified_date timestamp with time zone,
  CONSTRAINT collection_pkey PRIMARY KEY (id)
);
CREATE TABLE rat.country (
  id uuid NOT NULL DEFAULT uuid_generate_v4(),
  name character varying NOT NULL UNIQUE,
  created_by uuid NOT NULL,
  created_date timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_by uuid,
  modified_date timestamp with time zone,
  CONSTRAINT country_pkey PRIMARY KEY (id)
);
CREATE TABLE rat.location (
  id uuid NOT NULL DEFAULT uuid_generate_v4(),
  name character varying NOT NULL UNIQUE,
  country_id uuid,
  created_by uuid NOT NULL,
  created_date timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_by uuid,
  modified_date timestamp with time zone,
  CONSTRAINT location_pkey PRIMARY KEY (id),
  CONSTRAINT location_country_id_fkey FOREIGN KEY (country_id) REFERENCES rat.country(id)
);
CREATE TABLE rat.organisation (
  id uuid NOT NULL DEFAULT uuid_generate_v4(),
  name character varying UNIQUE,
  type character varying,
  country_id uuid,
  created_by uuid NOT NULL,
  created_date timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_by uuid,
  modified_date timestamp with time zone,
  CONSTRAINT organisation_pkey PRIMARY KEY (id),
  CONSTRAINT organisation_country_id_fkey FOREIGN KEY (country_id) REFERENCES rat.country(id)
);
CREATE TABLE rat.photographer (
  id uuid NOT NULL DEFAULT uuid_generate_v4(),
  name character varying NOT NULL UNIQUE,
  created_by uuid NOT NULL,
  created_date timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_by uuid,
  modified_date timestamp with time zone,
  CONSTRAINT photographer_pkey PRIMARY KEY (id)
);
CREATE TABLE rat.picture_metadata (
  id uuid NOT NULL DEFAULT uuid_generate_v4(),
  catalog_id uuid UNIQUE,
  file_name character varying,
  file_location character varying,
  file_type character varying,
  file_size bigint,
  width integer,
  height integer,
  resolution character varying,
  colour_space character varying CHECK (colour_space IS NULL OR (colour_space::text = ANY (ARRAY['sRGB'::character varying::text, 'Adobe RGB'::character varying::text, 'ProPhoto RGB'::character varying::text, 'CMYK'::character varying::text, 'LAB'::character varying::text, 'Grayscale'::character varying::text]))),
  colour_mode character varying CHECK (colour_mode IS NULL OR (colour_mode::text = ANY (ARRAY['colour'::character varying::text, 'black_and_white'::character varying::text, 'grayscale'::character varying::text, 'sepia'::character varying::text]))),
  ai_description text,
  tags ARRAY,
  created_by uuid NOT NULL,
  created_date timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_by uuid,
  modified_date timestamp with time zone,
  CONSTRAINT picture_metadata_pkey PRIMARY KEY (id),
  CONSTRAINT picture_metadata_catalog_id_fkey FOREIGN KEY (catalog_id) REFERENCES rat.catalog(id),
  CONSTRAINT picture_metadata_catalog_id_fkey1 FOREIGN KEY (catalog_id) REFERENCES rat.catalog(id)
);
CREATE TABLE rat.route (
  id uuid NOT NULL DEFAULT uuid_generate_v4(),
  name character varying NOT NULL UNIQUE,
  start_location_id uuid,
  end_location_id uuid,
  created_by uuid NOT NULL,
  created_date timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_by uuid,
  modified_date timestamp with time zone,
  CONSTRAINT route_pkey PRIMARY KEY (id),
  CONSTRAINT route_start_location_id_fkey FOREIGN KEY (start_location_id) REFERENCES rat.location(id),
  CONSTRAINT route_end_location_id_fkey FOREIGN KEY (end_location_id) REFERENCES rat.location(id)
);
CREATE TABLE rat.usage (
  catalog_id uuid UNIQUE,
  prints_allowed boolean,
  internet_use boolean,
  publications_use boolean,
  created_by uuid NOT NULL,
  created_date timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_by uuid,
  modified_date timestamp with time zone,
  CONSTRAINT usage_catalog_id_fkey FOREIGN KEY (catalog_id) REFERENCES rat.catalog(id)
);