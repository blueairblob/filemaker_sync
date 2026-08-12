-- Migrate data from ratroutes to Route
INSERT INTO Route (name, start_location_id, end_location_id)
SELECT 
    r.route,
    (SELECT location_id FROM Location WHERE name = r.start_location LIMIT 1),
    (SELECT location_id FROM Location WHERE name = r.end_location LIMIT 1)
FROM ratroutes r;

-- Migrate data from ratcollections to Collection
INSERT INTO Collection (name, owner, donor, storage_location)
SELECT collection, owner, donor, storage_location
FROM ratcollections;

-- Migrate data from ratbuilders to Builder
INSERT INTO Builder (code, name, location)
SELECT "Builder code", "Builder name", Location
FROM ratbuilders;

-- Migrate data from ratcatalogue to Catalog and related tables
INSERT INTO Catalog (
    image_no, accession_no, category, date_taken, condition, valuation, 
    entry_date, owners_ref, cd_no, description, picture, gauge, 
    works_number, year_built, plant_code, parent_folder, imgref_stem, website
)
SELECT 
    image_no, accession_no, category, date_taken, `condition`, valuation,
    entry_date, owners_ref, cd_no, description, picture, gauge,
    "Works number", "Year built", "Plant code", parent_folder, imgref_stem, website
FROM ratcatalogue;

-- Migrate data from images to PictureMetadata
INSERT INTO PictureMetadata (image_no, file_location)
SELECT image_no, picture
FROM images;

-- Migrate data from ratcopyright to Usage
INSERT INTO Usage (catalog_id, prints_allowed, internet_use, publications_use)
SELECT 
    (SELECT catalog_id FROM Catalog WHERE image_no = rc.image_no LIMIT 1),
    CASE WHEN rc.prints_allowed = 'Yes' THEN TRUE ELSE FALSE END,
    CASE WHEN rc.internet_use = 'Yes' THEN TRUE ELSE FALSE END,
    CASE WHEN rc.publications_use = 'Yes' THEN TRUE ELSE FALSE END
FROM ratcatalogue rc;

-- Populate Country table
INSERT INTO Country (name)
SELECT DISTINCT country FROM ratroutes
WHERE country IS NOT NULL AND country != '';

-- Populate Organisation table
INSERT INTO Organisation (name, type, country_id)
SELECT DISTINCT 
    r.organisation, 
    rc.organisation_type,
    (SELECT country_id FROM Country WHERE name = r.country LIMIT 1)
FROM ratroutes r
LEFT JOIN ratcatalogue rc ON r.organisation = rc.organisation
WHERE r.organisation IS NOT NULL AND r.organisation != '';

-- Populate Location table
INSERT INTO Location (name, country_id)
SELECT DISTINCT start_location, 
    (SELECT country_id FROM Country WHERE name = country LIMIT 1)
FROM ratroutes
WHERE start_location IS NOT NULL AND start_location != ''
UNION
SELECT DISTINCT end_location, 
    (SELECT country_id FROM Country WHERE name = country LIMIT 1)
FROM ratroutes
WHERE end_location IS NOT NULL AND end_location != '';

-- Populate Photographer table
INSERT INTO Photographer (name)
SELECT DISTINCT photographer
FROM ratcatalogue
WHERE photographer IS NOT NULL AND photographer != '';

-- Populate CatalogMetadata table
INSERT INTO CatalogMetadata (
    catalog_id, organisation_id, location_id, route_id, collection_id, photographer_id
)
SELECT 
    c.catalog_id,
    (SELECT organisation_id FROM Organisation WHERE name = rc.organisation LIMIT 1),
    (SELECT location_id FROM Location WHERE name = rc.location LIMIT 1),
    (SELECT route_id FROM Route WHERE name = rc.route LIMIT 1),
    (SELECT collection_id FROM Collection WHERE name = rc.collection LIMIT 1),
    (SELECT photographer_id FROM Photographer WHERE name = rc.photographer LIMIT 1)
FROM Catalog c
JOIN ratcatalogue rc ON c.image_no = rc.image_no;

-- Populate CatalogBuilder table
INSERT INTO CatalogBuilder (catalog_id, builder_id, builder_order)
SELECT 
    c.catalog_id,
    (SELECT builder_id FROM Builder WHERE code = rc."Builder code" LIMIT 1),
    1
FROM Catalog c
JOIN ratcatalogue rc ON c.image_no = rc.image_no
WHERE rc."Builder code" IS NOT NULL AND rc."Builder code" != '';

-- Handle additional builders if present
INSERT INTO CatalogBuilder (catalog_id, builder_id, builder_order)
SELECT 
    c.catalog_id,
    (SELECT builder_id FROM Builder WHERE code = rc."Builder code2" LIMIT 1),
    2
FROM Catalog c
JOIN ratcatalogue rc ON c.image_no = rc.image_no
WHERE rc."Builder code2" IS NOT NULL AND rc."Builder code2" != '';

INSERT INTO CatalogBuilder (catalog_id, builder_id, builder_order)
SELECT 
    c.catalog_id,
    (SELECT builder_id FROM Builder WHERE code = rc."Builder code3" LIMIT 1),
    3
FROM Catalog c
JOIN ratcatalogue rc ON c.image_no = rc.image_no
WHERE rc."Builder code3" IS NOT NULL AND rc."Builder code3" != '';