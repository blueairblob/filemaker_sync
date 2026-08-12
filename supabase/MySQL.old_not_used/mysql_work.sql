/*

use rat;
desc ratcatalogue;
drop table ratcatalogue;
select * from ratcatalogue;

INSERT INTO ratbuilders (`Builder code`, `Builder name`, `Location`, `Plant code`, `Builder plant`, `Remarks`)
VALUES('UEC', 'United Electric Car Co Ltd', NULL, NULL, 'Dick, Kerr Works
 Preston', NULL);

select * from ratbuilders where `Builder code` = 'UEC';
delete from ratbuilders;
SELECT count(*) FROM rat.ratbuilders;
drop table rattype;

SELECT * FROM rat.ratroutes;
DELETE FROM rat.ratroutes WHERE remarks like '%gaige%';

select count(*), start_location, end_location, organisation, route from ratroutes group by start_location, end_location, organisation, route having count(*)>1;
*/


-- Create LOVs

SELECT * FROM rat.ratcatalogue;
select user from MySQl.user;
select current_user() as results;
select user();

-- drop table
DROP TABLE IF EXISTS categories_lov;
-- Category	From Field: "RATcollections Copy::collection"
CREATE TABLE categories_lov (
    id INT AUTO_INCREMENT PRIMARY KEY,
    value VARCHAR(255),
    created_by VARCHAR(255),
    modified_by VARCHAR(255),
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
ALTER TABLE categories_lov ADD CONSTRAINT categories_lov_unique_value UNIQUE (value);

-- Populate new_table with distinct values from master_table
INSERT INTO categories_lov (value)
SELECT DISTINCT categories
FROM  rat.ratcatalogue order by 1;

-- Create a trigger to populate audit columns on INSERT
DELIMITER //
CREATE TRIGGER categories_lov_populate_audit_columns_on_insert
BEFORE INSERT ON categories_lov
FOR EACH ROW
BEGIN
    SET NEW.created_by = USER();
    SET NEW.modified_by = USER();
END;
//

-- Create a trigger to populate audit columns on UPDATE
CREATE TRIGGER categories_lov_populate_audit_columns_on_update
BEFORE UPDATE ON categories_lov
FOR EACH ROW
BEGIN
    SET NEW.modified_by = USER();
END;
//

DELIMITER ;

SELECT * FROM rat.categories_lov;

------------------------------------------------------------------------------------------------
SELECT DISTINCT organisation FROM ratcatalogue ORDER BY 1;

-- drop table
DROP TABLE IF EXISTS organisations_lov;
-- Category	From Field: "RATcollections Copy::collection"
CREATE TABLE organisations_lov (
    id INT AUTO_INCREMENT PRIMARY KEY,
    value VARCHAR(255),
    created_by VARCHAR(255),
    modified_by VARCHAR(255),
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
-- Add Unique contraint
ALTER TABLE organisations_lov ADD CONSTRAINT organisation_lov_unique_value UNIQUE (value);

-- Create a trigger to populate audit columns on INSERT
DELIMITER //
CREATE TRIGGER organisations_lov_populate_audit_columns_on_insert
BEFORE INSERT ON organisation_lov
FOR EACH ROW
BEGIN
    SET NEW.created_by = USER();
    SET NEW.modified_by = USER();
END;
//

-- Create a trigger to populate audit columns on UPDATE
CREATE TRIGGER organisations_lov_populate_audit_columns_on_update
BEFORE UPDATE ON organisations_lov
FOR EACH ROW
BEGIN
    SET NEW.modified_by = USER();
END;
//

DELIMITER ;

-- Populate new_table with distinct values from master_table
INSERT INTO organisations_lov (value) SELECT DISTINCT organisation FROM rat.ratcatalogue order by 1;

SELECT * FROM rat.organisations_lov;
------------------------------------------------------------------------------------------------
SELECT DISTINCT route FROM ratcatalogue ORDER BY 1;

-- drop table
DROP TABLE IF EXISTS routes_lov;
-- Category	From Field: "RATcollections Copy::collection"
CREATE TABLE routes_lov (
    id INT AUTO_INCREMENT PRIMARY KEY,
    value VARCHAR(255),
    created_by VARCHAR(255),
    modified_by VARCHAR(255),
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
-- Add Unique contraint
ALTER TABLE route_lov ADD CONSTRAINT route_lov_unique_value UNIQUE (value);

-- Create a trigger to populate audit columns on INSERT
DELIMITER //
CREATE TRIGGER routes_lov_populate_audit_columns_on_insert
BEFORE INSERT ON route_lov
FOR EACH ROW
BEGIN
    SET NEW.created_by = USER();
    SET NEW.modified_by = USER();
END;
//

-- Create a trigger to populate audit columns on UPDATE
CREATE TRIGGER routes_lov_populate_audit_columns_on_update
BEFORE UPDATE ON routes_lov
FOR EACH ROW
BEGIN
    SET NEW.modified_by = USER();
END;
//

DELIMITER ;

-- Populate new_table with distinct values from master_table
INSERT INTO routes_lov (value) SELECT DISTINCT route FROM  rat.ratcatalogue order by 1;

SELECT * FROM rat.routes_lov;

------------------------------------------------------------------------------------------------

SELECT DISTINCT location FROM ratcatalogue ORDER BY 1;

-- drop table
DROP TABLE IF EXISTS locations_lov;
-- Category	From Field: "RATcollections Copy::collection"
CREATE TABLE locations_lov (
    id INT AUTO_INCREMENT PRIMARY KEY,
    value VARCHAR(255),
    created_by VARCHAR(255),
    modified_by VARCHAR(255),
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
-- Add Unique contraint
ALTER TABLE locations_lov ADD CONSTRAINT locations_lov_unique_value UNIQUE (value);

-- Create a trigger to populate audit columns on INSERT
DELIMITER //
CREATE TRIGGER locations_lov_populate_audit_columns_on_insert
BEFORE INSERT ON location_lov
FOR EACH ROW
BEGIN
    SET NEW.created_by = USER();
    SET NEW.modified_by = USER();
END;
//

-- Create a trigger to populate audit columns on UPDATE
CREATE TRIGGER locations_lov_populate_audit_columns_on_update
BEFORE UPDATE ON locations_lov
FOR EACH ROW
BEGIN
    SET NEW.modified_by = USER();
END;
//

DELIMITER ;

-- Populate new_table with distinct values from master_table
INSERT INTO locations_lov (value) SELECT DISTINCT location FROM rat.ratcatalogue order by 1;

SELECT * FROM rat.locations_lov;


------------------------------------------------------------------------------------------------

SELECT DISTINCT gauge FROM ratcatalogue ORDER BY 1;
-- drop table
DROP TABLE IF EXISTS guage_lov;
-- Category	From Field: "RATcollections Copy::collection"
CREATE TABLE guage_lov (
    id INT AUTO_INCREMENT PRIMARY KEY,
    value VARCHAR(255),
    created_by VARCHAR(255),
    modified_by VARCHAR(255),
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
-- Add Unique contraint
ALTER TABLE guage_lov ADD CONSTRAINT guage_lov_unique_value UNIQUE (value);

-- Create a trigger to populate audit columns on INSERT
DELIMITER //
CREATE TRIGGER guage_lov_populate_audit_columns_on_insert
BEFORE INSERT ON guage_lov
FOR EACH ROW
BEGIN
    SET NEW.created_by = USER();
    SET NEW.modified_by = USER();
END;
//

-- Create a trigger to populate audit columns on UPDATE
CREATE TRIGGER guage_lov_populate_audit_columns_on_update
BEFORE UPDATE ON guage_lov
FOR EACH ROW
BEGIN
    SET NEW.modified_by = USER();
END;
//
DELIMITER ;

-- Populate new_table with distinct values from master_table
INSERT INTO guage_lov (value) SELECT DISTINCT gauge FROM  rat.ratcatalogue order by 1;
SELECT * FROM rat.guage_lov;

------------------------------------------------------------------------------------------------

SELECT DISTINCT photographer FROM ratcollections ORDER BY 1;
-- drop table
DROP TABLE IF EXISTS photographers_lov;
-- Category	From Field: "RATcollections Copy::collection"
CREATE TABLE photographers_lov (
    id INT AUTO_INCREMENT PRIMARY KEY,
    value VARCHAR(255),
    created_by VARCHAR(255),
    modified_by VARCHAR(255),
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
-- Add Unique contraint
ALTER TABLE photographers_lov ADD CONSTRAINT photographers_lov_unique_value UNIQUE (value);

-- Create a trigger to populate audit columns on INSERT
DELIMITER //
CREATE TRIGGER photographers_lov_populate_audit_columns_on_insert
BEFORE INSERT ON photographers_lov
FOR EACH ROW
BEGIN
    SET NEW.created_by = USER();
    SET NEW.modified_by = USER();
END;
//

-- Create a trigger to populate audit columns on UPDATE
CREATE TRIGGER photographers_lov_populate_audit_columns_on_update
BEFORE UPDATE ON photographers_lov
FOR EACH ROW
BEGIN
    SET NEW.modified_by = USER();
END;
//
DELIMITER ;

-- Populate new_table with distinct values from master_table
INSERT INTO photographers_lov (value) SELECT DISTINCT photographer FROM rat.ratcollections order by 1;
SELECT * FROM rat.photographers_lov;

------------------------------------------------------------------------------------------------

SELECT DISTINCT country FROM ratcatalogue ORDER BY 1;
-- drop table
DROP TABLE IF EXISTS countries_lov;
-- Category	From Field: "RATcollections Copy::collection"
CREATE TABLE countries_lov (
    id INT AUTO_INCREMENT PRIMARY KEY,
    value VARCHAR(255),
    created_by VARCHAR(255),
    modified_by VARCHAR(255),
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
-- Add Unique contraint
ALTER TABLE countries_lov ADD CONSTRAINT countries_lov_unique_value UNIQUE (value);

-- Create a trigger to populate audit columns on INSERT
DELIMITER //
CREATE TRIGGER countries_lov_populate_audit_columns_on_insert
BEFORE INSERT ON countries_lov
FOR EACH ROW
BEGIN
    SET NEW.created_by = USER();
    SET NEW.modified_by = USER();
END;
//

-- Create a trigger to populate audit columns on UPDATE
CREATE TRIGGER countries_lov_populate_audit_columns_on_update
BEFORE UPDATE ON countries_lov
FOR EACH ROW
BEGIN
    SET NEW.modified_by = USER();
END;
//
DELIMITER ;

-- Populate new_table with distinct values from master_table
INSERT INTO countries_lov (value) SELECT DISTINCT country FROM rat.ratcatalogue order by 1;
SELECT * FROM rat.countries_lov;


------------------------------------------------------------------------------------------------

SELECT DISTINCT organisation_type FROM ratcatalogue ORDER BY 1;
-- drop table
DROP TABLE IF EXISTS organisation_types_lov;
-- Category	From Field: "RATcollections Copy::collection"
CREATE TABLE organisation_types_lov (
    id INT AUTO_INCREMENT PRIMARY KEY,
    value VARCHAR(255),
    created_by VARCHAR(255),
    modified_by VARCHAR(255),
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
-- Add Unique contraint
ALTER TABLE organisation_types_lov ADD CONSTRAINT organisation_types_lov_unique_value UNIQUE (value);

-- Create a trigger to populate audit columns on INSERT
DELIMITER //
CREATE TRIGGER organisation_types_lov_populate_audit_columns_on_insert
BEFORE INSERT ON organisation_types_lov
FOR EACH ROW
BEGIN
    SET NEW.created_by = USER();
    SET NEW.modified_by = USER();
END;
//

-- Create a trigger to populate audit columns on UPDATE
CREATE TRIGGER organisation_types_lov_populate_audit_columns_on_update
BEFORE UPDATE ON organisation_types_lov
FOR EACH ROW
BEGIN
    SET NEW.modified_by = USER();
END;
//
DELIMITER ;

-- Populate new_table with distinct values from master_table
INSERT INTO organisation_types_lov (value) SELECT DISTINCT organisation_type FROM rat.ratcatalogue order by 1;
SELECT * FROM rat.organisation_types_lov;

------------------------------------------------------------------------------------------------

SELECT DISTINCT facility FROM ratcatalogue ORDER BY 1;
-- drop table
DROP TABLE IF EXISTS facility_lov;
-- Category	From Field: "RATcollections Copy::collection"
CREATE TABLE facility_lov (
    id INT AUTO_INCREMENT PRIMARY KEY,
    value VARCHAR(255),
    created_by VARCHAR(255),
    modified_by VARCHAR(255),
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
-- Add Unique contraint
ALTER TABLE facility_lov ADD CONSTRAINT facility_lov_unique_value UNIQUE (value);

-- Create a trigger to populate audit columns on INSERT
DELIMITER //
CREATE TRIGGER facility_lov_populate_audit_columns_on_insert
BEFORE INSERT ON facility_lov
FOR EACH ROW
BEGIN
    SET NEW.created_by = USER();
    SET NEW.modified_by = USER();
END;
//

-- Create a trigger to populate audit columns on UPDATE
CREATE TRIGGER facility_lov_populate_audit_columns_on_update
BEFORE UPDATE ON facility_lov
FOR EACH ROW
BEGIN
    SET NEW.modified_by = USER();
END;
//
DELIMITER ;

-- Populate new_table with distinct values from master_table
INSERT INTO facility_lov (value) SELECT DISTINCT facility FROM rat.ratcatalogue order by 1;
SELECT * FROM rat.facility_lov;

------------------------------------------------------------------------------------------------

SELECT DISTINCT active_area FROM ratcatalogue ORDER BY 1;
-- drop table
DROP TABLE IF EXISTS active_area_lov;
-- Category	From Field: "RATcollections Copy::collection"
CREATE TABLE active_area_lov (
    id INT AUTO_INCREMENT PRIMARY KEY,
    value VARCHAR(255),
    created_by VARCHAR(255),
    modified_by VARCHAR(255),
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
-- Add Unique contraint
ALTER TABLE active_area_lov ADD CONSTRAINT active_area_lov_unique_value UNIQUE (value);

-- Create a trigger to populate audit columns on INSERT
DELIMITER //
CREATE TRIGGER active_area_lov_populate_audit_columns_on_insert
BEFORE INSERT ON active_area_lov
FOR EACH ROW
BEGIN
    SET NEW.created_by = USER();
    SET NEW.modified_by = USER();
END;
//

-- Create a trigger to populate audit columns on UPDATE
CREATE TRIGGER active_area_lov_populate_audit_columns_on_update
BEFORE UPDATE ON active_area_lov
FOR EACH ROW
BEGIN
    SET NEW.modified_by = USER();
END;
//
DELIMITER ;

-- Populate new_table with distinct values from master_table
INSERT INTO active_area_lov (value) SELECT DISTINCT active_area FROM rat.ratcatalogue order by 1;
SELECT * FROM rat.active_area_lov;

------------------------------------------------------------------------------------------------

SELECT DISTINCT plant_code FROM ratcatalogue ORDER BY 1;
-- drop table
DROP TABLE IF EXISTS corporate_body_lov;
-- Category	From Field: "RATcollections Copy::collection"
CREATE TABLE corporate_body_lov (
    id INT AUTO_INCREMENT PRIMARY KEY,
    value VARCHAR(255),
    created_by VARCHAR(255),
    modified_by VARCHAR(255),
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
-- Add Unique contraint
ALTER TABLE corporate_body_lov ADD CONSTRAINT corporate_body_lov_unique_value UNIQUE (value);

-- Create a trigger to populate audit columns on INSERT
DELIMITER //
CREATE TRIGGER corporate_body_lov_populate_audit_columns_on_insert
BEFORE INSERT ON corporate_body_lov
FOR EACH ROW
BEGIN
    SET NEW.created_by = USER();
    SET NEW.modified_by = USER();
END;
//

-- Create a trigger to populate audit columns on UPDATE
CREATE TRIGGER corporate_body_lov_populate_audit_columns_on_update
BEFORE UPDATE ON corporate_body_lov
FOR EACH ROW
BEGIN
    SET NEW.modified_by = USER();
END;
//
DELIMITER ;

-- Populate new_table with distinct values from master_table
INSERT INTO corporate_body_lov (value) SELECT DISTINCT corporate_body FROM rat.ratcatalogue order by 1;
SELECT * FROM rat.corporate_body_lov;

------------------------------------------------------------------------------------------------

SELECT DISTINCT `plant code` FROM ratcatalogue ORDER BY 1;

-- drop table
DROP TABLE IF EXISTS plant_codes_lov;
CREATE TABLE plant_codes_lov (
    id INT AUTO_INCREMENT PRIMARY KEY,
    value VARCHAR(255),
    created_by VARCHAR(255),
    modified_by VARCHAR(255),
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
-- Add Unique contraint
ALTER TABLE plant_codes_lov ADD CONSTRAINT plant_codes_lov_unique_value UNIQUE (value);

-- Create a trigger to populate audit columns on INSERT
DELIMITER //
CREATE TRIGGER plant_codes_lov_populate_audit_columns_on_insert
BEFORE INSERT ON plant_codes_lov
FOR EACH ROW
BEGIN
    SET NEW.created_by = USER();
    SET NEW.modified_by = USER();
END;
//

-- Create a trigger to populate audit columns on UPDATE
CREATE TRIGGER plant_codes_lov_populate_audit_columns_on_update
BEFORE UPDATE ON plant_codes_lov
FOR EACH ROW
BEGIN
    SET NEW.modified_by = USER();
END;
//
DELIMITER ;

-- Populate new_table with distinct values from master_table
INSERT INTO plant_codes_lov (value) SELECT DISTINCT `plant code` FROM rat.ratcatalogue order by 1;
SELECT * FROM rat.plant_codes_lov;
------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------





