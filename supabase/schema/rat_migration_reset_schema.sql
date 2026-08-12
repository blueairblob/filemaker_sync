-- Drop tables
DROP TABLE rat_migration.prompts;
DROP TABLE rat_migration.ratbuilders;
DROP TABLE rat_migration.ratcatalogue;
DROP TABLE rat_migration.ratcollections;
DROP TABLE rat_migration.ratcopyright;
DROP TABLE rat_migration.ratroutes;
DROP TABLE rat_migration.ratlabels;
DROP TABLE rat_migration.prompts;
COMMIT;

-- The following Tables were generated directly from Filemaker Pro
-- Start DDL output from filemaker_extract.py
/*
	Table: prompts
	Date: 2024.08.13 19:24:38,
*/

CREATE TABLE IF NOT EXISTS rat_migration.prompts (
	prompt_field TEXT, 
	prompt_desc TEXT
);

/*
	Table: ratbuilders
	Date: 2024.08.13 19:24:38,
*/

CREATE TABLE IF NOT EXISTS rat_migration.ratbuilders (
	"Builder code" TEXT, 
	"Builder name" TEXT, 
	"Location" TEXT, 
	"Plant code" TEXT, 
	"Builder plant" TEXT, 
	"Remarks" TEXT
);

/*
	Table: ratcatalogue
	Date: 2024.08.13 19:24:38,
*/

CREATE TABLE IF NOT EXISTS rat_migration.ratcatalogue (
	image_no TEXT, 
	accession_no FLOAT(53), 
	category TEXT, 
	organisation_type TEXT, 
	country TEXT, 
	organisation TEXT, 
	route TEXT, 
	location TEXT, 
	circa TEXT, 
	imprecise_date TEXT, 
	date_taken DATE, 
	condition TEXT, 
	collection TEXT, 
	valuation FLOAT(53), 
	entry_date DATE, 
	owners_ref TEXT, 
	prints_allowed TEXT, 
	internet_use TEXT, 
	publications_use TEXT, 
	cd_no TEXT, 
	description TEXT, 
	picture TEXT, 
	gauge TEXT, 
	photographer TEXT, 
	cd_no_hr TEXT, 
	"BW_image_no" TEXT, 
	bw_cd_no TEXT, 
	active_area TEXT, 
	corporate_body TEXT, 
	facility TEXT, 
	"Builder code" TEXT, 
	"Works number" TEXT, 
	"Year built" TEXT, 
	"Plant code" TEXT, 
	"Scountry" TEXT, 
	layout TEXT, 
	sel_layout TEXT, 
	search TEXT, 
	"Sorganisation" TEXT, 
	"Sroute" TEXT, 
	"Slocation" TEXT, 
	"Sactive_area" TEXT, 
	"Pcategory" TEXT, 
	"Builder" TEXT, 
	"Scorporate_body" TEXT, 
	"Sfacility" TEXT, 
	"SdescExtract" TEXT, 
	"Simage_no" TEXT, 
	"Sgauge" TEXT, 
	"Scollection" TEXT, 
	"Sbuilder code" TEXT, 
	"Sworks number" TEXT, 
	"CurrFldName" TEXT, 
	"HelpNotes" TEXT, 
	"Builder code2" TEXT, 
	"Builder name1" TEXT, 
	"Builder name2" TEXT, 
	"Plant code2" TEXT, 
	"Works number2" TEXT, 
	"Year built2" TEXT, 
	"Builder code3" TEXT, 
	"Plant code3" TEXT, 
	"Works number3" TEXT, 
	"Year built3" TEXT, 
	"Builder name3" TEXT, 
	"Password" TEXT, 
	parent_folder TEXT, 
	imgref_stem TEXT, 
	website TEXT
);

/*
	Table: ratcollections
	Date: 2024.08.13 19:24:38,
*/

CREATE TABLE IF NOT EXISTS rat_migration.ratcollections (
	collection TEXT, 
	owner TEXT, 
	donor TEXT, 
	storage_location TEXT, 
	photographer TEXT, 
	print_sales TEXT, 
	internet_use TEXT, 
	publications_use TEXT, 
	contact TEXT, 
	remarks TEXT, 
	accession_number TEXT
);

/*
	Table: ratcopyright
	Date: 2024.08.13 19:24:38,
*/

CREATE TABLE IF NOT EXISTS rat_migration.ratcopyright (
	name TEXT, 
	subject TEXT, 
	collection TEXT, 
	remarks TEXT
);

/*
	Table: ratlabels
	Date: 2024.08.13 19:24:38,
*/

CREATE TABLE IF NOT EXISTS rat_migration.ratlabels (
	image_no TEXT, 
	date TEXT, 
	description TEXT, 
	date1 TEXT
);

/*
	Table: ratroutes
	Date: 2024.08.13 19:24:38,
*/

CREATE TABLE IF NOT EXISTS rat_migration.ratroutes (
	start_location TEXT, 
	end_location TEXT, 
	organisation TEXT, 
	route TEXT, 
	country TEXT, 
	"Remarks" TEXT
);

/*
	Table: prompts
	Date: 2024.08.13 19:29:23,
*/

CREATE TABLE IF NOT EXISTS rat_migration.prompts (
	prompt_field TEXT, 
	prompt_desc TEXT
);

-- Finished DDL output from filemaker_extract.py

-- Unique constraints 

ALTER TABLE rat_migration.prompts ADD CONSTRAINT pk_prompts_prompt_field PRIMARY KEY (prompt_field);
ALTER TABLE rat_migration.ratbuilders ADD CONSTRAINT pk_ratbuilders_builder_code PRIMARY KEY ("Builder code");
ALTER TABLE rat_migration.ratcatalogue ADD CONSTRAINT pk_ratcatalogue_image_no PRIMARY KEY (image_no);
ALTER TABLE rat_migration.ratroutes ADD CONSTRAINT pk_ratroutes_start_end_location PRIMARY KEY (start_location, end_location);
ALTER TABLE rat_migration.ratcollections ADD CONSTRAINT pk_ratcollections_collection PRIMARY KEY (collection);
ALTER TABLE rat_migration.ratlabels ADD CONSTRAINT pk_ratlabels_image_no PRIMARY KEY (image_no);
