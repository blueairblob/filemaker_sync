# Constraints
# IMAGES
ALTER TABLE rat.images DROP PRIMARY KEY;
ALTER TABLE rat.images ADD PRIMARY KEY (`image_no`(100));

# PROMPTS
ALTER TABLE rat.prompts DROP PRIMARY KEY;
ALTER TABLE rat.prompts ADD PRIMARY KEY (`prompt_field`(100));

# RATBUILDERS
ALTER TABLE rat.ratbuilders DROP PRIMARY KEY;
ALTER TABLE rat.ratbuilders ADD PRIMARY KEY (`Builder code`(100));

# RATCOLLECTIONS
ALTER TABLE rat.ratcollections DROP PRIMARY KEY;
ALTER TABLE rat.ratcollections ADD PRIMARY KEY (`collection`(100));

# RATCOPYRIGHT
ALTER TABLE rat.ratcopyright DROP PRIMARY KEY;
ALTER TABLE rat.ratcopyright ADD PRIMARY KEY (`name`(100));

# RATLABELS
ALTER TABLE rat.ratlabels DROP PRIMARY KEY;
ALTER TABLE rat.ratlabels ADD PRIMARY KEY (`image_no`(100));

# RATROUTES
#DELETE FROM rat.ratroutes WHERE remarks like '%gaige%';
DELETE FROM rat.ratroutes;
ALTER TABLE rat.ratroutes DROP PRIMARY KEY;
ALTER TABLE rat.ratroutes ADD PRIMARY KEY (`start_location`(100), `end_location`(100), `organisation`(100), `route`(100));

# RATCATALOGUE
DELETE FROM rat.ratcatalogue;
ALTER TABLE rat.ratcatalogue DROP PRIMARY KEY;
ALTER TABLE rat.ratcatalogue ADD PRIMARY KEY (`image_no`(100));

select `Builder code` from ratbuilders group by `builder code` having count(*) > 1;
select `Builder code`, count(*) from ratbuilders group by 1 order by 1;