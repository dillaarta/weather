CREATE TABLE IF NOT EXISTS ods.dataset_user (
user_id TEXT, 
name TEXT, 
review_count TEXT, 
yelping_since TEXT, 
useful TEXT, 
funny TEXT,
cool TEXT, 
elite TEXT, 
friends TEXT, 
fans TEXT, 
average_stars TEXT, 
compliment_hot TEXT,
compliment_more TEXT, 
compliment_profile TEXT, 
compliment_cute TEXT,
compliment_list TEXT, 
compliment_note TEXT, 
compliment_plain TEXT,
compliment_cool TEXT, 
compliment_funny TEXT, 
compliment_writer TEXT,
compliment_photos TEXT,
data_updated_at timestamp without time zone,
PRIMARY KEY (user_id)
);

CREATE INDEX IF NOT EXISTS dataset_user_id
    ON ods.dataset_user USING btree
    (user_id ASC NULLS LAST)
    TABLESPACE pg_default;