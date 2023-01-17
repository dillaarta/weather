CREATE TABLE IF NOT EXISTS ods.dataset_review (
review_id TEXT, 
user_id TEXT, 
business_id TEXT, 
stars TEXT, 
useful TEXT, 
funny TEXT,
cool TEXT, 
text TEXT, 
date TEXT,
data_updated_at timestamp without time zone);

CREATE INDEX IF NOT EXISTS dataset_review_id
    ON ods.dataset_review USING btree
    (review_id ASC NULLS LAST)
    TABLESPACE pg_default;