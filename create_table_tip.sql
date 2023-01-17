CREATE TABLE IF NOT EXISTS ods.dataset_tip (
user_id TEXT, 
business_id TEXT, 
text TEXT, 
date TEXT, 
compliment_count TEXT,
data_updated_at timestamp without time zone);

CREATE INDEX IF NOT EXISTS dataset_tip_user_id
    ON ods.dataset_tip USING btree
    (user_id ASC NULLS LAST)
    TABLESPACE pg_default;