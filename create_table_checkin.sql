CREATE TABLE IF NOT EXISTS ods.dataset_checkin (
business_id TEXT, 
date TEXT, 
data_updated_at timestamp without time zone,
PRIMARY KEY (business_id));

CREATE INDEX IF NOT EXISTS dataset_checkin_business_id
    ON ods.dataset_checkin USING btree
    (business_id ASC NULLS LAST)
    TABLESPACE pg_default;