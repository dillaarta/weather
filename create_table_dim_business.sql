CREATE TABLE IF NOT EXISTS dwh.dim_business (
business_id TEXT,
user_id TEXT,
review_id, TEXT
total_review TEXT,
city TEXT,
state TEXT,
postal_code TEXT,
total_stars FLOAT,
yelping_since TEXT,
date DATETIME,
text TEXT,
checkin_date DATETIME,
station TEXT,
location_name TEXT,
percipitation FLOAT,
snowfall FLOAT,
snow_depth FLOAT,
max_temperature FLOAT,
min_temperature FLOAT,
data_updated_at timestamp without time zone
PRIMARY KEY(business_id));

CREATE INDEX IF NOT EXISTS dataset_tip_user_id
    ON dwh.dim_business USING btree
    (user_id ASC NULLS LAST)
    TABLESPACE pg_default;