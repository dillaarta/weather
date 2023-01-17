SELECT
    b.business_id,
    b.user_id,
    b.review_id,
    b.total_review,
    b.city,
    b.state,
    b.postal_code,
    b.total_stars,
    u.yelping_since,
    r.date::DATETIME,
    r.text,
    d.checkin_date::DATETIME,
    w.station,
    w.location_name,
    w.prcp as percipitation,
    w.snow as snowfall,
    w.snwd as snow_depth,
    w.tmax as max_temperature,
    w.tmin as min_temperature
FROM ods.dataset_business b 
LEFT JOIN ods.dataset_review r 
ON b.review_id = r.review_id
LEFT JOIN ods.dataset_checkin c 
ON b.business_id = c.business_id
LEFT JOIN ods.dataset_user u  
ON b.user_id = u.user_id
LEFT JOIN ods.dataset_weather w 
ON b.state_id = substring(w.location_name,-5,-4);