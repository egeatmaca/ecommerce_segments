-- Export users
COPY (
    SELECT
        id,
        age AS user_age,
        gender AS user_gender,
        COALESCE(
            CASE WHEN country = 'España' THEN 'Spain' ELSE NULL END,
            CASE WHEN country = 'Deutschland' THEN 'Germany' ELSE NULL END,
            country
        ) AS user_country,
        city AS user_city,
        traffic_source AS user_traffic_source
    FROM users
) TO '/tmp/exports/users.csv' 
WITH CSV HEADER;