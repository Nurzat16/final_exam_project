SELECT task_id, status, row_count, error_text
FROM korgonbaeva_stg.etl_load_log
WHERE ds = DATE '2026-09-10'
ORDER BY id;

SELECT * FROM korgonbaeva_datamart.dm_daily_air_quality WHERE ds = DATE '2026-09-10';
-- How many observed sensor-hours are available at each station in September?
SELECT location_name, measurement_count, api_measurement_count, mean_value
FROM korgonbaeva_datamart.dm_monthly_by_station
WHERE month = DATE '2026-09-01'
ORDER BY measurement_count DESC, location_id;

SELECT
    (SELECT SUM(measurement_count) FROM korgonbaeva_datamart.dm_daily_air_quality
     WHERE ds >= DATE '2026-09-01' AND ds < DATE '2026-10-01') AS daily_sum,
    (SELECT SUM(measurement_count) FROM korgonbaeva_datamart.dm_monthly_by_station
     WHERE month = DATE '2026-09-01') AS monthly_sum;
