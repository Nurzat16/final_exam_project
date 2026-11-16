{{ config(materialized='view', schema='korgonbaeva_ods') }}
with archive_hours as (
    select sensor_id, location_id, date_trunc('hour', measured_at at time zone 'Asia/Bishkek') as local_hour,
           avg(value) as value, false as is_api
    from {{ source('ods', 'ods_sensors_history') }}
    where parameter = 'pm25' and unit = 'µg/m³'
    group by 1, 2, 3
), api_hours as (
    select sensor_id, location_id, measured_at at time zone 'Asia/Bishkek' as local_hour, value, true as is_api
    from {{ source('ods', 'ods_api_hourly') }}
    where parameter = 'pm25' and unit = 'µg/m³'
), combined as (
    select * from archive_hours
    union all
    select * from api_hours
)
select sensor_id, local_hour, local_hour::date as ds, value, is_api,
       case when value <= 9 then 'low_pm25'
            when value <= 35.4 then 'moderate_pm25'
            else 'high_pm25' end as category,
       location_id
from combined
