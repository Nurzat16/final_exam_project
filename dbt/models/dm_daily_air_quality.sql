{{ config(materialized='table', post_hook='alter table {{ this }} add primary key (ds)') }}
select ds, count(*) as measurement_count, sum(value)::numeric(20,4) as value_sum,
       round(avg(value),4)::numeric(14,4) as mean_value,
       count(*) filter (where is_api) as api_measurement_count,
       count(distinct is_api)::integer as source_count
from {{ ref('v_measurements_hourly') }}
group by ds
