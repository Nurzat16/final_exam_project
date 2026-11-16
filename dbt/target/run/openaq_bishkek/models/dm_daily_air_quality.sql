
  
    

  create  table "aiacademy"."korgonbaeva_datamart"."dm_daily_air_quality__dbt_tmp"
  
  
    as
  
  (
    
select ds, count(*) as measurement_count, sum(value)::numeric(20,4) as value_sum,
       round(avg(value),4)::numeric(14,4) as mean_value,
       count(*) filter (where is_api) as api_measurement_count,
       count(distinct is_api)::integer as source_count
from "aiacademy"."korgonbaeva_ods"."v_measurements_hourly"
group by ds
  );
  