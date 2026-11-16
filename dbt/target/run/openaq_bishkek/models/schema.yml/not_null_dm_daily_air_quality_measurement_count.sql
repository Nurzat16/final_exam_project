select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select measurement_count
from "aiacademy"."korgonbaeva_datamart"."dm_daily_air_quality"
where measurement_count is null



      
    ) dbt_internal_test