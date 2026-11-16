select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select ds
from "aiacademy"."korgonbaeva_datamart"."dm_daily_air_quality"
where ds is null



      
    ) dbt_internal_test