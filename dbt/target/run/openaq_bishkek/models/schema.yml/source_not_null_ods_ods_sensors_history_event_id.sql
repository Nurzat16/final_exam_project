select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select event_id
from "aiacademy"."korgonbaeva_ods"."ods_sensors_history"
where event_id is null



      
    ) dbt_internal_test