select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    ds as unique_field,
    count(*) as n_records

from "aiacademy"."korgonbaeva_datamart"."dm_daily_air_quality"
where ds is not null
group by ds
having count(*) > 1



      
    ) dbt_internal_test