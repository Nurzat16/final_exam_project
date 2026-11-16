
    
    

select
    ds as unique_field,
    count(*) as n_records

from "aiacademy"."korgonbaeva_datamart"."dm_daily_air_quality"
where ds is not null
group by ds
having count(*) > 1


