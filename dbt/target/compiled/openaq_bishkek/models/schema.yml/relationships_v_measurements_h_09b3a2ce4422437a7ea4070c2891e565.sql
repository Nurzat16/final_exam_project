
    
    

with child as (
    select location_id as from_field
    from "aiacademy"."korgonbaeva_ods"."v_measurements_hourly"
    where location_id is not null
),

parent as (
    select location_id as to_field
    from "aiacademy"."korgonbaeva_ods"."ods_locations_dictionary"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


