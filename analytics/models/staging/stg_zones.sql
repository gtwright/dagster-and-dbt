with raw_zones as (
    select *
    from {{ source('raw', 'taxi_zones') }}
)
select
    zone_id,
    zone as zone_name,
    borough,
    zone_name like '%Airport' as is_airport,
from raw_zones