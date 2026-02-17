with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
    select
        -- identifiers (standardized naming for consistency across yellow/green)
        cast(dispatching_base_num as integer) as dispatching_base_num,
        cast(affiliated_base_number as integer) as affiliated_base_number,
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,

        -- timestamps (standardized naming)
        cast(pickup_datetime as timestamp) as pickup_datetime,  -- tpep = Taxicab Passenger Enhancement Program (yellow taxis)
        cast(dropoff_datetime as timestamp) as dropoff_datetime,

        -- trip info
        cast(sr_flag as string) as sr_flag,

    from source
    -- Filter out records with null vendor_id (data quality requirement)
    where dispatching_base_num is not null
)

select * from renamed
-- Sample records for dev environment using deterministic date filter
{% if target.name == 'dev' %}
    where pickup_datetime >= '2019-01-01' and pickup_datetime < '2019-02-01'
{% endif %}
