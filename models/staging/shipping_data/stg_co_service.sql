with source as (

    select * from {{ source('shipping_data', 'co_service') }}

),

renamed as (

    select
        serviceid,
        customerorderid,
        hvr_isdelete

    from source

)

select * from renamed
