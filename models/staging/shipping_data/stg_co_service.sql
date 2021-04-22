with source as (

    select * from {{ source('shipping_data', 'co_service') }}

),

renamed as (

    select
        serviceid,
        customerorderid,
        serviceid || customerorderid as customer_service_id,
        hvr_isdelete

    from source

)

select * from renamed
