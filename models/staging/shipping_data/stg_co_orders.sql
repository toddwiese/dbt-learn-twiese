with source as (

    select * from {{ source('shipping_data', 'co_order') }}

),

renamed as (

    select
        customerorderid,
        customerordernumber,
        hvr_isdelete

    from source

)

select * from renamed
