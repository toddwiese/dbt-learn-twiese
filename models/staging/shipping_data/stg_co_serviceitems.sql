with source as (

    select * from {{ source('shipping_data', 'co_serviceitem') }}

),

renamed as (

    select
        itemid,
        serviceid,
        hvr_isdelete

    from source

)

select * from renamed
