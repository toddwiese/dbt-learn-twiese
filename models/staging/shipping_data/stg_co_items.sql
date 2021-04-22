with source as (

    select * from {{ source('shipping_data', 'co_item') }}

),

renamed as (

    select
        customerorderitemid,
        nmfc,
        totalweight,
        totalpallets,
        totalvolume,
        unitquantity,
        hvr_isdelete

    from source

)

select * from renamed
