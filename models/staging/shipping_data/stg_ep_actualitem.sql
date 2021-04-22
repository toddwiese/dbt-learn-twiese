with source as (

    select * from {{ source('shipping_data', 'ep_actualitem') }}

),

renamed as (

    select
        customerorderitemid,
        totalweight,
        unitquantity,
        totalpallets,
        totalpalletspaces,
        totalvolume,
        hvr_isdelete

    from source

)

select * from renamed
