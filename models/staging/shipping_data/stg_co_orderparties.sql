with source as (

    select * from {{ source('shipping_data', 'co_orderparty') }}

),

renamed as (

    select
        orderpartyid,
        partyrolerdn,
        partynumber,
        customerorderid,
        hvr_isdelete

    from source

)

select * from renamed
