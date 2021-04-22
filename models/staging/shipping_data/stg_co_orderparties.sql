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

limit 500
/* limit added automatically by dbt cloud */