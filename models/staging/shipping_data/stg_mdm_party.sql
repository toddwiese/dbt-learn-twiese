with source as (

    select * from {{ source('shipping_data', 'mdm_party') }}

),

renamed as (

    select
        partyid,
        partytyperdn,
        partynumber,
        partycode,
        hvr_isdelete

    from source

)

select * from renamed
