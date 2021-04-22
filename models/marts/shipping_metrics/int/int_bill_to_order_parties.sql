with

order_parties as (
    select * from {{ ref('stg_co_orderparties') }}
), 

final as (

    select 
        *
    from 
        order_parties
    where 
        order_parties.PartyRoleRDN = 326
)

select * from final