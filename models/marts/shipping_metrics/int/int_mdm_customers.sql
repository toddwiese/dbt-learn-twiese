with 

mdm_party as (
    select * from {{ ref('stg_mdm_party') }}
),

final as (

    select 
        *
    from mdm_party

    where PartyTypeRDN = 7    
)

select * from final