{{
    config(
        materialized='incremental'
    )
}}

with 

orders as (
    select * from {{ ref('stg_co_orders') }}
),

services as (
    select * from {{ ref('stg_co_service') }}
),

bill_to_order_parties as (
    select * from {{ ref('int_bill_to_order_parties') }}
),

mdm_customers as (
    
    select * from {{ ref('int_mdm_customers') }}
    
    where hvr_isdelete = 0

),

service_items as (
    select * from {{ ref('stg_co_serviceitems') }}
), 

items as (
    select * from {{ ref('stg_co_items') }}
), 

joined as (

    SELECT
        (services.ServiceID::varchar || '|1|0|1') AS ServiceNK_PK,
        services.customer_service_id,
        items.CustomerOrderItemID, --pk
        orders.CustomerOrderNumber as ShipmentID,
        mdm_customers.PartyCode as CustomerCode, --pk
        items.NMFC, 
        items.TotalWeight AS ActualWeightPounds,
        items.TotalPallets AS ActualPallets,
        -- ai.TotalPalletSpaces AS ActualPalletSpaces,
        items.TotalVolume AS ActualVolumeCubicFeet,
        items.UnitQuantity AS ActualQuantity,
        CASE WHEN orders.hvr_isdelete = 1 THEN 1
            WHEN services.hvr_isdelete = 1 THEN 1 
            WHEN bill_to_order_parties.hvr_isdelete = 1 THEN 1
            WHEN mdm_customers.hvr_isdelete = 1 THEN 1
            WHEN service_items.hvr_isdelete = 1 THEN 1
            WHEN items.hvr_isdelete = 1 THEN 1
            ELSE 0 
        END Raptor_IsDelete
    FROM orders
    INNER JOIN services
        ON orders.CustomerOrderID = services.CustomerOrderID
    INNER JOIN bill_to_order_parties
        ON bill_to_order_parties.CustomerOrderID = orders.CustomerOrderID
    INNER JOIN mdm_customers
        ON mdm_customers.PartyNumber = bill_to_order_parties.PartyNumber
    INNER JOIN service_items
        ON services.ServiceID = service_items.ServiceID
    INNER JOIN items
        ON service_items.ItemID = items.CustomerOrderItemID


    {% if is_incremental() %}
    
    where updated_at >= (select max(updated_at) from {{ this }} )
    
    {% endif %}
), 

aggregated as (

    select
        {{ dbt_utils.surrogate_key([
            'ServiceNK_PK',
            'CustomerOrderItemID',
            'CustomerCode'
        ])}} as primary_key,
        max(NMFC) as NMFC,
        max(ActualWeightPounds) as ActualWeightPounds,
        max(ActualPallets) as ActualPallets,
        max(ActualVolumeCubicFeet) as ActualVolumeCubicFeet,
        max(ActualQuantity) as ActualQuantity
    
    from joined  

    group by 1
)

select * from aggregated order by 1
