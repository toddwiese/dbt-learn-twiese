with ItemMeasures_Ungrouped as (
    SELECT
        (TO_VARCHAR(S.ServiceID) || '|1|0|1') AS ServiceNK,
        i.CustomerOrderItemID AS CustomerOrderItemID, --pk
        o.CustomerOrderNumber AS CustomerOrderNumber,
        p.PartyCode AS CustomerCode, --pk
        i.NMFC AS NMFC, --??
        IFNULL(ai.TotalWeight,i.TotalWeight) AS ActualWeightPounds,
        IFNULL(ai.TotalPallets,i.TotalPallets) AS ActualPallets,
        ai.TotalPalletSpaces AS ActualPalletSpaces,
        IFNULL(ai.TotalVolume,i.TotalVolume) AS ActualVolumeCubicFeet,
        IFNULL(ai.UnitQuantity,i.UnitQuantity) AS ActualQuantity,
        CASE WHEN o.hvr_isdelete = 1 THEN 1
            WHEN s.hvr_isdelete = 1 THEN 1 
            WHEN op.hvr_isdelete = 1 THEN 1
            WHEN p.hvr_isdelete = 1 THEN 1
            WHEN si.hvr_isdelete = 1 THEN 1
            WHEN i.hvr_isdelete = 1 THEN 1
            ELSE 0 END Raptor_IsDelete
        FROM raw.shipping_data.CO_Order o
        INNER JOIN raw.shipping_data.CO_Service s (NOLOCK)
            ON o.CustomerOrderID = s.CustomerOrderID
        INNER JOIN raw.shipping_data.CO_OrderParty OP (NOLOCK) 
            ON OP.CustomerOrderID = o.CustomerOrderID
                AND OP.PartyRoleRDN = 326
        INNER JOIN raw.shipping_data.mdm_Party p (NOLOCK) 
            ON p.PartyNumber = OP.PartyNumber
                AND p.PartyTypeRDN = 7 --CUSTOMER 
                AND p.hvr_isdelete = 0
        INNER JOIN raw.shipping_data.CO_ServiceItem si (NOLOCK)
            ON s.ServiceID = si.ServiceID
        INNER JOIN raw.shipping_data.CO_Item i (NOLOCK)
            ON si.ItemID = i.CustomerOrderItemID
        LEFT JOIN raw.shipping_data.EP_ActualItem ai (NOLOCK)
            ON i.CustomerOrderItemID = ai.CustomerOrderItemID
			AND ai.hvr_isdelete = 0 -- breaks grain if this is not done
),

ItemMeasures as (
    SELECT
        ServiceNK,
        CustomerOrderItemID,
        CustomerOrderNumber,
        CustomerCode,
        NMFC,
        MAX(ActualWeightPounds) as ActualWeightPounds,
        MAX(ActualPallets) as ActualPallets,
        MAX(ActualPalletSpaces) as ActualPalletSpaces,
        MAX(ActualVolumeCubicFeet) as ActualVolumeCubicFeet,
        MAX(ActualQuantity) as ActualQuantity,
        MAX(imu.Raptor_IsDelete) as Raptor_IsDelete
    FROM ItemMeasures_Ungrouped imu
    GROUP BY CustomerOrderNumber,
        ServiceNK,
        CustomerOrderItemID,
        CustomerCode,
        NMFC
),

Final as (
    SELECT 0 Raptor_Version,
        Raptor_IsDelete AS Raptor_IsDelete,
        ServiceNK,
        CustomerOrderItemID,
        CustomerOrderNumber,
        1 IsOrder,
        CustomerCode,
        NMFC,
        ActualWeightPounds,
        ActualPallets,
        ActualPalletSpaces,
        ActualVolumeCubicFeet,
        ActualQuantity
    FROM ItemMeasures
)

select * from Final
   
