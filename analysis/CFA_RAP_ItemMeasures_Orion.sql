USE [Raptor_Common]
GO

/****** Object:  StoredProcedure [CFA].[RAP_ItemMeasures_Orion]    Script Date: 4/19/2021 1:39:36 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE [CFA].[RAP_ItemMeasures_Orion]
    @p_StartDate DATETIME2 
  , @p_EndDate DATETIME2 
  , @p_IsCDC BIT 

AS 

/*
NAME: CFA.ItemMeasures_Orion

TEST: EXEC Raptor_Common.CFA.RAP_ItemMeasures_Orion '2021-04-01 00:00:00.0000000', '2021-04-27  10:20:00.0000000', 1

DESCRIPTION: 
    Get Orion-Originated modes data at Service level

TABLES USED:
    
PARAMETERS:
    @p_StartDate DATETIME2
  , @p_EndDate DATETIME2
  , @p_IsCDC BIT

RETURN VALUES:
    @Sp_ReturnCode          --0=success; -1=failure

REVISION HISTORY:
DATE            VERSION        AUTHOR               COMMENTS
08/25/2020        1.0          Mary McCreary        Initial set-up using RAP_ItemMeasures_Orion
09/21/2020        1.1          Mary McCreary        Prevent Null delete from being inserted
11/09/2020        1.2          Mary McCreary        Scorched earth deletes, send a delete to be processed before every insert, convert to HVR (Orion)
12/01/2020        2.0          Mike Cloutier        Convert to HVR (MDM)
03/08/2021        3.0          Mary McCreary        Rewrite for HVR
04/02/2021		  4.0		   Scott Nelson			Remove isTMC logic completely, rely on tmcflag in spend table
04/08/2021		  4.1		   Scott Nelson			Exclude deleted actualitem rows in join
*/

BEGIN
    SET NOCOUNT ON;

--  Declare Local Variables
    DECLARE    
      @KeyAudit_Start                BIGINT
    , @KeyAudit_END                  BIGINT 
    , @KeyBatch_Process              BIGINT      
    , @SpName                        VARCHAR (60)   --the name of the stored proc
    , @ErrNum                        INT            --local variable for ERROR_NUMBER()
    , @ErrMsg                        VARCHAR (MAX)  --local variable for ERROR_MESSAGE()
    , @LineNum                       INT            --local variable for ERROR_LINE() 
    , @Error_Msg                     VARCHAR (MAX)  --string containing error message
    , @Sp_ReturnCode                 INT            --local variable for return code after executing another proc
    , @Step                          VARCHAR (150)  --processing step message

    SET @Step = N'Step 0: Initialized Variables'

        SELECT
              @SpName = OBJECT_NAME(@@PROCID)
            , @Sp_ReturnCode = 0;

        SET @KeyAudit_Start
            = CAST(DATEPART(YEAR, @P_StartDate) AS VARCHAR (4))
                + RIGHT('00' + CAST(DATEPART(MONTH, @p_StartDate) AS VARCHAR (2)), 2)
                + RIGHT('00' + CAST(DATEPART(DAY, @p_StartDate) AS VARCHAR (2)), 2)
                + RIGHT('00' + CAST(DATEPART(HOUR, @p_StartDate) AS VARCHAR (2)), 2)
                + RIGHT('00' + CAST(DATEPART(MINUTE, @p_StartDate) AS VARCHAR (2)), 2)
                + RIGHT('00' + CAST(DATEPART(SECOND, @p_StartDate) AS VARCHAR (2)), 2)
                + LEFT(RIGHT('000000' + CAST(DATEPART(NANOSECOND, @p_StartDate) AS VARCHAR (9)), 9), 5);

        SET @KeyAudit_END
            = CAST(DATEPART(YEAR, @p_EndDate) AS VARCHAR (4))
                + RIGHT('00' + CAST(DATEPART(MONTH, @p_EndDate) AS VARCHAR (2)), 2)
                + RIGHT('00' + CAST(DATEPART(DAY, @p_EndDate) AS VARCHAR (2)), 2)
                + RIGHT('00' + CAST(DATEPART(HOUR, @p_EndDate) AS VARCHAR (2)), 2)
                + RIGHT('00' + CAST(DATEPART(MINUTE, @p_EndDate) AS VARCHAR (2)), 2)
                + RIGHT('00' + CAST(DATEPART(SECOND, @p_EndDate) AS VARCHAR (2)), 2)
                + LEFT(RIGHT('000000' + CAST(DATEPART(NANOSECOND, @p_EndDate) AS VARCHAR (9)), 9), 5);

        SET @KeyBatch_Process
            = CAST(DATEPART(YEAR, SYSDATETIME()) AS VARCHAR (4))
                + RIGHT('00' + CAST(DATEPART(MONTH, SYSDATETIME()) AS VARCHAR (2)), 2)
                + RIGHT('00' + CAST(DATEPART(DAY, SYSDATETIME()) AS VARCHAR (2)), 2)
                + RIGHT('00' + CAST(DATEPART(HOUR, SYSDATETIME()) AS VARCHAR (2)), 2)
                + RIGHT('00' + CAST(DATEPART(MINUTE, SYSDATETIME()) AS VARCHAR (2)), 2)
                + RIGHT('00' + CAST(DATEPART(SECOND, SYSDATETIME()) AS VARCHAR (2)), 2)
                + LEFT(RIGHT('000000' + CAST(DATEPART(NANOSECOND, SYSDATETIME()) AS VARCHAR (9)), 9), 5);

    BEGIN TRY

        --DROP TABLE IF EXISTS #ChangeMap;
		--DROP TABLE IF EXISTS #Order;
        --DROP TABLE IF EXISTS #ItemMeasures_Ungrouped
        --DROP TABLE IF EXISTS #ItemMeasures
        --DROP TABLE IF EXISTS #Final


        SET @Step = N'Step 1.0: Create temp tables to identify records to process';


			CREATE TABLE #ChangeMap (DatabaseName VARCHAR(100),SchemaName VARCHAR(50),TableName VARCHAR(100),ColumnName VARCHAR(150));
			
			CREATE TABLE #Order (CustomerOrderID INT);

            CREATE TABLE #ItemMeasures_Ungrouped ( 
				ServiceNK VARCHAR(35), 
				CustomerOrderItemID INT, 
				CustomerOrderNumber INT, 
				CustomerCode VARCHAR(50), 
				NMFC VARCHAR(50), 
                ActualWeightPounds DECIMAL(14,4), 
				ActualPallets DECIMAL(14,4), 
				ActualPalletSpaces DECIMAL(14,4), 
				ActualVolumeCubicFeet DECIMAL(14,4), 
				ActualQuantity DECIMAL(19,4), 
				Raptor_IsDelete INT);

            CREATE TABLE #ItemMeasures ( 
				ServiceNK VARCHAR(35), 
				CustomerOrderItemID INT, 
				CustomerOrderNumber INT, 
				CustomerCode VARCHAR(50), 
				NMFC VARCHAR(50), 
				ActualWeightPounds DECIMAL(14,4), 
				ActualPallets DECIMAL(14,4), 
				ActualPalletSpaces DECIMAL(14,4), 
				ActualVolumeCubicFeet DECIMAL(14,4), 
				ActualQuantity DECIMAL(19,4), 
				Raptor_IsDelete INT);

            CREATE TABLE #Final (
				Raptor_Version INT,
				Raptor_IsDelete BIT, 
				ServiceNK VARCHAR(35), 
				CustomerOrderItemID INT, 
				CustomerOrderNumber INT, 
				isOrder BIT,
				CustomerCode VARCHAR(50), 
				NMFC VARCHAR(50), 
				ActualWeightPounds DECIMAL(14,4), 
				ActualPallets DECIMAL(14,4), 
				ActualPalletSpaces DECIMAL(14,4), 
				ActualVolumeCubicFeet DECIMAL(14,4), 
				ActualQuantity DECIMAL(19,4));


        SET @Step = N'Step 2.0: Get the Database.Schema.Table.Columns to check from ChangeMap';

            INSERT INTO #ChangeMap ( 
                DatabaseName,
                SchemaName,
                TableName,
                ColumnName)
            SELECT cm.DatabaseName,
                cm.SchemaName,
                cm.TableName,
                cm.ColumnName
            FROM Raptor_Common.dbo.ChangeMap cm (NOLOCK)
            WHERE MapName = 'CFA_Orion_ItemMeasures';                


        SET @Step = N'Step 2.1: Get all Order change records we need to pull this run, based on ChangeMap rules';

            INSERT INTO #Order (CustomerOrderID)
            SELECT DISTINCT cr.SourceValue
            FROM #ChangeMap t 
                INNER JOIN Raptor_Common.dbo.ChangeRecord cr (NOLOCK) 
                    ON cr.DatabaseName = t.DatabaseName
                        AND cr.SchemaName = t.SchemaName
                        AND cr.TableName = t.TableName
                        AND cr.ColumnName = t.ColumnName
            WHERE   cr.KeyAudit_Modified BETWEEN @KeyAudit_Start AND @KeyAudit_END
                    AND cr.IsDeleted > -1 -- this is on purpose
                    AND cr.IsTMC > -1 -- this is on purpose
                    AND cr.DatabaseName = 'Orion_RAP'
                    AND cr.KeyPartition IN (DATEDIFF(DAY,'2018-01-01',@p_StartDate) % 14,DATEDIFF(DAY,'2018-01-01',@p_EndDate) % 14);


        SET @Step = N'Step 3: Get full dataset update/inserts';

            INSERT INTO #ItemMeasures_Ungrouped
            (
                ServiceNK,
                CustomerOrderItemID,
                CustomerOrderNumber,
                CustomerCode,
                NMFC,
                ActualWeightPounds,
                ActualPallets,
                ActualPalletSpaces,
                ActualVolumeCubicFeet,
                ActualQuantity,
                Raptor_IsDelete
            )
            SELECT
               CONVERT(VARCHAR, S.ServiceID) + '|1|0|1' AS ServiceNK_PK,
               i.CustomerOrderItemID, --pk
               o.CustomerOrderNumber as ShipmentID,
               p.PartyCode as CustomerCode, --pk
               i.NMFC, --??
               ISNULL(ai.TotalWeight,i.TotalWeight) AS ActualWeightPounds,
               ISNULL(ai.TotalPallets,i.TotalPallets) AS ActualPallets,
               ai.TotalPalletSpaces AS ActualPalletSpaces,
               ISNULL(ai.TotalVolume,i.TotalVolume) AS ActualVolumeCubicFeet,
               ISNULL(ai.UnitQuantity,i.UnitQuantity) AS ActualQuantity,
               CASE WHEN o.hvr_isdelete = 1 THEN 1
                    WHEN s.hvr_isdelete = 1 THEN 1 
                    WHEN op.hvr_isdelete = 1 THEN 1
                    WHEN p.hvr_isdelete = 1 THEN 1
                    WHEN si.hvr_isdelete = 1 THEN 1
                    WHEN i.hvr_isdelete = 1 THEN 1
                    ELSE 0 END Raptor_IsDelete
           FROM #Order t
               INNER JOIN Orion_RAP.HVR.CO_Order AS o (NOLOCK) 
                   ON t.CustomerOrderID = o.CustomerOrderID
               INNER JOIN Orion_RAP.HVR.CO_Service s (NOLOCK)
                   ON o.CustomerOrderID = s.CustomerOrderID
               INNER JOIN Orion_RAP.HVR.CO_OrderParty OP (NOLOCK) 
                   ON OP.CustomerOrderID = o.CustomerOrderID
                       AND OP.PartyRoleRDN = 326
               INNER JOIN MDMReport.HVR.mdm_Party p (NOLOCK) 
                   ON p.PartyNumber = OP.PartyNumber
                       AND p.PartyTypeRDN = 7 --CUSTOMER 
                       AND p.hvr_isdelete = 0
               INNER JOIN Orion_RAP.HVR.CO_ServiceItem si (NOLOCK)
                   ON s.ServiceID = si.ServiceID
               INNER JOIN Orion_RAP.HVR.CO_Item i (NOLOCK)
                   ON si.ItemID = i.CustomerOrderItemID
               LEFT JOIN Orion_RAP.HVR.EP_ActualItem ai (NOLOCK)
                   ON i.CustomerOrderItemID = ai.CustomerOrderItemID
					AND ai.hvr_isdelete = 0 -- breaks grain if this is not done


        SET @Step = N'Step 4: Group item measures';

            INSERT INTO #ItemMeasures
            (
                ServiceNK,
                CustomerOrderItemID,
                CustomerOrderNumber,
                CustomerCode,
                NMFC,
                ActualWeightPounds,
                ActualPallets,
                ActualPalletSpaces,
                ActualVolumeCubicFeet,
                ActualQuantity,
                Raptor_IsDelete
            )
            SELECT
                ServiceNK,
                CustomerOrderItemID,
                CustomerOrderNumber,
                CustomerCode,
                NMFC,
                MAX(ActualWeightPounds),
                MAX(ActualPallets),
                MAX(ActualPalletSpaces),
                MAX(ActualVolumeCubicFeet),
                MAX(ActualQuantity),
                MAX(imu.Raptor_IsDelete)
           FROM #ItemMeasures_Ungrouped imu
           GROUP BY CustomerOrderNumber,
               ServiceNK,
               CustomerOrderItemID,
               CustomerCode,
               NMFC

        SET @Step = N'Step 5: Populate #Final';

            INSERT INTO #Final (
                Raptor_Version,
                Raptor_IsDelete,
                ServiceNK,
                CustomerOrderItemID,
                CustomerOrderNumber,
                isOrder,
                CustomerCode,
                NMFC,
                ActualWeightPounds,
                ActualPallets,
                ActualPalletSpaces,
                ActualVolumeCubicFeet,
                ActualQuantity
            )
            SELECT 0 Raptor_Version,
                   Raptor_IsDelete Raptor_IsDelete,
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
            FROM #ItemMeasures
        

        SET @Step = N'Step 6: Return data';

            SELECT NEXT VALUE FOR CFA.Raptor_Sequence AS Raptor_Sequence ,
                Raptor_Version,
                Raptor_IsDelete,
                @KeyBatch_Process AS KeyBatch_Process, 
                ServiceNK AS ServiceNK_PK,
                CustomerOrderItemID AS CustomerOrderItemID_PK,
                CustomerCode CustomerCode_PK,
                CustomerOrderNumber ShipmentID,
                isOrder,
                NMFC,
                ActualWeightPounds,
                ActualPallets,
                ActualPalletSpaces,
                ActualVolumeCubicFeet,
                ActualQuantity
            FROM #Final;
            
    END TRY

    BEGIN CATCH
        --Capture the error information
        SELECT
            @Sp_ReturnCode = -1
          , @ErrNum = ERROR_NUMBER()
          , @ErrMsg = ERROR_MESSAGE()
          , @LineNum = ERROR_LINE()
          , @Error_Msg
                = N'Procedure failed at ' + @Step + N' -- LineNumber=' + CAST(@LineNum AS VARCHAR) + N',Error='
                  + CAST(@ErrNum AS VARCHAR) + N',ErrorMsg=' + @ErrMsg;
        SELECT @Error_Msg = @SpName + ':: ' + @Error_Msg;

        RAISERROR(@Error_Msg, 16, 1);
    END CATCH;
    SET NOCOUNT OFF;
    RETURN (@Sp_ReturnCode);
END;





GO


