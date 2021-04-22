USE [Raptor_Common]
GO

/****** Object:  StoredProcedure [CFA].[RAP_Location]    Script Date: 4/19/2021 1:35:21 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE   PROCEDURE [CFA].[RAP_Location]
    @p_StartDate DATETIME2,
    @p_EndDate DATETIME2,
    @p_IsCDC BIT
AS

/*
NAME: CFA.RAP_Location

TEST: EXEC CFA.RAP_Location '2018-10-01 12:58:27.0542270', '2018-11-11 12:58:27.0542270', 0

DESCRIPTION: 
	Get Location information

TABLES USED:
	
PARAMETERS:
	@p_StartDate DATETIME2
  , @p_EndDate DATETIME2
  , @p_IsCDC BIT

RETURN VALUES:
    @Sp_ReturnCode          --0=success; -1=failure

REVISION HISTORY:
DATE			VERSION		AUTHOR						COMMENTS
09/24/2018		1.0			Scott Nelson				Initial set-up
10/18/2018      1.1         Scott Nelson				Add Partitioning to ChangeRecord
11/09/2018		1.2			Anthony Nguyen				Update KeyPartition
11/14/2018		1.6			Anthony Nguyen				Change CDC logic to handle when Rapliform runs on NonCDC
11/20/2018		1.7			Mike Cloutier				Update LocationNK in temp tables to be NVARCHAR(550)
														Step 2.6 join to service to get to the base_location.  
														Added SourceValue to the ChangeRecord temp table.
12/04/2018		1.8			Mike Cloutier				WarehouseCode VARCHAR(50) from WarehouseCode VARCHAR(10)
														LocationNK NVARCHAR(340) from LocationNK NVARCHAR(550)
														Change Country NVARCHAR(100) from Country VARCHAR(2) 
12/13/2018		1.9			Mike Cloutier				Updated Table Name for MDM changes.
12/21/2018		1.10		Mike Cloutier				Update 2.5 Get changePK and then join to change records.
														Update 2.7 Get location number from location and then get to party.
04/19/2019		2.0			Mike Cloutier				REmove if cdc logic.
04/24/2019		2.1			Mike Cloutier				Update LocationName and WarehouseCode to NVARCHAR
03/19/2020		2.2			Mike Cloutier				Update step 2.9 to look at table name 'Base_Stops', not column name 'Base_Stops'
11/12/2020      3.0         Mary McCreary               Convert to HVR
12/01/2020      3.1         Mike Cloutier               Convert to HVR (MDM) 
02/15/2021      4.0         Mary McCreary               Convert to HVR (Express) 
*/

BEGIN
    SET NOCOUNT ON;

    --  Declare Local Variables--
    --DECLARE  @p_StartDate DATETIME2 = '09/01/2018 23:00:00',@p_EndDate DATETIME2 ='09/22/2018 23:59:59', @p_IsCDC BIT = 1
    DECLARE @KeyAudit_Start BIGINT,
            @KeyAudit_END BIGINT,
            @KeyBatch_Process BIGINT,
            @SpName VARCHAR(60),     --the name of the stored proc
            @ErrNum INT,             --local variable for ERROR_NUMBER()
            @ErrMsg VARCHAR(MAX),    --local variable for ERROR_MESSAGE()
            @LineNum INT,            --local variable for ERROR_LINE() 
            @Error_Msg VARCHAR(MAX), --string containing error message
            @Sp_ReturnCode INT,      --local variable for return code after executing another proc
            @Step VARCHAR(150);      --processing step message

    SET @Step = N'Step 0: Initialized Variables';
    SELECT @SpName = OBJECT_NAME(@@PROCID),
           @Sp_ReturnCode = 0;

    SET @KeyAudit_Start
        = CAST(DATEPART(YEAR, @p_StartDate) AS VARCHAR(4))
          + RIGHT('00' + CAST(DATEPART(MONTH, @p_StartDate) AS VARCHAR(2)), 2)
          + RIGHT('00' + CAST(DATEPART(DAY, @p_StartDate) AS VARCHAR(2)), 2)
          + RIGHT('00' + CAST(DATEPART(HOUR, @p_StartDate) AS VARCHAR(2)), 2)
          + RIGHT('00' + CAST(DATEPART(MINUTE, @p_StartDate) AS VARCHAR(2)), 2)
          + RIGHT('00' + CAST(DATEPART(SECOND, @p_StartDate) AS VARCHAR(2)), 2)
          + LEFT(RIGHT('000000' + CAST(DATEPART(NANOSECOND, @p_StartDate) AS VARCHAR(9)), 9), 5);

    SET @KeyAudit_END
        = CAST(DATEPART(YEAR, @p_EndDate) AS VARCHAR(4))
          + RIGHT('00' + CAST(DATEPART(MONTH, @p_EndDate) AS VARCHAR(2)), 2)
          + RIGHT('00' + CAST(DATEPART(DAY, @p_EndDate) AS VARCHAR(2)), 2)
          + RIGHT('00' + CAST(DATEPART(HOUR, @p_EndDate) AS VARCHAR(2)), 2)
          + RIGHT('00' + CAST(DATEPART(MINUTE, @p_EndDate) AS VARCHAR(2)), 2)
          + RIGHT('00' + CAST(DATEPART(SECOND, @p_EndDate) AS VARCHAR(2)), 2)
          + LEFT(RIGHT('000000' + CAST(DATEPART(NANOSECOND, @p_EndDate) AS VARCHAR(9)), 9), 5);

    SET @KeyBatch_Process
        = CAST(DATEPART(YEAR, SYSDATETIME()) AS VARCHAR(4))
          + RIGHT('00' + CAST(DATEPART(MONTH, SYSDATETIME()) AS VARCHAR(2)), 2)
          + RIGHT('00' + CAST(DATEPART(DAY, SYSDATETIME()) AS VARCHAR(2)), 2)
          + RIGHT('00' + CAST(DATEPART(HOUR, SYSDATETIME()) AS VARCHAR(2)), 2)
          + RIGHT('00' + CAST(DATEPART(MINUTE, SYSDATETIME()) AS VARCHAR(2)), 2)
          + RIGHT('00' + CAST(DATEPART(SECOND, SYSDATETIME()) AS VARCHAR(2)), 2)
          + LEFT(RIGHT('000000' + CAST(DATEPART(NANOSECOND, SYSDATETIME()) AS VARCHAR(9)), 9), 5);

    BEGIN TRY
        SET @Step = N'Step 1: Create Tables';
		IF (OBJECT_ID('tempdb..#ChangePK') IS NOT NULL) DROP TABLE #ChangePK
		CREATE TABLE #ChangePK
		(
			ChangeRecordID BIGINT NOT NULL
			,KeyPartition SMALLINT
		)


        IF (OBJECT_ID('tempdb..#ChangeMap') IS NOT NULL) DROP TABLE #ChangeMap;
        CREATE TABLE #ChangeMap
            (
              DatabaseName VARCHAR(100) ,
              SchemaName VARCHAR(50) ,
              TableName VARCHAR(100) ,
              ColumnName VARCHAR(150)
            );

        IF (OBJECT_ID('tempdb..#ChangeRecords') IS NOT NULL) DROP TABLE #ChangeRecords;
        CREATE TABLE #ChangeRecords
            (
              DatabaseName VARCHAR(100) ,
              SchemaName VARCHAR(50) ,
              TableName VARCHAR(100) ,
              ColumnName VARCHAR(150) ,
              Raptor_Sequence BIGINT ,
			  SourceValue int ,
			  SourceDescription VARCHAR(150),
              isDeleted BIT
            );

        IF (OBJECT_ID('tempdb..#CustomerOrderID_CC_Distinct') IS NOT NULL) DROP TABLE #CustomerOrderID_CC_Distinct;
        CREATE TABLE #CustomerOrderID_CC_Distinct ( CustomerOrderID INT );

		IF (OBJECT_ID('tempdb..#LoadNum_CC_Distinct') IS NOT NULL) DROP TABLE #LoadNum_CC_Distinct;
        CREATE TABLE #LoadNum_CC_Distinct ( LoadNum INT );

        IF (OBJECT_ID('tempdb..#LocationNK') IS NOT NULL) DROP TABLE #LocationNK;
        CREATE TABLE #LocationNK
            (
              LocationNK NVARCHAR(340) ,
              LocationName NVARCHAR(200) ,
              WarehouseCode NVARCHAR(50) ,
              Address1 NVARCHAR(200) ,
              Address2 NVARCHAR(200) ,
              City NVARCHAR(100) ,
              State NVARCHAR(100) ,
              Zip NVARCHAR(20) ,
              Country NVARCHAR(100) ,
            );

        IF (OBJECT_ID('tempdb..#LocationNK_Distinct') IS NOT NULL) DROP TABLE #LocationNK_Distinct;
        CREATE TABLE #LocationNK_Distinct
        (
            LocationNK NVARCHAR(340) ,
            Raptor_Version INT,
            LocationName NVARCHAR(200) NULL,
            WarehouseCode NVARCHAR(50) NULL,
            Address1 NVARCHAR(200) NULL,
            Address2 NVARCHAR(200) NULL,
            City NVARCHAR(100) NULL,
            State NVARCHAR(100) NULL,
            Zip NVARCHAR(20) NULL,
            Country NVARCHAR(100) NULL,
        );

		IF (OBJECT_ID('tempdb..#Base_Location') IS NOT NULL) DROP TABLE #Base_Location;
		CREATE TABLE #Base_Location
		(
		AddressNummber INT NOT NULL,
		LocationNumber INT NOT NULL,
		[Address1] [nvarchar] (200) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
		[Address2] [nvarchar] (200) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
		[City] [nvarchar] (100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
		[State] [nvarchar] (100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
		[PostalCode] [nvarchar] (20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
		[Country] [nvarchar] (100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
		) 

		CREATE CLUSTERED INDEX CI_RV_LN ON #Base_Location(LocationNumber)

   

		SET @Step = N'Step 2.4: Get the Database.Schema.Table.Columns to check from ChangeMap';
		INSERT  INTO #ChangeMap
				( DatabaseName ,
					SchemaName ,
					TableName ,
					ColumnName
				)
		SELECT  cm.DatabaseName ,
				cm.SchemaName ,
				cm.TableName ,
				cm.ColumnName
		FROM    Raptor_Common.dbo.ChangeMap cm ( NOLOCK )
		WHERE   MapName = 'CFA_Location';

		SET @Step = N'Step 2.5: Get all change records we need to pull this run, based on ChangeMap rules';
		INSERT INTO #ChangePK (ChangeRecordID,
		                       KeyPartition)

		SELECT DISTINCT cr.ChangeRecordID
				,cr.KeyPartition
		FROM    Raptor_Common.dbo.ChangeRecord cr (NOLOCK)
		INNER JOIN #ChangeMap t ON cr.DatabaseName = t.DatabaseName
									AND cr.SchemaName = t.SchemaName
									AND cr.TableName = t.TableName
									AND cr.ColumnName = t.ColumnName

		WHERE   cr.KeyAudit_Modified BETWEEN @KeyAudit_Start
										AND     @KeyAudit_END
				AND cr.IsDeleted = 0
				AND cr.IsTMC = 0
				AND cr.KeyPartition IN (DATEDIFF(DAY, '2018-01-01', @p_StartDate) % 14, DATEDIFF(DAY, '2018-01-01', @p_EndDate) % 14 );
		
		INSERT  INTO #ChangeRecords
				( DatabaseName ,
					SchemaName ,
					TableName ,
					ColumnName ,
					Raptor_Sequence ,
					SourceValue,
					SourceDescription,
					isDeleted
				)
		SELECT DISTINCT
				cr.DatabaseName ,
				cr.SchemaName ,
				cr.TableName ,
				cr.ColumnName ,
				cr.Raptor_Sequence ,
				cr.SourceValue,
				cr.SourceDescription,
				cr.IsDeleted
		FROM    #ChangePK CPK
		INNER JOIN Raptor_Common.dbo.ChangeRecord cr (NOLOCK)
		ON CPK.ChangeRecordID = cr.ChangeRecordID
		AND CPK.KeyPartition = cr.KeyPartition

		SET @Step = N'Step 2.6: Get data for Orion Location changes';
		INSERT  INTO #LocationNK
				( LocationNK ,
					LocationName ,
					WarehouseCode ,
					Address1 ,
					Address2 ,
					City ,
					State ,
					Zip ,
					Country
				)
		SELECT      dbo.udf_LocationNK(LP.PartyCode, LP.Name, RA.Country, RA.City, RA.State, RA.Address1) AS LocationNK,
					LP.Name,
					LP.PartyCode,
					RA.Address1,
					RA.Address2,
					RA.City,
					RA.State,
					RA.PostalCode,
					RA.Country
		  FROM      #ChangeRecords cr
		 INNER JOIN Orion_RAP.HVR.CO_Service S (NOLOCK)
			ON cr.SourceValue     = S.CustomerOrderID
		   AND S.hvr_isdelete   > -1
		 INNER JOIN Orion_RAP.HVR.CO_Location LO (NOLOCK)
		   ON LO.ServiceID       = S.ServiceID
		   AND LO.hvr_isdelete > -1
		 INNER JOIN Orion_RAP.HVR.Reference_Address RA (NOLOCK)
			ON RA.AddressNumber   = LO.AddressNumber
		   AND RA.hvr_isdelete > -1
		  LEFT JOIN MDMReport.HVR.mdm_Party AS LP (NOLOCK)
			ON LO.LocationNumber  = LP.PartyNumber
		 WHERE      cr.DatabaseName = 'Orion_RAP'
		   AND      cr.SchemaName        = 'CO'
		   AND      cr.TableName         = 'Base_Location'
		   AND      cr.isDeleted         = 0
		   AND      LP.hvr_isdelete		 = 0;

		SET @Step = N'Step 2.7: Get data for Orion Address changes';
		
		INSERT INTO #Base_Location (
		                            AddressNummber,
		                            LocationNumber,
		                            Address1,
		                            Address2,
		                            City,
		                            State,
		                            PostalCode,
		                            Country)
		SELECT RA.AddressNumber,
			   LO.LocationNumber,
               RA.Address1,
               RA.Address2,
               RA.City,
               RA.State,
               RA.PostalCode,
               RA.Country
        FROM  #ChangeRecords CR
		INNER JOIN Orion_RAP.HVR.Reference_Address RA ( NOLOCK ) ON 
		CR.SourceValue = RA.AddressNumber
		INNER JOIN Orion_RAP.HVR.CO_Location LO ( NOLOCK ) ON RA.AddressNumber = LO.AddressNumber
																AND RA.hvr_isdelete > -1

		 WHERE      cr.DatabaseName = 'Orion_RAP'
		   AND      cr.SchemaName        = 'Reference'
		   AND      cr.TableName         = 'Base_Address'
		   AND      cr.isDeleted         = 0
		   AND      RA.hvr_isdelete    > -1

		INSERT  INTO #LocationNK 
				( LocationNK ,
					LocationName ,
					WarehouseCode ,
					Address1 ,
					Address2 ,
					City ,
					State ,
					Zip ,
					Country
				)
		SELECT  dbo.udf_LocationNK(LP.PartyCode, LP.Name, RA.Country, RA.City, RA.State, RA.Address1) AS LocationNK , 
				LP.Name ,
				LP.PartyCode ,
				RA.Address1 ,
				RA.Address2 ,
				RA.City ,
				RA.State ,
				RA.PostalCode ,
				RA.Country
		FROM #Base_Location RA
		LEFT JOIN MDMReport.HVR.mdm_Party AS LP ( NOLOCK ) 
			ON RA.LocationNumber = LP.PartyNumber
			AND LP.hvr_isdelete > -1

		SET @Step = N'Step 2.8: Get data for MDM Party changes';
		INSERT  INTO #LocationNK
				( LocationNK ,
					LocationName ,
					WarehouseCode ,
					Address1 ,
					Address2 ,
					City ,
					State ,
					Zip ,
					Country
				)
		SELECT  dbo.udf_LocationNK(LP.PartyCode, LP.Name, RA.Country, RA.City,
									RA.State, RA.Address1) AS LocationNK ,
				LP.Name ,
				LP.PartyCode ,
				RA.Address1 ,
				RA.Address2 ,
				RA.City ,
				RA.State ,
				RA.PostalCode ,
				RA.Country
		FROM    #ChangeRecords cr
		INNER JOIN MDMReport.HVR.mdm_Party AS LP ( NOLOCK ) ON cr.SourceValue = LP.PartyNumber
		INNER JOIN Orion_RAP.HVR.CO_Location LO ( NOLOCK ) ON LO.LocationNumber = LP.PartyNumber
		INNER JOIN Orion_RAP.HVR.Reference_Address RA ( NOLOCK ) ON RA.AddressNumber = LO.AddressNumber
																		AND RA.hvr_isdelete > -1
		WHERE   cr.DatabaseName = 'MDMReport'
				AND cr.SchemaName = 'HVR'
				AND cr.TableName = 'Base_Party_PartyTypeID=8'
				AND cr.isDeleted = 0
				AND LP.hvr_isdelete > -1
                AND LO.hvr_isdelete > -1

		SET @Step = N'Step 2.9: Get data for Express location changes';
		INSERT  INTO #LocationNK
				( LocationNK ,
					LocationName ,
					WarehouseCode ,
					Address1 ,
					Address2 ,
					City ,
					State ,
					Zip ,
					Country
				)
		SELECT  dbo.udf_LocationNK(s.WarehouseCode, s.Name, s.Country, s.City,
									s.State, s.Address1) AS LocationNK ,
				s.Name ,
				s.WarehouseCode ,
				s.Address1 ,
				s.Address2 ,
				s.City ,
				s.State ,
				s.Zip ,
				s.Country
		FROM    #ChangeRecords cr
		INNER JOIN Express_RAP.hvr.dbo_Stops AS s ( NOLOCK ) ON CR.SourceValue = S.LoadNum
		INNER JOIN Express_RAP.hvr.dbo_Loads AS l ( NOLOCK ) ON s.LoadNum = l.LoadNum
																	AND s.hvr_isdelete > -1
		WHERE   cr.DatabaseName = 'Express_RAP'
				AND cr.TableName = 'Base_Stops'
				AND ISNULL(l.Source, '') <> 'EGO: ORION'
				AND cr.isDeleted = 0
				AND s.hvr_isdelete = 0

		SET @Step = N'Step 2.10: Get distinct location changes';
		INSERT  INTO #LocationNK_Distinct
				( LocationNK ,
					Raptor_Version ,
					LocationName ,
					WarehouseCode ,
					Address1 ,
					Address2 ,
					City ,
					State ,
					Zip ,
					Country
				)
		SELECT  DISTINCT
				LocationNK ,
				0 AS Raptor_Version ,
				LocationName ,
				WarehouseCode ,
				Address1 ,
				Address2 ,
				City ,
				State ,
				Zip ,
				Country
		FROM    #LocationNK;

        SET @Step = N'Step 3.0: Final Output';
        SELECT NEXT VALUE FOR CFA.Raptor_Sequence AS Raptor_Sequence,
               F.Raptor_Version AS Raptor_Version,
               @KeyBatch_Process AS KeyBatch_Process,
               F.LocationNK AS LocationNK_PK,
               F.LocationName,
               F.WarehouseCode,
               F.Address1,
               F.Address2,
               F.City,
               F.State,
               F.Zip,
               F.Country
        FROM #LocationNK_Distinct F;

    END TRY
    BEGIN CATCH

        --Capture the error information
        SELECT @Sp_ReturnCode = -1,
               @ErrNum = ERROR_NUMBER(),
               @ErrMsg = ERROR_MESSAGE(),
               @LineNum = ERROR_LINE(),
               @Error_Msg
                   = N'Procedure failed at ' + @Step + N' -- LineNumber=' + CAST(@LineNum AS VARCHAR) + N',Error='
                     + CAST(@ErrNum AS VARCHAR) + N',ErrorMsg=' + @ErrMsg;
        SELECT @Error_Msg = @SpName + ':: ' + @Error_Msg;

        RAISERROR(@Error_Msg, 16, 1);
    END CATCH;
    SET NOCOUNT OFF;
    RETURN (@Sp_ReturnCode);
END;







GO


