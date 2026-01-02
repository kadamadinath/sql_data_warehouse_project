 -------------------------------------------------------
 -- SQL Load scripts for CSV files(using bulk insert) --
 -------------------------------------------------------


 CREATE OR ALTER PROCEDURE bronze.load_bronze
 AS
 BEGIN
    DECLARE @BatchStartTime DATETIME, @BatchEndTime DATETIME;
    DECLARE @StartTime DATETIME, @EndTime DATETIME;
	BEGIN TRY
	     SET @BatchStartTime = GETDATE();
		 PRINT'====================='
		 PRINT'Loading Bronze Layer'
		 PRINT'====================='
		 PRINT'Loading CRM Tables'
		 PRINT'====================='

		 SET @StartTime = GETDATE();
		 TRUNCATE TABLE bronze.crm_cust_info;
		 BULK INSERT bronze.crm_cust_info
		 FROM 'C:\Users\adina\Desktop\studyMaterial\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		 WITH
		 (
			 FIRSTROW = 2,
			 FIELDTERMINATOR = ',',
			 TABLOCK
		 )
		 SET @EndTime = GETDATE();
		 PRINT'Load duration for bronze.crm_cust_info: '+ CAST(DATEDIFF(second,@StartTime,@EndTime)AS NVARCHAR) +' seconds';

		 ---------------------------------------------------------------------------------------------------------------
		 SET @StartTime = GETDATE();
		 TRUNCATE TABLE bronze.crm_prd_info;
		 BULK INSERT bronze.crm_prd_info
		 FROM 'C:\Users\adina\Desktop\studyMaterial\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		 WITH
		 (
			 FIRSTROW = 2,
			 FIELDTERMINATOR = ',',
			 TABLOCK
		 )
		 SET @EndTime = GETDATE();
		 PRINT'Load duration for bronze.crm_prd_info: '+ CAST(DATEDIFF(second,@StartTime,@EndTime)AS NVARCHAR)+' seconds'

		 ----------------------------------------------------------------------------------------------------------------
		 SET @StartTime = GETDATE();
		 TRUNCATE TABLE bronze.crm_sales_details;
		 BULK INSERT bronze.crm_sales_details
		 FROM 'C:\Users\adina\Desktop\studyMaterial\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		 WITH
		 (
			 FIRSTROW = 2,
			 FIELDTERMINATOR = ',',
			 TABLOCK
		 )
		 SET @EndTime = GETDATE();
		 PRINT'Load duration for bronze.crm_sales_details: '+ CAST(DATEDIFF(second,@StartTime,@EndTime) AS NVARCHAR) +' seconds'

		 ---------------------------------------------------------------------------------------------------------------
	 
		 PRINT'====================='
		 PRINT'Loading ERP Tables'
		 PRINT'====================='

		 SET @StartTime = GETDATE();
		 TRUNCATE TABLE bronze.erp_cust_az12;
		 BULK INSERT bronze.erp_cust_az12
		 FROM 'C:\Users\adina\Desktop\studyMaterial\SQL\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		 WITH
		 (
			 FIRSTROW = 2,
			 FIELDTERMINATOR = ',',
			 TABLOCK
		 )
		 SET @EndTime = GETDATE();
		 PRINT'Load duration for bronze.erp_cust_az12: '+ CAST(DATEDIFF(second,@StartTime,@EndTime) AS NVARCHAR) +' seconds'

		 ----------------------------------------------------------------------------------------------------------------
		 SET @StartTime = GETDATE();
		 TRUNCATE TABLE bronze.erp_loc_a101;
		 BULK INSERT bronze.erp_loc_a101
		 FROM 'C:\Users\adina\Desktop\studyMaterial\SQL\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		 WITH
		 (
			 FIRSTROW = 2,
			 FIELDTERMINATOR = ',',
			 TABLOCK
		 )
		 SET @EndTime = GETDATE();
		 PRINT'Load duration for bronze.erp_loc_a101: '+ CAST(DATEDIFF(second,@StartTime,@EndTime)AS NVARCHAR)+' seconds'

		 ----------------------------------------------------------------------------------------------------------------
		 SET @StartTime = GETDATE();
		 TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		 BULK INSERT bronze.erp_px_cat_g1v2
		 FROM 'C:\Users\adina\Desktop\studyMaterial\SQL\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		 WITH
		 (
			 FIRSTROW = 2,
			 FIELDTERMINATOR = ',',
			 TABLOCK
		 )
		 SET @EndTime = GETDATE();
		 PRINT'Load duration for bronze.erp_px_cat_g1v2: '+ CAST(DATEDIFF(second,@StartTime,@EndTime) AS NVARCHAR) +' seconds'
		 SET @BatchEndTime = GETDATE();
		 PRINT'==================================================================='
		 PRINT'Total duration for whole bronze layer:'+ CAST(DATEDIFF(second,@BatchStartTime,@BatchEndTime)AS NVARCHAR) +' seconds'
	END TRY
	BEGIN CATCH
		PRINT'========================================'
		PRINT'ERROR OCCURRED DURING LOADING BRONZE LAYER'
		PRINT'========================================'
		PRINT'error message:'+ ERROR_MESSAGE();
		PRINT'error message:'+ CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'========================================'
	END CATCH
END
 ----------------------------------------------------------------------------------------------------------------

------------------------
EXEC bronze.load_bronze
------------------------
