 -------------------------------------------------
 -- SQL Load scripts for BRONZE to SILVER LAYER --
 -------------------------------------------------
 
 CREATE OR ALTER PROCEDURE silver.load_silver 
 AS 
 BEGIN
    DECLARE @BatchStartTime DATETIME, @BatchEndTime DATETIME;
    DECLARE @StartTime DATETIME, @EndTime DATETIME;
	BEGIN TRY
		 -------------------------------------------------------
		 --Insering Data to Silver Layer(silver.crm_cust_info)--
		 -------------------------------------------------------
		 SET @BatchStartTime = GETDATE();
		 SET @StartTime = GETDATE();
		 PRINT'TRUNACTING TABLE silver.crm_cust_info'
		 TRUNCATE TABLE silver.crm_cust_info;
		 PRINT'INSERING DATA INTO silver.crm_cust_info'
		 INSERT INTO silver.crm_cust_info(
		 cst_id,
		 cst_key,
		 cst_firstname,
		 cst_lastname,
		 cst_marital_status,
		 cst_gndr,
		 cst_create_date
		 )
		 SELECT 
			   cst_id,
			   cst_key,
			   TRIM(cst_firstname) AS cst_firstname,
			   TRIM(cst_lastname) AS cst_lastname,
			   CASE
					WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
					WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Singal'
					ElSE 'n/a'
			   END AS cst_marital_status,
			   CASE
					WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
					WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
					ELSE 'n/a'
			   END AS cst_gndr,
			   cst_create_date
		 FROM
		 (
		 SELECT
		 ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag,
		 *
		 FROM bronze.crm_cust_info
		 WHERE cst_id IS NOT NULL
		 ) t
		 WHERE flag = 1;
		 SET @EndTime = GETDATE();
		 PRINT'Load duration for silver.crm_cust_info '+ CAST(DATEDIFF(second,@StartTime,@EndTime)AS NVARCHAR)+' seconds'
		 PRINT'**********************************************************'


		 ------------------------------------------------------
		 --Insering Data to Silver Layer(silver.crm_prd_info)--
		 ------------------------------------------------------
		 SET @StartTime = GETDATE();
		 PRINT'TRUNACTING TABLE silver.crm_prd_info'
		 TRUNCATE TABLE silver.crm_prd_info;
		 PRINT'INSERING DATA INTO silver.crm_prd_info'
		 INSERT INTO silver.crm_prd_info (
			prd_id,
			prd_cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt	
		 )
 
		 SELECT 
			  prd_id,
			  REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS prd_cat_id, --Extract Category ID
			  SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,          --Extract Product Key
			  prd_nm,
			  ISNULL(prd_cost,0)AS prd_cost,
			  CASE UPPER(TRIM(prd_line))
					WHEN 'M' THEN 'Mountain'
					WHEN 'S' THEN 'Other Sales'
					WHEN 'T' THEN 'Touring'
					WHEN 'R' THEN 'Road'
					ELSE 'n/a'
			  END AS prd_line,                                       -- Map product line codes to descriptive vales
			  CAST(prd_start_dt AS DATE) AS prd_start_dt,
			  CAST(DATEADD(DAY,-1,LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt))AS DATE)AS prd_end_dt --calculate end_date one day before next start_date
		  FROM bronze.crm_prd_info;
		  SET @EndTime = GETDATE();
		  PRINT'Load duration for silver.crm_prd_info '+ CAST(DATEDIFF(second,@StartTime,@EndTime)AS NVARCHAR)+' seconds'
		  PRINT'**********************************************************'

		 -----------------------------------------------------------
		 --Insering Data to Silver Layer(silver.crm_sales_details)--
		 -----------------------------------------------------------
		 SET @StartTime = GETDATE();
		 PRINT'TRUNACTING TABLE silver.crm_sales_details'
		 TRUNCATE TABLE silver.crm_sales_details;
		 PRINT'INSERING DATA INTO silver.crm_sales_details'
		 INSERT INTO silver.crm_sales_details
		 (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_price,
			sls_quantity
		)
		 SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
 			END AS	sls_order_dt,
			CASE 
				WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
 			END AS	sls_ship_dt,
			CASE 
				WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
 			END AS	sls_due_dt,
			CASE
				WHEN sls_sales <=0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales 
			END AS	sls_sales,
			sls_quantity,
			CASE
				WHEN sls_price <=0 OR sls_price IS NULL 
				THEN sls_sales / NULLIF(sls_quantity,0)
				ELSE sls_price
			END AS sls_price		
		FROM bronze.crm_sales_details;
		SET @EndTime = GETDATE();
		PRINT'Load duration for silver.crm_sales_details '+ CAST(DATEDIFF(second,@StartTime,@EndTime)AS NVARCHAR)+' seconds'
		PRINT'**********************************************************'

		 -----------------------------------------------------------
		 --Insering Data to Silver Layer(silver.crm_sales_details)--
		 -----------------------------------------------------------
		 SET @StartTime = GETDATE();
		 PRINT'TRUNACTING TABLE silver.erp_cust_az12'
		 TRUNCATE TABLE silver.erp_cust_az12;
		 PRINT'INSERING DATA INTO silver.erp_cust_az12'
		 INSERT INTO silver.erp_cust_az12
		 (
		 CID,
		 BDATE,
		 GEN
		 )
		 SELECT 
			CASE 
				WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID)) 
				ELSE CID
			END AS CID,	
			CASE 
				WHEN BDATE > GETDATE() THEN NULL
				ELSE BDATE
			END AS BDATE,
			CASE 
				WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
				WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
				ELSE 'n/a'
			END AS GEN
		FROM 
		bronze.erp_cust_az12;
		SET @EndTime = GETDATE();
		PRINT'Load duration for silver.erp_cust_az12 '+ CAST(DATEDIFF(second,@StartTime,@EndTime)AS NVARCHAR)+' seconds'
		PRINT'**********************************************************'

		 ------------------------------------------------------
		 --Insering Data to Silver Layer(silver.erp_loc_a101)--
		 ------------------------------------------------------
		 SET @StartTime = GETDATE();
		 PRINT'TRUNACTING TABLE silver.erp_loc_a101'
		 TRUNCATE TABLE silver.erp_loc_a101;
		 PRINT'INSERING DATA INTO silver.erp_loc_a101'
		 INSERT INTO silver.erp_loc_a101
		 (
		 CID,
		 CNTRY
		 )
		 SELECT
		  REPLACE(CID,'-','') AS CID,
		 CASE
			WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
			WHEN TRIM(CNTRY) IN('US','USA') THEN 'United States'
			WHEN TRIM(CNTRY) IS NULL  OR TRIM(CNTRY) = '' THEN 'n/a'
			ELSE TRIM(CNTRY)
		 END AS CNTRY    --- Normalize and handel missing or blank country codes
		 FROM
		 bronze.erp_loc_a101;
		 SET @EndTime = GETDATE();
		 PRINT'Load duration for silver.erp_loc_a101 '+ CAST(DATEDIFF(second,@StartTime,@EndTime)AS NVARCHAR)+' seconds'
		 PRINT'**********************************************************'

		 ---------------------------------------------------------
		 --Insering Data to Silver Layer(silver.erp_px_cat_g1v2)--
		 ---------------------------------------------------------
		 SET @StartTime = GETDATE();
		 PRINT'TRUNACTING TABLE silver.erp_px_cat_g1v2'
		 TRUNCATE TABLE silver.erp_px_cat_g1v2;
		 PRINT'INSERING DATA INTO silver.erp_px_cat_g1v2'
		 INSERT INTO silver.erp_px_cat_g1v2
		 (
		 ID,
		 CAT,
		 SUBCAT,
		 MAINTENANCE
		 )
		 SELECT
		 ID,
		 CAT,
		 SUBCAT,
		 MAINTENANCE
		 FROM 
		 bronze.erp_px_cat_g1v2;
		 SET @EndTime = GETDATE();
		 PRINT'Load duration for silver.erp_px_cat_g1v2 '+ CAST(DATEDIFF(second,@StartTime,@EndTime)AS NVARCHAR)+' seconds'
		 PRINT'**********************************************************'
		 SET @BatchEndTime = GETDATE();
		 PRINT'Total Load Duration for Silver Layer '+CAST(DATEDIFF(second,@BatchStartTime,@BatchEndTime)AS NVARCHAR)+' seconds'
	END TRY
	BEGIN CATCH
	PRINT'error occurred'+ ERROR_MESSAGE();
	PRINT'error message:'+ CAST(ERROR_NUMBER() AS NVARCHAR);
	END CATCH
END


--**************************************************************
EXEC silver.load_silver;
