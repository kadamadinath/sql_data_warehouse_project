 -------------------------------------------------------------------------
 --Post Insert Data Quality Check for Silver Layer(silver.crm_cust_info)--
 -------------------------------------------------------------------------
 -- 1. Check for Duplicates and Handling NULL
 SELECT
 COUNT(*) AS id_count,
 cst_id
 FROM silver.crm_cust_info
 GROUP BY cst_id
 HAVING COUNT(*) > 1 OR cst_id IS NULL;

 -- 2. Check for unwanted spaces
 SELECT cst_firstname
 FROM
 silver.crm_cust_info
 WHERE cst_firstname != TRIM(cst_firstname);

 -- 3.Data Standardization and Consistency
 SELECT DISTINCT cst_gndr
 FROM silver.crm_cust_info;


 ------------------------------------------------------------------------
 --Post Insert Data Quality Check for Silver Layer(silver.crm_prd_info)--
 ------------------------------------------------------------------------
 -- 1. Check for Duplicates and Handling NULL
 SELECT
 COUNT(*) AS id_count,
 prd_id
 FROM silver.crm_prd_info
 GROUP BY prd_id
 HAVING COUNT(*) > 1 OR prd_id IS NULL;

 -- 2. Check for unwanted spaces
 SELECT prd_nm
 FROM
 silver.crm_prd_info
 WHERE prd_nm != TRIM(prd_nm);

 -- 3.Data Standardization and Consistency
 SELECT DISTINCT prd_line
 FROM silver.crm_prd_info;

 --4. Check for a valid date order
 SELECT * 
 FROM 
 silver.crm_prd_info
 WHERE prd_start_dt > prd_end_dt;

 SELECT * 
 FROM silver.crm_prd_info;

-----------------------------------------------------------------------------
--Post Insert Data Quality Check for Silver Layer(silver.crm_sales_details)--
-----------------------------------------------------------------------------
 -- 1. Check for Invalid Date Order
 SELECT
 * 
 FROM
 silver.crm_sales_details
 WHERE
 sls_ship_dt < sls_order_dt OR sls_due_dt < sls_order_dt;

 -- 2.Check Data Consistency in sls_sales, sls_quantity, and sls_price
 SELECT
 sls_sales,
 sls_quantity,
 sls_price
 FROM
 silver.crm_sales_details
 WHERE sls_sales != sls_quantity * sls_price
 OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
 OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0;

  SELECT * FROM silver.crm_sales_details

-------------------------------------------------------------------------
--Post Insert Data Quality Check for Silver Layer(silver.erp_cust_az12)--
-------------------------------------------------------------------------
 -- 1. Identify out-of-range Dates
SELECT
* 
FROM
silver.erp_cust_az12
WHERE BDATE> GETDATE() OR BDATE < '1924-01-01';

-- 2.Data Standardization and Consistency
SELECT DISTINCT GEN
FROM
silver.erp_cust_az12;

SELECT * FROM bronze.erp_cust_az12
SELECT * FROM silver.erp_cust_az12

------------------------------------------------------------------------
--Post Insert Data Quality Check for Silver Layer(silver.erp_loc_a101)--
------------------------------------------------------------------------
 -- 1. Data Standardization and Consistency
 SELECT DISTINCT CNTRY
 FROM
 silver.erp_loc_a101;

 SELECT * FROM bronze.erp_loc_a101;

 SELECT * FROM silver.erp_loc_a101;
---------------------------------------------------------------------------
--Post Insert Data Quality Check for Silver Layer(silver.erp_px_cat_g1v2)--
---------------------------------------------------------------------------
 SELECT *
 FROM silver.erp_px_cat_g1v2
 WHERE CAT != TRIM(CAT) OR SUBCAT != TRIM(SUBCAT) OR MAINTENANCE != TRIM(MAINTENANCE);

  -- 2.Data Standardization and Consistency
 SELECT DISTINCT MAINTENANCE
 FROM
 silver.erp_px_cat_g1v2;

 SELECT * FROM bronze.erp_px_cat_g1v2;

 SELECT * FROM silver.erp_px_cat_g1v2;
