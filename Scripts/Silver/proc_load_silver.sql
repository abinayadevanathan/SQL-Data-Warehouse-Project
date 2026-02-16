/*
============================================================
Stored Procedure: Load silver layer(Bronze --> Silver)
============================================================
Script Purpose:
  This stored procedure performs the ETL(Extract, Transform and load) process to 
  populate the 'silver' schema tables from the 'bronze' schema.

Action Performed:
  Truncate silver tables
  Inserts transformed and cleansed data from bronze into silver table.

Parameters:
  None
  This stored procedure doesnot accept any parameters or returns any values.

Usage Example:
  EXEC silver.load_silver
==========================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
				PRINT '=============================================';
				PRINT 'LOADING SILVER LAYER';
				PRINT '=============================================';
				PRINT '---------------------------------------------';
				PRINT 'Loading CRM tables';
				PRINT '---------------------------------------------';
			--loading silver.crm_cust_info
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.crm_cust_info';
			TRUNCATE table silver.crm_cust_info;
			Print '>>Inserting data into: silver.crm_cust_info';
			INSERT INTO silver.crm_cust_info (
				cst_id,
				cst_key,
				cst_firstname,
				cst_lastname,
				cst_gndr,
				cst_material_status,
				cst_create_date)
			SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) As cst_firstname,  -- Transformation of removing unwanted spaces
			TRIM(cst_lastname) As cst_lastname,  -- Transformation of removing unwanted spaces
			CASE WHEN Upper(TRIM(cst_gndr)) = 'F' THEN 'Female'  -- Transforming Low cardinal columns --> In DWH, no abbreviations, it should be readable	
				 WHEN Upper(TRIM(cst_gndr)) = 'M' THEN 'Male'
				 Else 'N/A'
			END cst_gndr,
			CASE WHEN Upper(TRIM(cst_material_status)) = 'S' Then 'Single' --Normalize marital status values to readable format
				 WHEN Upper(Trim (cst_material_status)) = 'M' Then 'Married'
				 Else 'N/A'
			END cst_material_status,
			cst_create_date
			FROM
			(
			SELECT 
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last --TRANSFORMATION of removing duplicates
			from bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
			)t WHERE flag_last = 1;  -- t is an alias for the subquery , temporary table name // flag_last - select the most recent record per customer.
			SET @End_time = GETDATE();
				PRINT '>> Load Duration: '+ CAST(DATEDIFF(Second, @start_time, @End_time) AS NVARCHAR) + 'seconds';
				PRINT '------------------------';

			--LOADING silver.crm_prd_info
			SET @start_time = GETDATE();
			--TRANSFORMATION OF TABLE BRONZE.CRM_PRD_INFO

			PRINT '>> Truncating Table: silver.crm_prd_info';
			TRUNCATE table silver.crm_prd_info;
			Print '>>Inserting data into: silver.crm_prd_info';
			INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
			)
			SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-' , '_') AS Cat_id, --substring extracts a specific part of a string value, "Extracts Category ID"
			SUBSTRING(prd_key, 7, LEN(prd_key)) As prd_key,		--Extracts product key
			prd_nm,
			ISNULL(Prd_cost, 0) As Prd_cost,	--ISNULL function will used to replace the null value by 0.
			CASE UPPER(TRIM(prd_line))			--Quick Case when ideal for simple value mapping DATA STANDARDIZATION
				WHEN 'M' THEN 'Mountain'              
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other sales'
				WHEN 'T' THEN 'Touring'
				Else 'N/a'
			END AS prd_line,
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(
					DATEADD(
						DAY, 
						-1,
						LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)  --LEAD function is used to access value of the next row within same window
					) 
					AS DATE
				) AS prd_end_dt		
			FROM bronze.crm_prd_info;
			SET @End_time = GETDATE();
				PRINT '>> Load Duration: '+ CAST(DATEDIFF(Second, @start_time, @End_time) AS NVARCHAR) + 'seconds';
				PRINT '------------------------';

			--INSERTION INTO SILVER.CRM_SALES_DETAILS
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.crm_sales_details';
			TRUNCATE table silver.crm_sales_details;
			Print '>>Inserting data into: silver.crm_sales_details';
			INSERT INTO silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
			)
			--TRANSFORMATION OF TABLE BRONZE.CRM_SALES_DETAILS

			SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END as sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
					THEN sls_quantity * ABS(sls_price)
					Else sls_sales
			END AS sls_sales,
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <= 0 
					THEN sls_sales/NULLIF(sls_quantity,0)
					Else sls_price
			END AS sls_price
			FROM bronze.crm_sales_details;
			SET @End_time = GETDATE();
				PRINT '>> Load Duration: '+ CAST(DATEDIFF(Second, @start_time, @End_time) AS NVARCHAR) + 'seconds';
				PRINT '------------------------';
			    PRINT '---------------------------------------------';
				PRINT 'Loading ERP tables';
				PRINT '---------------------------------------------';
			--INSERTION INTO silver.erp_cust_az12
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.erp_cust_AZ12';
			TRUNCATE table silver.erp_cust_AZ12;
			Print '>>Inserting data into: silver.erp_cust_AZ12';
			INSERT INTO silver.erp_cust_AZ12(cid,bdate,gen)
			--TRANSFORMATION OF bronze.erp_cust_az12
			SELECT 
			CASE WHEN cid LIKE '%NAS%' THEN SUBSTRING (cid, 4, LEN(cid))		--Remove 'NAS' prefix if present
				ELSE cid
			END AS cid,
			CASE WHEN bdate > GETDATE() THEN Null								--Set future birthdate to null
				else bdate
			END AS Bdate,
			CASE WHEN UPPER(TRIM(Gen)) IN ('F','FEMALE') THEN 'Female'			--Normalize gender values and handle unknown cases
				 WHEN UPPER(TRIM(Gen)) IN ('M','MALE') THEN 'Male'
				 ELSE 'N/A'
			END AS Gen
			FROM bronze.erp_cust_AZ12;
			SET @End_time = GETDATE();
				PRINT '>> Load Duration: '+ CAST(DATEDIFF(Second, @start_time, @End_time) AS NVARCHAR) + 'seconds';
				PRINT '------------------------';


			--INSERTION INTO silver.erp_loc_a101
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.erp_loc_A101';
			TRUNCATE table silver.erp_loc_A101;
			Print '>>Inserting data into: silver.erp_loc_A101';
			INSERT INTO silver.erp_loc_A101(cid,cntry)
			--TRANSFORMATION OF BRONZE.ERP_LOC_A101
			SELECT
			REPLACE(cid,'-', '') cid,
			CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				When TRIM(cntry) IN ('US','USA') THEN 'United States'
				When TRIM(cntry) IS Null OR TRIM(cntry) = '' THEN 'N/A'
				Else cntry
			END AS Cntry
			FROM bronze.erp_loc_A101;
			SET @End_time = GETDATE();
				PRINT '>> Load Duration: '+ CAST(DATEDIFF(Second, @start_time, @End_time) AS NVARCHAR) + 'seconds';
				PRINT '------------------------';

			--INSERTION OF BRONZE.ERP_PX_CAT_G1V2
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
			TRUNCATE table silver.erp_px_cat_g1v2;
			Print '>>Inserting data into: silver.erp_px_cat_g1v2';
			INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
			SELECT 
			id,
			cat,
			subcat,
			maintenance
			FROM bronze.erp_px_cat_g1v2;
			SET @End_time = GETDATE();
				PRINT '>> Load Duration: '+ CAST(DATEDIFF(Second, @start_time, @End_time) AS NVARCHAR) + 'seconds';
				PRINT '------------------------';
		SET @batch_end_time = GETDATE();
			PRINT '==============================================';
			PRINT 'LOADING SILVER LAYER IS COMPLETED';
			PRINT ' - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'Seconds';
			PRINT '==============================================';
	END TRY

	BEGIN CATCH
		print '===============================================';
		print 'Error occured during loading silver layer'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '================================================';
	END CATCH
END

