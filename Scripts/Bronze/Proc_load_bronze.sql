/*
==========================================================================
Stored procedure: Load Bronze layer (Source --> Bronze)
==========================================================================
Script Purpose:
  This stored procedure loads data into the 'bronze' schema from external csv files.
  It performs the following actions:
  - Truncates the bronze tables before loading data.
  -uses the 'Bulk Insert' command to load data from csv files to bronze tables.
  -It includes Exception handling as well
  -It'll print the duration of the whole batch load and each table loading duration in seconds.
Parameters:
  None
This stored procedure does not accept any parameters or return any values.

Usage Example:
  EXEC bronze.load_bronze;
=============================================================================
*/
EXEC bronze.load_bronze;

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @End_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY			-- TRY ... CATCH for exception handling ; SQL runs the try block , and if it fails, it runs the CATCH block to handle the error
		PRINT '=========================================================================';
		SET @batch_start_time = GETDATE();
		PRINT 'Loading Bronze layer';
		PRINT '=========================================================================';
		PRINT 'Loading CRM tables';
		PRINT '-------------------------------------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>Truncating table :bronze.crm_cust_info';
		PRINT '-------------------------------------------------------------------------';
		PRINT '>>Inserting data into :bronze.crm_cust_info';
		PRINT '-------------------------------------------------------------------------';

		TRUNCATE TABLE bronze.crm_cust_info;           -- Empty the table before inserting bulk to make sure to avoid duplicates

		BULK INSERT bronze.crm_cust_info                 --Inserting bulk values into table from the source
		FROM 'C:\Users\abina\Desktop\DA Study Materials\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,               --Usually the firstrow might be a header with column names(it's not a data values) so instructing sql to start the fetch date from 2nd row
			FIELDTERMINATOR = ',',      --It's a file delimiter usually , ; | # " or separator b/w fields 
			TABLOCK                     --It will lock the entire table during loading bulk data
		);
		SET @End_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(Second, @start_time, @End_time) AS NVARCHAR) + 'seconds';

		PRINT '-------------------------------------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>Truncating table :bronze.crm_prd_info';
		PRINT '-------------------------------------------------------------------------';
		PRINT '>>Inserting data into :bronze.crm_prd_info';
		PRINT '-------------------------------------------------------------------------';

		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\abina\Desktop\DA Study Materials\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @End_time = GETDATE();
		PRINT '>>Load Duration:' + CAST(DATEDIFF(Second, @start_time, @End_time) AS NVARCHAR) + 'seconds';

		PRINT '-------------------------------------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>Truncating table :bronze.crm_sales_details';
		PRINT '-------------------------------------------------------------------------';
		PRINT '>>Inserting data into :bronze.crm_sales_details';
		PRINT '-------------------------------------------------------------------------';

		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\abina\Desktop\DA Study Materials\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			Fieldterminator = ',',
			TABLOCK
		);

		SET @End_time = GETDATE();
		PRINT '>>Load Duration:' + CAST(DATEDIFF(Second, @start_time, @End_time) AS NVARCHAR) + 'Seconds';
		PRINT '=========================================================================';
		PRINT 'Loading into ERP tables'
		PRINT '-------------------------------------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>Truncating table :bronze.erp_px_cat_g1v2';
		PRINT '-------------------------------------------------------------------------';
		PRINT '>>Inserting data into :bronze.erp_px_cat_g1v2';
		PRINT '-------------------------------------------------------------------------';


		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\abina\Desktop\DA Study Materials\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @End_time = GETDATE();
		PRINT '>>Load Duration:' + CAST(DATEDIFF(Second, @start_time, @End_time) AS NVARCHAR) + 'Seconds';

		PRINT '-------------------------------------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>Truncating table :bronze.erp_cust_AZ12';
		PRINT '-------------------------------------------------------------------------';
		PRINT '>>Inserting data into :bronze.erp_cust_AZ12';
		PRINT '-------------------------------------------------------------------------';

		TRUNCATE TABLE bronze.erp_cust_AZ12;
		BULK INSERT bronze.erp_cust_AZ12
		FROM 'C:\Users\abina\Desktop\DA Study Materials\sql-data-warehouse-project\datasets\source_erp\cust_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @End_time = GETDATE();
		PRINT '>>Load Duration:' + CAST(DATEDIFF(Second, @start_time, @End_time) AS NVARCHAR) + 'Seconds';
		PRINT '-------------------------------------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>Truncating table :bronze.erp_loc_A101';
		PRINT '-------------------------------------------------------------------------';
		PRINT '>>Inserting data into :bronze.erp_loc_A101';
		PRINT '-------------------------------------------------------------------------';

		TRUNCATE TABLE bronze.erp_loc_A101;
		BULK INSERT bronze.erp_loc_A101
		FROM 'C:\Users\abina\Desktop\DA Study Materials\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @End_time = GETDATE();
		PRINT '>>Load Duration:' + CAST(DATEDIFF(Second, @start_time, @End_time) AS NVARCHAR) + 'Seconds';
		PRINT'-------------------------';
		SET @batch_end_time = GETDATE();
		PRINT '======================================';
		PRINT 'Loading Bronze layer is completed';
		PRINT 'Total Load Duration: ' + CAST(DATEDIFF(SECOND , @batch_start_time , @batch_end_time) AS NVARCHAR) + 'Seconds';
		Print '======================================';
	END TRY
	BEGIN CATCH
	PRINT '==================================================================================';
	PRINT 'Error occured during loading bronze layer';
	PRINT 'Error Message' + ERROR_MESSAGE();
	PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
	PRINT '==================================================================================';

	THROW;

	END CATCH

END
