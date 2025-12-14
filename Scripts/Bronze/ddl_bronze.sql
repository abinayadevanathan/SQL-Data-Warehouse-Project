/*
============================================================================
DDL Script: create Bronze tables
============================================================================
Script purpose:
  This script creates tables in the 'bronze' schema, dropping existingtables
  If they already exist.
Also did some alter changes on column datatype as it's raw data,
Having some null and missing values throws invalid datatypes error.
Run this script to re-define the DDL Structure of 'bronze' Tables
=============================================================================
*/


-- Creating and defining tables using DDL commands

CREATE Table bronze.crm_cust_info(

	cst_id INT,
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR(50),
	cst_material_status VARCHAR(50),
	cst_gndr VARCHAR(50),
	cst_create_date DATE
);

SELECT @@version;

CREATE Table bronze.crm_prd_info(


prd_id INT,
prd_key VARCHAR(50),
prd_nm VARCHAR(50),
prd_cost INT,
prd_line VARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE

);

/*
SELECT DB_NAME() AS CurrentDatabase; -- To view the currently using DB

SELECT name FROM sys.schemas; --To view the existing schemas in current DB
*/

CREATE TABLE bronze.crm_sales_details(

sls_ord_num INT,
sls_prd_key NVARCHAR(50),
sls_cust_id	INT,
sls_order_dt DATE,
sls_ship_dt	DATE,
sls_due_dt DATE,
sls_sales DATE,
sls_quantity INT,
sls_price INT
);

ALTER TABLE bronze.crm_sales_details
ALTER COLUMN sls_ord_num NVARCHAR(50);

ALTER TABLE bronze.crm_sales_details
ALTER COLUMN sls_sales NVARCHAR(50);

ALTER TABLE bronze.crm_sales_details
ALTER COLUMN sls_order_dt NVARCHAR(50);


IF OBJECT_ID ('bronze.erp_cust_AZ12', 'U') IS NOT NULL    --Best practice to check if the table already exists before creating a table
	DROP TABLE bronze.erp_cust_AZ12;                     --If exists then delete it and create a new table
CREATE TABLE bronze.erp_cust_AZ12(

CID	INT,
BDATE DATE,
GEN NVARCHAR(50)

);

ALTER TABLE bronze.erp_cust_AZ12
ALTER COLUMN CID VARCHAR(50);

GO
IF OBJECT_ID ('bronze.erp_loc_A101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_A101;
CREATE TABLE bronze.erp_loc_A101(

CID	NVARCHAR(50),
CNTRY NVARCHAR(50)

);

GO

CREATE TABLE bronze.erp_px_cat_g1v2(

ID	INT,
CAT	NVARCHAR(50),
SUBCAT NVARCHAR(50),
MAINTENANCE NVARCHAR(50)

);

ALTER TABLE bronze.erp_px_cat_g1v2
ALTER COLUMN ID VARCHAR(50);
