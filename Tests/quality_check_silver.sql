/*
===============================================================================
QUALITY CHECKS
===============================================================================
Script Purpose:
  This script performs various data quality checks for data standardization, data consistency 
  and accuracy across the 'silver' schema. It includes checks for
    - Null or duplicate primary keys
    -Unwanted spaces in string fields
    -Data standardization and consistency.
    -Invalid date ranges and orders.
    -Data consistency between related fields.


Usage Notes:
    -Run these checks after data loading silver layer.
    -Investigate and resolve any discrepancies found during the checks.
=================================================================================  
*/
--Quality check for crm_cust_info table
--Check for nulls or duplicates in primary key
SELECT DB_NAME() AS Current_DB;
USE DataWarehouse;
SELECT 
cst_id,
Count(*)
FROM silver.crm_cust_info
Group by cst_id
HAVING count(*) > 1 or cst_id IS Null
;
SELECT
cst_firstname
FROM silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname);

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT * 
FROM silver.crm_cust_info;


--Quality Check on bronze.crm_prd_info
--Check for nulls and duplicates in primary key

Select prd_id,
count(*) AS Count_prd_id
from silver.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null;

USE DataWarehouse;

--Check for unwanted spaces on prd name
SELECT prd_nm
from silver.crm_prd_info
where prd_nm != TRIM(prd_nm);

--check for Nulls or Negative numbers on prd_cost

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;


--Check for data standardization and consistency
--Low cardinality columns

SELECT DISTINCT prd_line 
FROM silver.crm_prd_info;

--Check for invalid date orders
--(End date must not be earlier than the start date)

SELECT *
FROM Silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

SELECT * 
FROM silver.crm_prd_info;

--Quality Check on bronze.crm_sales_details
SELECT DB_NAME() AS CurrentDB;
USE DataWarehouse;
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_quantity,
sls_sales,
sls_price
FROM bronze.crm_sales_details;
--WHERE sls_ord_num != TRIM(sls_ord_num); NO SPACE, NO NEED OF TRANSFORMATION
--WHERE sls_prd_key  IN (SELECT prd_key from silver.crm_prd_info);		prd_key present in prd info table, Good quality
--WHERE sls_cust_id NOT IN (SELECT cst_id from silver.crm_cust_info);   Cst_id present in cust info table..-- Good quality

--Quality check of sls_order_dt

SELECT 
NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <=0 
OR LEN(sls_order_dt) != 8 
OR sls_order_dt > 20500101 
OR sls_order_dt < 19000101;

SELECT sls_order_dt from bronze.crm_sales_details;
SELECT sls_ship_dt from bronze.crm_sales_details;
SELECT sls_due_dt from bronze.crm_sales_details;

--Quality check of sls_ship_dt
SELECT 
NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <=0 
OR LEN(sls_ship_dt) != 8 
OR sls_ship_dt > 20500101 
OR sls_ship_dt < 19000101;

--Check for invalid date orders

SELECT 
*
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

--CHECK FOR NEGATIVE, ZERO, NULL AND SALES != QUANTITY * PRICE

SELECT DISTINCT
--sls_sales,
sls_quantity,
--sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
		Else sls_sales
END AS sls_sales,

CASE WHEN sls_price IS NULL OR sls_price <= 0 
		THEN sls_sales/NULLIF(sls_quantity,0)
		Else sls_price
END AS sls_price

FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales, sls_quantity, sls_price;


--QUALITY CHECK OF bronze.erp_cust_AZ12

SELECT 
CASE WHEN cid LIKE '%NAS%' THEN SUBSTRING (cid, 4, LEN(cid))
	ELSE cid
END AS cid,
bdate,
gen
FROM bronze.erp_cust_AZ12;

--IDENTIFY OUT-OF-RANGE DATES

SELECT DISTINCT
bdate
FROM bronze.erp_cust_AZ12
WHERE bdate < '1926-01-01' OR bdate > GETDATE();

--DATA STANDARDIZATION AND CONSISTENCY

SELECT Distinct
CASE WHEN UPPER(TRIM(Gen)) IN ('F','FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(Gen)) IN ('M','MALE') THEN 'Male'
	 ELSE 'N/A'
END AS Gen
FROM bronze.erp_cust_AZ12;


--QUALITY OF BRONZE.ERP_LOC_A101
--comparing cid of erp_loc_a101 with cid of crm_cust_info
SELECT
REPLACE(cid,'-', '') cid
FROM bronze.erp_loc_A101
WHERE REPLACE(cid,'-', '') NOT IN 
(SELECT 
cst_key
from silver.crm_cust_info);

--DATA STANDARDIZATION AND CONSISTENCY checks of cntry

SELECT DISTINCT
cntry
from bronze.erp_loc_A101
order by cntry;

SELECT DISTINCT
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	When TRIM(cntry) IN ('US','USA') THEN 'United States'
	When TRIM(cntry) IS Null OR TRIM(cntry) = '' THEN 'N/A'
	Else cntry
END AS Cntry
from bronze.erp_loc_A101
order by cntry;

SELECT * FROM silver.erp_loc_A101;

--QUALITY CHECK OF BRONZE.ERP_PX_CAT_G1V2

SELECT 
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2;

-- check for unwanted spaces 
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM (subcat) OR maintenance != TRIM(maintenance);

--Data standardization and data consistency
SELECT DISTINCT
maintenance
FROM bronze.erp_px_cat_g1v2;

