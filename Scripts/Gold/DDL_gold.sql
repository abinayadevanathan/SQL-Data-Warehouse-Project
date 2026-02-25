/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

--Building Dimension customers
/*
SELECT cst_id,
Count(*)
FROM 
(
*/
CREATE VIEW gold.dim_customers AS
SELECT 
ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
ci.cst_id As customer_id,
ci.cst_key As customer_number,
ci.cst_firstname As first_name,
ci.cst_lastname As last_name,
la.cntry As country,
ci.cst_material_status As marital_status,
CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr			--CRM is the the master table for gender info
	ELSE COALESCE(ca.GEN, 'n/a')
END as Gender,
ca.BDATE As birthdate,
ci.cst_create_date As create_date
FROM silver.crm_cust_info as ci
LEFT JOIN silver.erp_cust_AZ12 as ca
ON ci.cst_key = ca.CID
LEFT Join silver.erp_loc_A101 as la
On ci.cst_key = la.cid		--After joining table, check whether any duplicates were introduced by join logic.
/*
)t GROUP BY cst_id
HAVING count(*) > 1;	--No duplicates found
*/
--Build dimension products
CREATE VIEW gold.dim_products AS
--SELECT prd_key, count(*) FROM(		--To check duplicates 
SELECT
	row_number() over (order by pn.prd_start_dt,pn.prd_key) As product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.CAT AS category,
	pc.SUBCAT AS Sub_category,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM silver.crm_prd_info as pn
LEFT JOIN silver.erp_px_cat_g1v2 as pc
on pn.cat_id = pc.id
WHERE prd_end_dt IS NULL		--Filter out all historical data.
/*
)t GROUP BY prd_key
HAVING COUNT(*) >1;	
*/

--Building fact sales
CREATE VIEW gold.fact_sales AS
SELECT 
sd.sls_ord_num,
pr.product_number,
cu.customer_id,
sd.sls_order_dt,
sd.sls_ship_dt,
sd.sls_due_dt,
sd.sls_sales,
sd.sls_quantity,
sd.sls_price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id;

SELECT * FROM gold.fact_sales
