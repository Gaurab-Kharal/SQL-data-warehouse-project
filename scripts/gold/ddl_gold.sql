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

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
  DROP VIEW gold.dim_customers;

GO
CREATE VIEW gold.dim_customers AS
SELECT 
       ROW_NUMBER() 
       OVER(ORDER BY cst_create_date) AS customer_key
      ,ci.[cst_id]                    AS customer_id
      ,ci.[cst_key]                   AS customer_number
      ,ci.[cst_firstname]             AS first_name
      ,ci.[cst_lastname]              AS last_name
      ,la.[CUNTRY]                    AS country
      ,ci.[cst_marital_status]        AS marital_status
      ,ca.[BDATE]                     AS birth_date

      ,CASE 
            WHEN cst_gndr != 'n/a' THEN cst_gndr
            ELSE COALESCE(GEN, 'n/a')
       END AS gender
      ,ci.[cst_create_date]           AS create_date

  FROM [DataWarehouse].[silver].[crm_cust_info] AS ci
  LEFT JOIN [DataWarehouse].[silver].[erp_CUST_AZ12] AS ca
  ON ci.cst_key = ca.CID
  LEFT JOIN [DataWarehouse].[silver].[erp_LOC_A101] AS la
  ON ci.cst_key = la.CID;

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================

GO
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
  DROP VIEW gold.dim_products;

GO
CREATE VIEW gold.dim_products 
AS
SELECT
       ROW_NUMBER() 
       OVER(ORDER BY prd_id)             AS product_key
      ,pn.[prd_id]                       AS product_id
      ,pn.[prd_key]                      AS product_number
      ,pn.[prd_nm]                       AS product_name
      ,pn.[cat_id]                       AS category_id
      ,pc.[CAT]                          AS category 
      ,pc.[SUBCAT]                       AS subcategory
      ,pc.[MAINTENANCE]                  As maintenance                                   
      ,pn.[prd_cost]                     AS cost
      ,pn.[prd_line]                     AS product_line
      ,pn.[prd_start_dt]                 AS start_date


  FROM [DataWarehouse].[silver].[crm_prd_info] AS pn
  LEFT JOIN silver.erp_PX_CAT_G1V2 AS pc
  ON pn.cat_id = pc.ID
  where prd_end_dt IS NULL;


-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
DROP VIEW gold.fact_sales;
GO
CREATE VIEW gold.fact_sales AS

SELECT 
       sd.[sls_ord_num]           AS order_number
      ,pr.[product_key]
      ,cu.[customer_key]
      ,sd.[sls_order_dt]          AS order_date
      ,sd.[sls_ship_dt]           AS shipping_date
      ,sd.[sls_due_dt]            AS due_date
      ,sd.[sls_sales]             AS sales
      ,sd.[sls_quantity]          AS quantity
      ,sd.[sls_price]             AS price
  FROM [DataWarehouse].[silver].[crm_sales_details] AS sd
  LEFT JOIN gold.dim_products AS pr
  ON sd.sls_prd_key = pr.product_number
  LEFT JOIN gold.dim_customers AS cu
  ON sd.sls_cust_id = cu.customer_id;
GO
