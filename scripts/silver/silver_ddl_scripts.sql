/*
	=====================================
	DDL for silver layer 
	=====================================
	- We create similar structure of table from bronze layer
	- Before creating table we check if it already exist in silver schema
	- If table already exist we drop the table and create it
*/
use DataWarehouse;

IF OBJECT_ID ( 'silver.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_cust_info

CREATE TABLE silver.crm_cust_info
(
	cst_id			INT,
	cst_key			NVARCHAR(30),
	cst_firstname		NVARCHAR(30),
	cst_lastname		NVARCHAR(30),
	cst_marital_status	NVARCHAR(30),
	cst_gndr	        NVARCHAR(30),
	cst_create_date		DATE,
	dwh_create_date         DATETIME2 DEFAULT GETDATE()
);

GO

IF OBJECT_ID ( 'silver.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_prd_info

CREATE TABLE silver.crm_prd_info
(
	prd_id			INT,
	prd_key			NVARCHAR(30),
	prd_nm			NVARCHAR(50),
	prd_cost		INT,
	prd_line		NVARCHAR(30),
	prd_start_dt	        DATETIME,
	prd_end_dt		DATETIME,
	dwh_create_date DATETIME2 DEFAULT GETDATE()

);

GO

IF OBJECT_ID ( 'silver.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE silver.crm_sales_details

CREATE TABLE silver.crm_sales_details
(
	sls_ord_num     NVARCHAR(30),
	sls_prd_key		NVARCHAR(30),
	sls_cust_id		INT,
	sls_order_dt	        INT,
	sls_ship_dt		INT,
	sls_due_dt		INT,
	sls_sales		INT,
	sls_quantity	        INT,
	sls_price		INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

GO


IF OBJECT_ID ( 'silver.erp_CUST_AZ12', 'U') IS NOT NULL
	DROP TABLE silver.erp_CUST_AZ12

CREATE TABLE silver.erp_CUST_AZ12
(
	CID		   VARCHAR(30),
	BDATE	           DATE,
	GEN		   VARCHAR(30),
	dwh_create_date    DATETIME2 DEFAULT GETDATE()
);

GO



IF OBJECT_ID ( 'silver.erp_LOC_A101', 'U') IS NOT NULL
	DROP TABLE silver.erp_LOC_A101

CREATE TABLE silver.erp_LOC_A101
(
	CID		    VARCHAR(30),
	CUNTRY	            VARCHAR(30),
	dwh_create_date     DATETIME2 DEFAULT GETDATE()
);

GO


IF OBJECT_ID ( 'silver.erp_PX_CAT_G1V2', 'U') IS NOT NULL
	DROP TABLE silver.erp_PX_CAT_G1V2

CREATE TABLE silver.erp_PX_CAT_G1V2
(
        ID		    NVARCHAR(30),
	CAT	            NVARCHAR(30),
	SUBCAT		    NVARCHAR(30),
	MAINTENANCE         NVARCHAR(30),
	dwh_create_date     DATETIME2 DEFAULT GETDATE()

);
