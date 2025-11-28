/* 

==============================
Truncate and Load silver layer
==============================

    This is the procedure to clean messy, unorganized and unreliable data in bronze layer and load them to 
    silver layer

    - First we truncate each table in silver layer.
    - Then we load cleaned data to silver layer.
    - Error handeling mechanisim is also implemented.

--------------------------------------------------------------------------------------------------------------

******
Note : 
******
Parameters does not return any value and only are used to 
loading time for each layer and whole loading procedure.

--------------------------------------------------------------------------------------------------------------

==================================
Use case : EXEC silver.load_silver
==================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN
BEGIN TRY
    DECLARE  @st_date DATE, @end_date DATE;
    DECLARE  @st_silver_date DATE, @end_silver_date DATE;

    PRINT '---------------------------------------'
    PRINT 'Loading the silver layer'
    PRINT '---------------------------------------'

    PRINT '---------------------------------------'
    PRINT 'Loading crm tables'
    PRINT '---------------------------------------'

    -- Truncating and Loading silver.crm_cust_info

    SET @st_silver_date = GETDATE();
    SET @st_date = GETDATE();
    PRINT '>>> Truncating table    :  silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;
    PRINT '>>> Inserting data into :  silver.crm_cust_info';
    INSERT INTO silver.crm_cust_info 
    (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)

    SELECT
			     cst_id,
			     cst_key,
		    TRIM (cst_firstname)      [cst_firstname],
		    TRIM (cst_lastname)       [cst_lastname],
		
		    CASE
			    WHEN  UPPER (TRIM (cst_marital_status)) = 'M' THEN 'Married'
			    WHEN  UPPER (TRIM (cst_marital_status)) = 'S' THEN 'Single'
			    ELSE 'n/a'
	        END [cst_marital_status],
		
		
		
		    CASE
			    WHEN  UPPER (TRIM (cst_gndr)) = 'M' THEN 'Male'
			    WHEN  UPPER (TRIM (cst_gndr)) = 'F' THEN 'Female'
			    ELSE 'n/a'
	        END [cst_gndr],
		    cst_create_date
	
    FROM 
	    (
		    SELECT 
			    *,
			    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY  cst_create_date DESC) [first_rank_id]
		    FROM 
			    bronze.crm_cust_info
			    WHERE cst_id IS NOT NULL
	    )t
	    WHERE first_rank_id = 1;
        SET @end_date = GETDATE();
        PRINT 'Load time for : silver.crm_cust_info :' + CAST (DATEDIFF(SECOND, @end_date, @st_date) AS VARCHAR) + 'sec';

    print '***********************************************************************************************************************************'
    ---------------------------------------------------------------------------------------------------------------------------------------------
    -- Truncating and Loading silver.crm_prd_info

    SET @st_date = GETDATE();
    PRINT '>>> Truncating table    :  silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;
    PRINT '>>> Inserting data into :  silver.crm_prd_info';
    INSERT INTO silver.crm_prd_info
    (
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
           [prd_id]
          ,REPLACE (SUBSTRING(prd_key,1, 5), '-', '_') AS [cat_id]
          ,SUBSTRING(prd_key, 7, len(prd_key))         AS [prd_key]
          ,[prd_nm]
          ,ISNULL ([prd_cost], 0) AS [prd_cost]
          ,CASE
                UPPER(TRIM(prd_line))
                WHEN  'M' THEN 'Mountain'
                WHEN  'R' THEN 'Road'
                WHEN  'S' THEN 'Other sales'
                WHEN  'T'  THEN 'Touring'
                ELSE 'n/a'
          END [prd_line]

          ,CAST ([prd_start_dt] AS DATE) [prd_start_dt]
          ,CAST (LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) [prd_end_dt]
    FROM 
         [DataWarehouse].[bronze].[crm_prd_info]
         SET @end_date = GETDATE();
         PRINT 'Load time for : silver.crm_prd_info :' + CAST (DATEDIFF(SECOND, @end_date, @st_date) AS VARCHAR) + 'sec';


    print '***********************************************************************************************************************************'
    ------------------------------------------------------------------------------------------------------------------------------------------
    -- Truncating and Loading silver.crm_sales_details

    SET @st_date = GETDATE();
    PRINT '>>> Truncating table    :  silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;
    PRINT '>>> Inserting data into :  silver.crm_sales_details';
    INSERT INTO silver.crm_sales_details 
    (
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

    SELECT 
         [sls_ord_num]
        ,[sls_prd_key]
        ,[sls_cust_id]
        ,CASE
              WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
              ELSE
              FORMAT(CAST (CAST(sls_order_dt AS VARCHAR) AS DATE), 'yyyy-MM-dd')
          END  AS [sls_order_dt]

          ,
           CASE
              WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                  ELSE
                  FORMAT(CAST (CAST(sls_ship_dt AS VARCHAR) AS DATE), 'yyyy-MM-dd') 
             END AS [sls_ship_dt]

          ,
          CASE 
          WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
                  ELSE
                  FORMAT(CAST (CAST(sls_due_dt AS VARCHAR) AS DATE), 'yyyy-MM-dd') 
          END AS [sls_due_dt]

           ,CASE
               WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS (sls_price)
               THEN sls_quantity * ABS (sls_price)
               ELSE ABS (sls_sales)
            END  [sls_sales]  

          ,[sls_quantity]

          ,CASE
                WHEN sls_price IS NULL OR  sls_price <= 0 
                THEN ABS(sls_sales)/sls_quantity 
                ELSE ABS(sls_price)
           END [sls_price]

    FROM [DataWarehouse].[bronze].[crm_sales_details]
        SET @end_date = GETDATE();
        PRINT 'Load time for : silver.crm_sales_details :' + CAST (DATEDIFF(SECOND, @end_date, @st_date) AS VARCHAR) + 'sec';


    print '***********************************************************************************************************************************'
    -------------------------------------------------------------------------------------------------------------------------------------------
	
    PRINT '---------------------------------------'
    PRINT 'Loading the erp tables'
    PRINT '---------------------------------------'
    
    -- Truncating and Loading silver.erp_CUST_AZ12

    SET @st_date = GETDATE();
    PRINT '>>> Truncating table    :  silver.erp_CUST_AZ12';
    TRUNCATE TABLE silver.erp_CUST_AZ12;
    PRINT '>>> Inserting data into :  silver.erp_CUST_AZ12';
    INSERT INTO silver.erp_CUST_AZ12
	    (
		    CID,
		    BDATE,
		    GEN	
	    )
	
	
    SELECT 
		    CASE 
			    WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
			    ELSE CID 
		    END [CID],

		    CASE
			    WHEN BDATE > GETDATE() THEN NULL 
			    ELSE BDATE
		    END AS [BDATE],

		    CASE 

			    WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE' ) THEN 'Female'
			    WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN  'Male'
			    ELSE 'n/a'

		    END AS [GEN]
		
    FROM 
	    bronze.erp_CUST_AZ12
        SET @end_date = GETDATE();
        PRINT 'Load time for : silver.erp_CUST_AZ12 :' + CAST (DATEDIFF(SECOND, @end_date, @st_date) AS VARCHAR) + 'sec';

    print '***********************************************************************************************************************************'
    ------------------------------------------------------------------------------------------------------------------------------------------

    -- Truncating and Loading silver.erp_LOC_A101

    SET @st_date = GETDATE();
    PRINT '>>> Truncating table   :   silver.erp_LOC_A101';
    TRUNCATE TABLE silver.erp_LOC_A101;
    PRINT '>>> Inserting data into :  silver.erp_LOC_A101';
    INSERT INTO silver.erp_LOC_A101

        (CID,
        CUNTRY)

    SELECT  
            REPLACE (CID, '-', '')  [CID]
           ,CASE 
            WHEN UPPER(TRIM(CUNTRY)) IN ('US', 'USA', 'UNITED STATES') THEN 'United States'
            WHEN UPPER(TRIM(CUNTRY)) IN ('DE',  'GERMANY') THEN 'Germany'
            WHEN TRIM(CUNTRY) = '' OR CUNTRY IS NULL THEN 'n/a'
            ELSE TRIM(CUNTRY)
        END AS [CUNTRY]
      FROM [DataWarehouse].[bronze].[erp_LOC_A101]
        SET @end_date = GETDATE();
        PRINT 'Load time for : silver.erp_LOC_A101 :' + CAST (DATEDIFF(SECOND, @end_date, @st_date) AS VARCHAR) + 'sec';

    print '***********************************************************************************************************************************'
    ------------------------------------------------------------------------------------------------------------------------------------------
    
     -- Truncating and Loading silver.erp_PX_CAT_G1V2

    SET @st_date = GETDATE();
    PRINT '>>> Truncating table    :  silver.erp_PX_CAT_G1V2';
    TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
    PRINT '>>> Inserting data into :  silver.erp_PX_CAT_G1V2';
    INSERT INTO silver.erp_PX_CAT_G1V2
    (
        ID,
        CAT,
        SUBCAT,
        MAINTENANCE
    ) 

    SELECT 
           [ID]
          ,[CAT]
          ,[SUBCAT]
          ,[MAINTENANCE]
      FROM 
         [DataWarehouse].[bronze].[erp_PX_CAT_G1V2];


        SET @end_date = GETDATE();
        PRINT 'Load time for : erp_PX_CAT_G1V2 : ' + CAST (DATEDIFF(SECOND, @end_date, @st_date) AS VARCHAR) + 'sec';

        print '***********************************************************************************************************************************'
        ------------------------------------------------------------------------------------------------------------------------------------------

        SET @end_silver_date = GETDATE();
        PRINT 'Load time for inserting/loading to silver layer : ' + CAST (DATEDIFF(SECOND, @end_silver_date, @st_silver_date) AS VARCHAR) + 'sec';
END TRY

BEGIN CATCH
    PRINT '|| Error occurred || : ' + 
    ERROR_MESSAGE();

    PRINT '|| Error severity || : ' + 
    CAST (ERROR_SEVERITY() AS NVARCHAR);

    PRINT '|| Error number || : ' + 
    CAST (ERROR_NUMBER() AS NVARCHAR);
    
    PRINT '|| Error line || : ' + 
    CAST (ERROR_LINE() AS NVARCHAR);


    PRINT '|| Error state || : ' + 
    CAST (ERROR_STATE() AS NVARCHAR);

END CATCH
END
