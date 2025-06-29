/*
======================================================
Stored Procedure : Scripts for loading bronze layer
======================================================
We are loading data to our previously created tables from different source crm and erp.
Each source contains three tables.

*****************************************************
Loading method : Truncate and Bulk Insert
*****************************************************
- First we truncate for each table to delete every row without deleting table.
- Then we load data from source table in bulk which is fast and reliable.
- Scripts also checks for errors and handels them.
- Loading time is calculated for each table load and full load.

******************************************************
Parameters use case : . NONE
******************************************************
There is no use of any parameters so the procedure returns no value when executed.

USE CASE : EXEC bronze.load_bronze_data
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze_data AS
BEGIN
	BEGIN TRY
	  DECLARE @st_time_wholebatch DATETIME, @end_time_wholebatch DATETIME;
		DECLARE @st_time DATETIME, @end_time DATETIME;
		
		SET @st_time_wholebatch = GETDATE();
	  PRINT '********************************************************';
		PRINT 'Loading bronze layer data';
		PRINT '********************************************************';

		PRINT '________________________________________________________';
		PRINT 'Loading crm data';
		PRINT '________________________________________________________';

		
		SET @st_time = GETDATE();
		PRINT 'Truncate table data : [bronze.crm_cust_info]'
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT 'Loading data to table : [bronze.crm_cust_info]'
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\DataProjects\DataWarehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH
		(
		  FIRSTROW = 2,
		  FIELDTERMINATOR = ',',
		  TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Time :' + CAST(DATEDIFF(second, @st_time, @end_time) AS NVARCHAR) + 'sec';
		PRINT '-------------------';
	

		SET @st_time = GETDATE();
	    PRINT 'Truncate table data : [bronze.crm_prd_info]'
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT 'Loading data to table : [bronze.crm_prd_info]'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\DataProjects\DataWarehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH
		(
		  FIRSTROW = 2,
		  FIELDTERMINATOR = ',',
		  TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Time :' + CAST(DATEDIFF(second, @st_time, @end_time) AS NVARCHAR) + 'sec';
		PRINT '-------------------';



		SET @st_time = GETDATE();
	    PRINT 'Truncate table data : [bronze.crm_sales_details]'
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT 'Loading data to table : [bronze.crm_sales_details]'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\DataProjects\DataWarehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH
		(
		  FIRSTROW = 2,
		  FIELDTERMINATOR = ',',
		  TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Time :' + CAST(DATEDIFF(second, @st_time, @end_time) AS NVARCHAR) +  'sec';
		PRINT '-------------------';
	
	    PRINT '________________________________________________________'
		PRINT 'Loading erp data'
		PRINT '________________________________________________________'


		SET @st_time = GETDATE();
		PRINT 'Truncate table data : [bronze.erp_CUST_AZ12]'
		TRUNCATE TABLE bronze.erp_CUST_AZ12;
		PRINT 'Loading data to table : [bronze.erp_CUST_AZ12]'
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\DataProjects\DataWarehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK

		);
		SET @end_time = GETDATE();
		PRINT 'Load Time :' + CAST(DATEDIFF(second, @st_time, @end_time) AS NVARCHAR) + 'sec';
		PRINT '-------------------';

		
		SET @st_time = GETDATE();
		PRINT 'Truncate table data : [bronze.erp_LOC_A101]'
		TRUNCATE TABLE bronze.erp_LOC_A101;
		PRINT 'Loading data to table : [bronze.erp_LOC_A101]'
		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\DataProjects\DataWarehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK

		);
		SET @end_time = GETDATE();
		PRINT 'Load Time :' + CAST(DATEDIFF(second, @st_time, @end_time) AS NVARCHAR) + 'sec';
		PRINT '-------------------';

	
		SET @st_time = GETDATE();
		PRINT 'Truncate table data : [bronze.erp_PX_CAT_G1V2]'
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
		PRINT 'Loading data to table : [bronze.erp_PX_CAT_G1V2]'
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\DataProjects\DataWarehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK

		);
		SET @end_time = GETDATE();
		PRINT 'Load Time :' + CAST(DATEDIFF(second, @st_time, @end_time) AS NVARCHAR) + 'sec';
		PRINT '-------------------';

		PRINT '^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^';
		PRINT 'Bronze layer data load has completed'
		PRINT '^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^';
		SET @end_time_wholebatch = GETDATE();
		PRINT 'Bronze layer load time :' + CAST (DATEDIFF(second, @st_time_wholebatch, @end_time_wholebatch) AS NVARCHAR) + 'sec';
		PRINT '^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^';

	END TRY

	BEGIN CATCH
		PRINT 'Error message | There was an error :' + '   '+ ERROR_MESSAGE();
		PRINT 'Error number | There was an error on :' + '   ' + CAST (ERROR_NUMBER() AS VARCHAR);
		PRINT 'Error line :' + '  ' + CAST(ERROR_LINE() AS VARCHAR);
	END CATCH
END
