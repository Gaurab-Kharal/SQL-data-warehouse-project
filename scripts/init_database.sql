/*

scripts purpose:
============================================================
Creating database DataWarehouse and Schemas for each layer
============================================================

First we check if the database DataWarehouse exist in the system. If the database exist already we drop 
the database DataWarehouse and recreate it.
We also create schemas for each layer 'bronze', 'silver' and 'gold'.
--------------------------------------------------------------------------------------------------------
=========
Warnings:
=========
We are only allowing one user to access database for tiem then droping it.
If there is important data in DataWarehouse it will be permanently deleted.
So you should always be cautious before using the scripts below.
*/

-- Master Database is the system database where you can go and create other database
USE master;

GO

-- Checking if the database already exist in system if it exist then droping it 
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
	BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
		DROP DATABASE DataWarehouse
	END

GO

-- Creating new Database DataWarehouse
CREATE DATABASE DataWarehouse;

GO
 
USE DataWarehouse;

GO
-- Creating Schema for each layer
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
