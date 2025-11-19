/* 
===================================================================
CREATE Database and schemas
===================================================================
Script purpose:
This script creates a new database named ' Datawarehouse' after checking if it already exists.
If the database exists, it will drop and recreates the database. Additionally, the scripts setup three schemas within the database:
Bronze, Silver and gold schemas.
Bronze==> Stores raw data
Silver==> Stores cleansed and transformed data
Gold==> Business-ready curated data.

Warning:
Running this script will drop the entire ' Datawarehouse' database if alread exists.
All data in the database will be permenantly deleted. Proceed with caution 
and ensure that you have a proper backups of your DB before running this script.
*/
USE master;


If Exists (SELECT 1 FROM sys.databases WHERE name = 'Datawarehouse')

BEGIN 
	ALTER DATABASE Datawarehouse SET SINGLE_user with rollback immediate;
	DROP DATABASE Datawarehouse;
END;
Go

--creating the datawarehouse database

CREATE Database Datawarehouse;
Go

USE Datawarehouse;
Go

CREATE SCHEMA Bronze;
GO --separate batches when working with multiple SQL statements
CREATE SCHEMA Silver;
GO
CREATE SCHEMA Gold;
