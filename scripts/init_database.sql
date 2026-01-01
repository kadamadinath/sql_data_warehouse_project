--Use Database 'master'
USE master
GO

 --Drop and recreate Datawarehouse Database
DROP DATABASE DataWarehouse;

 --Create Database 'Datawarhouse'
CREATE DATABASE DataWarehouse;

--Use Database 'Datawarhouse'
 USE DataWarehouse;

--Create Schema 'bronze'
 CREATE SCHEMA bronze;
 GO

  --Create Schema 'silver'
 CREATE SCHEMA silver;
 GO

  --Create Schema "gold"
 CREATE SCHEMA gold;
 GO
