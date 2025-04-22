/*
=============================================================
                Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_database WHERE datname = 'DWH') THEN
        RAISE NOTICE 'Database exists. Dropping it...';

        -- Terminate active connections
        PERFORM pg_terminate_backend(pg_stat_activity.pid)
        FROM pg_stat_activity
        WHERE pg_stat_activity.datname = 'DWH'
          AND pid <> pg_backend_pid();

        -- Cannot use DROP DATABASE in DO block; just raise notice
        RAISE NOTICE 'Please run: DROP DATABASE DWH;';
    ELSE
        RAISE NOTICE 'Database does not exist.';
    END IF;
END $$;
--Droping database DWH
DROP DATABASE DWH;
--Creating database DataWarehouse
CREATE DATABASE DWH;
--Creating Bronze, Silver, and Gold
DO $$
BEGIN
    EXECUTE 'CREATE SCHEMA bronze;';
    EXECUTE 'CREATE SCHEMA silver;';
    EXECUTE 'CREATE SCHEMA gold;';
    RAISE NOTICE 'Bronze, Silver, and Gold schemas created.';
END $$;

