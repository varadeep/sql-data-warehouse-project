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
    -- Check if the database exists
    IF EXISTS (SELECT 1 FROM pg_database WHERE datname = 'DataWarehouse') THEN
        RAISE NOTICE 'Database exists. Dropping it...';

        -- Terminate active connections to the database
        PERFORM pg_terminate_backend(pg_stat_activity.pid)
        FROM pg_stat_activity
        WHERE pg_stat_activity.datname = 'DataWarehouse'
          AND pid <> pg_backend_pid();

        -- Use dynamic SQL to drop the database
        EXECUTE 'DROP DATABASE DataWarehouse';
        RAISE NOTICE 'Database dropped.';
    ELSE
        RAISE NOTICE 'Database does not exist.';
    END IF;

    -- Create the database (outside the IF block, since it's always needed)
    RAISE NOTICE 'Creating database...';
    EXECUTE 'CREATE DATABASE DataWarehouse';
    RAISE NOTICE 'Database created.';
    EXECUTE 'CREATE SCHEMA bronze;'
    EXECUTE 'CREATE SCHEMA silver;'
    EXECUTE 'CREATE SCHEMA gold;'
    RAISE NOTICE 'Bronze, Silver and Gold Schemas are  created.';

END $$;
