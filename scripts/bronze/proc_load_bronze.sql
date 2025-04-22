/*
============================================================================================================================
*****************************Stored procedure : Load Bronze Layer (Source ---> Bronze)**************************************
Script Purpose:
  This stored procedure loads data into the 'bronze' schema from external CSV files.
It performs the following actions:
- Truncates the bronze tables before loading data.
- Uses the 'COPY' command to load data from csv Files to bronze tables.
Parameters :
  None.
  This stored procedure does not accept any parameters or return any values.
Usage Example:
  EXEC bronze.load bronze;
============================================================================================================================
*/
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
  start_time TIMESTAMP;
  end_time TIMESTAMP;
  error_message TEXT;
BEGIN
  RAISE NOTICE '--------------------------------------';
  RAISE NOTICE '---------Loading Bronze Layer---------';
  RAISE NOTICE '--------------------------------------';

  start_time := clock_timestamp(); -- Record start time for the entire process

  -- CRM Data Loading
  RAISE NOTICE '-----------Loading CRM DATA-----------';
  BEGIN  -- Begin a block for CRM data loading (for specific error handling if needed)
    TRUNCATE TABLE bronze.crm_cust_info;
    RAISE NOTICE '>>Truncated Table: bronze.crm_cust_info.';
    COPY bronze.crm_cust_info
    FROM 'V:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
    WITH (FORMAT CSV, HEADER);
    RAISE NOTICE '>>Inserted Data Into bronze.crm_cust_info.';

    TRUNCATE TABLE bronze.crm_prd_info;
    RAISE NOTICE '>>Truncated Table: bronze.crm_prd_info.';
    COPY bronze.crm_prd_info
    FROM 'V:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH (FORMAT CSV, HEADER);
    RAISE NOTICE '>>Inserted Data Into bronze.crm_prd_info.';

    TRUNCATE TABLE bronze.crm_sales_details;
    RAISE NOTICE '>>Truncated Table :bronze.crm_sales_details.';
    COPY bronze.crm_sales_details
    FROM 'V:\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
    WITH (FORMAT CSV, HEADER);
    RAISE NOTICE '>>Inserted Data Into bronze.crm_sales_details.';
  EXCEPTION WHEN OTHERS THEN
      error_message := SQLERRM;
      RAISE NOTICE 'Error loading CRM data: %', error_message;
      -- You might want to RAISE EXCEPTION here to stop the whole process, or just continue
  END; -- End of CRM data loading block


  -- ERP Data Loading
  RAISE NOTICE '--------------------------------------';
  RAISE NOTICE '-----------Loading ERP DATA-----------';
  RAISE NOTICE '--------------------------------------';
  BEGIN -- Begin a separate block for ERP Data loading
    TRUNCATE TABLE bronze.erp_cust_az12;
    RAISE NOTICE '>>Truncated Table: bronze.erp_cust_az12.';
    COPY bronze.erp_cust_az12
    FROM 'V:\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
    WITH (FORMAT CSV, HEADER);
    RAISE NOTICE '>>Inserted Data Into bronze.erp_cust_az12.';

    TRUNCATE TABLE bronze.erp_loc_a101;
    RAISE NOTICE '>>Truncated Table: bronze.erp_loc_a101.';
    COPY bronze.erp_loc_a101
    FROM 'V:\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
    WITH (FORMAT CSV, HEADER);
    RAISE NOTICE '>>Inserted Data Into bronze.erp_loc_a101.';

    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    RAISE NOTICE '>>Truncated Table: bronze.erp_px_cat_g1v2.';
    COPY bronze.erp_px_cat_g1v2
    FROM 'V:\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
    WITH (FORMAT CSV, HEADER);
    RAISE NOTICE '>>Inserted Data Into bronze.erp_px_cat_g1v2.';
  EXCEPTION WHEN OTHERS THEN
      error_message := SQLERRM;
      RAISE NOTICE 'Error loading ERP data: %', error_message;
      -- You might want to RAISE EXCEPTION here to stop the whole process, or just continue
  END; -- End of ERP data loading block

  RAISE NOTICE 'Bulk load ERP & CRM completed.';
  end_time := clock_timestamp();

  RAISE NOTICE 'Time taken for bronze layer load: %', end_time - start_time;

EXCEPTION WHEN OTHERS THEN
  error_message := SQLERRM;
  RAISE NOTICE 'General error in bronze.load_bronze: %', error_message;
  end_time := clock_timestamp(); -- Record end time even on error
  RAISE NOTICE 'Total time taken (including error): %', end_time - start_time;
END;
$$;
EXEC bronze.load bronze;
CALL bronze.load_bronze()
