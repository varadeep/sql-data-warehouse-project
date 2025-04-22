/*
==============================================================================================
*******************Stored procedure : Load Silver Layer (Source ---> Silver)******************
==============================================================================================
Script Purpose:
  This stored procedure loads data into the 'Silver' schema from 'Bronze' layer tables.
It performs the following actions:
- Truncates the Silver tables before loading data.
 
Parameters :
  None.
  This stored procedure does not accept any parameters or return any values.
Usage Example:
  EXEC silver.load_bronze;
==============================================================================================

*/
CREATE OR REPLACE PROCEDURE SILVER.LOAD_SILVER () 
LANGUAGE PLPGSQL 
AS $$
DECLARE
  start_time TIMESTAMP;
  end_time TIMESTAMP;
  error_message TEXT;
BEGIN
	RAISE NOTICE '--------------------------------------';
	RAISE NOTICE '---------Loading Silver Layer---------';
	RAISE NOTICE '--------------------------------------';
	start_time := clock_timestamp(); -- Record start time for the entire process
	RAISE NOTICE '>>Truncate table:SILVER.CRM_CUST_INFO';
	TRUNCATE TABLE SILVER.CRM_CUST_INFO;
	RAISE NOTICE '>>Inserting Data:SILVER.CRM_CUST_INFO';
	INSERT INTO
		SILVER.CRM_CUST_INFO
	SELECT
		CST_ID,
		CST_KEY,
		TRIM(CST_FIRSTNAME),
		TRIM(CST_LASTNAME),
		CASE
			WHEN UPPER(TRIM(CST_MARITAL_STATUS)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(CST_MARITAL_STATUS)) = 'M' THEN 'Married'
			ELSE 'n/a'
		END AS CST_MARITAL_STATUS, --Normalize marital status values to readable formate
		CASE
			WHEN UPPER(TRIM(CST_GNDR)) = 'M' THEN 'Male'
			WHEN UPPER(TRIM(CST_GNDR)) = 'F' THEN 'Female'
			ELSE 'n/a'
		END AS CST_GNDR, --Normalize gender values to readable formate
		CST_CREATE_DATE
	FROM
		(
			SELECT
				*,
				ROW_NUMBER() OVER (
					PARTITION BY
						CST_ID
					ORDER BY
						CST_CREATE_DATE DESC
				) AS FLAG
			FROM
				BRONZE.CRM_CUST_INFO
		) AS SQ
	WHERE
		FLAG =1;
	RAISE NOTICE '-------------------------------------------------------------------------------------------------';
	RAISE NOTICE '>>Truncate table:SILVER.CRM_PRD_INFO';
	TRUNCATE TABLE SILVER.CRM_PRD_INFO;
	RAISE NOTICE '>>Inserting Data:SILVER.CRM_PRD_INFO';
	INSERT INTO
		SILVER.CRM_PRD_INFO
	SELECT
		PRD_ID,
		REPLACE(SUBSTRING(PRD_KEY, 1, 5), '-', '_') AS CAT_ID,--Extract category id
		SUBSTRING(PRD_KEY, 7, LENGTH(PRD_KEY)) AS PRD_KEY,--Extract product key
		PRD_NM,
		COALESCE(PRD_COST, 0) AS PRD_COST,
		CASE UPPER(TRIM(PRD_LINE))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS PRD_LINE ,--Map product line codes o descriptive values
		CAST(CAST(PRD_START_DT AS VARCHAR) AS DATE) AS PRD_START_DT,
		CAST(
			(
				LEAD(PRD_START_DT) OVER (
					PARTITION BY
						PRD_KEY
					ORDER BY
						PRD_START_DT
				) - INTERVAL '1 day'
			) AS DATE
		) AS PRD_END_DT -- calculate end date as one day before the next start date
	FROM
		BRONZE.CRM_PRD_INFO;
	
	RAISE NOTICE '-------------------------------------------------------------------------------------------------';
	RAISE NOTICE '>>Truncate table:SILVER.CRM_SALES_DETAILS';
	TRUNCATE TABLE SILVER.CRM_SALES_DETAILS;
	RAISE NOTICE '>>Inserting Data:SILVER.CRM_SALES_DETAILS';
	INSERT INTO
		SILVER.CRM_SALES_DETAILS
	SELECT
		SLS_ORD_NUM,
		SLS_PRD_KEY,
		SLS_CUST_ID,
		CASE
			WHEN SLS_ORDER_DT = 0
			OR LENGTH(CAST(SLS_ORDER_DT AS TEXT)) != 8 THEN NULL
			ELSE CAST(CAST(SLS_ORDER_DT AS VARCHAR) AS DATE)
		END AS SLS_ORDER_DT,
		CASE
			WHEN SLS_SHIP_DT = 0
			OR LENGTH(CAST(SLS_SHIP_DT AS TEXT)) != 8 THEN NULL
			ELSE CAST(CAST(SLS_SHIP_DT AS VARCHAR) AS DATE)
		END AS SLS_SHIP_DT,
		CASE
			WHEN SLS_DUE_DT = 0
			OR LENGTH(CAST(SLS_DUE_DT AS TEXT)) != 8 THEN NULL
			ELSE CAST(CAST(SLS_DUE_DT AS VARCHAR) AS DATE)
		END AS SLS_DUE_DT,
		SLS_QUANTITY,
		CASE
			WHEN SLS_SALES <= 0
			OR SLS_SALES IS NULL
			OR SLS_SALES != SLS_QUANTITY * ABS(SLS_PRICE) THEN SLS_QUANTITY * ABS(SLS_PRICE)
			ELSE SLS_SALES
		END AS SLS_SALES,--Recaluclate sales if original value is missing or incorrect
		CASE
			WHEN SLS_PRICE <= 0
			OR SLS_PRICE IS NULL THEN SLS_SALES / NULLIF(SLS_QUANTITY, 0)
			ELSE SLS_PRICE
		END AS SLS_PRICE--Derive price if original value is invalid
	FROM
		BRONZE.CRM_SALES_DETAILS;
	
	RAISE NOTICE '-------------------------------------------------------------------------------------------------';
	RAISE NOTICE '>>Truncate table:SILVER.ERP_CUST_AZ12';
	TRUNCATE TABLE SILVER.ERP_CUST_AZ12;
	RAISE NOTICE '>>Inserting Data:SILVER.ERP_CUST_AZ12';
	INSERT INTO
		SILVER.ERP_CUST_AZ12
	SELECT
		CASE
			WHEN CID ILIKE 'nas%' THEN SUBSTRING(CID, 4, LENGTH(CID))
			ELSE CID
		END AS CID,--Remove 'NAS' prefix if present
		CASE
			WHEN BDATE > CURRENT_DATE THEN NULL
			ELSE BDATE
		END AS BDATE,--Set futur birthdates to null
		CASE
			WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
			WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
			ELSE 'n/a'
		END AS GEN --normalize gender values and handle unknow cases
	FROM
		BRONZE.ERP_CUST_AZ12;
	
	RAISE NOTICE '-------------------------------------------------------------------------------------------------';
	RAISE NOTICE '>>Truncate table:SILVER.ERP_LOC_A101';
	TRUNCATE TABLE SILVER.ERP_LOC_A101;
	RAISE NOTICE '>>Inserting Data:SILVER.ERP_LOC_A101';
	INSERT INTO
		SILVER.ERP_LOC_A101
	SELECT
		REPLACE(CID, '-', '') AS CID,
		CASE
			WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
			WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
			WHEN TRIM(CNTRY) = ''
			OR CNTRY IS NULL THEN 'n/a'
			ELSE TRIM(CNTRY)
		END AS CNTRY --Normalize and handle missing or blank country codes
	FROM
		BRONZE.ERP_LOC_A101;
	RAISE NOTICE '-------------------------------------------------------------------------------------------------';
	RAISE NOTICE '>>Truncate table:SILVER.ERP_PX_CAT_G1V2';
	TRUNCATE TABLE SILVER.ERP_PX_CAT_G1V2;
	RAISE NOTICE '>>Inserting Data:SILVER.ERP_PX_CAT_G1V2';
	INSERT INTO
		SILVER.ERP_PX_CAT_G1V2
	SELECT
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
	FROM
		BRONZE.ERP_PX_CAT_G1V2;
	
	end_time := clock_timestamp();
	RAISE NOTICE 'Time taken for bronze layer load: %', end_time - start_time;

	EXCEPTION WHEN OTHERS THEN
  		error_message := SQLERRM;
  		RAISE NOTICE 'General error in bronze.load_bronze: %', error_message;
  		end_time := clock_timestamp(); -- Record end time even on error
  		RAISE NOTICE 'Total time taken (including error): %', end_time - start_time;
END;
$$
CALL SILVER.LOAD_SILVER ();
