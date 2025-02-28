/*-----------------------------------------------------------------------------
  cleanup.sql
  
  This script removes all resources created for both DEV and PROD environments.
  Use with caution - this will remove all project resources.
-----------------------------------------------------------------------------*/

-- 1) Switch to admin role and pick a warehouse
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-------------------------------------------------------------------------------
-- 2) Drop the stages (if the DB doesn't exist, we'll just ignore the error)
-------------------------------------------------------------------------------
BEGIN
    EXECUTE IMMEDIATE 'DROP STAGE IF EXISTS CO2_DB_DEV.EXTERNAL.NOAA_CO2_STAGE';
EXCEPTION WHEN OTHER THEN
    -- Database or schema doesn't exist, so ignore
    NULL;
END;

BEGIN
    EXECUTE IMMEDIATE 'DROP STAGE IF EXISTS CO2_DB_PROD.EXTERNAL.NOAA_CO2_STAGE';
EXCEPTION WHEN OTHER THEN
    -- Database or schema doesn't exist, so ignore
    NULL;
END;

-------------------------------------------------------------------------------
-- 3) Drop the storage integration (S3_CO2_INTEGRATION)
-------------------------------------------------------------------------------
DROP INTEGRATION IF EXISTS S3_CO2_INTEGRATION;

-------------------------------------------------------------------------------
-- 4) Drop the databases (safe: "IF EXISTS" prevents errors if missing)
-------------------------------------------------------------------------------
DROP DATABASE IF EXISTS CO2_DB_DEV;
DROP DATABASE IF EXISTS CO2_DB_PROD;

-------------------------------------------------------------------------------
-- 5) Drop the warehouses (same reason: "IF EXISTS" is safe)
-------------------------------------------------------------------------------
DROP WAREHOUSE IF EXISTS CO2_WH_DEV;
DROP WAREHOUSE IF EXISTS CO2_WH_PROD;


-------------------------------------------------------------------------------
-- 7) Drop the CO2_ROLE if it exists
-------------------------------------------------------------------------------
DROP ROLE IF EXISTS CO2_ROLE;

-------------------------------------------------------------------------------
-- 8) Suspend the warehouse to save costs
-------------------------------------------------------------------------------
ALTER WAREHOUSE COMPUTE_WH SUSPEND;

-------------------------------------------------------------------------------
-- 9) Final status
-------------------------------------------------------------------------------
SELECT 'Cleanup completed. All project resources have been removed.' AS CLEANUP_STATUS;
