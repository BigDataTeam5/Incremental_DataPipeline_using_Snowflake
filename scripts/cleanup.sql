-- Cleanup Script

-- Use with caution - this will remove all lab resources
USE ROLE ACCOUNTADMIN;

-- Drop databases
use role CO2_ROLE;
DROP DATABASE IF EXISTS CO2_DB_DEV;
DROP DATABASE IF EXISTS CO2_DB_PROD;

-- Drop warehouse
DROP WAREHOUSE IF EXISTS CO2_WH;

-- Switch to ACCOUNTADMIN to drop role
USE ROLE ACCOUNTADMIN;
DROP ROLE IF EXISTS CO2_ROLE;