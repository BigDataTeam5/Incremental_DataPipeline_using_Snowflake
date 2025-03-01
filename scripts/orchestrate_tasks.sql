/*-----------------------------------------------------------------------------
Orchestrate CO₂ Data Pipeline Jobs - Manual Execution
Script:       orchestrate_co2_jobs.sql
Author:       [Your Name]
Last Updated: [Today's Date]
-----------------------------------------------------------------------------*/

-- Set the execution context
USE ROLE CO2_ROLE;
USE WAREHOUSE CO2_WH;
USE DATABASE CO2_DB_DEV;

-- Set the default schema
USE SCHEMA ANALYTICS_CO2;

-- ----------------------------------------------------------------------------
-- Step #1: Suspend existing tasks (to allow modifications)
-- ----------------------------------------------------------------------------
ALTER TASK IF EXISTS CO2_HARMONIZED_TASK SUSPEND;

ALTER TASK IF EXISTS CO2_ANALYTICS_TASK SUSPEND;

-- ----------------------------------------------------------------------------
-- Step #2: Drop any existing tasks (to ensure a clean setup)
-- ----------------------------------------------------------------------------
DROP TASK IF EXISTS CO2_HARMONIZED_TASK;

DROP TASK IF EXISTS CO2_ANALYTICS_TASK;
-- ----------------------------------------------------------------------------
-- Step #3: Create tasks for manual execution
-- ----------------------------------------------------------------------------
-- Task to update the harmonized CO₂ data
SHOW STREAMS LIKE 'CO2_DATA_STREAM' IN SCHEMA RAW_CO2;
CREATE OR REPLACE TASK CO2_HARMONIZED_TASK
WAREHOUSE = CO2_WH
SCHEDULE = 'USING CRON 0 2 * * * UTC' -- Runs at 2 AM UTC daily
WHEN SYSTEM$STREAM_HAS_DATA('RAW_CO2.CO2_DATA_STREAM')
AS
CALL CO2_DB_DEV.HARMONIZED_CO2.CO2_HARMONIZED_SP();

-- Task to update the analytics (executes only after harmonized task completes)
CREATE OR REPLACE TASK CO2_ANALYTICS_TASK
WAREHOUSE = CO2_WH
AFTER CO2_HARMONIZED_TASK
AS
CALL CO2_DB_DEV.ANALYTICS_CO2.CO2_ANALYTICS_SP();

-- ----------------------------------------------------------------------------
-- Step #4: Resume and manually execute the tasks
-- ----------------------------------------------------------------------------

-- Resume both tasks so they are ready for execution
ALTER TASK CO2_ANALYTICS_TASK RESUME;
ALTER TASK CO2_HARMONIZED_TASK RESUME;

-- Manually execute the harmonized task to test the workflow
EXECUTE TASK CO2_HARMONIZED_TASK;

SELECT 
NAME,
STATE,
SCHEDULED_TIME,
COMPLETED_TIME,
ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
SCHEDULED_TIME_RANGE_START=>DATEADD('DAY',-1,CURRENT_TIMESTAMP()),
RESULT_LIMIT => 100))
ORDER BY SCHEDULED_TIME DESC;
