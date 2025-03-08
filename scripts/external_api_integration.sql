-- Step 1: Create the network rule for API Gateway access
USE ROLE ACCOUNTADMIN;
USE DATABASE CO2_DB_DEV;
USE SCHEMA EXTERNAL;
CREATE OR REPLACE NETWORK RULE co2_lambda_api_rule
  MODE = 'EGRESS'
  TYPE = 'HOST_PORT'
  VALUE_LIST = ('oc62udov6d.execute-api.us-east-2.amazonaws.com:443');

-- Step 2: Create a network rule for S3 access (needed for boto3)
CREATE OR REPLACE NETWORK RULE s3_access_rule
  MODE = 'EGRESS'
  TYPE = 'HOST_PORT'
  VALUE_LIST = ('s3.us-east-2.amazonaws.com:443', 's3.amazonaws.com:443');

-- Step 3: Create the external access integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION co2_data_ext_access
  ALLOWED_NETWORK_RULES = (co2_lambda_api_rule, s3_access_rule)
  ENABLED = TRUE;



-- Step 5: Grant usage permissions
GRANT USAGE ON INTEGRATION co2_data_ext_access TO ROLE CO2_ROLE_DEV;
GRANT USAGE ON INTEGRATION co2_data_ext_access TO ROLE CO2_ROLE_PROD;