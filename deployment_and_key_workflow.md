# Snowflake Deployment and RSA Key Authentication Flow

## Overview of the Deployment Architecture

The deployment process uses RSA key-based authentication to deploy UDFs and stored procedures to Snowflake without requiring MFA prompts. Here's how the components work together:

## 1. RSA Key Generation and Authentication

### Key Files:
- **rsa_key_pair_generator.md** - Documentation with manual steps for key generation
- **scripts/rsa_key_pair_authentication/generate_snowflake_keys.py** - Script to generate RSA keys
- **scripts/rsa_key_pair_authentication/check_snowflake_key_auth.py** - Validates key formatting

### Purpose:
These files help create and validate the RSA key pair needed for passwordless authentication with Snowflake. The process is:

1. Generate a private key (rsa_key.p8) and public key (rsa_key.pub)
2. Register the public key with your Snowflake user account
3. Store the private key securely locally for development and in GitHub secrets for CI/CD

## 2. Connection Configuration

### Key Files:
- **~/.snowflake/connections.toml** - Stores connection profiles (local development)
- **scripts/deployment_files/check_connections_file.py** - Diagnoses issues with connections.toml
- **scripts/deployment_files/test_key_auth.py** - Tests key authentication

### Purpose:
These files manage the connection details for different environments:

1. The connections.toml needs proper format with `private_key_path` pointing to your local key
2. test_key_auth.py validates the key works before attempting deployment
3. check_connections_file.py helps diagnose TOML parsing issues

## 3. Deployment Process

### Key Files:
- **scripts/deployment_files/snowflake_deployer.py** - Main deployment logic
- **.github/workflows/snowpark-ci-cd.yml** - CI/CD pipeline configuration

### Purpose:
These files handle the actual deployment:

1. GitHub workflow sets up authentication in CI/CD environment
2. snowflake_deployer.py:
   - Analyzes UDF function signatures
   - Packages code into ZIP files
   - Uploads to Snowflake stage
   - Creates UDFs/procedures with proper SQL DDL

## 4. UDF Parameter Handling

### Key Files:
- **scripts/check_and_fix_udf.py** - Fixes UDF parameter mismatches
- **udfs_and_spoc/*/function.py** - The actual UDF/procedure code

### Purpose:
The UDF files have different parameter signatures:
1. CO2_VOLATILITY and DAILY_CO2_CHANGES have 2 parameters (current_value, previous_value)
2. Stored procedures take session parameters
3. The deployment process must adapt SQL definitions to match these signatures

## Interrelationships Between Files

1. **Key Authentication Chain:**
   ```
   generate_snowflake_keys.py -> rsa_key.p8/.pub -> connections.toml -> snowflake_deployer.py
   ```

2. **Deployment Chain:**
   ```
   CI/CD workflow -> snowflake_deployer.py -> analyze_function_signature() -> Customized SQL DDL -> Snowflake
   ```

3. **Parameter Handling Chain:**
   ```
   UDF function.py -> analyze_function_signature() -> SQL with matching parameters
   ```

## Current Issues and Solutions

The issues you're experiencing are related to:

1. **TOML File Syntax**: The connections.toml file has `//` comments instead of `#` comments
2. **Missing Key Path**: Your connections.toml doesn't have `private_key_path` configured
3. **Authentication Flow**: The deployment tries password auth and triggers MFA

To fix:
1. Update connections.toml with proper TOML syntax (use # for comments)
2. Add private_key_path pointing to your local key
3. Set `client_request_mfa_token = false` to explicitly disable MFA

This workflow ensures secure, MFA-free deployments and handles different function signatures appropriately.