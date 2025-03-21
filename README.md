## CDC pipeline for NOAA CO2 Data in Snowflake using AWS Lambda 
A comprehensive data pipeline for processing, analyzing, and visualizing CO2 measurement data using Snowflake's modern data stack.

## Project Overview

This project implements an incremental data pipeline that processes CO2 concentration measurements from the Mauna Loa Observatory. The pipeline extracts data from public sources, loads it into Snowflake, and builds harmonized and analytical data layers to enable analysis of CO2 trends over time.

## Architecture

The pipeline follows a multi-layer data architecture:

1. **Raw Layer** - Contains raw CO2 data loaded directly from source files
2. **Harmonized Layer** - Standardized data with consistent formatting and data quality checks
3. **Analytics Layer** - Derived tables with aggregations, metrics, and enriched attributes for an!
4. **External Layer** - Storing all the stages and for implementing external access integration and policies for external outbound network call.

![Snowflake (4)](https://github.com/user-attachments/assets/fc49c7b6-77c8-4e3f-a36f-145011727b87)



### Key Components:

- **Raw Data Ingestion** - Loads CO2 data from S3 into the raw layer
- **Change Data Capture** - Uses Snowflake streams to track changes in the raw data
- **Harmonization** - Transforms raw data into a consistent format
- **Analytics Processing** - Calculates trends, aggregations, and derived metrics
- **UDFs** - Custom functions for CO2 calculations (volatility, daily/weekly changes)

## Technologies

- **Snowflake** - Cloud data warehouse
- **Python** - Primary programming language
- **Snowpark** - Snowflake's Python API for data processing
- **GitHub Actions** - CI/CD pipeline
- **AWS S3** - Data storage for source files
- **AWS Lambda** - Creating Lambda function with API Gateway for routing network api calls
- **pytest** - Testing framework

## Setup and Installation

### Prerequisites

- Python 3.10 or later
- Snowflake account
- AWS account with access to S3 buckets
- RSA key pair for Snowflake authentication

### Local Environment Setup

1. Clone the repository:

```bash
git clone https://github.com/BigDataTeam5/Incremental_DataPipleine_using_Snowflake.git
cd Incremental_DataPipleine_using_Snowflake
```

2. Create and activate a virtual environment using poetry:
    ### Windows Installation
    ```
    # Using PowerShell
    (Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | python -
    ```
    after installing, 
    ```
    cd Incremental_DataPipleine_using_Snowflake
    poetry show
    ```
    run any python file along with poetry command
    ```
    poetry run python <your_script>.py
    ```



4. Set up RSA key pair authentication:

```bash
mkdir -p ~/.snowflake/keys
poetry python scripts/rsa_key_pair_authentication/generate_snowflake_keys.py
```

5. Configure Snowflake connection by creating `~/.snowflake/connections.toml`:

```toml
[dev]
account = "your-account"
user = "your-username"
password= "your-password"
private_key_path = "~/.snowflake/keys/rsa_key.p8"
warehouse = "CO2_WH_DEV"
role = "CO2_ROLE_DEV"
database = "CO2_DB_DEV"
schema = "RAW_CO2"
client_request_mfa_token = false

[prod]
account = "your-account"
user = "your-username"
password= "your-password"
private_key_path = "~/.snowflake/keys/rsa_key.p8"
warehouse = "CO2_WH_PROD"
role = "CO2_ROLE_PROD"
database = "CO2_DB_PROD"
schema = "RAW_CO2"
client_request_mfa_token = false
```

6. Create a `.env` file with the following variables:

```
AWS_ACCESS_KEY=<your-access-key>
AWS_SECRET_KEY=<your-secret-key>
AWS_REGION=<your-region>
S3_BUCKET_NAME=noa-co2-datapipeline
PARENT_FOLDER=noaa-co2-data
SNOWFLAKE_ENV=dev
```

7. Create a `templates/environment.json` file:

```json
{
    "environment": "dev"
}
```

### Snowflake Setup

1. Register the public key with your Snowflake user:

```sql
ALTER USER YourUsername SET RSA_PUBLIC_KEY='<public-key-string>';
```

2. Create required Snowflake resources:

```bash
poetry run python scripts/deployment_files/snowflake_deployer.py sql --profile dev --file scripts/setup_dev.sql
```

## Project Structure

```
Incremental_DataPipleine_using_Snowflake/
├── .github/workflows/          # CI/CD workflows
├── scripts/                    # Utility scripts
│   ├── deployment_files/       # Deployment automation
│   ├── raw data loading/       # Data ingestion scripts
│   └── rsa_key_pair_authentication/  # Authentication utilities
├── templates/                  # Configuration templates
├── tests/                      # Test suite
├── udfs_and_spoc/              # User-Defined Functions and Stored Procedures
│   ├── co2_analytical_sp/      # Analytics stored procedure
│   ├── co2_harmonized_sp/      # Harmonization stored procedure
│   ├── daily_co2_changes/      # UDF for daily CO2 changes
│   ├── loading_co2_data_sp/    # Data loading stored procedure
│   ├── python_udf/             # Custom Python UDFs
│   └── weekly_co2_changes/     # UDF for weekly CO2 changes
├── .env                        # Environment variables (not in repo)
├── deployment_and_key_workflow.md  # Deployment documentation
├── pyproject.toml              # Project metadata and dependencies
├── pytest.ini                  # pytest configuration
├── README.md                   # This file
├── requirements.txt            # Python dependencies
└── rsa_key_pair_generator.md   # RSA key setup instructions
```

## Usage

### Loading Raw Data

```bash
poetry run python scripts/raw\ data\ loading\ and\ stream\ creation/raw_co2_data.py
```

### Creating Streams for Change Data Capture

```bash
poetry run python scripts/raw\ data\ loading\ and\ stream\ creation/02_create_rawco2data_stream.py
```

### Running Tests

```bash
pytest tests/
```

### Deploying Components to Snowflake

Deploy all components:
```bash
poetry run python scripts/deployment_files/snowflake_deployer.py deploy-all --profile dev --path udfs_and_spoc --check-changes
```

Deploy a specific component:
```bash
python scripts/deployment_files/snowflake_deployer.py deploy --profile dev --path udfs_and_spoc/co2_harmonized_sp --name HARMONIZE_CO2_DATA --type procedure
```

## Data Flow

1. **Data Ingestion**: CO2 data is loaded from S3 into the RAW_CO2 schema
2. **Change Detection**: Snowflake streams track changes in raw data
3. **Harmonization**: The HARMONIZE_CO2_DATA stored procedure transforms raw data into the harmonized layer
4. **Analytics**: The ANALYZE_CO2_DATA stored procedure creates analytical tables and views
5. **User Access**: End users query the analytics layer for insights

## Authentication

The project uses RSA key pair authentication for secure, MFA-free deployments to Snowflake:

1. Generate RSA key pair using the provided scripts
2. Register the public key with your Snowflake user
3. Configure the connection profile to use the private key
4. Store the private key securely in GitHub secrets for CI/CD

For detailed instructions, see `rsa_key_pair_generator.md` and `deployment_and_key_workflow.md`.

## CI/CD Pipeline

The GitHub Actions workflow automates:

1. Testing of all components
2. Deployment to dev/prod environments based on branch
3. Key-based authentication to Snowflake
4. Validation of code quality and functionality

## Troubleshooting

Common issues and solutions:

- **Authentication Errors**: Verify key permissions and format
- **Deployment Failures**: Check function signatures and parameter counts
- **Connection Issues**: Run `poetry run python scripts/deployment_files/check_connections_file.py` to validate your connections.toml

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests to ensure functionality
5. Submit a pull request
https://codelabs-preview.appspot.com/?file_id=18zcpbkzP3rvFD5bXkMybKJEIyr9HutwhhFf42tZ79WU#10
## License

[Specify your license here]

## Acknowledgments

- NOAA for providing the CO2 measurement data
- Snowflake for the data warehousing platform
