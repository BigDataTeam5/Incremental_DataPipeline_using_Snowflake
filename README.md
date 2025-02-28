# CI/CD with Snowflake: Data Masking UDF Lab

This repository contains the code for a CI/CD pipeline with Snowflake, demonstrating how to build, test, and deploy a data masking User-Defined Function (UDF) using GitHub Actions.



![co2_pipeline_simplified9 (2)](https://github.com/user-attachments/assets/b083b8be-01b4-4cd0-a95a-8fe5f85aae69)


## Overview

The Data Masking UDF provides functionality to mask Personally Identifiable Information (PII) such as:
- Email addresses
- Phone numbers
- Credit card numbers
- Social Security Numbers (SSNs)

The UDF supports different masking levels (high, medium, low) and is implemented using Snowpark Python.

## Project Structure

```
snowflake-cicd-lab/
├── .github/
│   └── workflows/
│       └── snowpark-ci-cd.yml     # GitHub Actions workflow
├── config/
│   ├── dev.yml                    # Development environment config
│   └── prod.yml                   # Production environment config
├── src/
│   └── data_masker/               # Snowpark UDF project
│       ├── requirements.txt       # Dependencies
│       ├── snowflake.yml          # Snowflake project definition
│       └── data_masker/
│           ├── __init__.py
│           └── function.py        # UDF implementation
├── scripts/
│   ├── setup_snowflake.sql        # Initial setup script
│   ├── load_sample_data.sql       # Load sample data with PII
│   ├── test_udf.sql               # SQL tests for the UDF
│   └── cleanup.sql                # Cleanup script
├── tests/
│   └── test_data_masker.py        # Python unit tests
├── deploy_snowpark_app.py         # Manual deployment script
└── README.md                      # This file
```

## Prerequisites

- Snowflake account with ACCOUNTADMIN privileges
- GitHub account
- Python 3.10 or later
- Snowflake CLI (snow) installed

## Step-by-Step Setup and Execution

### 1. Clone the Repository

```bash
git clone https://github.com/[your-username]/snowflake-cicd-lab.git
cd snowflake-cicd-lab
```

### 2. Set Up Snowflake Environment

1. Log in to your Snowflake account using ACCOUNTADMIN role
2. Run the setup script to create necessary Snowflake objects:

```bash
snowsql -f scripts/setup_snowflake.sql
```

Or execute the script through the Snowflake web UI.

3. Load the sample data:

```bash
snowsql -f scripts/load_sample_data.sql
```

### 3. Configure Your Local Environment

1. Install required Python packages:

```bash
pip install -r src/data_masker/requirements.txt
pip install pytest snowflake-cli-labs pyyaml
```

2. Set Snowflake connection environment variables:

```bash
export SNOWFLAKE_ACCOUNT="your-account-identifier"
export SNOWFLAKE_USER="your-username"
export SNOWFLAKE_PASSWORD="your-password"
export SNOWFLAKE_ROLE="CICD_LAB_ROLE"
export SNOWFLAKE_WAREHOUSE="CICD_LAB_WH"
```

### 4. Run Unit Tests Locally

```bash
pytest tests/test_data_masker.py -v
```

### 5. Deploy Manually to Development Environment

```bash
python deploy_snowpark_app.py --env dev
```

### 6. Test the UDF in Snowflake

Execute the test script in Snowflake:

```bash
snowsql -f scripts/test_udf.sql
```

Or through the Snowflake web UI.

### 7. Configure GitHub Actions for CI/CD

1. Fork this repository if you haven't already
2. In your GitHub repository, go to Settings > Secrets and Variables > Actions
3. Add the following repository secrets:
   - `SNOWFLAKE_ACCOUNT`: Your Snowflake account identifier
   - `SNOWFLAKE_USER`: Your Snowflake username
   - `SNOWFLAKE_PASSWORD`: Your Snowflake password
   - `SNOWFLAKE_ROLE`: CICD_LAB_ROLE
   - `SNOWFLAKE_WAREHOUSE`: CICD_LAB_WH

### 8. Experience the CI/CD Pipeline

The CI/CD pipeline is triggered automatically when you push to the repository:
- Pushing to the `dev` branch deploys to the development environment
- Pushing to the `main` branch deploys to the production environment


### Step-by-Step Workflow
🟢 Step 1: Data Ingestion (Raw Data Collection)
✅ Fetch CO₂ data from NOAA:

Download daily CO₂ levels from NOAA’s Mauna Loa Observatory.

Clean, validate, and parse the data.

✅ Store data in AWS S3:

Organize raw data into folders by year (s3://co2-bucket/YYYY/co2_daily.csv).

Upload the cleaned data using Boto3 (AWS SDK for Python).

✅ Load data into Snowflake:

Use COPY INTO to ingest data into RAW_CO2.CO2_DATA.

Enable incremental tracking via Snowflake Streams (CO2_DATA_STREAM).

🔵 Step 2: Data Harmonization (Transforming & Normalizing)
✅ Create a harmonized table:

Define HARMONIZED_CO2.harmonized_co2 schema.

Convert ppm values to metric tons.

✅ Merge new data using Snowflake Streams & Tasks:

Use CO2_HARMONIZED_TASK for merging new records.

Ensure incremental updates via SYSTEM$STREAM_HAS_DATA.

✅ Store processed data:

Maintain a structured and cleaned dataset in HARMONIZED_CO2.

🟠 Step 3: Analytics & Insights
✅ Compute key CO₂ metrics using UDFs:

Daily Percent Change (co2_percent_change_udf.py)

Volatility Analysis (co2_volatility_udf.py)

Trend Forecasting (ML-based models)

✅ Generate analytics tables:

Store insights in ANALYTICS_CO2.DAILY_CO2_METRICS.

Apply unit conversion functions (ppm → metric tons).

✅ Enable real-time dashboards & API access:

Provide Snowflake APIs for reporting.

Generate CO₂ monitoring dashboards.

🟣 Step 4: Automation & Deployment
✅ Automate Pipeline Execution in Snowflake:

Schedule Daily Task Execution (2 AM UTC).

Run CO2_HARMONIZED_TASK to update harmonized data.

Run CO2_ANALYTICS_TASK to refresh analytics tables.

✅ Enable CI/CD pipeline using GitHub Actions:

Automate deployment of Snowpark-based AI models.

Push updates to forecasting & anomaly detection models.

📌 Diagram Representation
The CO₂ Emissions Data Pipeline is visually represented in the following diagram:

(Refer to co2_pipeline_simplified.png for the architecture overview.)

🚀 How to Run the Pipeline
Set up AWS credentials in .env or use IAM roles.

Configure Snowflake credentials for secure access.

Run the ingestion script to fetch & store NOAA data.

Trigger Snowflake tasks to harmonize & process data.
Automate execution using GitHub Actions for daily processing.


🛠️ Technologies Used
AWS S3 (Storage)

Snowflake (Data Warehousing)

Python & Boto3 (Data Processing)

Snowpark (Machine Learning Models)

GitHub Actions (CI/CD Automation)

Streamlit / Power BI (Dashboards)

📧 Contact & Support
For any queries, f

## Troubleshooting

- **Authentication Issues**: Verify your Snowflake credentials are correctly set
- **Permission Errors**: Ensure the CICD_LAB_ROLE has all required privileges
- **Deployment Failures**: Check the GitHub Actions logs for detailed errors
- **Testing Errors**: Confirm your Python environment has all dependencies installed

## Resources

- [Snowflake Documentation](https://docs.snowflake.com/)
- [Snowpark Python Developer Guide](https://docs.snowflake.com/en/developer-guide/snowpark/python/index.html)
- [Snowflake CLI Documentation](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index.html)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)

## License

This project is licensed under the MIT License - see the LICENSE file for details.
