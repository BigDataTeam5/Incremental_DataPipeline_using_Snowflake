import requests
import pandas as pd
import boto3
import io
import os
from dotenv import load_dotenv
load_dotenv('.env')
# AWS S3 Configuration
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")    
S3_OBJECT_NAME = os.getenv("S3_OBJECT_NAME")
AWS_REGION = os.getenv("AWS_REGION")
PARENT_FOLDER = os.getenv("PARENT_FOLDER")
# Initialize S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

# URL of the data file
url = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_daily_mlo.txt"

# Fetch data
response = requests.get(url)
data = response.text

# Process lines
lines = data.split("\n")

# Remove comment lines (starting with "#")
data_lines = [line for line in lines if not line.startswith("#") and line.strip()]

# Split data into columns
parsed_data = [line.split() for line in data_lines]

# Check column count for each row
max_columns = max(len(row) for row in parsed_data)

# Define column names based on the dataset structure
columns = ["Year", "Month", "Day", "Decimal Date", "CO2 (ppm)"]
if max_columns == 6:
    columns.append("CO2 Daily Change")

# Create DataFrame
df = pd.DataFrame(parsed_data, columns=columns)

# Convert numeric columns to appropriate types
df["Year"] = df["Year"].astype(int)
df["Month"] = df["Month"].astype(int)
df["Day"] = df["Day"].astype(int)
df["Decimal Date"] = df["Decimal Date"].astype(float)
df["CO2 (ppm)"] = pd.to_numeric(df["CO2 (ppm)"], errors='coerce')

# Handle missing column conditionally
if "CO2 Daily Change" in df.columns:
    df["CO2 Daily Change"] = pd.to_numeric(df["CO2 Daily Change"], errors='coerce')

# Group data by Year and upload each year’s data separately within the parent folder
for year, year_df in df.groupby("Year"):
    # Save to in-memory buffer
    csv_buffer = io.StringIO()
    year_df.to_csv(csv_buffer, index=False)
    
    # Define dynamic S3 object name (folder structure within the parent folder)
    s3_object_name = f"{PARENT_FOLDER}/{year}/co2_daily_mlo.csv"

    # Upload to S3
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_object_name,
        Body=csv_buffer.getvalue()
    )

    print(f"File successfully uploaded to s3://{S3_BUCKET_NAME}/{s3_object_name}")

print("All files uploaded successfully!")
