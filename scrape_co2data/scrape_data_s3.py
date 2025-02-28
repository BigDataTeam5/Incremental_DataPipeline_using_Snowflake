import requests
import pandas as pd
import boto3
import io
# AWS S3 Configuration
AWS_ACCESS_KEY = "AKIA4WJPWIY6YZDSM75N"
AWS_SECRET_KEY = "7U1DysV+i5L8y9rEfFany+22mMqXIOtcAIJVuVT2"
S3_BUCKET_NAME = "co2emissionsdata"
S3_OBJECT_NAME = "co2_daily_mlo.csv"  # File name in S3
AWS_REGION = "us-east-2"  # e.g., 'us-east-1'
 
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
max_columns = max(len(row) for row in parsed_data)  # Find max columns in any row
 
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
 
# Save to in-memory buffer instead of a file
csv_buffer = io.StringIO()
df.to_csv(csv_buffer, index=False)
 
# Upload to S3
s3_client.put_object(
    Bucket=S3_BUCKET_NAME,
    Key=S3_OBJECT_NAME,
    Body=csv_buffer.getvalue()
)
 
print(f"File successfully uploaded to s3://{S3_BUCKET_NAME}/{S3_OBJECT_NAME}")