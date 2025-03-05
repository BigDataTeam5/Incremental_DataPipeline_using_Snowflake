import pytest
import boto3
import os
import logging
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv('.env')

# Constants
BUCKET_NAME = "co2emissionsdata"
BASE_PREFIX = "noaa-co2-data/"
EXPECTED_YEARS = range(1974, 2020)
FILE_NAME = "co2_daily_mlo.csv"

@pytest.fixture
def s3_client():
    """Create and return an S3 client using credentials from environment variables."""
    try:
        client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('AWS_SECRET_KEY')
        )
        return client
    except Exception as e:
        logger.error(f"Failed to create S3 client: {e}")
        pytest.fail(f"S3 client creation failed: {e}")

def test_s3_bucket_exists(s3_client):
    """Test that the S3 bucket exists and is accessible."""
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        logger.info(f"Successfully connected to bucket: {BUCKET_NAME}")
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code')
        if error_code == '404':
            pytest.fail(f"Bucket {BUCKET_NAME} does not exist")
        elif error_code == '403':
            pytest.fail(f"Access denied to bucket {BUCKET_NAME}. Check credentials.")
        else:
            pytest.fail(f"Error accessing bucket {BUCKET_NAME}: {e}")

def test_base_prefix_exists(s3_client):
    """Test that the base prefix (folder) exists in the bucket."""
    try:
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=BASE_PREFIX,
            MaxKeys=1
        )
        
        # If 'Contents' key is in response, the prefix exists
        assert 'Contents' in response, f"Prefix {BASE_PREFIX} not found in bucket {BUCKET_NAME}"
        logger.info(f"Base prefix {BASE_PREFIX} exists in bucket {BUCKET_NAME}")
    except ClientError as e:
        pytest.fail(f"Failed to list objects in bucket: {e}")

def test_year_folders_exist(s3_client):
    """Test that all expected year folders exist."""
    year_folders_found = set()
    
    try:
        # List all prefixes under the base prefix
        paginator = s3_client.get_paginator('list_objects_v2')
        
        # We'll use delimiter to list directories
        for page in paginator.paginate(
            Bucket=BUCKET_NAME,
            Prefix=BASE_PREFIX,
            Delimiter='/'
        ):
            for prefix in page.get('CommonPrefixes', []):
                prefix_name = prefix.get('Prefix')
                # Extract year from prefix
                if prefix_name:
                    year_str = prefix_name.rstrip('/').split('/')[-1]
                    if year_str.isdigit():
                        year_folders_found.add(int(year_str))
        
        # Check if all expected years are found
        for year in EXPECTED_YEARS:
            assert year in year_folders_found, f"Year folder {year} not found in S3"
        
        logger.info(f"Found {len(year_folders_found)} year folders in S3")
    except ClientError as e:
        pytest.fail(f"Failed to list year folders: {e}")

def test_sample_file_accessibility(s3_client):
    """Test accessing a sample file from each of the first, middle, and last year."""
    sample_years = [1974, 1995, 2019]  # First, middle, and last years
    
    for year in sample_years:
        file_path = f"{BASE_PREFIX}{year}/{FILE_NAME}"
        try:
            # Check if file exists
            response = s3_client.head_object(
                Bucket=BUCKET_NAME,
                Key=file_path
            )
            
            # If we get here, the file exists
            logger.info(f"Successfully accessed {file_path} (Size: {response.get('ContentLength')} bytes)")
            
            # Optionally, get a small sample of the file content
            sample = s3_client.get_object(
                Bucket=BUCKET_NAME,
                Key=file_path,
                Range="bytes=0-500"  # Get first 500 bytes as a sample
            )
            
            content = sample['Body'].read().decode('utf-8')
            logger.info(f"Sample of {file_path}:\n{content[:200]}...")
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            if error_code == '404':
                pytest.fail(f"File {file_path} not found")
            elif error_code == '403':
                pytest.fail(f"Access denied to file {file_path}")
            else:
                pytest.fail(f"Error accessing file {file_path}: {e}")

def test_count_files_in_each_year(s3_client):
    """Test that each year folder has at least one file."""
    for year in EXPECTED_YEARS:
        year_prefix = f"{BASE_PREFIX}{year}/"
        
        try:
            response = s3_client.list_objects_v2(
                Bucket=BUCKET_NAME,
                Prefix=year_prefix
            )
            
            file_count = len(response.get('Contents', []))
            assert file_count >= 1, f"No files found in year folder {year}"
            
            # Check specifically for the expected file name
            has_co2_file = any(
                obj['Key'].endswith(FILE_NAME) 
                for obj in response.get('Contents', [])
            )
            
            assert has_co2_file, f"Expected file {FILE_NAME} not found in year {year}"
            logger.info(f"Year {year} has {file_count} files, including co2_daily_mlo.csv")
            
        except ClientError as e:
            pytest.fail(f"Failed to list files for year {year}: {e}")

if __name__ == "__main__":
    # This allows the test file to be run directly
    pytest.main(["-v", __file__])
