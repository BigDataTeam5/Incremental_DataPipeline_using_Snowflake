import json
import requests

def lambda_handler(event, context):
    """
    Fetches daily CO2 data from NOAA and returns the data in the response body.
    """
    url = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_daily_mlo.txt"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return {
            "statusCode": 200,
            "body": response.text  # Return the raw text data
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Error fetching CO2 data: {str(e)}"
        }
