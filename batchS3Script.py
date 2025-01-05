"""
Project Title: Weather ETL Pipeline
Author: Shrinand Perumal
Date: 1/4/2025
Description:
    This script uses a weather API to acquire certain features of data
    that are sent to an AWS S3 bucket using date structured organization.
    Operations are recorded using logs.

Usage:
    - Ensure your database and S3 bucket connection is configured correctly.
"""

import boto3
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
from datetime import datetime, timezone
import os
import json

# AWS S3 Configuration
AWS_S3_BUCKET_NAME = 'replace-with-users-bucket-name'
AWS_REGION = 'replace-with-users-region'
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY")

# Initialize S3 Client
s3_client = boto3.client(
    service_name='s3',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

# Logging configuration
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Summary log file
SUMMARY_LOG_FILE = os.path.join(LOG_DIR, "summary.log")

def log_summary(message):
    """Append a message to the summary log."""
    with open(SUMMARY_LOG_FILE, "a") as summary_file:
        summary_file.write(message + "\n")

def log_daily(city_messages, date):
    """Create a single daily log file for all cities."""
    daily_log_file = os.path.join(LOG_DIR, f"upload_toS3_{date}.log")
    with open(daily_log_file, "w") as daily_file:
        daily_file.writelines(city_messages)
    log_summary(f"Daily log created: {daily_log_file}")
    print(f"Daily log saved: {daily_log_file}")

# Fetching weather data from an API
def fetch_weather_data(latitude, longitude):
    """Fetch weather data using Open-Meteo API."""
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": [
            "temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature",
            "precipitation_probability", "precipitation", "rain", "snowfall", "pressure_msl",
            "surface_pressure", "cloud_cover", "evapotranspiration", "wind_speed_10m",
            "wind_speed_80m", "wind_speed_120m", "temperature_2m", "temperature_80m", "temperature_120m",
            "soil_temperature_0cm", "soil_temperature_6cm", "soil_temperature_18cm",
            "soil_moisture_0_to_1cm", "soil_moisture_1_to_3cm", "soil_moisture_3_to_9cm",
            "uv_index_clear_sky", "is_day", "total_column_integrated_water_vapour"
        ],
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch",
        "past_days": 1,
        "forecast_days": 1
    }

    session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)
    return openmeteo.weather_api(url, params=params)[0]

def process_weather_data(response):
    """Organize weather data."""
    hourly = response.Hourly()
    timestamps = pd.to_datetime(hourly.Time(), unit="s", utc=True)

    # Organize data
    data = {"date": timestamps}
    variables = [
        "temperature_2m", "relative_humidity_2m", "dew_point_2m",
        "apparent_temperature", "precipitation_probability", "precipitation",
        "rain", "snowfall", "pressure_msl", "surface_pressure",
        "cloud_cover", "evapotranspiration", "wind_speed_10m",
        "wind_speed_80m", "wind_speed_120m", "temperature_2m",  "temperature_80m",
        "temperature_120m", "soil_temperature_0cm", "soil_temperature_6cm",
        "soil_temperature_18cm", "soil_moisture_0_to_1cm", "soil_moisture_1_to_3cm",
        "soil_moisture_3_to_9cm", "uv_index_clear_sky", "is_day",
        "total_column_integrated_water_vapour"
    ]

    for i, variable in enumerate(variables):
        try:
            data[variable] = hourly.Variables(i).ValuesAsNumpy()
        except IndexError:
            data[variable] = None  # Fill missing variables

    # Create a single DataFrame for all data
    df = pd.DataFrame(data)

    return df

def upload_to_s3(dataframe, city):
    """Upload DataFrame to AWS S3 as JSON."""
    json_data = dataframe.to_json(orient="records", date_format="iso")
    now = datetime.now(timezone.utc)
    year, month, day = now.year, now.month, now.day
    object_name = f"{city}/{year}/{month}/{day}/weather_data.json"
    s3_client.put_object(Body=json_data, Bucket=AWS_S3_BUCKET_NAME, Key=object_name)
    return object_name

# List of major cities with latitude and longitude
cities = [
    {"city": "Chicago", "latitude": 41.878113, "longitude": -87.629799},
    {"city": "San Francisco", "latitude": 37.774929, "longitude": -122.419416},
    {"city": "London", "latitude": 51.507351, "longitude": -0.127758},
    {"city": "New Delhi", "latitude": 28.613939, "longitude": 77.209023},
    {"city": "Tokyo", "latitude": 35.689487, "longitude": 139.691711},
    {"city": "New York", "latitude": 40.712776, "longitude": -74.005974},
    {"city": "Paris", "latitude": 48.856613, "longitude": 2.352222},
    {"city": "Sydney", "latitude": -33.868820, "longitude": 151.209290},
    {"city": "Moscow", "latitude": 55.755825, "longitude": 37.617298},
    {"city": "Beijing", "latitude": 39.904202, "longitude": 116.407394},
    {"city": "Cape Town", "latitude": -33.924870, "longitude": 18.424055},
    {"city": "Berlin", "latitude": 52.520008, "longitude": 13.404954},
    {"city": "Rio de Janeiro", "latitude": -22.906847, "longitude": -43.172897},
    {"city": "Dubai", "latitude": 25.276987, "longitude": 55.296249},
    {"city": "Singapore", "latitude": 1.352083, "longitude": 103.819836}
]

# Main logic
city_logs = []
current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

for city_info in cities:
    city_name = city_info["city"]
    latitude, longitude = city_info["latitude"], city_info["longitude"]

    try:
        response = fetch_weather_data(latitude, longitude)
        data = process_weather_data(response)

        # Upload data to S3
        object_key = upload_to_s3(data, city_name)

        # Add to log
        city_logs.append(f"{city_name}: Successfully uploaded to {object_key}\n")

    except Exception as e:
        city_logs.append(f"{city_name}: Failed to process due to error: {e}\n")

# Write daily log and update summary
log_daily(city_logs, current_date)