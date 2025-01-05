"""
Project Title: Weather ETL Pipeline
Author: Shrinand Perumal
Date: 1/4/2025
Description:
    This script extracts data from AWS S3 buckets into a Spark Dataframe.
    Data is transformed using various operations before being
    loaded into a PostgreSQL relational database. Operations
    are recorded using logs.

Usage:
    - Ensure your database and S3 bucket connection is configured correctly.
"""


import boto3
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType
from pyspark.sql.functions import monotonically_increasing_id, col, round, to_date, expr, lit
from pyspark.sql.functions import avg, max,min, sum, when
from datetime import datetime



# Initialize S3 Client
def get_s3_data(bucket_name, object_key, aws_region, aws_access_key, aws_secret_key):
    s3_client = boto3.client(
        service_name='s3',
        region_name=aws_region,
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    json_content = response['Body'].read().decode('utf-8')
    return json.loads(json_content)

# Function to initialize Spark session
def initialize_spark():
    """ Initializes Spark session """
    return SparkSession.builder \
        .appName("MySparkSession") \
        .master("local[*]") \
        .config("replace-with-jar-file-name-.jar", "replace-with-users-postgresql-jar-file-path") \
        .getOrCreate()


def define_schema():
    """ Defines the schema used to define the structure
        of the Spark Dataframe used to read in incoming
        JSON formatted data"""
    return StructType([
        StructField("date", StringType(), True),
        StructField("relative_humidity_2m", DoubleType(), True),
        StructField("dew_point_2m", DoubleType(), True),
        StructField("apparent_temperature", DoubleType(), True),
        StructField("precipitation_probability", DoubleType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("rain", DoubleType(), True),
        StructField("snowfall", DoubleType(), True),
        StructField("pressure_msl", DoubleType(), True),
        StructField("surface_pressure", DoubleType(), True),
        StructField("cloud_cover", DoubleType(), True),
        StructField("evapotranspiration", DoubleType(), True),
        StructField("wind_speed_10m", DoubleType(), True),
        StructField("wind_speed_80m", DoubleType(), True),
        StructField("wind_speed_120m", DoubleType(), True),
        StructField("temperature_2m", DoubleType(), True),
        StructField("temperature_80m", DoubleType(), True),
        StructField("temperature_120m", DoubleType(), True),
        StructField("soil_temperature_0cm", DoubleType(), True),
        StructField("soil_temperature_6cm", DoubleType(), True),
        StructField("soil_temperature_18cm", DoubleType(), True),
        StructField("soil_moisture_0_to_1cm", DoubleType(), True),
        StructField("soil_moisture_1_to_3cm", DoubleType(), True),
        StructField("soil_moisture_3_to_9cm", DoubleType(), True),
        StructField("uv_index_clear_sky", DoubleType(), True),
        StructField("is_day", DoubleType(), True),
        StructField("total_column_integrated_water_vapour", DoubleType(), True)
    ])


def process_dataframe(df, city_name):
    """ Function for handling all the operations involving aggregating,
        rounding, and creating new columns using expressions all through
        using PySpark dataframe"""
  df = df.withColumn("city", lit(city_name))
  df = df.withColumn("heat_index", expr("""
      temperature_2m + (0.5555 * (6.11 * exp((17.27 * temperature_2m) / (237.7 + temperature_2m)) - 10) * (1 - relative_humidity_2m / 100))
  """))
  df = df.withColumn("wind_chill", expr("""
      35.74 + (0.6215 * temperature_2m) - (35.75 * pow(wind_speed_10m, 0.16)) + (0.4275 * temperature_2m * pow(wind_speed_10m, 0.16))
  """))

  df = df.withColumn("potential_evapotranspiration", expr("""
      0.0023 * (temperature_2m + 17.8) * sqrt(abs(temperature_120m - temperature_80m)) * total_column_integrated_water_vapour
  """))

  df = df.withColumn("atmospheric_pressure_diff", col("pressure_msl") - col("surface_pressure"))
  df = df.withColumn("precipitation_rate", col("precipitation") + col("snowfall"))
  df = df.withColumn("is_day", when(df["is_day"] == 1.0, True).otherwise(False))
  columns_to_round = [
      "dew_point_2m",
      "apparent_temperature",
      "pressure_msl",
      "wind_speed_80m",
      "wind_speed_10m",
      "wind_speed_120m",
      "temperature_2m",
      "temperature_80m",
      "temperature_120m",
      "soil_temperature_0cm",
      "soil_temperature_6cm",
      "soil_temperature_18cm",
      "soil_moisture_0_to_1cm",
      "soil_moisture_1_to_3cm",
      "soil_moisture_3_to_9cm",
      "total_column_integrated_water_vapour",
      "heat_index",
      "wind_chill",
      "potential_evapotranspiration",
      "atmospheric_pressure_diff",
      "precipitation_rate",
      "rain",
      "precipitation"
  ]

  num_decimals = 4

  for column in columns_to_round:
      df = df.withColumn(column, round(col(column), num_decimals))

  return df

def split_dataframe(df):
    """ Splits the data for labeling forecast_data column as true or false
        before concatenating the dataframe back together """
  df = df.withColumn("date", to_date("date"))
  df_with_index = df.withColumn("hour", monotonically_increasing_id())

  # Split the DataFrame
  total_rows = df_with_index.count()
  midpoint = total_rows // 2

  first_half = df_with_index.filter(col("hour") < midpoint)
  second_half = df_with_index.filter(col("hour") >= midpoint)

  # Add forecast_data column to first_half and second_half
  first_half = first_half.withColumn("forecast_data", lit(True))
  second_half = second_half.withColumn("forecast_data", lit(False))

  # Adjust the row_index for second_half
  second_half = second_half.withColumn("hour", col("hour") - midpoint)

  concatenated_df = first_half.union(second_half)
  return concatenated_df

# Select relevant columns for the "meteorological_calculations" table
def send_data_to_db(concatenated_df, jdbc_url, properties):
    """ Creates smaller Spark Dataframes for all tables
        before writing them to their corresponding tables
        in the database"""
  meteorological_calculations_df = concatenated_df.select(
      "city",
      "date",
      "hour",
      "forecast_data",
      "heat_index",
      "wind_chill",
      "precipitation_rate",
      "potential_evapotranspiration",
      "atmospheric_pressure_diff",
      "is_day"
  )

  meteorological_calculations_df.write \
      .format("jdbc") \
      .option("url", jdbc_url) \
      .option("dbtable", "meteorological_calculations") \
      .option("user", properties["user"]) \
      .option("password", properties["password"]) \
      .option("driver", properties["driver"]) \
      .mode("append") \
      .save()

  print("uploaded meteorological calculations to db")

  aggregates_df = concatenated_df.groupBy("city", "date", "forecast_data").agg(
      sum("precipitation").alias("total_precipitation"),
      avg("temperature_2m").alias("average_temperature_2m"),
      max("temperature_2m").alias("max_temperature_2m"),
      min("temperature_2m").alias("min_temperature_2m"),
      avg("relative_humidity_2m").alias("average_relative_humidity_2m")
  )
  daily_weather_summary_df = aggregates_df.select(
      "city",
      "date",
      "forecast_data",
      "total_precipitation",
      "average_temperature_2m",
      "max_temperature_2m",
      "min_temperature_2m",
      "average_relative_humidity_2m"
  )
  columns_to_round = [
      "total_precipitation",
      "average_temperature_2m",
      "average_relative_humidity_2m",
  ]
  num_decimals = 4

  for column in columns_to_round:
      daily_weather_summary_df  = daily_weather_summary_df.withColumn(column, round(col(column), num_decimals))


  daily_weather_summary_df.write \
      .format("jdbc") \
      .option("url", jdbc_url) \
      .option("dbtable", "daily_weather_summary") \
      .option("user", properties["user"]) \
      .option("password", properties["password"]) \
      .option("driver", properties["driver"]) \
      .mode("append") \
      .save()

#  print("uploaded daily weather summary to db")
  # Database properties

  hourly_precipitation_evaporation_df = concatenated_df.select(
      "city",
      "date",
      "hour",
      "forecast_data",
      "precipitation",
      "rain",
      "snowfall",
      "evapotranspiration",
      "is_day"
  )
  hourly_precipitation_evaporation_df.write \
      .format("jdbc") \
      .option("url", jdbc_url) \
      .option("dbtable", "hourly_precipitation_evaporation") \
      .option("user", properties["user"]) \
      .option("password", properties["password"]) \
      .option("driver", properties["driver"]) \
      .mode("append") \
      .save()

 # print("uploaded hourly precipitation evaporation to df")

  hourly_soil_conditions_df = concatenated_df.select(
      "city",
      "date",
      "hour",
      "forecast_data",
      "soil_temperature_0cm",
      "soil_temperature_6cm",
      "soil_temperature_18cm",
      "soil_moisture_0_to_1cm",
      "soil_moisture_1_to_3cm",
      "soil_moisture_3_to_9cm",
      "is_day"
  )

  hourly_soil_conditions_df.write \
      .format("jdbc") \
      .option("url", jdbc_url) \
      .option("dbtable", "hourly_soil_conditions") \
      .option("user", properties["user"]) \
      .option("password", properties["password"]) \
      .option("driver", properties["driver"]) \
      .mode("append") \
      .save()

 # print("uploaded hourly soil conditions to db")

  hourly_weather_conditions_df  = concatenated_df.select(
      "city",
      "date",
      "hour",
      "forecast_data",
      "temperature_2m",
      "temperature_80m",
      "temperature_120m",
      "relative_humidity_2m",
      "dew_point_2m",
      "apparent_temperature",
      "cloud_cover",
      "uv_index_clear_sky",
      "is_day"
  )

  hourly_weather_conditions_df.write \
      .format("jdbc") \
      .option("url", jdbc_url) \
      .option("dbtable", "hourly_weather_conditions") \
      .option("user", properties["user"]) \
      .option("password", properties["password"]) \
      .option("driver", properties["driver"]) \
      .mode("append") \
    .save()

 # print("uploaded hourly weather conditionos to db")

  hourly_wind_atmospheric_conditions_df = concatenated_df.select(
      "city",
      "date",
      "hour",
      "forecast_data",
      "pressure_msl",
      "surface_pressure",
      "wind_speed_10m",
      "wind_speed_80m",
      "wind_speed_120m",
      "is_day"
  )
  hourly_wind_atmospheric_conditions_df.write \
      .format("jdbc") \
      .option("url", jdbc_url) \
      .option("dbtable", "hourly_wind_atmospheric_conditions") \
      .option("user", properties["user"]) \
      .option("password", properties["password"]) \
      .option("driver", properties["driver"]) \
      .mode("append") \
      .save()
 # print("uploaded hourly wind atmospheric weather conditions to db")


def initialize_logging():
    """Ensure the 'log' directory exists."""
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    return log_dir

def log_message(file_path, message):
    """Log a message to the specified file."""
    with open(file_path, "a") as log_file:
        log_file.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")

def main():
    # S3 Details
    cities = [
        "Chicago", "San Francisco", "London", "New Delhi", "Tokyo", "New York",
        "Paris", "Sydney", "Moscow", "Beijing", "Cape Town", "Berlin",
        "Rio de Janeiro", "Dubai", "Singapore"
    ]

    # Initialize logging
    log_dir = initialize_logging()
    today = datetime.now()
    today_date_str = today.strftime("%Y-%m-%d")
    year = today.strftime("%Y")
    month = str(int(today.strftime("%m")))  # Remove leading zero from the month
    day = str(int(today.strftime("%d")))    # Remove leading zero from the day
    daily_log_file = os.path.join(log_dir, f"upload_to_database_{today_date_str}.log")
    summary_log_file = os.path.join(log_dir, "summary.log")

    # Add an entry to the summary log
    log_message(summary_log_file, f"Created log file: {daily_log_file}")

    spark = initialize_spark()
    weather_schema = define_schema()

    bucket_name = 'replace-with-users-bucket-name'
    aws_region  = 'replace-with-users-region'
    aws_access_key = os.environ.get("AWS_ACCESS_KEY")
    aws_secret_key = os.environ.get("AWS_SECRET_KEY")

    jdbc_url = "your-db-endpoint"
    properties = {
        "user": "your-db-name",
        "password": "your-db-password",
        "driver": "org.postgresql.Driver"
    }

    print("day ", day)
    print("month ", month)
    print("year ", year)
    for city_name in cities:
        try:
            # Get data from S3
            object_key = f"{city_name}/{year}/{month}/{day}/weather_data.json"
            print("object_key = ", object_key)
            data = get_s3_data(bucket_name, object_key, aws_region, aws_access_key, aws_secret_key)
            log_message(daily_log_file, f"Fetched data for city: {city_name} (Key: {object_key})")

            # Process DataFrame
            df = spark.read.json(spark.sparkContext.parallelize([data]), schema=weather_schema)
            processed_df = process_dataframe(df, city_name)

            # Split DataFrame
            concatenated_df = split_dataframe(processed_df)

            # Send data to the database
            send_data_to_db(concatenated_df, jdbc_url, properties)
            log_message(daily_log_file, f"Uploaded data for city: {city_name} (Key: {object_key})")

        except Exception as e:
            log_message(daily_log_file, f"Error processing city: {city_name} - {str(e)}")

if __name__ == "__main__":
    main()