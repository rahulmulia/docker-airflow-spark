import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from decouple import config
import logging
from requests.exceptions import RequestException

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session():
    """Create and configure SparkSession with AWS credentials"""
    try:
        aws_access_key = config('AWS_ACCESS_KEY')
        aws_secret_key = config('AWS_SECRET_KEY')

        spark = SparkSession \
            .builder \
            .appName("MutualFundDataExtraction") \
            .getOrCreate()

        logger.info("SparkSession created successfully")
        return spark
    except Exception as e:
        logger.error(f"Failed to create SparkSession: {str(e)}")
        raise


def fetch_mutual_fund_data(url):
    """Fetch data from mutual fund API"""
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes
        return response.text
    except RequestException as e:
        logger.error(f"Failed to fetch data from API: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while fetching data: {str(e)}")
        raise


def process_mutual_fund_data(spark, data):
    """Process mutual fund data using Spark"""
    try:
        # Create RDD from the API response
        rdd = spark.sparkContext.parallelize([data])

        # Create DataFrame from JSON
        raw_json_dataframe = spark.read.json(rdd)
        logger.info("Successfully created DataFrame from JSON")

        # Print schema for debugging
        logger.info("DataFrame Schema:")
        raw_json_dataframe.printSchema()

        # Create temporary view
        raw_json_dataframe.createOrReplaceTempView("Mutual_benefit")

        # Process the DataFrame
        processed_df = raw_json_dataframe.withColumn("data", F.explode(F.col("data"))) \
            .withColumn('meta', F.expr("meta")) \
            .select("data.*", "meta.*")

        return processed_df
    except Exception as e:
        logger.error(f"Error processing mutual fund data: {str(e)}")
        raise


def save_to_csv(dataframe, filepath):
    """Save DataFrame to CSV"""
    try:
        pandas_df = dataframe.toPandas()
        pandas_df.to_csv(filepath, index=False)
        logger.info(f"Successfully saved data to {filepath}")
    except Exception as e:
        logger.error(f"Error saving to CSV: {str(e)}")
        raise


def main():
    spark = None
    try:
        # Initialize Spark
        spark = create_spark_session()

        # Fetch data from API
        mf_url = "https://api.mfapi.in/mf/118550"
        raw_data = fetch_mutual_fund_data(mf_url)
        logger.info("Successfully fetched mutual fund data")

        # Process the data
        processed_df = process_mutual_fund_data(spark, raw_data)

        # Show the results
        logger.info("Processed Data Preview:")
        processed_df.show(100, False)

        # Save to CSV
        save_to_csv(processed_df, "dataframe.csv")

    except Exception as e:
        logger.error(f"Main process failed: {str(e)}")
        raise

    finally:
        # Clean up Spark session
        if spark:
            try:
                spark.stop()
                logger.info("SparkSession stopped successfully")
            except Exception as e:
                logger.error(f"Error stopping SparkSession: {str(e)}")


if __name__ == "__main__":
    main()