import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *

# ---------------------------------------------------------
# Initialize AWS Glue Job
# ---------------------------------------------------------

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ---------------------------------------------------------
# Read Parquet Data from Amazon S3
# ---------------------------------------------------------

df = spark.read.parquet(
    "s3://demo-bucket/raw-data/"
)

# ---------------------------------------------------------
# Print Schema
# ---------------------------------------------------------

print("===== SOURCE SCHEMA =====")
df.printSchema()

# ---------------------------------------------------------
# Data Transformations
# ---------------------------------------------------------

transformed_df = (
    df
    # Remove invalid records
    .filter(col("passenger_count") > 0)
    .filter(col("trip_distance") > 0)
    .filter(col("fare_amount") > 0)

    # Create derived date columns
    .withColumn(
        "trip_year",
        year(col("tpep_pickup_datetime"))
    )
    .withColumn(
        "trip_month",
        month(col("tpep_pickup_datetime"))
    )
    .withColumn(
        "trip_day",
        dayofmonth(col("tpep_pickup_datetime"))
    )

    # Calculate fare per mile
    .withColumn(
        "fare_per_mile",
        round(
            col("fare_amount") / col("trip_distance"),
            2
        )
    )
)

# ---------------------------------------------------------
# Write Transformed Data Back to S3
# ---------------------------------------------------------

transformed_df.write \
    .mode("overwrite") \
    .partitionBy("trip_year", "trip_month") \
    .parquet(
        "s3://demo-bucket/transformed-data/"
    )

# ---------------------------------------------------------
# Commit Glue Job
# ---------------------------------------------------------

job.commit()