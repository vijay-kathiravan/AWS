# glue_spotify_transform.py
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATABASE_NAME', 'TABLE_NAME', 'TARGET_BUCKET'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read data using Glue Catalog (from crawler metadata)
datasource = glueContext.create_dynamic_frame.from_catalog(
    database=args['DATABASE_NAME'],
    table_name=args['TABLE_NAME']
)

print(f"Schema from catalog: {datasource.schema()}")
print(f"Original record count: {datasource.count()}")

# Convert to Spark DataFrame for easier manipulation
df = datasource.toDF()

# Flatten spotify_data struct using catalog schema
spotify_df = df.select(
    F.col("spotify_data.*"),  # Extract all nested fields based on catalog schema
    F.col("timestamp"),
    F.col("batch_id"),
    F.col("record_id")
)

# Simple null cleaning
cleaned_df = spotify_df.filter(
    F.col("track_id").isNotNull() &
    F.col("track_name").isNotNull()
).dropDuplicates(['track_id'])

print(f"Cleaned record count: {cleaned_df.count()}")

# Convert back to DynamicFrame for Glue optimizations
cleaned_dynamic_frame = DynamicFrame.fromDF(cleaned_df, glueContext, "cleaned_data")

# Write to Silver bucket as Parquet using Glue
glueContext.write_dynamic_frame.from_options(
    frame=cleaned_dynamic_frame,
    connection_type="s3",
    connection_options={
        "path": f"s3://{args['TARGET_BUCKET']}/spotify-parquet/",
        "partitionKeys": ["track_genre"]  # Partition by genre
    },
    format="parquet",
    format_options={
        "compression": "snappy"
    }
)

print(f"Data successfully written to s3://{args['TARGET_BUCKET']}/spotify-parquet/")

job.commit()