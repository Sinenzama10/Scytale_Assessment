# src/spark_process.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, max, split, lower, trim
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

def transform_and_save(input_path, output_path):
    """
    Reads JSON files from the specified input path, transforms the data according to the defined schema,
    and saves the transformed data as a Parquet file partitioned by 'Organization Name' to the specified output path.
    
    Parameters:
    - input_path: Path to the input JSON files.
    - output_path: Path where the output Parquet file should be saved.
    """
    
    # Define the schema for the JSON data
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("state", StringType(), True),
        StructField("title", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("closed_at", TimestampType(), True),
        StructField("merged_at", TimestampType(), True),
        StructField("user", StructType([
            StructField("login", StringType(), True),
        ]), True),
        StructField("head", StructType([
            StructField("repo", StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("full_name", StringType(), True),
            ]), True),
        ]), True),
    ])

    # Initialize SparkSession
    spark = SparkSession.builder.appName("Process GitHub Repos").getOrCreate()

    # Read JSON files using the defined schema
    df = spark.read.schema(schema).json(input_path)

    # Check if DataFrame is not empty to proceed with transformation
    if not df.rdd.isEmpty():
        # Transformation logic: Split 'full_name' to get 'Organization Name', extract repo details, aggregate, and check compliance
        transformed_df = df.withColumn("Organization Name", split(col("head.repo.full_name"), "/")[0]) \
            .withColumn("repository_id", col("head.repo.id")) \
            .withColumn("repository_name", col("head.repo.name")) \
            .withColumn("repository_owner", col("user.login")) \
            .groupBy("Organization Name", "repository_id", "repository_name", "repository_owner") \
            .agg(
                  count("id").alias("num_prs"),
                  count(when(col("merged_at").isNotNull(), True)).alias("num_prs_merged"), #Adjusted logic
                  max("merged_at").alias("latest_merged_at")
             ) \
             .withColumn("is_compliant",
                         ((col("num_prs") == col("num_prs_merged")) & 
                          lower(trim(col("repository_owner"))).contains("scytale")).cast("boolean"))
                            

        # Save transformed DataFrame as Parquet, partitioned by 'Organization Name'
        transformed_df.write.partitionBy("Organization Name").mode("overwrite").parquet(output_path)
    else:
        print("No data found in any file.")

    # Stop the Spark session
    spark.stop()

