# src/spark_process.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, max, split, expr, lit, coalesce
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, BooleanType

def transform_and_save(input_path, output_path):
    """
    Reads JSON files from the specified input path, transforms the data according to the defined schema,
    and saves the transformed data as a Parquet file partitioned by 'Organization Name' to the specified output path.
    
    Parameters:
    - input_path: Path to the input JSON files.
    - output_path: Path where the output Parquet file should be saved.
    """
    spark = SparkSession.builder.appName("GitHub Data Processing").getOrCreate()

    # Adjusted schema to include repository information
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("state", StringType(), True),
        StructField("title", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("closed_at", TimestampType(), True),
        StructField("merged_at", TimestampType(), True),
        StructField("user", StructType([
            StructField("login", StringType(), True)
        ]), True),
        StructField("head", StructType([
            StructField("repo", StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("full_name", StringType(), True)
            ]), True)
        ]), True),
        # Additional fields for repository details
        StructField("name", StringType(), True),
        StructField("owner", StructType([
            StructField("login", StringType(), True)
        ]), True),
        StructField("description", StringType(), True)
    ])

    df = spark.read.schema(schema).json(input_path)

    # Identify if the data row is PR or repository info based on 'title' or 'name' field presence
    df = df.withColumn("DataType", when(col("title").isNotNull(), "PR").otherwise("Repo"))

    # Process PR and Repo data separately using 'DataType' column
    transformed_df = df.withColumn("Organization Name", when(col("DataType") == "PR", split(col("head.repo.full_name"), "/")[0]).otherwise(split(col("owner.login"), "/")[0])) \
                       .withColumn("repository_id", coalesce(col("head.repo.id"), col("id"))) \
                       .withColumn("repository_name", coalesce(col("head.repo.name"), col("name"))) \
                       .withColumn("repository_owner", coalesce(split(col("head.repo.full_name"), "/").getItem(0), col("owner.login"))) \
                       .groupBy("Organization Name", "repository_id", "repository_name", "repository_owner") \
                       .agg(
                           count(when(col("DataType") == "PR", True)).alias("num_prs"),
                           count(when(col("merged_at").isNotNull(), True)).alias("num_prs_merged"),
                           max("merged_at").alias("latest_merged_at")
                       ) \
                       .withColumn("is_compliant", expr("num_prs_merged > 0 AND lower(trim(repository_owner)) LIKE '%scytale%'").cast(BooleanType()))

    transformed_df.write.partitionBy("Organization Name").mode("overwrite").parquet(output_path)

    spark.stop()

