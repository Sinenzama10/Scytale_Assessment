# src/spark_process.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, max, split, expr, lit, coalesce, lower
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, BooleanType

def transform_and_save(input_path, output_path):
    spark = SparkSession.builder.appName("GitHub Data Processing").getOrCreate()

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
        StructField("name", StringType(), True),
        StructField("owner", StructType([
            StructField("login", StringType(), True)
        ]), True),
        StructField("description", StringType(), True)
    ])

    df = spark.read.schema(schema).json(input_path)

    df = df.withColumn("DataType", when(col("title").isNotNull(), "PR").otherwise("Repo"))

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
                       .withColumn("is_compliant", 
                                   (col("num_prs") == col("num_prs_merged")) & 
                                   #(col("num_prs") > 0) &   Not sure what to do about empty repo but this is an option
                                   lower(col("repository_owner")).contains("scytale")
                       )

    transformed_df.write.partitionBy("Organization Name").mode("overwrite").parquet(output_path)

    spark.stop()




