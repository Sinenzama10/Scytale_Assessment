# parquet_check.py
from pyspark.sql import SparkSession

def check_parquet_file(parquet_file_path):
    spark = SparkSession.builder.appName("ParquetFileCheck").getOrCreate()
    parquet_df = spark.read.parquet(parquet_file_path)
    print("DataFrame Schema:")
    parquet_df.printSchema()
    print("First few rows of the DataFrame:")
    parquet_df.show()
    print("Row count:", parquet_df.count())
    spark.stop()
