#Just a script to visually validate the output of the parquet files
from pyspark.sql import SparkSession

def check_parquet_file():
    # Initialize Spark session
    spark = SparkSession.builder.appName("ParquetFileCheck").getOrCreate()

    # Path to your parquet file
    parquet_file_path = '../output/prs_aggregated.parquet'

    # Read the parquet file into a DataFrame
    parquet_df = spark.read.parquet(parquet_file_path)

    # Show the DataFrame schema to verify structure
    print("DataFrame Schema:")
    parquet_df.printSchema()

    # Show the first few rows of the DataFrame
    print("First few rows of the DataFrame:")
    parquet_df.show()

    # If you just want to verify the row count
    print("Row count:", parquet_df.count())

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    check_parquet_file()
