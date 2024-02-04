# src/parquet_check.py

from pyspark.sql import SparkSession

def check_parquet_file(parquet_file_path):
    spark = SparkSession.builder.appName("ParquetFileCheck").getOrCreate()
    
    # Read the Parquet file
    parquet_df = spark.read.parquet(parquet_file_path)
    
    # Reorder columns to have 'Organization Name' as the first column
    all_columns = parquet_df.columns
    if 'Organization Name' in all_columns:  # Check if 'Organization Name' is in the columns
        reordered_columns = ['Organization Name'] + [column for column in all_columns if column != 'Organization Name']
        parquet_df = parquet_df.select(*reordered_columns)
    
    # Show the DataFrame schema after reordering
    print("DataFrame Schema:")
    parquet_df.printSchema()
    
    # Show the first few rows of the DataFrame
    print("First few rows of the DataFrame:")
    parquet_df.show()
    
    # Print the row count
    print("Row count:", parquet_df.count())
    
    # Stop the Spark session
    spark.stop()
