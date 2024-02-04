# src/parquet_check.py

from pyspark.sql import SparkSession

def check_parquet_file(parquet_file_path):
    """
    Reads a Parquet file, reorders its columns to prioritize 'Organization Name' if present, 
    and displays information about the data.

    This function initializes a Spark session, reads the specified Parquet file into a DataFrame,
    and if 'Organization Name' exists among its columns, reorders the DataFrame to make it the first column.
    It then prints the DataFrame's schema, displays the first few rows, shows the total row count, 
    and finally stops the Spark session to release resources.

    Parameters:
    - parquet_file_path: str, the file system path to the Parquet file to be processed.

    Effects:
    - Prints the DataFrame schema, showing the structure of the data including column names and types.
    - Displays the first few rows of the DataFrame for a preview of the data.
    - Prints the total number of rows in the DataFrame, providing a sense of the dataset's size.
    - Stops the Spark session to clean up resources.
    """
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

