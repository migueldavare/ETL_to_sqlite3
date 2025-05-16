from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import min, max, sum
from pyspark.sql.types import StructType
import sqlite3

spark = SparkSession.builder.appName("pragma_test").getOrCreate()
conn= sqlite3.connect('my_db.db')
c=conn.cursor()

def read_csv_with_spark(file_path: str, schema: StructType, header: bool = True) -> DataFrame:
    """
    Reads a CSV file into a Spark DataFrame with a specified schema.

    Args:
        file_path (str): The path to the CSV file to be read. This can be a local file path
                         or a path to a distributed file system (e.g., HDFS, S3).
        schema (StructType): The schema to be applied to the DataFrame. This defines the
                             names and data types of the columns.
        header (bool, optional): Specifies whether the first row of the CSV file contains
                                 column headers. Defaults to True.

    Returns:
        pyspark.sql.DataFrame: A Spark DataFrame containing the data read from the CSV file
                               with the specified schema.  Returns None if an error occurs.

    Raises:
        Exception:  If an error occurs during the CSV reading process.
    """
    try:
        df = spark.read.csv(
            file_path,
            header=header,
            schema=schema,
        )
        return df
    except Exception as e:
        print(f"Error reading CSV file {file_path}: {e}")
        return None

def load_df_to_sqlite(df: DataFrame,table_name: str,db_name: str):
    """
    Loads data from a Spark DataFrame into an SQLite database table.

    Args:
        df (pyspark.sql.DataFrame): The Spark DataFrame containing the data to be loaded.
        table_name (str): The name of the table in the SQLite database where the data
                          will be inserted.
        db_name (str): The name (and path, if necessary) of the SQLite database file.
                         If the database file does not exist, it will be created.
    """
    placeholders = ", ".join(['?'] * len(df.columns))
    insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"

    for row in df.rdd.collect():
        try:
            c.execute(insert_sql, tuple(row))
        except sqlite3.Error as e:
            print(f"Error inserting row {row}: {e}")
    conn.commit()
    print(f"Data from Spark DataFrame loaded into table '{table_name}' in '{db_name}'.")

def stats_calc(df: DataFrame, id_load: int) -> list:
    """
    Calculates and prints basic statistics for the 'price' column of a Spark DataFrame.
    Returns a list containing a dictionary of these statistics.

    Args:
        df (pyspark.sql.DataFrame): The Spark DataFrame to analyze.  It is expected
            to have a column named 'price'.
        id_load (int): An identifier for the current data load or processing iteration.

    Returns:
        list: A list containing a dictionary with the calculated statistics.
            Returns an empty list if the DataFrame is empty.  The dictionary has the following keys:
              - "id_load":  The provided data load identifier.
              - "row_count": The number of rows in the DataFrame.
              - "min_price": The minimum value in the 'price' column.
              - "max_price": The maximum value in the 'price' column.
              - "sum_price": The sum of the values in the 'price' column.

    Raises:
        Exception: If an error occurs during the statistic calculation process
            (e.g., if the 'price' column does not exist).
    """
    try:
        row_count = df.count()
        if row_count > 0:
            min_val = df.agg(min("price")).alias("min").collect()[0]['min(price)']
            max_val = df.agg(max("price")).alias("max").collect()[0]['max(price)']
            sum_price = df.agg(sum("price")).alias("sum_price").collect()[0]['sum(price)']
            avg_price = sum_price / row_count
            print(f"Execution values: Rows to Load: {row_count}, Price Values Min: {min_val}, Max: {max_val}, Avg: {avg_price}")
            dict_stad = [{"id_load": id_load, "row_count": row_count, "min_price": min_val, "max_price": max_val, "sum_price": sum_price}]
            return dict_stad
        else:
            print("DataFrame is empty. No statistics calculated.")
            return []  # Return an empty list for an empty DataFrame
    except Exception as e:
        print(f"An error occurred during statistics calculation: {e}")
        return []
