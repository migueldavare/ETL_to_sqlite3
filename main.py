import sqlite3
import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from funtion_library import read_csv_with_spark,load_df_to_sqlite, stats_calc


folder_path="dataPruebaDataEngineer"
db_name='my_db.db'
pattern=r"^\d{4}" 
val_file='validation.csv'
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("price", IntegerType(), True),
    StructField("user_id", IntegerType(), True)
])
schema_stat = StructType([
    StructField("id_load", IntegerType(), True),
    StructField("row_count", IntegerType(), True),
    StructField("min_price", IntegerType(), True),
    StructField("max_price", IntegerType(), True),
    StructField("sum_price", IntegerType(), True)
])
query_stats="""
        SELECT 
            SUM(row_count) AS SUM,
            MIN(min_price)  AS MIN_PRICE,
            MAX(max_price)  AS MAX_PRICE,   
            SUM(sum_price)*1.0/SUM(row_count) AS AVG_P        
            FROM load_stats 
    """

spark = SparkSession.builder.appName("pragma_test").getOrCreate()
conn= sqlite3.connect('my_db.db')
c=conn.cursor()
# Creating Tables
c.execute("""
        CREATE TABLE user_transaction (
            timestamp TEXTO,
            price INTEGER,
            user_id INTEGER
            )
    """)
conn.commit()
c.execute("""
        CREATE TABLE load_stats (
                id_load TEINTEGERXTO,
                row_count INTEGER,
                min_price INTEGER,
                max_price INTEGER,
                sum_price INTEGER
                )
        """)
conn.commit()

filenames = [f for f in os.listdir(folder_path) if re.match(pattern, f)]
id_load=0

for file in filenames:
    file_path= f"{folder_path}/{file}"
    df = read_csv_with_spark(file_path,schema)
    load_df_to_sqlite(df,'user_transaction',db_name)
    dict_stad=stats_calc(df)
    df_stat=spark.createDataFrame(dict_stad,schema=schema_stat)
    load_df_to_sqlite(df_stat,'load_stats',db_name)
    stats=c.execute(query_stats).fetchall()
    print(f"DB values: Total Rows: {stats[0][0]},Price Values Min: {stats[0][1]}, Max: {stats[0][2]}, Avg: {stats[0][3]}")
    print("\n")
    id_load+=1
    
print("Processing Validation File")
val_file_path= f"{folder_path}/{val_file}"
df = read_csv_with_spark(val_file_path,schema)
load_df_to_sqlite(df,'user_transaction',db_name)
dict_stad=stats_calc(df)
df_stat=spark.createDataFrame(dict_stad,schema=schema_stat)
load_df_to_sqlite(df_stat,'load_stats',db_name)
stats=c.execute(query_stats).fetchall()
print(f"DB values: Total Rows: {stats[0][0]},Price Values Min: {stats[0][1]}, Max: {stats[0][2]}, Avg: {stats[0][3]}")

conn.close()
spark.stop()