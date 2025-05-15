# ETL_to_sqlite3
# Data Processing Pipeline with Spark and SQLite

This repository contains a Python script that processes CSV files located in a specified folder, loads the data into an SQLite database, and calculates basic statistics. It leverages Apache Spark for efficient data reading and processing and SQLite for persistent storage.

## Overview

The script performs the following steps:

1.  **Reads CSV Files:** It iterates through all CSV files in the `dataPruebaDataEngineer` folder that match files starting with four digits. It also processes a specific file named `validation.csv`.
2.  **Loads Data to SQLite:** For each processed CSV file, the data is loaded into the `user_transaction` table in the `my_db.db` SQLite database.
3.  **Calculates Statistics:** Basic statistics (row count, minimum price, maximum price, and avg of prices) are calculated for the 'price' column of each processed DataFrame.
4.  **Stores Statistics:** These statistics are stored in the `load_stats` table in the `my_db.db` SQLite database.
5.  **Prints Execution and Database Values:** The script prints the calculated statistics during the processing of each file and then queries the `load_stats` table to display aggregated statistics from the database.

## Repository Structure

**Contents:**

* `dataPruebaDataEngineer/`: This folder should contain your CSV data files. Ensure that the filenames of the main data files start with four digits (e.g., `2023-1.csv`) and that there is a file named `validation.csv`.
* `funtion_library.py`: This file contains the three Python functions used in the `main.py` script:
    * `read_csv_with_spark(file_path, schema, header=True)`: Reads a CSV file into a Spark DataFrame.
    * `load_df_to_sqlite(df, table_name, db_name)`: Loads data from a Spark DataFrame into an SQLite table.
    * `stats_calc(df)`: Calculates and returns basic statistics for the 'price' column of a Spark DataFrame.
* `main.py`: This is the main script that orchestrates the data processing pipeline. It sets up the Spark session, connects to the SQLite database, creates the necessary tables, processes the CSV files, and prints the statistics.

## Prerequisites

Before running the script, ensure you have the following installed:

* **Python 3.9**
* **Apache Spark:** You need to have Spark installed and configured on your system. (Tested with 3.3)
* **PySpark:** The Python API for Spark. You can install it using pip:
    ```bash
    pip install pyspark==3.32
    ```
* **SQLite3:** Python comes with built-in support for SQLite3, so you likely don't need to install it separately.

## Setup

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/migueldavare/ETL_to_sqlite3.git
    ```

2.  **Create the data folder:**
    ```bash
    mkdir dataPruebaDataEngineer
    ```

3.  **Place your CSV data files** inside the `dataPruebaDataEngineer` folder. Ensure that some filenames match the `^\d{4}` pattern and that you have a `validation.csv` file in this folder. The CSV files should have the following columns in order: `timestamp` (TEXT), `price` (INTEGER), and `user_id` (INTEGER).

## Running the Script

1.  Navigate to the repository directory in your terminal.
2.  Run the `main.py` script using Spark's `spark-submit`:
    ```bash
    spark-submit main.py
    ```


## Output

The script will print the following output to the console:

* For each processed CSV file:
    * Execution values including the number of rows loaded, the minimum price, the maximum price, and the average price calculated from the Spark DataFrame.
    * Database values showing the aggregated sum of rows, minimum price, maximum price, and average price retrieved from the `load_stats` table in the SQLite database after loading the current file's statistics.
* A final set of database values after processing the `validation.csv` file.

After the script completes, a SQLite database file named `my_db.db` will be created (if it doesn't exist) in the repository's root directory. This database will contain two tables:

* `user_transaction`: Contains all the transaction data loaded from the CSV files.
* `load_stats`: Contains the calculated statistics for each processed file.





