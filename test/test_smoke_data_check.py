import sys
import os

# Add the source directory to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# Set environment variables for PySpark
os.environ['SPARK_HOME'] = 'C:\\spark-3.5.4-bin-hadoop3'
os.environ['PYSPARK_PYTHON'] = 'C:\\Users\\admin\\AppData\\Local\\Programs\\Python\\Python39\\python.exe'

from src.data_check import get_count, get_column_names, filter_by_column_value


def test_csv_file_loading(spark):

    file_path = "C:\\Users\\admin\\OneDrive - TestPerform\\Desktop\\sample_data.csv"
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    assert df is not None
    assert df.count() > 0  # Ensures data exists in the file


def test_basic_get_count(spark):

    data = [("Test", 1), ("Data", 2)]
    df = spark.createDataFrame(data, ["name", "value"])

    # Test the `get_count` function
    result = get_count(df)

    assert result == 2  # Ensure the row count matches the data


def test_basic_get_column_names(spark):

    data = [("Test", 1), ("Data", 2)]
    df = spark.createDataFrame(data, ["name", "value"])

    # Test the `get_column_names` function
    result = get_column_names(df)

    assert result == ["name", "value"]  # Ensure the column names match


def test_basic_filter_by_column_value(spark):

    data = [("Alice", "New York"), ("Bob", "Los Angeles"), ("Charlie", "New York")]
    df = spark.createDataFrame(data, ["name", "city"])

    # Test the `filter_by_column_value` function
    filtered_df = filter_by_column_value(df, "city", "New York")

    assert filtered_df.count() == 2  # Ensure only rows with "New York" are filtered
