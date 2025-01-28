import sys
import os

# Add the source directory to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# Set environment variables for PySpark
os.environ['SPARK_HOME'] = 'C:\\spark-3.5.4-bin-hadoop3'
os.environ['PYSPARK_PYTHON'] = 'C:\\Users\\admin\\AppData\\Local\\Programs\\Python\\Python39\\python.exe'

from src.data_check import get_count, get_column_names, filter_by_column_value


def test_get_count(spark):
    df = spark.read.csv("C:\\Users\\admin\\OneDrive - TestPerform\\Desktop\\sample_data.csv", header=True, inferSchema=True)
    
    # Get the count
    result = get_count(df)
    
    # Assert the count matches the expected value
    assert result == 20  # Replace with the actual row count of your test file

def test_get_column_names(spark):
    df = spark.read.csv("C:\\Users\\admin\\OneDrive - TestPerform\\Desktop\\sample_data.csv", header=True, inferSchema=True)
    
    # Get column names
    result = get_column_names(df)
    
    # Assert the column names are as expected
    assert result == ["Name", "Age", "City"]

def test_filter_by_column_value(spark):
    df = spark.read.csv("C:\\Users\\admin\\OneDrive - TestPerform\\Desktop\\sample_data.csv", header=True, inferSchema=True)
    
    filtered_df = filter_by_column_value(df, "city", "New York")
    
    # Get the count of filtered rows
    filtered_count = get_count(filtered_df)
    
    # Assert that filtered rows are as expected (adjust the expected count based on your test data)
    assert filtered_count > 0
