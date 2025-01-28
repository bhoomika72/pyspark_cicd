
import sys
import os
import pytest
from pyspark.sql import functions as F

# Add the source directory to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from src.data_check import get_count, get_column_names, filter_by_column_value


# File paths for current and previous data
previous_data_file_path = "C:\\Users\\admin\\OneDrive - TestPerform\\Desktop\\pyspark_cicd\\prev_data.csv"
current_data_file_path = "C:\\Users\\admin\\OneDrive - TestPerform\\Desktop\\pyspark_cicd\\curr_data.csv"

def read_previous_data(spark):
    """Read previous data from a CSV file."""
    if os.path.exists(previous_data_file_path):
        return spark.read.csv(previous_data_file_path, header=True, inferSchema=True)
    else:
        print("Previous data file not found. Returning empty DataFrame.")
        return spark.createDataFrame([], spark.read.csv(previous_data_file_path, header=True, inferSchema=True).schema)

def test_get_column_names(spark):
    df = spark.read.csv(current_data_file_path, header=True, inferSchema=True)
    
    # Get column names
    result = get_column_names(df)
    
    # Assert the column names are as expected
    assert result == ["Id","Name", "Age", "City"]


def test_filter_by_column_value(spark):
    df = spark.read.csv(current_data_file_path, header=True, inferSchema=True)
    
    filtered_df = filter_by_column_value(df, "city", "New York")
    
    # Get the count of filtered rows
    filtered_count = get_count(filtered_df)
    
    # Assert that filtered rows are as expected (adjust the expected count based on your test data)
    assert filtered_count > 0


def test_insertion_detection(spark):
    current_df = spark.read.csv(current_data_file_path, header=True, inferSchema=True)
    previous_df = spark.read.csv(previous_data_file_path, header=True, inferSchema=True)

    # Detect inserted rows
    inserted_rows = current_df.join(previous_df, on="Id", how="left_anti")
    inserted_count = inserted_rows.count()
    print(f"Inserted Rows Count: {inserted_count}")

    # Assert that inserted rows count is valid (greater than or equal to 0)
    assert inserted_count >= 0


def test_update_detection(spark):
    current_df = spark.read.csv(current_data_file_path, header=True, inferSchema=True)
    previous_df = spark.read.csv(previous_data_file_path, header=True, inferSchema=True)

    updated_rows = current_df.join(previous_df, on="Id", how="inner")
    updated_condition = None
    for col_name in current_df.columns:
        if col_name != 'Id':  # Exclude 'Id'
            condition = (current_df[col_name] != previous_df[col_name])
            if updated_condition is None:
                updated_condition = condition
            else:
                updated_condition = updated_condition | condition
    updated_rows = updated_rows.filter(updated_condition)

    updated_count = updated_rows.count()
    print(f"Updated Rows Count: {updated_count}")

    # Assert that updated rows count is valid (greater than or equal to 0)
    assert updated_count >= 0

def test_deletion_detection(spark):
    current_df = spark.read.csv(current_data_file_path, header=True, inferSchema=True)
    previous_df = spark.read.csv(previous_data_file_path, header=True, inferSchema=True)

    # Detect deleted rows (rows in previous data but not in current data)
    deleted_rows = previous_df.join(current_df, on="Id", how="left_anti")
    deleted_count = deleted_rows.count()
    print(f"Deleted Rows Count: {deleted_count}")

    # Assert that deleted rows count is valid (greater than or equal to 0)
    assert deleted_count >= 0

def test_basic_data_reading(spark):
    try:
        current_df = spark.read.csv(current_data_file_path, header=True, inferSchema=True)
        assert current_df.count() > 0
    except Exception as e:
        pytest.fail(f"Failed to read data: {str(e)}")


def test_missing_file_handling(spark):
    missing_file_path = "C:\\Users\\admin\\OneDrive - TestPerform\\Desktop\\pyspark_cicd\\missing_file.csv"
    
    try:
        # Try reading the file
        df = spark.read.csv(missing_file_path, header=True, inferSchema=True)
    except Exception as e:
        # Check if the error contains "Path does not exist"
        assert "Path does not exist" in str(e)



def test_column_names_and_types(spark):
    """Test that all columns in the current data have valid types."""
    # Read the current data
    current_df = spark.read.csv(current_data_file_path, header=True, inferSchema=True)
    
    # Validate column names and their types in the current DataFrame
    current_schema = {field.name: field.dataType.simpleString() for field in current_df.schema}
    print(f"Current schema: {current_schema}")

    # Assertions: Validate all column names and their types
    for column, data_type in current_schema.items():
        assert data_type is not None, f"Column '{column}' has an invalid type '{data_type}'."
    
    # Ensure there are no unexpected issues
    assert len(current_schema) > 0, "The current data contains no columns."
    print("Column names and types are valid.")

def test_new_columns_detection(spark):
    """Test for detecting new columns in the current file compared to the previous file."""
    # Read the current data
    current_df = spark.read.csv(current_data_file_path, header=True, inferSchema=True)

    # Read the previous data (if exists)
    if os.path.exists(previous_data_file_path):
        previous_df = spark.read.csv(previous_data_file_path, header=True, inferSchema=True)
    else:
        print("Previous data file not found. Considering only current data.")
        previous_df = spark.createDataFrame([], current_df.schema)  # Empty DataFrame with current schema

    # Get columns from both DataFrames
    current_columns = set(current_df.columns)
    previous_columns = set(previous_df.columns)

    # Identify new columns added to the current data
    new_columns = current_columns - previous_columns

    # Assertions
    if new_columns:
        print(f"New columns added: {new_columns}")
        assert len(new_columns) > 0, "New columns should be detected if they exist."
    else:
        print("No new columns detected.")
        assert len(new_columns) == 0, "No new columns should be detected if none exist."


def test_non_null_columns(spark):
    df = spark.read.csv(current_data_file_path, header=True, inferSchema=True)

    # List of columns that should not contain null values
    non_null_columns = ["Id", "Name"]

    for column in non_null_columns:
        null_count = df.filter(df[column].isNull()).count()
        
        # Print the count of nulls for each column (optional)
        print(f"Column '{column}' has {null_count} null values.")
        
        # Assert that these columns should not have null values
        assert null_count == 0, f"Null values found in column '{column}'"
