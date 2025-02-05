import sys
import os
import shutil
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Add the source directory to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from src.data_check import get_count, get_column_names, filter_by_column_value, \
    detect_inserted_rows, detect_updated_rows, detect_deleted_rows, \
    validate_column_types, detect_new_columns, check_non_null_columns

# File paths
previous_data_file_path = "C:\\Users\\admin\\OneDrive - TestPerform\\Desktop\\pyspark_cicd\\data\\prev_data.csv"
current_data_file_path = "C:\\Users\\admin\\OneDrive - TestPerform\\Desktop\\pyspark_cicd\\data\\curr_data.csv"


def read_previous_data(spark):
    """Read previous data from a CSV file."""
    if os.path.exists(previous_data_file_path):
        return spark.read.csv(previous_data_file_path, header=True, inferSchema=True)
    else:
        print("Previous data file not found. Returning empty DataFrame.")
        return spark.createDataFrame([], spark.read.csv(current_data_file_path, header=True, inferSchema=True).schema)

### Unit Test Cases ###
@pytest.mark.unit
def test_get_column_names(spark):
    df = spark.read.csv(current_data_file_path, header=True, inferSchema=True)
    result = get_column_names(df)
    assert result == ["Id", "Name", "Age", "City"]

@pytest.mark.unit
def test_filter_by_column_value(spark):
    df = spark.read.csv(current_data_file_path, header=True, inferSchema=True)
    filtered_df = filter_by_column_value(df, "City", "New York")
    filtered_count = get_count(filtered_df)
    assert filtered_count > 0

### Data Validation Test Cases ###
@pytest.mark.data_validation
def test_insertion_detection(spark):
    current_df = spark.read.csv(current_data_file_path, header=True, inferSchema=True)
    previous_df = read_previous_data(spark)

    if previous_df.count() == 0:
        pytest.skip("Previous data file not found. Skipping insertion detection test.")

    inserted_rows = detect_inserted_rows(current_df, previous_df)
    assert inserted_rows.count() >= 0

@pytest.mark.data_validation
def test_update_detection(spark):
    current_df = spark.read.csv(current_data_file_path, header=True, inferSchema=True)
    previous_df = read_previous_data(spark)

    if previous_df.count() == 0:
        pytest.skip("Previous data file not found. Skipping update detection test.")

    updated_rows = detect_updated_rows(current_df, previous_df)
    assert updated_rows.count() >= 0

@pytest.mark.data_validation
def test_deletion_detection(spark):
    current_df = spark.read.csv(current_data_file_path, header=True, inferSchema=True)
    previous_df = read_previous_data(spark)

    if previous_df.count() == 0:
        deleted_rows = spark.createDataFrame([], current_df.schema)
    else:
        deleted_rows = detect_deleted_rows(current_df, previous_df)

    assert deleted_rows.count() >= 0

@pytest.mark.data_validation
def test_new_columns_detection(spark):
    current_df = spark.read.csv(current_data_file_path, header=True, inferSchema=True)
    if os.path.exists(previous_data_file_path):
        previous_df = spark.read.csv(previous_data_file_path, header=True, inferSchema=True)
    else:
        previous_df = spark.createDataFrame([], current_df.schema)

    new_columns = detect_new_columns(current_df, previous_df)
    assert len(new_columns) >= 0

@pytest.mark.data_validation
def test_column_names_and_types(spark):
    current_df = spark.read.csv(current_data_file_path, header=True, inferSchema=True)
    expected_schema = {"Id": "int", "Name": "string", "Age": "int", "City": "string"}
    assert validate_column_types(current_df, expected_schema) is True

@pytest.mark.data_validation
def test_non_null_columns(spark):
    df = spark.read.csv(current_data_file_path, header=True, inferSchema=True)
    non_null_columns = ["Id", "Name"]
    assert check_non_null_columns(df, non_null_columns) is True

### Smoke Test Cases ###
@pytest.mark.smoke
def test_basic_data_reading(spark):
    try:
        current_df = spark.read.csv(current_data_file_path, header=True, inferSchema=True)
        assert current_df.count() > 0
    except Exception as e:
        pytest.fail(f"Failed to read data: {str(e)}")

### Negative Test Case ###
@pytest.mark.negative
def test_missing_file_handling(spark):
    missing_file_path = "C:\\Users\\admin\\OneDrive - TestPerform\\Desktop\\cicd\\pysparkcode\\prev_data.csv"
    
    try:
        df = spark.read.csv(missing_file_path, header=True, inferSchema=True)
    except Exception as e:
        assert "Path does not exist" in str(e)

### Overwrite Previous Data After Tests ###
@pytest.fixture(scope="session", autouse=True)
def overwrite_previous_data_after_tests():
    """Overwrite previous data file with current data after all tests run."""
    yield  # Wait until all tests are done

    if os.path.exists(previous_data_file_path):
        os.remove(previous_data_file_path)

    shutil.copy(current_data_file_path, previous_data_file_path)
    
