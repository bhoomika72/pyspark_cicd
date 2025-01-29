import os
import pytest
from pyspark.sql import SparkSession

# Set environment variables for PySpark
os.environ['SPARK_HOME'] = 'C:\\spark-3.5.4-bin-hadoop3'
os.environ['PYSPARK_PYTHON'] = 'C:\\Users\\admin\\AppData\\Local\\Programs\\Python\\Python39\\python.exe'

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("PySpark Testing") \
        .master("local[*]") \
        .getOrCreate()
