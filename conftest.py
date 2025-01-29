import os
import pytest
from pyspark.sql import SparkSession

# Set environment variables for PySpark
os.environ['SPARK_HOME'] = 'C:\\spark-3.5.4-bin-hadoop3'
os.environ['PYSPARK_PYTHON'] = 'C:\\Users\\admin\\AppData\\Local\\Programs\\Python\\Python39\\python.exe'
os.environ['HADOOP_HOME']= 'C:\\hadoop-3.3.6-src'
@pytest.fixture(scope="session")
def spark():
    # Create the Spark session
    spark_session = SparkSession.builder \
        .appName("PySpark Testing") \
        .master("local[*]") \
        .getOrCreate()

    # Set the log level to ERROR to suppress warnings
    spark_session.sparkContext.setLogLevel("ERROR")
    
    # Yield the Spark session to be used in tests
    yield spark_session
    
    # Optionally, stop the Spark session after the tests are done
    spark_session.stop()
