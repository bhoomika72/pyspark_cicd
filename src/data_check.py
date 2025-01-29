from pyspark.sql import functions as F

def get_count(df):
    """
    Get the number of rows in the DataFrame.
    """
    return df.count()

def get_column_names(df):
    """
    Get the column names from the DataFrame.
    """
    return df.columns

def filter_by_column_value(df, column_name, value):
    """
    Filter the DataFrame by the specified column value.
    """
    return df.filter(df[column_name] == value)

def detect_inserted_rows(current_df, previous_df, key_column="Id"):
    """
    Detect rows that are in the current DataFrame but not in the previous one.
    """
    return current_df.join(previous_df, on=key_column, how="left_anti")

def detect_updated_rows(current_df, previous_df, key_column="Id"):
    """
    Detect rows that are updated (same Id but different column values).
    """
    updated_condition = None
    updated_rows = current_df.join(previous_df, on=key_column, how="inner")
    for col_name in current_df.columns:
        if col_name != key_column:
            condition = (current_df[col_name] != previous_df[col_name])
            if updated_condition is None:
                updated_condition = condition
            else:
                updated_condition = updated_condition | condition
    return updated_rows.filter(updated_condition)

def detect_deleted_rows(current_df, previous_df, key_column="Id"):
    """
    Detect rows that are in the previous DataFrame but not in the current one.
    """
    return previous_df.join(current_df, on=key_column, how="left_anti")

def validate_column_types(df, expected_schema):
    """
    Validate that the DataFrame columns match the expected schema.
    """
    actual_schema = {field.name: field.dataType.simpleString() for field in df.schema}
    for column, expected_type in expected_schema.items():
        actual_type = actual_schema.get(column)
        if actual_type != expected_type:
            raise ValueError(f"Column '{column}' type mismatch: expected '{expected_type}', found '{actual_type}'")
    return True

def detect_new_columns(current_df, previous_df):
    """
    Detect new columns in the current DataFrame compared to the previous one.
    """
    current_columns = set(current_df.columns)
    previous_columns = set(previous_df.columns)
    return current_columns - previous_columns

def check_non_null_columns(df, non_null_columns):
    """
    Check that specified columns do not contain any null values.
    """
    for column in non_null_columns:
        null_count = df.filter(df[column].isNull()).count()
        if null_count > 0:
            raise ValueError(f"Column '{column}' contains {null_count} null values.")
    return True
