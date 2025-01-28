def get_count(df):
    """
    Returns the count of rows in the given DataFrame.
    """
    return df.count()

def get_column_names(df):
    """
    Returns a list of column names in the given DataFrame.
    """
    return df.columns

def filter_by_column_value(df, column_name, value):
    """
    Filters the DataFrame where the specified column matches the given value.
    """
    return df.filter(df[column_name] == value)
