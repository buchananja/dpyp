'''
The 'clean' module contains functionality for cleaning, formatting, and
standardising dataframes.
'''

# Dependencies ################################################################
import pandas as pd

# Data Cleaning ###############################################################

def headers_to_snakecase(df, uppercase = False):
    '''
    Converts all column headers to lower snake case by defatul and uppercase 
    if 'uppercase' argument is True.
    '''
    if uppercase:
        df.columns = (df.columns.str.upper().str.replace(' ', '_'))
    else:
        df.columns = (df.columns.str.lower().str.replace(' ', '_'))
    return df

def values_to_snakecase(df, uppercase = False):
    '''
    Converts all string values in dataframe to lower snake case by default 
    and uppercase if 'uppercase' argument is True.
    '''
    if uppercase:
        df = df.apply(
            lambda col: col.str.upper().str.replace(' ', '_') 
            if col.dtype == "object" else col
        )
    else:
        df = df.apply(
            lambda col: col.str.lower().str.replace(' ', '_') 
            if col.dtype == "object" else col   
        )
    return df

def values_to_lowercase(df):
    '''
    Converts all string values in dataframe to lowercase.
    '''
    df = df.apply(
        lambda col: col.str.lower() if col.dtype == "object" else col
    )
    return df

def values_to_uppercase(df):
    '''
    Converts all string values in dataframe to uppercase.
    '''
    df = df.apply(
        lambda col: col.str.upper() if col.dtype == "object" else col
    )
    return df

def values_strip_whitespace(df):
    '''
    Converts all string values to lowercase.
    '''
    df = df.apply(
        lambda col: col.str.strip() if col.dtype == "object" else col
    )
    return df

def optimise_numeric_datatypes(df):
    '''
    Optimises the data types in a pandas DataFrame by attempting to convert
    strings to numerical data where possible and to the smallest possible 
    integer datatype.
    ''' 
    for col in df.columns:
        if df[col].dtype == object:
            pass
        else:
            if all(df[col] % 1 == 0):
                df[col] = pd.to_numeric(df[col], downcast = 'integer')
            else:
                df[col] = pd.to_numeric(df[col], downcast = 'float')
    return df

def values_to_string(df, clean_columns = [], all = False):
    '''
    - Converts all columns in clean_columns to strings.
    - If all is True, converts all values to lowercase.
    '''
    if all:
        clean_columns = df.columns.to_list()
    df.loc[:, clean_columns] = df.loc[:, clean_columns].astype(str)
    return df

# def columns_to_datetime(df, date_key = 'date', date_format = '%Y-%m-%d'):
#     '''
#     Converts columns containing 'date' to datetime datatype.
#     '''
#     date_columns = [col for col in df.columns if date_key in col]
#     for col in date_columns:
#         df[col] = pd.to_datetime(df[col], format = date_format)
#     return df