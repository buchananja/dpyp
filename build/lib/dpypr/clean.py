# Dependencies ################################################################
import pandas as pd
import os
import time
import logging
import sqlite3
import pyarrow.feather as feather
import openpyxl


# Data Cleaning ###############################################################
def headers_to_snakecase(df, uppercase = False):
    r'''
    Converts all column headers to lower snake case by defatul and uppercase if
    'uppercase' argument is True.
    '''
    if uppercase:
        df.columns = (df.columns.str.upper().str.replace(' ', '_'))
    else:
        df.columns = (df.columns.str.lower().str.replace(' ', '_'))
    return df

def values_to_lowercase(df):
    r'''
    Converts all string values in dataframe to lowercase.
    '''
    df = df.apply(lambda x: x.str.lower() if x.dtype == "object" else x)
    return df

def values_to_uppercase(df):
    r'''
    Converts all string values in dataframe to uppercase.
    '''
    df = df.apply(lambda x: x.str.upper() if x.dtype == "object" else x)
    return df

def values_strip_whitespace(df):
    r'''
    Converts all string values to lowercase.
    '''
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    return df

def optimise_numeric_datatypes(df):
    r'''
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
