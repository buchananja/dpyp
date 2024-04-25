'''
the 'calculate' module contains functionality for doing statistical and
mathematical operations on data
'''


import logging
logger = logging.getLogger(__name__)


def row_rate_of_change(df_row, col1, col2, default_rate):
    '''
    calculates the relative difference between two columns for single row with 
    default rate of change able to be specified
    '''
    
    if df_row[col1] > 0:
        result = (df_row[col1] - df_row[col2]) / df_row[col1]
    else:
        result = default_rate
    return result


def col_rate_of_change(df, col_name, col1, col2, default_rate):
    '''calculates and inserts a rate of change column'''

    df[col_name] = (df[col1] - df[col2]) / df[col1]
    df[col_name] = df[col_name].fillna(default_rate)
    return df


def row_difference(df_row, col1, col2):
    '''calculates the difference between two columns for single row'''

    return df_row[col1] - df_row[col2]


def col_difference(df, col_name, col1, col2):
    '''calculates the difference between two columns and inserts into column'''

    df[col_name] = df[col1] - df[col2] 
    return df


def row_product(df_row, col1, col2):
    '''calculates the product of two columns for single row'''

    return df_row[col1] * df_row[col2]


def col_product(df, col_name, col1, col2):
    '''calculates the product of two columns and inserts into column'''

    df[col_name] = df[col1] * df[col2] 
    return df


def row_percentage(df_row, col1, col2, dec_points = 0):
    '''calculates the percentage between two columns for single row'''

    return round(df_row[col1] / df_row[col2] * 100, dec_points)


def col_percentage(df, col_name, col1, col2, dec_points = 0):
    '''calculated the percentage between two columns and inserts into column'''

    df[col_name] = round(df[col1] / df[col2] * 100, dec_points)
    return df


def row_modulo(df_row, col1, col2):
    '''calculates the modulo between two columns for single row'''

    return df_row[col1] % df_row[col2]


def col_modulo(df, col_name, col1, col2):
    '''calculated the modulo between two columns and inserts into column'''

    df[col_name] = df[col1] % df[col2]
    return df


def row_subtraction(df_row, col1, col2):
    '''calculates the subtraction of two columns for single row'''

    return df_row[col1] - df_row[col2]


def col_subtraction(df, col_name, col1, col2):
    '''calculated the subtraction of two columns and inserts into column'''

    df[col_name] = df[col1] - df[col2]
    return df


def row_addition(df_row, col1, col2):
    '''calculates the addition of two columns for single row'''

    return df_row[col1] + df_row[col2]


def col_addition(df, col_name, col1, col2):
    '''calculated the subtraction of two columns and inserts into column'''

    df[col_name] = df[col1] + df[col2]
    return df