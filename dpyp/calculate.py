'''
the 'calculate' module contains functionality for doing statistical and
mathematical operations on data
'''


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


def col_rate_of_change(df, col1, col2, default_rate):
    '''calculates and inserts a rate of change column'''

    result = (df[col1] - df[col2]) / df[col1]
    result = result.fillna(default_rate)


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


def row_percentage(df_row, col1, col2):
    '''calculates the percentage between two columns for single row'''

    return df_row[col1] / df_row[col2] * 100


def col_percentage(df, col_name, col1, col2):
    '''calculated the percentage between two columns and inserts into column'''

    df[col_name] = df[col1] / df[col2] * 100
    return df


def row_modulo(df_row, col1, col2):
    '''calculates the modulo between two columns for single row'''

    return df_row[col1] % df_row[col2]


def col_modulo(df, col_name, col1, col2):
    '''calculated the modulo between two columns and inserts into column'''

    df[col_name] = df[col1] % df[col2]
    return df