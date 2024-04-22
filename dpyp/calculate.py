'''
the 'calculate' module contains functionality for doing statistical and
mathematical operations on data
'''


def rate_of_change(df_row, col1, col2, default_rate):
    '''
    calculates the relative difference between two columns by row with default 
    rate able to be specified; typically used with an apply function
    '''
    
    if df_row[col1] > 0:
        result = (df_row[col1] - df_row[col2]) / df_row[col1]
    else:
        result = default_rate
    return result