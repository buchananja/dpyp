'''
the 'calculate' module contains functionality for doing statistical and
mathematical operations on data
'''


import logging
logger = logging.getLogger(__name__)


class SingleCalc:
    '''contains calculations for applying iteratively to pandas dataframe columns'''
    
    
    def __init__(self, df_row):
        self.df_row = df_row
        
        
    def single_addition(df_row, col1, col2):
        '''calculates the addition of two columns for single row'''

        return df_row[col1] + df_row[col2]


    def single_power(df_row, col, power):
        '''calculates the power of two columns for single row'''

        return df_row[col]**power


    def single_subtraction(df_row, col1, col2):
        '''calculates the subtraction of two columns for single row'''

        return df_row[col1] - df_row[col2]


    def single_modulo(df_row, col1, col2):
        '''calculates the modulo between two columns for single row'''

        return df_row[col1] % df_row[col2]


    def single_percentage(df_row, col1, col2, dec_points = 0):
        '''calculates the percentage between two columns for single row'''

        return round(df_row[col1] / df_row[col2] * 100, dec_points)


    def single_product(df_row, col1, col2):
        '''calculates the product of two columns for single row'''

        return df_row[col1] * df_row[col2]


    def single_difference(df_row, col1, col2):
        '''calculates the difference between two columns for single row'''

        return df_row[col1] - df_row[col2]


    def single_rate_of_change(df_row, col1, col2, default_rate):
        '''
        calculates the relative difference between two columns for single row with 
        default rate of change able to be specified
        '''
        
        if df_row[col1] > 0:
            result = (df_row[col1] - df_row[col2]) / df_row[col1]
        else:
            result = default_rate
        return result


class BulkCalc:
    '''contains calculations for applying in-bulk to pandas dataframe columns'''
    
    
    def __init__(self, df_row):
        self.df_row = df_row
        
    
    def bulk_addition(df, col_name, col1, col2):
        '''calculated the subtraction of two columns and inserts into column'''

        df[col_name] = df[col1] + df[col2]
        return df


    def bulk_power(df, col_name, col, power):
        '''calculated the subtraction of two columns and inserts into column'''

        df[col_name] = df[col]**power
        return df


    def bulk_subtraction(df, col_name, col1, col2):
        '''calculated the subtraction of two columns and inserts into column'''

        df[col_name] = df[col1] - df[col2]
        return df


    def bulk_modulo(df, col_name, col1, col2):
        '''calculated the modulo between two columns and inserts into column'''

        df[col_name] = df[col1] % df[col2]
        return df


    def bulk_percentage(df, col_name, col1, col2, dec_points = 0):
        '''calculated the percentage between two columns and inserts into column'''

        df[col_name] = round(df[col1] / df[col2] * 100, dec_points)
        return df


    def bulk_product(df, col_name, col1, col2):
        '''calculates the product of two columns and inserts into column'''

        df[col_name] = df[col1] * df[col2] 
        return df


    def bulk_difference(df, col_name, col1, col2):
        '''calculates the difference between two columns and inserts into column'''

        df[col_name] = df[col1] - df[col2] 
        return df


    def bulk_rate_of_change(df, col_name, col1, col2, default_rate):
        '''calculates and inserts a rate of change column'''

        df[col_name] = (df[col1] - df[col2]) / df[col1]
        df[col_name] = df[col_name].fillna(default_rate)
        return df