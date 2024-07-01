'''
the 'transform' module contains functionality for transforming data into
alternative forms for standardisation and statistical analysis
'''


import numpy as np
import pandas as pd


class Norm:
    '''contains funcitonality for normalising columns in dataframes'''

    @staticmethod
    def min_max(df: pd.DataFrame, col: str) -> pd.Series:
        '''
        scales column values between 0 (min) and 1 (max), returns pandas series
        '''

        return (df[col] - df[col].min()) / (df[col].max() - df[col].min())

    @staticmethod
    def z_score(df: pd.DataFrame, col: str) -> pd.Series:
        '''
        scales column values between mean and standard deviation, returns 
        pandas series
        '''

        return (df[col] - df[col].mean()) / df[col].std()
    
    @staticmethod
    def sqrt(df: pd.DataFrame, col: str) -> pd.Series:
        '''square-root transforms column values, returns pandas series'''

        return np.sqrt(df[col])
    
    @staticmethod
    def log(df: pd.DataFrame, col: str, base: float = np.e) -> pd.Series:
        '''
        log transforms column values with variable base, returns pandas series
        '''

        try:
            if base == np.e:
                return np.log(df[col])
            elif base == 10:
                return np.log10(df[col])
            else:
                np.log(df[col]) / np.log(base)
        except ValueError:
            print('Column must be only positive numbers.')