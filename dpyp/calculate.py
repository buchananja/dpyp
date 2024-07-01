'''
the 'calculate' module contains functionality for doing statistical and
mathematical operations on data
'''


import logging
import pandas as pd
from typing import List


logger = logging.getLogger(__name__)


class SinCalc:
    '''contains calculations for applying iteratively to dataframe columns'''
    
    @staticmethod
    def single_addition(df_row: pd.Series, col1: str, col2: str) -> float:
        '''calculates the addition of two columns for single row'''

        return df_row[col1] + df_row[col2]

    @staticmethod
    def single_power(df_row: pd.Series, col: str, power: float) -> float:
        '''calculates the power of two columns for single row'''

        return df_row[col] ** power

    @staticmethod
    def single_subtraction(df_row: pd.Series, col1: str, col2: str) -> float:
        '''calculates the subtraction of two columns for single row'''

        return df_row[col1] - df_row[col2]

    @staticmethod
    def single_modulo(df_row: pd.Series, col1: str, col2: str) -> float:
        '''calculates the modulo between two columns for single row'''

        return df_row[col1] % df_row[col2]

    @staticmethod
    def single_percentage(
        df_row: pd.Series, 
        col1: str, 
        col2: str, 
        dec_points: int = 0
    ) -> float: 
        '''calculates the percentage between two columns for single row'''

        return round(df_row[col1] / df_row[col2] * 100, dec_points)

    @staticmethod
    def single_product(df_row: pd.Series, col1: str, col2: str) -> float:
        '''calculates the product of two columns for single row'''

        return df_row[col1] * df_row[col2]

    @staticmethod
    def single_difference(df_row: pd.Series, col1: str, col2: str) -> float:
        '''calculates the difference between two columns for single row'''

        return df_row[col1] - df_row[col2]

    @staticmethod
    def single_rate_of_change(
        df_row: pd.Series, 
        col1: str, 
        col2: str, 
        default_rate: float
    ) -> float:
        '''
        calculates the relative difference between two columns for single row 
        with default rate of change able to be specified
        '''
        
        if df_row[col1] > 0:
            return (df_row[col1] - df_row[col2]) / df_row[col1]
        else:
            return default_rate


class BulkCalc:
    '''contains calculations for applying in-bulk to dataframe columns'''
    
    @staticmethod
    def bulk_addition(
        df: pd.DataFrame, 
        col_name: str, 
        col1: str, 
        col2: str
    ) -> pd.DataFrame:
        '''calculated the subtraction of two columns and inserts into column'''

        df[col_name] = df[col1] + df[col2]
        return df

    @staticmethod
    def bulk_power(
        df: pd.DataFrame, 
        col_name: str,
        col: str, 
        power: float
    ) -> pd.DataFrame:
        '''calculated the power of two columns and inserts into column'''

        df[col_name] = df[col] ** power
        return df

    @staticmethod
    def bulk_subtraction(
        df: pd.DataFrame, 
        col_name: str, 
        col1: str, 
        col2: str
    ) -> pd.DataFrame:
        '''calculated the subtraction of two columns and inserts into column'''

        df[col_name] = df[col1] - df[col2]
        return df

    @staticmethod
    def bulk_modulo(
        df: pd.DataFrame, 
        col_name: str, 
        col1: str, 
        col2: str
    ) -> pd.DataFrame:
        '''calculated the modulo between two columns and inserts into column'''

        df[col_name] = df[col1] % df[col2]
        return df

    @staticmethod
    def bulk_percentage(
        df: pd.DataFrame, 
        col_name: str, 
        col1: str, 
        col2: str,
        dec_points: int = 0
    ) -> pd.DataFrame:
        '''calculated the percentage between two columns and inserts column'''

        df[col_name] = round(df[col1] / df[col2] * 100, dec_points)
        return df

    @staticmethod
    def bulk_product(
        df: pd.DataFrame, 
        col_name: str,
        col1: str, 
        col2: str,
    ) -> pd.DataFrame:
        '''calculates the product of two columns and inserts into column'''

        df[col_name] = df[col1] * df[col2] 
        return df

    @staticmethod
    def bulk_difference(
        df: pd.DataFrame, 
        col_name: str, 
        col1: str, 
        col2: str
    ) -> pd.DataFrame:
        '''calculates the difference between two columns and inserts column'''

        df[col_name] = df[col1] - df[col2] 
        return df

    @staticmethod
    def bulk_rate_of_change(
        df: pd.DataFrame, 
        col_name: str, 
        col1: str, 
        col2: str, 
        default_rate: float
    ) -> pd.DataFrame:
        '''calculates and inserts a rate of change column'''

        df[col_name] = (df[col1] - df[col2]) / df[col1]
        df[col_name] = df[col_name].fillna(default_rate)
        return df
    

class RankCalc:
    '''contains functionality for calculating ranks from numeric values'''
    
    @staticmethod
    def calc_rank_mid(
            df: pd.DataFrame, 
            current_col = 'rank', 
            output_col = 'rank'
        ):
        '''calculates numeric rank using range midpoint'''
        
        df[current_col] = df[current_col].astype(str).str.strip('+= ')
        df[['top', 'bottom']] = df[current_col].str.split('-', expand = True)
        df[['top', 'bottom']] = df[['top', 'bottom']].apply(pd.to_numeric)
        df['bottom'] = df['bottom'].fillna(df['top'])
        df[output_col] = ((df['top'] + df['bottom']) / 2).round(0)
        df = df.drop(columns = ['top', 'bottom'])
        return df

    @staticmethod
    def rank_cols(
            df: pd.DataFrame,
            rank_method: str,
            rank_ascending: bool,
            col_exclude: List,
            output_suffix: str = '_rank'
        ):
        '''calculates ranks for all columns except excluded'''
        
        valid_methods = ['average', 'min', 'max', 'first', 'dense']
        if rank_method not in valid_methods:
            logger.error('Invalid rank_method; Choose: "average", "min", "max", "first", "dense"')
            return
        if not isinstance(rank_ascending, bool):
            logger.error('Invalid rank_ascending; Choose "True" or "False"')
            return

        for col in df.columns:
            if col in col_exclude:
                continue
            else:
                df[f'{col}{output_suffix}'] = (df[col]
                    .fillna(0)
                    .rank(method = 'min', ascending = rank_ascending)
                    .astype(int)
                )
        return df