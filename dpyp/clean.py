'''
the 'clean' module contains functionality for cleaning, formatting, and
standardising dataframes
'''


import logging
import pandas as pd


logger = logging.getLogger(__name__)


class HeadClean:
    '''contains functionality for cleaning pandas dataframe headers'''
    
    
    @staticmethod
    def headers_rename(df: pd.DataFrame, rename_dict: dict) -> pd.DataFrame:
        '''renames dataframe columns using a dictionary'''
        
        # checks if columns exist in dict and df
        columns_to_rename = list(set(rename_dict.keys()) & set(df.columns))
        valid_rename_dict = {col: rename_dict[col] for col in columns_to_rename}
        df = df.rename(columns = valid_rename_dict)
        return df


    @staticmethod
    def headers_to_snakecase(
        df: pd.DataFrame, 
        uppercase: bool = False
    ) -> pd.DataFrame:
        '''
        converts all column headers to lower snake case by default and uppercase 
        if specified
        '''
        
        if uppercase:
            df.columns = (df.columns.str.upper().str.replace(' ', '_'))
        else:
            df.columns = (df.columns.str.lower().str.replace(' ', '_'))
        return df


class ColClean:
    '''contains functionality for cleaning pandas dataframe column data'''
    
    
    @staticmethod
    def columns_to_snakecase(
        df: pd.DataFrame, 
        uppercase: bool = False
    ) -> pd.DataFrame:
        '''
        converts all string values in dataframe to lower snake case by default 
        and uppercase if specified
        '''
        
        string_cols = df.select_dtypes(include = ['object', 'string']).columns
        if uppercase:
            for col in string_cols:
                df[col] = df[col].apply(
                    lambda val: val.upper().replace(' ', '_') 
                    if isinstance(val, str) 
                    else val
                )
        else:
            for col in string_cols:
                df[col] = df[col].apply(
                    lambda val: val.lower().replace(' ', '_') 
                    if isinstance(val, str) 
                    else val
                )
        return df


    @staticmethod
    def columns_to_lowercase(df: pd.DataFrame) -> pd.DataFrame:
        '''converts all string values in dataframe to lowercase'''
        
        string_cols = df.select_dtypes(include = ['object', 'string']).columns
        for col in string_cols:
            df[col] = df[col].apply(
                lambda val: val.lower() 
                if isinstance(val, str) 
                else val
            )
        return df


    @staticmethod
    def columns_to_uppercase(df: pd.DataFrame) -> pd.DataFrame:
        '''converts all string values in dataframe to uppercase'''
        
        string_cols = df.select_dtypes(include = ['object', 'string']).columns
        for col in string_cols:
            df[col] = df[col].apply(
                lambda val: val.upper()
                if isinstance(val, str) 
                else val
            )
        return df


    @staticmethod
    def columns_strip_whitespace(df: pd.DataFrame) -> pd.DataFrame:
        '''
        strips all leading and trailing whitespace from string dtypes in object and 
        string columns
        '''
    
        string_cols = df.select_dtypes(include = ['object', 'string']).columns
        for col in string_cols:
            df[col] = df[col].apply(
                lambda val: val.strip() 
                if isinstance(val, str) 
                else val
            )
        return df


    @staticmethod
    def columns_optimise_numerics(df: pd.DataFrame) -> pd.DataFrame:
        '''downcasts numeric datatypes in numeric columns of dataframe''' 
        
        for col in df.columns:
            if df[col].dtype != 'object':
                if all(df[col] % 1 == 0):
                    df[col] = pd.to_numeric(df[col], downcast = 'integer')
                else:
                    df[col] = pd.to_numeric(df[col], downcast = 'float')
        return df


    @staticmethod
    def columns_to_float(
        df: pd.DataFrame, 
        clean_columns: list
    ) -> pd.DataFrame:
        '''converts all columns in clean_columns to float'''

        # finds columns common to both df.columns and clean_columns
        clean_columns = list(set(clean_columns) & set(df.columns))
        for col in clean_columns:
            df[col] = df[col].astype(float)
        return df


    @staticmethod
    def columns_to_object(
        df: pd.DataFrame, 
        clean_columns: list
    ) -> pd.DataFrame:
        '''converts all columns in clean_columns to object'''

        # finds columns common to both df.columns and clean_columns
        clean_columns = list(set(clean_columns) & set(df.columns))
        for col in clean_columns:
            df[col] = df[col].astype('object')
        return df


    @staticmethod
    def columns_to_string(
        df: pd.DataFrame, 
        clean_columns: list
    ) -> pd.DataFrame:
        '''
        converts all columns in clean_columns to str objects 
        (not pandas string)
        '''

        # finds columns common to both df.columns and clean_columns
        clean_columns = list(set(clean_columns) & set(df.columns))
        for col in clean_columns:
            df[col] = df[col].astype('str')
        return df


    @staticmethod
    def columns_to_categorical(
        df: pd.DataFrame, 
        clean_columns: list
    ) -> pd.DataFrame:
        '''converts all columns in clean_columns to categories'''

        # finds columns common to both df.columns and clean_columns
        clean_columns = list(set(clean_columns) & set(df.columns))
        for col in clean_columns:
            df[col] = df[col].astype('category')
        return df


    @staticmethod
    def columns_to_datetime(
        df: pd.DataFrame, 
        clean_columns: list
    ) -> pd.DataFrame:
        '''converts all columns in clean_columns to datetime'''
        
        # finds columns common to both df.columns and clean_columns; converts 
        clean_columns = list(set(clean_columns) & set(df.columns))
        for col in clean_columns:
            df.loc[:, col] = pd.to_datetime(df.loc[:, col])
        return df


    @staticmethod
    def columns_to_boolean(
        df: pd.DataFrame, 
        clean_columns: list
    ) -> pd.DataFrame:
        '''converts all columns in clean_columns to True if "true", else False'''
        
        # finds columns common to both df.columns and clean_columns; converts 
        clean_columns = list(set(clean_columns) & set(df.columns))s
        for col in clean_columns:
            df[col] = df[col].apply(
                lambda val: True 
                if isinstance(val, str) 
                and val.lower() == 'true' 
                else False
            )
        return df


    @staticmethod
    def columns_fill_null(
        df: pd.DataFrame, 
        fill_word: str = 'unknown'
    ) -> pd.DataFrame:
        '''fills nulls within non-numeric columns with optional keyword'''

        fill_cols = df.select_dtypes(include = ['object', 'string', 'category']).columns
        for col in fill_cols:
            df[col] = df[col].fillna(fill_word)
        return df