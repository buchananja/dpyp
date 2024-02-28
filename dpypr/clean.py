import pandas as pd


'''
the 'clean' module contains functionality for cleaning, formatting, and
standardising dataframes
'''


def headers_to_snakecase(df, uppercase = False):
    '''
    converts all column headers to lower snake case by default and uppercase 
    if specified
    '''
    
    if uppercase:
        df.columns = (df.columns.str.upper().str.replace(' ', '_'))
    else:
        df.columns = (df.columns.str.lower().str.replace(' ', '_'))


def columns_to_snakecase(df, uppercase = False):
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


def columns_to_lowercase(df):
    '''converts all string values in dataframe to lowercase'''
    
    string_cols = df.select_dtypes(include = ['object', 'string']).columns
    for col in string_cols:
        df[col] = df[col].apply(
            lambda val: val.lower() 
            if isinstance(val, str) 
            else val
        )


def columns_to_uppercase(df):
    '''converts all string values in dataframe to uppercase'''
    
    string_cols = df.select_dtypes(include = ['object', 'string']).columns
    for col in string_cols:
        df[col] = df[col].apply(
            lambda val: val.upper()
            if isinstance(val, str) 
            else val
        )


def columns_strip_whitespace(df):
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


def columns_optimise_numerics(df):
    '''downcasts numeric datatypes in numeric columns of dataframe''' 
    
    for col in df.columns:
        # skips object columns
        if df[col].dtype == 'object':
            continue
        # checks whether column contains only intigers
        else:
            if all(df[col] % 1 == 0):
                df[col] = pd.to_numeric(df[col], downcast = 'integer')
            else:
                df[col] = pd.to_numeric(df[col], downcast = 'float')


def columns_to_string(df, clean_columns = []):
    '''converts all columns in clean_columns to string'''

    # finds columns common to both df.columns and clean_columns
    clean_columns = list(set(clean_columns) & set(df.columns))
    for col in clean_columns:
        df[col] = df[col].astype('string')


def columns_to_categorical(df, clean_columns = []):
    '''converts all columns in clean_columns to categories'''

    # finds columns common to both df.columns and clean_columns
    clean_columns = list(set(clean_columns) & set(df.columns))
    for col in clean_columns:
        df[col] = df[col].astype('category')


def columns_to_datetime(df, clean_columns = []):
    '''converts all columns in clean_columns to datetime'''
    
    # finds columns common to both df.columns and clean_columns
    clean_columns = list(set(clean_columns) & set(df.columns))
    
    # converts columns to datetime
    for col in clean_columns:
        df.loc[:, col] = pd.to_datetime(df.loc[:, col])


def columns_to_boolean(df, clean_columns = []):
    '''converts all columns in clean_columns to True if "true", else False'''
    
    clean_columns = list(set(clean_columns) & set(df.columns))
    
    # converts string values to True if "true", else False
    for col in clean_columns:
            df[col] = df[col].apply(
                lambda val: True 
                if isinstance(val, str) 
                and val.lower() == 'true' 
                else False
            )