'''
the 'clean' module contains functionality for cleaning, formatting, and
standardising dataframes
'''


import pandas as pd


def headers_to_snakecase(df, uppercase = False):
    '''
    converts all column headers to lower snake case by default and uppercase 
    if specified
    '''
    
    if uppercase:
        df.columns = (df.columns.str.upper().str.replace(' ', '_'))
    else:
        df.columns = (df.columns.str.lower().str.replace(' ', '_'))
    return df


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
    return df


def columns_to_lowercase(df):
    '''converts all string values in dataframe to lowercase'''
    
    string_cols = df.select_dtypes(include = ['object', 'string']).columns
    for col in string_cols:
        df[col] = df[col].apply(
            lambda val: val.lower() 
            if isinstance(val, str) 
            else val
        )
    return df


def columns_to_uppercase(df):
    '''converts all string values in dataframe to uppercase'''
    
    string_cols = df.select_dtypes(include = ['object', 'string']).columns
    for col in string_cols:
        df[col] = df[col].apply(
            lambda val: val.upper()
            if isinstance(val, str) 
            else val
        )
    return df


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
    return df


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
    return df


def columns_to_object(df, clean_columns = []):
    '''converts all columns in clean_columns to object'''

    # finds columns common to both df.columns and clean_columns
    clean_columns = list(set(clean_columns) & set(df.columns))
    for col in clean_columns:
        df[col] = df[col].astype('object')
    return df


def columns_to_string(df, clean_columns = []):
    '''
    converts all columns in clean_columns to str objects 
    (not pandas string)
    '''

    # finds columns common to both df.columns and clean_columns
    clean_columns = list(set(clean_columns) & set(df.columns))
    for col in clean_columns:
        df[col] = df[col].astype('str')
    return df


def columns_to_categorical(df, clean_columns = []):
    '''converts all columns in clean_columns to categories'''

    # finds columns common to both df.columns and clean_columns
    clean_columns = list(set(clean_columns) & set(df.columns))
    for col in clean_columns:
        df[col] = df[col].astype('category')
    return df


def columns_to_datetime(df, clean_columns = []):
    '''converts all columns in clean_columns to datetime'''
    
    # finds columns common to both df.columns and clean_columns
    clean_columns = list(set(clean_columns) & set(df.columns))
    
    # converts columns to datetime
    for col in clean_columns:
        df.loc[:, col] = pd.to_datetime(df.loc[:, col])
    return df


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
    return df


def columns_fill_null(df, fill_word = 'unknown'):
    '''fills nulls within non-numeric columns with optional keyword'''

    fill_cols = df.select_dtypes(
        include = ['object', 'string', 'category']
    ).columns
    
    for col in fill_cols:
        df[col] = df[col].fillna(fill_word)
    return df


def headers_rename(df, rename_dict):
    '''renames dataframe columns using a dictionary'''
    
    # checks if columns to rename exist in dict and df
    columns_to_rename = list(set(rename_dict.keys()) & set(df.columns))
    # creates new dictionary of valid names
    valid_rename_dict = {k: rename_dict[k] for k in columns_to_rename}
    # renames columns according to new dict
    df = df.rename(columns = valid_rename_dict)
    return df