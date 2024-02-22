import pandas as pd


'''
the 'clean' module contains functionality for cleaning, formatting, and
standardising dataframes
'''


def headers_to_snakecase(df, uppercase = False):
    '''
    converts all column headers to lower snake case by default and uppercase 
    if 'uppercase' if specified
    '''
    
    if uppercase:
        df.columns = (df.columns.str.upper().str.replace(' ', '_'))
    else:
        df.columns = (df.columns.str.lower().str.replace(' ', '_'))
    return df


def columns_to_snakecase(df, uppercase = False):
    '''converts all string values in dataframe to lower snake case by default 
    and uppercase if specified.
    '''
    
    if uppercase:
        df = df.apply(
            lambda col: col.str.upper().str.replace(' ', '_') 
            if col.dtype == "object" else col
        )
    else:
        df = df.apply(
            lambda col: col.str.lower().str.replace(' ', '_') 
            if col.dtype == "object" else col   
        )
    return df


def columns_to_lowercase(df):
    '''converts all string values in dataframe to lowercase'''
    
    df = df.apply(
        lambda col: col.str.lower() if col.dtype == "object" else col
    )
    return df


def columns_to_uppercase(df):
    '''converts all string values in dataframe to uppercase'''
    
    df = df.apply(
        lambda col: col.str.upper() if col.dtype == "object" else col
    )
    return df


def columns_strip_whitespace(df):
    '''converts all string values to lowercase'''
    
    df = df.apply(
        lambda col: col.str.strip() if col.dtype == "object" else col
    )
    return df


def columns_optimise_numerics(df):
    '''downcasts numeric datatypes in numeric columns of dataframe''' 
    
    for col in df.columns:
        # skips object columns
        if df[col].dtype == object:
            pass
        # checks whether column contains only intigers
        else:
            if all(df[col] % 1 == 0):
                df[col] = pd.to_numeric(df[col], downcast = 'integer')
            else:
                df[col] = pd.to_numeric(df[col], downcast = 'float')
    return df


def columns_to_string(df, clean_columns = []):
    '''converts all columns in clean_columns to strings'''

    # finds columns common to both df.columns and clean_columns
    clean_columns = list(set(clean_columns) & set(df.columns))
    df.loc[:, clean_columns] = df.loc[:, clean_columns].astype(str)
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