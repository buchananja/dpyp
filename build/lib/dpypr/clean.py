'''
The 'clean' module contains functionality for cleaning, formatting, and
standardising dataframes.
'''

# Dependencies ################################################################
import pandas as pd

# Data Cleaning ###############################################################

def headers_to_snakecase(self, uppercase = False):
    '''
    Converts all column headers to lower snake case by defatul and uppercase 
    if 'uppercase' argument is True.
    '''
    if uppercase:
        self.columns = (self.columns.str.upper().str.replace(' ', '_'))
    else:
        self.columns = (self.columns.str.lower().str.replace(' ', '_'))
    return self

def values_to_snakecase(self, uppercase = False):
    r'''
    Converts all string values in dataframe to lower snake case by default 
    and uppercase if 'uppercase' argument is True.
    '''
    if uppercase:
        self = self.apply(
            lambda col: col.str.upper().str.replace(' ', '_') 
            if col.dtype == "object" else col
        )
    else:
        self = self.apply(
            lambda col: col.str.lower().str.replace(' ', '_') 
            if col.dtype == "object" else col   
        )
    return self

def values_to_lowercase(self):
    '''
    Converts all string values in dataframe to lowercase.
    '''
    self = self.apply(
        lambda col: col.str.lower() if col.dtype == "object" else col
    )
    return self

def values_to_uppercase(self):
    '''
    Converts all string values in dataframe to uppercase.
    '''
    self = self.apply(
        lambda col: col.str.upper() if col.dtype == "object" else col
    )
    return self

def values_strip_whitespace(self):
    '''
    Converts all string values to lowercase.
    '''
    self = self.apply(
        lambda col: col.str.strip() if col.dtype == "object" else col
    )
    return self

def optimise_numeric_datatypes(self):
    '''
    Optimises the data types in a pandas DataFrame by attempting to convert
    strings to numerical data where possible and to the smallest possible 
    integer datatype.
    ''' 
    for col in self.columns:
        if self[col].dtype == object:
            pass
        else:
            if all(self[col] % 1 == 0):
                self[col] = pd.to_numeric(self[col], downcast = 'integer')
            else:
                self[col] = pd.to_numeric(self[col], downcast = 'float')
    return self

def values_to_string(self, clean_columns = [], all = False):
    '''
    - Converts all columns in clean_columns to strings.
    - If all is True, converts all values to lowercase.
    '''
    if all:
        clean_columns = self.columns.to_list()
    self.loc[:, clean_columns] = self.loc[:, clean_columns].astype(str)
    return self

# def columns_to_datetime(self, date_key = 'date', date_format = '%Y-%m-%d'):
#     '''
#     Converts columns containing 'date' to datetime datatype.
#     '''
#     date_columns = [col for col in self.columns if date_key in col]
#     for col in date_columns:
#         self[col] = pd.to_datetime(self[col], format = date_format)
#     return self