'''this file tests whether the 'dpypr.clean' module is working correctly'''


import pytest
import pandas as pd
import dpypr as dp


@pytest.fixture
def df_sample():
    '''basic example of 'student number planning'-style data'''
    
    df = {
        'Student Number': [
            123456,
            123457,
            123458,
            123459,
            123460,
            123461
        ],
        'Programme School': [
            'school_of_biological_sciences',
            'school_of_natural_science',
            'school_of_engineering',
            'school_of_biological_sciences',
            'school_of_music',
            'school_of_medicine'
        ],
        'Fee Region': [
            'scot',
            'scot',
            'scot',
            'rUK',
            'international',
            'international'
        ],
        'Academic Year': [
            2024,
            2024,
            2024,
            2024,
            2024,
            2024
        ],
        'Fees': [
            123.99,
            400.50,
            234,
            1900.0133,
            5,
            10.000001
        ],
        'Snapshot Date': [
            '2024/02/01',
            '2024/02/01',
            '2024/02/01',
            '2024/02/01',
            '2024/02/01',
            '2024/02/01'
        ]
    }
    df = pd.DataFrame(df)
    return df
   
    
def test_headers_to_lower_snakecase_by_default(df_sample):
    '''
    tests whether column headers correctly set to lower snakecase when
    'uppercase' argument is default
    '''
    
    df = df_sample
    df = dp.headers_to_snakecase(df)
    assert all((col.islower() and ' ' not in col) for col in df.columns)
    
    
def test_headers_to_lower_snakecase_manually(df_sample):
    '''
    tests whether column headers correctly set to lower snakecase when
    'uppercase' argument is set to False manually
    '''
    
    df = df_sample
    df = dp.headers_to_snakecase(df, uppercase = False)
    assert all((col.islower() and ' ' not in col) for col in df.columns)


def test_headers_to_upper_snakecase(df_sample):
    '''tests whether column headers correctly set to upper snakecase'''
    
    df = df_sample
    df = dp.headers_to_snakecase(df, uppercase = True)
    assert all((col.isupper() and ' ' not in col) for col in df.columns)
    
    
def test_columns_to_lowercase(df_sample):
    '''tests whether dataframe columns correctly set to lowercase'''
    
    df = df_sample
    df = dp.columns_to_lowercase(df)
    
    # returns True if column is all lowercase or not object. Asserts if True
    # i.e assert will only fail if columns contains uppercase letters
    assert df.apply(
        lambda col: (col == col.str.lower()).all()
        if col.dtype == 'object'
        else True
    ).all()


def test_columns_to_uppercase(df_sample):
    '''tests whether dataframe columns correctly set to uppercase'''
    
    df = df_sample
    df = dp.columns_to_uppercase(df)
    
    # returns True if column is all uppercase or not object. Asserts if True
    # i.e assert will only fail if columns contains lowercase letters
    assert df.apply(
        lambda col: (col == col.str.upper()).all()
        if col.dtype == 'object'
        else True
    ).all()
    
    
def test_columns_to_lower_snakecase_by_default(df_sample):
    '''
    tests whether dataframe columns correctly set to lower snakecase by default
    '''
    
    df = df_sample
    df = dp.columns_to_snakecase(df)
    
    # returns True if column is all lowercase with spaces replaced by 
    # underscores or not object. Asserts if True
    # i.e assert will only fail if columns contains lowercase letters
    assert df.apply(
        lambda col: (col == col.str.lower().str.replace(' ', '_')).all()
        if col.dtype == 'object'
        else True
    ).all()
    
    
def test_columns_to_lower_snakecase_manually(df_sample):
    '''
    tests whether dataframe columns correctly set to lowersnakecase when
    'uppercase' argument is set to False manually
    '''
    
    df = df_sample
    df = dp.columns_to_snakecase(df, uppercase = False)
    
    # returns True if column is all lowercase with spaces replaced by 
    # underscores or not object. Asserts if True
    # i.e assert will only fail if columns contains lowercase letters
    assert df.apply(
        lambda col: (col == col.str.lower().str.replace(' ', '_')).all()
        if col.dtype == 'object'
        else True
    ).all()
    
    
def test_columns_to_upper_snakecase(df_sample):
    '''tests whether dataframe columns correctly set to upper snakecase'''
    
    df = df_sample
    df = dp.columns_to_snakecase(df, uppercase = True)
    
    # returns True if column is all uppercase with spaces replaced by 
    # underscores or not object. Asserts if True
    # i.e assert will only fail if columns contains lowercase letters
    assert df.apply(
        lambda col: (col == col.str.upper().str.replace(' ', '_')).all()
        if col.dtype == 'object'
        else True
    ).all()
    
    
def test_columns_strip_whitespace(df_sample):
    '''
    tests whether whitespace has been correctly removed from dataframe columns
    '''
    
    df = df_sample
    df = dp.columns_to_snakecase(df, uppercase = True)
    
    assert df.apply(
        lambda col: (col.str.strip() if col.dtype == "object" else col).all()
        if col.dtype == 'object'
        else True
    ).all()
        
        
def test_optimise_numeric_datatypes(df_sample):
    '''
    - tests whether numeric datatypes have been correctly downcasted to the
    smallest possible type
    - tests whether floats and intigers are correctly identified and optimised
    - tests whether object columns are ignored correctly
    '''
    
    df = df_sample
    df = dp.columns_optimise_numerics(df)
    
    assert df['Student Number'].dtype == 'int32'
    assert df['Fees'].dtype == 'float32'
    assert df['Programme School'].dtype == 'object'
    assert df['Academic Year'].dtype == 'int16'