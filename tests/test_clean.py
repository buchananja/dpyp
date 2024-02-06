'''
This file tests whether the 'dpypr.clean' module is working correctly.
'''

# Dependencies ################################################################
import pytest
import pandas as pd
import dpypr as dp

# Fixtures ####################################################################
@pytest.fixture
def sample_dataframe_snp():
    '''
    Basic example of 'student number planning'-style data.
    '''
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
            '2024',
            '2024',
            '2024',
            '2024',
            '2024',
            '2024'
        ],
        'Fees': [
            123.99,
            400.50,
            234,
            1900.0133,
            5,
            10.000001
        ]
    }
    df = pd.DataFrame(df)
    return df

# @pytest.fixture
# def sample_dataframe_snp():
#     '''
#     Basic example of 'student number planning'-style data.
#     '''
#     df = {

#     }
#     df = pd.DataFrame(df))
#     return df
    
# Tests ####################################################################### 
def test_headers_to_lower_snakecase_by_default(sample_dataframe_snp):
    '''
    Tests whether column headers correctly set to lower snakecase when
    'uppercase' argument is default.
    '''
    df = sample_dataframe_snp
    df = dp.clean.headers_to_snakecase(df)
    assert all((col.islower() and ' ' not in col) for col in df.columns)
    
def test_headers_to_lower_snakecase_manually(sample_dataframe_snp):
    '''
    Tests whether column headers correctly set to lower snakecase when
    'uppercase' argument is set to False manually.
    '''
    df = sample_dataframe_snp
    df = dp.clean.headers_to_snakecase(df, uppercase = False)
    assert all((col.islower() and ' ' not in col) for col in df.columns)

def test_headers_to_upper_snakecase(sample_dataframe_snp):
    '''
    Tests whether column headers correctly set to upper snakecase.
    '''
    df = sample_dataframe_snp
    df = dp.clean.headers_to_snakecase(df, uppercase = True)
    assert all((col.isupper() and ' ' not in col) for col in df.columns)
    
def test_values_to_lowercase(sample_dataframe_snp):
    '''
    Tests whether dataframe values correctly set to lowercase.
    '''
    df = sample_dataframe_snp
    df = dp.clean.values_to_lowercase(df)
    
    # returns True if column is all lowercase or not object. Asserts if True.
    # i.e assert will only fail if columns contains uppercase letters.
    assert df.apply(
        lambda col: (col == col.str.lower()).all()
        if col.dtype == 'object'
        else True
    ).all()

def test_values_to_uppercase(sample_dataframe_snp):
    '''
    Tests whether dataframe values correctly set to uppercase.
    '''
    df = sample_dataframe_snp
    df = dp.clean.values_to_uppercase(df)
    
    # returns True if column is all uppercase or not object. Asserts if True.
    # i.e assert will only fail if columns contains lowercase letters.
    assert df.apply(
        lambda col: (col == col.str.upper()).all()
        if col.dtype == 'object'
        else True
    ).all()
    
def test_values_to_lower_snakecase_by_default(sample_dataframe_snp):
    '''
    Tests whether dataframe values correctly set to lower snakecase by default.
    '''
    df = sample_dataframe_snp
    df = dp.clean.values_to_snakecase(df)
    
    # returns True if column is all lowercase with spaces replaced by 
    # underscores or not object. Asserts if True.
    # i.e assert will only fail if columns contains lowercase letters.
    assert df.apply(
        lambda col: (col == col.str.lower().str.replace(' ', '_')).all()
        if col.dtype == 'object'
        else True
    ).all()
    
def test_values_to_lower_snakecase_manually(sample_dataframe_snp):
    '''
    Tests whether dataframe values correctly set to lowersnakecase when
    'uppercase' argument is set to False manually.
    '''
    df = sample_dataframe_snp
    df = dp.clean.values_to_snakecase(df, uppercase = False)
    
    # returns True if column is all lowercase with spaces replaced by 
    # underscores or not object. Asserts if True.
    # i.e assert will only fail if columns contains lowercase letters.
    assert df.apply(
        lambda col: (col == col.str.lower().str.replace(' ', '_')).all()
        if col.dtype == 'object'
        else True
    ).all()
    
def test_values_to_upper_snakecase(sample_dataframe_snp):
    '''
    Tests whether dataframe values correctly set to upper snakecase.
    '''
    df = sample_dataframe_snp
    df = dp.clean.values_to_snakecase(df, uppercase = True)
    
    # returns True if column is all uppercase with spaces replaced by 
    # underscores or not object. Asserts if True.
    # i.e assert will only fail if columns contains lowercase letters..
    assert df.apply(
        lambda col: (col == col.str.upper().str.replace(' ', '_')).all()
        if col.dtype == 'object'
        else True
    ).all()
    
def test_values_strip_whitespace(sample_dataframe_snp):
    '''
    Tests whether whitespace has been correctly removed from dataframe values.
    '''
    df = sample_dataframe_snp
    df = dp.clean.values_to_snakecase(df, uppercase = True)
    
    assert df.apply(
        lambda col: (col.str.strip() if col.dtype == "object" else col).all()
        if col.dtype == 'object'
        else True
    ).all()
        
def test_optimise_numeric_datatypes(sample_dataframe_snp):
    '''
    - Tests whether numeric datatypes have been correctly downcasted to the
    smallest possible type.
    - Tests whether floats and intigers are correctly identified and optimised.
    - Tests whether object values are ignored correcgtly.
    '''
    df = sample_dataframe_snp
    df = dp.clean.optimise_numeric_datatypes(df)
    
    assert df['Student Number'].dtype == 'int32'
    assert df['Fees'].dtype == 'float32'
    assert df['Programme School'].dtype == 'object'
    assert df['Academic Year'].dtype == 'object'