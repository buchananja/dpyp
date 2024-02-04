import pytest
import pandas as pd
import os
import time
import logging
import sqlite3
import pyped


@pytest.fixture
def sample_dataframe():
    df_sample = {
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
        ]
    }
    df_sample = pd.DataFrame(df_sample)
    return df_sample
    

# Data Cleaning ################################################################  
def test_headers_to_snakecase_is_lower(sample_dataframe):
    '''
    Tests whether column headers correctly set to lower snakecase.
    '''
    df = sample_dataframe
    df = pyped.headers_to_snakecase(df)
    assert all((col.islower() and ' ' not in col) for col in df.columns)

def test_headers_to_snakecase_is_upper(sample_dataframe):
    '''
    Tests whether column headers correctly set to upper snakecase.
    '''
    df = sample_dataframe
    df = pyped.headers_to_snakecase(df, uppercase = True)
    assert all((col.isupper() and ' ' not in col) for col in df.columns)
    
def test_values_to_lowercase(sample_dataframe):
    '''
    Tests whether dataframe values correctly set to lowercase.
    '''
    df = sample_dataframe
    df = pyped.values_to_lowercase(df)
    
    # returns True if column is all lowercase or not object. Asserts if True.
    # i.e assert will only fail if columns contains uppercase letters.
    assert df.apply(
        lambda col: (col == col.str.lower()).all()
        if col.dtype == 'object'
        else True
    ).all()

def test_values_to_uppercase(sample_dataframe):
    '''
    Tests whether dataframe values correctly set to uppercase.
    '''
    df = sample_dataframe
    df = pyped.values_to_uppercase(df)
    
    # returns True if column is all uppercase or not object. Asserts if True.
    # i.e assert will only fail if columns contains lowercase letters.
    assert df.apply(
        lambda col: (col == col.str.upper()).all()
        if col.dtype == 'object'
        else True
    ).all()