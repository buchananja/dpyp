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
    
def test_headers_to_snakecase(sample_dataframe):
    df = sample_dataframe
    df = pyped.headers_to_snakecase(df)
    assert all((col.islower() and ' ' not in col) for col in df.columns)
    
def test_values_to_lowercase(sample_dataframe):
    df = sample_dataframe
    df = pyped.values_to_lowercase(df)
    
    # returns True if all lowercase or not object. Asserts if True.
    # i.e assert will only fail if columns contains uppercse letters.
    assert df.apply(
        lambda col: col.str.lower.all() 
        if col.dtype == 'object'
        else True
    ).all()
    
    
# def values_to_lowercase(df):
#     '''
#     Converts all string values to lowercase.
#     '''
#     df = df.apply(lambda x: x.str.lower() if x.dtype == "object" else x)
#     return df