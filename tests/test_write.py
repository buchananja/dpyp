'''this file tests whether the 'dpypr.write' module is working correctly'''


import pytest
import pandas as pd
import dpypr as dp
import os

    
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
        ]
    }
    df = pd.DataFrame(df)
    return df


# write_dict_to_json ##########################################################
def test_write_dict_to_json_empty_dict(tmp_path):
    '''tests that writing empty dictionary to path produces no output'''
    
    sample_dictionary = dict()
    dp.write_dict_to_json(sample_dictionary, tmp_path)
    
    assert os.listdir(tmp_path) == list()


def test_write_dict_to_json_correct_files(df_sample, tmp_path):
    '''
    tests that all correct files with correct extensions are outputted to path
    and invalid/missing data skipped
    '''
    
    df_1 = df_sample
    df_2 = df_sample
    df_3 = df_sample
    df_4 = df_sample
    sample_dictionary = {
        'df_1_key': df_1, 
        'df_2_key': df_2, 
        'df_3_key': df_3,
        'invalid': df_4, # should be skipped, invalid name + valid data
        'df_5_key': '', # should be skipped, valid name + invalid data
    }
    dp.write_dict_to_json(sample_dictionary, tmp_path)
    
    # checks that only three files are written and names are correct
    assert len(os.listdir(tmp_path)) == 3
    assert all(
        filename in os.listdir(tmp_path) for filename in [
            'df_1_key.json', 'df_2_key.json', 'df_3_key.json'
        ]
    )


# write_dict_to_csv ###########################################################
def test_write_dict_to_csv_empty_dict(tmp_path):
    '''tests that writing empty dictionary to path produces no output'''
    
    sample_dictionary = dict()
    dp.write_dict_to_csv(sample_dictionary, tmp_path)
    
    assert os.listdir(tmp_path) == list()


def test_write_dict_to_csv_correct_files(df_sample, tmp_path):
    '''
    tests that all correct files with correct extensions are outputted to path
    and invalid/missing data skipped
    '''
    
    df_1 = df_sample
    df_2 = df_sample
    df_3 = df_sample
    df_4 = df_sample
    sample_dictionary = {
        'df_1_key': df_1, 
        'df_2_key': df_2, 
        'df_3_key': df_3,
        'invalid': df_4, # should be skipped, invalid name + valid data
        'df_5_key': '', # should be skipped, valid name + invalid data
    }
    dp.write_dict_to_csv(sample_dictionary, tmp_path)
    
    # checks that only three files are written and names are correct
    assert len(os.listdir(tmp_path)) == 3
    assert all(
        filename in os.listdir(tmp_path) for filename in [
            'df_1_key.csv', 'df_2_key.csv', 'df_3_key.csv'
        ]
    )
    

# write_dict_to_xlsx ##########################################################
def test_write_dict_to_xlsx_empty_dict(tmp_path):
    '''tests that writing empty dictionary to path produces no output'''
    
    sample_dictionary = dict()
    dp.write_dict_to_xlsx(sample_dictionary, tmp_path)
    
    assert os.listdir(tmp_path) == list()


def test_write_dict_to_xlsx_correct_files(df_sample, tmp_path):
    '''
    tests that all correct files with correct extensions are outputted to path
    and invalid/missing data skipped
    '''
    
    df_1 = df_sample
    df_2 = df_sample
    df_3 = df_sample
    df_4 = df_sample
    sample_dictionary = {
        'df_1_key': df_1, 
        'df_2_key': df_2, 
        'df_3_key': df_3,
        'invalid': df_4, # should be skipped, invalid name + valid data
        'df_5_key': '', # should be skipped, valid name + invalid data
    }
    dp.write_dict_to_xlsx(sample_dictionary, tmp_path)
    
    # checks that only three files are written and names are correct
    assert len(os.listdir(tmp_path)) == 3
    assert all(
        filename in os.listdir(tmp_path) for filename in [
            'df_1_key.xlsx', 'df_2_key.xlsx', 'df_3_key.xlsx'
        ]
    )


# write_dict_to_feather #######################################################
def test_write_dict_to_feather_empty_dict(tmp_path):
    '''tests that writing empty dictionary to path produces no output'''
    
    sample_dictionary = dict()
    dp.write_dict_to_feather(sample_dictionary, tmp_path)
    
    assert os.listdir(tmp_path) == list()


def test_write_dict_to_feather_correct_files(df_sample, tmp_path):
    '''
    tests that all correct files with correct extensions are outputted to path
    and invalid/missing data skipped
    '''
    
    df_1 = df_sample
    df_2 = df_sample
    df_3 = df_sample
    df_4 = df_sample
    sample_dictionary = {
        'df_1_key': df_1, 
        'df_2_key': df_2, 
        'df_3_key': df_3,
        'invalid': df_4, # should be skipped, invalid name + valid data
        'df_5_key': '', # should be skipped, valid name + invalid data
    }
    dp.write_dict_to_feather(sample_dictionary, tmp_path)
    
    # checks that only three files are written and names are correct
    assert len(os.listdir(tmp_path)) == 3
    assert all(
        filename in os.listdir(tmp_path) for filename in [
            'df_1_key.feather', 'df_2_key.feather', 'df_3_key.feather'
        ]
    )


# write_dict_to_parquet #######################################################
def test_write_dict_to_parquet_empty_dict(tmp_path):
    '''tests that writing empty dictionary to path produces no output'''
    
    sample_dictionary = dict()
    dp.write_dict_to_parquet(sample_dictionary, tmp_path)
    
    assert os.listdir(tmp_path) == list()


def test_write_dict_to_parquet_correct_files(df_sample, tmp_path):
    '''
    tests that all correct files with correct extensions are outputted to path
    and invalid/missing data skipped
    '''
    
    df_1 = df_sample
    df_2 = df_sample
    df_3 = df_sample
    df_4 = df_sample
    sample_dictionary = {
        'df_1_key': df_1, 
        'df_2_key': df_2, 
        'df_3_key': df_3,
        'invalid': df_4, # should be skipped, invalid name + valid data
        'df_5_key': '', # should be skipped, valid name + invalid data
    }
    dp.write_dict_to_parquet(sample_dictionary, tmp_path)
    
    # checks that only three files are written and names are correct
    assert len(os.listdir(tmp_path)) == 3
    assert all(
        filename in os.listdir(tmp_path) for filename in [
            'df_1_key.parquet', 'df_2_key.parquet', 'df_3_key.parquet'
        ]
    )


# write_dict_to_pickle ########################################################
def test_write_dict_to_pickle_empty_dict(tmp_path):
    '''tests that writing empty dictionary to path produces no output'''
    
    sample_dictionary = dict()
    dp.write_dict_to_pickle(sample_dictionary, tmp_path)
    
    assert os.listdir(tmp_path) == list()


def test_write_dict_to_pickle_correct_files(df_sample, tmp_path):
    '''
    tests that all correct files with correct extensions are outputted to path
    and invalid/missing data skipped
    '''
    
    df_1 = df_sample
    df_2 = df_sample
    df_3 = df_sample
    df_4 = df_sample
    sample_dictionary = {
        'df_1_key': df_1, 
        'df_2_key': df_2, 
        'df_3_key': df_3,
        'invalid': df_4, # should be skipped, invalid name + valid data
        'df_5_key': '', # should be skipped, valid name + invalid data
    }
    dp.write_dict_to_pickle(sample_dictionary, tmp_path)
    
    # checks that only three files are written and names are correct
    assert len(os.listdir(tmp_path)) == 3
    assert all(
        filename in os.listdir(tmp_path) for filename in [
            'df_1_key.pickle', 'df_2_key.pickle', 'df_3_key.pickle'
        ]
    )

# write_dict_to_sqlite ########################################################
# def test_write_dict_to_sqlite(df_sample):