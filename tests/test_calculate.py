'''this file tests whether the 'dpypr.calculate' module is working correctly'''


import pytest
import pandas as pd
import numpy as np
import dpyp as dp


@pytest.fixture
def df_sample():
    '''basic example of 'student number planning'-style data'''
    
    df = {
        'Programme School': [
            'school_of_biological_sciences',
            'school_of_natural_science',
            'school_of_engineering',
            'school_of_biological_sciences',
            'school_of_music',
            'school_of_medicine',
            'school_of_art'
        ],
        'Population_1': [
            13,
            40,
            23,
            19,
            5,
            10,
            np.nan
        ],
        'Population_2': [
            12,
            50,
            33,
            20,
            4,
            11,
            1
        ],
    }
    df = pd.DataFrame(df)
    return df


# single_rate_of_change ##########################################################
def test_single_rate_of_change_correct(df_sample):
    '''tests single_rate_of_change calculates correct value by row'''
    
    assert dp.single_rate_of_change(
        df_sample.iloc[0], 'Population_1', 'Population_2', 0
    ) == 0.07692307692307693
    

def test_single_rate_of_change_lambda(df_sample):
    '''tests single_rate_of_change returns dvalue between -1 and 1 for each row'''
    
    result = df_sample.apply(
        lambda row: dp.single_rate_of_change(
            row, 'Population_1', 'Population_2', 0
        ), 
        axis = 1
    )
    assert all(-1 <= value <= 1 for value in result)
    

# bulk_rate_of_change ##########################################################
def test_bulk_rate_of_change_correct(df_sample):
    '''
    tests bulk_rate_of_change calculates correct value-range with correct name
    and correct default rate filled into null values
    '''
    
    df = dp.bulk_rate_of_change(
        df = df_sample, 
        col_name = 'rate_of_change', 
        col1 = 'Population_1', 
        col2 = 'Population_2', 
        default_rate = 0
    )
    assert all(-1 <= row <= 1 for row in df['rate_of_change'])
    
    
# single_product #################################################################
def test_single_product_correct(df_sample):
    '''tests single_products calculates correct value by row'''
    
    assert dp.single_product(
        df_sample.iloc[0], 'Population_1', 'Population_2',
    ) == 156
    
    
# bulk_product #################################################################
def test_bulk_product_correct(df_sample):
    '''tests bulk_products calculates correct values, including nulls'''
    
    df_test = dp.bulk_product(df_sample, 'Product', 'Population_1', 'Population_2')
    expected_series = pd.Series(
        [156, 2000, 759, 380, 20, 110, np.nan], 
        name = 'Product'
    )
    assert df_test['Product'].equals(expected_series)
    
    
# single_percentage ##############################################################
def test_single_percentage_correct(df_sample):
    '''tests single_percentage outputs the correct value'''
    
    assert dp.single_percentage(
        df_sample.iloc[0], 'Population_1', 'Population_2',
    ) == 108
    

def test_single_percentage_decimal_points(df_sample):
    '''tests single_percentage outputs the correct decimal points'''
    
    assert dp.single_percentage(
        df_sample.iloc[0], 'Population_1', 'Population_2', dec_points = 1
    ) == 108.3
    
    assert dp.single_percentage(
        df_sample.iloc[0], 'Population_1', 'Population_2', dec_points = 2
    ) == 108.33
    
    
# bulk_percentage ##############################################################
def test_bulk_percentage_correct(df_sample):
    '''tests bulk_percentage calculates correct values, including nulls'''
    
    df_test = dp.bulk_percentage(
        df_sample, 'Percentage', 'Population_1', 'Population_2'
    )
    expected_series = pd.Series(
        [108.0, 80.0, 70.0, 95.0, 125.0, 91.0, np.nan], 
        name = 'Percentage'
    )
    assert df_test['Percentage'].equals(expected_series)
    

# single_modulo ##################################################################
def test_single_modulo_correct(df_sample):
    '''tests single_modulo outputs the correct value'''
    
    assert dp.single_modulo(
        df_sample.iloc[0], 'Population_1', 'Population_2',
    ) == 1.0
    

# bulk_modulo ##################################################################
def test_bulk_modulo_correct(df_sample):
    '''tests bulk_modulo calculates correct values, including nulls'''
    
    df_test = dp.bulk_modulo(
        df_sample, 'Modulo', 'Population_1', 'Population_2'
    )
    expected_series = pd.Series(
        [1.0, 40.0, 23.0, 19.0, 1.0, 10.0, np.nan], 
        name = 'Modulo'
    )
    assert df_test['Modulo'].equals(expected_series)
    
    
# single_subtraction #############################################################
def test_single_subtraction_correct(df_sample):
    '''tests single_subtraction outputs the correct value'''
    
    assert dp.single_subtraction(
        df_sample.iloc[0], 'Population_1', 'Population_2',
    ) == 1.0
    
    
# bulk_subtraction #############################################################
def test_bulk_subtraction_correct(df_sample):
    '''tests col_substraction calculates correct values, including nulls'''
    
    df_test = dp.bulk_subtraction(
        df_sample, 'Subtraction', 'Population_1', 'Population_2'
    )
    expected_series = pd.Series(
        [1.0, -10.0, -10.0, -1.0, 1.0, -1.0, np.nan], 
        name = 'Subtraction'
    )
    assert df_test['Subtraction'].equals(expected_series)
    
    
# single_addition ################################################################
def test_single_addition_correct(df_sample):
    '''tests single_addition outputs the correct value'''
    
    assert dp.single_addition(
        df_sample.iloc[0], 'Population_1', 'Population_2',
    ) == 25
    
    
# bulk_addition ################################################################
def test_bulk_addition_correct(df_sample):
    '''tests col_substraction calculates correct values, including nulls'''
    
    df_test = dp.bulk_addition(
        df_sample, 'Addition', 'Population_1', 'Population_2'
    )
    expected_series = pd.Series(
        [25.0, 90.0, 56.0, 39.0, 9.0, 21.0, np.nan], 
        name = 'Addition'
    )
    assert df_test['Addition'].equals(expected_series)
    

# single_power ###################################################################
def test_single_power_correct(df_sample):
    '''tests single_power outputs the correct value'''
    
    assert dp.single_power(
        df_sample.iloc[0], 'Population_1', 2,
    ) == 169
    

# bulk_power ###################################################################
def test_bulk_power_correct(df_sample):
    '''tests col_substraction calculates correct values, including nulls'''
    
    df_test = dp.bulk_power(
        df_sample, 'Power_2', 'Population_1', 2
    )
    expected_series = pd.Series(
        [169.0, 1600.0, 529.0, 361.0, 25.0, 100.0, np.nan], 
        name = 'Power_2'
    )
    assert df_test['Power_2'].equals(expected_series)