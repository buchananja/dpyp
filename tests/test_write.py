'''
This file tests whether the 'dpypr.write' module is working correctly.
'''


# Dependencies ################################################################
import pytest
import pandas as pd
import dpypr as dp

    
# Fixtures ####################################################################
@pytest.fixture
def df_sample():
    
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


# Tests #######################################################################