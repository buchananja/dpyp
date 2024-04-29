'''
the 'text' module contains functionality for parsing text
text
'''

import re
import logging


logger = logging.getLogger(__name__)


class TranText:
    '''transforms areas of text into useful conversions or additions'''
    
    
    @staticmethod
    def get_month_numeric(month_str):
        '''takes string month name and returns numeric calender position'''

        month_dict = {
            'jan': 1,
            'feb': 2,
            'mar': 3,
            'apr': 4,
            'may': 5,
            'jun': 6,
            'jul': 7,
            'aug': 8,
            'sep': 9,
            'oct': 10,
            'nov': 11,
            'dec': 12
        }
        if not month_str[:3].isalpha():
            raise ValueError(f"Invalid month string: {month_str}")
        
        if month_str[:3].lower() in month_dict.keys():
            return month_dict[month_str[:3].lower()]
        else:
            raise ValueError(f"Invalid month string: {month_str}")


class RemText:
    '''removes particular characters, sequences, or phrases from text'''
    
    
    @staticmethod
    def remove_trailing_char(line, char):
        '''returns line with trailing character removed'''

        if line.endswith(char):
            line = line[:-1]
        return line


    @staticmethod
    def remove_leading_char(line, char):
        '''returns line with leading character removed'''

        if line.startswith(char):
            line = line[1:]
        return line


class RepText:
    '''replaces specific secions of text'''
    
    
    @staticmethod
    def replace_consecutive_whitespace(
            line, 
            replacement_char, 
            ignore_tab = False,
            ignore_return = False,
            ignore_carriage_return = False
        ):
        '''returns line with all consecutive whitespace replaced with character'''
        
        # created regex
        pattern = r'[^\S'
        if ignore_tab:
            pattern += '\t'
        if ignore_return:
            pattern += '\n'
        if ignore_carriage_return:
            pattern += '\r'
        pattern += ']+'
        
        cleaned_line = re.sub(pattern, replacement_char, line)
        return cleaned_line


class GetText:
    '''gets specific text from larger strings'''
    
    
    @staticmethod        
    def get_split_index_text(phrase, split_char, index):
        '''gets text from index in phrase split by character'''

        text = phrase.strip().split(split_char)[index]
        return text


    @staticmethod
    def get_text_numerics(phrase, split_char, *indexes):
        '''
        takes a phrase and list of indexes to extract numbers from; appends numbers
        to a list and returns a tuple of numbers
        '''

        # checks all indexes are intigers
        if all([isinstance(index, int) for index in indexes]):      
            numbers = list()
            for index in indexes:
                try:
                    number = ''.join(filter(str.isdigit, phrase.split(split_char)[index]))
                    numbers.append(number)
                    numbers = tuple(numbers)
                except IndexError:
                    print('Index out of bounds')
        else:
            print('Index not intiger')
            raise ValueError
        return numbers


    @staticmethod
    def get_string_numerics(phrase):
        '''extracts and joins numeric characters from a string'''

        number = ''.join(filter(str.isdigit, phrase))
        return number

    
    @staticmethod
    def get_text_between_indexes(phrase, char_1, char_2):
        '''extracts text within a string between two characters'''

        start_index = phrase.index(char_1) + 1
        end_index = phrase.index(char_2)
        extracted_text = phrase[start_index:end_index]
        return extracted_text