'''
the 'text' module contains functionality for parsing text
text
'''

import re
import logging
from typing import List, Tuple


logger = logging.getLogger(__name__)


class TranText:
    '''transforms areas of text into useful conversions or additions'''
    
    
    # TODO: accepts any string containing month name (e.g. 'janitor' -> 1)
    @staticmethod
    def get_month_numeric(month_text: str) -> int:
        '''takes string month name and returns numeric calender position'''

        month_dict = {
            'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
            'may': 5, 'jun': 6, 'jul': 7, 'aug': 8,
            'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
        }
        if not month_text[:3].isalpha():
            raise ValueError(f"Invalid month string: {month_text}")
        
        if month_text[:3].lower() in month_dict.keys():
            month_number = month_dict[month_text[:3].lower()]
            return month_number
        else:
            raise ValueError(f"Invalid month string: {month_text}")


class RemText:
    '''removes particular characters, sequences, or phrases from text'''
    
    
    @staticmethod
    def remove_trailing_char(line: str, char: str) -> str:
        '''returns line with trailing character removed'''

        if line.endswith(char):
            line = line[:-1]
        return line


    @staticmethod
    def remove_leading_char(line: str, char: str) -> str:
        '''returns line with leading character removed'''

        if line.startswith(char):
            line = line[1:]
        return line


class RepText:
    '''replaces specific secions of text'''
    
    
    @staticmethod
    def replace_consecutive_whitespace(
        line: str,
        replacement_char: str, 
        ignore_tab: bool = False,
        ignore_return: bool = False,
        ignore_carriage_return: bool = False
    ) -> str:
        '''returns line with consecutive whitespace replaced with character'''
        
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
    def get_split_index_text(
        text: str, 
        split_char: str, 
        index: int
    ) -> str:
        '''gets text from index in phrase split by character'''

        text = text.strip().split(split_char)[index]
        return text


    @staticmethod
    def get_text_numerics(
        text: str, 
        split_char: str, 
        *indexes: int
    ) -> Tuple[str, ...]:
        '''returns tuple of numeric strings from phrase (list of strings)'''

        if all([isinstance(index, int) for index in indexes]):      
            numbers = list()
            for index in indexes:
                try:
                    number = ''.join(
                        filter(str.isdigit, text.split(split_char)[index])
                    )
                    numbers.append(number)
                except IndexError:
                    logger.debug('Index out of bounds')
        else:
            logger.debug('Index not intiger')
            raise ValueError
        
        return tuple(numbers)


    @staticmethod
    def get_string_numerics(text: str) -> str:
        '''extracts and joins numeric characters from a string'''

        number = ''.join(filter(str.isdigit, text))
        return number

    
    @staticmethod
    def get_text_between_indexes(text: str, char_1: str, char_2: str) -> str:
        '''extracts text within a string between two character indexes'''

        extracted_text = text[text.index(char_1) + 1:text.index(char_2)]
        return extracted_text