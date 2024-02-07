# **dpypr**
*(data-piper) a data pipeline convenience tool for Python*

<p align = "center">
  <img src = "logo/dpypr_logo.png" alt = "image" width = "300" height = "300">
</p>

## About
- This is a cursory attempt at building a data manipulation library in Python.
- I use this package daily when assembling and maintaining pipelines, and 
analysing data.
- Currently focuses on the use of 'data dictionaries', dictionaries with keys
corresponding with dataframe values. This makes gathering, cleaning, and
writing batches of data quick and simple.

## Use
- dpypr allows users to gather target files from directories into data 
dictionaries (e.g. all ending in .csv, .xlsx, .parquet, etc.).
- Users can then clean data using cleaning and transformation funcions that are
iteratively applied to all data in the dictionary, making batch processing
quick to write and more readable.
- Once batch processing is complete, data dictionaries can be unpacked as
global objects for further processing.
- When data processing is complete, dpypr allows users to gather global
dataframes into data_dictionaries for writing to output directories with
renaming.

## Aims
- Build common data manipulation procedures with clear syntax for pipelines.
- Build robust testing and error handling with high coverage.
- Allow customisation within functions for flexibility and ease of use.
- Maintain a cohesive naming scheme, syntax, and styling.
- Produce example code showcasing capabilities of library.