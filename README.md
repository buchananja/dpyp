# **dpypr**
*(data-piper) a data pipeline convenience tool for Python*

<p align = "center">
  <img src = "logo/dpypr_logo.png" alt = "image" width = "300" height = "300">
</p>

## About
- dpypr is a convenience wrapper for Pandas that aids the use of 'data 
dictionaries' in small-scale data pipelines.

## Use
- dpypr allows users to gather target files from directories into data 
dictionaries (e.g. all ending in .csv, .xlsx, .parquet, etc.).
- Users can then clean data using cleaning and transformation funcions that are
iteratively applied to all data in the dictionary, making batch processing
quick to write and more readable.
- Once batch processing is complete, data dictionaries can be unpacked as
global objects for further processing.
- When data processing is complete, dpypr allows users to gather global
dataframes into data dictionaries for writing to output directories with
renaming.

## Aims
- Build common data manipulation procedures with clear syntax for pipelines.
- Build robust testing and error handling with high coverage.
- Allow customisation within functions for flexibility and ease of use.
- Maintain a cohesive naming scheme, syntax, and styling.
- Produce example code showcasing capabilities of library.