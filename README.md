# **dpyp**
*A convenience tool for small-scale data pipelines in Python*

<p align = "center">
  <img src = "logo/dpyp_logo.png" alt = "image" width = "350" height = "350">
</p>

## About
- This project is a Python-based data-pipeline convenience tool containing functionality for reading and writing batches, cleaning data, diagnosing pipelines, manipulating text, and calculating fields.

## Usage
- Consists of six modules: 'calculate', 'clean', 'diagnose', 'read', 'text', and 'write'.
- Designed for use in small-scale Python pipelines with an emphasis on batch-processing via 'data-dictionaries'.
- Batch processing of data via dictionaries allows iterative functions to improve readability and ease of use.
- Built using a combination of base Python and pandas for writing robust small-scale pipelines with text manipulation capabilities for parsing csv/json/xml etc.

## Dependencies

- pandas
- pyarrow

## Installation

```bash
pip install dpyp
```

## License

- see [LICENSE.md](LICENSE.md)