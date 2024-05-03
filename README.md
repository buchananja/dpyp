# **dpyp**
*A convenience tool for small-scale data pipelines in Python*

<p align = "center">
  <img src = "logo/dpyp_logo.png" alt = "image" width = "350" height = "350">
</p>

## About
dpyp is a data-pipeline convenience tool containing functionality for reading and writing batches, cleaning data, diagnosing pipelines, manipulating text, and calculating fields in Python.

## Usage
- dpyp consists of six modules: 'calculate', 'clean', 'diagnose', 'read', 'text', and 'write'.
- Designed for use in small-scale Python pipelines with an emphasis on batch-processing via 'data-dictionaries'.
- Batch processing of data via dictionaries allows iterative functions to improve readability and ease of use.
- Built using a combination of base Python and pandas for writing robust small-scale pipelines with text manipulation capabilities.

## Dependencies
- pandas
- pyarrow

## Installation
```bash
pip install dpyp
```

## License
See [LICENSE.md](LICENSE.md)

## Contributing
See [CONTRIBUTING.md](CONTRIBUTING.md)