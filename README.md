# DataTrade Processing

> [!IMPORTANT]  
> Proyect developent has moved to [Codeberg](https://codeberg.org/gitinference/jp-imports)

## Overview

This project provides tools for processing trade data for Puerto Rico, integrating various data sources and performing necessary transformations using Polars and Ibis. The project also includes unit tests to ensure the integrity and correctness of data processing functionalities.

## Features

- Import and process trade data from Parquet files.
- Support for various aggregation types (e.g., total, NAICS, HTS).
- Flexible handling of different timeframes (e.g., yearly, monthly, quarterly).
- Unit tests for validating data processing against expected results.
- The default data storage is SQLite, but can be configured to use PostgreSQL (other databases are not yet supported).

## Requirements

- Python 3.10+
- `polars`
- `ibis-framework`
- `pytest`
- `sqlite3`

You can install the required libraries using pip:

```bash
pip install polars ibis-framework pytest
```

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/ouslan/jp-imports.git
   cd jp-imports
   ```

2. Install the necessary dependencies:

   ```bash
   pip install -r requirements.txt
   # or
   conda env create -f environment.yml
   ```

## Usage

### DataTrade Class

The main class for processing trade data is `DataTrade`. You can initialize it with a SQLite database connection string and then use its methods to insert and process data.

```python
from src.jp_imports.data_process import DataTrade

# Initialize DataTrade
data_trade = DataTrade("sqlite:///path/to/database.sqlite", dev=True)

# Insert data
data_trade.insert_int_jp("path/to/jp_data_sample.parquet", "path/to/code_agr.json")
data_trade.insert_int_org("path/to/org_data_sample.parquet")

# Process data
result_df = data_trade.process_int_jp("yearly", "total", True).to_polars()
```

However, you can call the `DataTrade` class directly and will do all the insert and data download steps for you. Example:

```python
from src.jp_imports.data_process import DataTrade

data_trade = DataTrade()
data_trade.process_int_jp("yearly", "total", True)
# or
data_trade.process_int_org("yearly", "total", True)
```

### Running Tests

To run the unit tests, use pytest:

```bash
pytest
```

This will execute all tests defined in the `tests` directory and report the results.

## Directory Structure

```
datatrade-processing/
├── src/
│   └── jp_imports/
│       |── data_process.py
|       └── data_pull.py
├── test/
│   └── test_inserts/
│       ├── jp_data_sample.parquet
│       ├── org_data_sample.parquet
│       └── jp_results_yearly_total_True.parquet
├── tests/
│   └── test_data_trade.py
└── requirements.txt
```

## License

This project is licensed under the GPL V3 License. See the [LICENSE](LICENSE) file for details.

