# DataTrade Processing

> [!IMPORTANT]
> Project development has moved to [Codeberg](https://codeberg.org/gitinference/jp-imports).

## Overview

**DataTrade Processing** is a robust, high-performance Python library designed for processing and analyzing international trade data for Puerto Rico. Built on top of [Polars](https://pola.rs/), the project employs a configuration-driven design to handle dynamic aggregation, unit conversions, and advanced rolling statistical analysis across various taxonomy levels and time frames.

---

## Key Features

- **High-Performance Processing:** Leverages Polars DataFrames for fast, memory-efficient data manipulation and aggregation.
- **Flexible Data Sources:** Supports ingestion from both organizational (`org`) and JP-specific (`jp`) trade data sources.
- **Multi-Level Classification:** Aggregate metrics by:

  - Total trade (`total`)
  - Harmonized Tariff Schedule code (`hts`)
  - North American Industry Classification System code (`naics`)
  - Country (`country`)

- **Flexible Time Frames:** Group trade data by calendar year, fiscal year (beginning in July), quarter, or month.
- **Granular Filtering:** Filter data dynamically by date ranges, specific calendar years, agricultural indicators (`agri_prod`), and taxonomy code prefixes.
- **Standardized Unit Conversions:** Automatically normalizes diverse source units (kilograms, liters, metric tons, dozens, cubic meters, grams, etc.) into a unified metric representation.
- **Advanced Price Analysis (`process_price`):**

  - Computes HS4-level import and export unit prices.
  - Calculates 3-month rolling averages and standard deviations.
  - Derives statistical price bands ($\pm 2\sigma$) and monthly market rankings.
  - Evaluates year-over-year percentage changes and ranking shifts.

---

## Requirements

- **Python:** 3.10 or higher
- **Core Dependencies:** `polars`
- **Development/Testing Dependencies:** `pytest`
- **External Dependencies:** [`pr-imports`](https://codeberg.org/gitinference/jp-imports)

---

## Installation

Clone the repository and set up your environment using your preferred package or environment manager:

```bash
git clone https://codeberg.org/gitinference/jp-imports.git
cd jp-imports

# Using devenv (recommended if configured)
devenv shell --profile dev

# Or via standard pip requirements
pip install -r requirements.txt
```

## Usage

### Initializing `JPTrade`

The core processing logic is encapsulated within the JPTrade class. By default, processed outputs are written to a local `data/` directory and logs are recorded in `data.log`.

```python
from jp_imports.jp_imports import JPTrade

# Initialize with default settings

trade = JPTrade()

# Or specify custom directories and logs

trade = JPTrade(saving_dir="data/", log_file="data.log")
```

### Processing International Trade Data

Use the `process_int_jp()` method to load, filter, convert, and aggregate trade datasets.

```python
result = trade.process_int_jp(
    level="total",
    time_frame="yearly",
)
```

#### Supported Parameters

| Parameter            | Type   | Description                                                                        |
| -------------------- | ------ | ---------------------------------------------------------------------------------- |
| `level`              | `str`  | Aggregation level: `"total"`, `"hts"`, `"naics"`, or `"country"`.                  |
| `time_frame`         | `str`  | Time period: `"yearly"`, `"fiscal"`, `"qtr"`, or `"monthly"`.                      |
| `datetime`           | `str`  | Optional filter for a single year (`"2024"`) or range (`"2024-01-01+2024-12-31"`). |
| `agriculture_filter` | `bool` | If True, restricts records to agricultural products (`agri_prod == 1`).            |
| `source`             | `str`  | Data source origin: `"org"` (default) or `"jp"`.                                   |
| `level_filter`       | `str`  | Optional taxonomy prefix filter (e.g., `level_filter="2207"` for HTS codes).       |

### Price Analysis Pipeline

To generate rolling price metrics, unit costs, bands, and year-over-year variations at the HS4 classification level over a 3 month window, use `process_price()`:

```python
# Standard price processing

prices = trade.process_price()

# Isolated for agricultural commodities

ag_prices = trade.process_price(agriculture_filter=True)
```

### Running Tests

Execute the test suite using pytest:

```bash
pytest
```

## Project Architecture

```text
jp-imports/
├── src/
│   └── jp_imports/
│       ├── jp_imports.py
│       └── resources/
│           └── code_agr.json
├── tests/
├── requirements.txt
├── environment.yml
├── README.md
└── LICENSE
```

## License

Distributed under the GPL v3 License. See LICENSE for more information.
