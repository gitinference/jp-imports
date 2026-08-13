# DataTrade Processing

> [!IMPORTANT]
> Project development has moved to [Codeberg](https://codeberg.org/gitinference/jp-imports).

## Overview

This project provides tools for processing international trade data for Puerto Rico. It uses [Polars](https://pola.rs/) for high-performance DataFrame processing and supports configurable aggregation by trade classification and time period.

The primary processing interface is the `JPTrade` class, which loads trade data, applies optional filters, standardizes quantity measurements, and produces aggregated import/export statistics.

## Features

- Process international trade data from organizational (`org`) or JP-specific (`jp`) sources.
- Aggregate trade data by:

  - Total trade
  - HTS code
  - NAICS code
  - Country

- Aggregate data by:

  - Calendar year
  - Fiscal year
  - Quarter
  - Month

- Filter data by:

  - Calendar year
  - Date ranges
  - HTS, NAICS, or country code prefixes
  - Agricultural products

- Convert source-specific quantity units into standardized quantities.
- Calculate import and export values and quantities.
- Calculate net imports and net exports.
- Generate monthly HS4-level price statistics.
- Calculate three-month rolling price averages and standard deviations.
- Generate price bands and monthly rankings.
- Calculate year-over-year price and ranking changes.
- Process data using Polars DataFrames.

## Requirements

- Python 3.10+
- `polars`
- `pytest` for running tests
- The project's [pr-imports](https://codeberg.org/gitinference/jp-imports) dependency

Install the Python dependencies with:

```bash
pip install -r requirements.txt
```

Alternatively, if an environment definition is provided using devenv:

```bash
devenv shell --profile dev
```

## Installation

Clone the repository and install its dependencies:

```bash
git clone https://github.com/ouslan/jp-imports.git
cd jp-imports
devenv shell --profile dev
```

Development of the project has moved to Codeberg:

<https://codeberg.org/gitinference/jp-imports>

## Usage

### `JPTrade`

The main processing class is `JPTrade`.

```python
from jp_imports.jp_imports import JPTrade

trade = JPTrade()
```

By default, processed data is stored under `data/` and logging is written to `data.log`. These locations can be customized:

```python
trade = JPTrade(
    saving_dir="data/",
    log_file="data.log",
)
```

### Processing International Trade Data

Use `process_int_jp()` to load and process international trade data.

```python
result = trade.process_int_jp(
    level="total",
    time_frame="yearly",
)
```

The `level` argument supports:

| Level     | Description              |
| --------- | ------------------------ |
| `total`   | Aggregate all trade data |
| `hts`     | Aggregate by HTS code    |
| `naics`   | Aggregate by NAICS code  |
| `country` | Aggregate by country     |

The `time_frame` argument supports:

| Time frame | Description               |
| ---------- | ------------------------- |
| `yearly`   | Calendar-year aggregation |
| `fiscal`   | Fiscal-year aggregation   |
| `qtr`      | Quarterly aggregation     |
| `monthly`  | Monthly aggregation       |

### Date Filtering

A single calendar year can be supplied:

```python
result = trade.process_int_jp(
    level="total",
    time_frame="yearly",
    datetime="2024",
)
```

A date range can also be supplied using `YYYY-MM-DD+YYYY-MM-DD`:

```python
result = trade.process_int_jp(
    level="total",
    time_frame="monthly",
    datetime="2024-01-01+2024-12-31",
)
```

### Filtering by Classification

HTS, NAICS, and country filtering use prefix matching through `level_filter`.

For example, to process an HTS prefix:

```python
result = trade.process_int_jp(
    level="hts",
    time_frame="monthly",
    level_filter="2207",
)
```

If the supplied classification prefix does not match any records, a `ValueError` is raised.

### Agricultural Products

Set `agriculture_filter=True` to restrict the data to records where `agri_prod` equals `1`:

```python
result = trade.process_int_jp(
    level="hts",
    time_frame="yearly",
    agriculture_filter=True,
)
```

### Selecting the Data Source

The `source` parameter determines which international trade source is loaded:

```python
# Organizational source (default)
result = trade.process_int_jp(
    level="total",
    time_frame="yearly",
    source="org",
)

# JP-specific source
result = trade.process_int_jp(
    level="total",
    time_frame="yearly",
    source="jp",
)
```

Supported values are `org` and `jp`.

## Output

`process_int_jp()` returns a Polars `DataFrame`.

The resulting data contains aggregated import and export values and quantities. Missing trade measures are filled with zero, and the following net measures are calculated:

- `imports`
- `exports`
- `imports_qty`
- `exports_qty`
- `net_imports`
- `net_exports`
- `net_imports_qty`
- `net_exports_qty`

The exact grouping columns depend on the selected `level` and `time_frame`.

For example:

```python
result = trade.process_int_jp(
    level="country",
    time_frame="monthly",
)

print(result)
```

## Price Analysis

`process_price()` produces monthly HS4-level price statistics from international trade data.

```python
prices = trade.process_price()
```

Agricultural products can optionally be isolated:

```python
ag_prices = trade.process_price(
    agriculture_filter=True,
)
```

The price-processing pipeline:

1. Processes monthly HTS-level trade data.
2. Converts HTS codes to HS4 classifications.
3. Aggregates trade values and quantities by HS4, year, and month.
4. Calculates import and export unit prices.
5. Calculates three-month rolling averages.
6. Calculates rolling standard deviations.
7. Generates upper and lower price bands using two standard deviations.
8. Produces monthly rankings.
9. Calculates prior-year prices and rankings.
10. Calculates year-over-year price and ranking changes.

Relevant output columns include:

- `hs4`
- `year`
- `month`
- `price_imports`
- `price_exports`
- `moving_price_imports`
- `moving_price_exports`
- `moving_price_imports_std`
- `moving_price_exports_std`
- `upper_band_imports`
- `lower_band_imports`
- `upper_band_exports`
- `lower_band_exports`
- `rank_imports`
- `rank_exports`
- `prev_year_imports`
- `prev_year_exports`
- `pct_change_imports_year_over_year`
- `pct_change_exports_year_over_year`
- `rank_imports_change_year_over_year`
- `rank_exports_change_year_over_year`

## Quantity Conversion

The `conversion()` method standardizes quantities from the source data before aggregation.

It handles source units including:

- kilograms (`kg`)
- liters (`l`)
- dozens (`doz`)
- cubic meters (`m3`)
- metric tons (`t`)
- `kts`
- `pfl`
- grams (`gm`)

The method also derives:

- `qtr`
- `fiscal_year`
- `month`
- `year`

and combines the converted primary and secondary quantities into the `qty` field used for aggregation.

The fiscal year begins in July. For example, July 2024 belongs to fiscal year 2025.

## Data Aggregation

Internally, trade records are separated into imports and exports using `trade_id`:

- `trade_id == 1` → imports
- `trade_id == 2` → exports

The records are grouped according to the requested time and classification dimensions. Import and export aggregates are then joined so that records appearing in either trade type are retained.

## Running Tests

Run the test suite with:

```bash
pytest
```

Tests should be placed in the `tests/` directory.

## Directory Structure

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

This project is licensed under the GPL v3 License. See the [LICENSE](LICENSE) file for details.
