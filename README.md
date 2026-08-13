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
git clone [https://codeberg.org/gitinference/jp-imports.git](https://codeberg.org/gitinference/jp-imports.git)
cd jp-imports

# Using devenv (recommended if configured)
devenv shell --profile dev

# Or via standard pip requirements
pip install -r requirements.txt
```
