# Product Chain Dashboard

This project builds a comprehensive Excel table for purchase and product analysis.
It combines stock, supplier, pricing, returns, movement history, seasonal, and web analytics data into a single decision-ready file to make supply-chain management, pricing, and ordering more data driven.

## What It Does

- Loads product and warehouse source files (CSV/Excel) from your configured data paths.
- Merges article master, supplier, stock, catalogue, warehouse movement, and price data.
- Calculates key analytics such as:
  - last 30-day and 12-month faktura/returns/storno metrics,
  - average monthly sales,
  - stock availability forecast,
  - margin, stock value, and return ratio,
  - action-oriented pricing recommendation fields (`MASSNAHME*`).
- Exports a formatted Excel workbook (`Produkt Analytik`) with filters, conditional formatting, and fixed column schema.
- Preserves manually maintained columns from the previous day’s output when available.

## Why It Matters

The generated Excel file is meant to support purchasing and supply-chain decisions from multiple angles in one place:

- **Ordering**: estimate when stock runs out and prioritize replenishment.
- **Pricing**: identify candidates for markdowns or price actions using current/old prices, margin, and movement behavior.
- **Inventory control**: monitor stock, open orders, returns, and seasonal fit.
- **Supplier steering**: connect supplier information directly to product-level performance.

## Pipeline Flow

1. Backup yesterday’s output file.
2. Load all source datasets.
3. Compute movement and sales analytics.
4. Merge all datasets into one product-level table.
5. Transform/standardize columns and compute business metrics.
6. Write the final Excel output.
7. Archive the new output into a history file.

Main entrypoint: `run.py`

## Project Structure

```text
run.py
pipeline/
  config.py                # paths, output schema, constants
  flow.py                  # Prefect flow orchestration
  tasks/
    load.py                # data loading tasks
    merge.py               # merge + analytics tasks
    transform.py           # business transformations
    export.py              # Excel export formatting/writing
  helpers/
    analytics.py
    classification.py
    pricing.py
    p_artikel.py
    excel_config.py
```

## Requirements

From `requirements.txt`:

- pandas
- numpy
- openpxyl
- xlsxwriter
- prefect

## Setup

1. Create and activate a Python environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Configure paths in `pipeline/config.py` for your environment:
   - source CSV/Excel files,
   - output Excel file,
   - history and backup destinations,
   - optional GA4 input file.

## Run

```bash
python run.py
```

The script runs through Prefect when available and automatically falls back to local mode if Prefect API/bootstrap is not reachable.

## Output

The pipeline writes a formatted Excel file to `PRODUCT_BESTAND_PATH` (configured in `pipeline/config.py`) and creates a dated archive copy in `PRODUCT_BESTAND_HISTORIE`.

