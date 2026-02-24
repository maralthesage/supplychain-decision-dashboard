"""
tasks/export.py
===============
Prefect task for writing the final DataFrame to a formatted Excel workbook.

All column-level formatting rules are read from helpers/excel_config.py so
this file contains only the mechanics of writing (XlsxWriter interactions),
not the decisions about which column gets which format.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from prefect import task
from xlsxwriter.utility import xl_col_to_name

from pipeline.config import PRODUCT_BESTAND_PATH
from pipeline.helpers.run_logging import get_logger
from pipeline.helpers.excel_config import (
    COLUMN_FORMAT_KEYS,
    DEFAULT_COLUMN_WIDTH,
    MIXED_CURRENCY_COLUMNS,
    SEASONAL_COLUMNS,
    WRAP_COLUMN_WIDTH,
    WRAP_COLUMNS,
    ZERO_WIDTH_COLUMNS,
)

SHEET_NAME = "Produkt Analytik"


# ---------------------------------------------------------------------------
# Format factory
# ---------------------------------------------------------------------------

def _build_formats(workbook) -> dict:
    """
    Create and return all XlsxWriter format objects keyed by the short names
    used in COLUMN_FORMAT_KEYS.
    """
    return {
        "text": workbook.add_format({"num_format": "@"}),
        "text_wrap": workbook.add_format({"text_wrap": True}),
        "date": workbook.add_format({"num_format": "yyyy-mm-dd"}),
        "currency": workbook.add_format(
            {"num_format": "#,##0.00 [$€-1];[Red]-#,##0.00 [$€-1]"}
        ),
        "int": workbook.add_format({"num_format": "#,##0"}),
        "decimal": workbook.add_format({"num_format": "#,##0.00"}),
        "percentage": workbook.add_format({"num_format": "0.00%"}),
        # special-purpose formats used only in conditional formatting
        "_green_text": workbook.add_format({"font_color": "green"}),
        "_red_font":   workbook.add_format({"font_color": "red"}),
        "_header": workbook.add_format(
            {"bold": True, "font_color": "black", "bg_color": "#D9EAD3", "border": 1}
        ),
    }


# ---------------------------------------------------------------------------
# Column-width + base-format helper
# ---------------------------------------------------------------------------

def _apply_column_widths(worksheet, columns: list[str], formats: dict) -> None:
    """
    Set column widths and base formats for every column in the sheet.

    Priority:
    1. WRAP_COLUMNS       → width 30, text_wrap format
    2. ZERO_WIDTH_COLUMNS → width 0  (hidden), int format
    3. Everything else    → DEFAULT_COLUMN_WIDTH, format from COLUMN_FORMAT_KEYS
    """
    for idx, col in enumerate(columns):
        if col in WRAP_COLUMNS:
            worksheet.set_column(idx, idx, WRAP_COLUMN_WIDTH, formats["text_wrap"])
        elif col in ZERO_WIDTH_COLUMNS:
            worksheet.set_column(idx, idx, 0, formats["int"])
        else:
            fmt_key = COLUMN_FORMAT_KEYS.get(col)
            fmt = formats.get(fmt_key) if fmt_key else None
            worksheet.set_column(idx, idx, DEFAULT_COLUMN_WIDTH, fmt)


# ---------------------------------------------------------------------------
# Conditional-formatting helpers
# ---------------------------------------------------------------------------

def _apply_lager_verfuegbarkeit_cf(
    worksheet,
    columns: list[str],
    last_excel_row: int,
    formats: dict,
) -> None:
    """Highlight 'Lager Verfügbarkeit bis' cells in current or next month red."""
    if "Lager Verfügbarkeit bis" not in columns:
        return

    today = datetime.today()
    first_this = today.replace(day=1)
    first_next = first_this + timedelta(days=32)
    current_month = today.strftime("%Y-%m")
    next_month    = first_next.strftime("%Y-%m")

    col_idx    = columns.index("Lager Verfügbarkeit bis")
    col_letter = xl_col_to_name(col_idx)
    rng        = f"{col_letter}2:{col_letter}{last_excel_row}"

    for month_str in (current_month, next_month):
        worksheet.conditional_format(
            rng,
            {"type": "cell", "criteria": "==", "value": f'"{month_str}"',
             "format": formats["_red_font"]},
        )


def _apply_seasonal_cf(
    worksheet,
    columns: list[str],
    last_excel_row: int,
    formats: dict,
) -> None:
    """Highlight TRUE values in seasonal columns green."""
    for col in SEASONAL_COLUMNS:
        if col not in columns:
            continue
        col_idx    = columns.index(col)
        col_letter = xl_col_to_name(col_idx)
        rng        = f"{col_letter}2:{col_letter}{last_excel_row}"
        worksheet.conditional_format(
            rng,
            {"type": "cell", "criteria": "==", "value": "TRUE",
             "format": formats["_green_text"]},
        )


def _apply_mixed_currency_cf(
    worksheet,
    columns: list[str],
    last_excel_row: int,
    formats: dict,
) -> None:
    """
    Apply currency conditional formatting to mixed text/numeric columns.
    The base column format is text; numbers are displayed as currency via
    two conditional format rules (>= 0 and < 0).
    """
    for col in MIXED_CURRENCY_COLUMNS:
        if col not in columns:
            continue
        col_idx    = columns.index(col)
        col_letter = xl_col_to_name(col_idx)
        rng        = f"{col_letter}2:{col_letter}{last_excel_row}"
        for criteria, value in [(">=", 0), ("<", 0)]:
            worksheet.conditional_format(
                rng,
                {"type": "cell", "criteria": criteria, "value": value,
                 "format": formats["currency"]},
            )


# ---------------------------------------------------------------------------
# Main export task
# ---------------------------------------------------------------------------

@task(name="Write Excel File")
def write_excel_file(
    products_data: pd.DataFrame,
    output_path: str | None = None,
) -> None:
    """
    Write *products_data* to a formatted Excel workbook.

    Parameters
    ----------
    products_data : pd.DataFrame
        The final processed product DataFrame.
    output_path : str, optional
        Override the default output path from config.
    """
    logger = get_logger(__name__)
    output_path = output_path or PRODUCT_BESTAND_PATH
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Writing Excel file → {output_path}")

    df = products_data.copy()
    df.columns = df.columns.str.strip()

    columns      = list(df.columns)
    max_rows     = len(df) + 1   # +1 for header
    last_excel_row = max(2, max_rows)

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        workbook = writer.book
        formats  = _build_formats(workbook)

        # Write data rows first (no header), then write styled header manually
        df.to_excel(writer, sheet_name=SHEET_NAME, index=False, header=False)
        worksheet = writer.sheets[SHEET_NAME]

        for col_num, header in enumerate(columns):
            worksheet.write(0, col_num, header, formats["_header"])

        # Auto-filter covers the entire data range
        worksheet.autofilter(0, 0, last_excel_row - 1, len(columns) - 1)

        # Column widths + base formats
        _apply_column_widths(worksheet, columns, formats)

        # Conditional formats
        _apply_lager_verfuegbarkeit_cf(worksheet, columns, last_excel_row, formats)
        _apply_seasonal_cf(worksheet, columns, last_excel_row, formats)
        _apply_mixed_currency_cf(worksheet, columns, last_excel_row, formats)

        # Freeze header row so it stays visible while scrolling
        worksheet.freeze_panes(1, 0)

    logger.info("Excel file written successfully")
