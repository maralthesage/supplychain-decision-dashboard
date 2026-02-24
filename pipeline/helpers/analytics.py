"""
helpers/analytics.py
====================
Pure-Python / pandas helpers for sales analytics and inventory forecasting.
No Prefect imports – these are called from Prefect tasks but are themselves
ordinary functions so they can be unit-tested without a Prefect context.
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd


# ---------------------------------------------------------------------------
# Average monthly sales
# ---------------------------------------------------------------------------

def compute_avg_monthly_sales(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute total sold quantity and average monthly sold quantity per LANUMMER.

    The period covered runs from SYS_ANLAGE (article creation date) to the
    last booking date that appears in df.  Only rows with BUCHKEY == 30
    (Faktura) should be passed in – the caller filters before calling.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain: LANUMMER, MENGE, BUCHDATUM, SYS_ANLAGE

    Returns
    -------
    pd.DataFrame with columns:
        LANUMMER, TOTAL_MENGE, NUM_MONATE, Durchschnittliche Monatliche Faktura
    """
    df = df.copy()
    df["BUCHDATUM"] = pd.to_datetime(df["BUCHDATUM"], errors="coerce")
    df["SYS_ANLAGE"] = pd.to_datetime(df["SYS_ANLAGE"], errors="coerce")

    # --- monthly totals per product ---
    df_valid = df.dropna(subset=["MENGE", "BUCHDATUM"]).copy()
    df_valid["MONTH"] = df_valid["BUCHDATUM"].dt.to_period("M")

    monthly = (
        df_valid.groupby(["LANUMMER", "MONTH"])["MENGE"]
        .sum()
        .reset_index()
    )
    total_sales = (
        monthly.groupby("LANUMMER")["MENGE"]
        .sum()
        .reset_index(name="TOTAL_MENGE")
    )

    # --- base info: creation date and last booking per product ---
    base = (
        df.groupby("LANUMMER")
        .agg(SYS_ANLAGE=("SYS_ANLAGE", "first"), LAST_BUCHDATUM=("BUCHDATUM", "max"))
        .reset_index()
    )

    def _num_monate(row: pd.Series) -> int:
        if pd.isna(row["LAST_BUCHDATUM"]) or pd.isna(row["SYS_ANLAGE"]):
            return 0
        return (
            (row["LAST_BUCHDATUM"].to_period("M") - row["SYS_ANLAGE"].to_period("M")).n
            + 1
        )

    base["NUM_MONATE"] = base.apply(_num_monate, axis=1)

    # --- merge and compute average ---
    result = base.merge(total_sales, on="LANUMMER", how="left")
    result["TOTAL_MENGE"] = result["TOTAL_MENGE"].fillna(0)
    result["Durchschnittliche Monatliche Faktura"] = result.apply(
        lambda row: (row["TOTAL_MENGE"] / row["NUM_MONATE"])
        if row["NUM_MONATE"] > 0
        else 0,
        axis=1,
    )

    return result[["LANUMMER", "TOTAL_MENGE", "NUM_MONATE", "Durchschnittliche Monatliche Faktura"]]


# ---------------------------------------------------------------------------
# Last-N-days sum
# ---------------------------------------------------------------------------

def get_last_n_days_sum(
    df: pd.DataFrame,
    buchkeys: int | list[int],
    menge_column_name: str,
    days: int,
    reference_date: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """
    Sum MENGE for the given BUCHKEY(s) over the last *days* days.

    Products that had the BUCHKEY at any point but no activity in the window
    are returned with 0 so that the merge in the caller does not drop them.

    Parameters
    ----------
    df : pd.DataFrame
        Full Lagerbewegung history. Must have: BUCHDATUM, BUCHKEY, NUMMER, MENGE.
    buchkeys : int or list[int]
        One or more booking-key values to include.
    menge_column_name : str
        Name of the output quantity column.
    days : int
        Window length in days (inclusive of reference_date).
    reference_date : pd.Timestamp, optional
        End of the window. Defaults to today.

    Returns
    -------
    pd.DataFrame with columns: LANUMMER, <menge_column_name>
    """
    if isinstance(buchkeys, int):
        buchkeys = [buchkeys]

    reference_date = reference_date or pd.Timestamp.today().normalize()
    start_date = reference_date - pd.Timedelta(days=days)

    df = df.copy()
    df["BUCHDATUM"] = pd.to_datetime(df["BUCHDATUM"], errors="coerce")

    mask_key = df["BUCHKEY"].isin(buchkeys)

    # All products ever seen with these buchkeys (to ensure 0-fill)
    all_lanummer = df.loc[mask_key, "NUMMER"].unique()

    filtered = df[
        mask_key
        & (df["BUCHDATUM"] >= start_date)
        & (df["BUCHDATUM"] <= reference_date)
    ]

    result = (
        filtered.groupby("NUMMER")["MENGE"]
        .sum()
        .reindex(all_lanummer, fill_value=0)
        .reset_index()
        .rename(columns={"NUMMER": "LANUMMER", "MENGE": menge_column_name})
    )

    return result


# ---------------------------------------------------------------------------
# Inventory forecast
# ---------------------------------------------------------------------------

def calculate_forecasted_order_month(
    monthly_sales_avg: float,
    current_stock: float,
    max_months: int = 120,
) -> str:
    """
    Estimate in which calendar month the current stock will be exhausted.

    Returns
    -------
    str
        - "N/A" when data is missing or consumption is zero / negative.
        - "> {max_months} Monate" when the stock lasts beyond the horizon.
        - "YYYY-MM" string of the forecast month otherwise.
        - "Datum zu weit in der Zukunft" on overflow edge cases.
    """
    if (
        pd.isna(monthly_sales_avg)
        or pd.isna(current_stock)
        or monthly_sales_avg <= 0
        or current_stock <= 0
    ):
        return "N/A"

    months_remaining = current_stock / monthly_sales_avg

    if months_remaining > max_months:
        return f"> {max_months} Monate"

    try:
        today = datetime.today()
        offset = int(months_remaining)  # truncate → 0 means stock runs out this month
        total_month = today.month + offset - 1
        forecast_year = today.year + total_month // 12
        forecast_month = total_month % 12 + 1
        return f"{forecast_year}-{forecast_month:02d}"
    except OverflowError:
        return "Datum zu weit in der Zukunft"
