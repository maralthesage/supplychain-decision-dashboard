"""
tasks/transform.py
==================
The final transformation step: takes the merged wide DataFrame and produces
the clean, analysis-ready output ready for Excel export.

The main Prefect task is process_final_data().
All sub-functions are plain Python so they can be unit-tested independently.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from prefect import task

from pipeline.config import (
    COLUMN_RENAMES,
    FINAL_COLUMNS,
    GA4_PATH,
    PRODUCT_BESTAND_PATH,
)
from pipeline.helpers.analytics import calculate_forecasted_order_month
from pipeline.helpers.classification import (
    WEBSHOP_DISCOUNT_MAP,
    classify_products_vectorized,
)
from pipeline.helpers.p_artikel import merge_p_artikels
from pipeline.helpers.run_logging import get_logger


# ---------------------------------------------------------------------------
# Sub-step 1: core financial metrics
# ---------------------------------------------------------------------------

def _calculate_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute Bestand VK Wert, Marge, EK-Volumen, Gesamt Retouren, Retourenquote.
    Uses VERF_BEST (available stock) not GES_BEST (total stock) for value calcs.
    """
    df = df.copy()

    verf = df["VERF_BEST"].astype(float).fillna(0.0)
    vk   = df["AKTUELLE_VKPREIS"]
    ek   = df["AKTUELLER_EKPREIS"]

    df["Bestand VK Wert"] = verf * vk
    df["Marge"]           = ((vk - ek) / vk).round(4)
    df["EK-Volumen"]      = ek * verf

    ret_plus  = df["Gesamt Retouren +"].astype(float).fillna(0.0)
    ret_minus = df["Gesamt Retouren -"].astype(float).fillna(0.0)
    gesamt_f  = df["TOTAL_MENGE"].astype(float).fillna(0.0)

    df["Gesamt Retouren"]   = ret_plus + ret_minus
    # Keep legacy behaviour from artikel_bestand_automation.py
    # (division uses 0-filled denominator directly, not NA replacement).
    df["Retourenquote (%)"] = df["Gesamt Retouren"].astype(float).fillna(0.0) / gesamt_f

    return df


# ---------------------------------------------------------------------------
# Sub-step 2: KALK_EK temp column (used for MASSNAHME diff column)
# ---------------------------------------------------------------------------

def _add_kalk_ek_temp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a helper column 'temp_kalk_ek':
    → KALK_EK when it is non-null and non-zero, otherwise fall back to EK-Preis.
    This is used only for the 'MASSNAHME NEUER VK minus Kalk EK (EK)' column.
    """
    df = df.copy()
    has_kalk = df["KALK_EK"].notna() & (df["KALK_EK"] != 0)
    df["temp_kalk_ek"] = np.where(has_kalk, df["KALK_EK"], df["EK-Preis"])
    return df


# ---------------------------------------------------------------------------
# Sub-step 3: availability forecast
# ---------------------------------------------------------------------------

def _calculate_forecasts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add 'Lager Verfügbarkeit bis': the estimated month when available stock
    runs out given the average monthly sales rate.
    """
    df = df.copy()
    df["Verfügbarer Lagerbestand"] = df["Verfügbarer Lagerbestand"].astype(float).fillna(0.0)
    df["Durchschnittliche Monatliche Faktura"] = (
        df["Durchschnittliche Monatliche Faktura"].astype(float).fillna(0.0)
    )
    df["Lager Verfügbarkeit bis"] = df.apply(
        lambda row: calculate_forecasted_order_month(
            row["Durchschnittliche Monatliche Faktura"],
            row["Verfügbarer Lagerbestand"],
        ),
        axis=1,
    )
    return df


# ---------------------------------------------------------------------------
# Sub-step 4: GA4 web analytics
# ---------------------------------------------------------------------------

def _merge_ga_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Left-join GA4 daily item-level report onto the product dataframe.
    Missing GA4 data → NaN in GA4 columns (not an error).
    """
    try:
        ga = pd.read_excel(GA4_PATH)
        ga = ga.rename(columns={"itemId": "Marketingartikel Nr."})
        ga["Marketingartikel Nr."] = ga["Marketingartikel Nr."].astype(str).str.strip()
        df = df.merge(ga, on="Marketingartikel Nr.", how="left")
    except FileNotFoundError:
        # GA4 export may not be present on weekends / holidays – just skip
        pass
    return df


# ---------------------------------------------------------------------------
# Sub-step 5: carry forward manually-entered columns from yesterday's file
# ---------------------------------------------------------------------------

def _merge_yesterday_extra_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Load yesterday's Excel output and bring over any columns that were manually
    filled in (e.g. Maßnahmen, Bemerkung, Aktion*, Rückstand Vortag, …) that
    don't exist in today's freshly computed data.

    Only columns NOT already present in df are merged in so computed values
    are never overwritten.
    """
    try:
        yesterday = pd.read_excel(PRODUCT_BESTAND_PATH)
    except FileNotFoundError:
        # First run – no previous file exists yet
        return df

    key = "Marketingartikel Nr."
    extra_cols = [
        col for col in yesterday.columns
        if col not in df.columns and col != key
    ]
    if not extra_cols:
        return df

    extra = yesterday[[key] + extra_cols]
    df = df.merge(extra, on=key, how="left")
    return df


# ---------------------------------------------------------------------------
# Sub-step 6: MASSNAHME columns
# ---------------------------------------------------------------------------

def _apply_massnahmen(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute all four MASSNAHME columns:

    MASSNAHME ERGREIFEN
        Stage label from classify_products_vectorized.
    MASSNAHME REDUZIERUNGEN STUFE 1-4
        Discount rate (float) mapped from stage label.
    MASSNAHME NEUER VK
        Reduced VK price for webshop stages; "prüfen" for Lagerverkauf /
        Vermarkter; "nein" when data is missing.
    MASSNAHME NEUER VK minus Kalk EK (EK)
        Margin check: reduced VK minus the kalkulierter EK.
    """
    df = df.copy()

    df["MASSNAHME ERGREIFEN"] = classify_products_vectorized(
        M=df["EK-Preis"],
        T=df["Letzte Lager Bewegung"],
        X=df["Verfügbarer Lagerbestand"],
    )

    stage = df["MASSNAHME ERGREIFEN"]
    df["MASSNAHME REDUZIERUNGEN STUFE 1-4"] = stage.map(WEBSHOP_DISCOUNT_MAP)

    is_stufe = stage.astype(str).str.contains("Stufe", case=False, na=False)
    df["MASSNAHME NEUER VK"] = np.where(
        is_stufe,
        (1 - df["MASSNAHME REDUZIERUNGEN STUFE 1-4"]) * df["Aktueller VK-Preis"],
        np.nan,
    )

    df["MASSNAHME NEUER VK minus Kalk EK (EK)"] = (
        df["MASSNAHME NEUER VK"] - df["temp_kalk_ek"]
    )

    # Overrides for non-webshop stages
    # This column contains both numbers and labels ("nein"/"prüfen"), so cast
    # before writing strings to avoid float64 assignment errors in recent pandas.
    df["MASSNAHME NEUER VK"] = df["MASSNAHME NEUER VK"].astype("object")

    df.loc[
        stage.str.contains("Spalten M,T,X", case=False, na=False),
        "MASSNAHME NEUER VK",
    ] = "nein"
    df.loc[
        stage.str.contains("Lagerverkauf Krefeld", case=False, na=False),
        "MASSNAHME NEUER VK",
    ] = "prüfen"
    df.loc[
        stage.str.contains("Vermarkter", case=False, na=False),
        "MASSNAHME NEUER VK",
    ] = "prüfen"

    return df


# ---------------------------------------------------------------------------
# Main Prefect task
# ---------------------------------------------------------------------------

@task(name="Process Final Data")
def process_final_data(products: pd.DataFrame) -> pd.DataFrame:
    """
    Orchestrate all transformation sub-steps and return the final DataFrame
    ready for Excel export.

    Pipeline:
        merge_p_artikels → metrics → rename columns → forecast →
        GA4 merge → yesterday columns → MASSNAHME columns →
        select & order final columns
    """
    logger = get_logger(__name__)
    logger.info("Processing final data")

    df = merge_p_artikels(products)
    df = _calculate_metrics(df)
    df = df.rename(columns=COLUMN_RENAMES)
    df = _add_kalk_ek_temp(df)          # needs renamed "EK-Preis" column
    df = _calculate_forecasts(df)       # needs renamed column names
    df = _merge_ga_data(df)
    df = _merge_yesterday_extra_columns(df)
    df = df.drop_duplicates(subset=["Marketingartikel Nr."])
    df = _apply_massnahmen(df)

    # Enforce exact legacy output schema/order from artikel_bestand_automation.py.
    # Missing columns are created as empty so the final table always matches.
    for col in FINAL_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[FINAL_COLUMNS]

    logger.info(f"Final data processed – {len(df):,} rows, {len(df.columns)} columns")
    return df
