"""
tasks/merge.py
==============
Prefect tasks that join the individual loaded datasets into one wide DataFrame
and compute cross-source analytics (monthly averages, rolling windows, etc.).
"""

from __future__ import annotations

from typing import Dict

import pandas as pd
from prefect import task

from pipeline.helpers.analytics import compute_avg_monthly_sales, get_last_n_days_sum
from pipeline.helpers.run_logging import get_logger


# ---------------------------------------------------------------------------
# Analytics computation
# ---------------------------------------------------------------------------

@task(name="Calculate Analytics")
def calculate_analytics(
    lagerbewegung_historie: pd.DataFrame,
    lager_stamm: pd.DataFrame,
) -> Dict[str, pd.DataFrame]:
    """
    Derive all time-series analytics from the full warehouse-movement history.

    Returns
    -------
    dict with keys:
        faktura_monatlich_analytik_fertig   – average monthly sales per article
        faktura_30d / brauchbare_30d / unbrauchbare_30d / lagerstorno_30d
        faktura_12M / brauchbare_12M / unbrauchbare_12M / lagerstorno_12M
    """
    logger = get_logger(__name__)
    logger.info("Calculating analytics")

    hist = lagerbewegung_historie.copy()
    hist["BUCHDATUM"] = pd.to_datetime(hist["BUCHDATUM"], errors="coerce")

    # --- average monthly faktura ---
    faktura_rows = hist[hist["BUCHKEY"] == 30].rename(columns={"NUMMER": "LANUMMER"})
    faktura_with_anlage = lager_stamm[["LANUMMER", "SYS_ANLAGE"]].merge(
        faktura_rows[["LANUMMER", "MENGE", "BUCHDATUM"]],
        on="LANUMMER",
        how="left",
    )
    monthly_avg = compute_avg_monthly_sales(faktura_with_anlage)
    monthly_avg["Durchschnittliche Monatliche Faktura"] = monthly_avg[
        "Durchschnittliche Monatliche Faktura"
    ].round(2)

    # --- rolling-window sums ---
    # rename NUMMER → LANUMMER is handled inside get_last_n_days_sum (uses NUMMER col)
    def _win(buchkeys, col, days):
        return get_last_n_days_sum(hist, buchkeys, col, days)

    logger.info("Successfully calculated analytics")
    return {
        "faktura_monatlich_analytik_fertig": monthly_avg,
        # 30-day windows
        "faktura_30d":       _win(30,       "Letzte 30T Faktura",                30),
        "brauchbare_30d":    _win(31,       "Letzte 30T Brauchbare Retouren",    30),
        "unbrauchbare_30d":  _win(34,       "Letzte 30T Unbrauchbare Retouren",  30),
        "lagerstorno_30d":   _win([35, 36], "Letzte 30T Lagerstorno",            30),
        # 12-month windows
        "faktura_12M":       _win(30,       "Letzte 12M Faktura",               365),
        "brauchbare_12M":    _win(31,       "Letzte 12M Brauchbare Retouren",   365),
        "unbrauchbare_12M":  _win(34,       "Letzte 12M Unbrauchbare Retouren", 365),
        "lagerstorno_12M":   _win([35, 36], "Letzte 12M Lagerstorno",           365),
    }


# ---------------------------------------------------------------------------
# Main merge
# ---------------------------------------------------------------------------

@task(name="Merge All Data")
def merge_all_data(
    marketing_stamm: pd.DataFrame,
    katalog_data: pd.DataFrame,
    vk_preis: pd.DataFrame,
    lager_data: Dict[str, pd.DataFrame],
    lieferant_info: pd.DataFrame,
    set_artikels: pd.DataFrame,
    warengruppe: pd.DataFrame,
    lagerbewegung_data: Dict[str, pd.DataFrame],
    analytics: Dict[str, pd.DataFrame],
    lagerplatz: pd.DataFrame,
) -> pd.DataFrame:
    """
    Join all loaded and pre-processed data sources into a single wide DataFrame.

    Merge order (all left joins unless noted):
    1.  Lager stamm  ← lieferant info ← EK-price link
    2.  Lager stamm  ← rolling-window analytics (8 tables)
    3.  Lager stamm  ← lagerbewegung summaries (4 tables)
    4.  Lager stamm  ← lager name, lagerplatz, katalog
    5.  Marketing stamm ← VK preis ← set artikels ← lager (inner join)
    6.  ← warengruppe
    7.  ← monthly avg analytics
    8.  EKG / HG / JG flag assignment
    """
    logger = get_logger(__name__)
    logger.info("Merging all data sources")

    # 1. Lieferant info joined to EK link
    lieferant_merged = (
        lager_data["verbinden_ekpreis_lagerstamm"][["LANUMMER", "LI_NUMMER"]]
        .merge(lieferant_info, on="LI_NUMMER", how="left")
    )

    # 2. Build lager core: stock + supplier
    lager_core = lager_data["lager_stamm"][[
        "LANUMMER", "GES_BEST", "VERF_BEST", "BEST_GES_M",
        "RCK_GES_BE", "EK_PREIS", "LAST_BEWEG", "KALK_EK",
    ]].merge(lieferant_merged, on="LANUMMER", how="left")

    # 3. Rolling-window analytics
    for key in [
        "faktura_30d", "brauchbare_30d", "unbrauchbare_30d", "lagerstorno_30d",
        "faktura_12M", "brauchbare_12M", "unbrauchbare_12M", "lagerstorno_12M",
    ]:
        lager_core = lager_core.merge(analytics[key], on="LANUMMER", how="left")

    # 4. Lagerbewegung summaries
    for key in [
        "lagerbewegung_historie_grouped",
        "gesamt_wareneingang",
        "gesamt_brauchbare_retouren",
        "gesamt_unbrauchbare_retouren",
    ]:
        lager_core = lager_core.merge(lagerbewegung_data[key], on="LANUMMER", how="left")

    # 5. Article names, storage location, seasonal catalogue
    lager_core = (
        lager_core
        .merge(lager_data["lager_name"], on="LANUMMER", how="left")
        .merge(lagerplatz,              on="LANUMMER", how="left")
        .merge(katalog_data,            on="LANUMMER", how="left")
        .drop_duplicates(subset="LANUMMER")
        .rename(columns={"EK_PREIS": "AKTUELLER_EKPREIS"})
    )

    # 6. Marketing stamm ← VK preis ← set artikels
    marketing_with_prices = (
        marketing_stamm
        .merge(vk_preis,                          on="BANUMMER", how="left")
        .merge(set_artikels[["BANUMMER", "FAKTOR"]], on="BANUMMER", how="left")
    )

    # 7. Inner join marketing ← lager  (only articles present in both)
    products = (
        marketing_with_prices
        .merge(lager_core, on="LANUMMER", how="inner")
        .drop_duplicates(subset=["BANUMMER", "LANUMMER"])
    )

    # 8. Warengruppe
    products["WARENGR"] = products["WARENGR"].astype(str).str.replace(".0", "", regex=False)
    products = (
        products
        .merge(warengruppe, on="WARENGR", how="left")
        .drop_duplicates(subset="LANUMMER")
    )

    # 9. EKG flag: "JG" for articles whose NUMMER matches the ^\d+H[A-Z]\d+ pattern
    products.loc[
        products["NUMMER"].str.contains(r"^\d+H[A-Z]\d+", na=False, regex=True),
        "EKG",
    ] = "JG"
    products.loc[products["EKG"].isna(), "EKG"] = "HG"

    # 10. Monthly avg analytics
    products = (
        products
        .merge(analytics["faktura_monatlich_analytik_fertig"], on="LANUMMER", how="left")
        .drop_duplicates(subset="BANUMMER")
    )

    logger.info(f"Merge complete – {len(products):,} product records")
    return products
