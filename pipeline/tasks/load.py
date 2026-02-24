"""
tasks/load.py
=============
All Prefect tasks responsible for loading raw data from CSV / Excel sources.
Each task is self-contained: it reads, does minimal cleaning, and returns a
typed DataFrame (or dict of DataFrames).  No business logic lives here.
"""

from __future__ import annotations

from typing import Dict

import pandas as pd
from prefect import task

from pipeline.config import (
    ar1001_path,
    ar1004_path,
    la1001_path,
    la1002_path,
    la1003_path,
    la1006_path,
    la1008_path,
    la1009_path,
    li1001_path,
    v2ar1005_path,
    wg_path,
)
from pipeline.helpers.run_logging import get_logger
from pipeline.helpers.pricing import determine_current_vkpreis


# ---------------------------------------------------------------------------
# Marketing Stamm
# ---------------------------------------------------------------------------

@task(name="Load Marketing Stamm Data")
def load_marketing_stamm() -> pd.DataFrame:
    """Load AR1001 – the marketing article master."""
    logger = get_logger(__name__)
    logger.info("Loading marketing stamm data")

    df = pd.read_csv(ar1001_path, sep=";", encoding="utf-8")
    df = df[[
        "WM", "NUMMER", "GROESSE", "FARBE",
        "SYS_ANLAGE", "LANUMMER", "BANUMMER", "WARENGR", "LIEFER_STA",
    ]]

    df["BANUMMER"]  = df["BANUMMER"].str.strip()
    df["LANUMMER"]  = df["LANUMMER"].str.strip()
    df["NUMMER"]    = df["NUMMER"].str.strip()
    df["LIEFER_STA"] = df["LIEFER_STA"].map({
        20: "Nachlieferung",
        30: "Absage",
        40: "Ausverkauft",
    })

    logger.info(f"Loaded {len(df):,} marketing stamm records")
    return df


# ---------------------------------------------------------------------------
# Set Artikels  (P-Artikel factor table)
# ---------------------------------------------------------------------------

@task(name="Load Set Artikels")
def load_set_artikels() -> pd.DataFrame:
    """Load V2AR1005 – set article definitions used for FAKTOR computation."""
    logger = get_logger(__name__)
    logger.info("Loading set artikels (V2AR1005)")

    df = pd.read_csv(v2ar1005_path, sep=";", encoding="utf-8", low_memory=False)
    df["NUMMER"]   = df["NUMMER"].astype(str).str.strip()
    df["SETNUMMER"] = df["SETNUMMER"].astype(str).str.strip()
    df = df.rename(columns={"NUMMER": "BANUMMER"})
    df = df[df["SETNUMMER"].str.endswith("P", na=False)]

    keep = [c for c in ["BANUMMER", "SETNUMMER", "FAKTOR"] if c in df.columns]
    df = df[keep].drop_duplicates()

    logger.info(f"Loaded {len(df):,} set artikels")
    return df


# ---------------------------------------------------------------------------
# Katalog (seasonal flags)
# ---------------------------------------------------------------------------

@task(name="Load Katalog Data")
def load_katalog_data() -> pd.DataFrame:
    """Load LA1009 – catalogue / seasonal assignment per article."""
    logger = get_logger(__name__)
    logger.info("Loading katalog data")

    seasonal_cols = [
        "FRUEHJAHR", "SOMMER", "HERBST", "WINTER",
        "FRUEHJ_W1", "FRUEHJ_W2", "FRUEHJ_W3", "FRUEHJ_W4",
        "SOMMER_W1", "SOMMER_W2", "SOMMER_W3", "SOMMER_W4",
        "HERBST_W1", "HERBST_W2", "HERBST_W3", "HERBST_W4",
        "WINTER_W1", "WINTER_W2", "WINTER_W3", "WINTER_W4",
    ]
    df = pd.read_csv(la1009_path, sep=";", encoding="utf-8")
    df = df[["NUMMER"] + seasonal_cols]
    df = df.rename(columns={"NUMMER": "LANUMMER"})
    df["LANUMMER"] = df["LANUMMER"].str.strip()

    logger.info(f"Loaded {len(df):,} katalog records")
    return df


# ---------------------------------------------------------------------------
# Lagerplatz (storage location)
# ---------------------------------------------------------------------------

@task(name="Load Lagerplatz Info")
def load_lagerplatz() -> pd.DataFrame:
    """Load LA1008 – warehouse bin / storage-location data."""
    logger = get_logger(__name__)
    logger.info("Loading lagerplatz data")

    df = pd.read_csv(la1008_path, sep=";")

    def _clean(col: str) -> pd.Series:
        return df[col].fillna("").astype(str).str.strip().str.replace(".0", "", regex=False)

    df["Lagerplatz"] = (
        _clean("ORT") + " " + _clean("BEREICH") + " "
        + _clean("GANG") + " " + _clean("EBENE") + " " + _clean("FACH")
    ).str.strip()

    df = df.rename(columns={"NUMMER": "LANUMMER"})

    logger.info(f"Loaded {len(df):,} lagerplatz records")
    return df[["LANUMMER", "Lagerplatz"]]


# ---------------------------------------------------------------------------
# VK Preis
# ---------------------------------------------------------------------------

@task(name="Load VK Preis Data")
def load_vk_preis() -> pd.DataFrame:
    """Load AR1004 – selling-price master and compute the current active VK price."""
    logger = get_logger(__name__)
    logger.info("Loading VK preis data")

    date_cols = ["VKVALIDD1", "VKBISDT1", "VKVALIDD2", "VKBISDT2"]
    df = pd.read_csv(ar1004_path, sep=";", encoding="utf-8", parse_dates=date_cols)

    # Keep DE/AT price group (null = default DE, "02" = AT)
    df = df[
        df["PREIS_GRP"].isna()
        | df["PREIS_GRP"].str.contains(r"^02$", regex=True, na=False)
    ].copy()

    df["AKTUELLE_VKPREIS"] = df.apply(determine_current_vkpreis, axis=1)
    df["IS_ALTPR"] = pd.to_numeric(df["IS_ALTPR"], errors="coerce")
    df["ALTPR"]    = pd.to_numeric(df["ALTPR"],    errors="coerce")

    df = (
        df[["NUMMER", "AKTUELLE_VKPREIS", "IS_ALTPR", "ALTPR"]]
        .rename(columns={
            "NUMMER":   "BANUMMER",
            "IS_ALTPR": "Alter VK-Preis 1",
            "ALTPR":    "Alter VK-Preis 2",
        })
    )
    df["BANUMMER"] = df["BANUMMER"].str.strip()

    logger.info(f"Processed {len(df):,} VK preis records")
    return df


# ---------------------------------------------------------------------------
# Lager data (stock, names, EK link)
# ---------------------------------------------------------------------------

@task(name="Load Lager Data")
def load_lager_data() -> Dict[str, pd.DataFrame]:
    """
    Load three lager-related CSV files and return them in a dict:
    - lager_stamm:                    LA1001 – current stock levels
    - lager_name:                     LA1002 – article descriptions
    - verbinden_ekpreis_lagerstamm:   LA1003 – EK price / supplier link
    """
    logger = get_logger(__name__)
    logger.info("Loading lager data")

    # Stock master
    lager_stamm = pd.read_csv(la1001_path, sep=";", encoding="utf-8", low_memory=False)

    # Article names
    lager_name = (
        pd.read_csv(la1002_path, sep=";", encoding="utf-8", usecols=["NUMMER", "LANAME1"])
        .rename(columns={"NUMMER": "LANUMMER", "LANAME1": "Beschreibung"})
    )
    lager_name["LANUMMER"] = lager_name["LANUMMER"].str.strip()

    # EK price / supplier link
    ek_link = pd.read_csv(la1003_path, sep=";", encoding="utf-8")
    ek_link["NUMMER"] = ek_link["NUMMER"].str.strip()
    ek_link = ek_link.rename(columns={"NUMMER": "LANUMMER"})
    ek_link["LI_NUMMER"] = (
        ek_link["LI_NUMMER"].astype(str).str.replace(".0", "", regex=False).str.strip()
    )

    logger.info("Successfully loaded all lager data")
    return {
        "lager_stamm":                  lager_stamm,
        "lager_name":                   lager_name,
        "verbinden_ekpreis_lagerstamm": ek_link,
    }


# ---------------------------------------------------------------------------
# Lieferant (supplier master)
# ---------------------------------------------------------------------------

@task(name="Load Lieferant Data")
def load_lieferant_data() -> pd.DataFrame:
    """Load LI1001 – supplier master."""
    logger = get_logger(__name__)
    logger.info("Loading lieferant data")

    df = (
        pd.read_csv(li1001_path, sep=";", encoding="utf-8")[["NUMMER", "NAME"]]
        .rename(columns={"NUMMER": "LI_NUMMER", "NAME": "LIEFERANTNAME"})
    )
    df["LI_NUMMER"] = df["LI_NUMMER"].astype(str).str.replace(".0", "", regex=False).str.strip()

    logger.info(f"Loaded {len(df):,} lieferant records")
    return df


# ---------------------------------------------------------------------------
# Warengruppe (product group / buyer assignment)
# ---------------------------------------------------------------------------

@task(name="Load Warengruppe Data")
def load_warengruppe_data() -> pd.DataFrame:
    """Load the Excel-based Warengruppe mapping (WG_CODE → WG_NAME + Einkäufer)."""
    logger = get_logger(__name__)
    logger.info("Loading warengruppe data")

    df = pd.read_excel(wg_path).rename(columns={"WG_CODE": "WARENGR"})
    df["WARENGR"] = df["WARENGR"].astype(str)

    logger.info(f"Loaded {len(df):,} warengruppe records")
    return df[["WARENGR", "WG_NAME", "Einkäufer"]]


# ---------------------------------------------------------------------------
# Lagerbewegung Historie (warehouse movement history)
# ---------------------------------------------------------------------------

@task(name="Process Lagerbewegung Historie")
def process_lagerbewegung_historie() -> Dict[str, pd.DataFrame]:
    """
    Load LA1006 and derive several pre-aggregated summary tables:

    Returns
    -------
    dict with keys:
        lagerbewegung_historie          – full history (sorted desc by date)
        lagerbewegung_historie_grouped  – last goods-receipt row per article
        gesamt_wareneingang             – total received qty per article
        gesamt_brauchbare_retouren      – total usable returns per article
        gesamt_unbrauchbare_retouren    – total unusable returns per article
    """
    logger = get_logger(__name__)
    logger.info("Processing lagerbewegung historie")

    hist = pd.read_csv(la1006_path, sep=";", encoding="utf-8", low_memory=False)
    hist["NUMMER"] = hist["NUMMER"].str.strip()
    hist = hist.sort_values("BUCHDATUM", ascending=False)

    # --- last goods receipt per article (BUCHKEY 10) ---
    last_receipt = (
        hist[hist["BUCHKEY"] == 10]
        .groupby(["NUMMER", "BUCHDATUM"])["MENGE"]
        .sum()
        .reset_index()
        .drop_duplicates(subset=["NUMMER"], keep="last")
        .rename(columns={
            "NUMMER":   "LANUMMER",
            "BUCHDATUM": "LETZTE_WARENEINGANG",
            "MENGE":    "WARENEINGAG_MENGE",
        })
    )
    last_receipt["LETZTE_WARENEINGANG"] = pd.to_datetime(
        last_receipt["LETZTE_WARENEINGANG"], errors="coerce"
    ).dt.date

    # --- helper: total qty for a single buchkey ---
    def _total(buchkey: int, out_col: str) -> pd.DataFrame:
        return (
            hist[hist["BUCHKEY"] == buchkey]
            .groupby("NUMMER")["MENGE"]
            .sum()
            .reset_index()
            .rename(columns={"NUMMER": "LANUMMER", "MENGE": out_col})
        )

    gesamt_wareneingang        = _total(10, "Gesamt Wareneingang")
    gesamt_brauchbare_retouren = _total(31, "Gesamt Retouren +")
    gesamt_unbrauchbare_retouren = _total(34, "Gesamt Retouren -")

    # Ensure return quantities are positive
    for df, col in [
        (gesamt_brauchbare_retouren, "Gesamt Retouren +"),
        (gesamt_unbrauchbare_retouren, "Gesamt Retouren -"),
    ]:
        df.loc[df[col] < 0, col] *= -1

    logger.info("Successfully processed lagerbewegung historie")
    return {
        "lagerbewegung_historie":         hist,
        "lagerbewegung_historie_grouped": last_receipt,
        "gesamt_wareneingang":            gesamt_wareneingang,
        "gesamt_brauchbare_retouren":     gesamt_brauchbare_retouren,
        "gesamt_unbrauchbare_retouren":   gesamt_unbrauchbare_retouren,
    }
