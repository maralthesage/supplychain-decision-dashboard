import pandas as pd
import numpy as np
import shutil
import os
from typing import Dict, Any
from xlsxwriter.utility import xl_col_to_name
from prefect import task, flow
from prefect.logging import get_run_logger
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
from artikel_automation_helper import (
    determine_current_vkpreis,
    merge_p_artikels,
    compute_avg_monthly_sales,
    get_last_n_days_sum,
    calculate_forecasted_order_month,
    classify_products_vectorized,
    webshop_discount,
    calculate_neuer_vk)
from artikel_automation_config import *




### =============== PATHS =============== ###
today = datetime.today().date()
yesterday = today - timedelta(days=1)

PRODUCT_BESTAND_PATH = fr"C:/Users/python-user/OneDrive - Hagen Grote GmbH/Teamwebsite - Artikel Bestand Taeglich/produkt_bestand.xlsx"
PRODUCT_BESTAND_HISTORIE = fr"C:/Users/python-user/OneDrive - Hagen Grote GmbH/Teamwebsite - Artikel Bestand Taeglich/Historie/produkt_bestand_{today}.xlsx"
PRODUCT_BESTAND_LOCAL_BACKUP = fr'C:/Python/7-product-bestand/backups/product_bestand_backup_{yesterday}.xlsx'
### ===================================== ###

### =============== LOAD MARKETING STAMM DATA =============== ###
@task(name="Load Marketing Stamm Data")
def load_marketing_stamm() -> pd.DataFrame:
    """Load and process marketing stamm data"""
    logger = get_run_logger()

    try:
        logger.info("Loading marketing stamm data")
        marketing_stamm = pd.read_csv(ar1001_path, sep=";", encoding="utf-8")
        marketing_stamm = marketing_stamm[
            [
                "WM",
                "NUMMER",
                "GROESSE",
                "FARBE",
                "SYS_ANLAGE",
                "LANUMMER",
                "BANUMMER",
                "WARENGR",
                "LIEFER_STA"
            ]
        ]

        # Clean string columns
        marketing_stamm["BANUMMER"] = marketing_stamm["BANUMMER"].str.strip()
        marketing_stamm["LANUMMER"] = marketing_stamm["LANUMMER"].str.strip()
        marketing_stamm["NUMMER"] = marketing_stamm["NUMMER"].str.strip()
        marketing_stamm['LIEFER_STA'] = marketing_stamm['LIEFER_STA'].map({20:'Nachlieferung',30:'Absage', 40:'Ausverkauft'})

        logger.info(f"Loaded {len(marketing_stamm)} marketing stamm records")
        return marketing_stamm

    except Exception as e:
        logger.error(f"Error loading marketing stamm data: {str(e)}")
        raise
### ================================================= ###

### =============== LOAD SET ARTIKEL DATA =============== ###
@task(name="Load Set Artikels")
def load_set_artikels() -> pd.DataFrame:
    """Load and preprocess set artikels (V2AR1005) used to compute *_FAKTORED columns."""
    logger = get_run_logger()
    try:
        logger.info("Loading set artikels (V2AR1005)")

        # Read CSV exactly like in your notebook (encoding/sep)
        df = pd.read_csv(v2ar1005_path, sep=';', encoding='utf-8', low_memory=False)
        df['NUMMER'] = df['NUMMER'].astype(str).str.strip()
        df['SETNUMMER'] = df['SETNUMMER'].astype(str).str.strip()
        df = df.rename(columns={'NUMMER': 'BANUMMER'})
        df = df[df['SETNUMMER'].str.endswith('P', na=False)]

        # Keep only what we need; FAKTOR is used for math
        keep_cols = [c for c in ['BANUMMER', 'SETNUMMER', 'FAKTOR'] if c in df.columns]
        df = df[keep_cols].drop_duplicates()

        logger.info(f"Loaded {len(df)} set artikels")
        return df

    except Exception as e:
        logger.error(f"Error loading set artikels: {e}")
        raise
### ================================================= ###


### =============== LOAD KATALOG DATA =============== ###
@task(name="Load Katlalog Data")
def load_katalog_data() -> pd.DataFrame:
    """Load and process marketing stamm data"""
    logger = get_run_logger()

    try:
        logger.info("Loading marketing stamm data")
        la1009 = pd.read_csv(la1009_path, sep=";", encoding="utf-8")
        la1009 = la1009[
            [
                "NUMMER",
                "FRUEHJAHR",
                "SOMMER",
                "HERBST",
                "WINTER",
                "FRUEHJ_W1",
                "FRUEHJ_W2",
                "FRUEHJ_W3",
                "FRUEHJ_W4",
                "SOMMER_W1",
                "SOMMER_W2",
                "SOMMER_W3",
                "SOMMER_W4",
                "HERBST_W1",
                "HERBST_W2",
                "HERBST_W3",
                "HERBST_W4",
                "WINTER_W1",
                "WINTER_W2",
                "WINTER_W3",
                "WINTER_W4",
            ]
        ]

        # Clean string columns
        la1009 = la1009.rename(columns={"NUMMER": "LANUMMER"})
        la1009["LANUMMER"] = la1009["LANUMMER"].str.strip()

        logger.info(f"Loaded {len(la1009)} Katalog Daten records")
        return la1009

    except Exception as e:
        logger.error(f"Error loading Katalog Daten : {str(e)}")
        raise
### ==================================================== ###


### =============== LOAD LAGERPLATZ DATA =============== ###
@task(name='Load Lagerplatz Info')
def load_lagerplatz():
    logger = get_run_logger()

    try:
        logger.info('Loading Lagerplatz info')
        lagerplatz = pd.read_csv(la1008_path, sep=';')
        lagerplatz['Lagerplatz'] = (lagerplatz['ORT'].fillna("").astype(str).str.strip().str.replace(".0","") + " " +\
                                   lagerplatz['BEREICH'].fillna("").astype(str).str.strip().str.replace(".0","") + " " + \
                                   lagerplatz['GANG'].fillna("").astype(str).str.strip().str.replace(".0","") + " " + \
                                   lagerplatz['EBENE'].fillna("").astype(str).str.strip().str.replace(".0","") + " " + \
                                   lagerplatz['FACH'].fillna("").astype(str).str.strip().str.replace(".0","")).str.strip()
        lagerplatz = lagerplatz.rename(columns={'NUMMER':'LANUMMER'})

    except Exception as e:
        logger.error(f"Error loading Lagerplatz data: {str(e)}")
        raise
    return lagerplatz[['LANUMMER','Lagerplatz']]
### ==================================================== ###


### =============== LOAD VK PREIS DATA =============== ###
@task(name="Load VK Preis Data")
def load_vk_preis() -> pd.DataFrame:
    """Load and process VK preis data"""
    logger = get_run_logger()

    try:
        logger.info("Loading VK preis data")
        vk_preis = pd.read_csv(
            ar1004_path,
            sep=";",
            encoding="utf-8",
            parse_dates=[
                "VKVALIDD1",
                "VKBISDT1",
                "VKVALIDD2",
                "VKBISDT2"

            ],
        )

        vk_preis_de_at = vk_preis[
            (vk_preis["PREIS_GRP"].isna())
            | (vk_preis["PREIS_GRP"].str.contains(r"^02$", regex=True, na=False))
        ]
        

        vk_preis_de_at["AKTUELLE_VKPREIS"] = vk_preis_de_at.apply(
            determine_current_vkpreis, axis=1
        )
        vk_preis_de_at['IS_ALTPR'] = pd.to_numeric(vk_preis_de_at['IS_ALTPR'],errors='coerce')
        vk_preis_de_at['ALTPR'] = pd.to_numeric(vk_preis_de_at['ALTPR'],errors='coerce')

        vk_preis_de_at = vk_preis_de_at[
            ["NUMMER", "AKTUELLE_VKPREIS", "IS_ALTPR", "ALTPR"]
        ].rename(
            columns={
                "NUMMER": "BANUMMER",
                "IS_ALTPR": "Alter VK-Preis 1",
                "ALTPR": "Alter VK-Preis 2",
            }
        )
        vk_preis_de_at["BANUMMER"] = vk_preis_de_at["BANUMMER"].str.strip()

        logger.info(f"Processed {len(vk_preis_de_at)} VK preis records")
        return vk_preis_de_at

    except Exception as e:
        logger.error(f"Error loading VK preis data: {str(e)}")
        raise

### ==================================================== ###


### =============== LOAD LAGER DATA =============== ###
@task(name="Load Lager Data")
def load_lager_data() -> Dict[str, pd.DataFrame]:
    """Load and process all lager-related data"""
    logger = get_run_logger()

    try:
        logger.info("Loading lager data")

        # Load lager stamm
        lager_stamm = pd.read_csv(
            la1001_path, sep=";", encoding="utf-8", low_memory=False
        )

        # Load lager name
        lager_name = pd.read_csv(
            la1002_path,
            sep=";",
            encoding="utf-8",
            usecols=["NUMMER", "LANAME1"],
        )
        lager_name = lager_name.rename(
            columns={"NUMMER": "LANUMMER", "LANAME1": "Beschreibung"}
        )
        lager_name["LANUMMER"] = lager_name["LANUMMER"].str.strip()

        # Load verbinden ekpreis lagerstamm
        verbinden_ekpreis_lagerstamm = pd.read_csv(
            la1003_path, sep=";", encoding="utf-8"
        )
        verbinden_ekpreis_lagerstamm["NUMMER"] = verbinden_ekpreis_lagerstamm[
            "NUMMER"
        ].str.strip()
        verbinden_ekpreis_lagerstamm = verbinden_ekpreis_lagerstamm.rename(
            columns={"NUMMER": "LANUMMER"}
        )
        verbinden_ekpreis_lagerstamm["LI_NUMMER"] = (
            verbinden_ekpreis_lagerstamm["LI_NUMMER"]
            .astype(str)
            .str.replace(".0", "")
            .str.strip()
        )

        logger.info("Successfully loaded all lager data")
        return {
            "lager_stamm": lager_stamm,
            "lager_name": lager_name,
            "verbinden_ekpreis_lagerstamm": verbinden_ekpreis_lagerstamm,
        }

    except Exception as e:
        logger.error(f"Error loading lager data: {str(e)}")
        raise
### ==================================================== ###


### =============== LOAD LIEFERANTEN DATA =============== ###
@task(name="Load Lieferant Data")
def load_lieferant_data() -> pd.DataFrame:
    """Load and process lieferant data"""
    logger = get_run_logger()

    try:
        logger.info("Loading lieferant data")
        lieferant_info = pd.read_csv(li1001_path, sep=";", encoding="utf-8")
        lieferant_info = lieferant_info[["NUMMER", "NAME"]].rename(
            columns={"NUMMER": "LI_NUMMER", "NAME": "LIEFERANTNAME"}
        )
        lieferant_info["LI_NUMMER"] = (
            lieferant_info["LI_NUMMER"].astype(str).str.replace(".0", "").str.strip()
        )

        logger.info(f"Loaded {len(lieferant_info)} lieferant records")
        return lieferant_info

    except Exception as e:
        logger.error(f"Error loading lieferant data: {str(e)}")
        raise
### ==================================================== ###


### =============== LOAD WARENGRUPPE DATA =============== ###
@task(name="Load Warengruppe Data")
def load_warengruppe_data() -> pd.DataFrame:
    """Load and process warengruppe data"""
    logger = get_run_logger()

    try:
        logger.info("Loading warengruppe data")
        warengruppe = pd.read_excel(
            wg_path,
        )
        warengruppe = warengruppe.rename(
            columns={"WG_CODE": "WARENGR"}
        )
        warengruppe['WARENGR'] = warengruppe['WARENGR'].astype(str)

        logger.info(f"Loaded {len(warengruppe)} warengruppe records")
        return warengruppe[['WARENGR','WG_NAME','Einkäufer']]

    except Exception as e:
        logger.error(f"Error loading warengruppe data: {str(e)}")
        raise

### ==================================================== ###


### =============== LOAD LAGERBEWEGUNG HISTORIE DATA =============== ###
@task(name="Process Lagerbewegung Historie")
def process_lagerbewegung_historie() -> Dict[str, pd.DataFrame]:
    """Process lagerbewegung historie data"""
    logger = get_run_logger()

    try:
        logger.info("Processing lagerbewegung historie")

        lagerbewegung_historie = pd.read_csv(la1006_path, sep=";", encoding="utf-8",low_memory=False)
        lagerbewegung_historie["NUMMER"] = lagerbewegung_historie["NUMMER"].str.strip()
        lagerbewegung_historie = lagerbewegung_historie.sort_values(
            "BUCHDATUM", ascending=False
        )

        # Process grouped data
        lagerbewegung_historie_grouped = (
            lagerbewegung_historie[lagerbewegung_historie["BUCHKEY"] == 10]
            .groupby(["NUMMER", "BUCHDATUM"])
            .agg({"MENGE": "sum"})
            .reset_index()
        )
        lagerbewegung_historie_grouped = lagerbewegung_historie_grouped.drop_duplicates(
            subset=["NUMMER"], keep="last"
        )
        lagerbewegung_historie_grouped = lagerbewegung_historie_grouped.rename(
            columns={
                "NUMMER": "LANUMMER",
                "BUCHDATUM": "LETZTE_WARENEINGANG",
                "MENGE": "WARENEINGAG_MENGE",
            }
        )
        lagerbewegung_historie_grouped["LETZTE_WARENEINGANG"] = pd.to_datetime(
            lagerbewegung_historie_grouped["LETZTE_WARENEINGANG"], errors="coerce"
        ).dt.date

        # Gesamt wareneingang
        gesamt_wareneingang = (
            lagerbewegung_historie[lagerbewegung_historie["BUCHKEY"] == 10]
            .groupby(["NUMMER"])
            .agg({"MENGE": "sum"})
            .reset_index()
        )
        gesamt_wareneingang = gesamt_wareneingang.rename(
            columns={"NUMMER": "LANUMMER", "MENGE": "Gesamt Wareneingang"}
        )

        # Brauchbare Retouren
        gesamt_brauchbare_retouren = (
            lagerbewegung_historie[lagerbewegung_historie["BUCHKEY"] == 31]
            .groupby(["NUMMER"])
            .agg({"MENGE": "sum"})
            .reset_index()
        )
        gesamt_brauchbare_retouren = gesamt_brauchbare_retouren.rename(
            columns={"NUMMER": "LANUMMER", "MENGE": "Gesamt Retouren +"}
        )

        # Unbrauchbare Retouren
        gesamt_unbrauchbare_retouren = (
            lagerbewegung_historie[lagerbewegung_historie["BUCHKEY"] == 34]
            .groupby(["NUMMER"])
            .agg({"MENGE": "sum"})
            .reset_index()
        )
        gesamt_unbrauchbare_retouren = gesamt_unbrauchbare_retouren.rename(
            columns={"NUMMER": "LANUMMER", "MENGE": "Gesamt Retouren -"}
        )

        # Fix negative values
        gesamt_brauchbare_retouren.loc[
            gesamt_brauchbare_retouren["Gesamt Retouren +"] < 0, "Gesamt Retouren +"
        ] *= -1
        gesamt_unbrauchbare_retouren.loc[
            gesamt_unbrauchbare_retouren["Gesamt Retouren -"] < 0, "Gesamt Retouren -"
        ] *= -1

        logger.info("Successfully processed lagerbewegung historie")
        return {
            "lagerbewegung_historie": lagerbewegung_historie,
            "lagerbewegung_historie_grouped": lagerbewegung_historie_grouped,
            "gesamt_wareneingang": gesamt_wareneingang,
            "gesamt_brauchbare_retouren": gesamt_brauchbare_retouren,
            "gesamt_unbrauchbare_retouren": gesamt_unbrauchbare_retouren,
        }

    except Exception as e:
        logger.error(f"Error processing lagerbewegung historie: {str(e)}")
        raise
### ==================================================== ###


### =============== CALCULATE ANALYTICS =============== ###

@task(name="Calculate Analytics")
def calculate_analytics(
    lagerbewegung_historie: pd.DataFrame, lager_stamm: pd.DataFrame
) -> Dict[str, pd.DataFrame]:
    """Calculate various analytics from lagerbewegung historie"""
    logger = get_run_logger()

    try:
        logger.info("Calculating analytics")

        # Convert date column
        lagerbewegung_historie["BUCHDATUM"] = pd.to_datetime(
            lagerbewegung_historie["BUCHDATUM"], errors="coerce"
        )

        # Monthly analytics
        faktura_monatlich_analytik = lagerbewegung_historie[
            lagerbewegung_historie["BUCHKEY"] == 30
        ].rename(columns={"NUMMER": "LANUMMER"})
        # faktura_monatlich_analytik["ORT"] = (
        #     faktura_monatlich_analytik["ORT"].fillna("").astype(str).str.strip().str.replace(".0","")
        # )
        # faktura_monatlich_analytik["BEREICH"] = (
        #     faktura_monatlich_analytik["BEREICH"].fillna("").astype(str).str.strip().str.replace(".0","")
        # )
        # faktura_monatlich_analytik["STELLPLATZ"] = (
        #     faktura_monatlich_analytik["STELLPLATZ"].fillna("").astype(str).str.strip().str.replace(".0","")
        # )
        # faktura_monatlich_analytik["Lagerplatz"] = (
        #     faktura_monatlich_analytik["ORT"]
        #     + " "
        #     + faktura_monatlich_analytik["BEREICH"]
        #     + " "
        #     + faktura_monatlich_analytik["STELLPLATZ"]
        # )
        # faktura_monatlich_analytik["Lagerplatz"] = faktura_monatlich_analytik[
        #     "Lagerplatz"
        # ].str.strip()
        faktura_monatlich_analytik = lager_stamm[["LANUMMER", "SYS_ANLAGE"]].merge(
            faktura_monatlich_analytik[
                ["LANUMMER", "MENGE", "BUCHDATUM"]
                # ["LANUMMER", "MENGE", "BUCHDATUM", "Lagerplatz"]
            ],
            on="LANUMMER",
            how="left",
        )

        faktura_monatlich_analytik_fertig = compute_avg_monthly_sales(
            faktura_monatlich_analytik
        )
        faktura_monatlich_analytik_fertig["AVG_MONAT_MENGE"] = (
            faktura_monatlich_analytik_fertig["AVG_MONAT_MENGE"].round(2)
        )
        faktura_monatlich_analytik_fertig = faktura_monatlich_analytik_fertig.rename(
            columns={"AVG_MONAT_MENGE": "Durchschnittliche Monatliche Faktura"}
        )

        # 30-day analytics
        faktura_30d = get_last_n_days_sum(
            lagerbewegung_historie, 30, "Letzte 30T Faktura", 30
        )
        brauchbare_30d = get_last_n_days_sum(
            lagerbewegung_historie, 31, "Letzte 30T Brauchbare Retouren", 30
        )
        unbrauchbare_30d = get_last_n_days_sum(
            lagerbewegung_historie, 34, "Letzte 30T Unbrauchbare Retouren", 30
        )
        lagerstorno_30d = get_last_n_days_sum(
            lagerbewegung_historie, [35, 36], "Letzte 30T Lagerstorno", 30
        )

        # 12-month analytics
        faktura_12M = get_last_n_days_sum(
            lagerbewegung_historie, 30, "Letzte 12M Faktura", 365
        )
        brauchbare_12M = get_last_n_days_sum(
            lagerbewegung_historie, 31, "Letzte 12M Brauchbare Retouren", 365
        )
        unbrauchbare_12M = get_last_n_days_sum(
            lagerbewegung_historie, 34, "Letzte 12M Unbrauchbare Retouren", 365
        )
        lagerstorno_12M = get_last_n_days_sum(
            lagerbewegung_historie, [35, 36], "Letzte 12M Lagerstorno", 365
        )

        logger.info("Successfully calculated analytics")
        return {
            "faktura_monatlich_analytik_fertig": faktura_monatlich_analytik_fertig,
            "faktura_30d": faktura_30d,
            "brauchbare_30d": brauchbare_30d,
            "unbrauchbare_30d": unbrauchbare_30d,
            "lagerstorno_30d": lagerstorno_30d,
            "faktura_12M": faktura_12M,
            "brauchbare_12M": brauchbare_12M,
            "unbrauchbare_12M": unbrauchbare_12M,
            "lagerstorno_12M": lagerstorno_12M,
        }

    except Exception as e:
        logger.error(f"Error calculating analytics: {str(e)}")
        raise
### ==================================================== ###


### =============== MERGE ALL DATA =============== ###

@task(name="Merge All Data")
def merge_all_data(
    marketing_stamm: pd.DataFrame,
    katalog_data: pd.DataFrame,
    vk_preis_de_at: pd.DataFrame,
    lager_data: Dict[str, pd.DataFrame],
    lieferant_info: pd.DataFrame,
    set_artikels:pd.DataFrame,
    warengruppe: pd.DataFrame,
    lagerbewegung_data: Dict[str, pd.DataFrame],
    analytics: Dict[str, pd.DataFrame],
    lagerplatz: pd.DataFrame
) -> pd.DataFrame:
    """Merge all data sources into final dataset"""
    logger = get_run_logger()

    try:
        logger.info("Merging all data")

        # Merge lieferant info
        lieferant_info_merged = lager_data["verbinden_ekpreis_lagerstamm"][
            ["LANUMMER", "LI_NUMMER"]
        ].merge(lieferant_info, on="LI_NUMMER", how="left")

        # Create alle_lager_info
        alle_lager_info = lager_data["lager_stamm"][
            ["LANUMMER", "GES_BEST", "VERF_BEST","BEST_GES_M", "RCK_GES_BE", "EK_PREIS", "LAST_BEWEG","KALK_EK"]
        ].merge(lieferant_info_merged, on="LANUMMER", how="left")

        # Merge analytics
        for key in [
            "faktura_30d",
            "brauchbare_30d",
            "unbrauchbare_30d",
            "lagerstorno_30d",
            "faktura_12M",
            "brauchbare_12M",
            "unbrauchbare_12M",
            "lagerstorno_12M",
        ]:
            alle_lager_info = alle_lager_info.merge(
                analytics[key], on="LANUMMER", how="left"
            )

        # Merge lagerbewegung data
        for key in [
            "lagerbewegung_historie_grouped",
            "gesamt_wareneingang",
            "gesamt_brauchbare_retouren",
            "gesamt_unbrauchbare_retouren",
        ]:
            alle_lager_info = alle_lager_info.merge(
                lagerbewegung_data[key], on="LANUMMER", how="left"
            )

        alle_lager_info = alle_lager_info.merge(
            lager_data["lager_name"], on="LANUMMER", how="left"
        )
        alle_lager_info = alle_lager_info.merge(lagerplatz, on="LANUMMER", how="left")
        alle_lager_info = alle_lager_info.merge(katalog_data, on="LANUMMER", how="left")
        alle_lager_info = alle_lager_info.drop_duplicates(subset="LANUMMER")
        alle_lager_info = alle_lager_info.rename(
            columns={"EK_PREIS": "AKTUELLER_EKPREIS"}
        )

        # Merge with marketing data
        marketingstamm_vkpreis = marketing_stamm.merge(
            vk_preis_de_at, on="BANUMMER", how="left"
        )
        ## NEW ADDED
        marketingstamm_vkpreis_set = marketingstamm_vkpreis.merge(
            set_artikels[['BANUMMER','FAKTOR']], on="BANUMMER", how="left"
        )
        lager_marketing_info = marketingstamm_vkpreis_set.merge(
            alle_lager_info, on="LANUMMER", how="inner"
        ).drop_duplicates(subset=["BANUMMER", "LANUMMER"])

        lager_marketing_info["WARENGR"] = (
            lager_marketing_info["WARENGR"].astype(str).str.replace(".0", "")
        )
        lager_marketing_info = lager_marketing_info.merge(
            warengruppe, on="WARENGR", how="left"
        )
        lager_marketing_info = lager_marketing_info.drop_duplicates(subset=["LANUMMER"])

        # Set EKG
        lager_marketing_info.loc[
            lager_marketing_info["NUMMER"].str.contains(r"^\d+H[A-Z]\d+", na=False),
            "EKG",
        ] = "JG"
        lager_marketing_info.loc[lager_marketing_info["EKG"].isna(), "EKG"] = "HG"

        # Final merge with analytics
        products = lager_marketing_info.merge(
            analytics["faktura_monatlich_analytik_fertig"], on="LANUMMER", how="left"
        )
        products = products.drop_duplicates(subset="BANUMMER")

        logger.info(
            f"Successfully merged data, final dataset has {len(products)} records"
        )
        return products

    except Exception as e:
        logger.error(f"Error merging data: {str(e)}")
        raise
### ==================================================== ###


### =============== PROCESS FINAL DATA =============== ###

@task(name="Process Final Data")
def process_final_data(products: pd.DataFrame) -> pd.DataFrame:
    """Process final data calculations and transformations"""
    logger = get_run_logger()

    try:
        logger.info("Processing final data")

        # Merge P artikels
        products_partikel_merged = merge_p_artikels(products)

        # Calculate values ## NEW --> from GES_BEST changed to VERF_BEST
        products_partikel_merged["Bestand VK Wert"] = (
            products_partikel_merged["VERF_BEST"].astype(float).fillna(0.0)
            * products_partikel_merged["AKTUELLE_VKPREIS"]
        )

        products_partikel_merged["Marge"] = round(
            (
                (products_partikel_merged["AKTUELLE_VKPREIS"] - products_partikel_merged["AKTUELLER_EKPREIS"])
                / products_partikel_merged["AKTUELLE_VKPREIS"]
            ),
            4,
        )

        # Rename columns
        column_renames = {
            "GES_BEST": "Gesamt Lagerbestand",
            "VERF_BEST": "Verfügbarer Lagerbestand",
            "RCK_GES_BE":"Rückstand in Stück",
            "BEST_GES_M":"offene Bestellungen",
            "LAST_BEWEG": "Letzte Lager Bewegung",
            "LI_NUMMER": "Lieferanten Nr.",
            "LIEFERANTNAME": "Lieferanten Name",
            "LETZTE_FAKTURA_MONAT": "Letzte Faktura Datum",
            "LETZTE_WARENEINGANG": "Letzte Wareneingang Datum",
            "WARENEINGAG_MENGE": "Letzte Wareneingang Menge",
            "WG_NAME": "Warengruppe",

            "LIEFER_STA":"Nachlieferstatus",
            "LANUMMER": "Lagerartikel Nr.",
            "BANUMMER": "Marketingartikel Nr.",
            "FAKTURA_MONAT": "Letzte Faktura Datum",
            "FAKTURA_MENGE": "Letzte Faktura Menge",
            "TOTAL_MENGE": "Gesamt Faktura",
            "AVG_MONAT_MENGE": "Durschnitt Monatliche Faktura",
            "AKTUELLER_EKPREIS": "EK-Preis",
            "AKTUELLE_VKPREIS": "Aktueller VK-Preis",
        }
        products_partikel_merged = products_partikel_merged.rename(
            columns=column_renames
        )

        # Calculate additional metrics
        products_partikel_merged["Gesamt Retouren"] = (
            products_partikel_merged["Gesamt Retouren +"].astype(float).fillna(0.0)
            + products_partikel_merged["Gesamt Retouren -"].astype(float).fillna(0.0)
        )
        products_partikel_merged["Retourenquote (%)"] = (
            products_partikel_merged["Gesamt Retouren"].astype(float).fillna(0.0)
            / products_partikel_merged["Gesamt Faktura"].astype(float).fillna(0.0)
        )

        # Calculate forecasted order month
        products_partikel_merged["Verfügbarer Lagerbestand"] = (
            products_partikel_merged["Verfügbarer Lagerbestand"]
            .astype(float)
            .fillna(0.0)
        )
        products_partikel_merged["Durchschnittliche Monatliche Faktura"] = (
            products_partikel_merged["Durchschnittliche Monatliche Faktura"]
            .astype(float)
            .fillna(0.0)
        )
        products_partikel_merged["Lager Verfügbarkeit bis"] = (
            products_partikel_merged.apply(
                lambda row: calculate_forecasted_order_month(
                    row["Durchschnittliche Monatliche Faktura"],
                    row["Verfügbarer Lagerbestand"],
                ),
                axis=1,
            )
        )

        products_partikel_merged['temp_col'] = np.where((products_partikel_merged['KALK_EK'].notna()) & (products_partikel_merged['KALK_EK'] != 0), products_partikel_merged['KALK_EK'], products_partikel_merged['EK-Preis'])
        products_partikel_merged["EK-Volumen"] = (
            products_partikel_merged["EK-Preis"]
            * products_partikel_merged["Verfügbarer Lagerbestand"]
        )
        ## NEW LINE
        # products_partikel_merged["Verfügbarer Lagerbestand"] = round(products_partikel_merged["Verfügbarer Lagerbestand"] / products_partikel_merged['FAKTOR'].fillna(1),0)

        ga_data = pd.read_excel(fr"C:\Python\7-product-bestand\ga4_daily_report.xlsx")
        ga_data = ga_data.rename(columns={"itemId": "Marketingartikel Nr."})
        ga_data["Marketingartikel Nr."] = (
            ga_data["Marketingartikel Nr."].astype(str).str.strip()
        )
        products_partikel_merged = products_partikel_merged.merge(
            ga_data, on="Marketingartikel Nr.", how="left"
        )


        output_yesterday = PRODUCT_BESTAND_PATH
        # last_product_data = pd.read_excel(output_yesterday, usecols=['Marketingartikel Nr.',"neuer VK Set","neuer VK","Maßnahmen","Bemerkung", "Rückstand Vortag","Rückstand abgebaut bis",
        #                         "Aktion1", "Zeitraum1", "Preis1", "Aktion2", "Zeitraum2", "Preis2", "Aktion3", "Zeitraum3", "Preis3", "Zusätzliche Informationen"])
        # products_partikel_merged = products_partikel_merged.merge(last_product_data,on='Marketingartikel Nr.',how='left')
        #### ======================================== ####
        # Load yesterday completely (no usecols restriction)
        last_product_data = pd.read_excel(output_yesterday)

        key = "Marketingartikel Nr."

        # Identify columns that:
        # 1. Exist in yesterday
        # 2. Do NOT exist in today's dataframe
        # 3. Are NOT the key column

        extra_columns = [
            col for col in last_product_data.columns
            if col not in products_partikel_merged.columns
            and col != key
        ]

        # Build reduced dataframe with only key + extra columns
        extra_data = last_product_data[[key] + extra_columns]

        # Merge them in
        products_partikel_merged = products_partikel_merged.merge(
            extra_data,
            on=key,
            how="left"
        )
        #### ======================================== ####
        products_partikel_merged = products_partikel_merged.drop_duplicates(subset=['Marketingartikel Nr.'])
        products_partikel_merged["MASSNAHME ERGREIFEN"] = classify_products_vectorized(
            M=products_partikel_merged["EK-Preis"],
            T=products_partikel_merged["Letzte Lager Bewegung"],
            X=products_partikel_merged["Verfügbarer Lagerbestand"],
        )
        mapping = {
            "Webshop Stufe 1": 0.10,
            "Webshop Stufe 2": 0.25,
            "Webshop Stufe 3": 0.50,
            "Webshop Stufe 4": 0.75,
        }

        s = products_partikel_merged["MASSNAHME ERGREIFEN"]
        products_partikel_merged["MASSNAHME REDUZIERUNGEN STUFE 1-4"] = s.map(mapping)

        # products_partikel_merged['MASSNAHME NEUER VK'] = calculate_neuer_vk(products_partikel_merged['MASSNAHME ERGREIFEN'],products_partikel_merged['MASSNAHME REDUZIERUNGEN STUFE 1-4'],products_partikel_merged['Aktueller VK-Preis'])
        mask = products_partikel_merged["MASSNAHME ERGREIFEN"].astype(str).str.contains("Stufe", case=False, na=False)
        products_partikel_merged["MASSNAHME NEUER VK"] = np.where(mask, (1 - products_partikel_merged["MASSNAHME REDUZIERUNGEN STUFE 1-4"]) * products_partikel_merged["Aktueller VK-Preis"], np.nan)
        products_partikel_merged['MASSNAHME NEUER VK minus Kalk EK (EK)'] = products_partikel_merged['MASSNAHME NEUER VK'] - products_partikel_merged['temp_col']
        products_partikel_merged.loc[products_partikel_merged["MASSNAHME ERGREIFEN"].str.contains("Spalten M,T,X",na=False,case=False),"MASSNAHME NEUER VK"] = 'nein'
        products_partikel_merged.loc[products_partikel_merged["MASSNAHME ERGREIFEN"].str.contains("Lagerverkauf Krefeld",na=False,case=False),"MASSNAHME NEUER VK"] = 'prüfen'
        products_partikel_merged.loc[products_partikel_merged["MASSNAHME ERGREIFEN"].str.contains("Vermarkter",na=False,case=False),"MASSNAHME NEUER VK"] = 'prüfen'
        final_columns = [
            "EKG",
            "NUMMER",
            "GROESSE",
            "FARBE",
            "P_ARTIKEL",
            "Lagerartikel Nr.",
            "Marketingartikel Nr.",
            "Beschreibung",
            "Warengruppe",
            "Einkäufer",
            "Lieferanten Nr.",
            "Lieferanten Name",
            "EK-Preis",
            "EK-Volumen",
            "Aktueller VK-Preis",
            "Alter VK-Preis 1",
            "Alter VK-Preis 2",
            "Bestand VK Wert",
            "Marge",
            "Letzte Lager Bewegung",
            "Letzte Wareneingang Datum",
            "Letzte Wareneingang Menge",
            "Gesamt Lagerbestand",
            "Verfügbarer Lagerbestand",
            "Rückstand in Stück",  
            "Rückstand Vortag",
            "Rückstand abgebaut bis",
            "offene Bestellungen",
            "Nachlieferstatus",
            "Letzte 30T Faktura",
            "Letzte 30T Brauchbare Retouren",
            "Letzte 30T Unbrauchbare Retouren",
            "Letzte 30T Lagerstorno",
            "Letzte 12M Faktura",
            "Letzte 12M Brauchbare Retouren",
            "Letzte 12M Unbrauchbare Retouren",
            "Letzte 12M Lagerstorno",
            "Durchschnittliche Monatliche Faktura",
            "Lager Verfügbarkeit bis",
            "Gesamt Wareneingang",
            "Gesamt Faktura",
            "Gesamt Retouren +",
            "Gesamt Retouren -",
            "Gesamt Retouren",
            "Retourenquote (%)",
            "FRUEHJAHR",
            "SOMMER",
            "HERBST",
            "WINTER",
            "FRUEHJ_W1",
            "FRUEHJ_W2",
            "FRUEHJ_W3",
            "FRUEHJ_W4",
            "SOMMER_W1",
            "SOMMER_W2",
            "SOMMER_W3",
            "SOMMER_W4",
            "HERBST_W1",
            "HERBST_W2",
            "HERBST_W3",
            "HERBST_W4",
            "WINTER_W1",
            "WINTER_W2",
            "WINTER_W3",
            "WINTER_W4",
            "Angeklickte Artikel im Internet",
            "Dem Warenkorb hinzugefügte Artikel im Internet",
            "Gekaufte Artikel im Internet",
            "Umsatz Artikel im Internet",
            "Lagerplatz",
            "neuer VK Set",
            "neuer VK",
            "Maßnahmen",
            "Bemerkung",
             "Aktion1", 
             "Zeitraum1", 
             "Preis1", 
             "Aktion2", 
             "Zeitraum2", 
             "Preis2", 
             "Aktion3", 
             "Zeitraum3", 
             "Preis3", 
             "Zusätzliche Informationen",
             'MASSNAHME ERGREIFEN',
             'MASSNAHME REDUZIERUNGEN STUFE 1-4',
             'MASSNAHME NEUER VK',
             'MASSNAHME NEUER VK minus Kalk EK (EK)',
             'MASSNAHMEN NICHT VERPLANTE ARTIKEL',
             'AKTION4 - FINALE MASSNAHME LT. PM',
             'ZEITRAUM4',
             'PREIS4'

        ]

        products_partikel_merged = products_partikel_merged[final_columns]

        logger.info("Successfully processed final data")

        return products_partikel_merged

    except Exception as e:
        logger.error(f"Error processing final data: {str(e)}")
        raise

### ==================================================== ###


### =============== WRITE EXCEL DATA =============== ###

@task(name="Write Excel File")
def write_excel_file(products_data: pd.DataFrame, output_path: str = None) -> None:
    """Write the final data to Excel with formatting (xlsxwriter)."""
    logger = get_run_logger()

    try:
        if output_path is None:
            output_path = PRODUCT_BESTAND_PATH

        logger.info(f"Writing Excel file to {output_path}")

        # Date helpers for conditional formatting
        today = datetime.today()
        current_month = today.strftime("%Y-%m")
        first_day_this_month = today.replace(day=1)
        first_day_next_month = first_day_this_month + timedelta(days=32)
        next_month = first_day_next_month.strftime("%Y-%m")

        # House rules
        column_width = 17
        max_rows = len(products_data) + 1  # + header row
        last_excel_row = max(2, max_rows)  # ensure at least row 2 exists

        # Normalize column names
        products_data = products_data.copy()
        products_data.columns = products_data.columns.str.strip()

        sheet_name = "Produkt Analytik"

        with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
            workbook = writer.book

            # ----------------------------
            # Cell formats
            # ----------------------------
            text_format = workbook.add_format({"num_format": "@"})
            green_text_format = workbook.add_format({"font_color": "green"})
            text_wrap_format = workbook.add_format({"text_wrap": True})
            date_format = workbook.add_format({"num_format": "yyyy-mm-dd"})
            currency_format_decimals = workbook.add_format(
                {"num_format": "#,##0.00 [$€-1];[Red]-#,##0.00 [$€-1]"}
            )
            int_format = workbook.add_format({"num_format": "#,##0"})
            decimal_format = workbook.add_format({"num_format": "#,##0.00"})
            percentage_format = workbook.add_format({"num_format": "0.00%"})
            red_font_format = workbook.add_format({"font_color": "red"})

            header_format = workbook.add_format(
                {"bold": True, "font_color": "black", "bg_color": "#D9EAD3", "border": 1}
            )

            # ----------------------------
            # Column-specific formats
            # NOTE: XlsxWriter uses ONE format per column via set_column.
            # Mixed type columns get a text base format + conditional formats for numeric cells.
            # ----------------------------
            column_formats = {
                "EKG": text_format,
                "NUMMER": text_format,
                "GROESSE": text_format,
                "FARBE": text_format,
                "P_ARTIKEL": text_format,
                "Lagerartikel Nr.": text_format,
                "Marketingartikel Nr.": text_format,
                "Beschreibung": text_wrap_format,
                "Lagerplatz": text_wrap_format,  # keep wrap, you had duplicates
                "Warengruppe": text_wrap_format,
                "Einkäufer": text_wrap_format,
                "Lieferanten Nr.": text_format,
                "Lieferanten Name": text_wrap_format,
                "Rückstand in Stück": int_format,
                "Rückstand Vortag": text_format,
                "Rückstand abgebaut bis": text_format,
                "offene Bestellungen": int_format,
                "Nachlieferstatus": text_format,
                "FRUEHJAHR": text_format,
                "SOMMER": text_format,
                "HERBST": text_format,
                "WINTER": text_format,
                "FRUEHJ_W1": text_format,
                "FRUEHJ_W2": text_format,
                "FRUEHJ_W3": text_format,
                "FRUEHJ_W4": text_format,
                "SOMMER_W1": text_format,
                "SOMMER_W2": text_format,
                "SOMMER_W3": text_format,
                "SOMMER_W4": text_format,
                "HERBST_W1": text_format,
                "HERBST_W2": text_format,
                "HERBST_W3": text_format,
                "HERBST_W4": text_format,
                "WINTER_W1": text_format,
                "WINTER_W2": text_format,
                "WINTER_W3": text_format,
                "WINTER_W4": text_format,
                "EK-Preis": currency_format_decimals,
                "EK-Volumen": currency_format_decimals,
                "Aktueller VK-Preis": currency_format_decimals,
                "Alter VK-Preis 1": currency_format_decimals,
                "Alter VK-Preis 2": currency_format_decimals,
                "Bestand VK Wert": currency_format_decimals,
                "Marge": decimal_format,
                "Retourenquote (%)": percentage_format,
                "Letzte Wareneingang Datum": date_format,
                "Lager Verfügbarkeit bis": date_format,
                "Letzte Lager Bewegung": date_format,
                "Letzte Wareneingang Menge": int_format,
                "Gesamt Lagerbestand": int_format,
                "Verfügbarer Lagerbestand": int_format,
                "Durchschnittliche Monatliche Faktura": decimal_format,
                "Gesamt Wareneingang": int_format,
                "Gesamt Faktura": int_format,
                "Gesamt Retouren +": int_format,
                "Gesamt Retouren -": int_format,
                "Gesamt Retouren": int_format,
                "Letzte 30T Faktura": int_format,
                "Letzte 30T Brauchbare Retouren": int_format,
                "Letzte 30T Unbrauchbare Retouren": int_format,
                "Letzte 30T Lagerstorno": int_format,
                "Letzte 12M Faktura": int_format,
                "Letzte 12M Brauchbare Retouren": int_format,
                "Letzte 12M Unbrauchbare Retouren": int_format,
                "Letzte 12M Lagerstorno": int_format,
                "Angeklickte Artikel im Internet": int_format,
                "Dem Warenkorb hinzugefügte Artikel im Internet": int_format,
                "Gekaufte Artikel im Internet": int_format,
                "Umsatz Artikel im Internet": currency_format_decimals,
                "neuer VK Set": currency_format_decimals,
                "neuer VK": currency_format_decimals,
                "Maßnahmen": text_wrap_format,
                "Bemerkung": text_wrap_format,
                "Aktion1": text_wrap_format,
                "Zeitraum1": text_wrap_format,
                "Preis1": currency_format_decimals,
                "Aktion2": text_wrap_format,
                "Zeitraum2": text_wrap_format,
                "Preis2": currency_format_decimals,
                "Aktion3": text_wrap_format,
                "Zeitraum3": text_wrap_format,
                "Preis3": currency_format_decimals,
                "Zusätzliche Informationen": text_wrap_format,

                # Derived columns from your formulas/functions:
                "MASSNAHME ERGREIFEN": text_wrap_format,                 # pure text output
                "MASSNAHME REDUZIERUNGEN STUFE 1-4": percentage_format,  # numeric percent or blank
                # mixed numeric/text -> set base as text, format numbers via conditional formatting
                "MASSNAHME NEUER VK": text_wrap_format,
                "MASSNAHME NEUER VK minus Kalk EK (EK)": text_format,
                'MASSNAHMEN NICHT VERPLANTE ARTIKEL':text_wrap_format,
                'AKTION4 - FINALE MASSNAHME LT. PM':text_wrap_format,
                'ZEITRAUM4':date_format,
                'PREIS4':currency_format_decimals
            }

            # ----------------------------
            # Write data (no header), then write styled header row manually
            # ----------------------------
            products_data.to_excel(writer, sheet_name=sheet_name, index=False, header=False)
            worksheet = writer.sheets[sheet_name]

            for col_num, header in enumerate(products_data.columns):
                worksheet.write(0, col_num, header, header_format)

            last_col = len(products_data.columns) - 1
            last_row = last_excel_row - 1  # zero-based index

            worksheet.autofilter(0, 0, last_row, last_col)
            # ----------------------------
            # Apply column widths + base formats
            # ----------------------------
            wrap_cols = {
                "Beschreibung",
                "Warengruppe",
                "Einkäufer",
                "Lieferanten Name",
                "Maßnahmen",
                "Bemerkung",
                "Zusätzliche Informationen",
                "Aktion1",
                "Aktion2",
                "Aktion3",
                "Zeitraum1",
                "Zeitraum2",
                "Zeitraum3",
                "Lagerplatz",
                "MASSNAHME ERGREIFEN",
                "MASSNAHME NEUER VK",
                "MASSNAHME NEUER VK minus Kalk EK (EK)"
            }

            zero_width_int_cols = {
                "Gesamt Lagerbestand",
                "Letzte 30T Brauchbare Retouren",
                "Letzte 30T Unbrauchbare Retouren",
                "Letzte 30T Lagerstorno",
                "Letzte 12M Brauchbare Retouren",
                "Letzte 12M Unbrauchbare Retouren",
                "Letzte 12M Lagerstorno",
                "Gesamt Retouren +",
                "Gesamt Retouren -",
                "FRUEHJ_W1",
                "FRUEHJ_W2",
                "FRUEHJ_W3",
                "FRUEHJ_W4",
                "SOMMER_W1",
                "SOMMER_W2",
                "SOMMER_W3",
                "SOMMER_W4",
                "HERBST_W1",
                "HERBST_W2",
                "HERBST_W3",
                "HERBST_W4",
                "WINTER_W1",
                "WINTER_W2",
                "WINTER_W3",
                "WINTER_W4",
            }

            for idx, column_name in enumerate(products_data.columns):
                fmt = column_formats.get(column_name)

                if column_name in wrap_cols:
                    worksheet.set_column(idx, idx, 30, text_wrap_format)
                elif column_name in zero_width_int_cols:
                    worksheet.set_column(idx, idx, 0, int_format)
                else:
                    if fmt is not None:
                        worksheet.set_column(idx, idx, column_width, fmt)
                    else:
                        worksheet.set_column(idx, idx, column_width)

            # ----------------------------
            # Conditional formatting
            # ----------------------------

            # (A) "Lager Verfügbarkeit bis": highlight current and next month
            if "Lager Verfügbarkeit bis" in products_data.columns:
                date_col_idx = products_data.columns.get_loc("Lager Verfügbarkeit bis")
                date_col_letter = xl_col_to_name(date_col_idx)  # you already have this helper
                rng = f"{date_col_letter}2:{date_col_letter}{last_excel_row}"

                worksheet.conditional_format(
                    rng,
                    {"type": "cell", "criteria": "==", "value": f'"{current_month}"', "format": red_font_format},
                )
                worksheet.conditional_format(
                    rng,
                    {"type": "cell", "criteria": "==", "value": f'"{next_month}"', "format": red_font_format},
                )

            # (B) Seasonal columns: TRUE -> green
            seasonal_cols = [
                "FRUEHJAHR",
                "SOMMER",
                "HERBST",
                "WINTER",
                "FRUEHJ_W1",
                "FRUEHJ_W2",
                "FRUEHJ_W3",
                "FRUEHJ_W4",
                "SOMMER_W1",
                "SOMMER_W2",
                "SOMMER_W3",
                "SOMMER_W4",
                "HERBST_W1",
                "HERBST_W2",
                "HERBST_W3",
                "HERBST_W4",
                "WINTER_W1",
                "WINTER_W2",
                "WINTER_W3",
                "WINTER_W4",
            ]
            for col in seasonal_cols:
                if col not in products_data.columns:
                    continue
                col_idx = products_data.columns.get_loc(col)
                col_letter = xl_col_to_name(col_idx)
                cell_range = f"{col_letter}2:{col_letter}{last_excel_row}"
                worksheet.conditional_format(
                    cell_range,
                    {"type": "cell", "criteria": "==", "value": "TRUE", "format": green_text_format},
                )

            # (C) Mixed columns: apply numeric formats only where cells are numeric
            # Base is text_format already via set_column().
            mixed_currency_cols = [
                "MASSNAHME NEUER VK",
                "MASSNAHME NEUER VK minus Kalk EK (EK)",
            ]
            for col in mixed_currency_cols:
                if col not in products_data.columns:
                    continue
                col_idx = products_data.columns.get_loc(col)
                col_letter = xl_col_to_name(col_idx)
                cell_range = f"{col_letter}2:{col_letter}{last_excel_row}"

                # >= 0
                worksheet.conditional_format(
                    cell_range,
                    {"type": "cell", "criteria": ">=", "value": 0, "format": currency_format_decimals},
                )
                # < 0 (if it happens)
                worksheet.conditional_format(
                    cell_range,
                    {"type": "cell", "criteria": "<", "value": 0, "format": currency_format_decimals},
                )

            # Freeze header row
            worksheet.freeze_panes(1, 0)

        logger.info("Excel file written successfully.")

    except Exception as e:
        logger.error(f"Error while writing Excel file: {e}")
        raise

### =============== ARTIKELBESTAND FLOW =============== ###
@flow(name="Artikelbestand Automation")
def artikelbestand_flow():
    # Execute the full pipeline
    shutil.copyfile(PRODUCT_BESTAND_PATH,PRODUCT_BESTAND_LOCAL_BACKUP)

    marketing_stamm = load_marketing_stamm()
    katalog_data = load_katalog_data()
    vk_preis = load_vk_preis()
    lager_data = load_lager_data()
    lieferant_info = load_lieferant_data()
    warengruppe = load_warengruppe_data()
    set_artikels = load_set_artikels()
    lagerbewegung_data = process_lagerbewegung_historie()
    analytics = calculate_analytics(
        lagerbewegung_data["lagerbewegung_historie"], lager_data["lager_stamm"]
    )
    lagerplatz = load_lagerplatz()

    products = merge_all_data(
        marketing_stamm,
        katalog_data,
        vk_preis,
        lager_data,
        lieferant_info,
        set_artikels,
        warengruppe,
        lagerbewegung_data,
        analytics,
        lagerplatz
    )
    final_df = process_final_data(products)
    write_excel_file(final_df)
    output_path_2 = PRODUCT_BESTAND_HISTORIE
    shutil.copyfile(PRODUCT_BESTAND_PATH,output_path_2)
### ==================================================== ###

if __name__ == "__main__":

    artikelbestand_flow()


