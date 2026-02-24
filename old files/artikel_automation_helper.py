import pandas as pd
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
import math

today = pd.Timestamp(datetime.today().date())


### ====================== get_current_ekpreis ====================== ###
def get_current_ekpreis(row):
    preis_options = []

    if pd.notna(row["EKPREIS1"]) and pd.notna(row["EKVALIDD1"]):
        if row["EKVALIDD1"] <= today:
            preis_options.append((row["EKVALIDD1"], row["EKPREIS1"]))

    if pd.notna(row["EKPREIS2"]) and pd.notna(row["EKVALIDD2"]):
        if row["EKVALIDD2"] <= today:
            preis_options.append((row["EKVALIDD2"], row["EKPREIS2"]))

    if preis_options:
        # Choose the one with the most recent start date
        return max(preis_options, key=lambda x: x[0])[1]
    else:
        return row["EKPREIS1"]


### ====================== determine_current_vkpreis ====================== ###
def determine_current_vkpreis(row):
    preis_options = []

    # Convert today to Timestamp if needed
    global today  # only if you want to use external 'today'
    today = pd.to_datetime(today).normalize()

    # VKPREIS1
    if pd.notna(row["VKPREIS1"]) and pd.notna(row["VKVALIDD1"]):
        start1 = pd.to_datetime(row["VKVALIDD1"], errors="coerce",format='mixed')
        end1 = (
            pd.to_datetime(row["VKBISDT1"], errors="coerce",format='mixed')
            if pd.notna(row["VKBISDT1"])
            else pd.Timestamp.max
        )
        # print("VKPREIS1: ", start1, end1)
        if pd.notna(start1) and start1 <= today <= end1:
            preis_options.append((start1, row["VKPREIS1"]))

    # VKPREIS2
    if pd.notna(row["VKPREIS2"]) and pd.notna(row["VKVALIDD2"]):
        start2 = pd.to_datetime(row["VKVALIDD2"], errors="coerce",format='mixed')
        end2 = (
            pd.to_datetime(row["VKBISDT2"], errors="coerce",format='mixed')
            if pd.notna(row["VKBISDT2"])
            else pd.Timestamp.max
        )
        # print("VKPREIS2: ", start2, end2)
        if pd.notna(start2) and start2 <= today <= end2:
            preis_options.append((start2, row["VKPREIS2"]))

    if preis_options:
        return max(preis_options, key=lambda x: x[0])[1]
    else:
        # print("No valid VKPREIS found for row:")
        # print(row)
        return row["VKPREIS1"]


### ====================== calculate_years_months ====================== ###
def calculate_years_months(date):
    if pd.isna(date):
        return pd.NA

    # Ensure both values are datetime.date or datetime.datetime
    today = datetime.today()
    rd = relativedelta(today, date)
    if rd.years == 0:
        return f">1 Jahr"
    else:
        return f"{rd.years} Jahr(e)"


### ====================== merge_p_artikels ====================== ###
def merge_p_artikels(df):
    df = df.copy()

    # Step 1: Clean and prepare component columns
    for col in ["GROESSE", "FARBE"]:
        df[col] = df[col].fillna("").astype(str).str.strip()

    df["NUMMER"] = df["NUMMER"].astype(str).str.strip()

    # Step 2: Construct LANUMMER
    df["LANUMMER"] = (
        df["NUMMER"].str.ljust(8)
        + df["GROESSE"].str.ljust(4)
        + df["FARBE"].str.ljust(2)
    ).str.strip()

    # Step 3: Identify P-Artikels
    df["IS_P"] = df["NUMMER"].str.endswith("P")
    # df["IS_LANUMMER_P"] = df["LANUMMER"].str.extract(r"(P)\b", expand=False).notna() | df["LANUMMER"].str.endswith("P")

    # Separate base and P
    df_base = df[~df["IS_P"]].copy()
    df_p = df[df["IS_P"]].copy()

    # Step 4: Compute LANUMMER without the "P" (only for P-Artikel side)
    df_p["LANUMMER_BASE"] = (
        df_p["NUMMER"]
        .str.replace(r"P(?=\s|$)", "", regex=True)
        .str.strip()
        .str.ljust(8)
        + df_p["GROESSE"].str.ljust(4)
        + df_p["FARBE"].str.ljust(2)
    ).str.strip()
    # Step 5: Merge on LANUMMER stripped of "P"
    merged = df_base.merge(
        df_p,
        left_on="LANUMMER",
        right_on="LANUMMER_BASE",
        how="left",
        suffixes=("_base", "_p"),
    )

    # # Step 6: Fallback logic for selected columns
    # def fallback(col):
    #     return merged.get(f"{col}_p", pd.NA).combine_first(merged.get(f"{col}_base", pd.NA))
    def fallback(col):
        col_p = f"{col}_p"
        col_base = f"{col}_base"

        s1 = (
            merged[col_p]
            if col_p in merged.columns
            else pd.Series([pd.NA] * len(merged))
        )
        s2 = (
            merged[col_base]
            if col_base in merged.columns
            else pd.Series([pd.NA] * len(merged))
        )

        return s1.combine_first(s2)

    result = pd.DataFrame(
        {
            "BANUMMER": merged["BANUMMER_base"],
            "NUMMER": merged["NUMMER_base"],
            "P_ARTIKEL": merged["NUMMER_p"],
            "LANUMMER": merged["LANUMMER_base"],
            "FARBE": fallback("FARBE"),
            "GROESSE": fallback("GROESSE"),
            "EKG": merged["EKG_base"],
            "Beschreibung": merged["Beschreibung_base"],
            "WG_NAME": merged["WG_NAME_base"],
            "FAKTOR": fallback("FAKTOR"),
            "Einkäufer": merged["Einkäufer_base"],
            "LIEFER_STA": merged["LIEFER_STA_base"],
            "AKTUELLER_EKPREIS": fallback("AKTUELLER_EKPREIS"),
            "KALK_EK": fallback("KALK_EK"),
            "AKTUELLE_VKPREIS": merged["AKTUELLE_VKPREIS_base"],
            "Alter VK-Preis 1": merged["Alter VK-Preis 1_base"],
            "Alter VK-Preis 2": merged["Alter VK-Preis 2_base"],
            "GES_BEST": fallback("GES_BEST"),
            "VERF_BEST": fallback("VERF_BEST"),
            "RCK_GES_BE": fallback("RCK_GES_BE"),
            "BEST_GES_M": fallback("BEST_GES_M"),
            "Gesamt Wareneingang": fallback("Gesamt Wareneingang"),
            "Gesamt Retouren +": fallback("Gesamt Retouren +"),
            "Gesamt Retouren -": fallback("Gesamt Retouren -"),
            "LI_NUMMER": fallback("LI_NUMMER"),
            "LIEFERANTNAME": fallback("LIEFERANTNAME"),

            "Lagerplatz": fallback("Lagerplatz"),
            "FRUEHJAHR": fallback("FRUEHJAHR"),
            "SOMMER": fallback("SOMMER"),
            "HERBST": fallback("HERBST"),
            "WINTER": fallback("WINTER"),
            "FRUEHJ_W1": fallback("FRUEHJ_W1"),
            "FRUEHJ_W2": fallback("FRUEHJ_W2"),
            "FRUEHJ_W3": fallback("FRUEHJ_W3"),
            "FRUEHJ_W4": fallback("FRUEHJ_W4"),
            "SOMMER_W1": fallback("SOMMER_W1"),
            "SOMMER_W2": fallback("SOMMER_W2"),
            "SOMMER_W3": fallback("SOMMER_W3"),
            "SOMMER_W4": fallback("SOMMER_W4"),
            "HERBST_W1": fallback("HERBST_W1"),
            "HERBST_W2": fallback("HERBST_W2"),
            "HERBST_W3": fallback("HERBST_W3"),
            "HERBST_W4": fallback("HERBST_W4"),
            "WINTER_W1": fallback("WINTER_W1"),
            "WINTER_W2": fallback("WINTER_W2"),
            "WINTER_W3": fallback("WINTER_W3"),
            "WINTER_W4": fallback("WINTER_W4"),
            "LAST_BEWEG": fallback("LAST_BEWEG"),
            "LETZTE_WARENEINGANG": fallback("LETZTE_WARENEINGANG"),
            "WARENEINGAG_MENGE": fallback("WARENEINGAG_MENGE"),
            "TOTAL_MENGE": fallback("TOTAL_MENGE"),
            "NUM_MONATE": fallback("NUM_MONATE"),
            "Durchschnittliche Monatliche Faktura": fallback(
                "Durchschnittliche Monatliche Faktura"
            ),
            "Letzte 30T Faktura": fallback("Letzte 30T Faktura"),
            "Letzte 30T Brauchbare Retouren": fallback(
                "Letzte 30T Brauchbare Retouren"
            ),
            "Letzte 30T Unbrauchbare Retouren": fallback(
                "Letzte 30T Unbrauchbare Retouren"
            ),
            "Letzte 30T Lagerstorno": fallback("Letzte 30T Lagerstorno"),
            "Letzte 12M Faktura": fallback("Letzte 12M Faktura"),
            "Letzte 12M Brauchbare Retouren": fallback(
                "Letzte 12M Brauchbare Retouren"
            ),
            "Letzte 12M Unbrauchbare Retouren": fallback(
                "Letzte 12M Unbrauchbare Retouren"
            ),
            "Letzte 12M Lagerstorno": fallback("Letzte 12M Lagerstorno"),
        }
    )

    return result


### ====================== compute_avg_monthly_sales ====================== ###
def compute_avg_monthly_sales(df, use_text_fallback=False):
    # Ensure datetime types
    df["BUCHDATUM"] = pd.to_datetime(df["BUCHDATUM"], errors="coerce")
    df["SYS_ANLAGE"] = pd.to_datetime(df["SYS_ANLAGE"], errors="coerce")

    # Work on a clean version of df
    df_clean = df.dropna(subset=["MENGE", "BUCHDATUM"]).copy()

    # STEP 1: Group monthly and sum MENGE
    df_clean["MONTH"] = df_clean["BUCHDATUM"].dt.to_period("M")
    monthly_sales = df_clean.groupby(["LANUMMER", "MONTH"])["MENGE"].sum().reset_index()

    # STEP 2: Total sales per product
    total_sales = (
        monthly_sales.groupby("LANUMMER")["MENGE"].sum().reset_index(name="TOTAL_MENGE")
    )

    # STEP 3: Get SYS_ANLAGE and LAST_BUCHDATUM for all products (including those without sales)
    base_info = (
        df.groupby("LANUMMER")
        .agg(
            SYS_ANLAGE=("SYS_ANLAGE", "first"),
            LAST_BUCHDATUM=("BUCHDATUM", "max"),
            # Lagerplatz=("Lagerplatz", "first"),
        )
        .reset_index()
    )

    # Compute number of months safely
    base_info["NUM_MONATE"] = base_info.apply(
        lambda row: (
            (row["LAST_BUCHDATUM"].to_period("M") - row["SYS_ANLAGE"].to_period("M")).n
            + 1
        )
        if pd.notna(row["LAST_BUCHDATUM"]) and pd.notna(row["SYS_ANLAGE"])
        else 0,
        axis=1,
    )

    # Merge with total sales
    result = pd.merge(base_info, total_sales, on="LANUMMER", how="left")
    result["TOTAL_MENGE"] = result["TOTAL_MENGE"].fillna(0)
    result["AVG_MONAT_MENGE"] = result.apply(
        lambda row: (
            row["TOTAL_MENGE"] / row["NUM_MONATE"]
            if row["NUM_MONATE"] > 0
            else ("Keine Angabe" if use_text_fallback else 0)
        ),
        axis=1,
    )

    return result[
        ["LANUMMER", "TOTAL_MENGE", "NUM_MONATE", "AVG_MONAT_MENGE"]
        # ["LANUMMER", "TOTAL_MENGE", "NUM_MONATE", "AVG_MONAT_MENGE", "Lagerplatz"]
    ]


### ====================== get_last_n_days_sum ====================== ###
def get_last_n_days_sum(df, buchkeys, menge_column_name, days, reference_date=None):
    """
    Filters the dataframe for specified BUCHKEY(s), computes the sum of MENGE
    per NUMMER over the last 30 days. Products with no activity in the last 30 days
    will return 0.

    Parameters:
        df (pd.DataFrame): Input DataFrame with 'BUCHDATUM', 'BUCHKEY', 'NUMMER', and 'MENGE'
        buchkeys (int or list of int): BUCHKEY(s) to filter
        menge_column_name (str): Output column name for summed 'MENGE'
        reference_date (datetime, optional): Reference end date (defaults to today)

    Returns:
        pd.DataFrame: DataFrame with 'LANUMMER' and 'menge_column_name'
    """
    if isinstance(buchkeys, int):
        buchkeys = [buchkeys]

    if reference_date is None:
        reference_date = pd.Timestamp.today().normalize()

    start_date = reference_date - pd.Timedelta(days=days)

    df["BUCHDATUM"] = pd.to_datetime(df["BUCHDATUM"], errors="coerce")
    filtered = df[
        (df["BUCHKEY"].isin(buchkeys))
        & (df["BUCHDATUM"] >= start_date)
        & (df["BUCHDATUM"] <= reference_date)
    ].copy()

    # Get all unique NUMMERs ever present with this BUCHKEY
    all_lanummer = df[df["BUCHKEY"].isin(buchkeys)]["NUMMER"].unique()

    # Group filtered data
    grouped = (
        filtered.groupby("NUMMER")
        .agg({"MENGE": "sum"})
        .reindex(all_lanummer, fill_value=0)  # ensure all are present
        .reset_index()
        .rename(columns={"NUMMER": "LANUMMER", "MENGE": menge_column_name})
    )

    return grouped


### ====================== Forecasting Functions ====================== ###

def calculate_forecasted_order_month(monthly_sales_avg, current_stock, max_months=120):
    """
    Berechnet den voraussichtlichen Bestellmonat auf Basis von Lagerbestand und durchschnittlichem Verbrauch.
    Gibt "> max_months" zurück, wenn der Wert zu hoch ist.
    Gibt den aktuellen Monat zurück, wenn der Lagerbestand innerhalb desselben Monats aufgebraucht wird.
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
        target_month_offset = int(months_remaining)  # truncate to get 0 if stock is used up this month
        forecast_year = today.year + (today.month + target_month_offset - 1) // 12
        forecast_month = (today.month + target_month_offset - 1) % 12 + 1
        return f"{forecast_year}-{forecast_month:02d}"
    except OverflowError:
        return "Datum zu weit in der Zukunft"


########### ====================== Marge Prediction ============================== ##############

    
def postprocess_apply_factored_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replaces the three 'FAKTORED' columns with their non-factored official counterparts.
    """
    df = df.copy()

    mapping = {
        'EK_FAKTORED': 'EK-Preis',
        'Gesamt Lagerbestand_FAKTORED': 'Gesamt Lagerbestand',
        'Verfügbarer Lagerbestand_FAKTORED': 'Verfügbarer Lagerbestand',
    }

    for src, dst in mapping.items():
        if src in df.columns and dst in df.columns:
            df[dst] = df[src]

    return df

#### -------------------- Christinas Spalten -------------------------- ####

from datetime import date

import numpy as np
import pandas as pd

def classify_products_vectorized(M: pd.Series, T: pd.Series, X: pd.Series) -> pd.Series:
    """
    Vectorized version of classify(...) for pandas Series.
    M = EK-Preis (numeric)
    T = Letzte Lager Bewegung (datetime)
    X = Verfügbarer Lagerbestand (numeric)
    Returns a Series of strings.
    """

    # Ensure datetime
    T_dt = pd.to_datetime(T, errors="coerce")
    today = pd.Timestamp.today().normalize()
    days = (today - T_dt).dt.days

    # Missing/invalid checks
    missing = X.isna() | M.isna() | T_dt.isna() | (X <= 0) | (M <= 0)

    out = pd.Series("", index=X.index, dtype="object")
    out[missing] = "daten/bestand fehlen, Spalten M,T,X prüfen"

    # Only consider valid rows for the rest
    valid = ~missing

    # If days < 90 => ""
    cond = valid & (days >= 90)

    # Build stage outputs using masks
    # Helper for age buckets
    def age_stage(d):
        return np.where(d <= 365, "Webshop Stufe 1",
               np.where(d <= 730, "Webshop Stufe 2", "Webshop Stufe 3"))

    # X <= 9 block
    m1 = cond & (X <= 9)
    out[m1 & (M <= 99)] = "Lagerverkauf Krefeld"
    out[m1 & (M > 99)] = age_stage(days[m1 & (M > 99)])

    # 9 < X <= 99 block
    m2 = cond & (X > 9) & (X <= 99)
    out[m2] = age_stage(days[m2])

    # X > 99 block
    m3 = cond & (X > 99)

    # M <= 20
    m3a = m3 & (M <= 20)
    out[m3a] = np.where(days[m3a] <= 365, "Webshop Stufe 1",
                np.where(days[m3a] <= 730, "Webshop Stufe 2", "Vermarkter"))

    # 20 < M <= 99
    m3b = m3 & (M > 20) & (M <= 99)
    out[m3b] = np.where(days[m3b] <= 365, "Webshop Stufe 2",
                np.where(days[m3b] <= 730, "Webshop Stufe 3", "Vermarkter"))

    # M > 99
    m3c = m3 & (M > 99)
    out[m3c] = np.where(days[m3c] <= 365, "Webshop Stufe 3",
                np.where(days[m3c] <= 730, "Webshop Stufe 4", "Vermarkter"))

    return out



def webshop_discount(CG):
    """
    Maps webshop stage to discount rate.
    Returns a float (e.g. 0.10 for 10%) or empty string if no match.
    """

    mapping = {
        "Webshop Stufe 1": 0.10,
        "Webshop Stufe 2": 0.25,
        "Webshop Stufe 3": 0.50,
        "Webshop Stufe 4": 0.75,
    }

    return mapping.get(CG, "")



def calculate_neuer_vk(CG, CH, O):
    """
    If 'Stufe' appears in CG (case-insensitive),
    return CH * O.
    Otherwise return 'nein'.
    """

    if isinstance(CG, str) and "stufe" in CG.lower():
        return CH * O
    return "nein"