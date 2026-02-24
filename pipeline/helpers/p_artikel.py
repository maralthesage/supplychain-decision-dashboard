"""
helpers/p_artikel.py
====================
Logic for merging "P-Artikel" (set/bundle articles) with their base articles.

A P-Artikel has a NUMMER ending in "P" (e.g. "12345678P").  It shares
size/colour slots with the corresponding base article but can carry its own
prices, stock levels, and sales figures.  The merge logic here combines both
rows into one, preferring the P-Artikel value when it exists (combine_first).
"""

from __future__ import annotations

import pandas as pd


def merge_p_artikels(df: pd.DataFrame) -> pd.DataFrame:
    """
    Left-join every base article with its matching P-Artikel (if one exists).

    Steps
    -----
    1.  Build LANUMMER from NUMMER + GROESSE + FARBE (fixed-width padding).
    2.  Split df into base rows and P rows.
    3.  For each P row, derive LANUMMER_BASE by stripping the trailing "P"
        from NUMMER before building the padded string.
    4.  Merge base ← P on LANUMMER == LANUMMER_BASE.
    5.  For every column that can come from either side, use combine_first so
        the P value wins when it is non-null.

    Parameters
    ----------
    df : pd.DataFrame
        The merged product dataframe *before* P-Artikel reconciliation.
        Must contain at minimum: NUMMER, GROESSE, FARBE, BANUMMER.

    Returns
    -------
    pd.DataFrame
        One row per base article.  A new column "P_ARTIKEL" carries the
        NUMMER value of the matched P-Artikel (or NaN when none exists).
    """
    df = df.copy()

    # --- normalise string columns used for key construction ---
    for col in ("GROESSE", "FARBE"):
        df[col] = df[col].fillna("").astype(str).str.strip()
    df["NUMMER"] = df["NUMMER"].astype(str).str.strip()

    # --- LANUMMER: NUMMER(8) + GROESSE(4) + FARBE(2), all left-justified ---
    df["LANUMMER"] = (
        df["NUMMER"].str.ljust(8)
        + df["GROESSE"].str.ljust(4)
        + df["FARBE"].str.ljust(2)
    ).str.strip()

    df["IS_P"] = df["NUMMER"].str.endswith("P")
    df_base = df[~df["IS_P"]].copy()
    df_p = df[df["IS_P"]].copy()

    # LANUMMER_BASE for P rows: strip trailing "P" from NUMMER before padding
    df_p["LANUMMER_BASE"] = (
        df_p["NUMMER"]
        .str.replace(r"P(?=\s|$)", "", regex=True)
        .str.strip()
        .str.ljust(8)
        + df_p["GROESSE"].str.ljust(4)
        + df_p["FARBE"].str.ljust(2)
    ).str.strip()

    merged = df_base.merge(
        df_p,
        left_on="LANUMMER",
        right_on="LANUMMER_BASE",
        how="left",
        suffixes=("_base", "_p"),
    )

    # --- helper: prefer P value, fall back to base value ---
    def _fallback(col: str) -> pd.Series:
        col_p = f"{col}_p"
        col_base = f"{col}_base"
        s_p = merged[col_p] if col_p in merged.columns else pd.Series(pd.NA, index=merged.index)
        s_base = merged[col_base] if col_base in merged.columns else pd.Series(pd.NA, index=merged.index)
        return s_p.combine_first(s_base)

    # Columns that are always taken directly from the base side (no P override)
    base_only = {
        "BANUMMER", "NUMMER", "LANUMMER",
        "EKG", "Beschreibung", "WG_NAME",
        "Einkäufer", "LIEFER_STA",
        "AKTUELLE_VKPREIS", "Alter VK-Preis 1", "Alter VK-Preis 2",
    }

    # All other columns get the combine_first treatment
    fallback_cols = [
        c.removesuffix("_base").removesuffix("_p")
        for c in merged.columns
        if c.endswith("_base") or c.endswith("_p")
    ]
    # deduplicate while preserving order
    seen: set[str] = set()
    fallback_cols_unique = [
        c for c in fallback_cols if not (c in seen or seen.add(c))  # type: ignore[func-returns-value]
    ]
    fallback_cols_unique = [c for c in fallback_cols_unique if c not in base_only]

    result = pd.DataFrame(index=merged.index)

    # Fixed base-only columns
    result["BANUMMER"]           = merged["BANUMMER_base"]
    result["NUMMER"]             = merged["NUMMER_base"]
    result["P_ARTIKEL"]          = merged.get("NUMMER_p")
    result["LANUMMER"]           = merged["LANUMMER_base"]
    result["EKG"]                = merged["EKG_base"] if "EKG_base" in merged.columns else merged.get("EKG")
    result["Beschreibung"]       = merged["Beschreibung_base"] if "Beschreibung_base" in merged.columns else merged.get("Beschreibung")
    result["WG_NAME"]            = merged["WG_NAME_base"] if "WG_NAME_base" in merged.columns else merged.get("WG_NAME")
    result["Einkäufer"]          = merged["Einkäufer_base"] if "Einkäufer_base" in merged.columns else merged.get("Einkäufer")
    result["LIEFER_STA"]         = merged["LIEFER_STA_base"] if "LIEFER_STA_base" in merged.columns else merged.get("LIEFER_STA")
    result["AKTUELLE_VKPREIS"]   = merged["AKTUELLE_VKPREIS_base"] if "AKTUELLE_VKPREIS_base" in merged.columns else merged.get("AKTUELLE_VKPREIS")
    result["Alter VK-Preis 1"]   = merged["Alter VK-Preis 1_base"] if "Alter VK-Preis 1_base" in merged.columns else merged.get("Alter VK-Preis 1")
    result["Alter VK-Preis 2"]   = merged["Alter VK-Preis 2_base"] if "Alter VK-Preis 2_base" in merged.columns else merged.get("Alter VK-Preis 2")
    # Dynamic combine_first columns
    for col in fallback_cols_unique:
        result[col] = _fallback(col)

    # Restore any columns that were not duplicated at all (no _base / _p suffix)
    all_suffixed_base = {c.removesuffix("_base") for c in merged.columns if c.endswith("_base")}
    all_suffixed_p    = {c.removesuffix("_p")    for c in merged.columns if c.endswith("_p")}
    all_suffixed = all_suffixed_base | all_suffixed_p

    for col in merged.columns:
        if col not in all_suffixed and col not in result.columns and not col.endswith(("_base", "_p")):
            result[col] = merged[col]

    return result
