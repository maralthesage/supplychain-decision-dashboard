"""
helpers/classification.py
=========================
Vectorised product-classification helpers used to produce the MASSNAHME columns.

classify_products_vectorized  – assigns each product to a disposal/action stage
webshop_discount              – maps stage to a discount rate
calculate_neuer_vk            – computes the reduced price for webshop stages
"""

from __future__ import annotations

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Stage → discount mapping (single source of truth)
# ---------------------------------------------------------------------------
WEBSHOP_DISCOUNT_MAP: dict[str, float] = {
    "Webshop Stufe 1": 0.10,
    "Webshop Stufe 2": 0.25,
    "Webshop Stufe 3": 0.50,
    "Webshop Stufe 4": 0.75,
}


# ---------------------------------------------------------------------------
# Main classifier
# ---------------------------------------------------------------------------

def classify_products_vectorized(
    M: pd.Series,  # EK-Preis
    T: pd.Series,  # Letzte Lager Bewegung (datetime-like)
    X: pd.Series,  # Verfügbarer Lagerbestand
) -> pd.Series:
    """
    Assign each product row to an action stage.

    Rules (M = EK price, T = last warehouse movement, X = available stock):

    - If any of M, T, X is missing / zero / negative → "daten/bestand fehlen …"
    - If last movement was < 90 days ago → "" (no action needed yet)
    - X <= 9:
        M <= 99  → "Lagerverkauf Krefeld"
        M > 99   → Webshop Stufe 1/2/3 by age
    - 9 < X <= 99:
        → Webshop Stufe 1/2/3 by age
    - X > 99:
        M <= 20  → Webshop Stufe 1/2/3 or Vermarkter
        20 < M <= 99 → Webshop Stufe 2/3/Vermarkter
        M > 99   → Webshop Stufe 3/4/Vermarkter

    Age thresholds: ≤365 days → Stufe A, ≤730 → Stufe B, >730 → Vermarkter/next stage

    Parameters
    ----------
    M, T, X : pd.Series (same index)

    Returns
    -------
    pd.Series[str]
    """
    T_dt = pd.to_datetime(T, errors="coerce")
    today = pd.Timestamp.today().normalize()
    days = (today - T_dt).dt.days

    invalid = X.isna() | M.isna() | T_dt.isna() | (X <= 0) | (M <= 0)

    out = pd.Series("", index=X.index, dtype="object")
    out[invalid] = "daten/bestand fehlen, Spalten M,T,X prüfen"

    # Only rows that are: valid AND last movement >= 90 days ago
    active = ~invalid & (days >= 90)

    # Helper: age-based webshop stage label
    def _age_stage(d: pd.Series, s1: str, s2: str, s3: str) -> np.ndarray:
        """Map days-series to three stage labels based on age thresholds."""
        return np.where(d <= 365, s1, np.where(d <= 730, s2, s3))

    # --- X <= 9 ---
    m_low = active & (X <= 9)
    out[m_low & (M <= 99)] = "Lagerverkauf Krefeld"
    out[m_low & (M > 99)] = _age_stage(
        days[m_low & (M > 99)],
        "Webshop Stufe 1", "Webshop Stufe 2", "Webshop Stufe 3",
    )

    # --- 9 < X <= 99 ---
    m_mid = active & (X > 9) & (X <= 99)
    out[m_mid] = _age_stage(
        days[m_mid],
        "Webshop Stufe 1", "Webshop Stufe 2", "Webshop Stufe 3",
    )

    # --- X > 99 ---
    m_high = active & (X > 99)

    m_high_cheap = m_high & (M <= 20)
    out[m_high_cheap] = _age_stage(
        days[m_high_cheap],
        "Webshop Stufe 1", "Webshop Stufe 2", "Vermarkter",
    )

    m_high_mid = m_high & (M > 20) & (M <= 99)
    out[m_high_mid] = _age_stage(
        days[m_high_mid],
        "Webshop Stufe 2", "Webshop Stufe 3", "Vermarkter",
    )

    m_high_exp = m_high & (M > 99)
    out[m_high_exp] = _age_stage(
        days[m_high_exp],
        "Webshop Stufe 3", "Webshop Stufe 4", "Vermarkter",
    )

    return out


# ---------------------------------------------------------------------------
# Discount lookup
# ---------------------------------------------------------------------------

def webshop_discount(stage: str) -> float | str:
    """
    Return the discount rate for a webshop stage, or "" when stage is not a
    webshop stage (e.g. "Vermarkter", "Lagerverkauf Krefeld", "N/A", "").

    Parameters
    ----------
    stage : str

    Returns
    -------
    float (e.g. 0.25) or "" when no discount applies.
    """
    return WEBSHOP_DISCOUNT_MAP.get(stage, "")


# ---------------------------------------------------------------------------
# New VK price calculation
# ---------------------------------------------------------------------------

def calculate_neuer_vk(stage: str, discount: float, current_vk: float) -> float | str:
    """
    Compute the reduced VK price for webshop stages.

    Returns
    -------
    float   – reduced price when stage contains "Stufe"
    "nein"  – when no webshop stage applies
    """
    if isinstance(stage, str) and "stufe" in stage.lower():
        return current_vk * discount
    return "nein"
