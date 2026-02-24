"""
helpers/pricing.py
==================
Functions for determining the currently valid EK and VK prices from
rows that carry multiple price slots with validity date ranges.
"""

from __future__ import annotations

import pandas as pd

# Single module-level reference date so every function uses the same value.
_TODAY = pd.Timestamp.today().normalize()


def get_current_ekpreis(row: pd.Series) -> float:
    """
    Return the currently valid EK price for a product row.

    Logic:
    - Collect every (valid_from_date, price) pair where valid_from_date <= today.
    - Return the price with the most recent valid_from_date (i.e. the one that
      became active last).
    - Fall back to EKPREIS1 when no valid option is found.
    """
    candidates: list[tuple[pd.Timestamp, float]] = []

    if pd.notna(row["EKPREIS1"]) and pd.notna(row["EKVALIDD1"]):
        if row["EKVALIDD1"] <= _TODAY:
            candidates.append((row["EKVALIDD1"], row["EKPREIS1"]))

    if pd.notna(row["EKPREIS2"]) and pd.notna(row["EKVALIDD2"]):
        if row["EKVALIDD2"] <= _TODAY:
            candidates.append((row["EKVALIDD2"], row["EKPREIS2"]))

    if candidates:
        return max(candidates, key=lambda x: x[0])[1]

    # No valid dated price found → use EKPREIS1 as unconditional fallback
    return row["EKPREIS1"]


def determine_current_vkpreis(
    row: pd.Series,
    reference_date: pd.Timestamp | None = None,
) -> float:
    """
    Return the currently valid VK price for a product row.

    Logic:
    - A price slot is valid when today falls within [VKVALIDD, VKBISDT].
    - VKBISDT is optional; when absent the slot is treated as open-ended.
    - Among all valid slots, the one with the latest start date wins.
    - Fall back to VKPREIS1 when no slot is valid today.

    Parameters
    ----------
    row : pd.Series
        Must contain VKPREIS1, VKVALIDD1, VKBISDT1, VKPREIS2, VKVALIDD2, VKBISDT2.
    reference_date : pd.Timestamp, optional
        Override 'today' – useful for back-testing. Defaults to today (UTC-normalised).
    """
    today = (reference_date or _TODAY).normalize()
    candidates: list[tuple[pd.Timestamp, float]] = []

    def _add_slot(price_key: str, start_key: str, end_key: str) -> None:
        if pd.isna(row.get(price_key)) or pd.isna(row.get(start_key)):
            return
        start = pd.to_datetime(row[start_key], errors="coerce", dayfirst=False)
        end = (
            pd.to_datetime(row[end_key], errors="coerce", dayfirst=False)
            if pd.notna(row.get(end_key))
            else pd.Timestamp.max
        )
        if pd.notna(start) and start <= today <= end:
            candidates.append((start, row[price_key]))

    _add_slot("VKPREIS1", "VKVALIDD1", "VKBISDT1")
    _add_slot("VKPREIS2", "VKVALIDD2", "VKBISDT2")

    if candidates:
        return max(candidates, key=lambda x: x[0])[1]

    return row["VKPREIS1"]
