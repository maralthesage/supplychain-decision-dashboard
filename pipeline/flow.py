"""
flow.py
=======
Prefect flow definition for the daily Artikelbestand pipeline.

This file is intentionally thin – it only wires tasks together.
No data logic lives here. Reading this file top-to-bottom gives a complete
picture of what the pipeline does and in what order.
"""

from __future__ import annotations

import os
import shutil

from prefect import flow

from pipeline.config import *
from pipeline.helpers.run_logging import get_logger
from pipeline.tasks.export import write_excel_file
from pipeline.tasks.load import (
    load_katalog_data,
    load_lager_data,
    load_lagerplatz,
    load_lieferant_data,
    load_marketing_stamm,
    load_set_artikels,
    load_vk_preis,
    load_warengruppe_data,
    process_lagerbewegung_historie,
)
from pipeline.tasks.merge import calculate_analytics, merge_all_data
from pipeline.tasks.transform import process_final_data


# ---------------------------------------------------------------------------
# Helpers (not Prefect tasks – just small utility calls)
# ---------------------------------------------------------------------------

def _backup_yesterday() -> None:
    """Copy today's (still yesterday's output) file to the local backup folder."""
    logger = get_logger(__name__)

    if not os.path.exists(PRODUCT_BESTAND_PATH):
        logger.warning(f"Backup skipped, source file not found: {PRODUCT_BESTAND_PATH}")
        return

    backup_dir = os.path.dirname(PRODUCT_BESTAND_LOCAL_BACKUP)
    if backup_dir:
        os.makedirs(backup_dir, exist_ok=True)

    logger.info(f"Backing up → {PRODUCT_BESTAND_LOCAL_BACKUP}")
    shutil.copyfile(PRODUCT_BESTAND_PATH, PRODUCT_BESTAND_LOCAL_BACKUP)


def _copy_to_history() -> None:
    """Archive today's freshly written output to the history folder."""
    logger = get_logger(__name__)

    if not os.path.exists(PRODUCT_BESTAND_PATH):
        logger.warning(f"Archive skipped, source file not found: {PRODUCT_BESTAND_PATH}")
        return

    history_dir = os.path.dirname(PRODUCT_BESTAND_HISTORIE)
    if history_dir:
        os.makedirs(history_dir, exist_ok=True)

    logger.info(f"Archiving → {PRODUCT_BESTAND_HISTORIE}")
    shutil.copyfile(PRODUCT_BESTAND_PATH, PRODUCT_BESTAND_HISTORIE)


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="Artikelbestand Daily Pipeline")
def artikelbestand_flow() -> None:
    """
    Daily pipeline that:
    1. Backs up yesterday's output.
    2. Loads all source data in parallel-friendly tasks.
    3. Merges and transforms it into a clean product DataFrame.
    4. Writes the result to Excel.
    5. Archives the output to the history folder.
    """
    # --- backup before overwriting ---
    _backup_yesterday()

    # --- load (all independent, can run in parallel with a Prefect work pool) ---
    marketing_stamm  = load_marketing_stamm()
    katalog_data     = load_katalog_data()
    vk_preis         = load_vk_preis()
    lager_data       = load_lager_data()
    lieferant_info   = load_lieferant_data()
    warengruppe      = load_warengruppe_data()
    set_artikels     = load_set_artikels()
    lagerplatz       = load_lagerplatz()
    lager_bewegung   = process_lagerbewegung_historie()

    # --- analytics depends on lager_bewegung ---
    analytics = calculate_analytics(
        lager_bewegung["lagerbewegung_historie"],
        lager_data["lager_stamm"],
    )

    # --- merge all sources ---
    merged = merge_all_data(
        marketing_stamm=marketing_stamm,
        katalog_data=katalog_data,
        vk_preis=vk_preis,
        lager_data=lager_data,
        lieferant_info=lieferant_info,
        set_artikels=set_artikels,
        warengruppe=warengruppe,
        lagerbewegung_data=lager_bewegung,
        analytics=analytics,
        lagerplatz=lagerplatz,
    )

    # --- transform & enrich ---
    final_df = process_final_data(merged)

    # --- export ---
    write_excel_file(final_df)

    # --- archive ---
    _copy_to_history()


def run_artikelbestand_pipeline_local() -> None:
    """
    Run the same pipeline steps without Prefect orchestration.
    This fallback is used when Prefect cannot bootstrap its API/server.
    """
    logger = get_logger(__name__)
    logger.info("Running pipeline in local mode (without Prefect orchestration)")

    _backup_yesterday()

    marketing_stamm = load_marketing_stamm.fn()
    katalog_data = load_katalog_data.fn()
    vk_preis = load_vk_preis.fn()
    lager_data = load_lager_data.fn()
    lieferant_info = load_lieferant_data.fn()
    warengruppe = load_warengruppe_data.fn()
    set_artikels = load_set_artikels.fn()
    lagerplatz = load_lagerplatz.fn()
    lager_bewegung = process_lagerbewegung_historie.fn()

    analytics = calculate_analytics.fn(
        lager_bewegung["lagerbewegung_historie"],
        lager_data["lager_stamm"],
    )

    merged = merge_all_data.fn(
        marketing_stamm=marketing_stamm,
        katalog_data=katalog_data,
        vk_preis=vk_preis,
        lager_data=lager_data,
        lieferant_info=lieferant_info,
        set_artikels=set_artikels,
        warengruppe=warengruppe,
        lagerbewegung_data=lager_bewegung,
        analytics=analytics,
        lagerplatz=lagerplatz,
    )

    final_df = process_final_data.fn(merged)
    write_excel_file.fn(final_df)
    _copy_to_history()

    logger.info("Local pipeline run completed")
