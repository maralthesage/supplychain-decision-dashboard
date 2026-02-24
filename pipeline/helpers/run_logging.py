"""
helpers/run_logging.py
======================
Logging helper that works both inside and outside a Prefect run context.
"""

from __future__ import annotations

import logging

from prefect.exceptions import MissingContextError
from prefect.logging import get_run_logger


def get_logger(name: str = "artikelbestand_pipeline") -> logging.Logger:
    """
    Return Prefect's run logger when available; otherwise return a standard
    Python logger for local/offline execution.
    """
    try:
        return get_run_logger()
    except MissingContextError:
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            )
        return logging.getLogger(name)
