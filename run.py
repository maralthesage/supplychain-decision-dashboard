"""
run.py
======
Entry point for the Artikelbestand daily pipeline.

Usage:
    python run.py
"""

from __future__ import annotations

import os
import socket
from urllib.parse import urlparse


def _is_local_prefect_api_unreachable() -> bool:
    """
    Return True when PREFECT_API_URL points to localhost but no API is reachable.
    In that case, we fallback to local ephemeral mode so `python run.py` works
    without requiring `prefect server start`.
    """
    api_url = os.environ.get("PREFECT_API_URL", "").strip()
    if not api_url:
        return False

    parsed = urlparse(api_url)
    host = (parsed.hostname or "").lower()
    port = parsed.port or 4200

    if host not in {"127.0.0.1", "localhost"}:
        return False

    try:
        with socket.create_connection((host, port), timeout=0.5):
            return False
    except OSError:
        return True


def _is_prefect_bootstrap_error(exc: BaseException) -> bool:
    """
    Identify Prefect startup failures where orchestration cannot start because
    its API/server is unavailable in the current runtime environment.
    """
    text = str(exc).lower()
    return (
        "unable to find an available port" in text
        or "failed to reach api" in text
        or "prefect server start" in text
    )


if __name__ == "__main__":
    if _is_local_prefect_api_unreachable():
        os.environ.pop("PREFECT_API_URL", None)
        os.environ.setdefault("PREFECT_SERVER_ALLOW_EPHEMERAL_MODE", "true")

    # Import after environment normalization so Prefect sees the final settings.
    from pipeline.flow import artikelbestand_flow, run_artikelbestand_pipeline_local

    try:
        artikelbestand_flow()
    except Exception as exc:
        if not _is_prefect_bootstrap_error(exc):
            raise
        print(
            "Prefect orchestration is not available in this environment; "
            "running pipeline in local mode."
        )
        run_artikelbestand_pipeline_local()
