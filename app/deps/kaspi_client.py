# app/deps/kaspi_client.py
"""
Compatibility shim: keep old import path working.

Usage in code can stay as:
    from app.deps.kaspi_client import KaspiClient
"""

from .kaspi_client_tenant import KaspiClient  # re-export
__all__ = ["KaspiClient"]
