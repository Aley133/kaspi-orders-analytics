# app/deps/kaspi_client.py
"""
Тонкий ре-экспорт клиентa Kaspi.
Нужен, чтобы main.py мог импортировать KaspiClient из app.deps.kaspi_client,
а фактическая реализация оставалась в app.deps.kaspi_client_tenant.
"""

from .kaspi_client_tenant import KaspiClient

__all__ = ["KaspiClient"]
