from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, time
from typing import Optional, Dict, List, Iterable, Tuple

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from cachetools import TTLCache
from dotenv import load_dotenv

# Подключаем KaspiClient
try:
    from app.kaspi_client import KaspiClient
except Exception:
    from kaspi_client import KaspiClient

# --- Безопасный импорт роутера склада ---
stock_router = None
try:
    from .api.products import router as stock_router
except Exception:
    try:
        from .api.products import router as stock_router  # если main.py запускается как модуль
    except Exception:
        try:
            from .api.products import router as stock_router
        except Exception:
            stock_router = None

# Поддержка старой фабрики, если она всё ещё существует
get_products_router = None
try:
    from .api.products import get_products_router
except Exception:
    try:
        from .api.products import get_products_router
    except Exception:
        try:
            from .api.products import get_products_router
        except Exception:
            get_products_router = None

load_dotenv()

# Настройки окружения
KASPI_TOKEN = os.getenv("KASPI_TOKEN")
KASPI_BASE_URL = os.getenv("KASPI_BASE_URL")
CACHE_TTL = int(os.getenv("CACHE_TTL", "600"))

# -------------------- FastAPI --------------------
app = FastAPI(title="Kaspi Orders Analytics")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Клиент Kaspi
client = KaspiClient(token=KASPI_TOKEN, base_url=KASPI_BASE_URL) if KASPI_TOKEN else None

# Кэш заказов
orders_cache = TTLCache(maxsize=128, ttl=CACHE_TTL)

# -------------------- Подключение роутеров --------------------
if stock_router is not None:    app.include_router(stock_router)

    # Новый способ — готовый router
# app.include_router(stock_router)  # moved below app creation
elif get_products_router is not None:
    # Совместимость со старой фабрикой
    try:
        app.include_router(get_products_router(client), prefix="/products")
    except Exception:
        app.include_router(get_products_router(None), prefix="/products")
else:
    # Если вообще нет роутера
    print(
        "[WARN] Router для /api/stock не найден. "
        "Проверь app/api/products.py — должен быть либо router, либо get_products_router.",
        file=sys.stderr,
    )

# -------------------- Healthcheck --------------------
@app.get("/health")
async def health():
    return {"status": "ok"}
