from __future__ import annotations
import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    KASPI_TOKEN: str = os.getenv("KASPI_TOKEN","")
    TZ: str = os.getenv("TZ","Asia/Almaty")
    CACHE_TTL: int = int(os.getenv("CACHE_TTL","300"))
    CURRENCY: str = os.getenv("CURRENCY","KZT")

    AMOUNT_FIELDS = [s.strip() for s in os.getenv("AMOUNT_FIELDS","totalPrice").split(",") if s.strip()]
    AMOUNT_DIVISOR: float = float(os.getenv("AMOUNT_DIVISOR","1"))

    DAY_CUTOFF: str = os.getenv("DAY_CUTOFF","20:00")
    PACK_LOOKBACK_DAYS: int = int(os.getenv("PACK_LOOKBACK_DAYS","3"))

settings = Settings()
