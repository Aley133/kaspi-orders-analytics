
from __future__ import annotations

from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base

# TODO: If your project already defines Base elsewhere, import it instead of creating a new one.
# from app.db import Base
Base = declarative_base()

class StoreSettings(Base):
    __tablename__ = "store_settings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    business_day_start = Column(String(5), nullable=False, default="20:00")  # "HH:MM"
    timezone = Column(String(64), nullable=False, default="Asia/Almaty")
