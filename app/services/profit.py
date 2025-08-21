from __future__ import annotations
import os
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple

from .inventory import fifo_allocate

KASPI_CATEGORY_COMMISSION = {
    "Аптека": 9.0,
    "Продукты питания": 7.0,
    "Бытовая техника": 12.0,
    "Детские товары": 12.0,
    "Канцелярские товары": 12.0,
    "Компьютеры": 12.0,
    "Красота и здоровье": 12.0,
    "Мебель": 12.0,
    "Обувь": 12.0,
    "Одежда": 12.0,
    "Подарки, товары для праздников": 12.0,
    "Спорт, туризм": 12.0,
    "Строительство, ремонт": 12.0,
    "ТВ, Аудио, Видео": 12.0,
    "Телефоны и гаджеты": 12.0,
    "Товары для дома и дачи": 12.0,
    "Аксессуары": 12.0,
    "Автотовары": 12.0,
    "Досуг, книги": 12.0,
}

DEFAULT_COMMISSION_PERCENT = float(os.getenv("DEFAULT_COMMISSION_PERCENT", "12.0"))
DEFAULT_ACQUIRING_PERCENT = float(os.getenv("DEFAULT_ACQUIRING_PERCENT", "0.0"))
DEFAULT_DELIVERY_SOURCE = os.getenv("DELIVERY_FEE_SOURCE", "api")  # api|fixed|calc
DEFAULT_DELIVERY_FIXED = float(os.getenv("DELIVERY_FIXED", "0.0"))

@dataclass
class ProfitRow:
    order_code: str
    product_code: Optional[str]
    product_name: Optional[str]
    category: Optional[str]
    qty: int
    gross: float
    commission: float
    acquiring: float
    delivery: float
    cost: float
    net: float

def pick_commission_percent(category_title: Optional[str]) -> float:
    if not category_title:
        return DEFAULT_COMMISSION_PERCENT
    return KASPI_CATEGORY_COMMISSION.get(category_title, DEFAULT_COMMISSION_PERCENT)

def calc_commission(gross: float, commission_percent: float, acquiring_percent: float) -> Tuple[float,float]:
    commission = gross * (commission_percent/100.0)
    acquiring = gross * (acquiring_percent/100.0) if acquiring_percent else 0.0
    return commission, acquiring

def choose_delivery_fee(order_attrs: Dict, fallback_fixed: float) -> float:
    if DEFAULT_DELIVERY_SOURCE == "api":
        v = order_attrs.get("deliveryCostForSeller") or order_attrs.get("deliveryCost")
        if isinstance(v, (int,float)):
            return float(v)
    if DEFAULT_DELIVERY_SOURCE == "fixed":
        return fallback_fixed
    return 0.0

def allocate_fifo_cost(product_code: Optional[str], qty: int) -> float:
    if not product_code or qty<=0:
        return 0.0
    cost, _ = fifo_allocate(product_code, qty)
    return cost

def calc_profit_for_entries(order: Dict, entries: List[Dict]) -> List[ProfitRow]:
    rows: List[ProfitRow] = []
    order_code = order.get("code") or order.get("number") or order.get("id")
    delivery_fee = choose_delivery_fee(order, DEFAULT_DELIVERY_FIXED)
    total_qty = sum(int(e.get("quantity") or 0) for e in entries) or 1
    delivery_per_unit = float(delivery_fee) / total_qty
    for e in entries:
        qty = int(e.get("quantity") or 0)
        if qty<=0: continue
        base_price = float(e.get("basePrice") or e.get("price") or 0.0)
        gross = base_price * qty
        category_title = None
        cat = e.get("category")
        if isinstance(cat, dict):
            category_title = cat.get("title")
        commission_percent = pick_commission_percent(category_title)
        commission, acquiring = calc_commission(gross, commission_percent, DEFAULT_ACQUIRING_PERCENT)
        cost = allocate_fifo_cost(e.get("product_code"), qty)
        delivery = delivery_per_unit * qty
        net = gross - commission - acquiring - delivery - cost
        rows.append(ProfitRow(
            order_code=order_code,
            product_code=e.get("product_code"),
            product_name=e.get("product_name"),
            category=category_title,
            qty=qty,
            gross=gross,
            commission=commission,
            acquiring=acquiring,
            delivery=delivery,
            cost=cost,
            net=net
        ))
    return rows
