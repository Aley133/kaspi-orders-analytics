from __future__ import annotations
import sqlite3
from typing import Dict, Optional
from pathlib import Path
from ..services.analytics import list_numbers
from ..core.config import settings

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "app.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

DEFAULT_CONFIG = {
    "commission_percent": 12.0,  # дефолт по Kaspi Гиду
    "acquiring_percent": 0.0,    # обычно эквайринг включён в маркетплейс-фии
    "delivery_fixed": 0.0,
    "other_fixed": 0.0
}

def _conn():
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL;")
    return con

def ensure_db():
    with _conn() as c:
        c.execute("""
        CREATE TABLE IF NOT EXISTS costs (
          number TEXT PRIMARY KEY,
          cost REAL NOT NULL DEFAULT 0,
          note TEXT, updated_at TEXT NOT NULL
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS config (
          key TEXT PRIMARY KEY, value TEXT NOT NULL
        )""")

def get_config()->Dict[str,float]:
    ensure_db()
    with _conn() as c:
        rows = c.execute("SELECT key,value FROM config").fetchall()
    if not rows:
        set_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG.copy()
    cfg = DEFAULT_CONFIG.copy()
    for k,v in rows:
        try: cfg[k]=float(v)
        except: pass
    return cfg

def set_config(cfg: Dict[str,float]):
    ensure_db()
    with _conn() as c:
        for k,v in cfg.items():
            c.execute("""INSERT INTO config(key,value) VALUES(?,?)
                         ON CONFLICT(key) DO UPDATE SET value=excluded.value""", (k, str(v)))

def set_cost(number: str, cost: float, note: Optional[str]=None):
    ensure_db()
    with _conn() as c:
        c.execute("""
        INSERT INTO costs(number,cost,note,updated_at)
        VALUES(?,?,?,datetime('now'))
        ON CONFLICT(number) DO UPDATE SET cost=excluded.cost, note=excluded.note, updated_at=datetime('now')
        """, (number, float(cost), note))

def get_costs_map()->Dict[str,float]:
    ensure_db()
    with _conn() as c:
        rows = c.execute("SELECT number,cost FROM costs").fetchall()
    return {r[0]: float(r[1]) for r in rows}

def compute_profit_for_range(*, start: str, end: str, tz: str, date_field: str,
                             states: Optional[str], exclude_canceled: bool,
                             end_time: Optional[str], cutoff_mode: bool, cutoff: str,
                             lookback_days: int):
    cfg = get_config()
    items = list_numbers(start=start, end=end, tz=tz, date_field=date_field,
                         states=states, exclude_canceled=exclude_canceled,
                         end_time=end_time, cutoff_mode=cutoff_mode,
                         cutoff=cutoff, lookback_days=lookback_days)
    costs = get_costs_map()
    totals = {"gross":0.0,"commission":0.0,"acquiring":0.0,"delivery_fixed":0.0,"other_fixed":0.0,"costs":0.0,"net":0.0}
    rows = []
    for it in items:
        gross = float(it.get("amount",0.0))
        commission = gross * (cfg["commission_percent"]/100.0)
        acquiring  = gross * (cfg["acquiring_percent"]/100.0)
        delivery_fixed = cfg["delivery_fixed"]
        other_fixed    = cfg["other_fixed"]
        cost = float(costs.get(it["number"], 0.0))
        net = gross - commission - acquiring - delivery_fixed - other_fixed - cost
        rows.append({
            "id": it["id"], "number": it["number"], "state": it["state"],
            "date": it["date"], "city": it.get("city","—"),
            "gross": round(gross,2), "commission": round(commission,2),
            "acquiring": round(acquiring,2), "delivery_fixed": round(delivery_fixed,2),
            "other_fixed": round(other_fixed,2), "cost": round(cost,2), "net": round(net,2)
        })
        totals["gross"]+=gross; totals["commission"]+=commission; totals["acquiring"]+=acquiring
        totals["delivery_fixed"]+=delivery_fixed; totals["other_fixed"]+=other_fixed; totals["costs"]+=cost; totals["net"]+=net
    for k in totals: totals[k]=round(totals[k],2)
    return {"items": rows, "totals": totals, "config": cfg}
