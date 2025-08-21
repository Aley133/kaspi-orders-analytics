from __future__ import annotations
import sqlite3
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from pathlib import Path
from datetime import datetime, date

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "app.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def _conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with _conn() as c:
        c.execute("""
        CREATE TABLE IF NOT EXISTS inventory_batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_code TEXT NOT NULL,
            product_name TEXT,
            received_at TEXT NOT NULL,
            unit_cost REAL NOT NULL,
            qty_in INTEGER NOT NULL CHECK(qty_in>=0),
            note TEXT
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS inventory_thresholds (
            product_code TEXT PRIMARY KEY,
            threshold INTEGER NOT NULL DEFAULT 0,
            preferred_name TEXT
        )
        """ )
        c.execute("""
        CREATE TABLE IF NOT EXISTS inventory_sales_cache (
            product_code TEXT PRIMARY KEY,
            qty_sold INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        )
        """ )

init_db()

@dataclass
class Batch:
    product_code: str
    product_name: str
    received_at: str  # ISO date string
    unit_cost: float
    qty_in: int
    note: Optional[str] = None

def add_batch(batch: Batch) -> int:
    with _conn() as c:
        cur = c.execute("""
            INSERT INTO inventory_batches (product_code, product_name, received_at, unit_cost, qty_in, note)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (batch.product_code, batch.product_name, batch.received_at, batch.unit_cost, batch.qty_in, batch.note))
        return cur.lastrowid

def set_threshold(product_code: str, threshold: int, preferred_name: Optional[str]=None) -> None:
    with _conn() as c:
        c.execute("""
            INSERT INTO inventory_thresholds (product_code, threshold, preferred_name)
            VALUES (?, ?, ?)
            ON CONFLICT(product_code) DO UPDATE SET threshold=excluded.threshold,
                                                    preferred_name=COALESCE(excluded.preferred_name, inventory_thresholds.preferred_name)
        """, (product_code, threshold, preferred_name))

def get_stock() -> List[Dict]:
    with _conn() as c:
        total_in = {r["product_code"]: {"qty": r["qty"], "name": r["product_name"]}
                    for r in c.execute("""
                        SELECT product_code, COALESCE(MAX(product_name), '') as product_name, SUM(qty_in) as qty
                        FROM inventory_batches GROUP BY product_code
                    """)}
        thresholds = {r["product_code"]: {"threshold": r["threshold"], "preferred_name": r["preferred_name"]}
                      for r in c.execute("SELECT product_code, threshold, preferred_name FROM inventory_thresholds")}
        sold = {r["product_code"]: r["qty_sold"] for r in c.execute("SELECT product_code, qty_sold FROM inventory_sales_cache")}
    rows = []
    for code, info in total_in.items():
        qty_in = info["qty"] or 0
        qty_sold = sold.get(code, 0)
        qty_left = qty_in - qty_sold
        thr = thresholds.get(code, {"threshold": 0, "preferred_name": None})
        rows.append({
            "product_code": code,
            "product_name": thr.get("preferred_name") or info["name"],
            "qty_in": qty_in,
            "qty_sold": qty_sold,
            "qty_left": qty_left,
            "threshold": thr["threshold"],
            "low": qty_left <= thr["threshold"] if thr["threshold"] else False
        })
    rows.sort(key=lambda r: (r["low"]==True, -r["qty_left"]), reverse=True)
    return rows

def reset_sales_cache() -> None:
    with _conn() as c:
        c.execute("DELETE FROM inventory_sales_cache")

def apply_sales_agg(sales_agg: Dict[str, int]) -> None:
    from datetime import datetime
    now = datetime.utcnow().isoformat()
    with _conn() as c:
        for code, qty in sales_agg.items():
            c.execute("""
                INSERT INTO inventory_sales_cache (product_code, qty_sold, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(product_code) DO UPDATE SET qty_sold=excluded.qty_sold, updated_at=excluded.updated_at
            """, (code, qty, now))

def fifo_allocate(product_code: str, qty: int) -> Tuple[float, List[Dict]]:
    with _conn() as c:
        rows = c.execute("""
            SELECT id, received_at, unit_cost, qty_in FROM inventory_batches
            WHERE product_code=? ORDER BY date(received_at) ASC, id ASC
        """, (product_code,)).fetchall()
    allocations = []
    remaining = qty
    total_cost = 0.0
    for r in rows:
        if remaining <= 0:
            break
        take = min(remaining, r["qty_in"])
        if take<=0:
            continue
        allocations.append({"batch_id": r["id"], "received_at": r["received_at"], "unit_cost": r["unit_cost"], "qty": take})
        total_cost += take * float(r["unit_cost"])
        remaining -= take
    return total_cost, allocations
