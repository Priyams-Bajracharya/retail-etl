#!/usr/bin/env python3
"""
generate_data.py: builds five days of the five Assignment 2.3 source files.

    item_master_YYYYMMDD.csv          daily snapshot  -> DIM_ITEM, DIM_SUBCATG, DIM_CATG
    store_master_YYYYMMDD.csv         store snapshot  -> DIM_LOCN, DIM_DISTRICT, DIM_REGN
    pos_sales_YYYYMMDD.csv            daily sales     -> FCT_SALES
    inventory_snapshot_YYYYMMDD.csv   daily position  -> FCT_INVENTORY
    customer_master_YYYYMMDD.csv      customer snapshot -> DIM_CUSTOMER

Every day gets all five files, so a sensor can wait on the five of them.

Day 3 carries five deliberate bad sales rows:
    1. two sales rows with a null item_id
    2. one sales row with a negative quantity
    3. one sale for an item that is not in the item master
    4. one duplicated transaction line
and one SCD2 event: store 1105 moves district (in store_master).

Other, legitimate changes happen on other days so the dimension loads have work to do:
    day 2: item 10000010 changes brand (SCD2)
    day 4: item 10000020 is renamed (SCD1), item 10000030 moves subclass (SCD2),
           and a new item 10000151 appears

Output (under --out):
    landing/    the 25 files the pipeline reads
    expected/   answers for tests and reconciliation (the pipeline must not read these)

Usage:  python generate_data.py --start-date 2026-11-09 --out ./data
Column names live in COLUMNS below, so changing a file layout is a one-place edit.
"""
from __future__ import annotations

import argparse
import csv
import random
from datetime import date, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

SEED = 42
N_DAYS = 5
DEFECT_DAY_INDEX = 2  # Day 3

COLUMNS = {
    "item_master": ["item_id", "item_name", "brand_name", "subclass_id", "subclass_name",
                    "category_id", "category_name"],
    "store_master": ["store_id", "store_name", "district_id", "district_name",
                     "region_id", "region_name", "open_date"],
    "customer_master": ["customer_id", "customer_name", "loyalty_flag"],
    "pos_sales": ["txn_id", "txn_line_id", "business_date", "store_id", "customer_id", "item_id",
                  "quantity", "unit_price", "discount_amount", "net_amount"],
    "inventory_snapshot": ["snapshot_date", "store_id", "item_id", "on_hand_qty"],
}

BRANDS = ["Cedar", "Maple", "Willow", "Aspen", "Juniper", "Hazel", "Laurel", "Rowan", "Elm", "Alder"]
# (subclass_id, subclass_name, category_id, category_name)
SUBCLASSES = [
    (201, "Tees", 10, "Apparel"), (202, "Jeans", 10, "Apparel"), (203, "Jackets", 10, "Apparel"),
    (204, "Sneakers", 11, "Footwear"), (205, "Boots", 11, "Footwear"), (206, "Sandals", 11, "Footwear"),
    (207, "Bags", 12, "Accessories"), (208, "Belts", 12, "Accessories"), (209, "Hats", 12, "Accessories"),
    (210, "Throws", 13, "Home"), (211, "Candles", 13, "Home"), (212, "Mugs", 13, "Home"),
    (213, "Tents", 14, "Outdoor"), (214, "Packs", 14, "Outdoor"), (215, "Bottles", 14, "Outdoor"),
]
REGIONS = {11: "Northeast", 12: "Midwest"}
DISTRICTS = {101: 11, 102: 11, 103: 12, 104: 12}  # district_id -> region_id
N_BASE_ITEMS = 150
NEW_ITEM_ID = 10000151

MONEY = Decimal("0.01")


def money(value) -> Decimal:
    return Decimal(value).quantize(MONEY, rounding=ROUND_HALF_UP)


def write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


# ----------------------------------------------------------------- reference data
def build_items() -> dict[int, dict]:
    items = {}
    for i in range(N_BASE_ITEMS):
        item_id = 10000001 + i
        sc = SUBCLASSES[i % len(SUBCLASSES)]
        brand = BRANDS[i % len(BRANDS)]
        items[item_id] = {
            "item_id": item_id,
            "item_name": f"{brand} {sc[1]} {item_id % 1000:03d}",
            "brand_name": brand,
            "subclass_id": sc[0], "subclass_name": sc[1],
            "category_id": sc[2], "category_name": sc[3],
        }
    items[10000010]["brand_name"] = "Alder"  # fixed starting brand for the day 2 change
    items[10000010]["item_name"] = f"Alder {items[10000010]['subclass_name']} 010"
    return items


def items_for_day(base: dict[int, dict], day_idx: int) -> list[dict]:
    items = {k: dict(v) for k, v in base.items()}
    if day_idx >= 1:  # SCD2: brand change
        items[10000010]["brand_name"] = "Birch"
    if day_idx >= 3:
        items[10000020]["item_name"] += " Deluxe"  # SCD1: name change
        sc = SUBCLASSES[2]  # SCD2: subclass change
        items[10000030].update(subclass_id=sc[0], subclass_name=sc[1], category_id=sc[2], category_name=sc[3])
        sc = SUBCLASSES[10]
        items[NEW_ITEM_ID] = {
            "item_id": NEW_ITEM_ID, "item_name": "Alder Candles 151", "brand_name": "Alder",
            "subclass_id": sc[0], "subclass_name": sc[1], "category_id": sc[2], "category_name": sc[3],
        }
    return [items[k] for k in sorted(items)]


def build_stores(rng: random.Random) -> list[dict]:
    stores = []
    store_ids = list(range(1101, 1113))
    for n, store_id in enumerate(store_ids):
        district_id = 101 + n // 3
        stores.append({
            "store_id": store_id, "store_name": f"Northline Store {store_id}",
            "district_id": district_id, "open_date": date(2010 + rng.randint(0, 12), rng.randint(1, 12), 1).isoformat(),
        })
    return stores


def stores_for_day(base: list[dict], day_idx: int) -> list[dict]:
    rows = []
    for s in base:
        district_id = s["district_id"]
        if s["store_id"] == 1105 and day_idx >= DEFECT_DAY_INDEX:
            district_id = 101  # SCD2 event: store 1105 moves from district 102 to 101
        region_id = DISTRICTS[district_id]
        rows.append({
            "store_id": s["store_id"], "store_name": s["store_name"],
            "district_id": district_id, "district_name": f"District {district_id}",
            "region_id": region_id, "region_name": REGIONS[region_id], "open_date": s["open_date"],
        })
    return rows

def build_customers() -> list[dict]:
    rng = random.Random(f"{SEED}-customers")
    return [{"customer_id": 5000001 + i, "customer_name": f"Customer {5000001 + i}",
             "loyalty_flag": "Y" if rng.random() < 0.6 else "N"} for i in range(200)]

# --------------------------------------------------------------------------- sales
def build_sales(day: date, day_idx: int, store_ids: list[int], assortment: dict[int, list[int]],
                prices: dict[int, Decimal], rng: random.Random) -> list[tuple[dict, str, str]]:
    """Returns (row, status, detail); status is 'ok' or a defect name."""
    rows: list[tuple[dict, str, str]] = []
    for store in store_ids:
        items_today = list(assortment[store])
        if day_idx >= 3:
            items_today.append(NEW_ITEM_ID)
        for t in range(1, rng.randint(10, 18) + 1):
            txn_id = f"{store}-{day:%Y%m%d}-{t:04d}"
            for line in range(1, rng.randint(1, 4) + 1):
                item = rng.choice(items_today)
                qty = rng.randint(1, 3)
                gross = prices[item] * qty
                disc = money(gross * Decimal("0.10")) if rng.random() < 0.15 else Decimal("0.00")
                rows.append(({
                    "txn_id": txn_id, "txn_line_id": line, "business_date": day.isoformat(),
                    "store_id": store, "item_id": item, "quantity": qty,
                    "unit_price": f"{prices[item]:.2f}", "discount_amount": f"{disc:.2f}",
                    "net_amount": f"{money(gross - disc):.2f}",
                }, "ok", ""))
    if day_idx == DEFECT_DAY_INDEX:
        def defect_row(seq: int, item, qty: int, price: str) -> dict:
            gross = Decimal(price) * qty
            return {
                "txn_id": f"1101-{day:%Y%m%d}-{seq:04d}", "txn_line_id": 1, "business_date": day.isoformat(),
                "store_id": 1101, "item_id": item, "quantity": qty, "unit_price": price,
                "discount_amount": "0.00", "net_amount": f"{money(gross):.2f}",
            }
        originals = [r for r in rows if r[1] == "ok"]
        dup_src = rng.choice(originals)[0]
        extras = [
            (defect_row(9001, "", 1, "24.99"), "null_item_id", "item_id is blank"),
            (defect_row(9002, "", 2, "12.50"), "null_item_id", "item_id is blank"),
            (defect_row(9003, 10000005, -3, "19.99"), "negative_quantity", "quantity is -3"),
            (defect_row(9004, 10009999, 1, "39.99"), "item_not_in_master", "item 10009999 is not in item_master"),
            (dict(dup_src), "duplicate_line", f"copy of {dup_src['txn_id']} line {dup_src['txn_line_id']}"),
        ]
        for extra in extras:
            rows.insert(rng.randint(0, len(rows)), extra)
    return rows


# ---------------------------------------------------------------------- inventory
def build_inventory(day: date, day_idx: int, state: dict[tuple[int, int], int], store_ids: list[int],
                    assortment: dict[int, list[int]], rng: random.Random) -> list[dict]:
    rows = []
    for store in store_ids:
        items_today = list(assortment[store]) + ([NEW_ITEM_ID] if day_idx >= 3 else [])
        for item in sorted(items_today):
            key = (store, item)
            if key not in state:
                state[key] = rng.randint(0, 80)
            elif rng.random() < 0.35:  # most positions stay unchanged from one day to the next
                state[key] = max(0, state[key] + rng.randint(-6, 20))
            rows.append({"snapshot_date": day.isoformat(), "store_id": store, "item_id": item,
                         "on_hand_qty": state[key]})
    return rows


# --------------------------------------------------------------------------- main
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-date", default="2026-11-09")
    ap.add_argument("--out", default="./data")
    args = ap.parse_args()
    start = date.fromisoformat(args.start_date)
    out = Path(args.out)
    landing, expected = out / "landing", out / "expected"

    setup_rng = random.Random(SEED)
    base_items = build_items()
    base_stores = build_stores(setup_rng)
    store_ids = [s["store_id"] for s in base_stores]
    prices = {k: money(setup_rng.uniform(4.99, 89.99)) for k in list(base_items) + [NEW_ITEM_ID]}
    assortment = {s: sorted(setup_rng.sample(list(base_items), int(N_BASE_ITEMS * 0.65))) for s in store_ids}
    inv_state: dict[tuple[int, int], int] = {}
    customers = build_customers()
    cust_ids = [c["customer_id"] for c in customers]

    totals, defects = [], []
    for day_idx in range(N_DAYS):
        day = start + timedelta(days=day_idx)
        tag = f"{day:%Y%m%d}"
        rng = random.Random(f"{SEED}-{tag}")

        write_csv(landing / f"item_master_{tag}.csv", COLUMNS["item_master"], items_for_day(base_items, day_idx))
        write_csv(landing / f"store_master_{tag}.csv", COLUMNS["store_master"], stores_for_day(base_stores, day_idx))
        write_csv(landing / f"customer_master_{tag}.csv", COLUMNS["customer_master"], customers)

        sales = build_sales(day, day_idx, store_ids, assortment, prices, rng)
        cust_rng, cust_of = random.Random(f"{SEED}-cust-{tag}"), {}
        for r, _, _ in sales:  # one customer per transaction; blank = anonymous sale
            if r["txn_id"] not in cust_of:
                cust_of[r["txn_id"]] = "" if cust_rng.random() < 0.30 else cust_rng.choice(cust_ids)
            r["customer_id"] = cust_of[r["txn_id"]]
        write_csv(landing / f"pos_sales_{tag}.csv", COLUMNS["pos_sales"], [r for r, _, _ in sales])
        write_csv(landing / f"inventory_snapshot_{tag}.csv", COLUMNS["inventory_snapshot"],
                  build_inventory(day, day_idx, inv_state, store_ids, assortment, rng))

        clean = [r for r, status, _ in sales if status == "ok"]
        for r, status, detail in sales:
            if status != "ok":
                defects.append({"business_date": day.isoformat(), "file": f"pos_sales_{tag}.csv",
                                "defect": status, "txn_id": r["txn_id"], "txn_line_id": r["txn_line_id"],
                                "store_id": r["store_id"], "item_id": r["item_id"], "detail": detail})
        totals.append({
            "business_date": day.isoformat(), "pos_sales_rows_in_file": len(sales),
            "rows_expected_clean": len(clean), "rows_expected_quarantined": len(sales) - len(clean),
            "net_sales_in_file": f"{sum(Decimal(r['net_amount']) for r, _, _ in sales):.2f}",
            "net_sales_expected_clean": f"{sum(Decimal(r['net_amount']) for r in clean):.2f}",
        })

    d = lambda i: (start + timedelta(days=i)).isoformat()
    defects.append({"business_date": d(DEFECT_DAY_INDEX), "file": f"store_master_{start + timedelta(days=DEFECT_DAY_INDEX):%Y%m%d}.csv",
                    "defect": "district_change", "txn_id": "", "txn_line_id": "", "store_id": 1105,
                    "item_id": "", "detail": "store 1105 moves from district 102 to 101 (SCD2 event)"})
    write_csv(expected / "expected_totals.csv", list(totals[0]), totals)
    write_csv(expected / "defects_manifest.csv", list(defects[0]), defects)
    write_csv(expected / "scd_events.csv", ["business_date", "table", "key", "attribute", "change", "scd_type"], [
        {"business_date": d(1), "table": "item_master", "key": 10000010, "attribute": "brand_name", "change": "Alder -> Birch", "scd_type": 2},
        {"business_date": d(2), "table": "store_master", "key": 1105, "attribute": "district_id", "change": "102 -> 101", "scd_type": 2},
        {"business_date": d(3), "table": "item_master", "key": 10000020, "attribute": "item_name", "change": "renamed (adds 'Deluxe')", "scd_type": 1},
        {"business_date": d(3), "table": "item_master", "key": 10000030, "attribute": "subclass_id", "change": "215 -> 203", "scd_type": 2},
        {"business_date": d(3), "table": "item_master", "key": NEW_ITEM_ID, "attribute": "(new item)", "change": "insert", "scd_type": ""},
    ])
    print(f"Wrote {N_DAYS * 5} files to {landing} and 3 answer files to {expected}")


if __name__ == "__main__":
    main()
