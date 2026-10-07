"""
Final Project - build materialised analytics views in MongoDB.

    python build_analytics_views.py

WHY THIS EXISTS
---------------
The headline finding joins three collections that live in different domains:

    shipment_dispatch   sku_shelf_life_days   (SKU master, via the consumer)
    shipment_delivery   delay_days            (logistics)
    warehouse_return    return_reason         (reverse logistics)

Atlas Charts charts ONE collection at a time. Rather than make the dashboard
run a cross-collection join on every refresh, the join is computed once here
and written to a small view collection that Charts can read directly. This is
the standard materialised-view pattern and it is what makes the cross-domain
chart possible at all.

Re-run it whenever the consumer reloads.

Creates:
    vw_shelf_risk     expiry rate and delay cost, banded by SKU shelf life
    vw_pipeline_kpi   one document of headline numbers, for KPI charts
"""
import argparse
import os
import sys
from collections import defaultdict

DB_NAME = "fmcg_integrated"

SHELF_BANDS = [(0, 20, "1. <=20 days"), (21, 100, "2. 21-100 days"),
               (101, 300, "3. 101-300 days"), (301, 10**6, "4. >300 days")]


def band_of(days):
    for lo, hi, label in SHELF_BANDS:
        if lo <= days <= hi:
            return label
    return "4. >300 days"


def build_shelf_risk(db):
    """Join dispatch x delivery x returns, band by shelf life, write the view."""
    # delay per shipment (secondary leg only - primary is plant-to-depot and
    # runs on a different reliability profile)
    delay = {d["shipment_id"]: d.get("delay_days", 0) for d in db.shipment_delivery.find(
        {"movement_type": "secondary"}, {"shipment_id": 1, "delay_days": 1, "_id": 0})}

    # shipments that came back as an expiry or near-expiry return
    expired = {r["shipment_id"] for r in db.warehouse_return.find(
        {"return_reason": {"$in": ["expiry", "near_expiry"]}},
        {"shipment_id": 1, "_id": 0}) if r.get("shipment_id")}

    bands = defaultdict(lambda: {"ships": 0, "exp": 0, "delay": 0.0, "life": 0})
    cats = defaultdict(lambda: defaultdict(int))
    for d in db.shipment_dispatch.find(
            {"movement_type": "secondary", "sku_shelf_life_days": {"$ne": None}},
            {"shipment_id": 1, "sku_shelf_life_days": 1, "sku_category": 1, "_id": 0}):
        life = d.get("sku_shelf_life_days")
        if life is None:
            continue
        label = band_of(life)
        b = bands[label]
        b["ships"] += 1
        b["life"] += life
        b["delay"] += delay.get(d["shipment_id"], 0)
        if d["shipment_id"] in expired:
            b["exp"] += 1
            cats[label][d.get("sku_category") or "unknown"] += 1

    docs = []
    for order, (_, _, label) in enumerate(SHELF_BANDS, start=1):
        v = bands.get(label)
        if not v or not v["ships"]:
            continue
        avg_delay = v["delay"] / v["ships"]
        avg_life = v["life"] / v["ships"]
        docs.append({
            "_id": label,
            "shelf_life_band": label[3:],          # strip the sort prefix
            "band_order": order,
            "shipments": v["ships"],
            "expiry_returns": v["exp"],
            "expiry_rate_pct": round(100.0 * v["exp"] / v["ships"], 1),
            "avg_delay_days": round(avg_delay, 2),
            "avg_shelf_life_days": round(avg_life, 0),
            "delay_pct_of_shelf_life": round(100.0 * avg_delay / avg_life, 2) if avg_life else 0.0,
            "categories_affected": ", ".join(sorted(cats[label])) or "none",
        })

    db.vw_shelf_risk.drop()
    if docs:
        db.vw_shelf_risk.insert_many(docs)
    return docs


def build_kpi(db):
    """One document of headline numbers, for Atlas Charts number/KPI charts."""
    sec = list(db.shipment_delivery.find({"movement_type": "secondary"},
                                         {"delay_days": 1, "short_qty_cases": 1,
                                          "delivered_qty_cases": 1,
                                          "delivery_status": 1, "_id": 0}))
    total = len(sec)
    otif = sum(1 for d in sec if d.get("delay_days") == 0
               and d.get("short_qty_cases") == 0
               and d.get("delivery_status") != "cancelled")
    delivered = sum(d.get("delivered_qty_cases", 0) for d in sec)
    short = sum(d.get("short_qty_cases", 0) for d in sec)

    sales = list(db.sales_transaction.find({}, {"line_value": 1, "_id": 0}))
    value = sum(s.get("line_value", 0) for s in sales)

    tel_total = db.machine_telemetry.count_documents({})
    tel_fail = db.machine_telemetry.count_documents({"machine_failure": 1})

    views = db.clickstream.count_documents({"click_type": "view"})
    carts = db.clickstream.count_documents({"click_type": "cart"})
    buys = db.clickstream.count_documents({"click_type": "purchase"})
    uncovered = db.clickstream.count_documents({"click_type": "view",
                                                "supply_chain_covered": 0})

    ret = list(db.warehouse_return.find({}, {"return_value": 1, "return_reason": 1, "_id": 0}))
    ret_value = sum(r.get("return_value", 0) for r in ret)
    exp_value = sum(r.get("return_value", 0) for r in ret
                    if r.get("return_reason") in ("expiry", "near_expiry"))

    short_band = db.vw_shelf_risk.find_one({"band_order": 1}) or {}

    doc = {
        "_id": "headline",
        "events_consumed": sum(db[c].count_documents({}) for c in (
            "production_batch", "machine_telemetry", "quality_result", "order_placed",
            "sales_transaction", "shipment_dispatch", "shipment_delivery",
            "warehouse_stock", "warehouse_return", "clickstream")),
        "dead_letter": db.dead_letter.count_documents({}),
        "shipments_secondary": total,
        "otif_pct": round(100.0 * otif / total, 1) if total else 0.0,
        "fill_rate_pct": round(100.0 * delivered / (delivered + short), 1)
        if (delivered + short) else 0.0,
        "sell_out_value": round(value, 2),
        "machine_failure_pct": round(100.0 * tel_fail / tel_total, 2) if tel_total else 0.0,
        "cart_abandonment_pct": round(100.0 * (1 - buys / carts), 1) if carts else 0.0,
        "product_views": views,
        "views_no_sku_pct": round(100.0 * uncovered / views, 1) if views else 0.0,
        "write_off_value": round(ret_value, 2),
        "expiry_write_off_value": round(exp_value, 2),
        "short_life_expiry_rate_pct": short_band.get("expiry_rate_pct", 0.0),
    }
    db.vw_pipeline_kpi.drop()
    db.vw_pipeline_kpi.insert_one(doc)
    return doc


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", default=os.environ.get("MONGODB_URI", ""))
    ap.add_argument("--db", default=DB_NAME)
    args = ap.parse_args()

    if not args.uri:
        print("[ERROR] set MONGODB_URI or pass --uri")
        sys.exit(1)
    try:
        from pymongo import MongoClient
    except ImportError:
        print("[ERROR] python -m pip install pymongo")
        sys.exit(1)

    print("=" * 72)
    print(" BUILDING ANALYTICS VIEWS")
    print("=" * 72)
    client = MongoClient(args.uri, serverSelectionTimeoutMS=20000)
    client.admin.command("ping")
    db = client[args.db]

    rows = build_shelf_risk(db)
    print("\n vw_shelf_risk")
    print(f"   {'band':<16}{'ships':>7}{'expiry':>8}{'rate':>8}{'delay':>8}{'% of life':>11}")
    for r in rows:
        print(f"   {r['shelf_life_band']:<16}{r['shipments']:>7}{r['expiry_returns']:>8}"
              f"{r['expiry_rate_pct']:>7}%{r['avg_delay_days']:>8}{r['delay_pct_of_shelf_life']:>10}%")

    kpi = build_kpi(db)
    print("\n vw_pipeline_kpi")
    for k, v in kpi.items():
        if k == "_id":
            continue
        print(f"   {k:<30}{v:>14,}" if isinstance(v, int) else f"   {k:<30}{v:>14}")

    print("\n" + "=" * 72)
    print(" Both views written. Add them as data sources in Atlas Charts.")
    print("=" * 72)
    client.close()


if __name__ == "__main__":
    main()
