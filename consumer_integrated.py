"""
Final Project - Unified Kafka Consumer
FMCG Integrated Supply Chain | Group 11

Consumes all three domains into ONE MongoDB database so a single dashboard can
ask questions no individual module can answer alone.

    Manufacturing + Sales  (Harshwardhan)  fmcg.production.* fmcg.sales.*
    Logistics              (Hritik)        fmcg.shipment.*  fmcg.warehouse.*
    E-commerce behaviour   (Preeti)        raw.events

TWO INTEGRATION PROBLEMS THIS FILE SOLVES
-----------------------------------------
1. TOPIC OWNERSHIP. The integration module and the logistics module both
   produced fmcg.shipment.* and fmcg.warehouse.stock with incompatible shipment
   id formats (SHP-0000019 vs SHP-P-000097). Logistics now owns them outright -
   its events carry primary freight, FEFO picking, returns and cold-chain that
   the generic version lacked. The integration module produces manufacturing and
   sales only, which is the correct split: an integration layer should own the
   master data, the envelope and the consumer, not domain events.

2. A DIVERGENT CONTRACT. The e-commerce module publishes to raw.events with its
   own envelope, keyed on user_session. Rather than force that module to change,
   the consumer adapts it here - which is what an integration layer is for, and
   what that module's own design argued for by isolating its source behind a
   canonical schema.

THE JOIN THAT MAKES IT ONE SYSTEM
---------------------------------
Clickstream category_code ("food.snacks.biscuits") is mapped onto the shared SKU
master taxonomy (Snacks / Biscuits). Ten of the twenty browsed categories map.
The ten that do not are not an error - they are categories consumers browse
online that the supply chain carries no SKU for, and that gap is only visible
once both domains sit in one database.
"""

import argparse
import hashlib
import json
import os
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone

BASE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(BASE, "data")
DB_NAME = "fmcg_integrated"
GROUP_ID = "fmcg-final"
SCHEMA_VERSIONS = {"1.0"}
DATE_SUFFIXES = ("_timestamp", "_date", "_time", "_at")

# topic -> (collection, required payload fields, business key fields, domain)
TOPIC_MAP = {
    # --- Manufacturing & Sales (integration module produces these) ---
    "fmcg.production.batch":  ("production_batch",  ["batch_id", "line_id", "sku_id"],
                               ["batch_id"], "manufacturing"),
    "fmcg.machine.telemetry": ("machine_telemetry", ["machine_id", "line_id", "event_timestamp"],
                               ["machine_id", "event_timestamp"], "manufacturing"),
    "fmcg.quality.result":    ("quality_result",    ["batch_id", "sku_id"],
                               ["batch_id"], "manufacturing"),
    "fmcg.order.placed":      ("order_placed",      ["order_id", "outlet_id", "sku_id"],
                               ["order_id"], "sales"),
    "fmcg.sales.transaction": ("sales_transaction", ["txn_id", "outlet_id", "sku_id"],
                               ["txn_id"], "sales"),
    # --- Logistics (logistics module owns these outright) ---
    "fmcg.shipment.dispatch": ("shipment_dispatch", ["shipment_id", "sku_id"],
                               ["shipment_id"], "logistics"),
    "fmcg.shipment.delivery": ("shipment_delivery", ["shipment_id"],
                               ["shipment_id"], "logistics"),
    "fmcg.warehouse.stock":   ("warehouse_stock",   ["warehouse_id", "sku_id", "movement_type",
                                                     "event_timestamp"],
                               ["warehouse_id", "sku_id", "batch_id", "movement_type",
                                "event_timestamp"], "logistics"),
    "fmcg.warehouse.return":  ("warehouse_return",  ["return_id", "sku_id"],
                               ["return_id"], "logistics"),
    # --- E-commerce behaviour (bridged from a different contract) ---
    "raw.events":             ("clickstream",       ["user_session", "event_type", "product_id"],
                               ["user_session", "event_time", "product_id"], "ecommerce"),
}

# Clickstream taxonomy -> shared SKU master taxonomy.
# A deliberate master-data mapping decision, recorded here so it is auditable.
# None means the supply chain carries no SKU for that browsed category.
CATEGORY_BRIDGE = {
    "beverages.carbonated":    ("Beverages", "Carbonated"),
    "beverages.juice":         ("Beverages", "Juice"),
    "beverages.tea_coffee":    None,
    "dairy.milk":              ("Dairy", "Milk"),
    "dairy.curd_paneer":       ("Dairy", "Curd"),
    "food.snacks.biscuits":    ("Snacks", "Biscuits"),
    "food.snacks.chips":       ("Snacks", "Chips"),
    "food.snacks.chocolate":   None,
    "food.staples.atta":       ("Staples", "Atta"),
    "food.staples.edible_oil": ("Staples", "Edible Oil"),
    "food.staples.rice":       None,
    "home_care.detergent":     ("Home Care", "Detergent"),
    "home_care.dishwash":      None,
    "home_care.cleaners":      None,
    "personal_care.soap":      ("Personal Care", "Soap"),
    "personal_care.shampoo":   None,
    "personal_care.oral_care": None,
    "personal_care.skincare":  None,
    "baby_care.diapers":       None,
    "baby_care.baby_food":     None,
}


def load_master(data_dir=DATA):
    import csv

    def read(fname, key):
        path = os.path.join(data_dir, fname)
        if not os.path.exists(path):
            return {}
        with open(path, newline="", encoding="utf-8") as fh:
            return {r[key]: r for r in csv.DictReader(fh)}

    sku = read("dim_sku.csv", "sku_id")
    outlet = read("dim_outlet.csv", "outlet_id")
    depot = read("dim_depot.csv", "depot_id")
    plant = {}
    path = os.path.join(data_dir, "dim_plant_line_machine.csv")
    if os.path.exists(path):
        with open(path, newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                plant[r["line_id"]] = r
    return sku, outlet, depot, plant


def parse_date(value):
    if not isinstance(value, str) or not value:
        return None
    text = value.replace(" UTC", "").replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def adapt_clickstream(msg):
    """Wrap the e-commerce module's FLAT canonical event in the shared envelope.

    That module publishes its own canonical schema with no envelope at all -
    event_time, event_type, product_id, category_code, brand, price, user_id,
    user_session, source - because its pipeline was designed to stand alone.
    The fmcg modules wrap everything in an envelope carrying event_id,
    producer_module, schema_version, key and payload.

    Rather than force that module to change, the integration layer adapts it
    here. This function is the ONLY place in the system that knows two
    contracts exist, which is exactly where that knowledge belongs.

    event_id is derived from the business key rather than generated, so a
    replay of the same message produces the same id and stays traceable.
    """
    session = msg.get("user_session")
    when = msg.get("event_time")
    product = msg.get("product_id")
    digest = hashlib.sha1(f"{session}|{when}|{product}".encode()).hexdigest()[:24]
    return {
        "event_id": "click-" + digest,
        "event_type": "clickstream.event",
        "event_timestamp": when,
        "producer_module": "ecommerce",
        "schema_version": "1.0",
        "key": str(session) if session else "UNKNOWN",
        "payload": dict(msg),
    }


class Processor:
    def __init__(self, master, enrich=True):
        self.sku, self.outlet, self.depot, self.plant = master
        self.enrich = enrich and bool(self.sku)
        self.reasons = Counter()
        # category -> representative sku rows, for the clickstream bridge
        self.by_cat = defaultdict(list)
        for row in self.sku.values():
            self.by_cat[(row.get("category"), row.get("sub_category"))].append(row)

    # ---- stage 1 ----
    def validate(self, env, topic):
        for field in ("event_id", "event_type", "producer_module",
                      "schema_version", "key", "payload"):
            if field not in env:
                raise ValueError(f"missing envelope field: {field}")
        if env["schema_version"] not in SCHEMA_VERSIONS:
            raise ValueError(f"unrecognised schema_version: {env['schema_version']}")
        if topic not in TOPIC_MAP:
            raise ValueError(f"unknown topic: {topic}")
        payload = env["payload"]
        if not isinstance(payload, dict):
            raise ValueError("payload is not an object")
        for field in TOPIC_MAP[topic][1]:
            if payload.get(field) in (None, ""):
                raise ValueError(f"missing required payload field: {field}")

    # ---- stage 2 ----
    @staticmethod
    def business_key(topic, payload):
        collection, _, key_fields, _ = TOPIC_MAP[topic]
        parts = []
        for field in key_fields:
            value = payload.get(field)
            if value in (None, ""):
                raise ValueError(f"missing business key field: {field}")
            parts.append(str(value))
        return collection + "|" + "|".join(parts)

    # ---- stage 3 ----
    def bridge_clickstream(self, payload):
        """Map the e-commerce taxonomy onto the shared SKU master taxonomy."""
        code = payload.get("category_code")
        mapped = CATEGORY_BRIDGE.get(code, None) if code else None
        if mapped:
            payload["sku_category"], payload["sku_sub_category"] = mapped
            payload["supply_chain_covered"] = 1
            # attach a representative SKU so clickstream can be compared to sell-out
            rows = self.by_cat.get(mapped, [])
            if rows:
                payload["mapped_sku_id"] = rows[0]["sku_id"]
        else:
            payload["sku_category"] = None
            payload["sku_sub_category"] = None
            # The finding, not an error: browsed online, no SKU in the chain.
            payload["supply_chain_covered"] = 0
        # The clickstream payload carries its own event_type (view / cart /
        # purchase). The envelope also has an event_type, and doc["event_type"]
        # is set from the envelope downstream - which would silently destroy the
        # browse action, the single most important field in this collection.
        # Rename it on the way in so both survive.
        if payload.get("event_type") is not None:
            payload["click_type"] = payload.pop("event_type")
        # normalise the field name so every collection shares one time field
        if payload.get("event_time") and not payload.get("event_timestamp"):
            payload["event_timestamp"] = payload["event_time"]

    def resolve(self, payload, topic):
        if topic == "raw.events":
            self.bridge_clickstream(payload)
            return
        if not self.enrich:
            return

        sku_id = payload.get("sku_id")
        if sku_id:
            row = self.sku.get(sku_id)
            if row is None:
                raise ValueError(f"unresolved sku_id: {sku_id}")
            payload["sku_category"] = row.get("category")
            payload["sku_sub_category"] = row.get("sub_category")
            payload["sku_brand"] = row.get("brand")
            payload["sku_storage_condition"] = row.get("storage_condition")
            # shelf life is what makes the delay->expiry link computable later
            try:
                payload["sku_shelf_life_days"] = int(row.get("shelf_life_days"))
            except (TypeError, ValueError):
                pass
            try:
                payload["sku_mrp"] = float(row.get("mrp"))
            except (TypeError, ValueError):
                pass

        outlet_id = payload.get("outlet_id") or payload.get("destination_id")
        if outlet_id and outlet_id in self.outlet:
            row = self.outlet[outlet_id]
            payload["outlet_city"] = row.get("city")
            payload["outlet_state"] = row.get("state")
            payload["outlet_region"] = row.get("region")
            payload.setdefault("outlet_type", row.get("outlet_type"))

        depot_id = (payload.get("warehouse_id") or payload.get("depot_id")
                    or payload.get("source_id"))
        if depot_id and depot_id in self.depot:
            row = self.depot[depot_id]
            payload["depot_city"] = row.get("city")
            payload["depot_region"] = row.get("region")

        line_id = payload.get("line_id")
        if line_id and line_id in self.plant:
            payload.setdefault("plant_id", self.plant[line_id].get("plant_id"))

    @staticmethod
    def coerce_dates(payload):
        for key in list(payload.keys()):
            if key.endswith(DATE_SUFFIXES) and isinstance(payload[key], str):
                parsed = parse_date(payload[key])
                if parsed is not None:
                    payload[key] = parsed

    def process(self, env, topic, partition, offset):
        # The e-commerce module speaks a different contract. Adapt before
        # validating, or every one of its events fails the envelope check.
        if topic == "raw.events" and "payload" not in env:
            env = adapt_clickstream(env)
        self.validate(env, topic)
        collection, _, _, domain = TOPIC_MAP[topic]

        payload = dict(env["payload"])
        doc_id = self.business_key(topic, payload)
        self.resolve(payload, topic)
        self.coerce_dates(payload)

        event_ts = payload.get("event_timestamp")
        if isinstance(event_ts, str):
            event_ts = parse_date(event_ts)

        doc = dict(payload)
        doc["_id"] = doc_id
        doc["event_id"] = env["event_id"]
        doc["event_type"] = env["event_type"]
        doc["producer_module"] = env["producer_module"]
        doc["domain"] = domain
        doc["kafka_topic"] = topic
        doc["kafka_partition"] = partition
        doc["kafka_offset"] = offset
        doc["kafka_key"] = env["key"]
        doc["consumed_at"] = datetime.now(timezone.utc)

        log = {
            "_id": doc_id, "event_id": env["event_id"],
            "kafka_topic": topic, "kafka_partition": partition,
            "event_type": env["event_type"], "domain": domain,
            "producer_module": env["producer_module"],
            "event_timestamp": event_ts, "consumed_at": doc["consumed_at"],
        }
        return collection, doc, log


def flush(db, buffers, counts):
    from pymongo.errors import BulkWriteError
    for collection, docs in buffers.items():
        if not docs:
            continue
        try:
            db[collection].insert_many(docs, ordered=False)
            counts["inserted"] += len(docs)
        except BulkWriteError as err:
            details = err.details or {}
            counts["inserted"] += details.get("nInserted", 0)
            errors = details.get("writeErrors", [])
            dupes = sum(1 for e in errors if e.get("code") == 11000)
            counts["duplicates"] += dupes
            if len(errors) - dupes:
                counts["write_errors"] += len(errors) - dupes
        docs.clear()


def connect_mongo(uri):
    from pymongo import MongoClient
    client = MongoClient(uri, serverSelectionTimeoutMS=20000,
                         connectTimeoutMS=20000, retryWrites=True)
    client.admin.command("ping")
    return client


def main():
    ap = argparse.ArgumentParser(description="Unified Kafka -> MongoDB consumer (final project)")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--uri", default=os.environ.get("MONGODB_URI", ""))
    ap.add_argument("--db", default=DB_NAME)
    ap.add_argument("--topics", default="")
    ap.add_argument("--batch-size", type=int, default=2000)
    ap.add_argument("--idle-timeout", type=int, default=25)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--reset", action="store_true")
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args()

    print("=" * 78)
    print(" FMCG INTEGRATED SUPPLY CHAIN | Group 11 | FINAL PROJECT")
    print(" Unified Consumer - Manufacturing + Sales + Logistics + E-commerce")
    print("=" * 78)

    if not args.uri:
        print("[ERROR] no connection string. $env:MONGODB_URI = '...'  or --uri")
        sys.exit(1)
    try:
        import kafka                                             # noqa: F401
        import pymongo                                           # noqa: F401
        from kafka import KafkaConsumer
        from kafka.errors import KafkaError
    except ImportError as err:
        print(f"[ERROR] import failed: {err}")
        print("        python -m pip install kafka-python pymongo")
        sys.exit(1)

    try:
        client = connect_mongo(args.uri)
        db = client[args.db]
        print(f" Atlas       : connected, database '{args.db}'")
    except Exception as err:                                     # noqa: BLE001
        text = str(err)
        print(f"[ERROR] cannot reach Atlas: {type(err).__name__}: {err}")
        if "bad auth" in text or "Authentication failed" in text:
            print("        Atlas WAS reached - credentials, not network.")
        sys.exit(1)

    selected = set(t.strip() for t in args.topics.split(",") if t.strip())
    topics = sorted(selected) if selected else sorted(TOPIC_MAP)

    group = GROUP_ID
    if args.reset:
        group = f"{GROUP_ID}-reset-{int(time.time())}"
        print(f" Reset       : dropping collections, fresh group '{group}'")
        for coll, _, _, _ in TOPIC_MAP.values():
            db[coll].drop()
        db["event_log"].drop()
        db["dead_letter"].drop()

    try:
        consumer = KafkaConsumer(
            *topics, bootstrap_servers=args.bootstrap.split(","),
            group_id=group, auto_offset_reset="earliest", enable_auto_commit=False,
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            key_deserializer=lambda b: b.decode("utf-8") if b else None,
            consumer_timeout_ms=args.idle_timeout * 1000,
            max_poll_records=args.batch_size,
        )
        print(f" Kafka       : connected to {args.bootstrap}")
    except KafkaError as err:
        print(f"[ERROR] cannot reach Kafka at {args.bootstrap}: {err}")
        sys.exit(1)

    proc = Processor(load_master())
    print(f" Topics      : {len(topics)} across 4 domains")
    print(f" Group       : {group}")
    if args.check:
        print("=" * 78)
        print(" CHECK PASSED - both connections live. Nothing written.")
        print("=" * 78)
        consumer.close(); client.close(); return
    print("=" * 78)

    buffers, logs = defaultdict(list), []
    counts, per_domain = Counter(), Counter()
    started = time.time()

    try:
        for msg in consumer:
            counts["read"] += 1
            try:
                collection, doc, log = proc.process(
                    msg.value, msg.topic, msg.partition, msg.offset)
                buffers[collection].append(doc)
                logs.append(log)
                per_domain[doc["domain"]] += 1
            except ValueError as err:
                reason = str(err).split(":")[0]
                proc.reasons[reason] += 1
                counts["rejected"] += 1
                buffers["dead_letter"].append({
                    "_id": f"dlq-{msg.topic}-{msg.partition}-{msg.offset}",
                    "kafka_topic": msg.topic, "kafka_partition": msg.partition,
                    "kafka_offset": msg.offset, "reason": str(err),
                    "reason_class": reason,
                    "rejected_at": datetime.now(timezone.utc),
                    "raw_event": msg.value,
                })
            if counts["read"] % args.batch_size == 0:
                buffers["event_log"].extend(logs); logs = []
                flush(db, buffers, counts)
                consumer.commit()
                el = time.time() - started
                print(f"  consumed {counts['read']:>6}  inserted {counts['inserted']:>6}  "
                      f"dup {counts['duplicates']:>5}  dlq {counts['rejected']:>4}  "
                      f"{counts['read']/max(el,.01):6.1f} ev/s")
            if args.limit and counts["read"] >= args.limit:
                break
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] flushing...")

    buffers["event_log"].extend(logs)
    flush(db, buffers, counts)
    try:
        consumer.commit()
    except Exception:                                            # noqa: BLE001
        pass
    consumer.close()

    print("=" * 78)
    print(" CONSUMER SUMMARY")
    print("=" * 78)
    print(f"  {'messages read':<26} {counts['read']:>8}")
    print(f"  {'documents inserted':<26} {counts['inserted']:>8}")
    print(f"  {'duplicates suppressed':<26} {counts['duplicates']:>8}   (business key)")
    print(f"  {'rejected to dead_letter':<26} {counts['rejected']:>8}")
    print(f"  {'elapsed':<26} {time.time()-started:>8.1f} sec")
    if proc.reasons:
        print("  rejection reasons:")
        for r, n in proc.reasons.most_common():
            print(f"      {r:<46} {n:>6}")
    print("-" * 78)
    print("  by domain:")
    for d, n in per_domain.most_common():
        print(f"      {d:<24} {n:>8}")
    print("-" * 78)
    print("  collections:")
    for coll in sorted({c for c, _, _, _ in TOPIC_MAP.values()} |
                       {"event_log", "dead_letter"}):
        print(f"      {coll:<24} {db[coll].count_documents({}):>8}")
    print("=" * 78)
    client.close()


if __name__ == "__main__":
    main()
