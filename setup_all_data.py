"""
FINAL PROJECT - Step 1: generate every module's data, in dependency order.

Run this from the folder that CONTAINS the three module folders:

    C:\\kafka-demo\\  <- run here
        harshwardhan_a2\\
        hritik_a2\\
        preeti_a2\\

    python setup_all_data.py

WHY A SCRIPT AND NOT A LIST OF COMMANDS
---------------------------------------
The three modules are not independent. The logistics module cannot generate
anything until it has the shared master data AND three upstream event files
from the manufacturing/sales module - it picks stock FEFO against real batches
with real expiry dates, so it needs those batches to exist first. Copying six
files by hand between folders in the right order is the kind of step that goes
wrong silently: miss one and the generator runs but produces a thinner dataset,
and nothing tells you.

ORDER
  1. Manufacturing + Sales   masters, then 5 event streams
  2. -> copy masters + 3 upstream files into the logistics module
  3. Logistics               vehicle/freight masters, then 4 event streams
  4. E-commerce              clickstream (independent of the other two)
"""

import os
import shutil
import subprocess
import sys

BASE = os.path.dirname(os.path.abspath(__file__))
H = os.path.join(BASE, "harshwardhan_a2")
L = os.path.join(BASE, "hritik_a2")
P = os.path.join(BASE, "preeti_a2")

MASTERS = ["dim_sku.csv", "dim_outlet.csv", "dim_depot.csv",
           "dim_route.csv", "dim_plant_line_machine.csv"]
UPSTREAM = ["events_production_batch.csv", "events_quality_result.csv",
            "events_order_placed.csv"]


def run(script_dir, script, label):
    path = os.path.join(script_dir, script)
    if not os.path.exists(path):
        print(f"  [FAIL] {script} not found in {os.path.basename(script_dir)}")
        return False
    print(f"\n  --- {label} ---")
    r = subprocess.run([sys.executable, path], cwd=script_dir,
                       capture_output=True, text=True)
    out = (r.stdout or "").strip()
    if out:
        for line in out.splitlines():
            print("  " + line)
    if r.returncode != 0:
        print(f"  [FAIL] {script} exited {r.returncode}")
        print((r.stderr or "")[-900:])
        return False
    return True


def need(folder, label):
    if not os.path.isdir(folder):
        print(f"[FAIL] folder not found: {folder}")
        print(f"       Expected the {label} module beside this script.")
        return False
    return True


def main():
    print("=" * 74)
    print(" FMCG FINAL PROJECT - GENERATING ALL MODULE DATA")
    print("=" * 74)
    print(f" working from: {BASE}")

    for folder, label in ((H, "manufacturing/sales"), (L, "logistics"), (P, "e-commerce")):
        if not need(folder, label):
            sys.exit(1)

    # ---- 1. Manufacturing + Sales -------------------------------------
    print("\n[1/4] MANUFACTURING + SALES  (harshwardhan_a2)")
    if not run(H, "generate_master_data.py", "master data"):
        sys.exit(1)
    if not run(H, "generate_sample_events.py", "event streams"):
        sys.exit(1)

    # ---- 2. Hand the shared keys to logistics -------------------------
    print("\n[2/4] WIRING MASTERS + UPSTREAM INTO LOGISTICS")
    src = os.path.join(H, "data")
    dst = os.path.join(L, "data")
    up = os.path.join(dst, "upstream")
    os.makedirs(up, exist_ok=True)
    missing = []
    for f in MASTERS:
        s = os.path.join(src, f)
        if os.path.exists(s):
            shutil.copy2(s, os.path.join(dst, f))
            print(f"    master   {f}")
        else:
            missing.append(f)
    for f in UPSTREAM:
        s = os.path.join(src, f)
        if os.path.exists(s):
            shutil.copy2(s, os.path.join(up, f))
            print(f"    upstream {f}")
        else:
            missing.append(f)
    if missing:
        print(f"  [FAIL] missing from harshwardhan_a2/data: {missing}")
        sys.exit(1)

    # ---- 3. Logistics --------------------------------------------------
    print("\n[3/4] LOGISTICS  (hritik_a2)")
    if not run(L, "generate_logistics_master.py", "vehicle + freight masters"):
        sys.exit(1)
    if not run(L, "generate_logistics_events.py", "event streams"):
        sys.exit(1)

    # ---- 4. E-commerce -------------------------------------------------
    print("\n[4/4] E-COMMERCE  (preeti_a2)")
    # The 30-day generator is required for the integrated project: the A2
    # version replayed a single day, which against a 30-day supply chain makes
    # every time-based cross-domain chart a one-day spike.
    gen = ("generate_clickstream_30d.py"
           if os.path.exists(os.path.join(P, "generate_clickstream_30d.py"))
           else "generate_sample_data.py")
    if gen == "generate_sample_data.py":
        print("  [WARN] generate_clickstream_30d.py not found - falling back to the")
        print("         single-day A2 generator. Time-based cross-domain charts will")
        print("         show one spike. Put the 30-day generator in preeti_a2 first.")
    if not run(P, gen, "clickstream"):
        sys.exit(1)

    # ---- summary -------------------------------------------------------
    print("\n" + "=" * 74)
    print(" SUMMARY - rows generated per module")
    print("=" * 74)
    total = 0
    for folder, name in ((H, "harshwardhan_a2"), (L, "hritik_a2"), (P, "preeti_a2")):
        d = os.path.join(folder, "data")
        if not os.path.isdir(d):
            continue
        print(f"\n  {name}/data")
        for f in sorted(os.listdir(d)):
            if not f.endswith(".csv"):
                continue
            with open(os.path.join(d, f), encoding="utf-8", errors="ignore") as fh:
                n = sum(1 for _ in fh) - 1
            if f.startswith("events_") or f.startswith("clickstream"):
                total += n
            print(f"      {f:<38}{n:>8}")
    print(f"\n  {'TOTAL EVENT ROWS (all modules)':<44}{total:>8}")
    print("=" * 74)
    print(" Next: create the Kafka topics, then run the three producers.")


if __name__ == "__main__":
    main()
