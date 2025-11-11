#!/usr/bin/env python3
import argparse
import csv
import sys
import time
import json
from typing import Any, Dict, List, Tuple
import requests

def to_hex(n: int) -> str:
    return hex(n)

def build_payload(req_id: int, address: str, slot: str, block_number: int) -> Dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": req_id,
        "method": "eth_getProof",
        "params": [address, [slot], to_hex(block_number)],
    }

def build_curl(rpc_url: str, payload: Dict[str, Any]) -> str:
    body = json.dumps(payload, separators=(",", ":"))
    return f"curl -s -X POST '{rpc_url}' -H 'Content-Type: application/json' -d '{body}'"

def main():
    parser = argparse.ArgumentParser(description="Benchmark eth_getProof over rows from a CSV.")
    parser.add_argument("--rpc", required=True)
    parser.add_argument("--in", dest="inp", required=True, default="dataset.csv")
    parser.add_argument("--fail-out", default="eth_getproof_failures.txt")
    args = parser.parse_args()

    session = requests.Session()
    TIMEOUT = 60

    try:
        fh = open(args.inp, "r", newline="")
    except Exception as e:
        print(f"Error opening CSV: {e}", file=sys.stderr)
        sys.exit(1)

    reader = csv.DictReader(fh)
    required = {"block_number", "to", "storage_slot"}
    missing = [x for x in required if x not in reader.fieldnames]
    if missing:
        print(f"CSV missing columns: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    rows = [r for r in reader]
    fh.close()

    filtered: List[Tuple[int, str, str]] = []
    for r in rows:
        slot = (r.get("storage_slot") or "").strip()
        addr = (r.get("to") or "").strip()
        bn_raw = (r.get("block_number") or "").strip()

        if not addr or not slot or slot.lower() == "null" or not bn_raw:
            continue

        try:
            bn = int(bn_raw)
        except ValueError:
            continue

        filtered.append((bn, addr, slot))

    total = len(filtered)
    if total == 0:
        print("No usable rows.")
        sys.exit(0)

    latencies: List[float] = []
    successes = 0
    failures = 0
    req_id = 0
    failure_records: List[str] = []

    for i, (bn, addr, slot) in enumerate(filtered, start=1):
        req_id += 1
        payload = build_payload(req_id, addr, slot, bn)
        started = time.monotonic()

        http_status = None
        body_text = None
        ok = False
        error_msg = None

        try:
            resp = session.post(args.rpc, json=payload, timeout=TIMEOUT)
            http_status = resp.status_code
            body_text = resp.text
            resp.raise_for_status()

            data = resp.json()
            if "error" in data:
                ok = False
                error_msg = f"RPC error {data['error'].get('code')}: {data['error'].get('message')}"
            else:
                res = data.get("result")
                if isinstance(res, dict) and "accountProof" in res and "storageProof" in res:
                    ok = True
                else:
                    ok = False
                    error_msg = "Missing expected fields in result."

        except Exception as e:
            ok = False
            error_msg = str(e)

        elapsed = time.monotonic() - started
        latencies.append(elapsed)

        if ok:
            print(f"[{i}/{total}] SUCCESS block={bn} addr={addr} slot={slot}")
            successes += 1
        else:
            print(f"[{i}/{total}] FAILURE block={bn} addr={addr} slot={slot} error={error_msg}")
            failures += 1
            failure_records.append("# ---- FAILURE -----------------------------------------")
            failure_records.append(f"# #{i}/{total} block={bn} addr={addr} slot={slot}")
            if http_status:
                failure_records.append(f"# HTTP {http_status}")
            if error_msg:
                failure_records.append(f"# Error: {error_msg}")
            failure_records.append(build_curl(args.rpc, payload))
            if body_text:
                failure_records.append("# Response:")
                failure_records.append(body_text)
            failure_records.append("")

    lat_sorted = sorted(latencies)
    p50 = lat_sorted[int(0.5*(len(lat_sorted)-1))]
    p95 = lat_sorted[int(0.95*(len(lat_sorted)-1))]
    p99 = lat_sorted[int(0.99*(len(lat_sorted)-1))] if len(lat_sorted) >= 100 else lat_sorted[-1]
    fail_rate = (failures / total) * 100.0

    print("----- eth_getProof Benchmark -----")
    print(f"Endpoint         : {args.rpc}")
    print(f"Requests         : {total}")
    print(f"Successes        : {successes}")
    print(f"Failures         : {failures}")
    print(f"Fail rate        : {fail_rate:.2f}%")
    print(f"P50 latency      : {p50:.3f}s")
    print(f"P95 latency      : {p95:.3f}s")
    print(f"P99 latency      : {p99:.3f}s")
    print("----------------------------------")

    if failure_records:
        try:
            with open(args.fail_out, "w", encoding="utf-8") as fo:
                fo.write("\n".join(failure_records))
            print(f"Logged failures to {args.fail_out}")
        except Exception as e:
            print(f"Could not write failure log: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
