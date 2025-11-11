#!/usr/bin/env python3
import argparse
import csv
import sys
import time
import json
from typing import Any, Dict, List, Tuple, Callable, Union, Optional
import requests

def to_hex(n: int) -> str:
    return hex(n)

def build_payload(req_id: int, address: str, slot: Optional[str], block_param: Union[int, str]) -> Dict[str, Any]:
    block_tag = to_hex(block_param) if isinstance(block_param, int) else block_param  # e.g., "latest"
    storage_keys = [] if slot is None else [slot]
    return {
        "jsonrpc": "2.0",
        "id": req_id,
        "method": "eth_getProof",
        "params": [address, storage_keys, block_tag],
    }

def build_curl(rpc_url: str, payload: Dict[str, Any]) -> str:
    body = json.dumps(payload, separators=(",", ":"))
    return f"curl -s -X POST '{rpc_url}' -H 'Content-Type: application/json' -d '{body}'"

def load_rows(path: str) -> List[Tuple[Optional[int], str, Optional[str]]]:
    """
    Load rows from CSV produced by the new exporter with columns:
      block_number, address, storage_slot, randomized_account, randomized_slot

    - address: required (skip if empty)
    - block_number: may be empty; keep as None to reuse the last seen block later
    - storage_slot: "null"/empty -> None (we'll send an empty slot list to eth_getProof)
    - randomized_*: ignored here (we include all rows)
    """
    try:
        fh = open(path, "r", newline="")
    except Exception as e:
        print(f"Error opening CSV: {e}", file=sys.stderr)
        sys.exit(1)

    reader = csv.DictReader(fh)
    required = {"block_number", "address", "storage_slot"}
    missing = [x for x in required if x not in reader.fieldnames]
    if missing:
        print(f"CSV missing columns: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    rows: List[Tuple[Optional[int], str, Optional[str]]] = []
    for r in reader:
        addr = (r.get("address") or "").strip()
        if not addr:
            continue

        raw_slot = (r.get("storage_slot") or "").strip()
        slot: Optional[str] = None if (raw_slot == "" or raw_slot.lower() == "null") else raw_slot

        bn_raw = (r.get("block_number") or "").strip()
        bn: Optional[int] = None
        if bn_raw:
            try:
                bn = int(bn_raw)
            except ValueError:
                bn = None  # will try to use last valid later

        rows.append((bn, addr, slot))

    fh.close()
    return rows

def resolve_block_numbers(rows: List[Tuple[Optional[int], str, Optional[str]]]) -> List[Tuple[int, Optional[int], str, Optional[str]]]:
    """
    Turn (bn_opt, addr, slot) into (effective_bn, original_bn_opt, addr, slot).
    If bn_opt is None, reuse last effective_bn. If none seen yet, skip the row.
    """
    resolved: List[Tuple[int, Optional[int], str, Optional[str]]] = []
    last_bn: Optional[int] = None
    skipped = 0
    for bn_opt, addr, slot in rows:
        if bn_opt is not None:
            effective = bn_opt
            last_bn = bn_opt
        else:
            if last_bn is None:
                skipped += 1
                continue
            effective = last_bn
        resolved.append((effective, bn_opt, addr, slot))
    if skipped:
        print(f"[WARN] Skipped {skipped} rows with empty/invalid block_number before any valid block was seen.", file=sys.stderr)
    return resolved

def run_batch(
    mode_name: str,
    rpc_url: str,
    rows: List[Tuple[int, Optional[int], str, Optional[str]]],  # (effective_bn, original_bn_opt, addr, slot)
    fail_out: str,
    block_param_fn: Callable[[int], Union[int, str]],
):
    session = requests.Session()
    TIMEOUT = 60

    total = len(rows)
    if total == 0:
        print(f"[{mode_name}] No usable rows.")
        return

    latencies: List[float] = []
    successes = 0
    failures = 0
    req_id = 0
    failure_records: List[str] = []

    for i, (effective_bn, original_bn_opt, addr, slot) in enumerate(rows, start=1):
        req_id += 1
        block_param = block_param_fn(effective_bn)
        payload = build_payload(req_id, addr, slot, block_param)
        started = time.monotonic()

        http_status = None
        body_text = None
        ok = False
        error_msg = None

        try:
            resp = session.post(rpc_url, json=payload, timeout=TIMEOUT)
            http_status = resp.status_code
            body_text = resp.text
            resp.raise_for_status()

            data = resp.json()
            if "error" in data:
                ok = False
                error_msg = f"RPC error {data['error'].get('code')}: {data['error'].get('message')}"
            else:
                res = data.get("result")
                # accountProof should exist; storageProof may be empty if no slots were requested
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

        slot_disp = slot if slot is not None else "-"
        if ok:
            print(f"[{mode_name}] [{i}/{total}] SUCCESS block_eff={effective_bn} block_orig={original_bn_opt} addr={addr} slot={slot_disp}")
            successes += 1
        else:
            print(f"[{mode_name}] [{i}/{total}] FAILURE block_eff={effective_bn} block_orig={original_bn_opt} addr={addr} slot={slot_disp} error={error_msg}")
            failures += 1
            failure_records.append("# ---- FAILURE -----------------------------------------")
            failure_records.append(
                f"# Mode={mode_name} Row {i}/{total} effective_block={effective_bn} original_block={original_bn_opt} addr={addr} slot={slot_disp}"
            )
            if http_status:
                failure_records.append(f"# HTTP {http_status}")
            if error_msg:
                failure_records.append(f"# Error: {error_msg}")
            failure_records.append(build_curl(rpc_url, payload))
            if body_text:
                failure_records.append("# Response:")
                failure_records.append(body_text)
            failure_records.append("")

    lat_sorted = sorted(latencies)
    p50 = lat_sorted[int(0.5*(len(lat_sorted)-1))]
    p95 = lat_sorted[int(0.95*(len(lat_sorted)-1))]
    p99 = lat_sorted[int(0.99*(len(lat_sorted)-1))] if len(lat_sorted) >= 100 else lat_sorted[-1]
    fail_rate = (failures / total) * 100.0

    print(f"----- eth_getProof Benchmark [{mode_name}] -----")
    print(f"Endpoint         : {rpc_url}")
    print(f"Requests         : {total}")
    print(f"Successes        : {successes}")
    print(f"Failures         : {failures}")
    print(f"Fail rate        : {fail_rate:.2f}%")
    print(f"P50 latency      : {p50:.3f}s")
    print(f"P95 latency      : {p95:.3f}s")
    print(f"P99 latency      : {p99:.3f}s")
    print("-----------------------------------------------")

    if failure_records:
        try:
            with open(fail_out, "a", encoding="utf-8") as fo:
                fo.write("\n".join(failure_records) + "\n")
            print(f"[{mode_name}] Logged failures to {fail_out}")
        except Exception as e:
            print(f"[{mode_name}] Could not write failure log: {e}", file=sys.stderr)

def main():
    parser = argparse.ArgumentParser(description="Benchmark eth_getProof over rows from a CSV.")
    parser.add_argument("--rpc", required=True)
    parser.add_argument("--in", dest="inp", default="dataset.csv")
    parser.add_argument("--fail-out", default="eth_getproof_failures.txt")
    parser.add_argument("--simulate-latest", action="store_true",
                        help="Use 'latest' as the block tag instead of per-row block numbers.")
    parser.add_argument("--simulate-all", action="store_true",
                        help="Run twice: once with 'latest' and once with the real block numbers.")
    args = parser.parse_args()

    base_rows = load_rows(args.inp)  # (bn_opt, address, slot_opt)
    print(len(base_rows), "rows loaded from", args.inp)
    rows = resolve_block_numbers(base_rows)  # (effective_bn, original_bn_opt, address, slot_opt)

    # Initialize failure log
    try:
        with open(args.fail_out, "w", encoding="utf-8") as fo:
            fo.write(f"# eth_getProof failures\n# RPC: {args.rpc}\n\n")
    except Exception as e:
        print(f"Could not initialize failure log: {e}", file=sys.stderr)

    if args.simulate_all:
        run_batch(
            mode_name="SIM-LATEST",
            rpc_url=args.rpc,
            rows=rows,
            fail_out=args.fail_out,
            block_param_fn=lambda _bn: "latest",
        )
        run_batch(
            mode_name="REAL-BLOCKS",
            rpc_url=args.rpc,
            rows=rows,
            fail_out=args.fail_out,
            block_param_fn=lambda bn: bn,
        )
    elif args.simulate_latest:
        run_batch(
            mode_name="SIM-LATEST",
            rpc_url=args.rpc,
            rows=rows,
            fail_out=args.fail_out,
            block_param_fn=lambda _bn: "latest",
        )
    else:
        run_batch(
            mode_name="REAL-BLOCKS",
            rpc_url=args.rpc,
            rows=rows,
            fail_out=args.fail_out,
            block_param_fn=lambda bn: bn,
        )

if __name__ == "__main__":
    main()
