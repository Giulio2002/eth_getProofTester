#!/usr/bin/env python3
import argparse
import csv
import sys
import time
import secrets
from typing import Any, Dict, List, Optional
import requests

# ---------- JSON-RPC helper ----------
class JsonRpc:
    def __init__(self, url: str, timeout: int = 60, max_retries: int = 3, backoff: float = 0.0):
        self.url = url
        self.timeout = timeout
        this=self
        self.max_retries = max_retries
        self.backoff = backoff
        self._id = 0
        self._session = requests.Session()

    def call(self, method: str, params: List[Any]) -> Any:
        self._id += 1
        payload = {"jsonrpc": "2.0", "id": self._id, "method": method, "params": params}
        retries = 0
        last_err = None
        while retries <= self.max_retries:
            try:
                r = self._session.post(self.url, json=payload, timeout=self.timeout)
                r.raise_for_status()
                data = r.json()
                if "error" in data:
                    raise RuntimeError(f"RPC error {data['error'].get('code')}: {data['error'].get('message')}")
                return data["result"]
            except Exception as e:
                last_err = e
                if retries == self.max_retries:
                    break
                time.sleep(self.backoff * (2 ** retries))
                retries += 1
        raise RuntimeError(f"RPC call failed for {method}: {last_err}")

# ---------- Utilities ----------
def to_hex(n: int) -> str:
    return hex(n)

def to_int(x: Optional[str]) -> Optional[int]:
    if x is None:
        return None
    return int(x, 16)

def get_block_with_txs(rpc: JsonRpc, block_num: int) -> Dict[str, Any]:
    return rpc.call("eth_getBlockByNumber", [to_hex(block_num), True])

def is_contract_by_nonce_zero(rpc: JsonRpc, address: str, block_tag: str) -> bool:
    try:
        nonce_hex = rpc.call("eth_getTransactionCount", [address, block_tag])
        nonce = to_int(nonce_hex) or 0
        return nonce == 1
    except Exception:
        return False

def get_4th_storage_slot_via_debug(rpc: JsonRpc, block_hash: str, tx_index: int, addr: str) -> Optional[str]:
    try:
        start_key = "0x" + "0"*64
        res = rpc.call("debug_storageRangeAt", [block_hash, tx_index, addr, start_key, 16])
        storage = res.get("storage") if isinstance(res, dict) else None
        if not isinstance(storage, dict):
            return None
        slot_keys = sorted(storage.keys())
        if len(slot_keys) >= 4:
            return slot_keys[3]
        return None
    except Exception:
        return None

def rand_address() -> str:
    return "0x" + secrets.token_hex(20)

def rand_slot_key() -> str:
    return "0x" + secrets.token_hex(32)

def main():
    parser = argparse.ArgumentParser(description="Export address pairs and storage slots.")
    parser.add_argument("--rpc", required=True)
    parser.add_argument("--start", type=int, required=True)
    parser.add_argument("--end", type=int, required=True)
    parser.add_argument("--out", default="dataset.csv")
    parser.add_argument("--pause", type=float, default=0.0)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--backoff", type=float, default=0.0)
    parser.add_argument("--noisy", action="store_true")
    args = parser.parse_args()

    if args.end < args.start:
        print("Error: --end must be >= --start", file=sys.stderr)
        sys.exit(1)

    rpc = JsonRpc(args.rpc, timeout=args.timeout, max_retries=args.retries, backoff=args.backoff)

    latest_hex = rpc.call("eth_blockNumber", [])
    latest = to_int(latest_hex)
    if latest is None:
        print("Error: could not determine latest block number", file=sys.stderr)
        sys.exit(1)
    end_block = min(args.end, latest)

    total_blocks = end_block - args.start + 1

    fields = ["block_number", "address", "storage_slot", "randomized_account", "randomized_slot"]
    out_fh = open(args.out, "w", newline="")
    writer = csv.DictWriter(out_fh, fieldnames=fields)
    writer.writeheader()

    try:
        for idx, bn in enumerate(range(args.start, end_block + 1), start=1):
            block = get_block_with_txs(rpc, bn)
            if not block:
                print(f"[{idx}/{total_blocks}] Skipped block {bn}: no data")
                continue

            block_tag = to_hex(bn)
            block_hash = block.get("hash")
            txs = block.get("transactions") or []
            txsContract = 0

            for tx in txs:
                from_addr = tx.get("from") or ""
                to_addr = tx.get("to") or ""
                tx_index_hex = tx.get("transactionIndex")
                tx_index = to_int(tx_index_hex) if tx_index_hex is not None else None

                storage_slot = None

                if from_addr and to_addr and block_hash is not None and tx_index is not None:
                    try:
                        if is_contract_by_nonce_zero(rpc, to_addr, block_tag):
                            storage_slot = get_4th_storage_slot_via_debug(rpc, block_hash, tx_index, to_addr)
                            if storage_slot is not None:
                                txsContract += 1
                    except Exception:
                        storage_slot = None

                writer.writerow({
                    "block_number": bn,
                    "address": to_addr,
                    "storage_slot": storage_slot if storage_slot else "null",
                    "randomized_account": False,
                    "randomized_slot": False
                })
                writer.writerow({
                    "block_number": bn,
                    "address": from_addr,
                    "storage_slot": "null",
                    "randomized_account": False,
                    "randomized_slot": False
                })

                if args.noisy:
                    writer.writerow({
                        "block_number": bn,
                        "address": from_addr,
                        "storage_slot": "null",
                        "randomized_account": True,
                        "randomized_slot": False
                    })
                    writer.writerow({
                            "block_number": bn,
                            "address": to_addr,
                            "storage_slot": "null",
                            "randomized_account": False,
                            "randomized_slot": True
                        })

                    if storage_slot is not None:
                        writer.writerow({
                            "block_number": bn,
                            "address": from_addr,
                            "storage_slot": rand_slot_key(),
                            "randomized_account": False,
                            "randomized_slot": True
                        })
                        writer.writerow({
                            "block_number": bn,
                            "address": to_addr,
                            "storage_slot": rand_slot_key(),
                            "randomized_account": False,
                            "randomized_slot": True
                        })
                        writer.writerow({
                            "block_number": bn,
                            "address": to_addr,
                            "storage_slot": "null",
                            "randomized_account": False,
                            "randomized_slot": True
                        })

            print(f"[{idx}/{total_blocks}] Processed block {bn} with {len(txs)} transactions, {txsContract} to contracts.")

            if args.pause > 0:
                time.sleep(args.pause)
    finally:
        out_fh.close()

    if args.noisy:
        out_fh = open(args.out, "a", newline="")
        writer = csv.DictWriter(out_fh, fieldnames=fields)
        for _ in range(100):
            writer.writerow({
                "block_number": "",
                "address": rand_address(),
                "storage_slot": rand_slot_key(),
                "randomized_account": True,
                "randomized_slot": True
            })
        out_fh.close()

    print(f"Done. Wrote {args.out}")

if __name__ == "__main__":
    main()
