#!/usr/bin/env python3
"""
gl1-peercheck - audit GenesisL1's official peer/seed list for dead nodes.

Fetches the community peer + seed lists (from the genesis-parameters repo),
TCP-dials every `id@host:port`, and reports which are reachable, with latency,
plus a list of unreachable entries that are candidates to prune. Zero
dependencies - Python 3 standard library only.

Usage:
  python3 gl1-peercheck.py            # human-readable report
  python3 gl1-peercheck.py --json     # machine-readable JSON
  python3 gl1-peercheck.py --timeout 5

Exit code is 1 if any peer is unreachable (handy for cron/CI), else 0.

Scope note: this checks TCP reachability of the p2p port (is the node alive and
accepting connections). It does NOT do a full p2p handshake, so it won't catch
protocol-level misbehaviour (e.g. a node sending oversized messages). Finding
dead/unreachable entries in the official list is the goal here.
"""
import argparse
import json
import socket
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

BASE = "https://raw.githubusercontent.com/alpha-omega-labs/genesis-parameters/main/genesis_29-2"
SOURCES = {"peers": BASE + "/peers.txt", "seeds": BASE + "/seeds.txt"}
TIMEOUT = 3.0


def fetch_list(url):
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            raw = r.read().decode().strip()
    except Exception:
        return []
    raw = raw.strip().strip('"').strip()
    return [e.strip() for e in raw.split(",") if e.strip()] if raw else []


def parse(entry):
    nodeid, _, hostport = entry.partition("@")
    host, _, port = hostport.rpartition(":")
    return nodeid, host, port


def dial(kind, entry):
    nodeid, host, port = parse(entry)
    t0 = time.time()
    try:
        with socket.create_connection((host, int(port)), timeout=TIMEOUT):
            return {"kind": kind, "entry": entry, "id": nodeid, "host": host,
                    "port": port, "up": True, "ms": int((time.time() - t0) * 1000),
                    "err": None}
    except Exception as e:
        return {"kind": kind, "entry": entry, "id": nodeid, "host": host,
                "port": port, "up": False, "ms": None, "err": type(e).__name__}


def main():
    global TIMEOUT
    ap = argparse.ArgumentParser(description="Audit GenesisL1 official peer/seed reachability")
    ap.add_argument("--json", action="store_true", help="output JSON")
    ap.add_argument("--timeout", type=float, default=TIMEOUT, help="dial timeout seconds (default 3)")
    args = ap.parse_args()
    TIMEOUT = args.timeout

    # gather + dedupe entries across peers and seeds
    seen, entries = set(), []
    for kind, url in SOURCES.items():
        for e in fetch_list(url):
            if e not in seen:
                seen.add(e)
                entries.append((kind, e))

    if not entries:
        print("Could not fetch any peers/seeds (network issue or empty lists).", file=sys.stderr)
        sys.exit(2)

    with ThreadPoolExecutor(max_workers=32) as ex:
        results = list(ex.map(lambda ke: dial(*ke), entries))

    up = [r for r in results if r["up"]]
    down = [r for r in results if not r["up"]]

    if args.json:
        print(json.dumps({
            "checked_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "total": len(results), "reachable": len(up), "down": len(down),
            "results": results,
        }, indent=2))
        sys.exit(1 if down else 0)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print("GenesisL1 peer health - {}".format(now))
    print("Source: genesis-parameters (peers + seeds)\n")
    for r in sorted(results, key=lambda r: (r["up"], r["ms"] if r["up"] else 0)):
        tag = "UP  " if r["up"] else "DOWN"
        detail = "{} ms".format(r["ms"]) if r["up"] else r["err"]
        print("  {}  {:<22} {}…  {}".format(tag, r["host"] + ":" + r["port"], r["id"][:10], detail))
    print("\nSummary: {}/{} reachable, {} down".format(len(up), len(results), len(down)))
    if down:
        print("\nUnreachable (candidates to prune from persistent_peers/seeds):")
        for r in down:
            print("  - {}".format(r["entry"]))
    sys.exit(1 if down else 0)


if __name__ == "__main__":
    main()
