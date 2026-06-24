#!/usr/bin/env python3
"""
gl1-peercheck - audit (and refresh) GenesisL1's bootstrap peer/seed list.

Two modes:

  (default) AUDIT the official list - fetch the community peer + seed lists from
  the genesis-parameters repo, TCP-dial every `id@host:port`, and report which
  are reachable plus a list of unreachable entries to prune.

  (--export) BUILD a fresh list - combine the survivors of the official list with
  currently-active peers discovered from the live network (public RPC /net_info),
  verify every candidate by dialing, and print a ready-to-use `persistent_peers`
  string of only the reachable ones. Handy for refreshing default install scripts.

Zero dependencies - Python 3 standard library only.

Usage:
  python3 gl1-peercheck.py            # audit report
  python3 gl1-peercheck.py --json     # audit as JSON
  python3 gl1-peercheck.py --export   # fresh persistent_peers string (TOML-ready)
  python3 gl1-peercheck.py --export --no-discover   # export only official survivors
  python3 gl1-peercheck.py --timeout 5

Scope note: this checks TCP reachability of the p2p port (is the node alive and
accepting connections). It does NOT do a full p2p handshake. A node showing DOWN
may be dead/moved OR a healthy validator that just doesn't expose a public p2p
port (sentry setups) - either way it's not usable as a bootstrap peer.
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
# public RPC endpoints (from the cosmos chain-registry) used to discover live peers
RPCS = ["https://26657.genesisl1.org", "https://genesisl1-rpc.zenode.app"]
TIMEOUT = 3.0


def _get_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": "gl1-peercheck/1.1"})
    with urllib.request.urlopen(req, timeout=12) as r:
        return json.load(r)


def fetch_list(url):
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            raw = r.read().decode().strip()
    except Exception:
        return []
    raw = raw.strip().strip('"').strip()
    return [e.strip() for e in raw.split(",") if e.strip()] if raw else []


def discover_live(rpcs):
    """Pull currently-connected peers from public RPC /net_info as id@ip:port."""
    out = set()
    for rpc in rpcs:
        try:
            d = _get_json(rpc.rstrip("/") + "/net_info")
            for p in d.get("result", {}).get("peers", []):
                ni = p.get("node_info", {})
                pid = ni.get("id")
                ip = (p.get("remote_ip") or "").strip()
                laddr = ni.get("listen_addr", "") or ""
                port = laddr.rsplit(":", 1)[-1] if ":" in laddr else "26656"
                if pid and ip and port.isdigit():
                    out.add("{}@{}:{}".format(pid, ip, port))
        except Exception:
            continue
    return out


def parse(entry):
    nodeid, _, hostport = entry.partition("@")
    host, _, port = hostport.rpartition(":")
    return nodeid, host, port


def dial(entry):
    nodeid, host, port = parse(entry)
    t0 = time.time()
    try:
        with socket.create_connection((host, int(port)), timeout=TIMEOUT):
            return {"entry": entry, "id": nodeid, "host": host, "port": port,
                    "up": True, "ms": int((time.time() - t0) * 1000), "err": None}
    except Exception as e:
        return {"entry": entry, "id": nodeid, "host": host, "port": port,
                "up": False, "ms": None, "err": type(e).__name__}


def main():
    global TIMEOUT
    ap = argparse.ArgumentParser(description="Audit or refresh GenesisL1's bootstrap peer/seed list")
    ap.add_argument("--json", action="store_true", help="audit output as JSON")
    ap.add_argument("--export", action="store_true", help="print a fresh persistent_peers string of reachable peers")
    ap.add_argument("--no-discover", action="store_true", help="(with --export) don't pull live peers from RPC, only re-test the official list")
    ap.add_argument("--timeout", type=float, default=TIMEOUT, help="dial timeout seconds (default 3)")
    args = ap.parse_args()
    TIMEOUT = args.timeout

    # official list (peers + seeds), de-duped
    official, seen = [], set()
    for url in SOURCES.values():
        for e in fetch_list(url):
            if e not in seen:
                seen.add(e)
                official.append(e)

    # ----- EXPORT: build a fresh, verified persistent_peers string -----------
    if args.export:
        candidates = set(official)
        n_disc = 0
        if not args.no_discover:
            live = discover_live(RPCS)
            n_disc = len(live)
            candidates |= live
        if not candidates:
            print("# could not gather any candidate peers", file=sys.stderr)
            sys.exit(2)
        with ThreadPoolExecutor(max_workers=48) as ex:
            results = list(ex.map(dial, candidates))
        reachable = [r for r in results if r["up"]]
        # keep the fastest reachable address per node id
        best = {}
        for r in sorted(reachable, key=lambda r: r["ms"]):
            best.setdefault(r["id"], r["entry"])
        peers = list(best.values())
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        print("# GenesisL1 persistent_peers - generated by gl1-peercheck on {}".format(now))
        print("# {} reachable peers (from {} official + {} discovered live, all dial-verified)".format(
            len(peers), len(official), n_disc))
        print('persistent_peers = "{}"'.format(",".join(peers)))
        sys.exit(0)

    # ----- AUDIT: report reachability of the official list -------------------
    if not official:
        print("Could not fetch the official peers/seeds list.", file=sys.stderr)
        sys.exit(2)
    with ThreadPoolExecutor(max_workers=32) as ex:
        results = list(ex.map(dial, official))
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
        print("  {}  {:<22} {}...  {}".format(tag, r["host"] + ":" + r["port"], r["id"][:10], detail))
    print("\nSummary: {}/{} reachable, {} down".format(len(up), len(results), len(down)))
    if down:
        print("\nUnreachable (candidates to prune from persistent_peers/seeds):")
        for r in down:
            print("  - {}".format(r["entry"]))
    print("\nTip: run with --export to print a fresh, dial-verified persistent_peers string.")
    sys.exit(1 if down else 0)


if __name__ == "__main__":
    main()
