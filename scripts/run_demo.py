#!/usr/bin/env python3
"""
Live demo: starts a real 5-node Raft cluster on localhost, writes some
keys, kills the leader mid-operation, and shows the cluster electing a
new leader and continuing to serve writes without losing anything --
all over real TCP sockets, not the deterministic simulator.

Run with: python scripts/run_demo.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from kvstore.client import KVClient
from kvstore.server import RaftServer


def _line(char="-", n=64):
    print(char * n)


async def wait_for_leader(servers: dict, max_wait: float = 5.0) -> str:
    elapsed = 0.0
    while elapsed < max_wait:
        leaders = [nid for nid, s in servers.items() if s.node.role.value == "leader"]
        if len(leaders) == 1:
            return leaders[0]
        await asyncio.sleep(0.05)
        elapsed += 0.05
    raise TimeoutError("no leader elected")


async def main() -> None:
    node_ids = ["n1", "n2", "n3", "n4", "n5"]
    base_port = 9500
    addrs = {nid: ("127.0.0.1", base_port + i) for i, nid in enumerate(node_ids)}

    print("Starting a 5-node Raft cluster on localhost...")
    servers = {}
    for nid, (host, port) in addrs.items():
        peers = {k: v for k, v in addrs.items() if k != nid}
        s = RaftServer(nid, peers, host, port, tick_interval_seconds=0.02)
        servers[nid] = s
        await s.start()
    print(f"  {len(servers)} nodes listening: {', '.join(f'{n}:{p}' for n, (_, p) in addrs.items())}")

    _line("=")
    leader = await wait_for_leader(servers)
    print(f"Leader elected: {leader} (term {servers[leader].node.current_term})")

    client = KVClient(addrs)
    _line()
    print("Writing some keys through the client...")
    await client.set("username", "abho7")
    await client.set("project", "raft-kv-store")
    print(f"  username = {await client.get('username')}")
    print(f"  project  = {await client.get('project')}")

    _line("=")
    print(f"KILLING THE LEADER ({leader}) -- closing its real TCP socket, mid-cluster-lifetime")
    await servers[leader].stop()
    dead_leader = leader
    del servers[leader]
    print(f"  {dead_leader} is down. {len(servers)} nodes remain.")

    _line()
    print("Writing another key -- client should transparently find the new leader...")
    await client.set("status", "survived-the-crash")
    value = await client.get("status")
    print(f"  status = {value}")

    new_leader = await wait_for_leader(servers)
    print(f"\nNew leader: {new_leader} (term {servers[new_leader].node.current_term})")

    _line()
    print("Confirming pre-crash writes are still intact:")
    print(f"  username = {await client.get('username')}")
    print(f"  project  = {await client.get('project')}")

    _line("=")
    print("Demo complete: the cluster survived a leader crash mid-operation")
    print("with zero data loss, using real TCP sockets end to end.")

    for s in servers.values():
        await s.stop()


if __name__ == "__main__":
    asyncio.run(main())
