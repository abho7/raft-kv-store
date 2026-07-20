"""
Tests against the real asyncio TCP transport (kvstore/server.py +
kvstore/client.py), not the deterministic simulator. These spin up
actual RaftServer instances bound to real localhost ports within the
test process and drive them with a real KVClient over real sockets.

Written as sync `test_*` functions wrapping `asyncio.run(...)` rather
than `async def test_*`, so these run under plain pytest without needing
the pytest-asyncio plugin, and also run under this project's manual
test runner.
"""

from __future__ import annotations

import asyncio

from kvstore.client import KVClient
from kvstore.server import RaftServer

_next_port = [9300]  # avoid port collisions between tests in the same run


def _alloc_ports(n: int) -> list[int]:
    ports = list(range(_next_port[0], _next_port[0] + n))
    _next_port[0] += n
    return ports


async def _make_cluster(node_ids: list[str]) -> tuple[dict, dict]:
    ports = _alloc_ports(len(node_ids))
    addrs = {nid: ("127.0.0.1", port) for nid, port in zip(node_ids, ports)}
    servers = {}
    for nid, (host, port) in addrs.items():
        peers = {k: v for k, v in addrs.items() if k != nid}
        s = RaftServer(nid, peers, host, port, tick_interval_seconds=0.02)
        servers[nid] = s
        await s.start()
    return servers, addrs


async def _wait_for_leader(servers: dict, max_wait_seconds: float = 5.0) -> str:
    elapsed = 0.0
    step = 0.05
    while elapsed < max_wait_seconds:
        leaders = [nid for nid, s in servers.items() if s.node.role.value == "leader"]
        if len(leaders) == 1:
            return leaders[0]
        await asyncio.sleep(step)
        elapsed += step
    raise TimeoutError("no leader elected within timeout")


async def _stop_all(servers: dict) -> None:
    for s in servers.values():
        await s.stop()


async def _async_set_get_roundtrip():
    servers, addrs = await _make_cluster(["n1", "n2", "n3"])
    try:
        await _wait_for_leader(servers)
        client = KVClient(addrs)
        await client.set("name", "raft-over-real-tcp")
        value = await client.get("name")
        assert value == "raft-over-real-tcp"
    finally:
        await _stop_all(servers)


def test_set_get_roundtrip_over_real_sockets():
    asyncio.run(_async_set_get_roundtrip())


async def _async_leader_crash_and_failover():
    servers, addrs = await _make_cluster(["n1", "n2", "n3"])
    try:
        leader_id = await _wait_for_leader(servers)
        client = KVClient(addrs)
        await client.set("x", "1")

        await servers[leader_id].stop()
        del servers[leader_id]

        await client.set("y", "2")
        assert await client.get("y") == "2"
        assert await client.get("x") == "1"

        remaining_leaders = [nid for nid, s in servers.items() if s.node.role.value == "leader"]
        assert len(remaining_leaders) == 1
        assert remaining_leaders[0] != leader_id
    finally:
        await _stop_all(servers)


def test_leader_crash_and_client_failover_over_real_sockets():
    asyncio.run(_async_leader_crash_and_failover())


async def _async_client_finds_leader_without_being_told():
    servers, addrs = await _make_cluster(["n1", "n2", "n3"])
    try:
        await _wait_for_leader(servers)
        client = KVClient(addrs)
        assert client._known_leader is None
        await client.set("k", "v")
        assert client._known_leader is not None
        assert await client.get("k") == "v"
    finally:
        await _stop_all(servers)


def test_client_discovers_leader_via_redirect():
    asyncio.run(_async_client_finds_leader_without_being_told())


async def _async_delete_removes_key():
    servers, addrs = await _make_cluster(["n1", "n2", "n3"])
    try:
        await _wait_for_leader(servers)
        client = KVClient(addrs)
        await client.set("temp", "value")
        assert await client.get("temp") == "value"
        await client.delete("temp")
        assert await client.get("temp") is None
    finally:
        await _stop_all(servers)


def test_delete_over_real_sockets():
    asyncio.run(_async_delete_removes_key())
