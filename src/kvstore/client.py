"""
Client for the Raft KV store. Handles the "find the leader" problem: a
client doesn't know which node is currently leading, so it tries a node,
and if that node isn't the leader it replies with a hint of who is
(`leader_id`, which may itself be stale) -- the client follows that hint
and retries, falling back to trying every known node in turn if the hint
is wrong or missing (e.g. during an election with no leader yet).
"""

from __future__ import annotations

import asyncio
import json

from kvstore.protocol import serialize_client_request


class KVClientError(Exception):
    pass


class KVClient:
    def __init__(
        self,
        node_addresses: dict[str, tuple[str, int]],
        *,
        request_timeout_seconds: float = 2.0,
        max_retries: int = 8,
        retry_backoff_seconds: float = 0.3,
    ):
        self.node_addresses = node_addresses
        self.request_timeout_seconds = request_timeout_seconds
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds
        self._known_leader: str | None = None

    async def _send(self, node_id: str, command: dict) -> dict:
        host, port = self.node_addresses[node_id]
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=self.request_timeout_seconds
        )
        try:
            writer.write((serialize_client_request(command) + "\n").encode())
            await writer.drain()
            line = await asyncio.wait_for(reader.readline(), timeout=self.request_timeout_seconds)
            if not line:
                raise KVClientError(f"{node_id} closed the connection without responding")
            return json.loads(line.decode())
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass

    async def _request(self, command: dict) -> dict:
        """Retries across multiple full rounds through the known nodes,
        with a short backoff between rounds -- not just one pass. This
        matters concretely right after a leader crash: there's a real,
        unavoidable window (roughly one election timeout) where no node
        is leader yet because an election is in progress. A client that
        gives up after a single pass through the cluster will spuriously
        fail requests made during that window; a correct client waits
        it out. Found by testing this against real crashed TCP servers,
        not simulated ones -- the deterministic simulator's immediate
        message delivery doesn't have an analogous real-time gap to
        expose this."""
        last_error: Exception | None = None

        for round_num in range(self.max_retries):
            candidates = [self._known_leader] if self._known_leader else []
            candidates += [n for n in self.node_addresses if n != self._known_leader]

            for node_id in candidates:
                try:
                    resp = await self._send(node_id, command)
                except (OSError, asyncio.TimeoutError, KVClientError) as e:
                    last_error = e
                    continue

                if resp.get("success"):
                    self._known_leader = node_id
                    return resp

                hint = resp.get("leader_hint")
                if hint and hint in self.node_addresses:
                    self._known_leader = hint
                else:
                    self._known_leader = None  # no leader known cluster-wide right now (election in progress)

            if round_num < self.max_retries - 1:
                await asyncio.sleep(self.retry_backoff_seconds)

        raise KVClientError(f"Could not complete request after {self.max_retries} rounds (last error: {last_error})")

    async def set(self, key: str, value: str) -> None:
        await self._request({"op": "set", "key": key, "value": value})

    async def delete(self, key: str) -> None:
        await self._request({"op": "delete", "key": key})

    async def get(self, key: str) -> str | None:
        resp = await self._request({"op": "get", "key": key})
        return resp.get("value")
