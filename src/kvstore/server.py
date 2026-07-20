"""
Wraps a RaftNode with real asyncio TCP networking so it can run as an
actual OS process talking to other actual OS processes over localhost
(or a real network) -- as opposed to raft/simulator.py's in-memory
deterministic harness, which is for testing the algorithm itself.

The RaftNode instance and its handler methods are completely unmodified
between the two -- this file is purely a transport adapter: receive
bytes off a socket, deserialize into the same message types the
simulator uses, call the same node.handle_*() methods, serialize the
resulting Actions.messages back out over sockets to the right peers.

Client reads are served locally by whichever node happens to receive
them, straight from its state machine, without a formal read-index or
lease protocol. This is a known, explicitly named simplification: it
means a client could observe slightly stale data from a follower a beat
behind the leader, or from an isolated former leader that doesn't yet
know it's been superseded. A production system needs one of Raft's
linearizable-read techniques (read-index, leader lease) to close this
gap; implementing that is listed as future work in the README rather
than silently glossed over here.
"""

from __future__ import annotations

import asyncio
import json
import logging

from kvstore.protocol import deserialize_rpc, serialize_client_response, serialize_rpc
from kvstore.state_machine import KVStateMachine
from raft.messages import AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse
from raft.node import Actions, RaftNode, Role

logger = logging.getLogger(__name__)


class RaftServer:
    def __init__(
        self,
        node_id: str,
        peer_addresses: dict[str, tuple[str, int]],
        listen_host: str,
        listen_port: int,
        *,
        election_timeout_ticks: tuple[int, int] = (10, 20),
        heartbeat_interval_ticks: int = 3,
        tick_interval_seconds: float = 0.05,
        client_commit_timeout_seconds: float = 5.0,
    ):
        self.node_id = node_id
        self.peer_addresses = peer_addresses
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.tick_interval_seconds = tick_interval_seconds
        self.client_commit_timeout_seconds = client_commit_timeout_seconds

        self.node = RaftNode(
            node_id,
            list(peer_addresses.keys()),
            election_timeout_ticks=election_timeout_ticks,
            heartbeat_interval_ticks=heartbeat_interval_ticks,
        )
        self.state_machine = KVStateMachine()
        self._pending_client_futures: dict[int, asyncio.Future] = {}
        self._server: asyncio.base_events.Server | None = None
        self._tick_task: asyncio.Task | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle_connection, self.listen_host, self.listen_port)
        self._tick_task = asyncio.create_task(self._tick_loop())
        logger.info("Node %s listening on %s:%d", self.node_id, self.listen_host, self.listen_port)

    async def stop(self) -> None:
        if self._tick_task:
            self._tick_task.cancel()
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def _tick_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.tick_interval_seconds)
                actions = self.node.tick()
                await self._process_actions(actions)
        except asyncio.CancelledError:
            pass

    async def _process_actions(self, actions: Actions) -> None:
        self.state_machine.apply_all(actions.committed_entries)
        for entry in actions.committed_entries:
            fut = self._pending_client_futures.pop(entry.index, None)
            if fut is not None and not fut.done():
                fut.set_result(True)
        for recipient_id, message in actions.messages:
            asyncio.create_task(self._send_rpc(recipient_id, message))

    async def _send_rpc(self, recipient_id: str, message) -> None:
        if recipient_id not in self.peer_addresses:
            return
        host, port = self.peer_addresses[recipient_id]
        try:
            reader, writer = await asyncio.open_connection(host, port)
            writer.write((serialize_rpc(self.node_id, message) + "\n").encode())
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        except (ConnectionRefusedError, OSError) as e:
            # Peer unreachable (down, partitioned, not started yet). Raft's
            # own retry-via-heartbeat mechanism handles this naturally --
            # no special handling needed here beyond not crashing the
            # sending task.
            logger.debug("Could not reach %s: %s", recipient_id, e)

    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            line = await reader.readline()
            if not line:
                return
            data = json.loads(line.decode())

            if data["kind"] == "rpc":
                await self._handle_rpc(data)
            elif data["kind"] == "client_request":
                await self._handle_client_request(data["command"], writer)
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning("Malformed message from %s: %s", writer.get_extra_info("peername"), e)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass

    async def _handle_rpc(self, data: dict) -> None:
        sender_id = data["sender"]
        message = deserialize_rpc(data)
        if isinstance(message, RequestVoteRequest):
            actions = self.node.handle_request_vote(sender_id, message)
        elif isinstance(message, RequestVoteResponse):
            actions = self.node.handle_request_vote_response(sender_id, message)
        elif isinstance(message, AppendEntriesRequest):
            actions = self.node.handle_append_entries(sender_id, message)
        elif isinstance(message, AppendEntriesResponse):
            actions = self.node.handle_append_entries_response(sender_id, message)
        else:
            return
        await self._process_actions(actions)

    async def _handle_client_request(self, command: dict, writer: asyncio.StreamWriter) -> None:
        if command.get("op") == "get":
            if self.node.role != Role.LEADER:
                resp = serialize_client_response(success=False, leader_hint=self.node.leader_id)
            else:
                value = self.state_machine.get(command["key"])
                resp = serialize_client_response(success=True, value=value)
            writer.write((resp + "\n").encode())
            await writer.drain()
            return

        entry, actions = self.node.propose(command)
        if entry is None:
            resp = serialize_client_response(success=False, leader_hint=self.node.leader_id, error="not leader")
            writer.write((resp + "\n").encode())
            await writer.drain()
            return

        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending_client_futures[entry.index] = fut
        await self._process_actions(actions)

        try:
            await asyncio.wait_for(fut, timeout=self.client_commit_timeout_seconds)
            resp = serialize_client_response(success=True)
        except asyncio.TimeoutError:
            self._pending_client_futures.pop(entry.index, None)
            resp = serialize_client_response(success=False, error="commit timed out")
        writer.write((resp + "\n").encode())
        await writer.drain()
