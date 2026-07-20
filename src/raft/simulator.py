"""
A deterministic, in-memory simulated cluster. This is the test harness
that makes it possible to write things like "kill the leader mid-write,
then assert no committed entry is ever lost" as an actual, repeatable
unit test instead of a flaky integration test involving real processes,
real sockets, and real timing.

Simplification, stated plainly: message delivery within a call to
`tick()` is immediate and recursive (a vote response can trigger
becoming leader, which triggers heartbeats, which get delivered and
trigger AppendEntries responses, all inside one `tick()` call). This
models an idealized zero-latency network for the purposes of testing
Raft's *safety* properties (which must hold regardless of timing) and
*eventual* liveness (a leader gets elected, entries get committed) --
it does not model real network latency/jitter, which is what
kvstore/server.py's real asyncio transport is for.
"""

from __future__ import annotations

import random

from raft.messages import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    LogEntry,
    RequestVoteRequest,
    RequestVoteResponse,
)
from raft.node import Actions, RaftNode, Role


class SimulatedCluster:
    def __init__(
        self,
        node_ids: list[str],
        *,
        election_timeout_ticks: tuple[int, int] = (10, 20),
        heartbeat_interval_ticks: int = 3,
        drop_rate: float = 0.0,
        seed: int = 0,
    ):
        self.node_ids = list(node_ids)
        self.nodes: dict[str, RaftNode] = {
            nid: RaftNode(
                nid,
                [p for p in node_ids if p != nid],
                election_timeout_ticks=election_timeout_ticks,
                heartbeat_interval_ticks=heartbeat_interval_ticks,
                random_seed=f"{seed}-{nid}",
            )
            for nid in node_ids
        }
        self.alive: dict[str, bool] = {nid: True for nid in node_ids}
        # Keyed by log index, not a plain append-only list: a restarted
        # node's last_applied volatile state resets to 0 per the Raft
        # paper, so after rejoining it legitimately re-reports its
        # already-applied prefix as "newly applied" from its own point of
        # view. A real state machine layer must apply idempotently by
        # index for exactly this reason -- this dict models that
        # correctly (re-applying index 3 just overwrites the same key),
        # where a naive append-only list would double-count it.
        self.applied_log: dict[str, dict[int, LogEntry]] = {nid: {} for nid in node_ids}
        self._partition: tuple[set[str], set[str]] | None = None
        self.drop_rate = drop_rate
        self._rng = random.Random(seed)
        self.current_tick = 0

        self._election_timeout_range = election_timeout_ticks
        self._heartbeat_interval = heartbeat_interval_ticks

    # --------------------------------------------------------- chaos controls

    def kill_node(self, node_id: str) -> None:
        self.alive[node_id] = False

    def restart_node(self, node_id: str) -> None:
        """Simulates a crash + restart: only the persistent state (term,
        vote, log) survives, exactly as the Raft paper requires. Volatile
        state (role, commit_index, last_applied, leader knowledge) resets
        -- a restarted node always rejoins as a humble follower and
        rediscovers commit_index by receiving AppendEntries from whoever
        the current leader turns out to be."""
        old = self.nodes[node_id]
        new_node = RaftNode(
            node_id,
            old.peer_ids,
            election_timeout_ticks=self._election_timeout_range,
            heartbeat_interval_ticks=self._heartbeat_interval,
            random_seed=f"restart-{self.current_tick}-{node_id}",
        )
        new_node.current_term = old.current_term
        new_node.voted_for = old.voted_for
        new_node.log = old.log
        self.nodes[node_id] = new_node
        self.alive[node_id] = True

    def partition(self, group_a: set[str], group_b: set[str]) -> None:
        assert group_a | group_b == set(self.node_ids), "partition groups must cover every node"
        assert not (group_a & group_b), "partition groups must be disjoint"
        self._partition = (group_a, group_b)

    def heal_partition(self) -> None:
        self._partition = None

    def _can_deliver(self, sender_id: str, recipient_id: str) -> bool:
        if not self.alive.get(sender_id, False) or not self.alive.get(recipient_id, False):
            return False
        if self._partition is not None:
            a, b = self._partition
            if (sender_id in a and recipient_id in b) or (sender_id in b and recipient_id in a):
                return False
        return True

    # ------------------------------------------------------------- stepping

    def tick(self) -> None:
        self.current_tick += 1
        for nid, node in list(self.nodes.items()):
            if not self.alive[nid]:
                continue
            actions = node.tick()
            self._handle_actions(nid, actions)

    def run_ticks(self, n: int) -> None:
        for _ in range(n):
            self.tick()

    def _handle_actions(self, sender_id: str, actions: Actions) -> None:
        for entry in actions.committed_entries:
            self.applied_log[sender_id][entry.index] = entry
        for recipient_id, message in actions.messages:
            self._deliver(sender_id, recipient_id, message)

    def _deliver(self, sender_id: str, recipient_id: str, message) -> None:
        if not self._can_deliver(sender_id, recipient_id):
            return
        if self.drop_rate > 0 and self._rng.random() < self.drop_rate:
            return

        target = self.nodes[recipient_id]
        if isinstance(message, RequestVoteRequest):
            actions = target.handle_request_vote(sender_id, message)
        elif isinstance(message, RequestVoteResponse):
            actions = target.handle_request_vote_response(sender_id, message)
        elif isinstance(message, AppendEntriesRequest):
            actions = target.handle_append_entries(sender_id, message)
        elif isinstance(message, AppendEntriesResponse):
            actions = target.handle_append_entries_response(sender_id, message)
        else:
            raise TypeError(f"Unknown message type: {type(message)!r}")
        self._handle_actions(recipient_id, actions)

    # ------------------------------------------------------------ utilities

    def propose(self, command: dict) -> LogEntry | None:
        """Test convenience: find whichever alive node is currently the
        leader and propose directly to it (a real client instead talks
        over the network and gets redirected -- see kvstore/client.py)."""
        for nid, node in self.nodes.items():
            if self.alive[nid] and node.role == Role.LEADER:
                entry, actions = node.propose(command)
                self._handle_actions(nid, actions)
                return entry
        return None

    def applied_commands(self, node_id: str) -> list[dict]:
        """Commands this node has applied, in log-index order. Safe to
        call even after a restart -- duplicates from re-applying the
        pre-crash prefix are already deduplicated by index in
        applied_log itself, this just returns them in the right order."""
        entries = self.applied_log[node_id]
        return [entries[i].command for i in sorted(entries)]

    def current_leaders(self) -> list[str]:
        """All alive nodes that currently believe they're the leader.
        Raft's Election Safety property guarantees this has at most one
        entry *per term* -- but during a leadership transition it's valid
        for an old leader to not yet know it's been superseded until its
        next RPC. Tests should check this converges to length <= 1 after
        enough ticks, not that it's always instantaneously true."""
        return [nid for nid, node in self.nodes.items() if self.alive[nid] and node.role == Role.LEADER]

    def run_until_leader(self, max_ticks: int = 200) -> str | None:
        for _ in range(max_ticks):
            self.tick()
            leaders = self.current_leaders()
            if len(leaders) == 1:
                return leaders[0]
        return None

    def run_until_committed(self, index: int, max_ticks: int = 200) -> bool:
        """Runs ticks until a MAJORITY of nodes have applied an entry at
        `index`, or max_ticks is exhausted. Deliberately majority, not
        "every alive node" -- those aren't the same thing. A node can be
        alive but cut off by a network partition, in which case it's
        expected and correct for it to lag behind indefinitely until the
        partition heals; Raft's own definition of "committed" is majority
        agreement, and testing against a stricter "everyone must have it"
        bar would make this method report failure on scenarios that are
        actually working exactly as designed."""
        majority = (len(self.node_ids) // 2) + 1

        def _count() -> int:
            return sum(1 for nid in self.node_ids if len(self.applied_log[nid]) >= index)

        for _ in range(max_ticks):
            if _count() >= majority:
                return True
            self.tick()
        return _count() >= majority
