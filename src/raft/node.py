"""
The core Raft consensus algorithm (Ongaro & Ousterhout, "In Search of an
Understandable Consensus Algorithm"), implemented as a pure, transport-
agnostic state machine.

Design choice: every method that reacts to an event (a tick, an incoming
RPC, an incoming RPC response) returns an `Actions` object describing
what should happen next (outgoing messages, newly-committed log entries)
instead of directly calling into a network layer. RaftNode never sends
anything itself. This is what makes deterministic simulation testing
possible: a test harness can call these methods directly, inspect
exactly what a node wants to send, and choose to deliver it, delay it,
or drop it -- without any real networking, threads, or non-determinism
in the loop. The same RaftNode class, unmodified, is what
kvstore/server.py wraps with real asyncio TCP sockets for actual
multi-process operation.

Persistence note: `current_term`, `voted_for`, and `log` are the three
pieces of state Raft requires to be durable (survive a crash) in a real
deployment -- this implementation keeps them in memory only. See
kvstore/server.py for where a real system would fsync these to disk
before responding to any RPC.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from enum import Enum

from raft.log import ReplicatedLog
from raft.messages import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    LogEntry,
    RequestVoteRequest,
    RequestVoteResponse,
)


class Role(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


Message = RequestVoteRequest | RequestVoteResponse | AppendEntriesRequest | AppendEntriesResponse


@dataclass
class Actions:
    """Everything a single event-handling call produced. `messages` is a
    list of (recipient_id, message) pairs -- the caller (simulator or
    real transport) is responsible for actually delivering them."""
    messages: list[tuple[str, Message]] = field(default_factory=list)
    committed_entries: list[LogEntry] = field(default_factory=list)
    became_leader: bool = False

    def extend(self, other: "Actions") -> "Actions":
        self.messages.extend(other.messages)
        self.committed_entries.extend(other.committed_entries)
        self.became_leader = self.became_leader or other.became_leader
        return self


class RaftNode:
    def __init__(
        self,
        node_id: str,
        peer_ids: list[str],
        *,
        election_timeout_ticks: tuple[int, int] = (10, 20),
        heartbeat_interval_ticks: int = 3,
        random_seed: int | None = None,
    ):
        self.node_id = node_id
        self.peer_ids = list(peer_ids)
        self.cluster_size = len(self.peer_ids) + 1
        self._rng = random.Random(random_seed if random_seed is not None else node_id)

        # Persistent state (would be fsync'd to disk in a real deployment)
        self.current_term = 0
        self.voted_for: str | None = None
        self.log = ReplicatedLog()

        # Volatile state
        self.role = Role.FOLLOWER
        self.commit_index = 0
        self.last_applied = 0
        self.leader_id: str | None = None

        # Leader-only volatile state, reset every time we become leader
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

        # Election bookkeeping
        self.votes_received: set[str] = set()
        self._election_timeout_range = election_timeout_ticks
        self._heartbeat_interval = heartbeat_interval_ticks
        self.election_elapsed = 0
        self.election_timeout = self._random_election_timeout()
        self.heartbeat_elapsed = 0

    def _random_election_timeout(self) -> int:
        lo, hi = self._election_timeout_range
        return self._rng.randint(lo, hi)

    # ---------------------------------------------------------------- tick

    def tick(self) -> Actions:
        """Advance the node's internal clock by one unit. Call this at a
        fixed rate (simulated or real) for every node independently."""
        actions = Actions()
        if self.role in (Role.FOLLOWER, Role.CANDIDATE):
            self.election_elapsed += 1
            if self.election_elapsed >= self.election_timeout:
                actions.extend(self._start_election())
        elif self.role == Role.LEADER:
            self.heartbeat_elapsed += 1
            if self.heartbeat_elapsed >= self._heartbeat_interval:
                self.heartbeat_elapsed = 0
                actions.extend(self._send_append_entries_to_all())
        return actions

    # ------------------------------------------------------------ election

    def _start_election(self) -> Actions:
        self.role = Role.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}
        self.leader_id = None
        self.election_elapsed = 0
        self.election_timeout = self._random_election_timeout()

        actions = Actions()
        if len(self.peer_ids) == 0:
            # Single-node cluster: we're trivially our own majority.
            actions.extend(self._become_leader())
            return actions

        req = RequestVoteRequest(
            term=self.current_term,
            candidate_id=self.node_id,
            last_log_index=self.log.last_index,
            last_log_term=self.log.last_term,
        )
        for peer in self.peer_ids:
            actions.messages.append((peer, req))
        return actions

    def handle_request_vote(self, sender_id: str, req: RequestVoteRequest) -> Actions:
        if req.term > self.current_term:
            self._step_down(req.term)

        vote_granted = False
        if req.term >= self.current_term:
            log_ok = (
                req.last_log_term > self.log.last_term
                or (req.last_log_term == self.log.last_term and req.last_log_index >= self.log.last_index)
            )
            can_vote = self.voted_for is None or self.voted_for == req.candidate_id
            if req.term == self.current_term and can_vote and log_ok:
                vote_granted = True
                self.voted_for = req.candidate_id
                self.election_elapsed = 0  # granting a vote means we trust there's a live candidate; don't also start our own election

        resp = RequestVoteResponse(term=self.current_term, vote_granted=vote_granted, voter_id=self.node_id)
        return Actions(messages=[(sender_id, resp)])

    def handle_request_vote_response(self, sender_id: str, resp: RequestVoteResponse) -> Actions:
        if resp.term > self.current_term:
            self._step_down(resp.term)
            return Actions()
        if self.role != Role.CANDIDATE or resp.term != self.current_term:
            return Actions()  # stale response from an earlier term/role, ignore

        if resp.vote_granted:
            self.votes_received.add(sender_id)
            if len(self.votes_received) * 2 > self.cluster_size:
                return self._become_leader()
        return Actions()

    def _become_leader(self) -> Actions:
        self.role = Role.LEADER
        self.leader_id = self.node_id
        self.next_index = {peer: self.log.last_index + 1 for peer in self.peer_ids}
        self.match_index = {peer: 0 for peer in self.peer_ids}
        self.heartbeat_elapsed = 0
        actions = self._send_append_entries_to_all()
        actions.became_leader = True
        return actions

    def _step_down(self, new_term: int) -> None:
        self.current_term = new_term
        self.voted_for = None
        self.role = Role.FOLLOWER
        self.leader_id = None
        self.election_elapsed = 0
        self.election_timeout = self._random_election_timeout()

    # ------------------------------------------------------- log replication

    def _send_append_entries_to_all(self) -> Actions:
        actions = Actions()
        for peer in self.peer_ids:
            actions.extend(self._send_append_entries_to(peer))
        return actions

    def _send_append_entries_to(self, peer_id: str) -> Actions:
        next_idx = self.next_index.get(peer_id, self.log.last_index + 1)
        prev_log_index = next_idx - 1
        prev_log_term = self.log.term_at(prev_log_index)
        entries = tuple(self.log.slice_from(next_idx))
        req = AppendEntriesRequest(
            term=self.current_term,
            leader_id=self.node_id,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries,
            leader_commit=self.commit_index,
        )
        return Actions(messages=[(peer_id, req)])

    def handle_append_entries(self, sender_id: str, req: AppendEntriesRequest) -> Actions:
        if req.term > self.current_term:
            self._step_down(req.term)

        if req.term < self.current_term:
            # Stale leader; reject and tell it our current term so it steps down.
            resp = AppendEntriesResponse(term=self.current_term, success=False, responder_id=self.node_id)
            return Actions(messages=[(sender_id, resp)])

        # Valid leader for our current term (or just became so) -- reset election timer.
        self.role = Role.FOLLOWER
        self.leader_id = req.leader_id
        self.election_elapsed = 0

        if not self.log.matches(req.prev_log_index, req.prev_log_term):
            resp = AppendEntriesResponse(term=self.current_term, success=False, responder_id=self.node_id)
            return Actions(messages=[(sender_id, resp)])

        for i, entry in enumerate(req.entries):
            idx = req.prev_log_index + 1 + i
            existing = self.log.get(idx)
            if existing is not None and existing.term != entry.term:
                self.log.truncate_from(idx)  # conflict: our log diverges here, discard our version and everything after
                self.log.append(entry)
            elif existing is None:
                self.log.append(entry)
            # else: existing.term == entry.term, entries are identical (idempotent replay), no-op

        match_index_reply = req.prev_log_index + len(req.entries)

        if req.leader_commit > self.commit_index:
            self.commit_index = min(req.leader_commit, self.log.last_index)

        resp = AppendEntriesResponse(
            term=self.current_term, success=True, responder_id=self.node_id, match_index=match_index_reply
        )
        actions = Actions(messages=[(sender_id, resp)])
        actions.committed_entries = self._apply_committed_entries()
        return actions

    def handle_append_entries_response(self, sender_id: str, resp: AppendEntriesResponse) -> Actions:
        if resp.term > self.current_term:
            self._step_down(resp.term)
            return Actions()
        if self.role != Role.LEADER or resp.term != self.current_term:
            return Actions()  # stale, ignore

        if resp.success:
            self.match_index[sender_id] = max(self.match_index.get(sender_id, 0), resp.match_index)
            self.next_index[sender_id] = self.match_index[sender_id] + 1
            self._advance_commit_index()
            return Actions(committed_entries=self._apply_committed_entries())
        else:
            # Log inconsistency: back off nextIndex and retry immediately.
            # A production implementation would use the conflict-term hint
            # from the paper's end to skip back further in one round trip;
            # this decrement-by-one is simpler and still correct, just
            # slower to converge for a follower that's very far behind.
            self.next_index[sender_id] = max(1, self.next_index.get(sender_id, self.log.last_index + 1) - 1)
            return self._send_append_entries_to(sender_id)

    def _advance_commit_index(self) -> None:
        # Raft safety rule: a leader may only advance commit_index to cover
        # entries from ITS OWN current term by counting replicas directly.
        # Entries from earlier terms get committed only as a side effect of
        # committing a later entry that's after them in the log -- never
        # by directly counting their own replica count. This is what
        # prevents a subtle safety violation described in Figure 8 of the
        # Raft paper (a previously-uncommitted entry from an old term
        # getting silently overwritten after being "committed" by a stale
        # majority count).
        for n in range(self.log.last_index, self.commit_index, -1):
            if self.log.term_at(n) != self.current_term:
                continue
            replica_count = 1 + sum(1 for peer in self.peer_ids if self.match_index.get(peer, 0) >= n)
            if replica_count * 2 > self.cluster_size:
                self.commit_index = n
                return

    def _apply_committed_entries(self) -> list[LogEntry]:
        applied = []
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log.get(self.last_applied)
            if entry is not None:
                applied.append(entry)
        return applied

    # ------------------------------------------------------------ client API

    def propose(self, command: dict) -> tuple[LogEntry | None, Actions]:
        """Client wants to append `command`. Returns (None, empty Actions)
        if this node isn't the leader -- the caller should redirect to
        `self.leader_id` if known. Otherwise appends to the leader's own
        log and immediately attempts replication (doesn't wait for the
        next heartbeat tick, for responsiveness)."""
        if self.role != Role.LEADER:
            return None, Actions()

        entry = LogEntry(term=self.current_term, index=self.log.last_index + 1, command=command)
        self.log.append(entry)
        actions = self._send_append_entries_to_all()
        return entry, actions
