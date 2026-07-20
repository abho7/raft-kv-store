"""
Message types for the two Raft RPCs (RequestVote, AppendEntries) plus
their responses, and a generic envelope the simulated/real transports
both use.

These are plain dataclasses, not tied to any transport -- the same
message types are produced and consumed whether the underlying wire is
an in-memory queue (tests/simulation) or real asyncio TCP sockets
(kvstore/server.py). That's what lets the exact same RaftNode logic run
in both places unmodified.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class LogEntry:
    term: int
    index: int
    command: dict  # opaque to Raft itself; interpreted by the state machine (e.g. {"op": "set", "key": ..., "value": ...})


@dataclass(frozen=True)
class RequestVoteRequest:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass(frozen=True)
class RequestVoteResponse:
    term: int
    vote_granted: bool
    voter_id: str


@dataclass(frozen=True)
class AppendEntriesRequest:
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: tuple[LogEntry, ...]
    leader_commit: int


@dataclass(frozen=True)
class AppendEntriesResponse:
    term: int
    success: bool
    responder_id: str
    # Optimization from the Raft paper's end (not in the core figure, but
    # standard in real implementations): tells the leader the highest
    # index the follower's log actually matches on, so a failed
    # AppendEntries can jump nextIndex back further than 1-at-a-time
    # decrement would, which matters a lot for a follower that's far
    # behind after being partitioned or crashed for a while.
    match_index: int = 0


@dataclass(frozen=True)
class Envelope:
    """Wraps any of the above with sender/recipient for transport-level
    routing. Raft's own logic never looks at this -- only the transport
    (simulator.py or the real asyncio server) does."""
    sender: str
    recipient: str
    payload: RequestVoteRequest | RequestVoteResponse | AppendEntriesRequest | AppendEntriesResponse
