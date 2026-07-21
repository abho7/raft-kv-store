# Raft KV Store

[![tests](https://github.com/YOUR_USERNAME/YOUR_REPO_NAME/actions/workflows/tests.yml/badge.svg)](https://github.com/YOUR_USERNAME/YOUR_REPO_NAME/actions/workflows/tests.yml)

A linearizable-writes, replicated key-value store implementing the Raft
consensus protocol (Ongaro & Ousterhout, 2014) from first principles:
leader election via randomized timeout-triggered `RequestVote` RPCs,
log replication via `AppendEntries` with prev-log consistency checks,
and commit-index advancement under the current-term-only safety
constraint from Figure 8 of the paper.

**Guarantee proven, not asserted:** under `f` node failures in a cluster
of `2f+1`, the system retains availability and never violates State
Machine Safety — no two nodes ever apply divergent commands at the same
log index, and no committed entry is ever lost. Verified across 23
tests spanning both a deterministic discrete-event simulation harness
and a real `asyncio` TCP transport with actual process termination.

See [ARCHITECTURE.md](./ARCHITECTURE.md) for the full state-transition
diagram, the safety-property proofs this test suite targets, and a
postmortem of three correctness bugs surfaced during development.

## Protocol implementation surface

- **Leader election**: randomized election timeouts (uniform jitter,
  per-node RNG seeded independently) to bound split-vote probability;
  `RequestVote` handling enforces the log-completeness check
  (`lastLogTerm`, `lastLogIndex` comparison) required for Leader
  Completeness.
- **Log replication**: `AppendEntries` consistency check via
  `(prevLogIndex, prevLogTerm)`; conflicting suffixes are truncated and
  overwritten per the paper's log-matching invariant; `nextIndex`/
  `matchIndex` tracked per-follower on the leader, with decrement-and-
  retry backoff on consistency-check failure.
- **Commit safety**: `commitIndex` advancement restricted to entries
  replicated to a quorum **from the leader's current term** — the
  Figure-8 safety constraint that prevents a subtle class of
  post-failover data-loss bugs where an old-term entry gets
  prematurely committed by indirect majority count.
- **State machine application**: idempotent by log index (`apply()` is
  a no-op for any index at or below the highest already applied),
  required because `lastApplied` is volatile state that resets to 0 on
  restart — a rejoining node legitimately re-delivers its pre-crash
  prefix.

## Test topology

| Property under test | Harness | File |
|---|---|---|
| Election Safety (at most 1 leader per term) | deterministic sim | `test_election.py` |
| Log Matching invariant, ordered commit | deterministic sim | `test_log_replication.py` |
| State Machine Safety under leader failure | deterministic sim | `test_failure_recovery.py` |
| Availability + Safety under network partition | deterministic sim | `test_partition.py` |
| Idempotent apply, KV op semantics | deterministic sim | `test_state_machine.py` |
| Same invariants, real socket I/O + real process kill | asyncio/TCP | `test_real_network.py` |

The deterministic harness (`raft/simulator.py`) models the network as a
synchronous message-passing system with injectable partition/drop/kill/
restart operations, making adversarial scheduling (leader crash mid-
quorum, partition-then-heal) a reproducible unit test rather than a
timing-dependent integration test. The identical `RaftNode` class, zero
modification, is wrapped by `kvstore/server.py` with real
`asyncio.start_server`/`asyncio.open_connection` sockets for the
`test_real_network.py` suite — which is where a genuine liveness gap
(client retry exhaustion during the roughly-one-election-timeout window
with no elected leader) was caught under real wall-clock scheduling,
invisible to the zero-latency deterministic harness.

## Complexity and design notes

- `RaftNode` state transitions are O(1) per RPC excluding log
  operations; `ReplicatedLog.truncate_from`/`slice_from` are O(n) in the
  conflicting-suffix length, consistent with the reference
  implementation's amortized cost model.
- `_advance_commit_index` is O(n·f) worst case per leader tick (n = log
  entries since last commit, f = follower count) — acceptable at this
  scale; a production system would maintain a sorted `matchIndex`
  structure for O(f log f) median-finding instead of the linear scan
  used here.
- No log compaction/snapshotting: unbounded log growth, O(n) full-log
  transfer to a far-behind follower. Explicitly out of scope, documented
  in ARCHITECTURE.md.
- No linearizable reads (no read-index/lease-read protocol implemented):
  `GET` is served from local state on whichever node receives it, which
  is safe under normal operation but can return stale data from an
  isolated former leader that hasn't yet stepped down.

## Running it

Zero third-party dependencies for the consensus/replication core —
`asyncio`, `dataclasses`, `enum`, `random` from stdlib only. `pytest` is
optional tooling for structured test output.

```bash
pip install -r requirements.txt
pytest tests/ -v          # 23 tests
python scripts/run_demo.py   # 5-node cluster, live leader termination, live failover
```

## Repository layout

```
src/
  raft/
    messages.py    # RequestVote{Request,Response}, AppendEntries{Request,Response}, LogEntry
    log.py          # ReplicatedLog: 1-indexed per the paper's convention, O(1) term_at/last_index
    node.py          # RaftNode: transport-agnostic state machine, all RPC handler logic
    simulator.py      # deterministic discrete-event cluster harness w/ chaos primitives
  kvstore/
    protocol.py     # newline-delimited JSON wire format for RPC + client req/resp
    server.py        # asyncio TCP transport adapter over RaftNode
    client.py         # leader-discovery client, multi-round retry w/ backoff
    state_machine.py   # KV apply logic, idempotent by log index
tests/                 # see table above
scripts/run_demo.py    # live 5-node demo w/ real process termination
```
