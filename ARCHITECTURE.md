# Architecture

## 1. What Raft actually guarantees

Raft is a consensus algorithm: it lets a cluster of machines agree on an
ordered sequence of commands (a replicated log) even when some machines
crash or the network partitions, as long as a majority of the cluster
can still talk to each other. Two properties matter most, and this
project's tests are organized around proving both directly rather than
asserting them:

- **Election Safety** -- at most one leader can be elected in a given
  term. Two nodes can never both believe they're the leader *for the
  same term* at the same time.
- **Leader Completeness / State Machine Safety** -- once an entry is
  committed (replicated to a majority), it is never lost, and no other
  node will ever apply a *different* command at that same log index.
  This is what makes the crash-recovery and partition tests the real
  point of the project, not just the election mechanics.

## 2. Dataflow

```
                     +---------------------------+
                     |         Client               |
                     |  (kvstore/client.py)          |
                     +--------------+--------------+
                                    | SET/GET/DELETE (TCP, JSON-line)
                                    v
                     +---------------------------+
                     |   RaftServer (any node)      |
                     |   (kvstore/server.py)          |
                     +--------------+--------------+
                                    | not leader? -> {success:false, leader_hint}
                                    | client retries against the hint
                                    v
                     +---------------------------+
                     |    RaftNode.propose()         |<-----------+
                     |    (raft/node.py)               |            |
                     +--------------+--------------+            |
                                    | append to own log,             |
                                    | send AppendEntries to peers      |
                                    v                                 |
                  +----------------------------------+              |
                  |   Peer RaftNodes (other machines)    |              |
                  |   handle_append_entries() checks:      |              |
                  |     - term validity                      |              |
                  |     - log consistency (prevLogIndex/Term)  |              |
                  |     - conflict resolution (truncate+append) |            |
                  +------------------+-----------------+              |
                                    | AppendEntriesResponse              |
                                    +-------------------------------------+
                                    | leader counts majority match_index
                                    v
                     +---------------------------+
                     |   commit_index advances        |
                     |   (only for entries from the     |
                     |    leader's OWN current term)      |
                     +--------------+--------------+
                                    v
                     +---------------------------+
                     |   KVStateMachine.apply()       |
                     |   (kvstore/state_machine.py)     |
                     |   idempotent by log index          |
                     +---------------------------+
```

Two implementations of the transport sit on either side of the exact
same `RaftNode` class, unmodified:

- **`raft/simulator.py`** -- an in-memory, deterministic harness. Message
  delivery is immediate and synchronous, chaos operations (`kill_node`,
  `partition`, `restart_node`) are direct method calls. This is what the
  `tests/test_election.py`, `test_log_replication.py`,
  `test_failure_recovery.py`, and `test_partition.py` suites run
  against -- it makes "kill the leader mid-write, assert no data loss" a
  fast, deterministic, repeatable unit test instead of a flaky
  integration test.
- **`kvstore/server.py`** -- real `asyncio` TCP sockets, real
  independently-ticking event loops, real network round trips. This is
  what `tests/test_real_network.py` and `scripts/run_demo.py` run
  against, and it's what you'd actually deploy.

## 3. Real bugs found while building this, and how

Three real correctness issues surfaced during development -- documented
here because *finding and fixing them through testing* is a stronger
signal than a green test suite that never had anything to catch.

**1. Test harness conflated "alive" with "caught up."** The first
version of `run_until_committed` waited for *every alive node* to apply
an entry before declaring it committed. That's wrong: Raft's actual
definition of "committed" is *majority* agreement, and a node can be
alive but cut off by a network partition, in which case it's correct and
expected for it to lag behind indefinitely until the partition heals.
Fixed by changing the wait condition to majority count, matching Raft's
own definition.

**2. Idempotent re-application after restart.** A restarted node's
`last_applied` counter resets to 0 per the Raft paper (only
`current_term`, `voted_for`, and the log itself are required to be
durable). That means a node that crashes and rejoins will legitimately
re-report its entire already-committed prefix as "newly applied" from
its own point of view. The test harness's bookkeeping was a naive
append-only list, so it double-counted the pre-crash entries as
duplicates. Fixed by tracking applied entries by log index (an
idempotent overwrite) instead of appending -- which is also exactly what
a real state machine layer must do, and `KVStateMachine.apply()` is
written that way for the same reason. See
`tests/test_failure_recovery.py::test_restart_reapplication_is_idempotent_not_duplicated`.

**3. The real-socket client needed multi-round retry, not single-pass.**
Right after a leader crashes, there's a genuine window -- roughly one
election timeout -- where no node is leader yet because an election is
in progress. A client that tries every node once and gives up will
spuriously fail requests made during that window. This only showed up
when testing against *real* crashed TCP servers with real wall-clock
timing; the deterministic simulator's instantaneous message delivery has
no equivalent gap to expose it. Fixed by making `KVClient` retry across
multiple rounds with backoff instead of one pass through the cluster.
See `tests/test_real_network.py::test_leader_crash_and_client_failover_over_real_sockets`.

## 4. Project layout

```
raft-kv-store/
  src/
    raft/
      messages.py     # RPC message types (RequestVote, AppendEntries + responses)
      log.py           # ReplicatedLog: 1-indexed, matches the paper's convention
      node.py           # RaftNode: the actual consensus algorithm, transport-agnostic
      simulator.py       # deterministic in-memory test harness (chaos: kill/restart/partition)
    kvstore/
      state_machine.py  # KV apply logic, idempotent by log index
      protocol.py        # JSON-line wire serialization
      server.py           # real asyncio TCP server wrapping RaftNode
      client.py           # leader-discovery + multi-round retry client
  tests/
    test_election.py         # Election Safety: exactly one leader per term
    test_log_replication.py   # writes replicate and commit in order
    test_failure_recovery.py   # THE core test: crash the leader, no data loss
    test_partition.py           # split-brain prevention
    test_state_machine.py        # KV semantics, idempotent apply
    test_real_network.py          # same properties, over actual TCP sockets
  scripts/
    run_demo.py                    # watch a real 5-node cluster survive a real leader crash
```

## 5. What's simplified, stated plainly

- **No persistence to disk.** `current_term`, `voted_for`, and the log
  are supposed to survive a real process crash by being fsync'd before
  responding to any RPC. This implementation keeps them in memory only
  -- `restart_node` in the simulator models what *should* survive, but a
  real crash of the `kvstore/server.py` process would lose everything.
  Adding a write-ahead log to disk is the highest-value next step for
  making this production-real rather than demo-real.
- **No linearizable reads.** Client `GET`s are served locally by
  whichever node receives them, straight from its state machine, with no
  read-index or leader-lease protocol. A client could read slightly
  stale data from a lagging follower, or from an isolated former leader
  that doesn't yet know it's been superseded.
- **No log compaction / snapshotting.** The log grows forever. A real
  deployment needs periodic snapshots so old entries can be discarded.
- **Decrement-by-one nextIndex backoff on AppendEntries failure**, not
  the conflict-term-index optimization from the end of the Raft paper.
  Correct, just slower to converge for a follower that's very far behind.
