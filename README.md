# Raft KV Store

[![tests](https://github.com/YOUR_USERNAME/YOUR_REPO_NAME/actions/workflows/tests.yml/badge.svg)](https://github.com/YOUR_USERNAME/YOUR_REPO_NAME/actions/workflows/tests.yml)

A distributed key-value store built on a real implementation of the Raft
consensus algorithm -- leader election, log replication, and crash
recovery, not a toy simplification of any of them.

**The one-sentence version:** kill the leader mid-write and the cluster
elects a new one and keeps serving requests with zero data loss --
proven with 23 passing tests, including four that run against actual
TCP sockets and a real crashed process, not just a simulation.

See [ARCHITECTURE.md](./ARCHITECTURE.md) for the dataflow diagram, the
Raft safety properties this project actually proves, and three real bugs
that testing caught during development (not glossed over -- documented,
because that's more convincing than a test suite that never had anything
to catch).

## Why this project

Raft is one of the most respected "I actually understand distributed
systems" things to have built, precisely because it's hard to fake: a
correct implementation has to get leader election, log matching, and
commit safety right *simultaneously*, under crashes and network
partitions, or it silently corrupts data in ways that only show up under
adversarial testing. This is the algorithm behind etcd, CockroachDB, and
most modern distributed databases.

## Quickstart

No external dependencies for the core algorithm -- it's pure Python
stdlib (`asyncio`, `dataclasses`, `random`). `pytest` is only needed to
run the test suite with nice output; the tests also run under a plain
Python script if you don't have it installed.

```bash
pip install -r requirements.txt   # just pytest, for running tests nicely
pytest tests/ -v                  # 23 tests
```

Watch a real 5-node cluster survive a real leader crash:
```bash
python scripts/run_demo.py
```

## What's tested, and how

| Property | Test file | Harness |
|---|---|---|
| Exactly one leader per term | test_election.py | deterministic simulator |
| Writes replicate and commit in order | test_log_replication.py | deterministic simulator |
| No data loss after a leader crash | test_failure_recovery.py | deterministic simulator |
| No split-brain after a network partition heals | test_partition.py | deterministic simulator |
| KV semantics, idempotent apply | test_state_machine.py | deterministic simulator |
| Same crash-recovery guarantee, real sockets | test_real_network.py | real asyncio TCP |

The deterministic simulator (`raft/simulator.py`) is what makes "kill
the leader mid-write and prove no committed entry is lost" a fast,
repeatable unit test instead of a flaky integration test involving real
processes and real timing. The exact same `RaftNode` class, unmodified,
also runs under real TCP sockets in `kvstore/server.py` -- so the real
network tests aren't testing a different implementation, they're testing
the same algorithm under real wall-clock timing and real socket failure,
which is where one of the three bugs documented in ARCHITECTURE.md was
actually found.

## Project layout

See the "Project layout" section in [ARCHITECTURE.md](./ARCHITECTURE.md)
for the full annotated tree.

## What's left to do

Stated plainly in ARCHITECTURE.md's "What's simplified" section --
briefly: no disk persistence (everything is in-memory, so a real process
crash loses state that a production system would fsync), no
linearizable reads (a read-index or leader-lease protocol), no log
compaction/snapshotting, and the simpler decrement-by-one nextIndex
backoff instead of the paper's conflict-term optimization.
