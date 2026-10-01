"""
Deterministic reproduction of Figure 8 of the Raft paper (Ongaro &
Ousterhout, 2014): why a leader must not commit an entry from an earlier
term by counting its replicas.

The cluster is placed directly into the state of Fig. 8(b)/(c) by writing
each node's persistent state (term, vote, log), then driven through
elections with the simulator's normal delivery path:

  (c) S1 is elected in term 4 and replicates its term-2 entry at index 2
      to a majority.  A correct leader must NOT treat it as committed.
  (d) S1 crashes; S5 (whose index-2 entry is from term 3) wins term 5 and
      overwrites index 2 on the majority.

State Machine Safety requires that no two nodes ever apply different
entries at the same index. With the current-term restriction removed,
S1 applies (term 2, "a") and S5 later applies (term 3, "b") at index 2.
"""
from raft.messages import LogEntry
from raft.simulator import SimulatedCluster

NODES = ["s1", "s2", "s3", "s4", "s5"]


def _load(node, entries, term, voted_for=None):
    for e in entries:
        node.log.append(e)
    node.current_term = term
    node.voted_for = voted_for


def _elect(cluster, nid, term_before):
    node = cluster.nodes[nid]
    node.current_term = term_before
    cluster._handle_actions(nid, node._start_election())
    return node.role.value == "leader"


def _conflicting_applies(cluster):
    seen = {}
    for nid in NODES:
        for idx, e in cluster.applied_log[nid].items():
            if idx in seen and seen[idx] != (e.term, repr(e.command)):
                return idx
            seen.setdefault(idx, (e.term, repr(e.command)))
    return None


def test_figure8_old_term_entry_is_not_committed_by_replica_count():
    c = SimulatedCluster(NODES, election_timeout_ticks=(10_000, 10_001), seed=0)
    base = LogEntry(term=1, index=1, command={"op": "set", "key": "k", "value": "base"})
    a = LogEntry(term=2, index=2, command={"op": "set", "key": "k", "value": "a"})
    b = LogEntry(term=3, index=2, command={"op": "set", "key": "k", "value": "b"})

    # Fig. 8(b): S1 led term 2 and replicated `a` only to S2, then crashed.
    # S5 won term 3 with votes from S3, S4 and appended `b` locally only.
    _load(c.nodes["s1"], [base, a], term=2, voted_for="s1")
    _load(c.nodes["s2"], [base, a], term=2, voted_for="s1")
    _load(c.nodes["s3"], [base], term=3, voted_for="s5")
    _load(c.nodes["s4"], [base], term=3, voted_for="s5")
    _load(c.nodes["s5"], [base, b], term=3, voted_for="s5")
    c.kill_node("s5")

    # Fig. 8(c): S1 wins term 4 and replicates `a` to S2, S3, S4.
    assert _elect(c, "s1", term_before=3)
    c.run_ticks(5)
    assert all(c.nodes[n].log.get(2) == a for n in ["s1", "s2", "s3", "s4"])
    assert c.nodes["s1"].commit_index < 2, "leader committed an old-term entry by counting replicas"

    # Fig. 8(d): S1 crashes; S5 wins term 5 and overwrites index 2 with `b`.
    c.kill_node("s1")
    c.restart_node("s5")
    assert _elect(c, "s5", term_before=4)
    c.run_ticks(5)
    assert all(c.nodes[n].log.get(2) == b for n in ["s2", "s3", "s4", "s5"])

    # Once S5 commits an entry of its own term, index 2 (`b`) commits with it.
    c.propose({"op": "set", "key": "k", "value": "c"})
    c.run_ticks(5)
    assert c.nodes["s5"].commit_index >= 3
    assert _conflicting_applies(c) is None
