from raft.simulator import SimulatedCluster
from kvstore.state_machine import KVStateMachine


def _sync_machines(cluster, machines):
    for nid in cluster.node_ids:
        entries_sorted = [cluster.applied_log[nid][i] for i in sorted(cluster.applied_log[nid])]
        machines[nid].apply_all(entries_sorted)


def test_set_get_delete_replicate_and_converge():
    cluster = SimulatedCluster(["n1", "n2", "n3"], seed=4)
    cluster.run_until_leader(max_ticks=100)
    machines = {nid: KVStateMachine() for nid in cluster.node_ids}

    e1 = cluster.propose({"op": "set", "key": "name", "value": "raft"})
    cluster.run_until_committed(e1.index, max_ticks=100)
    _sync_machines(cluster, machines)
    assert all(m.get("name") == "raft" for m in machines.values())

    e2 = cluster.propose({"op": "set", "key": "name", "value": "updated"})
    cluster.run_until_committed(e2.index, max_ticks=100)
    _sync_machines(cluster, machines)
    assert all(m.get("name") == "updated" for m in machines.values())

    e3 = cluster.propose({"op": "delete", "key": "name"})
    cluster.run_until_committed(e3.index, max_ticks=100)
    _sync_machines(cluster, machines)
    assert all(m.get("name") is None for m in machines.values())

    reference_snapshot = machines["n1"].snapshot()
    assert all(m.snapshot() == reference_snapshot for m in machines.values())


def test_apply_is_idempotent_for_repeated_index():
    machine = KVStateMachine()
    from raft.messages import LogEntry

    entry = LogEntry(term=1, index=1, command={"op": "set", "key": "x", "value": "1"})
    machine.apply(entry)
    machine.apply(entry)  # re-applying the same index must be a no-op
    machine.apply(entry)
    assert machine.get("x") == "1"
    assert machine.last_applied_index == 1


def test_get_on_missing_key_returns_none():
    machine = KVStateMachine()
    assert machine.get("nonexistent") is None


def test_unknown_op_raises():
    from raft.messages import LogEntry

    machine = KVStateMachine()
    entry = LogEntry(term=1, index=1, command={"op": "not_a_real_op", "key": "x"})
    try:
        machine.apply(entry)
        assert False, "expected ValueError for unknown op"
    except ValueError:
        pass
