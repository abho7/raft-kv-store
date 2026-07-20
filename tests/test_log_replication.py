from raft.simulator import SimulatedCluster


def test_write_replicates_and_commits_on_healthy_cluster():
    cluster = SimulatedCluster(["n1", "n2", "n3"], seed=5)
    cluster.run_until_leader(max_ticks=100)

    entry = cluster.propose({"op": "set", "key": "x", "value": "1"})
    assert entry is not None

    committed = cluster.run_until_committed(entry.index, max_ticks=100)
    assert committed

    for nid in cluster.node_ids:
        commands = cluster.applied_commands(nid)
        assert {"op": "set", "key": "x", "value": "1"} in commands


def test_multiple_sequential_writes_all_commit_in_order():
    cluster = SimulatedCluster(["n1", "n2", "n3"], seed=8)
    cluster.run_until_leader(max_ticks=100)

    entries = []
    for i in range(5):
        e = cluster.propose({"op": "set", "key": f"k{i}", "value": str(i)})
        cluster.run_until_committed(e.index, max_ticks=100)
        entries.append(e)

    for nid in cluster.node_ids:
        commands = cluster.applied_commands(nid)
        expected = [{"op": "set", "key": f"k{i}", "value": str(i)} for i in range(5)]
        assert commands == expected, f"{nid} has out-of-order or missing commands: {commands}"


def test_all_nodes_agree_on_committed_prefix():
    cluster = SimulatedCluster(["n1", "n2", "n3", "n4", "n5"], seed=11)
    cluster.run_until_leader(max_ticks=100)

    for i in range(3):
        e = cluster.propose({"op": "set", "key": f"k{i}", "value": str(i)})
        cluster.run_until_committed(e.index, max_ticks=100)

    logs = [cluster.applied_commands(nid) for nid in cluster.node_ids]
    reference = logs[0]
    for log in logs[1:]:
        assert log == reference
