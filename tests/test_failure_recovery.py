from raft.simulator import SimulatedCluster


def test_cluster_survives_leader_crash_with_no_data_loss():
    """The central claim of the whole project: kill the leader mid-operation,
    the cluster elects a new one, writes continue, and every committed
    entry is preserved with no corruption. This is what Raft is FOR."""
    cluster = SimulatedCluster(["n1", "n2", "n3"], seed=7)
    leader1 = cluster.run_until_leader(max_ticks=100)

    entry1 = cluster.propose({"op": "set", "key": "x", "value": "1"})
    assert cluster.run_until_committed(entry1.index, max_ticks=100)

    cluster.kill_node(leader1)

    new_leader = cluster.run_until_leader(max_ticks=200)
    assert new_leader is not None
    assert new_leader != leader1

    entry2 = cluster.propose({"op": "set", "key": "y", "value": "2"})
    assert cluster.run_until_committed(entry2.index, max_ticks=100)

    alive = [n for n in cluster.node_ids if cluster.alive[n]]
    reference = cluster.applied_commands(alive[0])
    for nid in alive[1:]:
        assert cluster.applied_commands(nid) == reference
    assert {"op": "set", "key": "x", "value": "1"} in reference
    assert {"op": "set", "key": "y", "value": "2"} in reference


def test_cluster_survives_two_sequential_leader_crashes():
    cluster = SimulatedCluster(["n1", "n2", "n3", "n4", "n5"], seed=21)
    leader1 = cluster.run_until_leader(max_ticks=100)

    e1 = cluster.propose({"op": "set", "key": "a", "value": "1"})
    cluster.run_until_committed(e1.index, max_ticks=100)

    cluster.kill_node(leader1)
    leader2 = cluster.run_until_leader(max_ticks=200)
    assert leader2 is not None and leader2 != leader1

    e2 = cluster.propose({"op": "set", "key": "b", "value": "2"})
    cluster.run_until_committed(e2.index, max_ticks=100)

    cluster.kill_node(leader2)
    leader3 = cluster.run_until_leader(max_ticks=200)
    assert leader3 is not None and leader3 not in (leader1, leader2)

    e3 = cluster.propose({"op": "set", "key": "c", "value": "3"})
    assert cluster.run_until_committed(e3.index, max_ticks=100)

    alive = [n for n in cluster.node_ids if cluster.alive[n]]
    assert len(alive) == 3  # 5 nodes, 2 killed
    reference = cluster.applied_commands(alive[0])
    for nid in alive[1:]:
        assert cluster.applied_commands(nid) == reference
    assert len(reference) == 3


def test_restarted_node_recovers_its_log_and_rejoins():
    cluster = SimulatedCluster(["n1", "n2", "n3"], seed=15)
    leader = cluster.run_until_leader(max_ticks=100)

    e1 = cluster.propose({"op": "set", "key": "x", "value": "1"})
    cluster.run_until_committed(e1.index, max_ticks=100)

    follower = next(n for n in cluster.node_ids if n != leader)
    log_before_crash = cluster.nodes[follower].log.all_entries()

    cluster.kill_node(follower)
    cluster.run_ticks(20)
    cluster.restart_node(follower)

    # Restarted node's persistent log should have survived the "crash".
    assert cluster.nodes[follower].log.all_entries() == log_before_crash

    # It should be able to catch up on new writes after rejoining. Note:
    # majority commit (2 of 3 nodes) can complete without THIS specific
    # follower having caught up yet, since it's not required for the
    # majority -- so we wait specifically for this node, not just for
    # cluster-wide commit.
    e2 = cluster.propose({"op": "set", "key": "y", "value": "2"})
    assert e2 is not None
    for _ in range(100):
        if len(cluster.applied_log[follower]) >= e2.index:
            break
        cluster.tick()
    commands = cluster.applied_commands(follower)
    assert {"op": "set", "key": "y", "value": "2"} in commands


def test_restart_reapplication_is_idempotent_not_duplicated():
    """A restarted node's `last_applied` volatile counter resets to 0 per
    the Raft paper, so once it reconnects it legitimately re-reports its
    entire already-committed prefix as 'newly applied' from its own point
    of view. A correct state machine layer (and this test's own
    bookkeeping) must treat re-application as an idempotent overwrite by
    index, not an append -- otherwise a restarted node's history would
    silently accumulate duplicate entries every time it crashes and
    rejoins, which found and fixed a real bug in this project's own test
    harness while building it (see SimulatedCluster.applied_commands)."""
    cluster = SimulatedCluster(["n1", "n2", "n3"], seed=15)
    leader = cluster.run_until_leader(max_ticks=100)

    e1 = cluster.propose({"op": "set", "key": "x", "value": "1"})
    cluster.run_until_committed(e1.index, max_ticks=100)

    follower = next(n for n in cluster.node_ids if n != leader)
    cluster.kill_node(follower)
    cluster.run_ticks(20)
    cluster.restart_node(follower)
    cluster.run_ticks(20)  # let it reconnect and re-report its pre-crash prefix

    commands = cluster.applied_commands(follower)
    assert commands.count({"op": "set", "key": "x", "value": "1"}) == 1, (
        f"expected exactly one copy of the pre-crash entry, got {commands} -- "
        "re-application after restart produced a duplicate"
    )
