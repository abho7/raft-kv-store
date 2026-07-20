from raft.simulator import SimulatedCluster


def test_isolated_minority_leader_cannot_commit():
    """The other central safety claim: if the old leader gets cut off in
    a minority partition, it must NOT be able to commit new writes, even
    though it still believes it's the leader -- because it can't reach a
    majority of the cluster."""
    cluster = SimulatedCluster(["n1", "n2", "n3", "n4", "n5"], seed=3)
    leader1 = cluster.run_until_leader(max_ticks=100)

    e1 = cluster.propose({"op": "set", "key": "a", "value": "1"})
    cluster.run_until_committed(e1.index, max_ticks=100)

    others = [n for n in cluster.node_ids if n != leader1]
    cluster.partition({leader1}, set(others))
    cluster.run_ticks(50)

    isolated = cluster.nodes[leader1]
    assert isolated.role.value == "leader"  # it doesn't know yet -- that's expected and correct

    stale_entry, actions = isolated.propose({"op": "set", "key": "STALE", "value": "x"})
    cluster._handle_actions(leader1, actions)
    cluster.run_ticks(20)

    assert isolated.commit_index == 1, "isolated minority leader must not be able to commit without majority contact"


def test_majority_partition_elects_new_leader_and_makes_progress():
    cluster = SimulatedCluster(["n1", "n2", "n3", "n4", "n5"], seed=3)
    leader1 = cluster.run_until_leader(max_ticks=100)
    e1 = cluster.propose({"op": "set", "key": "a", "value": "1"})
    cluster.run_until_committed(e1.index, max_ticks=100)

    others = [n for n in cluster.node_ids if n != leader1]
    cluster.partition({leader1}, set(others))
    cluster.run_ticks(50)

    majority_leaders = [n for n in others if cluster.nodes[n].role.value == "leader"]
    assert len(majority_leaders) == 1

    new_leader = majority_leaders[0]
    entry, actions = cluster.nodes[new_leader].propose({"op": "set", "key": "b", "value": "2"})
    cluster._handle_actions(new_leader, actions)
    assert cluster.run_until_committed(entry.index, max_ticks=100)


def test_no_split_brain_and_no_stale_writes_survive_after_healing():
    cluster = SimulatedCluster(["n1", "n2", "n3", "n4", "n5"], seed=3)
    leader1 = cluster.run_until_leader(max_ticks=100)
    e1 = cluster.propose({"op": "set", "key": "a", "value": "1"})
    cluster.run_until_committed(e1.index, max_ticks=100)

    others = [n for n in cluster.node_ids if n != leader1]
    cluster.partition({leader1}, set(others))
    cluster.run_ticks(50)

    # Stale write to the isolated old leader -- must never appear anywhere after healing.
    stale_entry, actions = cluster.nodes[leader1].propose({"op": "set", "key": "STALE", "value": "x"})
    cluster._handle_actions(leader1, actions)

    # Real write via the majority's new leader.
    new_leader = next(n for n in others if cluster.nodes[n].role.value == "leader")
    real_entry, actions2 = cluster.nodes[new_leader].propose({"op": "set", "key": "b", "value": "2"})
    cluster._handle_actions(new_leader, actions2)
    cluster.run_until_committed(real_entry.index, max_ticks=100)

    cluster.heal_partition()
    cluster.run_ticks(50)

    leaders_after_heal = cluster.current_leaders()
    assert len(leaders_after_heal) == 1, f"expected exactly one leader after healing, got {leaders_after_heal}"
    assert leaders_after_heal[0] == new_leader, "the majority partition's leader should remain leader"

    for nid in cluster.node_ids:
        commands = cluster.applied_commands(nid)
        assert not any(c.get("key") == "STALE" for c in commands), (
            f"{nid} has the stale never-committed write -- this is a split-brain safety violation"
        )
