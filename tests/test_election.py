from raft.simulator import SimulatedCluster


def test_healthy_cluster_elects_exactly_one_leader():
    cluster = SimulatedCluster(["n1", "n2", "n3"], seed=42)
    leader = cluster.run_until_leader(max_ticks=100)
    assert leader is not None
    assert len(cluster.current_leaders()) == 1


def test_single_node_cluster_is_trivially_its_own_leader():
    cluster = SimulatedCluster(["solo"], seed=1)
    leader = cluster.run_until_leader(max_ticks=50)
    assert leader == "solo"


def test_five_node_cluster_elects_leader():
    cluster = SimulatedCluster(["n1", "n2", "n3", "n4", "n5"], seed=99)
    leader = cluster.run_until_leader(max_ticks=150)
    assert leader is not None
    assert len(cluster.current_leaders()) == 1


def test_election_is_deterministic_given_a_seed():
    cluster_a = SimulatedCluster(["n1", "n2", "n3"], seed=123)
    cluster_b = SimulatedCluster(["n1", "n2", "n3"], seed=123)
    leader_a = cluster_a.run_until_leader(max_ticks=100)
    leader_b = cluster_b.run_until_leader(max_ticks=100)
    assert leader_a == leader_b
    assert cluster_a.current_tick == cluster_b.current_tick


def test_different_seeds_can_elect_different_leaders():
    """Not a strict requirement, just confirms the random election timeout
    jitter is actually doing something rather than being a no-op."""
    leaders = set()
    for seed in range(10):
        cluster = SimulatedCluster(["n1", "n2", "n3"], seed=seed)
        leader = cluster.run_until_leader(max_ticks=100)
        leaders.add(leader)
    assert len(leaders) > 1, "expected different seeds to sometimes produce different leaders"
