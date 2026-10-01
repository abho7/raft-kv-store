"""
Randomized chaos fuzzing of the deterministic simulator, with invariant
checking after every tick, plus mutation analysis.

Invariants checked (Ongaro & Ousterhout 2014, Fig. 3):
  ES  Election Safety: at most one leader per term (over the whole run).
  SMS State Machine Safety: no two nodes apply different entries at an index.
  LC  Leader Completeness: if an entry is committed by term T, every leader
      of a term > T holds it at the same index with the same term. We use
      the applying node's term at first observation as an upper bound on T,
      so the check never flags a stale leader of an older term.

Usage: python experiments/chaos_fuzz.py [--seeds N] [--ticks T] [--mutant NAME]
"""
from __future__ import annotations
import argparse, random, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from raft.simulator import SimulatedCluster
from raft.node import Role
import mutants
from delayed_sim import DelayedCluster

NODES = ["n1", "n2", "n3", "n4", "n5"]


def run_one(seed: int, ticks: int, drop_rate: float = 0.1, max_delay: int = 0):
    rng = random.Random(seed)
    c = (DelayedCluster(NODES, drop_rate=drop_rate, seed=seed, max_delay=max_delay)
         if max_delay > 0 else SimulatedCluster(NODES, drop_rate=drop_rate, seed=seed))
    leaders_by_term: dict[int, set[str]] = {}
    committed: dict[int, tuple[int, object]] = {}  # index -> (term, command)
    seen_at: dict[int, int] = {}  # index -> applying node's term when first observed
    stats = {"proposals": 0, "kills": 0, "restarts": 0, "partitions": 0}
    nxt = 0
    for t in range(ticks):
        r = rng.random()
        alive = [n for n in NODES if c.alive[n]]
        dead = [n for n in NODES if not c.alive[n]]
        if r < 0.01 and len(alive) > 1:
            c.kill_node(rng.choice(alive)); stats["kills"] += 1
        elif r < 0.02 and dead:
            c.restart_node(rng.choice(dead)); stats["restarts"] += 1
        elif r < 0.025:
            k = rng.randint(1, 2)
            a = set(rng.sample(NODES, k)); c.partition(a, set(NODES) - a); stats["partitions"] += 1
        elif r < 0.035:
            c.heal_partition()
        if rng.random() < 0.3:
            nxt += 1
            if c.propose({"op": "set", "key": f"k{nxt % 7}", "value": nxt}) is not None:
                stats["proposals"] += 1
        c.tick()

        # --- invariants ---
        for nid, node in c.nodes.items():
            if c.alive[nid] and node.role == Role.LEADER:
                s = leaders_by_term.setdefault(node.current_term, set()); s.add(nid)
                if len(s) > 1:
                    return "ES", t, stats
        for nid in NODES:
            for idx, e in c.applied_log[nid].items():
                prev = committed.get(idx)
                if prev is None:
                    committed[idx] = (e.term, e.command)
                    seen_at[idx] = c.nodes[nid].current_term
                elif prev != (e.term, e.command):
                    return "SMS", t, stats
        for nid, node in c.nodes.items():
            if c.alive[nid] and node.role == Role.LEADER:
                for idx, (term, _) in committed.items():
                    if node.current_term <= seen_at[idx]:
                        continue
                    le = node.log.get(idx)
                    if le is None or le.term != term:
                        return "LC", t, stats
    stats["committed"] = len(committed)
    return None, ticks, stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", type=int, default=200)
    ap.add_argument("--ticks", type=int, default=3000)
    ap.add_argument("--mutant", default="none")
    ap.add_argument("--max-delay", type=int, default=0, help="0 = stock zero-latency simulator")
    a = ap.parse_args()
    undo = mutants.apply(a.mutant)
    t0 = time.perf_counter()
    viol, first = {}, None
    tot = {"proposals": 0, "kills": 0, "restarts": 0, "partitions": 0, "committed": 0}
    for s in range(a.seeds):
        v, t, st = run_one(s, a.ticks, max_delay=a.max_delay)
        for k in tot: tot[k] += st.get(k, 0)
        if v:
            viol[v] = viol.get(v, 0) + 1
            if first is None: first = (s, t, v)
    undo()
    dt = time.perf_counter() - t0
    failing = sum(viol.values())
    print(f"mutant={a.mutant} delay={a.max_delay} seeds={a.seeds} ticks={a.ticks} failing_seeds={failing} "
          f"by_invariant={viol} first={first} totals={tot} secs={dt:.1f}")


if __name__ == "__main__":
    main()
