# Experiments

Scripts behind the paper "Deterministic Simulation Is Not Enough: Dual-Harness
Verification of a From-Scratch Raft Implementation". Raw outputs are in `../results/`.
None of these files modify `src/`; mutants are applied at run time.

| Script | What it does |
|---|---|
| `chaos_fuzz.py` | Random crash/restart/partition/drop schedules on a 5-node cluster, checking Election Safety, State Machine Safety and Leader Completeness after every tick. `--max-delay D` switches to the latency-injecting simulator. |
| `delayed_sim.py` | `DelayedCluster`: `SimulatedCluster` with random 1..D-tick message delays (late, out-of-order delivery). |
| `mutants.py` | Injected bugs: `fig8` (no current-term commit rule), `no_logcheck` (vote without log check), `double_vote` (ignore votedFor). |
| `run_tests_with_mutant.py` | Runs the pytest suite with one mutant applied. |
| `failover.py` | Real asyncio/TCP 5-node cluster: election latency, client-observed recovery vs. retry backoff, single-pass client (`--max-retries 1`). |

Reproduce:

```bash
for d in 0 4; do for m in none fig8 no_logcheck double_vote; do
  python experiments/chaos_fuzz.py --seeds 200 --ticks 3000 --max-delay $d --mutant $m
done; done
for m in none fig8 no_logcheck double_vote; do python experiments/run_tests_with_mutant.py $m; done
python experiments/failover.py --mode election --trials 30
for b in 0.05 0.1 0.2 0.3; do python experiments/failover.py --trials 20 --backoff $b; done
python experiments/failover.py --trials 20 --backoff 0.3 --max-retries 1
```

`tests/test_figure8.py` is a deterministic reconstruction of Figure 8 of the Raft paper and is part of the normal test suite.
