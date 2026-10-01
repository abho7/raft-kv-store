"""
Real-network failover experiments on a 5-node asyncio/TCP cluster (localhost).

For each trial: start cluster, wait for a leader, write a key, stop the
leader's server, then immediately issue a new write through KVClient and
record (a) election latency: kill -> a new leader exists among survivors,
(b) client-observed recovery: kill -> the new write is acknowledged,
(c) whether all pre-crash data is still readable.

Usage: python experiments/failover.py --trials 20 --backoff 0.3 --max-retries 8
"""
import argparse, asyncio, statistics, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from kvstore.client import KVClient, KVClientError
from kvstore.server import RaftServer

IDS = ["n1", "n2", "n3", "n4", "n5"]


async def _leader(servers):
    while True:
        ls = [n for n, s in servers.items() if s.node.role.value == "leader"]
        if len(ls) == 1:
            return ls[0]
        await asyncio.sleep(0.002)


async def trial(port, backoff, max_retries):
    addrs = {n: ("127.0.0.1", port + i) for i, n in enumerate(IDS)}
    servers = {}
    for n, (h, p) in addrs.items():
        s = RaftServer(n, {k: v for k, v in addrs.items() if k != n}, h, p, tick_interval_seconds=0.02)
        servers[n] = s
        await s.start()
    try:
        old = await _leader(servers)
        client = KVClient(addrs, retry_backoff_seconds=backoff, max_retries=max_retries)
        await client.set("pre", "crash")
        t0 = time.perf_counter()
        await servers[old].stop(); del servers[old]
        elect_task = asyncio.create_task(_leader(servers))
        ok = True
        try:
            await client.set("post", "crash")
        except KVClientError:
            ok = False
        recover = time.perf_counter() - t0
        await elect_task
        intact = ok and (await client.get("pre")) == "crash" and (await client.get("post")) == "crash"
        return ok, intact, recover
    finally:
        for s in servers.values():
            await s.stop()


async def election_trial(port):
    addrs = {n: ("127.0.0.1", port + i) for i, n in enumerate(IDS)}
    servers = {}
    for n, (h, p) in addrs.items():
        s = RaftServer(n, {k: v for k, v in addrs.items() if k != n}, h, p, tick_interval_seconds=0.02)
        servers[n] = s
        await s.start()
    try:
        old = await _leader(servers)
        await asyncio.sleep(0.2)
        t0 = time.perf_counter()
        await servers[old].stop(); del servers[old]
        await _leader(servers)
        return time.perf_counter() - t0
    finally:
        for s in servers.values():
            await s.stop()


async def main(a):
    port = 20000
    if a.mode == "election":
        xs = []
        for i in range(a.trials):
            xs.append(await election_trial(port + i * 10) * 1000)
        q = statistics.quantiles(xs, n=20)
        print(f"election_latency_ms trials={a.trials} median={statistics.median(xs):.0f} "
              f"p5={q[0]:.0f} p95={q[-1]:.0f} min={min(xs):.0f} max={max(xs):.0f}")
        return
    res = []
    for i in range(a.trials):
        res.append(await trial(port + i * 10, a.backoff, a.max_retries))
    oks = [r for r in res if r[0]]
    ts = [r[2] * 1000 for r in oks]
    summary = f"median_ms={statistics.median(ts):.0f} min_ms={min(ts):.0f} max_ms={max(ts):.0f}" if ts else ""
    print(f"backoff={a.backoff} max_retries={a.max_retries} trials={a.trials} "
          f"write_succeeded={len(oks)} data_intact={sum(r[1] for r in res)} {summary}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["recovery", "election"], default="recovery")
    ap.add_argument("--trials", type=int, default=20)
    ap.add_argument("--backoff", type=float, default=0.3)
    ap.add_argument("--max-retries", type=int, default=8)
    asyncio.run(main(ap.parse_args()))
