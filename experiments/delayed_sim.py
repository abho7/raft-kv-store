"""
SimulatedCluster variant with non-zero, random message latency.

The stock simulator delivers messages immediately and recursively inside
one tick, which models a zero-latency network. Here every message is
instead queued with a random delay of 1..max_delay ticks, so messages can
arrive late and out of order, and concurrent elections can overlap.
The RaftNode class is untouched.
"""
from __future__ import annotations
import heapq, itertools
from raft.simulator import SimulatedCluster


class DelayedCluster(SimulatedCluster):
    def __init__(self, *args, max_delay: int = 4, **kw):
        super().__init__(*args, **kw)
        self.max_delay = max_delay
        self._q: list = []
        self._ctr = itertools.count()

    def _deliver(self, sender_id, recipient_id, message):
        due = self.current_tick + self._rng.randint(1, self.max_delay)
        heapq.heappush(self._q, (due, next(self._ctr), sender_id, recipient_id, message))

    def tick(self):
        super().tick()  # advances current_tick, runs node timers (sends get queued)
        while self._q and self._q[0][0] <= self.current_tick:
            _, _, s, r, m = heapq.heappop(self._q)
            SimulatedCluster._deliver(self, s, r, m)
