"""
The actual application built on top of Raft: a key-value store whose
state is fully determined by replaying the committed log in order.

Important correctness property, exercised directly in
tests/test_state_machine.py: apply() must be safe to call more than once
for the same log index with the same command (idempotent), because a
node that restarts legitimately re-reports its already-applied prefix
once it reconnects (see raft/simulator.py and
tests/test_failure_recovery.py::test_restart_reapplication_is_idempotent_not_duplicated
for where this was actually discovered and fixed). This state machine
achieves that simply by tracking the highest index it's applied and
ignoring anything at or below that -- the log itself is the source of
truth for ordering, so a state machine doesn't need per-command
idempotency keys, just a watermark.
"""

from __future__ import annotations

from raft.messages import LogEntry


class KVStateMachine:
    def __init__(self) -> None:
        self._data: dict[str, str] = {}
        self._last_applied_index = 0

    def apply(self, entry: LogEntry) -> None:
        if entry.index <= self._last_applied_index:
            return  # already applied this index (or an older one) -- idempotent no-op
        self._last_applied_index = entry.index

        op = entry.command.get("op")
        if op == "set":
            self._data[entry.command["key"]] = entry.command["value"]
        elif op == "delete":
            self._data.pop(entry.command["key"], None)
        elif op == "noop":
            pass  # used for leader no-op entries in some Raft implementations; harmless here
        else:
            raise ValueError(f"Unknown command op: {op!r}")

    def apply_all(self, entries: list[LogEntry]) -> None:
        for entry in entries:
            self.apply(entry)

    def get(self, key: str) -> str | None:
        return self._data.get(key)

    def snapshot(self) -> dict[str, str]:
        return dict(self._data)

    @property
    def last_applied_index(self) -> int:
        return self._last_applied_index
