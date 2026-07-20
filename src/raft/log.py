"""
The replicated log itself. Indices are 1-based to match the Raft paper's
convention exactly (index 0 is a sentinel meaning "before the log
starts"), which makes the prevLogIndex=0 case in AppendEntries fall out
naturally instead of needing a special case.
"""

from __future__ import annotations

from raft.messages import LogEntry


class ReplicatedLog:
    def __init__(self) -> None:
        self._entries: list[LogEntry] = []  # _entries[0] corresponds to index 1

    def __len__(self) -> int:
        return len(self._entries)

    @property
    def last_index(self) -> int:
        return len(self._entries)

    @property
    def last_term(self) -> int:
        if not self._entries:
            return 0
        return self._entries[-1].term

    def term_at(self, index: int) -> int:
        """Term of the entry at `index` (1-based), or 0 if index is 0
        (the before-the-log sentinel) or out of range."""
        if index <= 0 or index > len(self._entries):
            return 0
        return self._entries[index - 1].term

    def get(self, index: int) -> LogEntry | None:
        if index <= 0 or index > len(self._entries):
            return None
        return self._entries[index - 1]

    def slice_from(self, index: int) -> list[LogEntry]:
        """Entries from `index` (1-based, inclusive) to the end."""
        if index <= 0:
            return list(self._entries)
        return list(self._entries[index - 1:])

    def append(self, entry: LogEntry) -> None:
        self._entries.append(entry)

    def truncate_from(self, index: int) -> None:
        """Delete this entry and everything after it (1-based). Used when
        a follower's log conflicts with the leader's and must be
        overwritten -- Raft's log matching property guarantees this is
        always safe to do to *uncommitted* entries."""
        if index <= 0:
            self._entries = []
        else:
            self._entries = self._entries[: index - 1]

    def matches(self, index: int, term: int) -> bool:
        """The AppendEntries consistency check: does our entry at `index`
        have the given term? Index 0 always matches (sentinel)."""
        if index == 0:
            return True
        return self.term_at(index) == term

    def all_entries(self) -> list[LogEntry]:
        return list(self._entries)
