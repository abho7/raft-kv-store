"""
JSON-line wire serialization for Raft RPC messages and client
request/response. One JSON object per line (newline-delimited), sent
over a fresh short-lived TCP connection per message -- simpler and more
robust to get correct than a persistent-connection protocol with its own
framing/reconnection logic, at the cost of some per-message connection
overhead that's irrelevant at the scale this project targets.
"""

from __future__ import annotations

import json

from raft.messages import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    LogEntry,
    RequestVoteRequest,
    RequestVoteResponse,
)

_TYPE_NAMES = {
    RequestVoteRequest: "RequestVoteRequest",
    RequestVoteResponse: "RequestVoteResponse",
    AppendEntriesRequest: "AppendEntriesRequest",
    AppendEntriesResponse: "AppendEntriesResponse",
}


def _entry_to_dict(e: LogEntry) -> dict:
    return {"term": e.term, "index": e.index, "command": e.command}


def _entry_from_dict(d: dict) -> LogEntry:
    return LogEntry(term=d["term"], index=d["index"], command=d["command"])


def serialize_rpc(sender_id: str, message) -> str:
    type_name = _TYPE_NAMES[type(message)]
    if isinstance(message, RequestVoteRequest):
        payload = {
            "term": message.term, "candidate_id": message.candidate_id,
            "last_log_index": message.last_log_index, "last_log_term": message.last_log_term,
        }
    elif isinstance(message, RequestVoteResponse):
        payload = {"term": message.term, "vote_granted": message.vote_granted, "voter_id": message.voter_id}
    elif isinstance(message, AppendEntriesRequest):
        payload = {
            "term": message.term, "leader_id": message.leader_id,
            "prev_log_index": message.prev_log_index, "prev_log_term": message.prev_log_term,
            "entries": [_entry_to_dict(e) for e in message.entries],
            "leader_commit": message.leader_commit,
        }
    elif isinstance(message, AppendEntriesResponse):
        payload = {
            "term": message.term, "success": message.success,
            "responder_id": message.responder_id, "match_index": message.match_index,
        }
    else:
        raise TypeError(f"Cannot serialize message of type {type(message)!r}")

    return json.dumps({"kind": "rpc", "type": type_name, "sender": sender_id, "payload": payload})


def deserialize_rpc(data: dict):
    type_name = data["type"]
    p = data["payload"]
    if type_name == "RequestVoteRequest":
        return RequestVoteRequest(**p)
    if type_name == "RequestVoteResponse":
        return RequestVoteResponse(**p)
    if type_name == "AppendEntriesRequest":
        return AppendEntriesRequest(
            term=p["term"], leader_id=p["leader_id"],
            prev_log_index=p["prev_log_index"], prev_log_term=p["prev_log_term"],
            entries=tuple(_entry_from_dict(e) for e in p["entries"]),
            leader_commit=p["leader_commit"],
        )
    if type_name == "AppendEntriesResponse":
        return AppendEntriesResponse(**p)
    raise TypeError(f"Unknown RPC type name: {type_name!r}")


def serialize_client_request(command: dict) -> str:
    return json.dumps({"kind": "client_request", "command": command})


def serialize_client_response(*, success: bool, value=None, leader_hint: str | None = None, error: str | None = None) -> str:
    return json.dumps({
        "kind": "client_response", "success": success,
        "value": value, "leader_hint": leader_hint, "error": error,
    })
