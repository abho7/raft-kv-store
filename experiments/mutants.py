"""Deliberately injected protocol bugs for mutation analysis."""
from raft.node import RaftNode, Role
from raft.messages import RequestVoteResponse
from raft.node import Actions

_orig = {}

def _save(name):
    _orig[name] = getattr(RaftNode, name)

def m_fig8(self):
    # M1: drop the current-term restriction (Raft paper, Figure 8).
    for n in range(self.log.last_index, self.commit_index, -1):
        rc = 1 + sum(1 for p in self.peer_ids if self.match_index.get(p, 0) >= n)
        if rc * 2 > self.cluster_size:
            self.commit_index = n
            return

def m_no_logcheck(self, sender_id, req):
    # M2: grant votes without the log up-to-date check.
    if req.term > self.current_term:
        self._step_down(req.term)
    granted = False
    if req.term == self.current_term and (self.voted_for in (None, req.candidate_id)):
        granted = True; self.voted_for = req.candidate_id; self.election_elapsed = 0
    return Actions(messages=[(sender_id, RequestVoteResponse(term=self.current_term, vote_granted=granted, voter_id=self.node_id))])

def m_double_vote(self, sender_id, req):
    # M3: ignore voted_for (a node may vote for several candidates per term).
    if req.term > self.current_term:
        self._step_down(req.term)
    log_ok = (req.last_log_term > self.log.last_term or
              (req.last_log_term == self.log.last_term and req.last_log_index >= self.log.last_index))
    granted = req.term == self.current_term and log_ok
    if granted:
        self.voted_for = req.candidate_id; self.election_elapsed = 0
    return Actions(messages=[(sender_id, RequestVoteResponse(term=self.current_term, vote_granted=granted, voter_id=self.node_id))])

MUTANTS = {
    "fig8": ("_advance_commit_index", m_fig8),
    "no_logcheck": ("handle_request_vote", m_no_logcheck),
    "double_vote": ("handle_request_vote", m_double_vote),
}

def apply(name):
    if name == "none":
        return lambda: None
    attr, fn = MUTANTS[name]
    orig = getattr(RaftNode, attr)
    setattr(RaftNode, attr, fn)
    return lambda: setattr(RaftNode, attr, orig)
