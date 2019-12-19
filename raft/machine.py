# machine.py
#
# Implementation of the Raft state machine

from typing import NamedTuple
from .raftlog import RaftLog, Entry

# RPC Messages
class AppendEntries(NamedTuple):
    source: int
    dest: int
    term: int
    prev_log_index: int
    prev_log_term: int
    entries: list
    leader_commit: int

class AppendEntriesResponse(NamedTuple):
    source: int
    dest: int
    term: int
    success: bool
    match_index: int

class RequestVote(NamedTuple):
    source: int
    dest: int
    term: int
    last_log_index: int
    last_log_term: int

class RequestVoteResponse(NamedTuple):
    source: int
    dest: int
    term: int
    vote_granted: bool

class RaftMachine:
    def __init__(self, control):
        self.state = None
        self.control = control

        # Persistent state.  ?????? Persistence where? who?
        self.current_term = 0
        self.voted_for = None
        self.log = RaftLog()

        # Volatile state
        self.commit_index = -1       # Highest log entry known to be committed        
        self.last_applied = -1       # Highest log entry applied to the state machine

        # Volatile state on leaders
        self.next_index = { }        # Index of next log entry that gets sent
        self.match_index = { }       # Index of highest log entry known to be replicated

        self.become_follower()
        
    def client_add_entry(self, value):
        # Append a new entry to the log (from clients)
        assert self.state == 'LEADER'
        self.log.append_entries(
            len(self.log)-1,
            self.log[-1].term if self.log else -1,
            [ Entry(self.current_term, value) ]
            )

    def become_follower(self):
        self.state = "FOLLOWER"
        print(self.control.address, "became follower")

    def become_candidate(self):
        self.state = "CANDIDATE"
        self.current_term += 1
        self.voted_for = self.control.address
        self.votes_granted = 0    # How many votes for myself
        last_log_index = len(self.log) - 1
        last_log_term = self.log[-1].term if self.log else -1 
        
        # Send a requestvote to all peers
        for n in self.control.peers:
            self.control.send_message(
                RequestVote(
                    self.control.address,
                    n,
                    self.current_term,
                    last_log_index,
                    last_log_term
                )
            )
        print(self.control.address, "became candidate")

    def become_leader(self):
        self.state = "LEADER"
        # Upon becoming leader, we have to start tracking information about the followers.
        # However, we know nothing.  So, start off by assuming that all followers are
        # as up-to-date as us
        self.next_index = { n: len(self.log) for n in self.control.peers }
        self.match_index = { n: -1 for n in self.control.peers }

        # Upon becoming leader, immediately send empty AppendEntries
        self.send_append_entries()
        print(self.control.address, "became leader")

    def handle_message(self, msg):
        # Received any kind of message
        # print("handle_message", msg)
        # If we get a message with a lower term than us, ignore? Stale.
        if msg.term < self.current_term:
            return

        # If we ever get a message with a term higher than us, we immediately become a follower no matter what
        if msg.term > self.current_term:
            self.current_term = msg.term
            self.voted_for = None
            self.become_follower()
        
        if isinstance(msg, AppendEntries):
            self.handle_AppendEntries(msg)
        elif isinstance(msg, AppendEntriesResponse):
            self.handle_AppendEntriesResponse(msg)
        elif isinstance(msg, RequestVote):
            self.handle_RequestVote(msg)
        elif isinstance(msg, RequestVoteResponse):
            self.handle_RequestVoteResponse(msg)
        else:
            raise RuntimeError(f"Bad Message {msg}")

    def handle_election_timeout(self):
        # Call an election
        if self.state == 'LEADER':
            return 
            
        self.become_candidate()

    def send_append_entries_one(self, n):
        prev_log_index = self.next_index[n] - 1
        # print("prev_log_index", prev_log_index)
        prev_log_term = self.log[prev_log_index].term if prev_log_index >= 0 else -1

        self.control.send_message(
                AppendEntries(
                    self.control.address,
                    n,
                    self.current_term,
                    prev_log_index,
                    prev_log_term,
                    self.log[self.next_index[n]:],
                    self.commit_index
                )
            )

    def send_append_entries(self):
        # This method tells the leader to send AppendEntries messages to all followers
        if self.state != "LEADER":
            return

        # Send AppendEntries to all followers
        for n in self.control.peers:
            self.send_append_entries_one(n)


    def handle_AppendEntries(self, msg):
        assert self.state != 'LEADER'
        self.control.reset_election_timeout()

        if self.state == 'CANDIDATE':
            self.become_follower()
        
        success = self.log.append_entries(msg.prev_log_index, msg.prev_log_term, msg.entries)
        self.control.send_message(
            AppendEntriesResponse(
                msg.dest,
                msg.source,
                self.current_term,
                success,
                msg.prev_log_index + len(msg.entries) if success else -1
                )
        )
        if success and msg.leader_commit > self.commit_index:
            self.commit_index = min(msg.leader_commit, len(self.log) -1)
            # Must apply to client state machine (possibly)
            
    def handle_AppendEntriesResponse(self, msg):
        if self.state != 'LEADER':
            return
        if msg.success:
            # It worked!
            self.match_index[msg.source] = msg.match_index
            self.next_index[msg.source] = msg.match_index + 1

            # Check for consensus here
            committed = sorted(self.match_index.values())[len(self.match_index)//2]   # The "median"
            if committed > self.commit_index and self.log[committed].term == self.current_term:
                self.commit_index = committed
            # Might need to apply state machine

        else:
            # It failed! Must retry by backing the log index down by one
            self.next_index[msg.source] -= 1
            self.send_append_entries_one(msg.source)

    def handle_RequestVote(self, msg):
        success = ((self.voted_for is None)                        # Can't have previously voted
                   and (msg.last_log_index >= len(self.log) - 1)   # Candidate must have at least as many log entries as me
                   and (not self.log                               # My log could be empty (grant vote)
                          or (msg.last_log_term > self.log[msg.last_log_index].term)   # Message has greater term in last index
                          or (msg.last_log_term == self.log[msg.last_log_index].term   # Message has greater log length in same term
                               and msg.last_log_index >= len(self.log) - 1)
                   )
        )
        if success:
            self.voted_for = msg.source

        self.control.send_message(
            RequestVoteResponse(
                msg.dest,
                msg.source,
                self.current_term,
                success
            )
        )

    
    def handle_RequestVoteResponse(self, msg):
        if self.state != "CANDIDATE":
            return

        if msg.vote_granted:
            self.votes_granted += 1
            if self.votes_granted >= (len(self.control.peers)//2):
                self.become_leader()
        
# -----------------------  TESTING 

class MockController:
    def __init__(self, address, peers):
        self.address = address
        self.peers = peers
        self.messages = []
        self.machine = RaftMachine(self)

    def clear(self):
        self.messages = []

    def send_message(self, msg):
        self.messages.append(msg)

def test_machine():
    c = MockController(0, [1,2,3,4])

    # All machines should start in the follower state
    assert c.machine.state == 'FOLLOWER'

    # Some basic operational features

    # If any message is received with a higher term, the machine reverts to
    # to follower state and updates its current term to the new term
    c.machine.state == 'LEADER'
    c.machine.handle_message(
        AppendEntriesResponse(
            0,
            1,
            2,
            True,
            0
        )
    )
    assert c.machine.state == 'FOLLOWER'
    assert c.machine.current_term == 2

    # Upon promotion to candidate, empty AppendEntries messages should be sent to followers
    c.clear()
    c.machine.become_leader()
    assert c.machine.state == 'LEADER'
    assert len(c.messages) == 4
    assert { m.dest for m in c.messages } == { 1, 2, 3, 4}
    assert all(m.entries == [] for m in c.messages) 

    # Figure 7. a-f.  This tries to test what happens to each follower
    c.clear()
    scenarios = {
        'a': [1, 1, 1, 4, 4, 5, 5, 6, 6],
        'b': [1, 1, 1, 4],
        'c': [1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6],
        'd': [1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7],
        'e': [1, 1, 1, 4, 4, 4, 4, 4],
        'f': [1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3]
    }
    scenarios = { k: [ Entry(t, 0) for t in v ] for k, v in scenarios.items() }
    
    def run_scenario(control, name, entries=[]):
        control.clear()
        control.machine.state = "FOLLOWER"
        control.machine.current_term = 7
        control.machine.commit_index = 0
        control.machine.log.append_entries(-1,-1, scenarios[name])
        msg = AppendEntries(1, 0, 8, 9, 6, entries, 9)
        control.machine.handle_message(msg)

    run_scenario(c, 'a')
    assert not c.messages[0].success                # Log is too short (missing an entry)
    assert c.machine.log.entries == scenarios['a']  # Log unchanged

    run_scenario(c, 'b')
    assert not c.messages[0].success                # Log too short
    assert c.machine.log.entries == scenarios['b']

    run_scenario(c, 'c')
    assert c.messages[0].success        # Log has all committed entries
    assert c.machine.log.entries == scenarios['c']
    assert c.machine.commit_index == 9   # Should reflect the leader

    run_scenario(c, 'c', [Entry(8, 0)])
    assert c.messages[0].success
    assert len(c.machine.log) == 11, len(c.machine.log)     # Log should be truncated to leader length
    assert c.machine.log[10] == Entry(8, 0)
    assert c.machine.commit_index == 9

    run_scenario(c, 'd')
    assert c.messages[0].success
    assert c.machine.log.entries == scenarios['d']
    
    run_scenario(c, 'd', [Entry(8, 0)])
    assert c.messages[0].success
    assert len(c.machine.log) == 11, len(c.machine.log)     # Log should be truncated to leader length
    assert c.machine.log[10] == Entry(8, 0)

    run_scenario(c, 'e')
    assert not c.messages[0].success

    run_scenario(c, 'f')
    assert not c.messages[0].success

    # ----------- Tests of consensus





if __name__ == '__main__':
    test_machine()




