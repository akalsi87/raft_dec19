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

class RaftMachine:
    def __init__(self, control):
        self.state = None
        self.control = control

        # Persistent state 
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

    def become_candidate(self):
        self.state = "CANDIDATE"

    def become_leader(self):
        self.state = "LEADER"
        # Upon becoming leader, we have to start tracking information about the followers.
        # However, we know nothing.  So, start off by assuming that all followers are
        # as up-to-date as us
        self.next_index = { n: len(self.log) for n in self.control.peers }
        self.match_index = { n: -1 for n in self.control.peers }

        # Upon becoming leader, immediately send empty AppendEntries
        self.send_append_entries()

    def handle_message(self, msg):
        # Received any kind of message
        # print("handle_message", msg)
        # If we get a message with a lower term than us, ignore? Stale.
        if msg.term < self.current_term:
            return

        # If we ever get a message with a term higher than us, we immediately become a follower no matter what
        if msg.term > self.current_term:
            self.current_term = msg.term
            self.become_follower()
        
        if isinstance(msg, AppendEntries):
            self.handle_AppendEntries(msg)
        elif isinstance(msg, AppendEntriesResponse):
            self.handle_AppendEntriesResponse(msg)
        else:
            raise RuntimeError(f"Bad Message {msg}")

    def handle_election_timeout(self, msg):
        # Call an election
        print("election_timeout")

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

    def handle_AppendEntriesResponse(self, msg):
        if self.state != 'LEADER':
            return
        if msg.success:
            # It worked!
            self.match_index[msg.source] = msg.match_index
            self.next_index[msg.source] = msg.match_index + 1
        else:
            # It failed! Must retry by backing the log index down by one
            self.next_index[msg.source] -= 1
            self.send_append_entries_one(msg.source)
