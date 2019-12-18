# state.py
#
# Implementation of the Raft state machine

class RaftMachine:
    def __init__(self, control):
        self.state = None
        self.control = control

    def become_follower(self):
        self.state = "FOLLOWER"

    def become_candidate(self):
        self.state = "CANDIDATE"

    def become_leader(self):
        self.state = "LEADER"

    def handle_message(self, msg):
        # Received any kind of message
        pass
    
    def handle_election_timeout(self, msg):
        # Call an election
        pass

    def send_append_entries(self):
        assert self.state == 'LEADER'
        # Send AppendEntries to all followers


