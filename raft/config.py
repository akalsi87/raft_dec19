# config.py
#
# This file contains the default configuration of various settings. They're
# hard-coded here.  Eventually, you'd probably want something better.


# Connection endpoints for the Raft servers.  Used by servers to find each other.
SERVERS = {
    0: ('localhost', 15000),
    1: ('localhost', 15001),
    2: ('localhost', 15002),
    3: ('localhost', 15003),
    4: ('localhost', 15004)
    }

LEADER_HEARTBEAT = 1
ELECTION_TIMEOUT = 4
ELECTION_TIMEOUT_SPREAD = 1
