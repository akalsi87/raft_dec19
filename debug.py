# debug.py
#
# Run this program as python -i debug.py.  It creates various variables representing 
# a full cluster of Raft servers.  Interact via the created controllers.

import logging
logging.basicConfig(level=logging.DEBUG,
                    filename="raft.log",
                    filemode="w")

from raft.net import RaftNet
from raft.control import RaftControl
import raft.config

control = [RaftControl(RaftNet(n)) for n in raft.config.SERVERS]
for c in control:
    c.start()

# Example of replication
control[0].machine.become_leader()
control[0].client_add_entry("spam")

import time
time.sleep(2)
for c in control:
    print(c.machine.log)

