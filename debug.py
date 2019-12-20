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
# control[0].machine.become_leader()
# control[0].client_add_entry("spam")

# import time
# time.sleep(2)
# for c in control:
#     print(c.machine.log)

def show_logs():
    for n, c in enumerate(control):
        print(f'{n}:{c.machine.state:<10s}:{c.machine.log}')


def block(server, *nodes):
    control[server].net.block(*nodes)
    for n in nodes:
        control[n].net.block(server)

def unblock(server, *nodes):
    control[server].net.unblock(*nodes)
    for n in nodes:
        control[n].net.unblock(server)

