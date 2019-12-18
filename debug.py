# debug.py
import logging
logging.basicConfig(level=logging.INFO)

from raft.net import RaftNet

nets = [ RaftNet(n) for n in range(5)]
for n in nets:
    n.start()
