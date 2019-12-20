# net.py
#
# Implementation of a networking layer for Raft servers.  Essentially
# we try to make it easy to send/recv messages between servers while 
# hiding implementation details concerning sockets and whatnot.

import queue
import threading
from socket import *
import pickle
import logging
from collections import defaultdict

from . import config
from . import msgpass

class RaftNet:
    def __init__(self, address):
        self.address = address
        self._inbox = queue.Queue()
        self._outbound = defaultdict(queue.Queue)
        self._socks = { }
        self._debuglog = logging.getLogger(f'{self.address}.net')
        self._blocked = set()


    def start(self):
        self._debuglog.info("Network starting")
        threading.Thread(target=self._receiver_server, daemon=True).start()

    def _receiver_server(self):
        sock = socket(AF_INET, SOCK_STREAM)
        sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
        sock.bind(config.SERVERS[self.address])
        sock.listen(1)
        self._debuglog.info("Receiver listening")
        while True:
            client, addr = sock.accept()
            self._debuglog.info("Connection from %r", addr)
            threading.Thread(target=self._receiver, args=(client,), daemon=True).start()
    
    def _receiver(self, client):
        with client:
            while True:
                data = msgpass.recv_message(client)
                msg = pickle.loads(data)
                self._debuglog.debug("Received %r", msg)
                self._inbox.put(msg)

    def _sender(self, dest):
        # Thread responsible for sending outbound messages to a given destination
        while True:
            msg = self._outbound[dest].get()
            if dest not in self._socks:
                try:
                    sock = socket(AF_INET, SOCK_STREAM)
                    sock.connect(config.SERVERS[dest])
                except IOError as e:
                    self._debuglog.info("Connection to %d failed", dest, exc_info=True)
                    continue
                self._socks[dest] = sock
        
            try:
                msgpass.send_message(self._socks[dest], pickle.dumps(msg))
            except IOError as e:
                self._debuglog.info("Send %r to %d failed", msg, dest, exc_info=True)
                self._socks[dest].close()
                del self._socks[dest]

    def send(self, dest, msg):
        if dest in self._blocked:
            return
        self._debuglog.debug("Sent %r", msg)
        if dest not in self._outbound:
            threading.Thread(target=self._sender, args=(dest,), daemon=True).start()
    
        self._outbound[dest].put(msg)

    def recv(self):
        return self._inbox.get()


    def block(self, *nodes):
        self._blocked.update(nodes)

    def unblock(self, *nodes):
        for node in nodes:
            self._blocked.discard(node)


