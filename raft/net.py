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

from . import config
from . import msgpass

class RaftNet:
    def __init__(self, address):
        self.address = address
        self._inbox = queue.Queue()
        self._socks = { }
        self._debuglog = logging.getLogger(f'{self.address}.net')

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

    def send(self, dest, msg):
        if dest not in self._socks:
            try:
                sock = socket(AF_INET, SOCK_STREAM)
                sock.connect(config.SERVERS[dest])
            except IOError as e:
                self._debuglog.info("Connection to %d failed", dest, exc_info=True)
                return
            self._socks[dest] = sock
        
        try:
            msgpass.send_message(self._socks[dest], pickle.dumps(msg))
        except IOError as e:
            self._debuglog.info("Send %r to %d failed", msg, dest, exc_info=True)
            self._socks[dest].close()
            del self._socks[dest]

    def recv(self):
        return self._inbox.get()


    
