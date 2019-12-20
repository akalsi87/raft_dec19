# control.py
#
# Implementation of the Raft state machine controller.  All system-level
# functions such as networking, persistence, and other details get implemented
# here.

import queue
import logging
import threading
import time
import random
from concurrent.futures import Future

from .machine import RaftMachine
from . import config

class RaftControl:
    def __init__(self, net, application_callback=lambda v: print(f'applying: {v}')):
        self.net = net
        self.address = net.address
        self.peers = [n for n in config.SERVERS if n != self.address]
        self.application_callback = application_callback
        self.machine = RaftMachine(self)
        self._events = queue.Queue()
        self._debuglog = logging.getLogger(f'{self.net.address}.control')

    def send_message(self, msg):
        self.net.send(msg.dest, msg)

    def apply_state_machine(self, value):
        self.application_callback(value)

    def start(self):
        self._stopped = False

        # Start the networking layer
        self.net.start()

        # Launch the event loop
        threading.Thread(target=self._event_loop, daemon=True).start()

        # Launch the message receiver
        threading.Thread(target=self._message_receiver, daemon=True).start()
        # Launch the heartbeat timer
        threading.Thread(target=self._heartbeat_timer, daemon=True).start()
        # Launch the election timer
        threading.Thread(target=self._election_timer, daemon=True).start()

    def _message_receiver(self):
        while True:
            msg = self.net.recv()
            self._events.put(('message', msg))

    def _heartbeat_timer(self):
        while True:
            time.sleep(config.LEADER_HEARTBEAT)
            self._events.put(('heartbeat',))

    def _election_timer(self):
        while True:
            self._reset_election = False
            timeout = config.ELECTION_TIMEOUT + (random.random() - 0.5)*config.ELECTION_TIMEOUT_SPREAD            
            time.sleep(timeout)
            if not self._reset_election:
                self._events.put(('election_timeout',))
            
    def reset_election_timeout(self):
        self._reset_election = True
        
    def client_add_entry(self, value):
        fut = Future()
        self._events.put(('addentry', value, fut))
        return fut.result()

    def _event_loop(self):
        while True:
            evt, *args = self._events.get()
            if self._stopped:
                continue

            self._debuglog.debug("Event %r %r", evt, args)

            # Process the event on the underlying machine.  This is the only place where it is safe
            # to do things on the underlying raft state machine.  Basically, all operations occur
            # within a single thread.   There's no chance of concurrent operation. 
            if evt == 'message':
                self.machine.handle_message(*args)
            elif evt == 'election_timeout':
                self.machine.handle_election_timeout()
            elif evt == 'heartbeat':
                self.machine.send_append_entries()
            elif evt == 'addentry':
                if self.machine.state == 'LEADER':
                    self.machine.client_add_entry(args[0])
                    args[1].set_result('ok')   
                else:
                    args[1].set_result("error")
            else:
                raise RuntimeError("Unknown event")


    def suspend(self):
        self._stopped = True

    def resume(self):
        self._stopped = False

    def append_entry(self, entry):
        # Fake client function
        ...


    