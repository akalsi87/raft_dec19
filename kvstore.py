# kvstore.py

from socket import *
import threading
import time
import pickle

import logging                    

from raft.control import RaftControl
from raft.net import RaftNet
from raft import msgpass

import kvconfig

class KVStore:
    def __init__(self, address):
        self.address = address 
        self.control = RaftControl(RaftNet(self.address), self.apply_operation)
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value):
        self.data[key] = value

    def delete(self, key):
        if key in self.data:
            del self.data[key]

    def apply_operation(self, value):
        name, args = value
        getattr(self, name)(*args)

    def start(self):
        self.control.start()
        threading.Thread(target=self.run_client_server, daemon=True).start()

    def run_client_server(self):
        sock = socket(AF_INET, SOCK_STREAM)
        sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
        sock.bind(kvconfig.KVSERVER[self.address])
        sock.listen(1)
        print("KV Server running for node", self.address)
        while True:
            client, addr = sock.accept()
            print("KV Client from", addr)
            threading.Thread(target=self.client_handler, args=(client,), daemon=True).start()

    def client_handler(self, sock):
        try:
            if not self.control.machine.state == 'LEADER':
                sock.close()
                return

            msgpass.send_message(sock, b'ok')
            while True:
                msg = pickle.loads(msgpass.recv_message(sock))
                if not self.control.machine.state == 'LEADER':
                    sock.close()
                    return

                name, *args = msg
                if name == 'get':
                    result = self.get(*args)
                elif name == 'set':
                    result = self.control.client_add_entry(('set', args))
                elif name == 'delete':
                    result = self.control.client_add_entry(('delete', args))
                else:
                    result = "error: bad request"
                msgpass.send_message(sock, pickle.dumps(result))
        except IOError:
            print("Connection closed")

    
if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python kvstore.py nodenum")
    nodenum = int(sys.argv[1])

    logging.basicConfig(level=logging.DEBUG,
                    filename=f"{nodenum}.log",
                    filemode="w")
                        
    kv = KVStore(nodenum)
    kv.start()
    while True:
        time.sleep(1)


    