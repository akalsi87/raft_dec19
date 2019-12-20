# kvclient.py

import kvconfig
from socket import *
from raft import msgpass
import time
import pickle


class KVClient:
    def __init__(self):
        self.sock = None

    def _connect(self):
        while not self.sock:
            for addr in kvconfig.KVSERVER.values():
                try:
                    sock = socket(AF_INET, SOCK_STREAM)
                    sock.connect(addr)
                    msg = msgpass.recv_message(sock)
                    if msg == b'ok':
                        self.sock = sock
                        return
                except IOError as e:
                    pass
            print("Can't connect. Retrying")
            time.sleep(1)
    
    def get(self, key):
        while True:
            try:
                self._connect()
                msgpass.send_message(self.sock, pickle.dumps(('get', key)))
                return pickle.loads(msgpass.recv_message(self.sock))
            except IOError:
                self.sock.close()
                self.sock = None

    def set(self, key, value):
        while True:
            try:
                self._connect()
                msgpass.send_message(self.sock, pickle.dumps(('set', key, value)))
                resp = pickle.loads(msgpass.recv_message(self.sock))
                if resp == 'ok':
                    return resp
                else:
                    self.sock.close()
                    self.sock = None
            except IOError:
                self.sock.close()
                self.sock = None

    def delete(self, key):
        while True:
            try:
                self._connect()
                msgpass.send_message(self.sock, pickle.dumps(('delete', key)))
                resp =  pickle.loads(msgpass.recv_message(self.sock))
                if resp == 'ok':
                    return resp
                else:
                    self.sock.close()
                    self.sock = None
            except IOError:
                self.sock.close()
                self.sock = None

if __name__ == '__main__':
    kv = KVClient()




