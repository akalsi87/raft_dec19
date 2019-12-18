# msgpass.py
#
# Implements a basic message passing layer using size-prefixed messages.

def recv_exactly(sock, nbytes):
    '''
    Receive exactly nbytes of data on a socket
    '''
    parts = []
    while nbytes > 0:
        chunk = sock.recv(nbytes)   # Might return partial data (whatever received so far)
        if not chunk:
            # Connection closed!
            raise IOError("Connection closed")
        parts.append(chunk)
        nbytes -= len(chunk)
    return b''.join(parts)

def send_size(sock, sz: int):
    sock.sendall(sz.to_bytes(8, "big"))

def recv_size(sock):
    msg = recv_exactly(sock, 8)
    return int.from_bytes(msg, "big")

def send_message(sock, msg):
    send_size(sock, len(msg))
    sock.sendall(msg)

def recv_message(sock):
    sz = recv_size(sock)          
    msg = recv_exactly(sock, sz)  
    return msg



