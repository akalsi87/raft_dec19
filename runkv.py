# runkv.py

import subprocess
import time
import atexit

procs = { }

def start(n):
    procs[n] = subprocess.Popen(['python', 'kvstore.py', str(n)])

def stop(n):
    procs[n].terminate()

for n in range(5):
    start(n)

atexit.register(lambda: [stop(n) for n in range(5)])
        
