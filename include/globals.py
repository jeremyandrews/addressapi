'''
  Global variables used across all scripts.
'''
import time


def init():
    global start
    global args
    global debug
    global requests
    global notify
    # Track a timestamp of when the script was started.
    start = time.time()
    # Make command line arguments available to all scripts and utilities.
    args = None
    debug = []
    # Track how many requests this thread processes.
    requests = 1
    notify = {}
