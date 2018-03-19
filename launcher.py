#!/usr/bin/python

from __future__ import print_function

import os
import time
from multiprocessing import Pipe, cpu_count

# Local imports
print('Loading axr... (why does this take so long!?)')
from axr import AXRSession
print('Loading coordinate DB, this will take a long time...')
from search import get_coords

def threemer_range():
    """ Generator returns the range of all valid 3-residue
        amino acid sequences. """

    valid_letters = 'ARNDCEQZGHILKMFPSTWYV'

    for first in valid_letters:
        for second in valid_letters:
            for third in valid_letters:
                yield first + second + third


# Get ready for fork
# Overprovision due to the delay in waiting for the API...
num_threads = cpu_count() * 2
newpid = None
processes = []

print('Forking processes...')
# Fork a number of threads based on the number of CPUs available
for p in xrange(0, num_threads):

    # Set up the pipes
    parent_conn, child_conn = Pipe()
    # Start the process
    processes.append(parent_conn)

    # Fork
    newpid = os.fork()

    # Okay, we are the child
    if newpid == 0:

        # We are low priority
        os.nice(19)

        # Fire up an API connection
        with AXRSession() as api:

            # Loop until our director tells us to stop
            while True:
                # Tell our parent we are ready to render
                child_conn.send("ready")
                next_pair = child_conn.recv()

                result = 'data'

                # TODO: Hesam's code goes here -
                # result = do_all_the_calculations(search_method=get_coords)

                # Store the results
                res = api.store(next_pair[0] + next_pair[1], result)

                # See if we're done
                if next_pair == 'stop':
                    break
                print("In thread %s doing task %s: %s" % (p, next_pair, res))

        print("Child %s shutting down..." % p)
        # Close the socket to our parent and then exit
        child_conn.close()
        parent_conn.close()
        os._exit(0)
    else:
        child_conn.close()


def send_to_next_free_worker(job):
    """ Waits for a worker to be free and then sends it a job. """

    # Wait for a free worker, send them the job
    while True:
        for x in range(0, num_threads):
            if processes[x].poll():
                # Read the "ready" message
                processes[x].recv()
                # Send the job
                processes[x].send(job)
                return
        time.sleep(.01)

# Keep track of state
first_threemer = 'AAA'
second_threemer_gen = threemer_range()

# Send all the jobs for processing
for second_threemer in second_threemer_gen:
    send_to_next_free_worker((first_threemer, second_threemer))

# Reap the children
for child in xrange(0, num_threads):
    processes[child].recv()
    processes[child].send('stop')
    res = os.wait()

print("We're done!")
