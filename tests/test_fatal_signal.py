#################################################################
#                                                               #
# Copyright (c) 2021-2025 YottaDB LLC and/or its subsidiaries.  #
# All rights reserved.                                          #
#                                                               #
#   This source code contains the intellectual property         #
#   of its copyright holder(s), and is made available           #
#   under a license.  If you do not know the terms of           #
#   the license, please stop and do not read further.           #
#                                                               #
#################################################################
import os
import psutil
import time
import random
import signal
from multiprocessing import Process, Event
from test_threeenp1 import test_threeenp1


def test_fatal_signal():
    ready_event = Event()

    # Randomly assign a value(SIGTERM or SIGINT) to kill_signal
    # and the respective exit code value(241 or 254) to exit_code
    if random.randrange(2):
        kill_signal = signal.SIGINT
        exit_code = 254
    else:
        kill_signal = signal.SIGTERM
        exit_code = 241

    # Start test_threeenp1.py as a child process
    p = Process(target=test_threeenp1, args=(True, exit_code, ready_event))
    p.start()

    # Wait for process to spawn threads before killing it below
    ready_event.wait()

    # Get parent PID, parent process and all child processes
    parent_pid = os.getpid()
    parent = psutil.Process(parent_pid)
    children = parent.children(recursive=True)

    # Send the kill signal to all child processes
    for child in children:
        child.send_signal(kill_signal)

    # Wait for process to complete
    p.join()

    # The exit codes for the child processes are checked in
    # test_threenp1.py
    assert p.exitcode == exit_code

    # Run integrity check on database
    integ_check = os.system('mupip integ -reg "*" > tmp.mupip 2>&1 &')
    if integ_check:
        print("Database Integrity compromised")
    else:
        print("Database Integrity maintained")
