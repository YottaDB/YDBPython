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
import pytest
import datetime
import multiprocessing
import sys

import yottadb
from yottadb import YDB_OK
from conftest import setup_db, teardown_db


MAX_PROCESSES = 32
MAX_VALUE_LEN = 256


class GVNList:
    def __init__(self):
        self.limits = yottadb.Key("^limits")
        self.count = yottadb.Key("^count")
        self.reads = yottadb.Key("^reads")
        self.updates = yottadb.Key("^updates")
        self.highest = yottadb.Key("^highest")
        self.result = yottadb.Key("^result")
        self.step = yottadb.Key("^step")


# The following function unconditionally adds the number of reads & writes
# performed by this thread to the number of reads & writes performed by
# all threads, and sets the highest for all threads if the highest
# calculated by this thread is greater than that calculated so far for all
# threads
def gvstats_incr(reads: int, updates: int, highest: int):
    gvnlist = GVNList()
    gvnlist.reads.incr(reads)
    gvnlist.updates.incr(updates)
    highest_global = gvnlist.highest.get()
    if highest_global is None:
        highest_global = "0"

    if highest > int(highest_global):
        gvnlist.highest.set(highest_global)

    return yottadb.YDB_OK


# Implements M entryref dbinit^threeen1f
def db_init(gvnlist: GVNList):
    # Reset database values between lines
    gvnlist.limits.value = "0"
    gvnlist.count.value = "0"
    gvnlist.reads.value = "0"
    gvnlist.updates.value = "0"
    gvnlist.highest.value = "0"
    gvnlist.result.value = "0"
    gvnlist.step.value = "0"


# Implements M entryref digitsinit^threeen1f
def init_digits():
    digitstrings = ["zero", "eins", "deux", "tres", "quattro", "пять", "ستة", "सात", "捌", "ஒன்பது"]

    digitstring = yottadb.Key("ds")
    digitint = yottadb.Key("di")
    for digit in range(0, 10):
        digitint[digitstrings[digit]].value = str(digit)
        digitstring[str(digit)].value = digitstring[digit]


# Returning YDB_OK is necessary since this is used as a TP callback
def set_maximum(num_steps: int) -> int:
    gvnlist = GVNList()
    result = gvnlist.result.get()
    if result is not None:
        maximum = int(result)
    else:
        maximum = 0

    if num_steps > maximum:
        gvnlist.result.set(str(num_steps))

    return YDB_OK


def do_block(start: int):
    try:
        # No need to invoke "init_digits()" as child thread
        # has access to all local variables of parent thread
        gvnlist = GVNList()

        # Decrement ^count to say this thread is alive
        gvnlist.count.incr(-1)
        # Initialize dictionary for tracking global variable statistics
        gvstats = {"reads": 0, "updates": 0, "highest": 0}

        # Process the next block in ^limits that needs processing; quit when done
        i = 0
        while True:
            i += 1
            if gvnlist.limits[str(i)].data == 0:
                break

            result = int(gvnlist.limits[str(i)][str(1)].incr())
            if result != 1:
                continue

            if gvnlist.limits[str(i - 1)].value is not None:
                first = int(gvnlist.limits[str(i - 1)].value) + 1
            else:
                first = start

            last = int(gvnlist.limits[str(i)].value)
            do_step(first, last, gvstats)

            try:
                yottadb.tp(gvstats_incr, reads=gvstats["reads"], updates=gvstats["updates"], highest=gvstats["highest"])
            except yottadb.YDBTPRestart:
                return

    except KeyboardInterrupt:
        # This is a case where the test sent a SIGINT. Possible if YottaDB drove the Python signal handler which would
        # have registered this as a keyboard interrupt in case of a Ctrl-C in case this test was run from a terminal.
        # In this case, we want the process to terminate with an exit code of -2 since that is what YottaDB signal
        # handler for SIGINT would do (not doing so would cause the test to randomly fail due to a different exit code).
        # Hence the below logic.
        sys.exit(-2)


# Implements M entryref dostep^threeen1f
def do_step(first: int, last: int, gvstats: dict):
    gvnlist = GVNList()
    # Make each thread running "do_step" operate on a different local
    # variable name by appending the process id to "cur_path"
    curpath = yottadb.Key("curpath{}".format(os.getpid()))
    for current in range(first, last + 1):
        number = current

        # curpath holds path to 1 for current
        curpath.delete_tree()

        # Go till we reach 1 or a number with a known number of steps
        num_steps = 0
        while True:
            gvstats["reads"] += 1
            if number == 1:
                break

            if gvnlist.step[str(number)].data != 0:
                break

            # Log `number` as current number in sequence
            curpath[str(num_steps)].value = str(number)

            # Compute the next number
            if 0 == (number % 2):
                number /= 2
            else:
                number = (3 * number) + 1

            # See if we have a new highest number reached
            if number > gvstats["highest"]:
                gvstats["highest"] = number

            num_steps += 1

        # If 0 == num_steps we already have an answer for `number`,
        # so nothing to do here
        if 0 < num_steps:
            if 1 < number:
                num_steps = num_steps + int(gvnlist.step[str(number)].value)

            # Atomically set maximum
            try:
                yottadb.tp(set_maximum, args=(num_steps,))
            except yottadb.YDBTPRestart:
                return

            for path in curpath[""]:
                path_number = int(path.name)
                gvstats["updates"] += 1
                gvnlist.step[curpath[path.name]].value = str(num_steps - path_number)
            curpath.delete_tree()


# Find the maximum number of steps for the 3n+1 problem for all integers
# through two input integers.
#  See http://docs.google.com/View?id=dd5f3337_24gcvprmcw
#
# Assumes input format is 3 integers separated by a space with the first
# integer smaller than the second.
#
# The third integer is the number of parallel computation streams. If it is
# less than twice the number of CPUs or cores, the parameter is modified to
# that value. An optional fourth integer is the sizes of blocks of integers
# on which spawned child processes operate. If it is not specified, the
# block size is approximately the range divided by the number of parallel
# streams. If the block size is larger than the range divided by the number
# of execution streams, it is reduced to that value. No input error checking
# is done.
#
# Although the problem can be solved by using strictly integer subscripts and
# values, this program is written to show that the YottaDB key-value store can
# use arbitrary strings for both keys and values - each subscript and value is
# spelled out using the strings in the program source line labelled "digits".
# Furthermore, the strings are in a number of international languages when
# YottaDB is run in UTF-8 mode.
#
# The arguments fatal_signal_flag and exit_code are passed with default values
# and are required for a different test case (test_fatal_signal.py).
# These arguments are needed to check the exit codes of the do_block processes,
# when test_threeenp1 is invoked by test_fatal_signal.
def test_threeenp1(fatal_signal_flag=False, exit_code=0, ready_event=None):
    try:
        db = setup_db()
        # Initialize global variables and store in an object for
        # easy argument passing
        gvnlist = GVNList()
        db_init(gvnlist)  # Initialize database for next run

        # Get number of available CPUs, will use all available CPUs below. Syntax:
        # https://docs.python.org/3/library/multiprocessing.html#multiprocessing.cpu_count
        streams = len(os.sched_getaffinity(0))  # Number of parallel streams
        assert streams != 0

        # Specify parameters for threeenp1 problem based on available CPUs
        #
        # Note that this logic is derived from v62002/u_inref/gtm6638.csh in the
        # YDBTest repository. It is moved here to avoid reliance on stdin, which is
        # captured by pytest by default, and simplify test configuration.
        start = 1  # Starting integer for the problem
        if 4 < streams:
            end = 30000  # Ending/upper bound integer for the problem
            expected = 307
        else:
            end = 2500  # Ending/upper bound integer for the problem
            expected = 208
        block_size = 100  # Size of blocks of integer
        with open("3nparms.out", "w") as output_file:
            output_file.write(f"upperbound={end} expected={expected}\n")

        with open("threeenp1.out", "w") as output_file:
            # Print starting and ending integers and number of execution streams
            output_file.write(f"{start} {end} ({streams})")
            assert streams <= MAX_PROCESSES

            i = (end - start + streams) / streams  # default / maximum block size
            if block_size is not None and block_size <= i:
                output_file.write(f"{block_size}")  # print block size, optionally corrected
            else:
                output_file.write(f" ({block_size}->{i})")  # print number of execution streams, optionally corrected
                block_size = i

            # Define blocks of integers for child processes to work on
            gvnlist.limits.delete_tree()
            block = 1
            i = start - 1
            while i != end:
                i = i + block_size
                if i > end:
                    i = end
                gvnlist.limits[str(block)].value = str(i)
                block += 1

            # Launch threads
            gvnlist.count.value = str(0)  # Clear ^count - may have residual value if restarting from crash
            processes = []
            for _ in range(0, streams):
                gvnlist.count.incr()
                process = multiprocessing.Process(target=do_block, args=(start,))
                process.start()
                processes.append(process)

            start_time = datetime.datetime.now()  # Get starting time
            if ready_event is not None:
                # Signal to the caller to proceed
                ready_event.set()
            # Wait for threads to finish
            for process in processes:
                process.join()
                if fatal_signal_flag:
                    assert process.exitcode == exit_code
            end_time = datetime.datetime.now()  # Get ending time

            # Time between start_time and end_time is the elapsed time
            duration = start_time - end_time

            output_file.write(
                f" {int(gvnlist.result.value)} {int(gvnlist.highest.value)}"
                f" {duration.total_seconds()} {int(gvnlist.updates.value)}"
                f" {int(gvnlist.reads.value)}"
            )

            # If duration is greater than 0 seconds, display update and read rates
            if duration != 0:
                update_rate = int(gvnlist.updates.value) / duration.total_seconds()
                read_rate = int(gvnlist.reads.value) / duration.total_seconds()
                output_file.write(f"{update_rate} {read_rate}\n")

        assert expected == int(gvnlist.result.value)
        # Cleanup database for other tests
        gvnlist.limits.delete_tree()
        gvnlist.count.delete_tree()
        gvnlist.reads.delete_tree()
        gvnlist.updates.delete_tree()
        gvnlist.highest.delete_tree()
        gvnlist.result.delete_tree()
        gvnlist.step.delete_tree()
        teardown_db(db)
    except KeyboardInterrupt:
        # See comment before similar code block in tests/test_threeenp1.py for why the -2 is needed below.
        sys.exit(-2)
