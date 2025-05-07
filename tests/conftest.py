#################################################################
#                                                               #
# Copyright (c) 2019-2021 Peter Goss All rights reserved.       #
#                                                               #
# Copyright (c) 2019-2025 YottaDB LLC and/or its subsidiaries.  #
# All rights reserved.                                          #
#                                                               #
#   This source code contains the intellectual property         #
#   of its copyright holder(s), and is made available           #
#   under a license.  If you do not know the terms of           #
#   the license, please stop and do not read further.           #
#                                                               #
#################################################################
import os
import shutil
import subprocess
import multiprocessing
import shlex
import time
import sys
import signal
import random
import pathlib
import pytest  # type: ignore # ignore due to pytest not having type annotations
from typing import Union

import yottadb


YDB_DIST = os.environ["ydb_dist"]
TEST_DATA_DIRECTORY = "/tmp/test_yottadb/"
CUR_DIR = os.getcwd()

SIMPLE_DATA = (
    (("^Test5", ()), "test5value"),
    (("^test1", ()), "test1value"),
    (("^test2", ("sub1",)), "test2value"),
    (("^test3", ()), "test3value1"),
    (("^test3", ("sub1",)), "test3value2"),
    (("^test3", ("sub1", "sub2")), "test3value3"),
    (("^test4", ()), "test4"),
    (("^test4", ("sub1",)), "test4sub1"),
    (("^test4", ("sub1", "subsub1")), "test4sub1subsub1"),
    (("^test4", ("sub1", "subsub2")), "test4sub1subsub2"),
    (("^test4", ("sub1", "subsub3")), "test4sub1subsub3"),
    (("^test4", ("sub2",)), "test4sub2"),
    (("^test4", ("sub2", "subsub1")), "test4sub2subsub1"),
    (("^test4", ("sub2", "subsub2")), "test4sub2subsub2"),
    (("^test4", ("sub2", "subsub3")), "test4sub2subsub3"),
    (("^test4", ("sub3",)), "test4sub3"),
    (("^test4", ("sub3", "subsub1")), "test4sub3subsub1"),
    (("^test4", ("sub3", "subsub2")), "test4sub3subsub2"),
    (("^test4", ("sub3", "subsub3")), "test4sub3subsub3"),
    (("^test6", ("sub6", "subsub6")), "test6value"),
    (("^test7", (b"sub1\x80",)), "test7value"),  # Test subscripts with non-UTF-8 data
    (("^test7", (b"sub2\x80", "sub7")), "test7sub2value"),
    (("^test7", (b"sub3\x80", "sub7")), "test7sub3value"),
    (("^test7", (b"sub4\x80", "sub7")), "test7sub4value"),
)


str2zwr_tests = [
    (b"X\0ABC", b'"X"_$C(0)_"ABC"', b'"X"_$C(0)_"ABC"'),
    (
        bytes("你好世界", encoding="utf-8"),
        b'"\xe4\xbd\xa0\xe5\xa5\xbd\xe4\xb8"_$C(150)_"\xe7"_$C(149,140)',
        b'"\xe4\xbd\xa0\xe5\xa5\xbd\xe4\xb8\x96\xe7\x95\x8c"',
    ),
]


# Set ydb_ci and ydb_routines for testing and return a dictionary
# containing the previous values of these environment variables
# to facilitate resetting the environment when testing is complete
def set_ci_environment(cur_dir: str, ydb_ci: str, prefix: bool = False) -> dict:
    previous = {}
    previous["calltab"] = os.environ.get("ydb_ci")
    previous["routines"] = os.environ.get("ydb_routines")
    os.environ["ydb_ci"] = ydb_ci
    if prefix:
        os.environ["ydb_routines"] = cur_dir + "/tests/m_routines " + os.environ["ydb_routines"]
    else:
        os.environ["ydb_routines"] = cur_dir + "/tests/m_routines"

    return previous


# Reset ydb_ci and ydb_routines to the previous values specified
# by the "calltab" and "routines" members of a dictionary, respectively
def reset_ci_environment(previous: dict):
    # Reset environment
    if previous["calltab"] is not None:
        os.environ["ydb_ci"] = previous["calltab"]
    else:
        os.environ["ydb_ci"] = ""
    if previous["routines"] is not None:
        os.environ["ydb_routines"] = previous["routines"]
    else:
        os.environ["ydb_routines"] = ""


# Lock a value in the database
def lock_value(key: Union[yottadb.Key, tuple], ppid: int, ready_event, timeout: int = 1):
    interrupt_event = multiprocessing.Event()

    # Create a handler to release the held lock and clean up the process
    # upon the reception of a signal from the parent process
    def handle_SIGINT(sig_num, stackframe):
        print("# SIGINT received")
        yottadb.lock_decr(varname, subsarray)
        print("# Lock Released")
        sys.exit(0)

    # Extract the local or global variable name and subscripts from the given key object
    if isinstance(key, yottadb.Key):
        varname = key.varname
        subsarray = key.subsarray
    else:
        varname = key[0]
        subsarray = key[1]
    if len(subsarray) == 0:
        subsarray = None

    # Attempt to lock the LVN or GVN using the module-level lock_incr() function
    has_lock = False
    try:
        yottadb.lock_incr(varname, subsarray, timeout_nsec=(timeout * 1_000_000_000))
        print("\n# Lock Success")
        has_lock = True
    except yottadb.YDBLockTimeoutError:
        print("# Lock Failed")
        sys.exit(1)
    except Exception as e:
        print(f"# Lock Error: {repr(e)}")
        sys.exit(2)

    assert has_lock
    print("# Define SIGINT handler")
    signal.signal(signal.SIGINT, handle_SIGINT)
    print("# Set ready event")
    ready_event.set()
    # Wait until SIGINT is received from the parent process and, in that case,
    # release the lock and exit with a success code.
    interrupt_event.wait()
    print("# Lock Not Released")
    sys.exit(3)


def execute(command: str, stdin: str = "") -> str:
    """
    A utility function to simplify the running of shell commands.

    :param command: the command to run
    :param stdin: optional text that may be piped to the command
    :return: returns a 2-item list containing stdout and stderr decoded as a strings, in that order
    """
    process = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    output = process.communicate(stdin.encode())
    return [output[0].decode().strip(), output[1].decode().strip()]  # 0 == stdout, 1 == stderr


def setup_db() -> str:
    db = {}

    test_name = os.environ["PYTEST_CURRENT_TEST"].split(":")[-1].split(" ")[0].split("[")[0]
    suffix = test_name
    prefix = CUR_DIR + TEST_DATA_DIRECTORY
    while os.path.exists(prefix + suffix):
        suffix = test_name + str(random.randint(0, 1000))
    db["dir"] = prefix + suffix + "/"

    assert not os.path.exists(db["dir"])
    pathlib.Path(db["dir"]).mkdir(parents=True, exist_ok=True)

    db["gld"] = db["dir"] + "test_db.gld"
    os.environ["ydb_gbldir"] = db["gld"]

    execute(f"{CUR_DIR}/tests/createdb.sh {YDB_DIST} {db['dir'] + 'test_db.dat'}")

    return db


def teardown_db(previous: dict):
    shutil.rmtree(previous["dir"])


@pytest.fixture(scope="function")
def simple_data():
    """
    A pytest fixture that creates a test database, adds the above SIMPLE_DATA tuple to that
    database, and deletes the data and database after testing is complete. This fixture is
    in function scope so it will be executed for each test that uses it.

    Note that pytest runs tests sequentially, so it is safe for the test database to be
    updated for each test function.
    """

    db = setup_db()

    for key, value in SIMPLE_DATA:
        yottadb.set(key[0], key[1], value=value)

    yield

    for key, value in SIMPLE_DATA:
        yottadb.delete_tree(key[0], key[1])

    teardown_db(db)


@pytest.fixture(scope="function")
def new_db():
    """
    A pytest fixture that creates a test database prior to test execution,
    and then deletes it after testing is complete. This fixture is
    in function scope so it will be executed for each test that uses it.
    """

    db = setup_db()

    yield

    teardown_db(db)
