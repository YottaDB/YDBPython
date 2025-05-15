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
import pytest  # type: ignore # ignore due to pytest not having type annotations
import multiprocessing
import subprocess
import signal
import psutil
import random
import datetime
import time
import os
import re
import sys
import json
import requests
from urllib.request import urlretrieve
from typing import NamedTuple, Callable, Tuple, Sequence, AnyStr

import yottadb
from yottadb import YDBError, YDBNodeEnd
from conftest import lock_value, str2zwr_tests, set_ci_environment, reset_ci_environment, setup_db, teardown_db, SIMPLE_DATA
from conftest import execute


# Confirm that YDB_ERR_ZGBLDIRACC is raised when a YDB global directory
# cannot be found. Use pytest-order run this test first in order to force
# YDB to read the ydb_gbldir environment variable, which will not happen
# if another test opens a Global Directory before this test runs.
@pytest.mark.order(1)
def test_no_ydb_gbldir():
    # Unset $ydb_gbldir and $gtmgbldir prior to running any to prevent erroneous use of
    # any previously set global directory. This is done here since this test is run before
    # all other tests.
    try:
        del os.environ["ydb_gbldir"]
        del os.environ["gtmgbldir"]
    except KeyError:
        # Do not fail the test if these variables are already unset
        pass

    cur_dir = os.getcwd()
    try:
        os.environ["ydb_gbldir"]
    except KeyError:
        pass
    os.environ["ydb_gbldir"] = cur_dir + "/yottadb.gld"  # Set ydb_gbldir to non-existent global directory file

    lclname = b"^x"
    key = yottadb.Key(lclname)
    try:
        key.set("")
        assert False  # Exception should be raised before hitting this assert
    except YDBError as e:
        assert yottadb.YDB_ERR_ZGBLDIRACC == e.code()
        assert "418809578,(SimpleAPI),%YDB-E-ZGBLDIRACC, Cannot access global directory " + os.environ[
            "ydb_gbldir"
        ] + ".  Cannot continue.,%SYSTEM-E-ENO2, No such file or directory" == str(e)


def test_ci_table(new_db):
    cur_dir = os.getcwd()
    previous = set_ci_environment(cur_dir, "")
    cur_handle = yottadb.open_ci_table(cur_dir + "/tests/calltab.ci")
    yottadb.switch_ci_table(cur_handle)

    # Ensure no errors from basic/expected usage
    assert "3241" == yottadb.ci("HelloWorld2", ["1", "24", "3"], has_retval=True)
    # Test updating of IO parameter (second parameter) with a shorter value,
    # i.e. set arg_list[1] = arg_list[0]
    arg_list = ["1", "24", "3"]
    assert "3241" == yottadb.ci("HelloWorld2", arg_list, has_retval=True)
    assert arg_list[1] == "1"
    # Test None returned from routine with no return value,
    # also test update of output only parameter
    outarg = ""
    outargs = [outarg]
    assert yottadb.ci("NoRet", outargs) is None
    assert outargs[0] == "testeroni"
    # Test routine with no parameters
    assert "entry called" == yottadb.ci("HelloWorld1", has_retval=True)
    # Test routine with output parameter succeeds where the length of
    # the passed argument is equal to the length needed to store the result
    assert "1234567890" == yottadb.ci("StringExtend", [9876543210], has_retval=True)
    # Test routine with output parameter succeeds where the length of
    # the passed argument is greater than the length needed to store the result
    assert "1234567890" == yottadb.ci("StringExtend", [98765432101234], has_retval=True)
    # Test routine with output parameter succeeds when passed the empty string
    assert "1234567890" == yottadb.ci("StringExtend", [""], has_retval=True)
    # Test routine with output parameter succeeds where the length of
    # the passed argument (4 bytes) is LESS than the length needed to store the result (10 bytes).
    # In this case, the output parameter value is truncated to fit the available space (4 bytes).
    # The return value though is not truncated (i.e. it is 10 bytes long).
    outargs = [6789]
    assert "1234567890" == yottadb.ci("StringExtend", outargs, has_retval=True)
    assert [1234] == outargs

    old_handle = cur_handle
    cur_handle = yottadb.open_ci_table(cur_dir + "/tests/testcalltab.ci")
    last_handle = yottadb.switch_ci_table(cur_handle)
    assert last_handle == old_handle
    assert "entry was called" == yottadb.ci("HelloWorld99", has_retval=True)

    # Reset call-in table for other tests
    cur_handle = yottadb.open_ci_table(cur_dir + "/tests/calltab.ci")
    yottadb.switch_ci_table(cur_handle)

    reset_ci_environment(previous)


# Test ci() call using ydb_ci environment variable to specify call-in table
# location. This is the default usage.
def test_ci_default(new_db):
    cur_dir = os.getcwd()
    previous = set_ci_environment(cur_dir, cur_dir + "/tests/calltab.ci")

    # Ensure no errors from basic/expected usage
    assert "-1" == yottadb.ci("Passthrough", [-1], has_retval=True)
    assert "3241" == yottadb.ci("HelloWorld2", [1, 24, 3], has_retval=True)
    assert "3241" == yottadb.ci("HelloWorld2", ["1", "24", "3"], has_retval=True)
    # Test updating of IO parameter (second parameter) with a shorter value,
    # i.e. set arg_list[1] = arg_list[0]
    arg_list = ["1", "24", "3"]
    assert "3241" == yottadb.ci("HelloWorld2", arg_list, has_retval=True)
    assert arg_list[1] == "1"
    # Test None returned from routine with no return value,
    # also test update of output only parameter
    outarg = ""
    outargs = [outarg]
    assert yottadb.ci("NoRet", outargs) is None
    assert outargs[0] == "testeroni"
    # Test routine with no parameters
    assert "entry called" == yottadb.ci("HelloWorld1", has_retval=True)
    # Test call to ci() with more args than max raises ValueError
    with pytest.raises(ValueError) as verr:
        yottadb.ci("HelloWorld1", args=("b",) * (yottadb.max_ci_args + 1), has_retval=True)
    assert re.match(".*exceeds max for a.*bit system architecture.*", str(verr.value))  # Confirm correct ValueError message

    reset_ci_environment(previous)


def test_cip(new_db):
    cur_dir = os.getcwd()
    previous = set_ci_environment(cur_dir, cur_dir + "/tests/calltab.ci")

    # Ensure no errors from basic/expected usage
    assert "-1" == yottadb.cip("Passthrough", [-1], has_retval=True)
    assert "3241" == yottadb.cip("HelloWorld2", [1, 24, 3], has_retval=True)
    assert "3241" == yottadb.cip("HelloWorld2", ["1", "24", "3"], has_retval=True)
    # Test updating of IO parameter (second parameter) with a shorter value,
    # i.e. set arg_list[1] = arg_list[0]
    arg_list = ["1", "24", "3"]
    assert "3241" == yottadb.cip("HelloWorld2", arg_list, has_retval=True)
    assert arg_list[1] == "1"
    # Test None returned from routine with no return value,
    # also test update of output only parameter
    outarg = ""
    outargs = [outarg]
    assert yottadb.cip("NoRet", outargs) is None
    assert outargs[0] == "testeroni"
    outargs[0] = "new value"
    assert yottadb.cip("NoRet", outargs) is None
    assert outargs[0] == "testeroni"
    # Test routine with no parameters
    assert "entry called" == yottadb.cip("HelloWorld1", has_retval=True)
    # Test routine with output parameter succeeds where the length of
    # the passed argument is equal to the length needed to store the result
    assert "1234567890" == yottadb.cip("StringExtend", [9876543210], has_retval=True)
    # Test routine with output parameter succeeds where the length of
    # the passed argument is greater than the length needed to store the result
    assert "1234567890" == yottadb.cip("StringExtend", [98765432101234], has_retval=True)
    # Test routine with output parameter succeeds when passed the empty string
    assert "1234567890" == yottadb.ci("StringExtend", [""], has_retval=True)

    reset_ci_environment(previous)


# Confirm delete_node() and delete_tree() raise YDBError exceptions
def test_delete_errors():
    with pytest.raises(yottadb.YDBError):
        yottadb.delete_node(varname="\x80")
    with pytest.raises(yottadb.YDBError):
        yottadb.delete_tree(varname="\x80")


def test_delete_except():
    yottadb.set(varname="delex1", subsarray=("sub1", "sub2"), value="1")
    yottadb.set(varname="delex2", subsarray=("sub1", "sub2"), value="2")
    yottadb.set(varname="delex3", subsarray=("sub1", "sub2"), value="3")
    key1 = yottadb.Key("delex4")["sub1"]["sub2"]
    key2 = yottadb.Key("delex5")["sub1"]["sub2"]
    key3 = yottadb.Key("delex6")["sub1"]["sub2"]
    key1.value = "4"
    key2.value = "5"
    key3.value = "6"
    yottadb.delete_except(("delex1", "delex3", key1, key3))
    assert yottadb.get(varname="delex1", subsarray=("sub1", "sub2")) == b"1"
    assert yottadb.get(varname="delex2", subsarray=("sub1", "sub2")) == None
    assert yottadb.get(varname="delex3", subsarray=("sub1", "sub2")) == b"3"
    assert key1.value == b"4"
    assert key2.value == None
    assert key3.value == b"6"


def test_message():
    assert yottadb.message(yottadb.YDB_ERR_INVSTRLEN) == "%YDB-E-INVSTRLEN, Invalid string length !UL: max !UL"
    try:
        yottadb.message(0)  # Raises unknown error number error
        assert False
    except yottadb.YDBError as e:
        assert yottadb.YDB_ERR_UNKNOWNSYSERR == e.code()


def test_release():
    release = yottadb.release()
    assert re.match("pywr.*", release) is not None


def test_Key_object(simple_data):
    # Key creation, varname only
    key = yottadb.Key("^test1")
    assert key == b"test1value"
    assert key.name == "^test1"
    assert key.varname_key == key
    assert key.varname == "^test1"
    assert key.subsarray == []
    # Using bytes argument
    key = yottadb.Key(b"^test1")
    assert key == b"test1value"
    assert key.name == b"^test1"
    assert key.varname_key == key
    assert key.varname == b"^test1"
    assert key.subsarray == []

    # Key creation, varname and subscript
    key = yottadb.Key("^test2")["sub1"]
    assert key == b"test2value"
    assert key.name == "sub1"
    assert key.varname_key == yottadb.Key("^test2")
    assert key.varname == "^test2"
    assert key.subsarray == ["sub1"]
    # Using bytes arguments
    key = yottadb.Key(b"^test2")[b"sub1"]
    assert key == b"test2value"
    assert key.name == b"sub1"
    assert key.varname_key == yottadb.Key(b"^test2")
    assert key.varname == b"^test2"
    assert key.subsarray == [b"sub1"]

    # Key creation and value update, varname and subscript
    key = yottadb.Key("test3local")["sub1"]
    key.value = "smoketest3local"
    assert key == b"smoketest3local"
    assert key.name == "sub1"
    assert key.varname_key == yottadb.Key("test3local")
    assert key.varname == "test3local"
    assert key.subsarray == ["sub1"]
    # Using bytes arguments
    key = yottadb.Key(b"test3local")[b"sub1"]
    key.value = b"smoketest3local"
    assert key == b"smoketest3local"
    assert key.name == b"sub1"
    assert key.varname_key == yottadb.Key(b"test3local")
    assert key.varname == b"test3local"
    assert key.subsarray == [b"sub1"]

    # Key creation by setting parent explicitly
    key = yottadb.Key("^myglobal")["sub1"]["sub2"]
    key = yottadb.Key("sub3", parent=key)  # Raises TypeError for non-Key `parent` argument
    assert '^myglobal("sub1","sub2","sub3")' == str(key)

    # YDBLVUNDEFError and YDBGVUNDEFError for Key.value return None
    key = yottadb.Key("^nonexistent")  # Undefined global
    assert key.value is None
    key = yottadb.Key("nonexistent")  # Undefined local
    assert key.value is None

    # Key incrementation
    key = yottadb.Key("localincr")
    assert key.incr() == b"1"
    assert key.incr(-1) == b"0"
    assert key.incr("2") == b"2"
    assert key.incr("testeroni") == b"2"  # Not a canonical number, leaves value unchanged
    with pytest.raises(TypeError):
        key.incr(None)  # Must be int, str, float, or bytes
    # Using bytes argument for Key (carrying over previous node value)
    key = yottadb.Key(b"localincr")
    assert key.incr() == b"3"
    assert key.incr(-1) == b"2"
    assert key.incr(b"2") == b"4"
    with pytest.raises(TypeError):
        key.incr([])  # Must be int, str, float, or bytes

    # Key comparison via __eq__ (Key/value comparisons tested in various other
    # test cases below)
    key = yottadb.Key("testeroni")["sub1"]
    key_copy = yottadb.Key("testeroni")["sub1"]
    key2 = yottadb.Key("testeroni")["sub2"]
    # Same varname/subscripts, but different object should be equal
    assert key == key_copy
    # Different varname/subscripts should not be equal
    assert key != key2

    # Incrementation/decrementation using += and -= syntax (__iadd__ and __isub__ methods)
    key = yottadb.Key("iadd")
    key += 1
    assert int(key.value) == 1
    key -= 1
    assert int(key.value) == 0
    key += "2"
    assert int(key.value) == 2
    key -= "3"
    assert int(key.value) == -1
    key += 0.5
    assert float(key.value) == -0.5
    key -= -1.5
    assert int(key.value) == 1
    key += 0.5
    assert float(key.value) == 1.5
    key += "testeroni"  # Not a canonical number, leaves value unchanged
    assert float(key.value) == 1.5
    with pytest.raises(TypeError):
        key += ("tuple",)  # Must be int, str, float, or bytes


def test_Key_construction_errors():
    # Raise error if attempt to create Key from an object other than a Key
    with pytest.raises(TypeError) as terr:
        yottadb.Key(1)
    assert re.match("'name' must be an instance of str or bytes", str(terr.value))  # Confirm correct TypeError message

    # Raise error if attempt to create Key from an object other than a Key
    with pytest.raises(TypeError) as terr:
        yottadb.Key("^test1", "not a Key object")
    assert re.match("'parent' must be of type Key", str(terr.value))  # Confirm correct TypeError message

    # No error when constructing a Key on an Intrinsic Special Variable
    i = 0
    oldkey = yottadb.Key("$zyrelease")
    assert re.match("YottaDB r.*", oldkey.value.decode("utf-8"))

    # Raise error if attempt to create a Key with more than YDB_MAX_SUBS subscripts
    i = 0
    oldkey = yottadb.Key("mylocal")
    with pytest.raises(ValueError) as verr:
        while i < (yottadb.YDB_MAX_SUBS + 1):
            newkey = oldkey[str(i)]
            oldkey = newkey
            i += 1
    assert re.match("Cannot create Key with .* subscripts [(]max: .*[)]", str(verr.value))  # Confirm correct ValueError message


def test_Key__str__():
    assert str(yottadb.Key("test")) == "test"
    assert str(yottadb.Key("test")["sub1"]) == 'test("sub1")'
    assert str(yottadb.Key("test")["sub1"]["sub2"]) == 'test("sub1","sub2")'


def test_Key_get_value1(simple_data):
    assert yottadb.Key("^test1") == b"test1value"


def test_Key_get_value2(simple_data):
    assert yottadb.Key("^test2")["sub1"] == b"test2value"


def test_Key_get_value3(simple_data):
    assert yottadb.Key("^test3") == b"test3value1"
    assert yottadb.Key("^test3")["sub1"] == b"test3value2"
    assert yottadb.Key("^test3")["sub1"]["sub2"] == b"test3value3"


def test_Key_subsarray(simple_data):
    assert yottadb.Key("^test3").subsarray == []
    assert yottadb.Key("^test3")["sub1"].subsarray == ["sub1"]
    assert yottadb.Key("^test3")["sub1"]["sub2"].subsarray == ["sub1", "sub2"]
    # Confirm no UnboundLocalError when a Key has no subscripts
    for subscript in yottadb.Key("^test3").subscripts:
        pass


def test_Key_varname(simple_data):
    assert yottadb.Key("^test3").varname == "^test3"
    assert yottadb.Key("^test3")["sub1"].varname == "^test3"
    assert yottadb.Key("^test3")["sub1"]["sub2"].varname == "^test3"


def test_Key_set_value1():
    testkey = yottadb.Key("test4")
    testkey.value = "test4value"
    assert testkey == b"test4value"


def test_Key_set_value2():
    testkey = yottadb.Key("test5")["sub1"]
    testkey.value = "test5value"
    assert testkey == b"test5value"
    assert yottadb.Key("test5")["sub1"] == b"test5value"


def test_Key_set_value3():
    yottadb.Key("test5")["sub1"] = "test5value"
    assert yottadb.Key("test5")["sub1"] == b"test5value"


def test_Key_delete_node():
    testkey = yottadb.Key("test6")
    subkey = testkey["sub1"]
    testkey.value = "test6value"
    subkey.value = "test6 subvalue"

    assert testkey == b"test6value"
    assert subkey == b"test6 subvalue"

    testkey.delete_node()

    assert testkey.value is None
    assert subkey == b"test6 subvalue"


def test_Key_delete_tree():
    testkey = yottadb.Key("test7")
    subkey = testkey["sub1"]
    testkey.value = "test7value"
    subkey.value = "test7 subvalue"

    assert testkey == b"test7value"
    assert subkey == b"test7 subvalue"

    testkey.delete_tree()

    assert testkey.value is None
    assert subkey.value is None
    assert testkey.data == 0


def test_Key_data(simple_data):
    assert yottadb.Key("nodata").data == yottadb.YDB_DATA_UNDEF
    assert yottadb.Key("^test1").data == yottadb.YDB_DATA_VALUE_NODESC
    assert yottadb.Key("^test2").data == yottadb.YDB_DATA_NOVALUE_DESC
    assert yottadb.Key("^test2")["sub1"].data == yottadb.YDB_DATA_VALUE_NODESC
    assert yottadb.Key("^test3").data == yottadb.YDB_DATA_VALUE_DESC
    assert yottadb.Key("^test3")["sub1"].data == yottadb.YDB_DATA_VALUE_DESC
    assert yottadb.Key("^test3")["sub1"]["sub2"].data == yottadb.YDB_DATA_VALUE_NODESC

    # Confirm errors from C API are raised as YDBError exceptions
    with pytest.raises(yottadb.YDBError):
        yottadb.Key("^\x80").data


def test_Key_has_value(simple_data):
    assert not yottadb.Key("nodata").has_value
    assert yottadb.Key("^test1").has_value
    assert not yottadb.Key("^test2").has_value
    assert yottadb.Key("^test2")["sub1"].has_value
    assert yottadb.Key("^test3").has_value
    assert yottadb.Key("^test3")["sub1"].has_value
    assert yottadb.Key("^test3")["sub1"]["sub2"].has_value

    # Confirm errors from C API are raised as YDBError exceptions
    with pytest.raises(yottadb.YDBError):
        yottadb.Key("^\x80").has_value


def test_Key_has_tree(simple_data):
    assert not yottadb.Key("nodata").has_tree
    assert not yottadb.Key("^test1").has_tree
    assert yottadb.Key("^test2").has_tree
    assert not yottadb.Key("^test2")["sub1"].has_tree
    assert yottadb.Key("^test3").has_tree
    assert yottadb.Key("^test3")["sub1"].has_tree
    assert not yottadb.Key("^test3")["sub1"]["sub2"].has_tree

    # Confirm errors from C API are raised as YDBError exceptions
    with pytest.raises(yottadb.YDBError):
        yottadb.Key("^\x80").has_tree


def test_Key_subscript_next(simple_data):
    key = yottadb.Key("testsubsnext")
    key["sub1"] = "1"
    key["sub2"] = "2"
    key["sub3"] = "3"
    key["sub4"] = "4"

    assert key.subscript_next() == b"sub1"
    assert key.subscript_next() == b"sub2"
    assert key.subscript_next() == b"sub3"
    assert key.subscript_next() == b"sub4"

    try:
        key.subscript_next()
        assert False
    except YDBNodeEnd:
        assert key[key.subscript_next(reset=True)].value == b"1"
        assert key[key.subscript_next()].value == b"2"
        assert key[key.subscript_next()].value == b"3"
        assert key[key.subscript_next()].value == b"4"

    with pytest.raises(YDBNodeEnd):
        key.subscript_next()

    sub = key.subscript_next(reset=True)  # Reset starting subscript to ""
    count = 1
    assert sub == bytes(("sub" + str(count)).encode("ascii"))
    while True:
        try:
            sub = key.subscript_next()
            count += 1
            assert sub == bytes(("sub" + str(count)).encode("ascii"))
        except YDBNodeEnd:
            break

    # Confirm errors from C API are raised as YDBError exceptions
    with pytest.raises(yottadb.YDBError):
        yottadb.Key("^\x80").subscript_next()


def test_Key_subscript_previous(simple_data):
    key = yottadb.Key("testsubsprev")
    key["sub1"] = "1"
    key["sub2"] = "2"
    key["sub3"] = "3"
    key["sub4"] = "4"

    assert key.subscript_previous() == b"sub4"
    assert key.subscript_previous() == b"sub3"
    assert key.subscript_next() == b"sub4"  # Confirm compatibility with subscript_next()
    assert key.subscript_previous() == b"sub3"
    assert key.subscript_previous() == b"sub2"
    assert key.subscript_previous() == b"sub1"

    try:
        key.subscript_previous()
        assert False
    except YDBNodeEnd:
        assert key[key.subscript_previous(reset=True)].value == b"4"
        assert key[key.subscript_previous()].value == b"3"
        assert key[key.subscript_previous()].value == b"2"
        assert key[key.subscript_previous()].value == b"1"

    with pytest.raises(YDBNodeEnd):
        key.subscript_previous()

    sub = key.subscript_previous(reset=True)  # Reset starting subscript to ""
    count = 4
    assert sub == bytes(("sub" + str(count)).encode("ascii"))
    while True:
        try:
            sub = key.subscript_previous()
            count -= 1
            assert sub == bytes(("sub" + str(count)).encode("ascii"))
        except YDBNodeEnd:
            break

    # Confirm errors from C API are raised as YDBError exceptions
    # Note that a similar test case is not included in all test functions
    # to reduce duplication, as the same exception mechanism for this case
    # is operative across all wrapper functions and methods.
    with pytest.raises(yottadb.YDBError):
        yottadb.Key("^\x80").subscript_previous()


# transaction decorator smoke tests
@yottadb.transaction
def simple_transaction(key1: yottadb.Key, value1: str, key2: yottadb.Key, value2: str) -> None:
    key1.value = value1
    key2.value = value2

    rand = random.randint(0, 2)
    if 0 == rand:
        # Trigger YDBError to confirm exception raised to caller
        yottadb.get("\x80")
    elif 1 == rand:
        # Trigger and catch YDBError, then manually return to confirm return error code
        # to confirm value passed back to tp() by yottadb.transaction() as is
        try:
            yottadb.get("\x80")
            assert False
        except yottadb.YDBError as e:
            return e.code()
    else:
        # Return None, to validate that YDB_OK will be returned to caller by yottadb.transaction()
        return None


def test_transaction_smoke_test1(new_db) -> None:
    test_base_key = yottadb.Key("^TransactionDecoratorTests")["smoke test 1"]
    test_base_key.delete_tree()
    key1 = test_base_key["key1"]
    value1 = b"v1"
    key2 = test_base_key["key2"]
    value2 = b"v2"
    assert key1.data == yottadb.YDB_DATA_UNDEF
    assert key2.data == yottadb.YDB_DATA_UNDEF

    try:
        assert yottadb.YDB_OK == simple_transaction(key1, value1, key2, value2)
        assert key1 == value1
        assert key2 == value2
    except YDBError as e:
        assert yottadb.YDB_ERR_INVVARNAME == e.code()

    test_base_key.delete_tree()


@yottadb.transaction
def simple_rollback_transaction(key1: yottadb.Key, value1: str, key2: yottadb.Key, value2: str) -> None:
    key1.value = value1
    key2.value = value2
    raise yottadb.YDBTPRollback("rolling back transaction.")


def test_transaction_smoke_test2(new_db) -> None:
    test_base_key = yottadb.Key("^TransactionDecoratorTests")["smoke test 2"]
    test_base_key.delete_tree()
    key1 = test_base_key["key1"]
    value1 = "v1"
    key2 = test_base_key["key2"]
    value2 = "v2"
    assert key1.data == yottadb.YDB_DATA_UNDEF
    assert key2.data == yottadb.YDB_DATA_UNDEF

    try:
        simple_rollback_transaction(key1, value1, key2, value2)
        assert False
    except yottadb.YDBTPRollback as e:
        print(str(e))
        assert str(e) == f"{yottadb.YDB_TP_ROLLBACK}, %YDB-TP-ROLLBACK: Transaction not committed."

    assert key1.data == yottadb.YDB_DATA_UNDEF
    assert key2.data == yottadb.YDB_DATA_UNDEF
    test_base_key.delete_tree()


@yottadb.transaction
def simple_restart_transaction(key1: yottadb.Key, key2: yottadb.Key, value: str, restart_tracker: yottadb.Key) -> None:
    if restart_tracker.data == yottadb.YDB_DATA_UNDEF:
        key1.value = value
        restart_tracker.value = "1"
        raise yottadb.YDBTPRestart("restating transaction")
    else:
        key2.value = value


def test_transaction_smoke_test3(new_db) -> None:
    test_base_global_key = yottadb.Key("^TransactionDecoratorTests")["smoke test 3"]
    test_base_local_key = yottadb.Key("TransactionDecoratorTests")["smoke test 3"]
    test_base_global_key.delete_tree()
    test_base_local_key.delete_tree()

    key1 = test_base_global_key["key1"]
    key2 = test_base_global_key["key2"]
    value = b"val"
    restart_tracker = test_base_local_key["restart tracker"]

    assert key1.data == yottadb.YDB_DATA_UNDEF
    assert key2.data == yottadb.YDB_DATA_UNDEF
    assert restart_tracker.data == yottadb.YDB_DATA_UNDEF

    simple_restart_transaction(key1, key2, value, restart_tracker)

    assert key1.value is None
    assert key2 == value
    assert restart_tracker == b"1"

    test_base_global_key.delete_tree()
    test_base_local_key.delete_tree()


def no_action() -> None:
    pass


def set_key(key: yottadb.Key, value: str) -> None:
    key.value = value


def conditional_set_key(key1: yottadb.Key, key2: yottadb.Key, value: str, traker_key: yottadb.Key) -> None:
    if traker_key.data != yottadb.DATA_NO_DATA:
        key1.value = value
    else:
        key2.value = value


def raise_standard_python_exception() -> None:
    1 / 0


class TransactionData(NamedTuple):
    action: Callable = no_action
    action_arguments: Tuple = ()
    restart_key: yottadb.Key = None
    return_value: int = yottadb._yottadb.YDB_OK


@yottadb.transaction
def process_transaction(nested_transaction_data: Tuple[TransactionData]) -> int:
    current_data = nested_transaction_data[0]
    current_data.action(*current_data.action_arguments)
    sub_data = nested_transaction_data[1:]
    if len(sub_data) > 0:
        process_transaction(sub_data)

    if current_data.return_value == yottadb._yottadb.YDB_TP_RESTART:
        if current_data.restart_key.data == yottadb.DATA_NO_DATA:
            current_data.restart_key.value = "restarted"
            return yottadb._yottadb.YDB_TP_RESTART
        else:
            return yottadb._yottadb.YDB_OK

    return current_data.return_value


def test_transaction_return_YDB_OK(new_db):
    key = yottadb.Key("^transactiontests")["test_transaction_return_YDB_OK"]
    value = b"return YDB_OK"
    transaction_data = TransactionData(action=set_key, action_arguments=(key, value), return_value=yottadb._yottadb.YDB_OK)

    key.delete_tree()
    assert key.value is None
    assert key.data == 0

    process_transaction((transaction_data,))
    assert key == value
    key.delete_tree()


def test_nested_transaction_return_YDB_OK(new_db):
    key1 = yottadb.Key("^transactiontests")["test_transaction_return_YDB_OK"]["outer"]
    value1 = b"return YDB_OK"
    outer_transaction = TransactionData(action=set_key, action_arguments=(key1, value1), return_value=yottadb._yottadb.YDB_OK)
    key2 = yottadb.Key("^transactiontests")["test_transaction_return_YDB_OK"]["inner"]
    value2 = b"neseted return YDB_OK"
    inner_transaction = TransactionData(action=set_key, action_arguments=(key2, value2), return_value=yottadb._yottadb.YDB_OK)

    process_transaction((outer_transaction, inner_transaction))

    assert key1 == value1
    assert key2 == value2
    key1.delete_tree()
    key2.delete_tree()


YDB_MAX_TP_DEPTH = 12


@pytest.mark.parametrize("depth", range(1, YDB_MAX_TP_DEPTH + 1))
def test_transaction_return_YDB_OK_to_depth(depth):
    def key_at_level(level: int) -> yottadb.Key:
        sub1 = f"test_transaction_return_YDB_OK_to_depth{depth}"
        sub2 = f"level{level}"
        return yottadb.Key("^tptests")[sub1][sub2]

    def value_at_level(level: int) -> bytes:
        return bytes(f"level{level} returns YDB_OK", encoding="utf-8")

    db = setup_db()

    transaction_data = []
    for level in range(1, depth + 1):
        transaction_data.append(TransactionData(action=set_key, action_arguments=(key_at_level(level), value_at_level(level))))

    process_transaction(transaction_data)

    for level in range(1, depth + 1):
        assert key_at_level(level) == value_at_level(level)

    sub1 = f"test_transaction_return_YDB_OK_to_depth{depth}"
    yottadb.Key("^tptests")[sub1].delete_tree()

    teardown_db(db)


# Test various lock functions and lock methods on Key object
def test_locks(new_db):
    cur_dir = os.getcwd()
    previous = set_ci_environment(cur_dir, cur_dir + "/tests/calltab.ci")

    ppid = os.getpid()
    ready_event = multiprocessing.Event()

    yottadb.lock()
    print("## Test 1: Key.lock_incr() increments the lock level for the given key object")
    print("# Run key.lock_incr() 10 times to increment lock level to 10")
    key = yottadb.Key("test1")["sub1"]["sub2"]
    for i in range(0, 10):
        key.lock_incr()
    print("# Confirm the lock level for the given key is 10")
    result = yottadb.ci("ShowLocks", has_retval=True)
    assert result == 'LOCK test1("sub1","sub2") LEVEL=10'
    print("## Test 2: Key.lock_incr() decrements the lock level for the given key object")
    print("# Run key.lock_decr() 10 times to decrement lock level from 10 to 0")
    for i in range(0, 10):
        key.lock_decr()
    result = yottadb.ci("ShowLocks", has_retval=True)
    print("# Confirm the lock level for the given key is 0")
    assert result == "LOCK LEVEL=0"

    print("## Test 3: yottadb.lock() and Key.lock() can be used to acquire and release the same locks")
    key = yottadb.Key("^test4")["sub1"]["sub2"]
    print("# Acquire a lock on the given key using Key.lock()")
    key.lock()
    print("# Attempt to increment the lock from another process")
    print("# Expect this to fail due to a YDBLockTimeoutError and")
    print("# the child process to exit with a code of 1")
    process = multiprocessing.Process(target=lock_value, args=(key, ppid, ready_event))
    process.start()
    process.join()
    assert process.exitcode == 1
    print("# Release all locks on the given key using yottadb.lock()")
    yottadb.lock()
    print("# Again attempt to increment/decrement the previously locked resource to confirm it was released")
    process = multiprocessing.Process(target=lock_value, args=(key, ppid, ready_event))
    process.start()
    ready_event.wait()
    print("# Send SIGINT to instruct child process to stop execution")
    os.kill(process.pid, signal.SIGINT)
    process.join()
    assert process.exitcode == 0
    print("# Clear the ready event for reuse in next subtest")
    ready_event.clear()

    print("## Test 4: Key.lock_incr() raises YDBLockTimeoutError when another")
    print("## process holds a lock for the given Key value.")
    print("# Create a child process to acquire and hold a lock on the given key")
    key = yottadb.Key("^test2")["sub1"]
    process = multiprocessing.Process(target=lock_value, args=(key, ppid, ready_event))
    process.start()
    print("# Wait for a signal from the child process to indicate that")
    print("# a lock is held and notify the parent to continue execution.")
    ready_event.wait()
    print("# Once the lock is held, try to acquire it from the parent process")
    print("# Expect a YDBLockTimeoutError since the child process holds the lock")
    with pytest.raises(yottadb.YDBLockTimeoutError):
        key.lock_incr()
    result = yottadb.ci("ShowLocks", has_retval=True)
    assert result == "LOCK LEVEL=0"
    print("# Send SIGINT to instruct child process to stop execution")
    os.kill(process.pid, signal.SIGINT)
    process.join()
    assert process.exitcode == 0

    reset_ci_environment(previous)


def test_YDB_ERR_TIME2LONG(new_db):
    t1 = yottadb.Key("^test1")
    t2 = yottadb.Key("^test2")["sub1"]
    t3 = yottadb.Key("^test3")["sub1"]["sub2"]
    keys_to_lock = (t1, t2, t3)
    # Attempt to get locks for keys t1,t2 and t3
    try:
        yottadb.lock(keys=keys_to_lock, timeout_nsec=(yottadb.YDB_MAX_TIME_NSEC + 1))
        assert False
    except YDBError as e:
        assert yottadb.YDB_ERR_TIME2LONG == e.code()


def test_YDB_ERR_PARMOFLOW(new_db):
    keys_to_lock = []
    for i in range(0, 12):
        keys_to_lock.append(yottadb.Key(f"^t{i}"))
    # Attempt to get locks for more names than supported, i.e. 11,
    # per https://docs.yottadb.com/MultiLangProgGuide/pythonprogram.html#python-lock.
    with pytest.raises(ValueError) as e:
        yottadb.lock(keys=keys_to_lock, timeout_nsec=(yottadb.YDB_MAX_TIME_NSEC + 1))
    assert re.match(
        "'keys' argument invalid: invalid sequence length 12: max 11", str(e.value)
    )  # Confirm correct ValueError message


def test_isv_error():
    # Error when attempting to get the value of a subscripted Intrinsic Special Variable
    with pytest.raises(YDBError) as yerr:
        yottadb.get("$zyrelease", ("sub1", "sub2"))  # Raises ISVSUBSCRIPTED
    assert re.match(".*YDB-E-ISVSUBSCRIPTED.*", str(yerr.value))
    # Error when attempting to set the value of a subscripted Intrinsic Special Variable
    with pytest.raises(YDBError) as yerr:
        yottadb.set("$zyrelease", ("sub1", "sub2"), "test")  # Raises SVNOSET
    assert re.match(".*YDB-E-SVNOSET.*", str(yerr.value))


@pytest.mark.parametrize("input, output1, output2", str2zwr_tests)
def test_module_str2zwr(input, output1, output2):
    if os.environ.get("ydb_chset") == "UTF-8":
        assert yottadb.str2zwr(input) == output2
    else:
        assert yottadb.str2zwr(input) == output1


@pytest.mark.parametrize("output1, output2, input", str2zwr_tests)
def test_module_zwr2str(input, output1, output2):
    assert yottadb.zwr2str(input) == output1


def test_module_node_next(simple_data):
    assert yottadb.node_next("^test3") == (b"sub1",)
    assert yottadb.node_next("^test3", subsarray=("sub1",)) == (b"sub1", b"sub2")
    with pytest.raises(YDBNodeEnd):
        yottadb.node_next(varname="^test3", subsarray=("sub1", "sub2"))
    assert yottadb.node_next("^test6") == (b"sub6", b"subsub6")

    # Initialize test node and maintain full subscript list for later validation
    all_subs = []
    for i in range(1, 6):
        all_subs.append((b"sub" + bytes(str(i), encoding="utf-8")))
        yottadb.set("mylocal", all_subs, ("val" + str(i)))
    # Begin iteration over subscripts of node
    node_subs = yottadb.node_next("mylocal")
    num_subs = 1
    assert node_subs == (b"sub1",)
    assert num_subs == len(node_subs)
    while True:
        try:
            num_subs += 1
            node_subs = yottadb.node_next("mylocal", node_subs)
            assert set(node_subs).issubset(all_subs)
            assert num_subs == len(node_subs)
        except YDBNodeEnd:
            break

    # Ensure no UnicodeDecodeError for non-UTF-8 subscripts
    varname = "^x"
    yottadb.delete_tree(varname)
    yottadb.set(varname, (b"\xa0",), "")
    node_subs = yottadb.node_next(varname)
    yottadb.delete_tree(varname)


def test_module_node_previous(simple_data):
    with pytest.raises(YDBNodeEnd):
        yottadb.node_previous("^test3")
    assert yottadb.node_previous("^test3", ("sub1",)) == ()
    assert yottadb.node_previous("^test3", subsarray=("sub1", "sub2")) == (b"sub1",)

    # Initialize test node and maintain full subscript list for later validation
    all_subs = []
    for i in range(1, 6):
        all_subs.append((b"sub" + bytes(str(i), encoding="utf-8")))
        yottadb.set("mylocal", all_subs, ("val" + str(i)))
    # Begin iteration over subscripts of node
    node_subs = yottadb.node_previous("mylocal", all_subs)
    num_subs = len(("sub1", "sub2", "sub3", "sub4"))
    assert node_subs == (b"sub1", b"sub2", b"sub3", b"sub4")
    assert len(node_subs) == num_subs
    while True:
        try:
            num_subs -= 1
            node_subs = yottadb.node_previous("mylocal", node_subs)
            assert set(node_subs).issubset(all_subs)
            assert num_subs == len(node_subs)
        except YDBNodeEnd:
            break


def test_nodes_iter(simple_data):
    nodes = [
        (),
        (b"sub1",),
        (b"sub1", b"subsub1"),
        (b"sub1", b"subsub2"),
        (b"sub1", b"subsub3"),
        (b"sub2",),
        (b"sub2", b"subsub1"),
        (b"sub2", b"subsub2"),
        (b"sub2", b"subsub3"),
        (b"sub3",),
        (b"sub3", b"subsub1"),
        (b"sub3", b"subsub2"),
        (b"sub3", b"subsub3"),
    ]

    # Validate NodesIter.__next__() using a node in the middle of a tree
    i = 0
    for node in yottadb.nodes("^test4"):
        assert node == nodes[i]
        i += 1

    # Validate NodesIter.__next__() using a node in the middle of a tree
    i = 0
    # Omit "sub1" tree by excluding first 5 elements, including ("sub2",), since
    # this will be the starting subsarray for the call
    some_nodes = nodes[6:]
    for node in yottadb.nodes("^test4", ("sub2",)):
        assert node == some_nodes[i]
        i += 1

    # Validates support for subscripts that are both `bytes` and `str` objects,
    # i.e. no TypeError if any subscripts are `bytes` objects.
    i = 0
    # Omit "sub1" tree by excluding first 6 elements, including ("sub2", "subsub1"), since
    # this will be the starting subsarray for the call
    some_nodes = nodes[7:]
    for node in yottadb.nodes("^test4", (b"sub2", "subsub1")):
        assert node == some_nodes[i]
        i += 1

    # Validates support for subscripts that are `bytes` objects,
    # i.e. no TypeError if any subscripts are `bytes` objects.
    i = 0
    # Omit "sub1" tree by excluding first 5 elements, including ("sub2",), since
    # this will be the starting subsarray for the call
    some_nodes = nodes[6:]
    for node in yottadb.nodes("^test4", (b"sub2",)):  # Subscript is `bytes`
        assert node == some_nodes[i]
        i += 1

    # Validate NodesIter.__reversed__()
    i = 0
    rnodes = list(reversed(nodes))
    for node in reversed(yottadb.nodes("^test4")):
        print(f"node: {node}")
        print(f"nodes[i]: {nodes[i]}")
        assert node == rnodes[i]
        i += 1

    # Validate NodesIter.__reversed__() using a node in the middle of a tree
    i = 0
    # Omit "sub3" tree by excluding first 4 elements since the nodes list has already been reversed above
    nodes = rnodes[4:]
    for node in reversed(yottadb.nodes("^test4", ("sub2",))):
        assert node == nodes[i]
        i += 1

    # Validates support for subscripts that are `bytes` objects,
    # i.e. no TypeError if any subscripts are `bytes` objects.
    i = 0
    # Omit "sub3" tree by excluding first 4 elements since the nodes list has already been reversed above
    nodes = rnodes[4:]
    for node in reversed(yottadb.nodes("^test4", (b"sub2",))):  # Subscript is `bytes`
        assert node == nodes[i]
        i += 1

    # Validates support for subscripts that are both `bytes` and `str` objects,
    # i.e. no TypeError if any subscripts are `bytes` objects.
    i = 0
    # Omit "sub3" tree by excluding first 4 elements since the nodes list has already been reversed above
    nodes = rnodes[4:]
    for node in reversed(yottadb.nodes("^test4", (b"sub2", "subsub3"))):
        assert node == nodes[i]
        i += 1

    # Confirm errors from node_next()/node_previous() are raised as exceptions
    with pytest.raises(ValueError):
        for node in yottadb.nodes("a" * (yottadb.YDB_MAX_IDENT + 1)):
            pass
    with pytest.raises(ValueError):
        for node in reversed(yottadb.nodes("a" * (yottadb.YDB_MAX_IDENT + 1))):
            pass

    # Confirm errors from underlying API calls are raised as exceptions
    try:
        for node in yottadb.nodes("\x80"):
            pass
    except yottadb.YDBError as e:
        assert yottadb.YDB_ERR_INVVARNAME == e.code()
    try:
        for node in reversed(yottadb.nodes("\x80")):
            pass
    except yottadb.YDBError as e:
        assert yottadb.YDB_ERR_INVVARNAME == e.code()


def test_module_subscript_next(simple_data):
    assert yottadb.subscript_next(varname="^test1") == b"^test2"
    assert yottadb.subscript_next(varname="^test2") == b"^test3"
    assert yottadb.subscript_next(varname="^test3") == b"^test4"
    with pytest.raises(YDBNodeEnd):
        yottadb.subscript_next(varname="^test7")

    subscript = yottadb.subscript_next(varname="^test4", subsarray=("",))
    count = 1
    assert subscript == bytes(("sub" + str(count)).encode("ascii"))
    while True:
        count += 1
        try:
            subscript = yottadb.subscript_next(varname="^test4", subsarray=(subscript,))
            assert subscript == bytes(("sub" + str(count)).encode("ascii"))
        except YDBNodeEnd:
            break

    assert yottadb.subscript_next(varname="^test4", subsarray=("sub1", "")) == b"subsub1"
    assert yottadb.subscript_next(varname="^test4", subsarray=("sub1", "subsub1")) == b"subsub2"
    assert yottadb.subscript_next(varname="^test4", subsarray=("sub1", "subsub2")) == b"subsub3"
    with pytest.raises(YDBNodeEnd):
        yottadb.subscript_next(varname="^test4", subsarray=("sub3", "subsub3"))

    # Test subscripts that include a non-UTF-8 character
    assert yottadb.subscript_next(varname="^test7", subsarray=("",)) == b"sub1\x80"
    assert yottadb.subscript_next(varname="^test7", subsarray=(b"sub1\x80",)) == b"sub2\x80"
    assert yottadb.subscript_next(varname="^test7", subsarray=(b"sub2\x80",)) == b"sub3\x80"
    assert yottadb.subscript_next(varname="^test7", subsarray=(b"sub3\x80",)) == b"sub4\x80"
    with pytest.raises(YDBNodeEnd):
        yottadb.subscript_next(varname="^test7", subsarray=(b"sub4\x80",))


def test_module_subscript_previous(simple_data):
    assert yottadb.subscript_previous(varname="^test1") == b"^Test5"
    assert yottadb.subscript_previous(varname="^test2") == b"^test1"
    assert yottadb.subscript_previous(varname="^test3") == b"^test2"
    assert yottadb.subscript_previous(varname="^test4") == b"^test3"
    with pytest.raises(YDBNodeEnd):
        yottadb.subscript_previous(varname="^Test5")

    subscript = yottadb.subscript_previous(varname="^test4", subsarray=("",))
    count = 3
    assert subscript == bytes(("sub" + str(count)).encode("ascii"))
    while True:
        count -= 1
        try:
            subscript = yottadb.subscript_previous(varname="^test4", subsarray=(subscript,))
            assert subscript == bytes(("sub" + str(count)).encode("ascii"))
        except YDBNodeEnd:
            break

    assert yottadb.subscript_previous(varname="^test4", subsarray=("sub1", "")) == b"subsub3"
    assert yottadb.subscript_previous(varname="^test4", subsarray=("sub1", "subsub2")) == b"subsub1"
    assert yottadb.subscript_previous(varname="^test4", subsarray=("sub1", "subsub3")) == b"subsub2"
    with pytest.raises(YDBNodeEnd):
        yottadb.subscript_previous(varname="^test4", subsarray=("sub3", "subsub1"))

    # Test subscripts that include a non-UTF-8 character
    assert yottadb.subscript_previous(varname="^test7", subsarray=("",)) == b"sub4\x80"
    assert yottadb.subscript_previous(varname="^test7", subsarray=(b"sub4\x80",)) == b"sub3\x80"
    assert yottadb.subscript_previous(varname="^test7", subsarray=(b"sub3\x80",)) == b"sub2\x80"
    assert yottadb.subscript_previous(varname="^test7", subsarray=(b"sub2\x80",)) == b"sub1\x80"
    with pytest.raises(YDBNodeEnd):
        yottadb.subscript_previous(varname="^test7", subsarray=(b"sub1\x80",))


def test_subscripts_iter(simple_data):
    subs = [b"sub1", b"sub2", b"sub3"]

    # Validate SubscriptsIter.__next__() starting from the first subscript of a subscript level
    i = 0
    for subscript in yottadb.subscripts("^test4", ("",)):
        assert subscript == subs[i]
        i += 1

    # Validate SubscriptsIter.__next__() using a subscript in the middle of a subscript level
    i = 1
    for subscript in yottadb.subscripts("^test4", ("sub1",)):
        assert subscript == subs[i]
        i += 1

    # Validate SubscriptsIter.__reversed__() starting from the first subscript of a subscript level
    i = 0
    rsubs = list(reversed(subs))
    for subscript in reversed(yottadb.subscripts("^test4", ("",))):
        assert subscript == rsubs[i]
        i += 1

    # Validate SubscriptsIter.__reversed__() using a subscript in the middle of a subscript level
    i = 1
    for subscript in reversed(yottadb.subscripts("^test4", ("sub3",))):
        assert subscript == rsubs[i]
        i += 1

    varnames = [b"^Test5", b"^test1", b"^test2", b"^test3", b"^test4", b"^test6", b"^test7"]
    i = 0
    for subscript in yottadb.subscripts("^%"):
        assert subscript == varnames[i]
        i += 1
    assert len(varnames) == i

    i = 0
    rvarnames = list(reversed(varnames))
    for subscript in reversed(yottadb.subscripts("^z")):
        assert subscript == rvarnames[i]
        i += 1
    assert len(rvarnames) == i

    # Confirm errors from subscript_next()/subscript_previous() are raised as exceptions
    with pytest.raises(ValueError):
        for subscript in yottadb.subscripts("a" * (yottadb.YDB_MAX_IDENT + 1)):
            pass
    with pytest.raises(ValueError):
        for subscript in reversed(yottadb.subscripts("a" * (yottadb.YDB_MAX_IDENT + 1))):
            pass

    # Confirm errors from underlying API calls are raised as exceptions
    try:
        for subscript in yottadb.subscripts("\x80"):
            pass
    except yottadb.YDBError as e:
        assert yottadb.YDB_ERR_INVVARNAME == e.code()
    try:
        for subscript in reversed(yottadb.subscripts("\x80")):
            pass
    except yottadb.YDBError as e:
        assert yottadb.YDB_ERR_INVVARNAME == e.code()


# Helper function that creates a node + value tuple that mirrors the
# format used in SIMPLE_DATA to simplify output verification in
# test_all_nodes_iter.
def assemble_node(gblname: str, node_subs: Tuple[bytes]) -> Tuple[Tuple[str, Tuple[AnyStr, ...]], str]:
    subs = []
    for sub in node_subs:
        try:
            subs.append(sub.decode("utf-8"))
        except UnicodeError:
            subs.append(sub)
    node = ((gblname.decode("utf-8"), tuple(subs)), yottadb.get(gblname, node_subs).decode("utf-8"))
    return node


def test_all_nodes_iter(simple_data):
    # Get all nodes in database using forward subscripts() and forward nodes()
    gblname = "^%"
    all_nodes = []
    for gblname in yottadb.subscripts(gblname):
        for node_subs in yottadb.nodes(gblname):
            all_nodes.append(assemble_node(gblname, node_subs))
    for expected, actual in zip(SIMPLE_DATA, all_nodes):
        assert expected == actual

    # Initialize result set in proper order
    sdata = [
        (("^Test5", ()), "test5value"),
        (("^test1", ()), "test1value"),
        (("^test2", ("sub1",)), "test2value"),
        (("^test3", ("sub1", "sub2")), "test3value3"),
        (("^test3", ("sub1",)), "test3value2"),
        (("^test3", ()), "test3value1"),
        (("^test4", ("sub3", "subsub3")), "test4sub3subsub3"),
        (("^test4", ("sub3", "subsub2")), "test4sub3subsub2"),
        (("^test4", ("sub3", "subsub1")), "test4sub3subsub1"),
        (("^test4", ("sub3",)), "test4sub3"),
        (("^test4", ("sub2", "subsub3")), "test4sub2subsub3"),
        (("^test4", ("sub2", "subsub2")), "test4sub2subsub2"),
        (("^test4", ("sub2", "subsub1")), "test4sub2subsub1"),
        (("^test4", ("sub2",)), "test4sub2"),
        (("^test4", ("sub1", "subsub3")), "test4sub1subsub3"),
        (("^test4", ("sub1", "subsub2")), "test4sub1subsub2"),
        (("^test4", ("sub1", "subsub1")), "test4sub1subsub1"),
        (("^test4", ("sub1",)), "test4sub1"),
        (("^test4", ()), "test4"),
        (("^test6", ("sub6", "subsub6")), "test6value"),
        (("^test7", (b"sub4\x80", "sub7")), "test7sub4value"),
        (("^test7", (b"sub3\x80", "sub7")), "test7sub3value"),
        (("^test7", (b"sub2\x80", "sub7")), "test7sub2value"),
        (("^test7", (b"sub1\x80",)), "test7value"),
    ]
    # Get all nodes in database using forward subscripts() and reverse nodes()
    gblname = "^%"
    all_nodes = []
    for gblname in yottadb.subscripts(gblname):
        for node_subs in reversed(yottadb.nodes(gblname)):
            all_nodes.append(assemble_node(gblname, node_subs))
    for expected, actual in zip(sdata, all_nodes):
        assert expected == actual

    # Initialize result set in proper order
    sdata = [
        (("^test7", (b"sub1\x80",)), "test7value"),
        (("^test7", (b"sub2\x80", "sub7")), "test7sub2value"),
        (("^test7", (b"sub3\x80", "sub7")), "test7sub3value"),
        (("^test7", (b"sub4\x80", "sub7")), "test7sub4value"),
        (("^test6", ("sub6", "subsub6")), "test6value"),
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
        (("^test3", ()), "test3value1"),
        (("^test3", ("sub1",)), "test3value2"),
        (("^test3", ("sub1", "sub2")), "test3value3"),
        (("^test2", ("sub1",)), "test2value"),
        (("^test1", ()), "test1value"),
        (("^Test5", ()), "test5value"),
    ]
    # Get all nodes in database using reverse subscripts() and forward nodes()
    gblname = "^zzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
    all_nodes = []
    for gblname in reversed(yottadb.subscripts(gblname)):
        for node_subs in yottadb.nodes(gblname):
            all_nodes.append(assemble_node(gblname, node_subs))
    for expected, actual in zip(sdata, all_nodes):
        assert expected == actual

    # Get all nodes in database using reverse subscripts() and reverse nodes()
    gblname = "^zzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
    all_nodes = []
    for gblname in reversed(yottadb.subscripts(gblname)):
        for node_subs in reversed(yottadb.nodes(gblname)):
            all_nodes.append(assemble_node(gblname, node_subs))
    for expected, actual in zip(reversed(SIMPLE_DATA), all_nodes):
        assert expected == actual


def test_import():
    assert yottadb.YDB_DEL_TREE == 1
    assert yottadb.YDB_DEL_NODE == 2

    assert yottadb.YDB_SEVERITY_WARNING == 0
    assert yottadb.YDB_SEVERITY_SUCCESS == 1
    assert yottadb.YDB_SEVERITY_ERROR == 2
    assert yottadb.YDB_SEVERITY_INFORMATIONAL == 3
    assert yottadb.YDB_SEVERITY_FATAL == 4

    assert yottadb.YDB_DATA_UNDEF == 0
    assert yottadb.YDB_DATA_VALUE_NODESC == 1
    assert yottadb.YDB_DATA_NOVALUE_DESC == 10
    assert yottadb.YDB_DATA_VALUE_DESC == 11
    assert yottadb.YDB_DATA_ERROR == 0x7FFFFF00

    assert yottadb.YDB_MAIN_LANG_C == 0
    assert yottadb.YDB_MAIN_LANG_GO == 1

    assert yottadb.YDB_RELEASE >= 133

    assert yottadb.YDB_MAX_IDENT == 31
    assert yottadb.YDB_MAX_NAMES == 35
    assert yottadb.YDB_MAX_STR == (1 * 1024 * 1024)
    assert yottadb.YDB_MAX_SUBS == 31
    assert yottadb.YDB_MAX_PARMS == 32
    assert yottadb.YDB_MAX_TIME_NSEC == 2147483647000000
    assert yottadb.YDB_MAX_YDBERR == (1 << 30)
    assert yottadb.YDB_MAX_ERRORMSG == 1024
    assert yottadb.YDB_MIN_YDBERR == (1 << 27)

    assert yottadb.YDB_OK == 0

    assert yottadb.YDB_INT_MAX == 0x7FFFFFFF
    assert yottadb.YDB_TP_RESTART == (yottadb.YDB_INT_MAX - 1)
    assert yottadb.YDB_TP_ROLLBACK == (yottadb.YDB_INT_MAX - 2)
    assert yottadb.YDB_NOTOK == (yottadb.YDB_INT_MAX - 3)
    assert yottadb.YDB_LOCK_TIMEOUT == (yottadb.YDB_INT_MAX - 4)
    assert yottadb.YDB_DEFER_HANDLER == (yottadb.YDB_INT_MAX - 5)

    assert yottadb.DEFAULT_DATA_SIZE == 32
    assert yottadb.DEFAULT_SUBSCR_CNT == 2
    assert yottadb.DEFAULT_SUBSCR_SIZE == 16

    assert yottadb.YDB_NOTTP == 0

    # Selection of constants from libydberrors.h
    assert yottadb.YDB_ERR_INVSTRLEN == -150375522
    assert yottadb.YDB_ERR_VERSION == -150374082
    assert yottadb.YDB_ERR_FILENOTFND == -150374338

    # Selection of constants from libydberrors2.h
    assert yottadb.YDB_ERR_FATALERROR2 == -151027828
    assert yottadb.YDB_ERR_TIME2LONG == -151027834
    assert yottadb.YDB_ERR_VARNAME2LONG == -151027842


def test_ctrl_c(simple_data):
    def infinite_set():
        try:
            while True:
                key = yottadb.Key("^x")
                key.set("")
        except KeyboardInterrupt:
            # See comment before similar code block in tests/test_threeenp1.py for why the -2 is needed below.
            sys.exit(-2)

    process = multiprocessing.Process(target=infinite_set)

    process.start()
    time.sleep(0.5)
    psutil.Process(process.pid).send_signal(signal.SIGINT)
    process.join()

    assert process.exitcode == 254


def test_tp_callback_single(new_db):
    # Define a simple callback function
    def callback(fruit1: yottadb.Key, value1: str, fruit2: yottadb.Key, value2: str, fruit3: yottadb.Key, value3: str) -> int:
        # Generate a random number to signal whether to raise an exception and,
        # if so, which exception to raise
        rand_ret = random.randint(0, 2)

        fruit1.value = value1
        fruit2.value = value2
        if 0 == rand_ret:
            raise yottadb.YDBTPRestart
        fruit3.value = value3
        if 1 == rand_ret:
            raise yottadb.YDBTPRollback

        return yottadb.YDB_OK

    apples = yottadb.Key("fruits")["apples"]
    bananas = yottadb.Key("fruits")["bananas"]
    oranges = yottadb.Key("fruits")["oranges"]

    # Initial node values
    apples_val1 = b"10"
    bananas_val1 = b"5"
    oranges_val1 = b"12"
    # Target node values
    apples_val2 = b"5"
    bananas_val2 = b"10"
    oranges_val2 = b"8"
    # Set nodes to initial values
    apples.value = apples_val1
    bananas.value = bananas_val1
    oranges.value = oranges_val1

    # Call the callback function that will attempt to update the given nodes
    try:
        yottadb.tp(callback, args=(apples, apples_val2, bananas, bananas_val2, oranges, oranges_val2))
        assert apples.value == apples_val2
        assert bananas.value == bananas_val2
        assert oranges.value == oranges_val2
    except yottadb.YDBTPRestart:
        assert apples.value == apples_val2
        assert bananas.value == bananas_val2
        assert oranges.value == oranges_val1  # Should issue YDBTPRestart before updating oranges
    except yottadb.YDBTPRollback:
        assert apples.value == apples_val2
        assert bananas.value == bananas_val2
        assert oranges.value == oranges_val2
    except Exception:
        assert False


def test_tp_callback_multi(new_db):
    # Define a simple callback function that attempts to increment the global variable nodes represented
    # by the given Key objects. If a YDBTPRestart is encountered, the function will retry the continue
    # attempting the increment operation until it succeeds.
    def callback(fruit1: yottadb.Key, fruit2: yottadb.Key, fruit3: yottadb.Key) -> int:
        fruit1_done = False
        fruit2_done = False
        fruit3_done = False

        while not fruit1_done or not fruit2_done or not fruit3_done:
            if not fruit1_done:
                fruit1.incr()
                fruit1_done = True

            time.sleep(0.1)
            if not fruit2_done:
                fruit2.incr()
                fruit2_done = True

            if not fruit3_done:
                fruit3.incr()
                fruit3_done = True

        return yottadb.YDB_OK

    # Define a simple wrapper function to call the callback function via tp().
    # This wrapper will then be used to spawn multiple processes, each of which
    # calls tp() using the callback function.
    def wrapper(function: Callable[..., object], args: Sequence[yottadb.Key]) -> int:
        return yottadb.tp(function, args=args)

    # Create keys
    apples = yottadb.Key("^fruits")["apples"]
    bananas = yottadb.Key("^fruits")["bananas"]
    oranges = yottadb.Key("^fruits")["oranges"]
    # Initialize nodes
    apples_init = "0"
    bananas_init = "5"
    oranges_init = "10"
    apples.value = apples_init
    bananas.value = bananas_init
    oranges.value = oranges_init

    # Spawn some processes that will each call the callback function
    # and attempt to access the same nodes simultaneously. This will
    # trigger YDBTPRestarts, until each callback function successfully
    # updates the nodes.
    num_procs = 10
    processes = []
    for proc in range(0, num_procs):
        # Call the callback function that will attempt to update the given nodes
        process = multiprocessing.Process(target=wrapper, args=(callback, (apples, bananas, oranges)))
        process.start()
        processes.append(process)
    # Gracefully terminate each process and confirm it exited without an error
    for process in processes:
        process.join()
        assert process.exitcode == 0

    # Confirm all nodes incremented by num_procs, i.e. by one per callback process spawned
    assert int(apples.value) == int(apples_init) + num_procs
    assert int(bananas.value) == int(bananas_init) + num_procs
    assert int(oranges.value) == int(oranges_init) + num_procs


def test_Key_load_tree(simple_data):
    test4 = yottadb.Key("^test4")
    test4_dict = test4.load_tree()

    assert test4_dict["value"] == "test4"
    assert test4_dict["sub1"]["value"] == "test4sub1"
    assert test4_dict["sub1"]["subsub1"]["value"] == "test4sub1subsub1"
    assert test4_dict["sub1"]["subsub2"]["value"] == "test4sub1subsub2"
    assert test4_dict["sub1"]["subsub3"]["value"] == "test4sub1subsub3"
    assert test4_dict["sub2"]["value"] == "test4sub2"
    assert test4_dict["sub2"]["subsub1"]["value"] == "test4sub2subsub1"
    assert test4_dict["sub2"]["subsub2"]["value"] == "test4sub2subsub2"
    assert test4_dict["sub2"]["subsub3"]["value"] == "test4sub2subsub3"
    assert test4_dict["sub3"]["value"] == "test4sub3"
    assert test4_dict["sub3"]["subsub1"]["value"] == "test4sub3subsub1"
    assert test4_dict["sub3"]["subsub2"]["value"] == "test4sub3subsub2"
    assert test4_dict["sub3"]["subsub3"]["value"] == "test4sub3subsub3"

    # Verify that load_tree() no longer return previous data like it once did
    test4.delete_tree()
    test4_dict = test4.load_tree()
    assert repr(test4_dict) == "{}"


def test_Key_save_tree(simple_data):
    test4 = yottadb.Key("^test4")
    test4_dict = test4.load_tree()

    test4.delete_tree()
    test4.save_tree(test4_dict)

    assert test4.value == b"test4"
    assert test4["sub1"].value == b"test4sub1"
    assert test4["sub1"]["subsub1"].value == b"test4sub1subsub1"
    assert test4["sub1"]["subsub2"].value == b"test4sub1subsub2"
    assert test4["sub1"]["subsub3"].value == b"test4sub1subsub3"
    assert test4["sub2"].value == b"test4sub2"
    assert test4["sub2"]["subsub1"].value == b"test4sub2subsub1"
    assert test4["sub2"]["subsub2"].value == b"test4sub2subsub2"
    assert test4["sub2"]["subsub3"].value == b"test4sub2subsub3"
    assert test4["sub3"].value == b"test4sub3"
    assert test4["sub3"]["subsub1"].value == b"test4sub3subsub1"
    assert test4["sub3"]["subsub2"].value == b"test4sub3subsub2"
    assert test4["sub3"]["subsub3"].value == b"test4sub3subsub3"

    test4_sub1 = yottadb.Key("^test4")["sub1"]
    test4_sub1_dict = test4_sub1.load_tree()

    test4_sub1.delete_tree()
    test4_sub1.save_tree(test4_sub1_dict)

    assert test4_sub1.value == b"test4sub1"
    assert test4_sub1["subsub1"].value == b"test4sub1subsub1"
    assert test4_sub1["subsub2"].value == b"test4sub1subsub2"
    assert test4_sub1["subsub3"].value == b"test4sub1subsub3"


def test_json_roundtrip_empty_object(new_db):
    key = yottadb.Key("emptyjson")
    key.save_json({})
    assert {} == key.load_json()


def test_deserialize_JSON(new_db):
    response = requests.get("https://rxnav.nlm.nih.gov/REST/relatedndc.json?relation=product&ndc=0069-3060")
    json_data = json.loads(response.content)
    key = yottadb.Key("^rxnav")
    key.save_json(json_data)
    loaded_json = key.load_json()
    # Confirm that the JSON that was stored in YDB and retrieved matches
    # the original source JSON. We assert on type to confirm that two
    # different objects and types of objects are being compared.
    assert type(loaded_json) == type(json_data)
    assert loaded_json == json_data

    response = requests.get("https://gitlab.com/api/v4/projects")
    json_data = json.loads(response.content)
    key = yottadb.Key("^v4")
    key.save_json(json_data)
    loaded_json = key.load_json()
    assert type(loaded_json) == type(json_data)
    assert loaded_json == json_data


def test_manipulate_JSON_in_place(new_db):
    response = requests.get("https://rxnav.nlm.nih.gov/REST/relatedndc.json?relation=product&ndc=0069-3060")
    original_json = json.loads(response.content)
    key = yottadb.Key("^rxnorm")
    key.delete_tree()
    key.save_json(original_json)

    saved_json = key.load_json()
    key["ndcInfoList"]["ndcInfo"]["3"]["ndc11"].value = b"00069306087"
    revised_json = key.load_json()
    assert revised_json["ndcInfoList"]["ndcInfo"][2]["ndc11"] != saved_json["ndcInfoList"]["ndcInfo"][2]["ndc11"]
    assert revised_json["ndcInfoList"]["ndcInfo"][2]["ndc11"] == "00069306087"
