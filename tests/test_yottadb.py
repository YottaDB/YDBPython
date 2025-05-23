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
    node = yottadb.Node(lclname)
    try:
        node.set("")
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
        yottadb.delete_node(name="\x80")
    with pytest.raises(yottadb.YDBError):
        yottadb.delete_tree(name="\x80")


def test_delete_except():
    yottadb.set(name="delex1", subsarray=("sub1", "sub2"), value="1")
    yottadb.set(name="delex2", subsarray=("sub1", "sub2"), value="2")
    yottadb.set(name="delex3", subsarray=("sub1", "sub2"), value="3")
    node1 = yottadb.Node("delex4")["sub1"]["sub2"]
    node2 = yottadb.Node("delex5")["sub1"]["sub2"]
    node3 = yottadb.Node("delex6")["sub1"]["sub2"]
    node1.value = "4"
    node2.value = "5"
    node3.value = "6"
    yottadb.delete_except(("delex1", "delex3", node1, node3))
    assert yottadb.get(name="delex1", subsarray=("sub1", "sub2")) == b"1"
    assert yottadb.get(name="delex2", subsarray=("sub1", "sub2")) == None
    assert yottadb.get(name="delex3", subsarray=("sub1", "sub2")) == b"3"
    assert node1.value == b"4"
    assert node2.value == None
    assert node3.value == b"6"


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


def test_Node_object(simple_data):
    # Node creation, name only
    node = yottadb.Node("^test1")
    assert node == b"test1value"
    assert node.leaf == "^test1"
    assert node.name == "^test1"
    assert node.subsarray == []
    # Using bytes argument
    node = yottadb.Node(b"^test1")
    assert node == b"test1value"
    assert node.leaf == b"^test1"
    assert node.name == b"^test1"
    assert node.subsarray == []

    # Node creation, name and subscript
    node = yottadb.Node("^test2")["sub1"]
    assert node == b"test2value"
    assert node.leaf == "sub1"
    assert node.name == "^test2"
    assert node.subsarray == ["sub1"]
    # Using bytes arguments
    node = yottadb.Node(b"^test2")[b"sub1"]
    assert node == b"test2value"
    assert node.leaf == b"sub1"
    assert node.name == b"^test2"
    assert node.subsarray == [b"sub1"]

    # Node creation and value update, name and subscript
    node = yottadb.Node("test3local")["sub1"]
    node.value = "smoketest3local"
    assert node == b"smoketest3local"
    assert node.leaf == "sub1"
    assert node.name == "test3local"
    assert node.subsarray == ["sub1"]
    # Using bytes arguments
    node = yottadb.Node(b"test3local")[b"sub1"]
    node.value = b"smoketest3local"
    assert node == b"smoketest3local"
    assert node.leaf == b"sub1"
    assert node.name == b"test3local"
    assert node.subsarray == [b"sub1"]

    # Node creation by setting subsarray implicitly
    node = yottadb.Node("^myglobal", ("sub1", "sub2"))
    assert '^myglobal("sub1","sub2")' == str(node)

    # Node creation by setting subsarray explicitly
    node = yottadb.Node("^myglobal", subsarray=("sub1", "sub2"))
    assert '^myglobal("sub1","sub2")' == str(node)

    # YDBLVUNDEFError and YDBGVUNDEFError for Node.value return None
    node = yottadb.Node("^nonexistent")  # Undefined global
    assert node.value is None
    node = yottadb.Node("nonexistent")  # Undefined local
    assert node.value is None

    # Node incrementation
    node = yottadb.Node("localincr")
    assert node.incr() == b"1"
    assert node.incr(-1) == b"0"
    assert node.incr("2") == b"2"
    assert node.incr("testeroni") == b"2"  # Not a canonical number, leaves value unchanged
    with pytest.raises(TypeError):
        node.incr(None)  # Must be int, str, float, or bytes
    # Using bytes argument for Node (carrying over previous node value)
    node = yottadb.Node(b"localincr")
    assert node.incr() == b"3"
    assert node.incr(-1) == b"2"
    assert node.incr(b"2") == b"4"
    with pytest.raises(TypeError):
        node.incr([])  # Must be int, str, float, or bytes

    # Node comparison via __eq__ (Node/value comparisons tested in various other
    # test cases below)
    node = yottadb.Node("testeroni")["sub1"]
    node_copy = yottadb.Node("testeroni")["sub1"]
    node2 = yottadb.Node("testeroni")["sub2"]
    # Same name/subscripts, but different object should be equal
    assert node == node_copy
    # Different name/subscripts should not be equal
    assert node != node2

    # Incrementation/decrementation using += and -= syntax (__iadd__ and __isub__ methods)
    node = yottadb.Node("iadd")
    node += 1
    assert int(node.value) == 1
    node -= 1
    assert int(node.value) == 0
    node += "2"
    assert int(node.value) == 2
    node -= "3"
    assert int(node.value) == -1
    node += 0.5
    assert float(node.value) == -0.5
    node -= -1.5
    assert int(node.value) == 1
    node += 0.5
    assert float(node.value) == 1.5
    node += "testeroni"  # Not a canonical number, leaves value unchanged
    assert float(node.value) == 1.5
    with pytest.raises(TypeError):
        node += ("tuple",)  # Must be int, str, float, or bytes


def test_Node_callable():
    node1 = yottadb.Node("calltest", ("sub1", "sub2"))
    node2 = node1("sub3", "sub4", "sub5")
    assert node2.name == "calltest"
    assert node2.subsarray == ["sub1", "sub2", "sub3", "sub4", "sub5"]


def test_Node_iter(simple_data):
    node = yottadb.Node("^test4")
    for i, node2 in enumerate(node):
        assert node2.leaf == bytes(f"sub{i+1}".encode("ascii"))
        for j, node3 in enumerate(node2):
            assert node3.leaf == bytes(f"subsub{j+1}".encode("ascii"))


def test_Node_reversed(simple_data):
    node = yottadb.Node("^test4")
    i = 4
    for node2 in reversed(node):
        assert node2.leaf == bytes(f"sub{i-1}".encode("ascii"))
        j = 4
        for node3 in reversed(node2):
            assert node3.leaf == bytes(f"subsub{j-1}".encode("ascii"))
            j -= 1
        i -= 1


def test_Node_construction_errors():
    # Raise TypeError if attempt to create Node from a name that is not str or bytes
    with pytest.raises(TypeError) as terr:
        yottadb.Node(1)
    assert re.match("'name' must be an instance of str or bytes", str(terr.value))  # Confirm correct TypeError message

    # Raise TypeError if attempt to create Node from a subsarray that is not a list or tuple
    with pytest.raises(TypeError) as terr:
        yottadb.Node("^test1", "not a list or tuple")
    assert re.match("'subsarray' must be an instance of list or tuple", str(terr.value))  # Confirm correct TypeError message

    # Raise ValueError if attempt to create a Node with more than YDB_MAX_SUBS subscripts
    i = 0
    oldnode = yottadb.Node("mylocal")
    with pytest.raises(ValueError) as verr:
        while i < (yottadb.YDB_MAX_SUBS + 1):
            newnode = oldnode[str(i)]
            oldnode = newnode
            i += 1
    assert re.match("Cannot create Node with .* subscripts [(]max: .*[)]", str(verr.value))  # Confirm correct ValueError message

    # No error when constructing a Node on an Intrinsic Special Variable
    i = 0
    oldnode = yottadb.Node("$zyrelease")
    assert re.match("YottaDB r.*", oldnode.value.decode("utf-8"))


def test_Node___str__():
    assert str(yottadb.Node("test")) == "test"
    assert str(yottadb.Node("test")["sub1"]) == 'test("sub1")'
    assert str(yottadb.Node("test")["sub1"]["sub2"]) == 'test("sub1","sub2")'


def test_Node___repr__():
    node = yottadb.Node("var1")
    assert node == eval("yottadb." + repr(node))
    node = yottadb.Node("var1", ("sub1",))
    assert node == eval("yottadb." + repr(node))
    node = yottadb.Node("var1", ("sub1", "sub2", "sub3"))
    assert node == eval("yottadb." + repr(node))


def test_Node_get_value1(simple_data):
    assert yottadb.Node("^test1") == b"test1value"


def test_Node_get_value2(simple_data):
    assert yottadb.Node("^test2")["sub1"] == b"test2value"


def test_Node_get_value3(simple_data):
    assert yottadb.Node("^test3") == b"test3value1"
    assert yottadb.Node("^test3")["sub1"] == b"test3value2"
    assert yottadb.Node("^test3")["sub1"]["sub2"] == b"test3value3"


def test_Node_subscripts(simple_data):
    node = yottadb.Node("^test4", ("sub3",))
    for i, subscript in enumerate(node.subscripts):
        assert subscript == bytes(f"subsub{i+1}", encoding="utf-8")
        assert node[subscript].value == bytes(f"test4sub3subsub{i+1}", encoding="utf-8")


def test_Node_subsarray(simple_data):
    assert yottadb.Node("^test3").subsarray == []
    assert yottadb.Node("^test3")["sub1"].subsarray == ["sub1"]
    assert yottadb.Node("^test3")["sub1"]["sub2"].subsarray == ["sub1", "sub2"]
    # Confirm no UnboundLocalError when a Node has no subscripts
    for subscript in yottadb.Node("^test3").subscripts:
        pass


def test_Node_name(simple_data):
    assert yottadb.Node("^test3").name == "^test3"
    assert yottadb.Node("^test3")["sub1"].name == "^test3"
    assert yottadb.Node("^test3")["sub1"]["sub2"].name == "^test3"


def test_Node_set_value1():
    testnode = yottadb.Node("test4")
    testnode.value = "test4value"
    assert testnode == b"test4value"


def test_Node_set_value2():
    testnode = yottadb.Node("test5")["sub1"]
    testnode.value = "test5value"
    assert testnode == b"test5value"
    assert yottadb.Node("test5")["sub1"] == b"test5value"


def test_Node_set_value3():
    yottadb.Node("test5")["sub1"] = "test5value"
    assert yottadb.Node("test5")["sub1"] == b"test5value"


def test_Node_delete_node():
    testnode = yottadb.Node("test6")
    subnode = testnode["sub1"]
    testnode.value = "test6value"
    subnode.value = "test6 subvalue"

    assert testnode == b"test6value"
    assert subnode == b"test6 subvalue"

    testnode.delete_node()

    assert testnode.value is None
    assert subnode == b"test6 subvalue"


def test_Node_delete_tree():
    testnode = yottadb.Node("test7")
    subnode = testnode["sub1"]
    testnode.value = "test7value"
    subnode.value = "test7 subvalue"

    assert testnode == b"test7value"
    assert subnode == b"test7 subvalue"

    testnode.delete_tree()

    assert testnode.value is None
    assert subnode.value is None
    assert testnode.data == 0


def test_Node_data(simple_data):
    assert yottadb.Node("nodata").data == yottadb.YDB_DATA_UNDEF
    assert yottadb.Node("^test1").data == yottadb.YDB_DATA_VALUE_NODESC
    assert yottadb.Node("^test2").data == yottadb.YDB_DATA_NOVALUE_DESC
    assert yottadb.Node("^test2")["sub1"].data == yottadb.YDB_DATA_VALUE_NODESC
    assert yottadb.Node("^test3").data == yottadb.YDB_DATA_VALUE_DESC
    assert yottadb.Node("^test3")["sub1"].data == yottadb.YDB_DATA_VALUE_DESC
    assert yottadb.Node("^test3")["sub1"]["sub2"].data == yottadb.YDB_DATA_VALUE_NODESC

    # Confirm errors from C API are raised as YDBError exceptions
    with pytest.raises(yottadb.YDBError):
        yottadb.Node("^\x80").data


def test_Node_has_value(simple_data):
    assert not yottadb.Node("nodata").has_value
    assert yottadb.Node("^test1").has_value
    assert not yottadb.Node("^test2").has_value
    assert yottadb.Node("^test2")["sub1"].has_value
    assert yottadb.Node("^test3").has_value
    assert yottadb.Node("^test3")["sub1"].has_value
    assert yottadb.Node("^test3")["sub1"]["sub2"].has_value

    # Confirm errors from C API are raised as YDBError exceptions
    with pytest.raises(yottadb.YDBError):
        yottadb.Node("^\x80").has_value


def test_Node_has_subtree(simple_data):
    assert not yottadb.Node("nodata").has_subtree
    assert not yottadb.Node("^test1").has_subtree
    assert yottadb.Node("^test2").has_subtree
    assert not yottadb.Node("^test2")["sub1"].has_subtree
    assert yottadb.Node("^test3").has_subtree
    assert yottadb.Node("^test3")["sub1"].has_subtree
    assert not yottadb.Node("^test3")["sub1"]["sub2"].has_subtree

    # Confirm errors from C API are raised as YDBError exceptions
    with pytest.raises(yottadb.YDBError):
        yottadb.Node("^\x80").has_subtree


def test_Node_copy(new_db):
    node1 = yottadb.Node("init", ("sub1", "sub2"))
    node1.value = "1"
    node2 = node1.copy()
    assert node2.value == b"1"
    assert node2 == node1


def test_Node_mutate(new_db):
    node1 = yottadb.Node("init")
    node2 = node1.mutate("changed")
    assert node1.name == "init"
    assert node2.name == "changed"
    node1 = node2.mutate("init")
    assert node1.name == "init"
    assert node2.name == "init"
    assert node1 is node2

    node1 = yottadb.Node("init", ("sub1",))
    node2 = node1.mutate("sub2")
    assert node2.subsarray == ["sub2"]
    node1 = node2.mutate("sub3")
    assert node1.subsarray == ["sub3"]
    assert node2.subsarray == ["sub3"]
    assert node1 is node2

    node1 = yottadb.Node("init", ("sub1", "sub2"))
    node2 = node1.mutate("sub3")
    assert node2.subsarray == ["sub1", "sub3"]
    node1 = node2.mutate("sub4")
    assert node1.subsarray == ["sub1", "sub4"]
    assert node2.subsarray == ["sub1", "sub4"]
    assert node1 is node2

    node1 = yottadb.Node("init", ("sub1", "sub2", "sub3"))
    node2 = node1.mutate("sub4")
    assert node2.subsarray == ["sub1", "sub2", "sub4"]
    node1 = node2.mutate("sub5")
    assert node1.subsarray == ["sub1", "sub2", "sub5"]
    assert node2.subsarray == ["sub1", "sub2", "sub5"]
    assert node1 is node2


def test_Node_subscript_next(new_db):
    node1 = yottadb.Node("testsubsnext1")
    node2 = yottadb.Node("testsubsnext2")
    node3 = yottadb.Node("testsubsnext3")
    nodes = [node1, node2, node3]
    for node in nodes:
        node.value = "0"
        node["sub1"] = "1"
        node["sub2"] = "2"
        node["sub3"] = "3"
        node["sub4"] = "4"

    # Confirm Node.subscript_next() correctly iterates over variable names
    next_sub = node1.subscript_next()
    assert next_sub == b"testsubsnext2"
    node1A = node1.mutate(next_sub)
    assert node1A.subscript_next() == b"testsubsnext3"
    next_sub = node1A.subscript_next()
    assert next_sub == b"testsubsnext3"
    node1B = node1A.mutate(next_sub)
    with pytest.raises(yottadb.YDBNodeEnd):
        node1B.subscript_next()
    with pytest.raises(yottadb.YDBNodeEnd):
        node3.subscript_next()

    for node in nodes:
        # Confirm Node.subscript_next() returns the correct subscript for a given level
        next_sub = node[""].subscript_next()
        assert next_sub == b"sub1"
        assert node[next_sub].value == b"1"
        next_sub = node[next_sub].subscript_next()
        assert next_sub == b"sub2"
        assert node[next_sub].value == b"2"
        next_sub = node[next_sub].subscript_next()
        assert next_sub == b"sub3"
        assert node[next_sub].value == b"3"
        next_sub = node[next_sub].subscript_next()
        assert next_sub == b"sub4"
        assert node[next_sub].value == b"4"

        with pytest.raises(YDBNodeEnd):
            node[next_sub].subscript_next()

    # Confirm errors from C API are raised as YDBError exceptions
    with pytest.raises(yottadb.YDBError):
        yottadb.Node("^\x80").subscript_next()


def test_Node_subscript_previous(new_db):
    node1 = yottadb.Node("testsubsnext1")
    node2 = yottadb.Node("testsubsnext2")
    node3 = yottadb.Node("testsubsnext3")
    nodes = [node1, node2, node3]
    for node in nodes:
        node.value = "0"
        node["sub1"] = "1"
        node["sub2"] = "2"
        node["sub3"] = "3"
        node["sub4"] = "4"

    # Confirm Node.subscript_next() correctly iterates over variable names
    assert node3.subscript_previous() == b"testsubsnext2"
    assert node2.subscript_previous() == b"testsubsnext1"
    with pytest.raises(yottadb.YDBNodeEnd):
        node1.subscript_previous()

    for node in nodes:
        # Confirm Node.subscript_next() returns the correct subscript for a given level
        prev = node["sub4"].subscript_previous()
        assert prev == b"sub3"
        assert node[prev].value == b"3"
        prev = node[prev].subscript_previous()
        assert prev == b"sub2"
        assert node[prev].value == b"2"
        prev = node[prev].subscript_previous()
        assert prev == b"sub1"
        assert node[prev].value == b"1"

        with pytest.raises(YDBNodeEnd):
            node[prev].subscript_previous()

    # Confirm errors from C API are raised as YDBError exceptions
    with pytest.raises(yottadb.YDBError):
        yottadb.Node("^\x80").subscript_previous()


# transaction decorator smoke tests
@yottadb.transaction
def simple_transaction(node1: yottadb.Node, value1: str, node2: yottadb.Node, value2: str) -> None:
    node1.value = value1
    node2.value = value2

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
    test_base_node = yottadb.Node("^TransactionDecoratorTests")["smoke test 1"]
    test_base_node.delete_tree()
    node1 = test_base_node["node1"]
    value1 = b"v1"
    node2 = test_base_node["node2"]
    value2 = b"v2"
    assert node1.data == yottadb.YDB_DATA_UNDEF
    assert node2.data == yottadb.YDB_DATA_UNDEF

    try:
        assert yottadb.YDB_OK == simple_transaction(node1, value1, node2, value2)
        assert node1 == value1
        assert node2 == value2
    except YDBError as e:
        assert yottadb.YDB_ERR_INVVARNAME == e.code()

    test_base_node.delete_tree()


@yottadb.transaction
def simple_rollback_transaction(node1: yottadb.Node, value1: str, node2: yottadb.Node, value2: str) -> None:
    node1.value = value1
    node2.value = value2
    raise yottadb.YDBTPRollback("rolling back transaction.")


def test_transaction_smoke_test2(new_db) -> None:
    test_base_node = yottadb.Node("^TransactionDecoratorTests")["smoke test 2"]
    test_base_node.delete_tree()
    node1 = test_base_node["node1"]
    value1 = "v1"
    node2 = test_base_node["node2"]
    value2 = "v2"
    assert node1.data == yottadb.YDB_DATA_UNDEF
    assert node2.data == yottadb.YDB_DATA_UNDEF

    try:
        simple_rollback_transaction(node1, value1, node2, value2)
        assert False
    except yottadb.YDBTPRollback as e:
        print(str(e))
        assert str(e) == f"{yottadb.YDB_TP_ROLLBACK}, %YDB-TP-ROLLBACK: Transaction not committed."

    assert node1.data == yottadb.YDB_DATA_UNDEF
    assert node2.data == yottadb.YDB_DATA_UNDEF
    test_base_node.delete_tree()


@yottadb.transaction
def simple_restart_transaction(node1: yottadb.Node, node2: yottadb.Node, value: str, restart_tracker: yottadb.Node) -> None:
    if restart_tracker.data == yottadb.YDB_DATA_UNDEF:
        node1.value = value
        restart_tracker.value = "1"
        raise yottadb.YDBTPRestart("restating transaction")
    else:
        node2.value = value


def test_transaction_smoke_test3(new_db) -> None:
    test_base_global_node = yottadb.Node("^TransactionDecoratorTests")["smoke test 3"]
    test_base_local_node = yottadb.Node("TransactionDecoratorTests")["smoke test 3"]
    test_base_global_node.delete_tree()
    test_base_local_node.delete_tree()

    node1 = test_base_global_node["node1"]
    node2 = test_base_global_node["node2"]
    value = b"val"
    restart_tracker = test_base_local_node["restart tracker"]

    assert node1.data == yottadb.YDB_DATA_UNDEF
    assert node2.data == yottadb.YDB_DATA_UNDEF
    assert restart_tracker.data == yottadb.YDB_DATA_UNDEF

    simple_restart_transaction(node1, node2, value, restart_tracker)

    assert node1.value is None
    assert node2 == value
    assert restart_tracker == b"1"

    test_base_global_node.delete_tree()
    test_base_local_node.delete_tree()


def no_action() -> None:
    pass


def set_node(node: yottadb.Node, value: str) -> None:
    node.value = value


def conditional_set_node(node1: yottadb.Node, node2: yottadb.Node, value: str, traker_node: yottadb.Node) -> None:
    if traker_node.data != yottadb.DATA_NO_DATA:
        node1.value = value
    else:
        node2.value = value


def raise_standard_python_exception() -> None:
    1 / 0


class TransactionData(NamedTuple):
    action: Callable = no_action
    action_arguments: Tuple = ()
    restart_node: yottadb.Node = None
    return_value: int = yottadb._yottadb.YDB_OK


@yottadb.transaction
def process_transaction(nested_transaction_data: Tuple[TransactionData]) -> int:
    current_data = nested_transaction_data[0]
    current_data.action(*current_data.action_arguments)
    sub_data = nested_transaction_data[1:]
    if len(sub_data) > 0:
        process_transaction(sub_data)

    if current_data.return_value == yottadb._yottadb.YDB_TP_RESTART:
        if current_data.restart_node.data == yottadb.DATA_NO_DATA:
            current_data.restart_node.value = "restarted"
            return yottadb._yottadb.YDB_TP_RESTART
        else:
            return yottadb._yottadb.YDB_OK

    return current_data.return_value


def test_transaction_return_YDB_OK(new_db):
    node = yottadb.Node("^transactiontests")["test_transaction_return_YDB_OK"]
    value = b"return YDB_OK"
    transaction_data = TransactionData(action=set_node, action_arguments=(node, value), return_value=yottadb._yottadb.YDB_OK)

    node.delete_tree()
    assert node.value is None
    assert node.data == 0

    process_transaction((transaction_data,))
    assert node == value
    node.delete_tree()


def test_nested_transaction_return_YDB_OK(new_db):
    node1 = yottadb.Node("^transactiontests")["test_transaction_return_YDB_OK"]["outer"]
    value1 = b"return YDB_OK"
    outer_transaction = TransactionData(action=set_node, action_arguments=(node1, value1), return_value=yottadb._yottadb.YDB_OK)
    node2 = yottadb.Node("^transactiontests")["test_transaction_return_YDB_OK"]["inner"]
    value2 = b"neseted return YDB_OK"
    inner_transaction = TransactionData(action=set_node, action_arguments=(node2, value2), return_value=yottadb._yottadb.YDB_OK)

    process_transaction((outer_transaction, inner_transaction))

    assert node1 == value1
    assert node2 == value2
    node1.delete_tree()
    node2.delete_tree()


YDB_MAX_TP_DEPTH = 12


@pytest.mark.parametrize("depth", range(1, YDB_MAX_TP_DEPTH + 1))
def test_transaction_return_YDB_OK_to_depth(depth):
    def node_at_level(level: int) -> yottadb.Node:
        sub1 = f"test_transaction_return_YDB_OK_to_depth{depth}"
        sub2 = f"level{level}"
        return yottadb.Node("^tptests")[sub1][sub2]

    def value_at_level(level: int) -> bytes:
        return bytes(f"level{level} returns YDB_OK", encoding="utf-8")

    db = setup_db()

    transaction_data = []
    for level in range(1, depth + 1):
        transaction_data.append(TransactionData(action=set_node, action_arguments=(node_at_level(level), value_at_level(level))))

    process_transaction(transaction_data)

    for level in range(1, depth + 1):
        assert node_at_level(level) == value_at_level(level)

    sub1 = f"test_transaction_return_YDB_OK_to_depth{depth}"
    yottadb.Node("^tptests")[sub1].delete_tree()

    teardown_db(db)


# Test various lock functions and lock methods on Node object
def test_locks(new_db):
    cur_dir = os.getcwd()
    previous = set_ci_environment(cur_dir, cur_dir + "/tests/calltab.ci")

    ppid = os.getpid()
    ready_event = multiprocessing.Event()

    yottadb.lock()
    print("## Test 1: Node.lock_incr() increments the lock level for the given node object")
    print("# Run node.lock_incr() 10 times to increment lock level to 10")
    node = yottadb.Node("test1")["sub1"]["sub2"]
    for i in range(0, 10):
        node.lock_incr()
    print("# Confirm the lock level for the given node is 10")
    result = yottadb.ci("ShowLocks", has_retval=True)
    assert result == 'LOCK test1("sub1","sub2") LEVEL=10'
    print("## Test 2: Node.lock_incr() decrements the lock level for the given node object")
    print("# Run node.lock_decr() 10 times to decrement lock level from 10 to 0")
    for i in range(0, 10):
        node.lock_decr()
    result = yottadb.ci("ShowLocks", has_retval=True)
    print("# Confirm the lock level for the given node is 0")
    assert result == "LOCK LEVEL=0"

    print("## Test 3: yottadb.lock() and Node.lock() can be used to acquire and release the same locks")
    node = yottadb.Node("^test4")["sub1"]["sub2"]
    print("# Acquire a lock on the given node using Node.lock()")
    node.lock()
    print("# Attempt to increment the lock from another process")
    print("# Expect this to fail due to a YDBLockTimeoutError and")
    print("# the child process to exit with a code of 1")
    process = multiprocessing.Process(target=lock_value, args=(node, ppid, ready_event))
    process.start()
    process.join()
    assert process.exitcode == 1
    print("# Release all locks on the given node using yottadb.lock()")
    yottadb.lock()
    print("# Again attempt to increment/decrement the previously locked resource to confirm it was released")
    process = multiprocessing.Process(target=lock_value, args=(node, ppid, ready_event))
    process.start()
    ready_event.wait()
    print("# Send SIGINT to instruct child process to stop execution")
    os.kill(process.pid, signal.SIGINT)
    process.join()
    assert process.exitcode == 0
    print("# Clear the ready event for reuse in next subtest")
    ready_event.clear()

    print("## Test 4: Node.lock_incr() raises YDBLockTimeoutError when another")
    print("## process holds a lock for the given Node value.")
    print("# Create a child process to acquire and hold a lock on the given node")
    node = yottadb.Node("^test2")["sub1"]
    process = multiprocessing.Process(target=lock_value, args=(node, ppid, ready_event))
    process.start()
    print("# Wait for a signal from the child process to indicate that")
    print("# a lock is held and notify the parent to continue execution.")
    ready_event.wait()
    print("# Once the lock is held, try to acquire it from the parent process")
    print("# Expect a YDBLockTimeoutError since the child process holds the lock")
    with pytest.raises(yottadb.YDBLockTimeoutError):
        node.lock_incr()
    result = yottadb.ci("ShowLocks", has_retval=True)
    assert result == "LOCK LEVEL=0"
    print("# Send SIGINT to instruct child process to stop execution")
    os.kill(process.pid, signal.SIGINT)
    process.join()
    assert process.exitcode == 0

    reset_ci_environment(previous)


def test_YDB_ERR_TIME2LONG(new_db):
    t1 = yottadb.Node("^test1")
    t2 = yottadb.Node("^test2")["sub1"]
    t3 = yottadb.Node("^test3")["sub1"]["sub2"]
    nodes_to_lock = (t1, t2, t3)
    # Attempt to get locks for nodes t1,t2 and t3
    try:
        yottadb.lock(nodes=nodes_to_lock, timeout_nsec=(yottadb.YDB_MAX_TIME_NSEC + 1))
        assert False
    except YDBError as e:
        assert yottadb.YDB_ERR_TIME2LONG == e.code()


def test_YDB_ERR_PARMOFLOW(new_db):
    nodes_to_lock = []
    for i in range(0, 12):
        nodes_to_lock.append(yottadb.Node(f"^t{i}"))
    # Attempt to get locks for more names than supported, i.e. 11,
    # per https://docs.yottadb.com/MultiLangProgGuide/pythonprogram.html#python-lock.
    with pytest.raises(ValueError) as e:
        yottadb.lock(nodes=nodes_to_lock, timeout_nsec=(yottadb.YDB_MAX_TIME_NSEC + 1))
    assert re.match(
        "'nodes' argument invalid: invalid sequence length 12: max 11", str(e.value)
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
        yottadb.node_next(name="^test3", subsarray=("sub1", "sub2"))
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
    name = "^x"
    yottadb.delete_tree(name)
    yottadb.set(name, (b"\xa0",), "")
    node_subs = yottadb.node_next(name)
    yottadb.delete_tree(name)


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
    assert yottadb.subscript_next(name="^test1") == b"^test2"
    assert yottadb.subscript_next(name="^test2") == b"^test3"
    assert yottadb.subscript_next(name="^test3") == b"^test4"
    with pytest.raises(YDBNodeEnd):
        yottadb.subscript_next(name="^test7")

    subscript = yottadb.subscript_next(name="^test4", subsarray=("",))
    count = 1
    assert subscript == bytes(("sub" + str(count)).encode("ascii"))
    while True:
        count += 1
        try:
            subscript = yottadb.subscript_next(name="^test4", subsarray=(subscript,))
            assert subscript == bytes(("sub" + str(count)).encode("ascii"))
        except YDBNodeEnd:
            break

    assert yottadb.subscript_next(name="^test4", subsarray=("sub1", "")) == b"subsub1"
    assert yottadb.subscript_next(name="^test4", subsarray=("sub1", "subsub1")) == b"subsub2"
    assert yottadb.subscript_next(name="^test4", subsarray=("sub1", "subsub2")) == b"subsub3"
    with pytest.raises(YDBNodeEnd):
        yottadb.subscript_next(name="^test4", subsarray=("sub3", "subsub3"))

    # Test subscripts that include a non-UTF-8 character
    assert yottadb.subscript_next(name="^test7", subsarray=("",)) == b"sub1\x80"
    assert yottadb.subscript_next(name="^test7", subsarray=(b"sub1\x80",)) == b"sub2\x80"
    assert yottadb.subscript_next(name="^test7", subsarray=(b"sub2\x80",)) == b"sub3\x80"
    assert yottadb.subscript_next(name="^test7", subsarray=(b"sub3\x80",)) == b"sub4\x80"
    with pytest.raises(YDBNodeEnd):
        yottadb.subscript_next(name="^test7", subsarray=(b"sub4\x80",))


def test_module_subscript_previous(simple_data):
    assert yottadb.subscript_previous(name="^test1") == b"^Test5"
    assert yottadb.subscript_previous(name="^test2") == b"^test1"
    assert yottadb.subscript_previous(name="^test3") == b"^test2"
    assert yottadb.subscript_previous(name="^test4") == b"^test3"
    with pytest.raises(YDBNodeEnd):
        yottadb.subscript_previous(name="^Test5")

    subscript = yottadb.subscript_previous(name="^test4", subsarray=("",))
    count = 3
    assert subscript == bytes(("sub" + str(count)).encode("ascii"))
    while True:
        count -= 1
        try:
            subscript = yottadb.subscript_previous(name="^test4", subsarray=(subscript,))
            assert subscript == bytes(("sub" + str(count)).encode("ascii"))
        except YDBNodeEnd:
            break

    assert yottadb.subscript_previous(name="^test4", subsarray=("sub1", "")) == b"subsub3"
    assert yottadb.subscript_previous(name="^test4", subsarray=("sub1", "subsub2")) == b"subsub1"
    assert yottadb.subscript_previous(name="^test4", subsarray=("sub1", "subsub3")) == b"subsub2"
    with pytest.raises(YDBNodeEnd):
        yottadb.subscript_previous(name="^test4", subsarray=("sub3", "subsub1"))

    # Test subscripts that include a non-UTF-8 character
    assert yottadb.subscript_previous(name="^test7", subsarray=("",)) == b"sub4\x80"
    assert yottadb.subscript_previous(name="^test7", subsarray=(b"sub4\x80",)) == b"sub3\x80"
    assert yottadb.subscript_previous(name="^test7", subsarray=(b"sub3\x80",)) == b"sub2\x80"
    assert yottadb.subscript_previous(name="^test7", subsarray=(b"sub2\x80",)) == b"sub1\x80"
    with pytest.raises(YDBNodeEnd):
        yottadb.subscript_previous(name="^test7", subsarray=(b"sub1\x80",))


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

    names = [b"^Test5", b"^test1", b"^test2", b"^test3", b"^test4", b"^test6", b"^test7"]
    i = 0
    for subscript in yottadb.subscripts("^%"):
        assert subscript == names[i]
        i += 1
    assert len(names) == i

    i = 0
    rnames = list(reversed(names))
    for subscript in reversed(yottadb.subscripts("^z")):
        assert subscript == rnames[i]
        i += 1
    assert len(rnames) == i

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
                node = yottadb.Node("^x")
                node.set("")
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
    def callback(fruit1: yottadb.Node, value1: str, fruit2: yottadb.Node, value2: str, fruit3: yottadb.Node, value3: str) -> int:
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

    apples = yottadb.Node("fruits")["apples"]
    bananas = yottadb.Node("fruits")["bananas"]
    oranges = yottadb.Node("fruits")["oranges"]

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
    # by the given Node objects. If a YDBTPRestart is encountered, the function will retry the continue
    # attempting the increment operation until it succeeds.
    def callback(fruit1: yottadb.Node, fruit2: yottadb.Node, fruit3: yottadb.Node) -> int:
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
    def wrapper(function: Callable[..., object], args: Sequence[yottadb.Node]) -> int:
        return yottadb.tp(function, args=args)

    # Create nodes
    apples = yottadb.Node("^fruits")["apples"]
    bananas = yottadb.Node("^fruits")["bananas"]
    oranges = yottadb.Node("^fruits")["oranges"]
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


def test_Node_load_tree(simple_data):
    test4 = yottadb.Node("^test4")
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


def test_Node_save_tree(simple_data):
    test4 = yottadb.Node("^test4")
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

    test4_sub1 = yottadb.Node("^test4")["sub1"]
    test4_sub1_dict = test4_sub1.load_tree()

    test4_sub1.delete_tree()
    test4_sub1.save_tree(test4_sub1_dict)

    assert test4_sub1.value == b"test4sub1"
    assert test4_sub1["subsub1"].value == b"test4sub1subsub1"
    assert test4_sub1["subsub2"].value == b"test4sub1subsub2"
    assert test4_sub1["subsub3"].value == b"test4sub1subsub3"


def test_json_roundtrip_empty_object(new_db):
    node = yottadb.Node("emptyjson")
    node.save_json({})
    assert {} == node.load_json()


def test_deserialize_JSON(new_db):
    response = requests.get("https://rxnav.nlm.nih.gov/REST/relatedndc.json?relation=product&ndc=0069-3060")
    json_data = json.loads(response.content)
    node = yottadb.Node("^rxnav")
    node.save_json(json_data)
    loaded_json = node.load_json()
    # Confirm that the JSON that was stored in YDB and retrieved matches
    # the original source JSON. We assert on type to confirm that two
    # different objects and types of objects are being compared.
    assert type(loaded_json) == type(json_data)
    assert loaded_json == json_data

    response = requests.get("https://gitlab.com/api/v4/projects")
    json_data = json.loads(response.content)
    node = yottadb.Node("^v4")
    node.save_json(json_data)
    loaded_json = node.load_json()
    assert type(loaded_json) == type(json_data)
    assert loaded_json == json_data


def test_manipulate_JSON_in_place(new_db):
    response = requests.get("https://rxnav.nlm.nih.gov/REST/relatedndc.json?relation=product&ndc=0069-3060")
    original_json = json.loads(response.content)
    node = yottadb.Node("^rxnorm")
    node.delete_tree()
    node.save_json(original_json)

    saved_json = node.load_json()
    node["ndcInfoList"]["ndcInfo"]["3"]["ndc11"].value = b"00069306087"
    revised_json = node.load_json()
    assert revised_json["ndcInfoList"]["ndcInfo"][2]["ndc11"] != saved_json["ndcInfoList"]["ndcInfo"][2]["ndc11"]
    assert revised_json["ndcInfoList"]["ndcInfo"][2]["ndc11"] == "00069306087"
