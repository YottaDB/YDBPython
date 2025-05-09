#################################################################
#                                                               #
# Copyright (c) 2020-2021 Peter Goss All rights reserved.       #
#                                                               #
# Copyright (c) 2020-2025 YottaDB LLC and/or its subsidiaries.  #
# All rights reserved.                                          #
#                                                               #
#   This source code contains the intellectual property         #
#   of its copyright holder(s), and is made available           #
#   under a license.  If you do not know the terms of           #
#   the license, please stop and do not read further.           #
#                                                               #
#################################################################
"""
This file is for tests of the validation of input from Python. Most tests
are named by the function being called and the parameter that is having
its data validation tested.

Some background: originally the plan was to have all validation be done by
the underlying YottaDB C API, however a bug arose in the transition from
using 'int' to 'Py_size_t' that meant that length as well as type must be
validated (see documentation for
`test_unsigned_int_length_bytes_overflow()` below for additional detail).
Since we were needing to test the length anyway the decision was to make
that length equal to YottaDB's limitations. This also has the benefit of
making these types of input errors raise the normal 'TypeError' and
'ValueError' exceptions as is expected in Python.

Note: Many functions have "varname" and "subsarray" parameters which have
the same rules for valid input. Each of these functions are passed to
"varname_invalid" and "subsarray_invalid" for testing.
"""
import pytest
import _yottadb
import yottadb
import psutil
import os
import re

from conftest import set_ci_environment, reset_ci_environment


def varname_invalid(function):
    """
    Tests whether the function passed correctly validates variable names by
    verifying behavior under the following conditions:
        1) varname is of incorrect type (not str or bytes): Raise a TypeError
        2) length of varname > _yottadb.YDB_MAX_IDENT: Raise ValueError
        3) length of varname <= _yottadb.YDB_MAX_IDENT: No error

    :param function: Any function that takes "varname" as a parameter.
    """
    # Case 1: varname is not str or bytes: Raise TypeError
    with pytest.raises(TypeError):
        function(varname=1)

    # Case 2: length of varname > _yottadb.YDB_MAX_IDENT: Raise ValueError
    with pytest.raises(ValueError):
        function(varname="b" * (_yottadb.YDB_MAX_IDENT + 1))

    # Case 3: length of varname == _yottadb.YDB_MAX_IDENT: No ValueError
    try:
        if "get" == function.__name__:
            # If testing _yottadb.get(), avoid an LVUNDEF by setting the node before
            # the call, since this test is intended to confirm that the maximum
            # variable name length is accepted.
            _yottadb.set(varname="b" * (_yottadb.YDB_MAX_IDENT), value="val")
        function(varname="b" * (_yottadb.YDB_MAX_IDENT))
    except _yottadb.YDBNodeEnd:  # Testing C-extention's validation, not YDB's
        pass

    # Case 4: A non-ASCII character is used in a variable name: Propagate error from YDB to caller via YDBError
    with pytest.raises(_yottadb.YDBError):
        function(varname="\x80")  # str case
    with pytest.raises(_yottadb.YDBError):
        function(varname=b"\x80")  # bytes case


def subsarray_invalid(function):
    """
    Tests whether the function passed does correct validation of a list of
    subscripts, i.e. a subsarray. A subsarray must be a non-bytes-like
    sequence, e.g. a list or tuple, of str objects. This means that str and
    bytes, though technically sequences, should not be accepted.

    The following conditions are tested:
        1) Subsarray parameter is not a valid sequence: Raise TypeError
        2) Sequence length == _yottadb.YDB_MAX_SUBS: No Error
        3) Sequence length > _yottadb.YDB_MAX_SUBS: Raise ValueError
        4) Any value in the sequence is not of type str: Raise TypeError
        5) Length of str object == _yottadb.YDB_MAX_STR: No Error
        6) Length of str object > _yottadb.YDB_MAX_STR: Raise ValueError

    :param function: A function that takes "varname" and "subsarray" parameters.
    """
    # Case 1: subsarray is not a valid sequence: raise TypeError
    with pytest.raises(TypeError):
        function(varname="test", subsarray="this is the wrong kind of sequence")

    # Case 2: subsarray length == _yottadb.YDB_MAX_SUBS: no error
    try:
        if "get" == function.__name__:
            # If testing _yottadb.get(), avoid an LVUNDEF by setting the node before
            # the call, since this test is intended to confirm that the maximum
            # variable name length is accepted.
            _yottadb.set(varname="test", subsarray=("b",) * (_yottadb.YDB_MAX_SUBS), value="val")
        function(varname="test", subsarray=("b",) * (_yottadb.YDB_MAX_SUBS))
    except _yottadb.YDBNodeEnd:  # Testing C-extention's validation, not YottaDB's
        pass

    # Case 3: subsarray length > _yottadb.YDB_MAX_SUBS: raise ValueError
    with pytest.raises(ValueError):
        function(varname="test", subsarray=("b",) * (_yottadb.YDB_MAX_SUBS + 1))

    # Case 4: items in subsarray not of type str: raise TypeError
    with pytest.raises(TypeError):
        function(varname="test", subsarray=(1,))

    # Case 5: str length == _yottadb.YDB_MAX_STR: no error
    try:
        if "get" == function.__name__:
            # If testing _yottadb.get(), avoid an LVUNDEF by setting the node before
            # the call, since this test is intended to confirm that the maximum
            # variable name length is accepted.
            _yottadb.set(varname="test", subsarray=("b" * (_yottadb.YDB_MAX_STR),), value="val")
        function("test", ("b" * (_yottadb.YDB_MAX_STR),))
    except _yottadb.YDBNodeEnd:  # Node iterations will complete if no other error, so accept that case here
        pass
    except _yottadb.YDBError as e:
        if _yottadb.YDB_ERR_LOCKSUB2LONG == e.code():
            # Subscripts for lock functions are shorter, and so will raise an error for subscripts with lengths > 255,
            # so accept that case here.
            pass
        else:
            raise e

    # Case 6: str length > _yottadb.YDB_MAX_STR: raise ValueError
    with pytest.raises(ValueError):
        function("test", ("b" * (_yottadb.YDB_MAX_STR + 1),))


# data()
def test_data_varname():
    varname_invalid(_yottadb.data)


def test_data_subsarray():
    subsarray_invalid(_yottadb.data)


# delete()
def test_delete_varname():
    varname_invalid(_yottadb.delete)


def test_delete_subsarray():
    subsarray_invalid(_yottadb.delete)


# delete_except()
def test_delete_except_varnames():
    """
    This function tests the validation of the delete_except function's varnames parameter.
    It tests that the delete_except function:
        1) Raises a TypeError if the varnames parameter is not a proper Sequence (list or tuple)
        2) Raises a TypeError if the contents of the varname list or tuple is not a str object
        3) Accepts up to _yottadb.YDB_MAX_NAMES without raising an exception
        4) Raise a ValueError if varnames is longer than _yottadb.YDB_MAX_NAMES
        5) Accept item in varnames up to _yottadb.YDB_MAX_IDENT without raising exception
        6) Raises a ValueError if an item in the varnames list or tuple is longer than _yottadb.YDB_MAX_IDENT
    """
    # Case 1: Raises a TypeError if varnames is not a list or tuple
    with pytest.raises(TypeError):
        _yottadb.delete_except(varnames="not a sequence")

    # Case 2: Raises a TypeError if the contents of the varname list or tuple is not a str object
    with pytest.raises(TypeError):
        _yottadb.delete_except(varnames=(1,))

    # Case 3: Accepts up to _yottadb.YDB_MAX_NAMES without raising an exception
    _yottadb.delete_except(varnames=["test" + str(x) for x in range(0, _yottadb.YDB_MAX_NAMES)])

    # Case 4: Raise a ValueError if varnames is longer than _yottadb.YDB_MAX_NAMES
    with pytest.raises(ValueError):
        _yottadb.delete_except(varnames=["test" + str(x) for x in range(0, _yottadb.YDB_MAX_NAMES + 1)])

    # Case 5: Accept item in varnames up to _yottadb.YDB_MAX_IDENT without raising exception
    _yottadb.delete_except(varnames=["b" * (_yottadb.YDB_MAX_IDENT)])

    # Case 6: Raises a ValueError if an item in the varnames list or tuple is longer than _yottadb.YDB_MAX_IDENT
    with pytest.raises(ValueError):
        _yottadb.delete_except(varnames=["b" * (_yottadb.YDB_MAX_IDENT + 1)])


# get()
def test_get_varname():
    varname_invalid(_yottadb.get)


def test_get_subsarray():
    subsarray_invalid(_yottadb.get)


# incr()
def test_incr_varname():
    varname_invalid(_yottadb.incr)


def test_incr_subsarray():
    subsarray_invalid(_yottadb.incr)


def test_incr_increment():
    """
    This function tests the validation of the incr function's increment parameter.
    It tests that the incr function:
        1) Raises a TypeError if the value that is passed to it is not a str object
        2) Raises a ValueError if the value is longer than _yottadb.YDB_MAX_STR
    """
    node = {"varname": "test", "subsarray": ("b",)}
    # Case 1: Raises a TypeError if the value that is passed to it is not a str object
    with pytest.raises(TypeError):
        _yottadb.incr(**node, increment=1)

    # Case 2: Raises a ValueError if the value is longer than _yottadb.YDB_MAX_STR
    with pytest.raises(ValueError):
        _yottadb.incr(**node, increment="1" * (_yottadb.YDB_MAX_STR + 1))


# lock()
"""
This function tests the lock function`s node parameter.
It tests that the lock function:
    1)  Raises a Type Error if the value is a not a list or tuple.
    2)  Accepts a list of nodes as long as _yottadb.YDB_LOCK_MAX_NODES without raising a exception
    3)  Raises a ValueError if the list passed to it is longer than _yottadb.YDB_LOCK_MAX_NODES
    4)  Raises a TypeError if the first element of a node is not a str object
    5)  Raises a ValueError if a node doesn't have any element
    6)  Raise a ValueError if a node has more than 2 elements
    7)  Raises a TypeError if the first element of a node (representing a
    varname) is not a str object
    8)  The first element of a node (varname) may be up to _yottadb.YDB_MAX_IDENT in length without raising an exception
    9)  Raises a TypeError if the second element of a node is not a list or tuple
    10) Accepts a subsarray list of str up to _yottadb.YDB_MAX_SUBS without raising an exception
    11) Raises a ValueError if a subsarray is longer than _yottadb.YDB_MAX_SUBS
    12) Raises a TypeError if an element of a subsarray is not a str object
    13) Accepts an item in a subsarray of length _yottadb.YDB_MAX_STR without raising an exception
    14) Raises a Value Error if a subsarray has an element that is longer than _yottadb.YDB_MAX_STR
    """


def test_lock_YDBError():
    # Case 0: Raises a YDBError if the underlying ydb_lock_s() call fails for some reason,
    # e.g. with a bad variable name
    with pytest.raises(yottadb.YDBError) as e:
        _yottadb.lock((("\x80", ()),))
        assert yottadb.YDB_ERR_INVVARNAME == e.code()


def test_lock_typeerror():
    # Case 1: Raises a Type Error if the value is a not a list or tuple.
    with pytest.raises(TypeError) as e:
        _yottadb.lock("not list or tuple")
    print(e)
    assert re.match("'nodes' argument invalid: node must be list or tuple.", str(e.value))  # Confirm correct TypeError message


def test_lock_max_nodes_ok(new_db):
    # Case 2: Accepts a list of nodes as long as _yottadb.YDB_LOCK_MAX_NODES without raising a exception
    nodes = [["test" + str(x)] for x in range(0, _yottadb.YDB_LOCK_MAX_NODES)]
    _yottadb.lock(nodes)


def test_lock_too_many_nodes():
    # Case 3: Raises a ValueError if the list passed to it is longer than _yottadb.YDB_LOCK_MAX_NODES
    with pytest.raises(ValueError):
        nodes = [["test" + str(x)] for x in range(0, _yottadb.YDB_LOCK_MAX_NODES + 1)]
        _yottadb.lock(nodes)


def test_lock_first_node_wrong_type():
    # Case 4: Raises a type Error if the first element of a node is not a str object
    with pytest.raises(TypeError):
        _yottadb.lock([1])


def test_lock_empty_node():
    # Case 5: Raises a ValueError if a node doesn't have any element
    with pytest.raises(ValueError):
        _yottadb.lock(([],))


def test_lock_too_many_node_args():
    # Case 6: Raise a ValueError if a node has more than 2 elements
    with pytest.raises(ValueError):
        _yottadb.lock((["varname", ["subscript"], "extra"],))  # too many


def test_lock_varname_wrong_type():
    # Case 7: Raises a TypeError if the first element of a node (representing a
    # varname) is not a str object
    with pytest.raises(TypeError):
        _yottadb.lock(((1,),))


def test_lock_varname_max_ident(new_db):
    # Case 8: The first element of a node (varname) may be up to _yottadb.YDB_MAX_IDENT in length without raising an exception
    _yottadb.lock((("a" * (_yottadb.YDB_MAX_IDENT),),))
    try:
        _yottadb.lock((("a" * (_yottadb.YDB_MAX_IDENT + 1),),))
        assert False
    except yottadb.YDBError as e:
        assert _yottadb.YDB_ERR_VARNAME2LONG == e.code()


def test_lock_subsarray_wrong_type():
    # Case 9: Raises a TypeError if the second element of a node is not a list or tuple
    with pytest.raises(TypeError):
        _yottadb.lock((("test", "not list or tuple"),))


def test_lock_subsarray_max_subs(new_db):
    # Case 10: Accepts a subsarray list of str up to _yottadb.YDB_MAX_SUBS without raising an exception
    subsarray = ["test" + str(x) for x in range(0, _yottadb.YDB_MAX_SUBS)]
    _yottadb.lock((("test", subsarray),))


def test_lock_too_many_subs():
    # Case 11: Raises a ValueError if a subsarray is longer than _yottadb.YDB_MAX_SUBS
    with pytest.raises(ValueError):
        subsarray = ["test" + str(x) for x in range(0, _yottadb.YDB_MAX_SUBS + 1)]
        node = ("test", subsarray)
        _yottadb.lock([node])


def test_lock_subscript_wrong_type():
    # Case 12: Raises a TypeError if an element of a subsarray is not a str object
    with pytest.raises(TypeError):
        _yottadb.lock((("test", [1]),))


def test_lock_max_subscript_length():
    # Case 13: Accepts an item in a subsarray of length _yottadb.YDB_MAX_STR without raising an exception
    try:
        _yottadb.lock((("test", ["a" * (_yottadb.YDB_MAX_STR)]),))
        assert False
    except _yottadb.YDBError:  # Testing C-extention's validation, not YottaDB's
        pass


def test_lock_nodes_case14():
    # Case 14: Raises a Value Error if a subsarray has an element that is longer than _yottadb.YDB_MAX_STR
    with pytest.raises(ValueError):
        _yottadb.lock((("test", ["a" * (_yottadb.YDB_MAX_STR + 1)]),))


# lock_decr()
def test_decr_varname(new_db):
    varname_invalid(_yottadb.lock_decr)


def test_decr_subsarray(new_db):
    subsarray_invalid(_yottadb.lock_decr)


# lock_incr()
def test_lock_incr_varname(new_db):
    varname_invalid(_yottadb.lock_incr)


def test_lock_incr_subsarray(new_db):
    subsarray_invalid(_yottadb.lock_incr)


# node_next()
def test_node_next_varname():
    varname_invalid(_yottadb.node_next)


def test_node_next_subsarray():
    subsarray_invalid(_yottadb.node_next)


# node_previous()
def test_node_previous_varname():
    varname_invalid(_yottadb.node_previous)


def test_node_previous_subsarray():
    subsarray_invalid(_yottadb.node_previous)


# set()
def test_set_varname():
    varname_invalid(_yottadb.set)


def test_set_subsarray():
    subsarray_invalid(_yottadb.set)


def test_set_value():
    """
    This function tests the validation of the set function's value parameter.
    It tests that the set function:
        1) Raises a TypeError if the value that is passed to it is not a str object
        2) Accepts a value up to _yottadb.YDB_MAX_STR in length without raising an exception
        3) Raises a ValueError if the value is longer than _yottadb.YDB_MAX_STR
    """
    node = {"varname": "test", "subsarray": ("b",)}
    # Case 1: Raises a TypeError if the value that is passed to it is not a str object
    with pytest.raises(TypeError):
        _yottadb.set(**node, value=1)

    # Case 2: Accepts a value up to _yottadb.YDB_MAX_STR in length without raising an exception
    _yottadb.set(**node, value="b" * (_yottadb.YDB_MAX_STR))

    # Case 3: Raises a ValueError if the value is longer than _yottadb.YDB_MAX_STR
    with pytest.raises(ValueError):
        _yottadb.set(**node, value="b" * (_yottadb.YDB_MAX_STR + 1))


# str2zwr()
def test_str2zwr_input():
    """
    This function tests the validation of the str2zwr function's input parameter.
    It tests that the str2zwr function:
        1) Raises a TypeError if input is not of type str
        2) Accepts a value up to _yottadb.YDB_MAX_STR without raising an exception
        3) Raises a ValueError if input is longer than _yottadb.YDB_MAX_STR
    """
    # Case 1: Raises a TypeError if input is not of type str
    with pytest.raises(TypeError):
        _yottadb.str2zwr(1)

    # Case 2: Accepts a value up to _yottadb.YDB_MAX_STR - 2 without raising an exception
    # The - 2 is necessary since ydb_str2zwr() introduces 2 double-quote characters at the
    # start and end of the string. Other Simple API functions don't do this, so YDB_MAX_STR
    # is used in those cases.
    _yottadb.str2zwr("b" * (_yottadb.YDB_MAX_STR - 2))

    # Case 3: Raises a ValueError if input is longer than _yottadb.YDB_MAX_STR
    with pytest.raises(ValueError):
        _yottadb.str2zwr("b" * (_yottadb.YDB_MAX_STR + 1))


# subscript_next()
def test_subscript_next_varname():
    varname_invalid(_yottadb.subscript_next)


def test_subscript_next_subsarray():
    subsarray_invalid(_yottadb.subscript_next)


# subscript_previous()
def test_subscript_previous_varname():
    varname_invalid(_yottadb.subscript_previous)


def test_subscript_previous_subsarray():
    subsarray_invalid(_yottadb.subscript_previous)


# tp()
def simple_transaction() -> None:
    """
    A simple callback for testing the tp function that does nothing and returns _yottadb.YDB_OK
    """
    return _yottadb.YDB_OK


def callback_that_returns_wrong_type():
    """
    A simple callback for testing the tp function that returns the wrong type.
    """
    return "not an int"


def test_tp_callback_not_a_callable():
    """
    Test that tp() raises a TypeError when the callback parameter is not callable.
    """
    with pytest.raises(TypeError):
        _yottadb.tp(callback="not a callable")


def test_tp_callback_return_wrong_type(new_db):
    """
    Tests that tp() raises TypeError when a callback returns the wrong type.
    """
    with pytest.raises(TypeError):
        _yottadb.tp(callback_that_returns_wrong_type)


def test_tp_args():
    """
    Tests that tp() raises TypeError if the args parameter is not a list or tuple
    """
    with pytest.raises(TypeError):
        _yottadb.tp(callback=simple_transaction, args="not a sequence of arguments")


def test_tp_kwargs():
    """
    Tests that tp() raises a TypeError if the kwargs parameter is not a dictionary
    """
    with pytest.raises(TypeError):
        _yottadb.tp(callback=simple_transaction, kwargs="not a dictionary of keyword arguments")


def test_tp_varnames(new_db):
    """
    This function tests the validation of the tp function's varnames parameter.
    It tests that the tp function:
        1) Raises a TypeError if the varnames parameter is not a proper Sequence (list or tuple)
        2) Raises a TypeError if the contents of the varname list or tuple is
            not a str object
        3) Accepts up to _yottadb.YDB_MAX_NAMES without raising an exception
        4) Raise a ValueError if varnames is longer than _yottadb.YDB_MAX_NAMES
        5) Accept item in varnames up to _yottadb.YDB_MAX_IDENT without raising exception
        6) Raises a ValueError if an item in the varnames list or tuple is longer than _yottadb.YDB_MAX_IDENT
    """
    # Case 1: Raises a TypeError if the varnames parameter is not a proper Sequence (list or tuple)
    with pytest.raises(TypeError):
        _yottadb.tp(callback=simple_transaction, varnames="not a sequence")

    # Case 2: Raises a TypeError if the contents of the varname list or tuple is not a str object
    with pytest.raises(TypeError):
        _yottadb.tp(callback=simple_transaction, varnames=(1,))

    # Case 3: Accepts up to _yottadb.YDB_MAX_NAMES without raising an exception
    varnames = ["test" + str(x) for x in range(0, _yottadb.YDB_MAX_NAMES)]
    _yottadb.tp(callback=simple_transaction, varnames=varnames)

    # case 4: Raise a ValueError if varnames is longer than _yottadb.YDB_MAX_NAMES
    varnames = ["test" + str(x) for x in range(0, _yottadb.YDB_MAX_NAMES + 1)]
    with pytest.raises(ValueError):
        _yottadb.tp(callback=simple_transaction, varnames=varnames)

    # Case 5: Accept item in varnames up to _yottadb.YDB_MAX_IDENT without raising exception
    _yottadb.tp(callback=simple_transaction, varnames=["b" * (_yottadb.YDB_MAX_IDENT)])

    # Case 6: Raises a ValueError if an item in the varnames list or tuple is longer than _yottadb.YDB_MAX_IDENT
    with pytest.raises(ValueError):
        _yottadb.tp(callback=simple_transaction, varnames=["b" * (_yottadb.YDB_MAX_IDENT + 1)])


# zwr2str()
def test_zwr2str_input():
    """
    This function tests the validation of the zwr2str function's input parameter.
    It tests that the zwr2str function:
        1) Raises a TypeError if input is not of type str
        2) Accepts a value up to _yottadb.YDB_MAX_STR without raising an exception
        3) Raises a ValueError if input is longer than _yottadb.YDB_MAX_STR
    """
    # Case 1: Raises a TypeError if input is not of type str
    with pytest.raises(TypeError):
        _yottadb.zwr2str(1)

    # Case 2: Accepts a value up to _yottadb.YDB_MAX_STR - 2 without raising an exception
    # The - 2 is necessary since ydb_zwr2str() introduces 2 double-quote characters at the
    # start and end of the string. Other Simple API functions don't do this, so YDB_MAX_STR
    # is used in those cases.
    _yottadb.zwr2str("b" * _yottadb.YDB_MAX_STR)

    # Case 3: Raises a ValueError if input is longer than _yottadb.YDB_MAX_STR
    with pytest.raises(ValueError):
        _yottadb.zwr2str("b" * (_yottadb.YDB_MAX_STR + 1))


def test_ci_input():
    """
    This function tests the validation of the ci call-in function's input
    parameters. Specifically, it tests that _yottadb.ci():
        1) Raises a TypeError if no routine name is provided
        2) Raises a TypeError if the routine name is not str or bytes
        3) Raises a TypeError if the arguments passed don't match the routine parameters
    """
    cur_dir = os.getcwd()
    previous = set_ci_environment(cur_dir, cur_dir + "/tests/calltab.ci")

    # Raise TypeError when argument list is immutable,
    # but routine includes output arguments
    with pytest.raises(TypeError):
        _yottadb.ci("HelloWorld2", (1, 2, 3), has_retval=True)
    with pytest.raises(TypeError):
        _yottadb.ci("NoRet", (1,))

    # Raise YDB_ERR_INVSTRLEN when has_retval doesn't match the call-in table
    # NOTE: This test is disabled as it may raise a garbage value and fail
    # when using a Debug build of YottaDB due to imprecision in how YDBPython
    # handles an incorrect setting for has_retval, that is, when this option
    # does not reflect the call-in table entry for the given routine. This problem is
    # tracked by YDBPython#24. This tests may be re-enabled and this comment removed
    # when that issue is resolved.
    try:
        # yottadb.ci("HelloWorld2", ["1", "24", "3"], has_retval=False)
        pass
    except yottadb.YDBError as e:
        assert _yottadb.YDB_ERR_INVSTRLEN == e.code()

    # Raise TypeError when arguments not passed as a Sequence
    with pytest.raises(TypeError):
        _yottadb.ci()
    with pytest.raises(TypeError):
        _yottadb.ci(1)
    with pytest.raises(TypeError):
        _yottadb.ci("HelloWorld2", "123", has_retval=True)
    with pytest.raises(TypeError):
        _yottadb.ci("HelloWorld2", b"123", has_retval=True)
    with pytest.raises(TypeError):
        _yottadb.ci("HelloWorld2", 1, has_retval=True)
    with pytest.raises(TypeError):
        _yottadb.ci("HelloWorld2", 1, 2, has_retval=True)

    # Raise ValueError when arguments don't match the call-in table
    with pytest.raises(ValueError):
        _yottadb.ci("HelloWorld1", (1, 2, 3), has_retval=True)
    with pytest.raises(ValueError):
        _yottadb.ci("HelloWorld2")
    with pytest.raises(ValueError):
        _yottadb.ci("HelloWorld2", ("1",))
    with pytest.raises(ValueError):
        _yottadb.ci("HelloWorld2", ("1", "2"))
    with pytest.raises(ValueError):
        _yottadb.ci("HelloWorld2", ("1", "2", 3, 4))
    with pytest.raises(ValueError):
        _yottadb.ci("HelloWorld2", has_retval=True)
    with pytest.raises(ValueError):
        _yottadb.ci("HelloWorld2", ("1",), has_retval=True)
    with pytest.raises(ValueError):
        _yottadb.ci("HelloWorld2", ("1", "2"), has_retval=True)
    with pytest.raises(ValueError):
        _yottadb.ci("HelloWorld2", ("1", "2", 3, 4), has_retval=True)

    # Raise ValueError when an invalid call-in table filename is specified
    with pytest.raises(ValueError):
        _yottadb.open_ci_table("")

    # Ensure YDBError raised for failure in underlying C call, e.g.
    # YDB_ERR_CITABOPN or YDB_ERR_PARAMINVALID
    try:
        _yottadb.open_ci_table("\x80")
        assert False
    except yottadb.YDBError as e:
        assert yottadb.YDB_ERR_CITABOPN == e.code()
    try:
        _yottadb.switch_ci_table(-1)
        assert False
    except yottadb.YDBError as e:
        assert yottadb.YDB_ERR_PARAMINVALID == e.code()

    reset_ci_environment(previous)


# This test requires a lot of memory and will fail if there is not enough memory on the system that running
# the tests so this test will be skipped if the available memory is less than
# (2 ** 32) str.
@pytest.mark.skipif(psutil.virtual_memory().available < (2**32), reason="not enough memory for this test.")
def test_unsigned_int_length_bytes_overflow():
    """
    Python bytes objects may have more bytes than can be represented by a 32-bit unsigned integer.
    Prior to validation a length that was 1 more than that length would act as if it was only a 0-byte
    long bytes object. This tests all scenarios where that could happen and that when that happens the
    function will raise a ValueError instead of continuing as if a single byte was passed to it.
    """
    BYTES_LONGER_THAN_UNSIGNED_INT_IN_LENGTH = "1" * (2**32)  # works for python 3.8/Ubuntu 20.04
    varname_subsarray_functions = (
        _yottadb.data,
        _yottadb.delete,
        _yottadb.get,
        _yottadb.incr,
        _yottadb.lock_decr,
        _yottadb.lock_incr,
        _yottadb.node_next,
        _yottadb.node_previous,
        _yottadb.set,
        _yottadb.subscript_next,
        _yottadb.subscript_previous,
    )
    for function in varname_subsarray_functions:
        with pytest.raises(ValueError):
            function(varname=BYTES_LONGER_THAN_UNSIGNED_INT_IN_LENGTH)

        with pytest.raises(ValueError):
            function("test", (BYTES_LONGER_THAN_UNSIGNED_INT_IN_LENGTH,))

    node = {"varname": "test", "subsarray": ("b",)}
    with pytest.raises(ValueError):
        _yottadb.incr(**node, increment=BYTES_LONGER_THAN_UNSIGNED_INT_IN_LENGTH)

    with pytest.raises(ValueError):
        _yottadb.set(**node, value=BYTES_LONGER_THAN_UNSIGNED_INT_IN_LENGTH)

    with pytest.raises(ValueError):
        _yottadb.str2zwr(BYTES_LONGER_THAN_UNSIGNED_INT_IN_LENGTH)

    with pytest.raises(ValueError):
        _yottadb.zwr2str(BYTES_LONGER_THAN_UNSIGNED_INT_IN_LENGTH)


# Confirm validation exceptions produce the correct error message
def test_validation_exception_message(simple_data):
    t1 = yottadb.Node("^test1")
    t2 = yottadb.Node("^test2")["sub1"]
    t3 = yottadb.Node("^test3")["sub1"]["b" * (_yottadb.YDB_MAX_STR + 1)]
    nodes_to_lock = (t1, t2, t3)
    with pytest.raises(ValueError):
        yottadb.lock(nodes_to_lock)

    try:
        yottadb.lock(nodes_to_lock)
        assert False
    except ValueError as e:
        assert (
            str(e)
            == "'nodes' argument invalid: item 2 in node sequence has invalid subsarray: invalid bytes length 1048577: max 1048576"
        )
