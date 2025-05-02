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
"""
YDBPython.

YDBPython provides a Pythonic API for accessing YottaDB databases.
"""

__version__ = "1.1.1"
__author__ = "YottaDB LLC"
__credits__ = "Peter Goss"

from typing import Optional, List, Union, Generator, AnyStr, Any, Callable, NewType, Tuple, Mapping
import copy
import struct
from builtins import property
import sys, os

# Need to do future proof name resolution as the encryption plugin tries to
# resolve symbols in libyottadb.so, and cannot find them unless RTLD_GLOBAL is
# set
sys.setdlopenflags(sys.getdlopenflags() | os.RTLD_GLOBAL)
import _yottadb
from _yottadb import *


# Create Type objects for each custom type used in this module
# for use in type annotations
Key = NewType("Key", object)
SubscriptsIter = NewType("SubscriptsIter", object)
NodesIter = NewType("NodesIter", object)

# Get the maximum number of arguments accepted by ci()/cip()
# based on whether the CPU architecture is 32-bit or 64-bit
arch_bits = 8 * struct.calcsize("P")
max_ci_args = 34 if 64 == arch_bits else 33


# Get the YottaDB numeric error code for the given
# YDBError by extracting it from the exception message.
def get_error_code(YDBError):
    if type(YDBError.args[0]) is bytes:
        error_code = int(YDBError.args[0].split(b",")[0])  # Extract error code from between parentheses in error message
    else:
        error_code = int(YDBError.args[0].split(",")[0])  # Extract error code from between parentheses in error message
    if 0 < error_code:
        error_code *= -1  # Multiply by -1 for conformity with negative YDB error codes
    return error_code


# Note that the following setattr() call is done due to how the PyErr_SetObject()
# Python C API function works. That is, this function calls the constructor of a
# specified Exception type, in this case YDBError, and sets Python's
# internal error indicator causing the exception mechanism to fire and
# raise an exception visible at the Python level. Since both of these things
# are done by this single function, there is no opportunity for the calling
# C code to modify the created YDBError object instance and append the YDB
# error code.
#
# Moreover, it is not straightforward (and perhaps not possible) to define
# a custom constructor for custom exceptions defined in C code, e.g. YDBError.
# Such might allow for an error code integer to be set on the YDBError object
# when it is created by PyErr_SetObject() without the need for returning control
# to the calling C code to update the object.
#
# Attach error code lookup function to the YDBError class
# as a method for convenience.
setattr(YDBError, "code", get_error_code)


def adjust_stdout_stderr() -> None:
    """
    Check whether stdout (file descriptor 1) and stderr (file descriptor 2) are the same file, and if so,
    route stderr writes to stdout instead. This ensures that output appears in the order in which it was written.
    Otherwise, owing to I/O buffering, output can appear in an order different from that in which it was written.

    Application code which mixes Python and M code, and which explicitly redirects stdout or stderr
    (e.g. by modifying sys.stdout or sys.stderr), should call this function as soon as possible after the redirection.

    :returns: None
    """
    return _yottadb.adjust_stdout_stderr()


def get(varname: AnyStr, subsarray: Tuple[AnyStr] = ()) -> Optional[bytes]:
    """
    Retrieve the value of the local or global variable node specified by the `varname` and `subsarray` pair.

    :param varname: A bytes-like object representing a YottaDB local or global variable name.
    :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
    :returns: If the specified node has a value, returns it as a bytes object. If not, returns None.
    """
    try:
        return _yottadb.get(varname, subsarray)
    except YDBError as e:
        ecode = e.code()
        if _yottadb.YDB_ERR_LVUNDEF == ecode or _yottadb.YDB_ERR_GVUNDEF == ecode:
            return None
        else:
            raise e


def set(varname: AnyStr, subsarray: Tuple[AnyStr] = (), value: AnyStr = "") -> None:
    """
    Set the local or global variable node specified by the `varname` and `subsarray` pair.

    :param varname: A bytes-like object representing a YottaDB local or global variable name.
    :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
    :param value: A bytes-like object representing the value of a YottaDB local or global variable node.
    :returns: None.
    """
    _yottadb.set(varname, subsarray, value)
    return None


def ci(routine: AnyStr, args: Tuple[Any] = (), has_retval: bool = False) -> Any:
    """
    Call an M routine specified in a YottaDB call-in table using the specified arguments, if any.
    If the routine has a return value, this must be indicated using the has_retval parameter by
    setting it to True if the routine has a return value, and False otherwise.

    Note that the call-in table used to derive the routine interface may be specified by either the
    ydb_ci environment variable, or via the switch_ci_table() function included in the YDBPython
    module.

    :param routine: The name of the M routine to be called.
    :param args: The arguments to pass to that routine.
    :param has_retval: Flag indicating whether the routine has a return value.
    :returns: The return value of the routine, or else None.
    """
    num_args = len(args)
    if num_args > max_ci_args:
        raise ValueError(
            f"ci(): number of arguments ({num_args}) exceeds max for a {arch_bits}-bit system architecture ({max_ci_args})"
        )
    return _yottadb.ci(routine, args, has_retval)


def message(errnum: int) -> str:
    """
    Lookup the error message string for the given error code.

    :param errnum: A valid YottaDB error code number.
    :returns: A string containing the error message for the given error code.
    """
    return _yottadb.message(errnum)


def cip(routine: AnyStr, args: Tuple[Any] = (), has_retval: bool = False) -> Any:
    """
    Call an M routine specified in a YottaDB call-in table using the specified arguments, if any,
    reusing the internal YottaDB call-in handle on subsequent calls to the same routine
    as a performance optimization.

    If the routine has a return value, this must be indicated using the has_retval parameter by
    setting it to True if the routine has a return value, and False otherwise.

    Note that the call-in table used to derive the routine interface may be specified by either the
    ydb_ci environment variable, or via the switch_ci_table() function included in the YDBPython
    module.

    :param routine: The name of the M routine to be called.
    :param args: The arguments to pass to that routine.
    :param has_retval: Flag indicating whether the routine has a return value.
    :returns: The return value of the routine, or else None.
    """
    num_args = len(args)
    if num_args > max_ci_args:
        raise ValueError(
            f"cip(): number of arguments ({num_args}) exceeds max for a {arch_bits}-bit system architecture ({max_ci_args})"
        )
    return _yottadb.cip(routine, args, has_retval)


def release() -> str:
    """
    Lookup the current YDBPython and YottaDB release numbers.

    :returns: A string containing the current YDBPython and YottaDB release numbers.
    """
    return "pywr " + "v0.10.0 " + _yottadb.release()


def open_ci_table(filename: AnyStr) -> int:
    """
    Open the YottaDB call-in table at the specified location. Once opened,
    the call-in table may be activated by passing the returned call-in table
    handle to switch_ci_table().

    :param filename: The name of the YottaDB call-in table to open.
    :returns: An integer representing the call-in table handle opened by YottaDB.
    """
    return _yottadb.open_ci_table(filename)


def switch_ci_table(handle: int) -> int:
    """
    Switch the active YottaDB call-in table to that represented by the passed handle,
    as obtained through a previous call to open_ci_table().

    :param handle: An integer value representing a call-in table handle.
    :returns: An integer value representing a the previously active call-in table handle
    """
    result = _yottadb.switch_ci_table(handle)
    if result == 0:
        return None

    return result


def data(varname: AnyStr, subsarray: Tuple[AnyStr] = ()) -> int:
    """
    Get the following information about the status of the local or global variable node specified
    by the `varname` and `subsarray` pair:

    0: There is neither a value nor a subtree, i.e., it is undefined.
    1: There is a value, but no subtree
    10: There is no value, but there is a subtree.
    11: There are both a value and a subtree.

    :param varname: A bytes-like object representing a YottaDB local or global variable name.
    :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
    :returns: 0, 1, 10, or 11, representing the various possible statuses of the specified node.
    """
    return _yottadb.data(varname, subsarray)


def delete_node(varname: AnyStr, subsarray: Tuple[AnyStr] = ()) -> None:
    """
    Deletes the value at the local or global variable node specified by the `varname` and `subsarray` pair.

    :param varname: A bytes-like object representing a YottaDB local or global variable name.
    :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
    :returns: None.
    """
    _yottadb.delete(varname, subsarray, YDB_DEL_NODE)


def delete_tree(varname: AnyStr, subsarray: Tuple[AnyStr] = ()) -> None:
    """
    Deletes the value and any subtree of the local or global variable node specified by the `varname` and `subsarray` pair.

    :param varname: A bytes-like object representing a YottaDB local or global variable name.
    :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
    :returns: None.
    """
    _yottadb.delete(varname, subsarray, YDB_DEL_TREE)


def incr(varname: AnyStr, subsarray: Tuple[AnyStr] = (), increment: Union[int, float, str, bytes] = "1") -> bytes:
    """
    Increments the value of the local or global variable node specified by the `varname` and `subsarray` pair
    by the amount specified by `increment`.

    :param varname: A bytes-like object representing a YottaDB local or global variable name.
    :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
    :param increment: A numeric value specifying the amount by which to increment the given node.
    :returns: The new value of the node as a bytes object.
    """
    if (
        not isinstance(increment, int)
        and not isinstance(increment, str)
        and not isinstance(increment, bytes)
        and not isinstance(increment, float)
    ):
        raise TypeError("unsupported operand type(s) for +=: must be 'int', 'float', 'str', or 'bytes'")
    # Implicitly convert integers and floats to string for passage to API
    if isinstance(increment, bytes):
        # bytes objects cast to str prepend `b'` and append `'`, yielding an invalid numeric
        # so cast to float first to guarantee a valid numeric value
        increment = float(increment)
    increment = str(increment)
    return _yottadb.incr(varname, subsarray, increment)


def subscript_next(varname: AnyStr, subsarray: Tuple[AnyStr] = ()) -> bytes:
    """
    Retrieves the next subscript at the given subscript level of the local or global variable node
    specified by the `varname` and `subsarray` pair.

    :param varname: A bytes-like object representing a YottaDB local or global variable name.
    :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
    :returns: The next subscript at the given subscript level as a bytes object.
    """
    return _yottadb.subscript_next(varname, subsarray)


def subscript_previous(varname: AnyStr, subsarray: Tuple[AnyStr] = ()) -> bytes:
    """
    Retrieves the previous subscript at the given subscript level of the local or global variable node
    specified by the `varname` and `subsarray` pair.

    :param varname: A bytes-like object representing a YottaDB local or global variable name.
    :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
    :returns: The previous subscript at the given subscript level as a bytes object.
    """
    return _yottadb.subscript_previous(varname, subsarray)


def node_next(varname: AnyStr, subsarray: Tuple[AnyStr] = ()) -> Tuple[bytes, ...]:
    """
    Retrieves the next node from the local or global variable node specified by the `varname`
    and `subsarray` pair.

    :param varname: A bytes-like object representing a YottaDB local or global variable name.
    :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
    :returns: A subscript array representing the next node as a tuple of bytes objects.
    """
    return _yottadb.node_next(varname, subsarray)


def node_previous(varname: AnyStr, subsarray: Tuple[AnyStr] = ()) -> Tuple[bytes, ...]:
    """
    Retrieves the previous node from the local or global variable node specified by the `varname`
    and `subsarray` pair.

    :param varname: A bytes-like object representing a YottaDB local or global variable name.
    :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
    :returns: A subscript array representing the previous node as a tuple of bytes objects.
    """
    return _yottadb.node_previous(varname, subsarray)


def lock_incr(varname: AnyStr, subsarray: Tuple[AnyStr] = (), timeout_nsec: int = 0) -> None:
    """
    Without releasing any locks held by the process attempt to acquire a lock on the local or global
    variable node specified by the `varname` and `subsarray` pair, incrementing it if already held.

    :param varname: A bytes-like object representing a YottaDB local or global variable name.
    :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
    :param timeout_nsec: The time in nanoseconds that the function waits to acquire the requested lock.
    :returns: None.
    """
    return _yottadb.lock_incr(varname, subsarray, timeout_nsec)


def lock_decr(varname: AnyStr, subsarray: Tuple[AnyStr] = ()) -> None:
    """
    Decrements the lock count held by the process on the local or global variable node specified by
    the `varname` and `subsarray` pair.

    If the lock count goes from 1 to 0 the lock is released. If the specified lock is not held by the
    process calling `lock_decr()`, the call is ignored.

    :param varname: A bytes-like object representing a YottaDB local or global variable name.
    :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
    :returns: None.
    """
    return _yottadb.lock_decr(varname, subsarray)


def str2zwr(string: AnyStr) -> bytes:
    """
    Converts the given bytes-like object into YottaDB $ZWRITE format.

    :param string: A bytes-like object representing an arbitrary string.
    :returns: A bytes-like object representing `string` in YottaDB $ZWRITE format.
    """
    return _yottadb.str2zwr(string)


def zwr2str(string: AnyStr) -> bytes:
    """
    Converts the given bytes-like object from YottaDB $ZWRITE format into regular
    character string.

    :param string: A bytes-like object representing an arbitrary string.
    :returns: A bytes-like object representing the YottaDB $ZWRITE formatted `string` as a character string.
    """
    return _yottadb.zwr2str(string)


def tp(callback: object, args: tuple = None, transid: str = "", varnames: Tuple[AnyStr] = None, **kwargs) -> int:
    """
    Calls the function referenced by `callback` passing it the arguments specified by `args` using YottaDB Transaction Processing.

    Transcation throughput and latency may be improved by passing a case-insensitive value of "BA" or "BATCH" to `transid`,
    indicating that at transaction commit, YottaDB need not ensure Durability (it always ensures Atomicity, Consistency,
    and Isolation).

    Use of this value may improve latency and throughput for those applications where an alternative mechanism
    (such as a checkpoint) provides acceptable Durability. If a transaction that is not flagged as "BATCH" follows
    one or more transactions so flagged, Durability of the later transaction ensures Durability of the the earlier
    "BATCH" transaction(s).

    If varnames == ("*",), then all local variables are restored on a transaction restart.

    :param callback: A function object representing a Python function definition.
    :param args: A tuple of arguments accepted by the `callback` function.
    :param transid: A string that, when passed "BA" or "BATCH", optionally improves transaction throughput and latency,
        while removing the guarantee of Durability from ACID transactions.
    :param varnames: A tuple of YottaDB local or global variable names to restore to their original values when the
        transaction is restarted
    :returns: A bytes-like object representing the YottaDB $ZWRITE formatted `string` as a character string.
    """
    return _yottadb.tp(callback, args, kwargs, transid, varnames)


def node_to_dict(key: Tuple[AnyStr, Tuple[AnyStr]], child_subs: List[AnyStr], result: dict) -> Mapping:
    """
    Recursively constructs a series of nested dictionaries representing a the YottaDB node specified by `key`
    using the YottaDB subscripts in `subsarray` as keys. The last dictionary entry includes the value of the node.

    Note: This function is for internal use only, and not intended for use by users of YDBPython.

    :param key: A two-element tuple consisting of a YottaDB variable name and a tuple of YottaDB subscripts.
    :param child_subs: A list of YottaDB subscripts that need to be converted to Python dictionary keys.
    :param result: A previously allocated dictionary for storing any remaining subscripts from `child_subs`
        and the value of the node specified by `key`.
    :returns: A dictionary object representing node specified by `key`.
    """
    sub = child_subs[0]
    if sub not in result:
        # Allocate a new dictionary for the given subscript key if one
        # was not previously allocated.
        result[sub] = {}

    if len(child_subs) == 1:
        # No more subscripts to nest: retrieve the value, store it,
        # and cease recursing.
        key_status = data(key[0], list(key[1]) + list(child_subs))
        if key_status == 1 or key_status == 11:
            result[sub]["value"] = get(key[0], list(key[1]) + list(child_subs)).decode("utf-8")
    elif len(child_subs) > 1:
        # There are more subscripts to convert to dictionary keys, continue
        # recursing with `node_to_dict()`
        result[sub] = node_to_dict(key, child_subs[1:], result[sub])
    else:
        assert False

    return result


def save_tree(tree: dict, key: Key):
    """
    Stores data from a nested Python dictionary in YottaDB under the node represented by`key`.

    The nested dictionary must adhere to the following structure:

        {
            GVN: {
                { sub1:
                    {
                        nested_sub1: {
                            ...
                        },
                        nested_sub2: {
                            ...
                        },
                        "value": node_value
                    }
                },
                { sub2:
                    ...
                }
            }
        }

    :param tree: A Python dictionary representing a YottaDB tree or subtree.
    :param key: A YottaDB `Key` object representing a YottaDB database node
    """
    # Recurse through each key in the dictionary and, when a leaf node is encountered,
    # store the value in the database.
    for sub, value in tree.items():
        if isinstance(value, dict):
            save_tree(value, key[sub])
        elif sub == "value":
            key.value = value
        else:
            assert False


def replace_tree(tree: dict, key: Key):
    """
    Stores data from a nested Python dictionary in YottaDB under the node represented by`key`,
    replacing the existing tree and deleting any pre-existing values from the database that
    are not included in the new tree.

    The nested dictionary must adhere to the following structure:

        {
            GVN: {
                { sub1:
                    {
                        nested_sub1: {
                            ...
                        },
                        nested_sub2: {
                            ...
                        },
                        "value": node_value
                    }
                },
                { sub2:
                    ...
                }
            }
        }

    :param tree: A Python dictionary representing a YottaDB tree or subtree.
    :param key: A YottaDB `Key` object representing a YottaDB database node
    """
    key.delete_tree()
    key.save_tree(tree)


def load_tree(key: Key, child_subs: List[AnyStr] = None, result: dict = None, first_call: bool = False) -> dict:
    """
    Converts a `Key` object into a Python dictionary object representing the full YottaDB subtree under the
    database node specified by `key`.

    :param key: A `Key` object representing a YottaDB database node.
    :param child_subs: A list of subscripts describing a child node under the node
        represented by `key`.
    :param result: A dictionary object representing a partial YottaDB subtree under the
        database node specified by `key`. This dictionary is incrementally populated through
        recursive calls to `load_tree()`. If not supplied, a new dictionary is returned.
    :param first_call: A flag signalling whether the given call to `load_tree()` is the first
        of a series of recursive calls to `load_tree()`.
    :returns: A dictionary object representing the full YottaDB subtree under the
        database node specified by `key`.
    """
    if result is None:
        result = {}
    if child_subs is None:
        child_subs = []
    varname = key.varname
    subsarray = [] if key.subsarray is None else list(key.subsarray)
    if first_call and (key.data == 1 or key.data == 11):
        # Store the value of the root database node before recursively retrieving its
        # child nodes.
        result["value"] = key.value.decode("utf-8")
    for sub in subscripts(varname, subsarray + [""]):
        sub = sub.decode("utf-8")
        node_to_dict((varname, subsarray), tuple(child_subs + [sub]), result)
        load_tree(key[sub], child_subs + [sub], result)
    return result


class SubscriptsIter:
    """
    Iterator class for iterating over subscripts starting from the local or global variable node
    specified by the `varname` and `subsarray` pair passed to the `__init__()` constructor.
    """

    def __init__(self, varname: AnyStr, subsarray: Tuple[AnyStr] = ()) -> SubscriptsIter:
        """
        Creates a `SubscriptsIter` class object from the local or global variable node specified
        by the `varname` and `subsarray` pair.

        :param varname: A bytes-like object representing a YottaDB local or global variable name.
        :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
        :returns: A `SubscriptsIter` object.
        """
        self.varname = varname
        self.subsarray = list(subsarray)

    def __iter__(self) -> SubscriptsIter:
        """
        Converts the current object to an iterator. Since this is already an iterator object, just
        returns the object as is.

        :returns: A `SubscriptsIter` object.
        """
        return self

    def __next__(self) -> bytes:
        """
        Returns the next subscript relative to the current local or global variable node represented by
        the `self.varname` and `self.subsarray` pair, and updates `self.subsarray` with this next subscript
        in preparation for the next `__next__()` call.

        :returns: A bytes object representing the next subscript relative to the current local or global variable node.
        """
        try:
            if len(self.subsarray) > 0:
                sub_next = subscript_next(self.varname, self.subsarray)
                self.subsarray[-1] = sub_next
            else:
                # There are no subscripts and this is a variable-level iteration,
                # so do not modify subsarray (it is empty), but update the variable
                # name to the next variable instead.
                sub_next = subscript_next(self.varname)
                self.varname = sub_next
        except YDBNodeEnd:
            raise StopIteration
        return sub_next

    def __reversed__(self) -> list:
        """
        Creates a new iterable by compiling a list of all subscripts preceding the current local or global variable
        node represented by the `self.varname` and `self.subsarray` pair. The result is the list of subscripts from
        the current node in reverse order, which is then returned.

        :returns: A list of bytes objects representing the set of subscripts preceding the current local or global variable node.
        """
        result = []
        while True:
            try:
                sub_next = subscript_previous(self.varname, self.subsarray)
                if len(self.subsarray) != 0:
                    self.subsarray[-1] = sub_next
                else:
                    # There are no subscripts and this is a variable-level iteration,
                    # so do not modify subsarray (it is empty), but update the variable
                    # name to the next variable instead.
                    self.varname = sub_next
                result.append(sub_next)
            except YDBNodeEnd:
                break
        return result


def subscripts(varname: AnyStr, subsarray: Tuple[AnyStr] = ()) -> SubscriptsIter:
    """
    A convenience function that yields a `SubscriptsIter` class object from the local or global
    variable node specified by the `varname` and `subsarray` pair, providing a more readable
    interface for generating `SubscriptsIter` objects than calling the class constructor.

    :param varname: A bytes-like object representing a YottaDB local or global variable name.
    :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
    :returns: A `SubscriptsIter` object.
    """
    return SubscriptsIter(varname, subsarray)


class NodesIter:
    """
    Iterator class for iterating over YottaDB local or global variable nodes starting from the node
    specified by the `varname` and `subsarray` pair passed to the `__init__()` constructor.
    """

    def __init__(self, varname: AnyStr, subsarray: Tuple[AnyStr] = ()):
        """
        Creates a `NodesIter` class object from the local or global variable node specified
        by the `varname` and `subsarray` pair.

        :param varname: A bytes-like object representing a YottaDB local or global variable name.
        :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
        :returns: A `NodesIter` object.
        """
        self.varname = varname
        self.subsarray = [bytes(x, encoding="UTF-8") if isinstance(x, str) else x for x in subsarray]
        self.initialized = False

    def __iter__(self) -> NodesIter:
        """
        Converts the current object to an iterator. Since this is already an iterator object, just
        returns the object as is.

        :returns: A `NodesIter` object.
        """
        return self

    def __next__(self) -> Tuple[bytes]:
        """
        Returns the subscript array of the next node relative to the current local or global variable node represented by
        the `self.varname` and `self.subsarray` pair, and updates `self.subsarray` with this new subscript array
        in preparation for the next `__next__()` call.

        :returns: A tuple of bytes objects representing the subscript array for the next node relative to the current local
            or global variable node.
        """
        if not self.initialized:
            self.initialized = True
            status = data(self.varname)
            if 0 == len(self.subsarray) and (1 == status or 11 == status):
                return tuple(self.subsarray)
        try:
            self.subsarray = node_next(self.varname, self.subsarray)
        except YDBNodeEnd:
            raise StopIteration
        return self.subsarray

    def __reversed__(self):
        """
        Creates a new iterable for iterating over nodes preceding the current local or global variable in reverse by
        creating a new `NodesIterReversed` object and returning it.

        :returns: A NodesIterReversed object.
        """
        return NodesIterReversed(self.varname, self.subsarray)


class NodesIterReversed:
    """
    Iterator class for iterating in reverse over YottaDB local or global variable nodes starting from the node
    specified by the `varname` and `subsarray` pair passed to the `__init__()` constructor.
    """

    def __init__(self, varname: AnyStr, subsarray: Tuple[AnyStr] = ()):
        """
        Creates a `NodesIterReversed` class object from the local or global variable node specified
        by the `varname` and `subsarray` pair.

        :param varname: A bytes-like object representing a YottaDB local or global variable name.
        :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
        :returns: A `NodesIterReversed` object.
        """
        self.varname = varname
        self.subsarray = [bytes(x, encoding="UTF-8") if isinstance(x, str) else x for x in subsarray]
        self.reversed = [bytes(x, encoding="UTF-8") if isinstance(x, str) else x for x in subsarray]
        self.initialized = False

    def __iter__(self):
        """
        Converts the current object to an iterator. Since this is already an iterator object, just
        returns the object as is.

        :returns: A `NodesIterReversed` object.
        """
        return self

    def __next__(self):
        """
        Returns the subscript array of the previous node relative to the current local or global variable node represented by
        the `self.varname` and `self.reversed` pair, and updates `self.reversed` with this new subscript array
        in preparation for the next `__next__()` call.

        :returns: A tuple of bytes objects representing the subscript array for the previous node relative to the current local
            or global variable node.
        """
        # If this is the first iteration, then the last node of the reversed node iteration
        # is not yet known. So first look that up and return it, then signal to future calls
        # that this node is known by setting self.initialized, in which case future iterations
        # will skip last node lookup and simply return the preceding node via node_previous().
        if not self.initialized:
            # If the given subscript array points to a node or tree, append a "" subscript
            # to the subscript list to attempt to look up last subscript at the depth
            # of that array +1. If the subscript array doesn't point to a node or tree, then
            # it can be used as is to look up the last subscript at the given depth.
            if 0 < data(self.varname, self.subsarray):
                self.reversed.append("")
            while not self.initialized:
                try:
                    # If there is another subscript level, add its last subscript to the subscript list
                    self.reversed.insert(len(self.reversed) - 1, subscript_previous(self.varname, self.reversed))
                except YDBNodeEnd:
                    # Remove "" subscript now that the search for the last node is complete
                    self.reversed.pop()
                    self.initialized = True
            return tuple(self.reversed)

        try:
            self.reversed = node_previous(self.varname, self.reversed)
        except YDBNodeEnd:
            raise StopIteration

        return self.reversed

    def __reversed__(self):
        """
        Creates a new iterable for iterating over nodes following the current local or global variable by
        creating a new `NodesIter` object and returning it.

        :returns: A NodesIter object.
        """
        return NodesIter(self.varname, self.subarray)


def nodes(varname: AnyStr, subsarray: Tuple[AnyStr] = ()) -> NodesIter:
    """
    A convenience function that yields a `NodesIter` class object from the local or global
    variable node specified by the `varname` and `subsarray` pair, providing a more readable
    interface for generating `NodesIter` objects than calling the class constructor.

    :param varname: A bytes-like object representing a YottaDB local or global variable name.
    :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
    :returns: A `NodesIter` object.
    """
    return NodesIter(varname, subsarray)


class Key:
    """
    A class that represents a single YottaDB local or global variable node and supplies methods
    for performing various database operations on or relative to that node.
    """

    name: AnyStr
    parent: Key
    next_subsarray: List

    def __init__(self, name: AnyStr, parent: Key = None, subsarray: List = None) -> Key:
        """
        Creates a `Key` class object from the local or global variable node specified
        by the `varname` and `subsarray` pair.

        :param name: A bytes-like object representing a YottaDB local or global variable name, or subscript name.
        :param parent: A `Key` object representing a valid YottaDB local or global variable node.
        :returns: A `Key` object.
        """
        if isinstance(name, str) or isinstance(name, bytes):
            self.name = name
        else:
            raise TypeError("'name' must be an instance of str or bytes")

        if subsarray is not None:
            assert parent is None
            # Set the parent Key be creating a new key from the recieved variable name
            # and subscripts by omitting the last subscript from the list received.
            if len(subsarray) > 1:
                self.parent = Key(name, subsarray=subsarray[:-1])
            else:
                self.parent = Key(name)
        else:
            # Set the parent of the Key to the one specified by the caller
            self.parent = parent
            if parent is not None:
                if subsarray is not None:
                    raise ValueError("Cannot create Key from both a parent key and a subsarray. Please specify one or the other.")
                if not isinstance(parent, Key):
                    raise TypeError("'parent' must be of type Key")
        if _yottadb.YDB_MAX_SUBS < len(self.subsarray):
            raise ValueError(f"Cannot create Key with {len(self.subsarray)} subscripts (max: {_yottadb.YDB_MAX_SUBS})")

        # Initialize subsarray for use with Key.subscript_next()/Key.subscript_previous() methods
        if [] == self.subsarray:
            self.next_subsarray = [""]
        else:
            # Shallow copy the subscript array so that it is not mutated by Key.subscript_next()/Key.subscript_previous()
            self.next_subsarray = copy.copy(self.subsarray)
            self.next_subsarray.pop()
            self.next_subsarray.append("")

    def __repr__(self) -> str:
        """
        Produces a string representation of the current `Key` object that may be used to reproduce the object if passed to `eval()`.

        :returns: A string representation of the current `Key` object for passage to `eval()`.
        """
        result = f'{self.__class__.__name__}("{self.varname}")'
        for subscript in self.subsarray:
            result += f'["{subscript}"]'
        return result

    def __str__(self) -> str:
        """
        Produces a human-readable string representation of the current `Key` object.

        :returns: A human-readable string representation of the current `Key` object.
        """
        # Convert to ZWRITE format to allow decoding of binary blobs into `str` objects
        subscripts = ",".join([str2zwr(sub).decode("ascii") for sub in self.subsarray])
        if subscripts == "":
            return self.varname
        else:
            return f"{self.varname}({subscripts})"

    def __setitem__(self, item: AnyStr, value: AnyStr) -> None:
        """
        Sets the value of the local or global variable node specified by the current `Key` object with
        `item` appended to its subscript array to the value specified by `value`. This is done by creating
        a new `Key` object from the current one in combination with the subscript name specified by `item`.

        :param item: A bytes-like object representing a YottaDB subscript name.
        :param value: A bytes-like object representing the value of a YottaDB local or global variable node.
        :returns: None.
        """
        Key(name=item, parent=self).value = value

    def __getitem__(self, item):
        """
        Creates a new `Key` object representing the local or global variable node specified by the current
        `Key` object with `item` appended to its subscript array.

        :param item: A bytes-like object representing a YottaDB subscript name.
        :returns: A new `Key` object.
        """
        return Key(name=item, parent=self)

    def __iadd__(self, num: Union[int, float, str, bytes]) -> Key:
        """
        Increments the value of the local or global variable node specified by the current `Key` object
        by the amount specified by `num`.

        :param num: A numeric value specifying the amount by which to increment the given node.
        :returns: The current `Key` object.
        """
        self.incr(num)
        return self

    def __isub__(self, num: Union[int, float, str, bytes]) -> Key:
        """
        Decrements the value of the local or global variable node specified by the current `Key` object
        by the amount specified by `num`.

        :param num: A numeric value specifying the amount by which to decrement the given node.
        :returns: The current `Key` object.
        """
        if isinstance(num, float):
            self.incr(-float(num))
        else:
            self.incr(-int(num))
        return self

    def __eq__(self, other) -> bool:
        """
        Evaluates whether the current `Key` object represents the same YottaDB local or global variable name as `other`.

        :param other: A `Key` object representing a valid YottaDB local or global variable node.
        :returns: True if the two `Key`s represent the same node, or False otherwise.
        """
        if isinstance(other, Key):
            return self.varname == other.varname and self.subsarray == other.subsarray
        else:
            return self.value == other

    def __iter__(self) -> Generator:
        """
        A Generator that returns the a `Key` object representing the node at the next subscript relative to the local or
        global variable node represented by the current `Key` object on each iteration.

        :returns: A `Key` object representing the node at the next subscript relative to the local or global variable.
        """
        if len(self.subsarray) > 0:
            subscript_subsarray = list(self.subsarray)
        else:
            subscript_subsarray: List[AnyStr] = []
        subscript_subsarray.append("")
        while True:
            try:
                sub_next = subscript_next(self.varname, subscript_subsarray)
                subscript_subsarray[-1] = sub_next
                yield Key(sub_next, self)
            except YDBNodeEnd:
                return

    def __reversed__(self) -> Generator:
        """
        A Generator that returns the a `Key` object representing the node at the previous subscript relative to the
        local or global variable node represented by the current `Key` object on each iteration.

        :returns: A `Key` object representing the node at the previous subscript relative to the local or global variable.
        """
        if len(self.subsarray) > 0:
            subscript_subsarray = list(self.subsarray)
        else:
            subscript_subsarray: List[AnyStr] = []
        subscript_subsarray.append("")
        while True:
            try:
                sub_next = subscript_previous(self.varname, subscript_subsarray)
                subscript_subsarray[-1] = sub_next
                yield Key(sub_next, self)
            except YDBNodeEnd:
                return

    def get(self) -> Optional[bytes]:
        """
        Retrieve the value of the local or global variable node represented by the current `Key` object.

        :returns: If the specified node has a value, returns it as a bytes object. If not, returns None.
        """
        return get(self.varname, self.subsarray)

    def set(self, value: AnyStr = "") -> None:
        """
        Set the local or global variable node represented by the current `Key` object.

        :param value: A bytes-like object representing the value of a YottaDB local or global variable node.
        :returns: None.
        """
        return set(self.varname, self.subsarray, value)

    @property
    def data(self) -> int:
        """
        Get the following information about the status of the local or global variable node represented
        by the current `Key` object.

        0: There is neither a value nor a subtree, i.e., it is undefined.
        1: There is a value, but no subtree
        10: There is no value, but there is a subtree.
        11: There are both a value and a subtree.

        :returns: 0, 1, 10, or 11, representing the various possible statuses of the specified node.
        """
        return data(self.varname, self.subsarray)

    def delete_node(self) -> None:
        """
        Deletes the value at the local or global variable node represented by the current `Key` object.

        :returns: None.
        """
        delete_node(self.varname, self.subsarray)

    def delete_tree(self) -> None:
        """
        Deletes the value and any subtree of the local or global variable node represented by the current `Key` object.

        :returns: None.
        """
        delete_tree(self.varname, self.subsarray)

    def incr(self, increment: Union[int, float, str, bytes] = "1") -> bytes:
        """
        Increments the value of the local or global variable node represented by the current `Key` object
        by the amount specified by `increment`.

        :param increment: A numeric value specifying the amount by which to increment the given node.
        :returns: The new value of the node as a bytes object.
        """
        # incr() will enforce increment type
        return incr(self.varname, self.subsarray, increment)

    def subscript_next(self, reset: bool = False) -> bytes:
        """
        Iterate over the subscripts at the given subscript level of the local or global variable node
        represented by the current `Key` object. When all subscripts are exhausted this method will
        raise `YDBNodeEnd` until the iteration is reset by passing a value of `True` to the `reset`
        parameter.

        :param reset: A boolean value indicating whether or not to reset subscripts iteration to the original node.
        :returns: The next subscript at the given subscript level as a bytes object.
        """
        if reset:
            self.next_subsarray.pop()
            self.next_subsarray.append("")

        next_sub = subscript_next(self.varname, self.next_subsarray)
        self.next_subsarray.pop()
        self.next_subsarray.append(next_sub)

        return next_sub

    def subscript_previous(self, reset: bool = False) -> bytes:
        """
        Iterate over the subscripts at the given subscript level of the local or global variable node
        represented by the current `Key` object in reverse order. When all subscripts are exhausted
        this method will raise `YDBNodeEnd` until the iteration is reset by passing a value of `True` to
        the `reset` parameter.

        :param reset: A boolean value indicating whether or not to reset subscripts iteration to the original node.
        :returns: The previous subscript at the given subscript level as a bytes object.
        """
        if reset:
            self.next_subsarray.pop()
            self.next_subsarray.append("")

        prev_sub = subscript_previous(self.varname, self.next_subsarray)
        self.next_subsarray.pop()
        self.next_subsarray.append(prev_sub)

        return prev_sub

    def lock(self, timeout_nsec: int = 0) -> None:
        """
        Release any locks held by the process, and attempt to acquire a lock on the local or global variable node
        represented by the current `Key` object.

        The specified locks are released unconditionally, except in the case of an error. On return, the function will have acquired
        the requested lock or else no locks.

        :param timeout_nsec: The time in nanoseconds that the function waits to acquire the requested locks.
        :returns: None.
        """
        return lock((self,), timeout_nsec)

    def lock_incr(self, timeout_nsec: int = 0) -> None:
        """
        Without releasing any locks held by the process attempt to acquire a lock on the local or global
        variable node represented by the current `Key` object, incrementing it if already held.

        :param timeout_nsec: The time in nanoseconds that the function waits to acquire the requested lock.
        :returns: None.
        """
        return lock_incr(self.varname, self.subsarray, timeout_nsec)

    def lock_decr(self) -> None:
        """
        Decrements the lock count held by the process on the local or global variable node represented by
        the current `Key` object.

        If the lock count goes from 1 to 0 the lock is released. If the specified lock is not held by the
        process calling `lock_decr()`, the call is ignored.

        :returns: None.
        """
        return lock_decr(self.varname, self.subsarray)

    def load_tree(self) -> dict:
        return load_tree(self, first_call=True)

    def save_tree(self, tree: dict, key: Key = None):
        """
        Stores data from a nested Python dictionary in YottaDB. The dictionary must have been previously created using the
        `Key.load_tree()` method, or otherwise match the format used by that method.

        :param tree: A Python dictionary representing a YottaDB tree or subtree.
        :param key: A `Key` object representing the YottaDB database node that is the root of the tree
            structure represented by `tree`.
        """
        if key is None:
            key = self
        # Recurse through each key in the dictionary and, when a leaf node is encountered,
        # store the value in the database.
        for sub, value in tree.items():
            if isinstance(value, dict):
                self.save_tree(value, key[sub])
            elif sub == "value":
                key.value = value
            else:
                assert False

    def replace_tree(self, tree: dict):
        """
        Stores data from a nested Python dictionary in YottaDB. The dictionary must have been previously created using the
        `Key.load_tree()` method, or otherwise match the format used by that method.

        :param tree: A Python dictionary representing a YottaDB tree or subtree.
        :param key: A `Key` object representing the YottaDB database node that is the root of the tree
            structure represented by `tree`.
        """
        self.delete_tree()
        self.save_tree(tree)

    def save_json(self, json: object, key: Key = None):
        """
        Saves JSON data stored in a Python object under the YottaDB node represented by the calling `Key` object.

        :param self: A YottaDB `Key` object.
        :param json: A Python object representing a JSON object.
        """
        # Recurse through each item in the JSON object and,
        # when a leaf node is encountered, store the value in the database.
        if key is None:
            key = self
        if isinstance(json, dict):
            if not json:
                # use the same format as lists and strings
                key["\\d"].value = ""
            for sub, value in json.items():
                # There is another level of nested data, continue recursing.
                self.save_json(value, key[sub])
        elif isinstance(json, list):
            # The value is an array, store it in the database piecemeal by index,
            # starting from 1.
            if len(json) == 0:
                # use the same format as dicts and strings
                key["\\l"].value = ""
            else:
                for index, item in enumerate(json, start=1):
                    i = str(index)
                    self.save_json(json[index - 1], key[i])
        else:
            # No more nested data, save the value in the database.
            if not isinstance(json, str):
                json = str(json)
            else:
                # derived from https://github.com/KRMAssociatesInc/JDS-GTM
                key["\\s"].value = ""
            key.value = json
            # print(f"{key}={key.value}")

    def load_json(self, key: Key = None, spaces: str = "") -> object:
        """
        Retrieves JSON data stored under the YottaDB database node represented by the calling `Key` object,
        and returns it as a Python object.

        :param self: A YottaDB `Key` object.
        :param result: A Python object containing JSON data loaded from the database.
        :returns: A Python object representing a JSON object.
        """
        result = None
        if key is None:
            key = self
        # Check whether the current node is a value (i.e. a leaf node), or if it has a subtree.
        key_status = key.data
        if key["\\l"].data == 1:
            # The value is an empty JSON array, so just return an empty list.
            return []
        elif key["\\d"].data == 1:
            return {}
        # print(f"{spaces}key_status: {key_status}\t\\s: {slash_s}")
        if key_status == 10:
            # The node has a subtree, which represents a JSON array or a JSON object.
            for index, sub in enumerate(key.subscripts, start=1):
                sub = sub.decode("utf-8")
                # print(f"{spaces}{key[sub]}")
                # print(f"{spaces}index: {index}\tsub: {sub}")
                if str(index) == sub:
                    # The subtree represents a JSON array. This is so when the subscripts form a
                    # series of integers starting from 1, e.g. 1, 2, 3, etc. In that case, treat them
                    # as indices of a list such that their values or subtrees are elements of a list.
                    if result is None:
                        result = []
                    result.append(self.load_json(key[sub], spaces + "\t"))
                    # print(f"{spaces}ARRAY: list result: {result}")
                else:
                    # The node is a subtree, signifying a nested JSON object must be retrieved.
                    # If necessary, assign a dictionary to store it, then add the results to the
                    # dictionary using the current subscript.
                    if result is None:
                        result = {}
                    result[sub] = self.load_json(key[sub], spaces + "\t")
                    # print(f"{spaces}SUBTREE: {result}")
        elif key_status == 1 or key_status == 11:
            # The node is a value and not a subtree. So, retrieve the value, decode it into a Python string,
            # convert it to an appropriate type, if necessary, then return it to the caller.
            #
            # Note that `key_status` may be 11 in the case of a string literal, due to a "\s" node
            # stored by `Key.save_json()`. In that case, 11 is acceptable.
            value = key.value.decode("utf-8")
            # print(f"{spaces}value: {value}")
            if key["\\s"].data == 1:
                # A `"\s"` node accompanies the given value, signifying that the value is a string literal.
                # In that case, no type conversion is necessary.
                # print(f"{spaces}type: str")
                return value
            else:
                # The value is *not* a string literal and must be converted to the appropriate type. So,
                # infer the type here and convert accordingly.
                assert key_status == 1
                if value == "None":
                    # print(f"{spaces}type: None")
                    return None
                try:
                    # print(f"{spaces}type: int")
                    return int(value)
                except ValueError:
                    pass
                try:
                    # print(f"{spaces}type: float")
                    return float(value)
                except ValueError:
                    pass
                try:
                    # print(f"{spaces}type: bool")
                    return bool(value)
                except ValueError:
                    pass
            # print(f"{spaces}VALUE: {value}")
            result = value
        else:
            # `key_status` should not be 0 since the `Key.save_json()` method
            # should not have saved any empty nodes.
            assert False

        # print(f"{spaces}END RESULT: {result}")
        return result

    @property
    def varname_key(self) -> Key:
        """
        Returns a `Key` object representing the unsubscripted local or global variable the node represented by
        the current `Key` object falls under.

        :returns: A `Key` object representing an unsubscripted local or global variable.
        """
        if self.parent is None:
            return self
        ancestor = self.parent
        while ancestor.parent is not None:
            ancestor = ancestor.parent
        return ancestor

    @property
    def varname(self) -> AnyStr:
        """
        Returns the name of the local or global variable the node represented by the current `Key` object
        falls under.

        :returns: A bytes-like object representing a local or global variable name.
        """
        return self.varname_key.name  # str or bytes

    @property
    def subsarray_keys(self) -> List["Key"]:
        """
        Returns a list of all parent `Key`s of the current `Key` object.

        :returns: A list `Key` objects representing all parents nodes of the current `Key` object.
        """
        if self.parent is None:
            return []
        subs_array = [self]
        ancestor = self.parent
        while ancestor.parent is not None:
            subs_array.insert(0, ancestor)
            ancestor = ancestor.parent
        return subs_array

    @property
    def subsarray(self) -> List[AnyStr]:
        """
        Returns the names of all subscripts of the current `Key` object.

        :returns: A list of bytes-like objects representing the names of all subscripts of the current `Key` object.
        """
        ret_list = []
        for key in self.subsarray_keys:
            ret_list.append(key.name)
        return ret_list  # Returns List of str or bytes

    @property
    def value(self) -> Optional[bytes]:
        """
        Retrieve the value of the local or global variable node represented by the current `Key` object.

        :returns: If the specified node has a value, returns it as a bytes object. If not, returns None.
        """
        return get(self.varname, self.subsarray)

    @value.setter
    def value(self, value: AnyStr) -> None:
        """
        Set the value of the local or global variable node represented by the current `Key` object.

        :param value: A bytes-like object representing the value of a YottaDB local or global variable node.
        :returns: None.
        """
        # Value must be str or bytes
        set(self.varname, self.subsarray, value)

    @property
    def has_value(self):
        """
        Indicates whether the local or global variable node represented by the current `Key` object
        has a value or not.

        :returns: `True` if the node has a value, `False` otherwise.
        """
        if self.data == YDB_DATA_VALUE_NODESC or self.data == YDB_DATA_VALUE_DESC:
            return True
        else:
            return False

    @property
    def has_tree(self):
        """
        Indicates whether the local or global variable node represented by the current `Key` object
        has a subtree or not.

        :returns: `True` if the node has a subtree, `False` otherwise.
        """
        if self.data == YDB_DATA_NOVALUE_DESC or self.data == YDB_DATA_VALUE_DESC:
            return True
        else:
            return False

    @property
    def subscripts(self) -> Generator:
        """
        A Generator that returns the next subscript at the current subscript level relative to the
        local or global variable node represented by the current `Key` object on each iteration.

        :returns: A bytes objects representing the subscript following the current local or global variable node.
        """
        if len(self.subsarray) > 0:
            subscript_subsarray = list(self.subsarray)
        else:
            subscript_subsarray: List[AnyStr] = []
        subscript_subsarray.append("")
        while True:
            try:
                sub_next = subscript_next(self.varname, subscript_subsarray)
                subscript_subsarray[-1] = sub_next
                yield sub_next
            except YDBNodeEnd:
                return

    """
    def delete_excel(self): ...
    """


# Defined after Key class to allow access to that class
def lock(keys: Tuple[Tuple[AnyStr, Tuple[AnyStr]]] = None, timeout_nsec: int = 0) -> None:
    """
    Release any locks held by the process, and attempt to acquire all the locks named by `keys`. Each element
    of `keys` must be a tuple containing a bytes-like object representing a YottaDB local or global variable name
    and another tuple of bytes-like objects representing a subscript array. Together, these compose a single YottaDB
    key specification. For example, `("^myglobal", ("sub1", "sub2"))` represents the YottaDB node `^myglobal("sub1","sub2")`.

    The specified locks are released unconditionally, except in the case of an error. On return, the function will
    have acquired all requested locks or none of them. If no locks are requested (`keys` is empty), the function releases all
    locks and returns `None`.

    :param keys: A tuple of tuples, each representing a YottaDB local or global variable node.
    :param timeout_nsec: The time in nanoseconds that the function waits to acquire the requested locks.
    :returns: None.
    """
    if keys is not None:
        keys = [(key.varname, key.subsarray) if isinstance(key, Key) else key for key in keys]
    return _yottadb.lock(keys=keys, timeout_nsec=timeout_nsec)


def transaction(function) -> Callable[..., object]:
    """
    Convert the specified `function` into a transaction-safe function by wrapping it in a call to `tp()`. The new function
    can then be used to call the original function with YottaDB Transaction Processing, without the need for an explicit call
    to `tp()`. Can be used as a decorator.

    :param function: A Python object representing a Python function definition.
    :returns: A Python function object that may calls `function` using `tp()`.
    """

    def wrapper(*args, **kwargs) -> int:
        def wrapped_transaction(*args, **kwargs):
            ret_val = YDB_OK
            try:
                ret_val = function(*args, **kwargs)
                if ret_val is None:
                    ret_val = YDB_OK
            except YDBTPRestart:
                ret_val = _yottadb.YDB_TP_RESTART
            return ret_val

        return _yottadb.tp(wrapped_transaction, args=args, kwargs=kwargs)

    return wrapper
