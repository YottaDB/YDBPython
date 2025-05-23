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
Node = NewType("Node", object)
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


def get(name: AnyStr, subsarray: Tuple[AnyStr] = ()) -> Optional[bytes]:
    """
    Retrieve the value of the local or global variable node specified by the `name` and `subsarray` pair.

    :param name: A bytes-like object representing a YottaDB local or global variable name.
    :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
    :returns: If the specified node has a value, returns it as a bytes object. If not, returns None.
    """
    try:
        return _yottadb.get(name, subsarray)
    except YDBError as e:
        ecode = e.code()
        if _yottadb.YDB_ERR_LVUNDEF == ecode or _yottadb.YDB_ERR_GVUNDEF == ecode:
            return None
        else:
            raise e


def set(name: AnyStr, subsarray: Tuple[AnyStr] = (), value: AnyStr = "") -> None:
    """
    Set the local or global variable node specified by the `name` and `subsarray` pair.

    :param name: A bytes-like object representing a YottaDB local or global variable name.
    :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
    :param value: A bytes-like object representing the value of a YottaDB local or global variable node.
    :returns: None.
    """
    _yottadb.set(name, subsarray, value)
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


def data(name: AnyStr, subsarray: Tuple[AnyStr] = ()) -> int:
    """
    Get the following information about the status of the local or global variable node specified
    by the `name` and `subsarray` pair:

    0: There is neither a value nor a subtree, i.e., it is undefined.
    1: There is a value, but no subtree
    10: There is no value, but there is a subtree.
    11: There are both a value and a subtree.

    :param name: A bytes-like object representing a YottaDB local or global variable name.
    :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
    :returns: 0, 1, 10, or 11, representing the various possible statuses of the specified node.
    """
    return _yottadb.data(name, subsarray)


def delete_node(name: AnyStr, subsarray: Tuple[AnyStr] = ()) -> None:
    """
    Deletes the value at the local or global variable node specified by the `name` and `subsarray` pair.

    :param name: A bytes-like object representing a YottaDB local or global variable name.
    :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
    :returns: None.
    """
    _yottadb.delete(name, subsarray, YDB_DEL_NODE)


def delete_tree(name: AnyStr, subsarray: Tuple[AnyStr] = ()) -> None:
    """
    Deletes the value and any subtree of the local or global variable node specified by the `name` and `subsarray` pair.

    :param name: A bytes-like object representing a YottaDB local or global variable name.
    :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
    :returns: None.
    """
    _yottadb.delete(name, subsarray, YDB_DEL_TREE)


def delete_except(lvns: Tuple[AnyStr, Key, Node] = ()) -> None:
    """
    Deletes all local variable trees, except those under the variable names in `names`.

    :param names: A tuple or list of bytes-like objects or Node objects representing or
        containing YottaDB local variable names.
    :returns: None.
    """
    names = [lvn.name if isinstance(lvn, Node) or isinstance(lvn, Key) else lvn for lvn in lvns]
    _yottadb.delete_except(names)


def incr(name: AnyStr, subsarray: Tuple[AnyStr] = (), increment: Union[int, float, str, bytes] = "1") -> bytes:
    """
    Increments the value of the local or global variable node specified by the `name` and `subsarray` pair
    by the amount specified by `increment`.

    :param name: A bytes-like object representing a YottaDB local or global variable name.
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
    return _yottadb.incr(name, subsarray, increment)


def subscript_next(name: AnyStr, subsarray: Tuple[AnyStr] = ()) -> bytes:
    """
    Retrieves the next subscript at the given subscript level of the local or global variable node
    specified by the `name` and `subsarray` pair.

    :param name: A bytes-like object representing a YottaDB local or global variable name.
    :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
    :returns: The next subscript at the given subscript level as a bytes object.
    """
    return _yottadb.subscript_next(name, subsarray)


def subscript_previous(name: AnyStr, subsarray: Tuple[AnyStr] = ()) -> bytes:
    """
    Retrieves the previous subscript at the given subscript level of the local or global variable node
    specified by the `name` and `subsarray` pair.

    :param name: A bytes-like object representing a YottaDB local or global variable name.
    :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
    :returns: The previous subscript at the given subscript level as a bytes object.
    """
    return _yottadb.subscript_previous(name, subsarray)


def node_next(name: AnyStr, subsarray: Tuple[AnyStr] = ()) -> Tuple[bytes, ...]:
    """
    Retrieves the next node from the local or global variable node specified by the `name`
    and `subsarray` pair.

    :param name: A bytes-like object representing a YottaDB local or global variable name.
    :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
    :returns: A subscript array representing the next node as a tuple of bytes objects.
    """
    return _yottadb.node_next(name, subsarray)


def node_previous(name: AnyStr, subsarray: Tuple[AnyStr] = ()) -> Tuple[bytes, ...]:
    """
    Retrieves the previous node from the local or global variable node specified by the `name`
    and `subsarray` pair.

    :param name: A bytes-like object representing a YottaDB local or global variable name.
    :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
    :returns: A subscript array representing the previous node as a tuple of bytes objects.
    """
    return _yottadb.node_previous(name, subsarray)


def lock_incr(name: AnyStr, subsarray: Tuple[AnyStr] = (), timeout_nsec: int = 0) -> None:
    """
    Without releasing any locks held by the process attempt to acquire a lock on the local or global
    variable node specified by the `name` and `subsarray` pair, incrementing it if already held.

    :param name: A bytes-like object representing a YottaDB local or global variable name.
    :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
    :param timeout_nsec: The time in nanoseconds that the function waits to acquire the requested lock.
    :returns: None.
    """
    return _yottadb.lock_incr(name, subsarray, timeout_nsec)


def lock_decr(name: AnyStr, subsarray: Tuple[AnyStr] = ()) -> None:
    """
    Decrements the lock count held by the process on the local or global variable node specified by
    the `name` and `subsarray` pair.

    If the lock count goes from 1 to 0 the lock is released. If the specified lock is not held by the
    process calling `lock_decr()`, the call is ignored.

    :param name: A bytes-like object representing a YottaDB local or global variable name.
    :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
    :returns: None.
    """
    return _yottadb.lock_decr(name, subsarray)


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


def tp(callback: object, args: tuple = None, transid: str = "", names: Tuple[AnyStr] = None, **kwargs) -> int:
    """
    Calls the function referenced by `callback` passing it the arguments specified by `args` using YottaDB Transaction Processing.

    Transcation throughput and latency may be improved by passing a case-insensitive value of "BA" or "BATCH" to `transid`,
    indicating that at transaction commit, YottaDB need not ensure Durability (it always ensures Atomicity, Consistency,
    and Isolation).

    Use of this value may improve latency and throughput for those applications where an alternative mechanism
    (such as a checkpoint) provides acceptable Durability. If a transaction that is not flagged as "BATCH" follows
    one or more transactions so flagged, Durability of the later transaction ensures Durability of the the earlier
    "BATCH" transaction(s).

    If names == ("*",), then all local variables are restored on a transaction restart.

    :param callback: A function object representing a Python function definition.
    :param args: A tuple of arguments accepted by the `callback` function.
    :param transid: A string that, when passed "BA" or "BATCH", optionally improves transaction throughput and latency,
        while removing the guarantee of Durability from ACID transactions.
    :param names: A tuple of YottaDB local or global variable names to restore to their original values when the
        transaction is restarted
    :returns: A bytes-like object representing the YottaDB $ZWRITE formatted `string` as a character string.
    """
    return _yottadb.tp(callback, args, kwargs, transid, names)


def node_to_dict(node: Tuple[AnyStr, Tuple[AnyStr]], child_subs: List[AnyStr], result: dict) -> Mapping:
    """
    Recursively constructs a series of nested dictionaries representing a the YottaDB node specified by `node` using
    the YottaDB subscripts in `subsarray` as nodes. The last dictionary entry includes the value of the node.

    Note: This function is for internal use only, and not intended for use by users of YDBPython.

    :param node: A two-element tuple consisting of a YottaDB variable name and a tuple of YottaDB subscripts.
    :param child_subs: A list of YottaDB subscripts that need to be converted to Python dictionary nodes.
    :param result: A previously allocated dictionary for storing any remaining subscripts from `child_subs`
        and the value of the node specified by `node`.
    :returns: A dictionary object representing node specified by `node`.
    """
    sub = child_subs[0]
    if sub not in result:
        # Allocate a new dictionary for the given subscript if one
        # was not previously allocated.
        result[sub] = {}

    if len(child_subs) == 1:
        # No more subscripts to nest: retrieve the value, store it,
        # and cease recursing.
        node_status = data(node[0], list(node[1]) + list(child_subs))
        if node_status == 1 or node_status == 11:
            result[sub]["value"] = get(node[0], list(node[1]) + list(child_subs)).decode("utf-8")
    elif len(child_subs) > 1:
        # There are more subscripts to convert to dictionary keys, continue
        # recursing with `node_to_dict()`
        result[sub] = node_to_dict(node, child_subs[1:], result[sub])
    else:
        assert False

    return result


def save_tree(tree: dict, node: [Node, Key]):
    """
    Stores data from a nested Python dictionary in YottaDB under the node represented by `node`.

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
    :param node: A YottaDB `Node` or `Key` object representing a YottaDB database node
    """
    # Recurse through each node in the dictionary and, when a leaf node is encountered,
    # store the value in the database.
    for sub, value in tree.items():
        if isinstance(value, dict):
            save_tree(value, node[sub])
        elif sub == "value":
            node.value = value
        else:
            assert False


def replace_tree(tree: dict, node: [Node, Key]):
    """
    Stores data from a nested Python dictionary in YottaDB under the node represented by `node`,
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
    :param node: A YottaDB `Node` or `Key` object representing a YottaDB database node
    """
    node.delete_tree()
    node.save_tree(tree)


def load_tree(node: [Node, Key], child_subs: List[AnyStr] = None, result: dict = None, first_call: bool = False) -> dict:
    """
    Converts a `Node` or `Key` object into a Python dictionary object representing the full YottaDB subtree under the
    database node specified by `node`.

    :param node: A `Node` or `Key` object representing a YottaDB database node.
    :param child_subs: A list of subscripts describing a child node under the node
        represented by `node`.
    :param result: A dictionary object representing a partial YottaDB subtree under the
        database node specified by `node`. This dictionary is incrementally populated through
        recursive calls to `load_tree()`. If not supplied, a new dictionary is returned.
    :param first_call: A flag signalling whether the given call to `load_tree()` is the first
        of a series of recursive calls to `load_tree()`.
    :returns: A dictionary object representing the full YottaDB subtree under the
        database node specified by `node`.
    """
    if result is None:
        result = {}
    if child_subs is None:
        child_subs = []
    name = node.name
    subsarray = [] if node.subsarray is None else list(node.subsarray)
    if first_call and (node.data == 1 or node.data == 11):
        # Store the value of the root database node before recursively retrieving its
        # child nodes.
        result["value"] = node.value.decode("utf-8")
    for sub in subscripts(name, subsarray + [""]):
        sub = sub.decode("utf-8")
        node_to_dict((name, subsarray), tuple(child_subs + [sub]), result)
        load_tree(node[sub], child_subs + [sub], result)
    return result


class SubscriptsIter:
    """
    Iterator class for iterating over subscripts starting from the local or global variable node
    specified by the `name` and `subsarray` pair passed to the `__init__()` constructor.
    """

    def __init__(self, name: AnyStr, subsarray: Tuple[AnyStr] = ()) -> SubscriptsIter:
        """
        Creates a `SubscriptsIter` class object from the local or global variable node specified
        by the `name` and `subsarray` pair.

        :param name: A bytes-like object representing a YottaDB local or global variable name.
        :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
        :returns: A `SubscriptsIter` object.
        """
        self._name = name
        self._subsarray = list(subsarray)

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
        the `self._name` and `self._subsarray` pair, and updates `self._subsarray` with this next subscript
        in preparation for the next `__next__()` call.

        :returns: A bytes object representing the next subscript relative to the current local or global variable node.
        """
        try:
            if len(self._subsarray) > 0:
                sub_next = subscript_next(self._name, self._subsarray)
                self._subsarray[-1] = sub_next
            else:
                # There are no subscripts and this is a variable-level iteration,
                # so do not modify subsarray (it is empty), but update the variable
                # name to the next variable instead.
                sub_next = subscript_next(self._name)
                self._name = sub_next
        except YDBNodeEnd:
            raise StopIteration
        return sub_next

    def __reversed__(self) -> list:
        """
        Creates a new iterable by compiling a list of all subscripts preceding the current local or global variable
        node represented by the `self._name` and `self._subsarray` pair. The result is the list of subscripts from
        the current node in reverse order, which is then returned.

        :returns: A list of bytes objects representing the set of subscripts preceding the current local or global variable node.
        """
        result = []
        while True:
            try:
                sub_next = subscript_previous(self._name, self._subsarray)
                if len(self._subsarray) != 0:
                    self._subsarray[-1] = sub_next
                else:
                    # There are no subscripts and this is a variable-level iteration,
                    # so do not modify subsarray (it is empty), but update the variable
                    # name to the next variable instead.
                    self._name = sub_next
                result.append(sub_next)
            except YDBNodeEnd:
                break
        return result


def subscripts(name: AnyStr, subsarray: Tuple[AnyStr] = ()) -> SubscriptsIter:
    """
    A convenience function that yields a `SubscriptsIter` class object from the local or global
    variable node specified by the `name` and `subsarray` pair, providing a more readable
    interface for generating `SubscriptsIter` objects than calling the class constructor.

    :param name: A bytes-like object representing a YottaDB local or global variable name.
    :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
    :returns: A `SubscriptsIter` object.
    """
    return SubscriptsIter(name, subsarray)


class NodesIter:
    """
    Iterator class for iterating over YottaDB local or global variable nodes starting from the node
    specified by the `name` and `subsarray` pair passed to the `__init__()` constructor.
    """

    def __init__(self, name: AnyStr, subsarray: Tuple[AnyStr] = ()):
        """
        Creates a `NodesIter` class object from the local or global variable node specified
        by the `name` and `subsarray` pair.

        :param name: A bytes-like object representing a YottaDB local or global variable name.
        :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
        :returns: A `NodesIter` object.
        """
        self._name = name
        self._subsarray = [bytes(x, encoding="UTF-8") if isinstance(x, str) else x for x in subsarray]
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
        the `self._name` and `self._subsarray` pair, and updates `self._subsarray` with this new subscript array
        in preparation for the next `__next__()` call.

        :returns: A tuple of bytes objects representing the subscript array for the next node relative to the current local
            or global variable node.
        """
        if not self.initialized:
            self.initialized = True
            status = data(self._name)
            if 0 == len(self._subsarray) and (1 == status or 11 == status):
                return tuple(self._subsarray)
        try:
            self._subsarray = node_next(self._name, self._subsarray)
        except YDBNodeEnd:
            raise StopIteration
        return self._subsarray

    def __reversed__(self):
        """
        Creates a new iterable for iterating over nodes preceding the current local or global variable in reverse by
        creating a new `NodesIterReversed` object and returning it.

        :returns: A NodesIterReversed object.
        """
        return NodesIterReversed(self._name, self._subsarray)


class NodesIterReversed:
    """
    Iterator class for iterating in reverse over YottaDB local or global variable nodes starting from the node
    specified by the `name` and `subsarray` pair passed to the `__init__()` constructor.
    """

    def __init__(self, name: AnyStr, subsarray: Tuple[AnyStr] = ()):
        """
        Creates a `NodesIterReversed` class object from the local or global variable node specified
        by the `name` and `subsarray` pair.

        :param name: A bytes-like object representing a YottaDB local or global variable name.
        :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
        :returns: A `NodesIterReversed` object.
        """
        self._name = name
        self._subsarray = [bytes(x, encoding="UTF-8") if isinstance(x, str) else x for x in subsarray]
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
        the `self._name` and `self.reversed` pair, and updates `self.reversed` with this new subscript array
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
            if 0 < data(self._name, self._subsarray):
                self.reversed.append("")
            while not self.initialized:
                try:
                    # If there is another subscript level, add its last subscript to the subscript list
                    self.reversed.insert(len(self.reversed) - 1, subscript_previous(self._name, self.reversed))
                except YDBNodeEnd:
                    # Remove "" subscript now that the search for the last node is complete
                    self.reversed.pop()
                    self.initialized = True
            return tuple(self.reversed)

        try:
            self.reversed = node_previous(self._name, self.reversed)
        except YDBNodeEnd:
            raise StopIteration

        return self.reversed

    def __reversed__(self):
        """
        Creates a new iterable for iterating over nodes following the current local or global variable by
        creating a new `NodesIter` object and returning it.

        :returns: A NodesIter object.
        """
        return NodesIter(self._name, self.subarray)


def nodes(name: AnyStr, subsarray: Tuple[AnyStr] = ()) -> NodesIter:
    """
    A convenience function that yields a `NodesIter` class object from the local or global
    variable node specified by the `name` and `subsarray` pair, providing a more readable
    interface for generating `NodesIter` objects than calling the class constructor.

    :param name: A bytes-like object representing a YottaDB local or global variable name.
    :param subsarray: A tuple of bytes-like objects representing an array of YottaDB subscripts.
    :returns: A `NodesIter` object.
    """
    return NodesIter(name, subsarray)


class Node:
    """
    A class that represents a single YottaDB local or global variable node and supplies methods
    for performing various database operations on or relative to that node.
    """

    _name: AnyStr
    _subsarray: List
    _mutable: bool

    def __init__(self, name: AnyStr, subsarray: List = None) -> Node:
        """
        Creates a `Node` class object from the local or global variable node specified
        by the `name` and `subsarray` pair.

        :param name: A bytes-like object representing a YottaDB local or global
            variable name when `subsarray` is `None`, or a subscript name otherwise.
        :param subsarray: A list or tuple object containing bytes-like objects
            representing a subscript array.
        :returns: A `Node` object.
        """
        if not isinstance(name, str) and not isinstance(name, bytes):
            raise TypeError("'name' must be an instance of str or bytes")

        # Set the variable name, based on the passed `name`
        self._name = name
        # Make the new Node immutable by default
        self._mutable = False
        # Set the subsarray field, based on the passed `subsarray` list.
        if subsarray is None:
            # If no subsarray was specified, initialize an empty list for
            # compatibility with methods that expect subsarray to be a list
            self._subsarray = []
        else:
            # Take a shallow copy of the received subsarray to prevent mutation side-effects
            if isinstance(subsarray, list):
                self._subsarray = subsarray.copy()
            elif isinstance(subsarray, tuple):
                self._subsarray = list(subsarray)
            else:
                raise TypeError("'subsarray' must be an instance of list or tuple")

        if _yottadb.YDB_MAX_SUBS < len(self._subsarray):
            raise ValueError(f"Cannot create Node with {len(self._subsarray)} subscripts (max: {_yottadb.YDB_MAX_SUBS})")

    def __repr__(self) -> str:
        """
        Produces a string representation of the current `Node` object that may be used to reproduce the object if passed to `eval()`.

        :returns: A string representation of the current `Node` object for passage to `eval()`.
        """
        result = f'{self.__class__.__name__}("{self._name}"'
        if len(self._subsarray) > 0:
            result += f', ("{self._subsarray[0]}"'
            if len(self._subsarray) == 1:
                # Only 1 subscript, so add only a comma to make a single-element tuple
                result += ","
            for subscript in self._subsarray[1:]:
                result += f', "{subscript}"'
            result += f")"
        result += f")"
        return result

    def __str__(self) -> str:
        """
        Produces a human-readable string representation of the current `Node` object.

        :returns: A human-readable string representation of the current `Node` object.
        """
        # Convert to ZWRITE format to allow decoding of binary blobs into `str` objects
        subscripts = ",".join([str2zwr(sub).decode("ascii") for sub in self._subsarray])
        if subscripts == "":
            return self._name
        else:
            return f"{self._name}({subscripts})"

    def __setitem__(self, item: AnyStr, value: AnyStr) -> None:
        """
        Sets the value of the local or global variable node specified by the current `Node` object with
        `item` appended to its subscript array to the value specified by `value`. This is done by creating
        a new `Node` object from the current one in combination with the subscript name specified by `item`.

        :param item: A bytes-like object representing a YottaDB subscript name.
        :param value: A bytes-like object representing the value of a YottaDB local or global variable node.
        :returns: None.
        """
        # Use the set() function instead of creating a new `Node` object to reduce overhead
        set(self._name, self._subsarray + [item], value)

    def __getitem__(self, item):
        """
        Creates a new `Node` object representing the local or global variable node specified by the current
        `Node` object with `item` appended to its subscript array.

        :param item: A bytes-like object representing a YottaDB subscript name.
        :returns: A new `Node` object.
        """
        return Node(self._name, self._subsarray + [item])

    def __iadd__(self, num: Union[int, float, str, bytes]) -> Node:
        """
        Increments the value of the local or global variable node specified by
        the current `Node` object
        by the amount specified by `num`.

        :param num: A numeric value specifying the amount by which to increment the given node.
        :returns: The current `Node` object.
        """
        self.incr(num)
        return self

    def __isub__(self, num: Union[int, float, str, bytes]) -> Node:
        """
        Decrements the value of the local or global variable node specified by the current `Node` object
        by the amount specified by `num`.

        :param num: A numeric value specifying the amount by which to decrement the given node.
        :returns: The current `Node` object.
        """
        if isinstance(num, float):
            self.incr(-float(num))
        else:
            self.incr(-int(num))
        return self

    def __eq__(self, other) -> bool:
        """
        Evaluates whether the current `Node` object represents the same YottaDB local or global variable name as `other`.

        :param other: A `Node` object representing a valid YottaDB local or global variable node.
        :returns: True if the two `Node`s represent the same node, or False otherwise.
        """
        if isinstance(other, Node):
            return self._name == other.name and self._subsarray == other.subsarray
        else:
            return self.value == other

    def __iter__(self) -> Generator:
        """
        A Generator that successively yields mutable `Node` objects representing each child node of the local or
        global variable node represented by the calling `Node` object.

        :returns: A `Node` object representing a child node of the local or global variable node represented by the calling `Node` object.
        """
        # Duplicate calling Node with next subscript level initialized to ""
        # to prevent mutation of caller during successive iterations
        next_node = self("")
        # Flag the new node as mutable to signal to users of the new object
        # that it may change on subsequent loop iterations
        next_node._mutable = True
        while True:
            try:
                next_node._subsarray[-1] = next_node.subscript_next()
                yield next_node
            except YDBNodeEnd:
                return

    def __reversed__(self) -> Generator:
        """
        A Generator that returns the a `Node` object representing the node at the previous subscript relative to the
        local or global variable node represented by the current `Node` object on each iteration.

        :returns: A `Node` object representing the node at the previous subscript relative to the local or global variable.
        """
        # Duplicate calling Node with next subscript level initialized to ""
        # to prevent mutation of caller during successive iterations
        prev_node = self("")
        # Flag the new node as mutable to signal to users of the new object
        # that it may change on subsequent loop iterations
        prev_node._mutable = True
        while True:
            try:
                prev_node._subsarray[-1] = prev_node.subscript_previous()
                yield prev_node
            except YDBNodeEnd:
                return

    def __call__(self, *args) -> Node:
        """
        Create a new `Node` object from the current `Node` object and a list of
        subscripts, appending the new subscript list to the subscript array of
        the calling `Node` object.

        :param args: A list of bytes-like objects representing the YottaDB subscripts.
        :returns: None.
        """
        return Node(self._name, self._subsarray + list(args))

    def get(self) -> Optional[bytes]:
        """
        Retrieve the value of the local or global variable node represented by
        the current `Node` object.

        :returns: If the specified node has a value, returns it as a bytes object. If not, returns None.
        """
        return get(self._name, self._subsarray)

    def set(self, value: AnyStr = "") -> None:
        """
        Set the local or global variable node represented by the current `Node` object.

        :param value: A bytes-like object representing the value of a YottaDB local or global variable node.
        :returns: None.
        """
        return set(self._name, self._subsarray, value)

    def mutate(self, name: AnyStr) -> Node:
        """
        Return the `Node` object with its final subscript (or variable name, if there are no subscripts) changed to value in name.
        If the supplied node is immutable, create a mutable copy of it before changing the final subscript.

        :param name: A bytes-like object representing the a YottaDB variable name or subscript.
        :returns: A new, mutable `Node` object.
        """
        if len(self._subsarray) > 0:
            if self.mutable:
                self._subsarray[-1] = name
                mutable = self
            else:
                mutable = Node(self._name, self._subsarray[:-1] + [name])
        else:
            if self.mutable:
                self._name = name
                mutable = self
            else:
                mutable = Node(name)
        if not mutable._mutable:
            mutable._mutable = True
        return mutable

    def copy(self) -> Node:
        """
        Create an immutable copy of the calling `Node` object.

        :returns: A new, immutable `Node` object that duplicates the caller.
        """
        return Node(self._name, self._subsarray)

    @property
    def data(self) -> int:
        """
        Get the following information about the status of the local or global variable node represented
        by the current `Node` object.

        0: There is neither a value nor a subtree, i.e., it is undefined.
        1: There is a value, but no subtree
        10: There is no value, but there is a subtree.
        11: There are both a value and a subtree.

        :returns: 0, 1, 10, or 11, representing the various possible statuses of the specified node.
        """
        return data(self._name, self._subsarray)

    @property
    def leaf(self) -> AnyStr:
        """
        Return the last subscript in the subscript array defining the calling
        `Node` object, or the variable name if there are no subscripts.

        :returns: A bytes-like object representing the leaf node of the calling `Node` object.
        """
        if len(self._subsarray) > 0:
            return self._subsarray[-1]
        else:
            return self._name

    @property
    def parent(self) -> AnyStr:
        """
        Return a `Node` representing the parent node of the node represented by the calling `Node` object.

        :returns: A `Node` object representing the parent node of the calling `Node` object.
        """
        return Node(self._name, self._subsarray[:-1])

    @property
    def name(self) -> AnyStr:
        return self._name

    @property
    def subsarray(self) -> List[AnyStr]:
        return self._subsarray

    @property
    def mutable(self) -> bool:
        return self._mutable

    def delete_node(self) -> None:
        """
        Deletes the value at the local or global variable node represented by the current `Node` object.

        :returns: None.
        """
        delete_node(self._name, self._subsarray)

    def delete_tree(self) -> None:
        """
        Deletes the value and any subtree of the local or global variable node represented by the current `Node` object.

        :returns: None.
        """
        delete_tree(self._name, self._subsarray)

    def incr(self, increment: Union[int, float, str, bytes] = "1") -> bytes:
        """
        Increments the value of the local or global variable node represented by the current `Node` object
        by the amount specified by `increment`.

        :param increment: A numeric value specifying the amount by which to increment the given node.
        :returns: The new value of the node as a bytes object.
        """
        # incr() will enforce increment type
        return incr(self._name, self._subsarray, increment)

    def subscript_next(self) -> AnyStr:
        """
        Return the next subscript at the subscript level of the calling Node object. If there are no more subscripts
        at the given level, raise a `YDBNodeEnd` exception.

        :returns: The next subscript at the given subscript level as a bytes object.
        """
        if len(self._subsarray) == 0:
            return subscript_next(self._name)
        else:
            return subscript_next(self._name, self._subsarray)

    def subscript_previous(self, reset: bool = False) -> bytes:
        """
        Return the previous subscript at the subscript level of the calling Node object. If there are no more subscripts
        at the given level, raise a `YDBNodeEnd` exception.

        :returns: The previous subscript at the given subscript level as a bytes object.
        """
        if len(self._subsarray) == 0:
            return subscript_previous(self._name)
        else:
            return subscript_previous(self._name, self._subsarray)

    def lock(self, timeout_nsec: int = 0) -> None:
        """
        Release any locks held by the process, and attempt to acquire a lock on the local or global variable node
        represented by the current `Node` object.

        The specified locks are released unconditionally, except in the case of an error. On return, the function will have acquired
        the requested lock or else no locks.

        :param timeout_nsec: The time in nanoseconds that the function waits to acquire the requested locks.
        :returns: None.
        """
        return lock((self,), timeout_nsec)

    def lock_incr(self, timeout_nsec: int = 0) -> None:
        """
        Without releasing any locks held by the process attempt to acquire a lock on the local or global
        variable node represented by the current `Node` object, incrementing it if already held.

        :param timeout_nsec: The time in nanoseconds that the function waits to acquire the requested lock.
        :returns: None.
        """
        return lock_incr(self._name, self._subsarray, timeout_nsec)

    def lock_decr(self) -> None:
        """
        Decrements the lock count held by the process on the local or global variable node represented by
        the current `Node` object.

        If the lock count goes from 1 to 0 the lock is released. If the specified lock is not held by the
        process calling `lock_decr()`, the call is ignored.

        :returns: None.
        """
        return lock_decr(self._name, self._subsarray)

    def load_tree(self) -> dict:
        return load_tree(self, first_call=True)

    def save_tree(self, tree: dict, node: Node = None):
        """
        Stores data from a nested Python dictionary in YottaDB. The dictionary must have been previously created using the
        `Node.load_tree()` method, or otherwise match the format used by that method.

        :param tree: A Python dictionary representing a YottaDB tree or subtree.
        :param node: A `Node` object representing the YottaDB database node that is the root of the tree
            structure represented by `tree`.
        """
        if node is None:
            node = self
        # Recurse through each node in the dictionary and, when a leaf node is encountered,
        # store the value in the database.
        for sub, value in tree.items():
            if isinstance(value, dict):
                self.save_tree(value, node[sub])
            elif sub == "value":
                node.value = value
            else:
                assert False

    def replace_tree(self, tree: dict):
        """
        Stores data from a nested Python dictionary in YottaDB. The dictionary must have been previously created using the
        `Node.load_tree()` method, or otherwise match the format used by that method.

        :param tree: A Python dictionary representing a YottaDB tree or subtree.
        :param node: A `Node` object representing the YottaDB database node that is the root of the tree
            structure represented by `tree`.
        """
        self.delete_tree()
        self.save_tree(tree)

    def save_json(self, json: object, node: Node = None):
        """
        Saves JSON data stored in a Python object under the YottaDB node represented by the calling `Node` object.

        :param self: A YottaDB `Node` object.
        :param json: A Python object representing a JSON object.
        """
        # Recurse through each item in the JSON object and,
        # when a leaf node is encountered, store the value in the database.
        if node is None:
            node = self
        if isinstance(json, dict):
            if not json:
                # use the same format as lists and strings
                node["\\d"].value = ""
            for sub, value in json.items():
                # There is another level of nested data, continue recursing.
                self.save_json(value, node[sub])
        elif isinstance(json, list):
            # The value is an array, store it in the database piecemeal by index,
            # starting from 1.
            if len(json) == 0:
                # use the same format as dicts and strings
                node["\\l"].value = ""
            else:
                for index, item in enumerate(json, start=1):
                    i = str(index)
                    self.save_json(json[index - 1], node[i])
        else:
            # No more nested data, save the value in the database.
            if not isinstance(json, str):
                json = str(json)
            else:
                # derived from https://github.com/KRMAssociatesInc/JDS-GTM
                node["\\s"].value = ""
            node.value = json

    def load_json(self, node: Node = None, spaces: str = "") -> object:
        """
        Retrieves JSON data stored under the YottaDB database node represented by the calling `Node` object,
        and returns it as a Python object.

        :param self: A YottaDB `Node` object.
        :param result: A Python object containing JSON data loaded from the database.
        :returns: A Python object representing a JSON object.
        """
        result = None
        if node is None:
            node = self
        # Check whether the current node is a value (i.e. a leaf node), or if it has a subtree.
        node_status = node.data
        if node["\\l"].data == 1:
            # The value is an empty JSON array, so just return an empty list.
            return []
        elif node["\\d"].data == 1:
            return {}
        if node_status == 10:
            # The node has a subtree, which represents a JSON array or a JSON object.
            for index, sub in enumerate(node.subscripts, start=1):
                sub = sub.decode("utf-8")
                if str(index) == sub:
                    # The subtree represents a JSON array. This is so when the subscripts form a
                    # series of integers starting from 1, e.g. 1, 2, 3, etc. In that case, treat them
                    # as indices of a list such that their values or subtrees are elements of a list.
                    if result is None:
                        result = []
                    result.append(self.load_json(node[sub], spaces + "\t"))
                else:
                    # The node is a subtree, signifying a nested JSON object must be retrieved.
                    # If necessary, assign a dictionary to store it, then add the results to the
                    # dictionary using the current subscript.
                    if result is None:
                        result = {}
                    result[sub] = self.load_json(node[sub], spaces + "\t")
        elif node_status == 1 or node_status == 11:
            # The node is a value and not a subtree. So, retrieve the value, decode it into a Python string,
            # convert it to an appropriate type, if necessary, then return it to the caller.
            #
            # Note that `node_status` may be 11 in the case of a string literal, due to a "\s" node
            # stored by `Node.save_json()`. In that case, 11 is acceptable.
            value = node.value.decode("utf-8")
            if node["\\s"].data == 1:
                # A `"\s"` node accompanies the given value, signifying that the value is a string literal.
                # In that case, no type conversion is necessary.
                return value
            else:
                # The value is *not* a string literal and must be converted to the appropriate type. So,
                # infer the type here and convert accordingly.
                assert node_status == 1
                if value == "None":
                    return None
                try:
                    return int(value)
                except ValueError:
                    pass
                try:
                    return float(value)
                except ValueError:
                    pass
                try:
                    return bool(value)
                except ValueError:
                    pass
            result = value
        else:
            # `node_status` should not be 0 since the `Node.save_json()` method
            # should not have saved any empty nodes.
            assert False

        return result

    @property
    def value(self) -> Optional[bytes]:
        """
        Retrieve the value of the local or global variable node represented by the current `Node` object.

        :returns: If the specified node has a value, returns it as a bytes object. If not, returns None.
        """
        return get(self._name, self._subsarray)

    @value.setter
    def value(self, value: AnyStr) -> None:
        """
        Set the value of the local or global variable node represented by the current `Node` object.

        :param value: A bytes-like object representing the value of a YottaDB local or global variable node.
        :returns: None.
        """
        # Value must be str or bytes
        set(self._name, self._subsarray, value)

    @property
    def has_value(self):
        """
        Indicates whether the local or global variable node represented by the current `Node` object
        has a value or not.

        :returns: `True` if the node has a value, `False` otherwise.
        """
        if self.data == YDB_DATA_VALUE_NODESC or self.data == YDB_DATA_VALUE_DESC:
            return True
        else:
            return False

    @property
    def has_subtree(self):
        """
        Indicates whether the local or global variable node represented by the current `Node` object
        has a subtree or not.

        :returns: `True` if the node has a subtree, `False` otherwise.
        """
        if self.data == YDB_DATA_NOVALUE_DESC or self.data == YDB_DATA_VALUE_DESC:
            return True
        else:
            return False

    @property
    def has_both(self):
        """
        Indicates whether the local or global variable node represented by the current `Node` object
        has both a value and a subtree, or not.

        :returns: `True` if the node has both a value and a subtree, `False` otherwise.
        """
        if self.data == YDB_DATA_VALUE_DESC:
            return True
        else:
            return False

    @property
    def has_neither(self):
        """
        Indicates whether the local or global variable node represented by the current `Node` object
        has neither a value nor a subtree, or not.

        :returns: `True` if the node has neither a value nor a subtree, `False` otherwise.
        """
        if self.data == YDB_DATA_UNDEF:
            return True
        else:
            return False

    @property
    def subscripts(self) -> Generator:
        """
        A Generator that successively yields the names of all the child subscripts of the
        local or global variable node represented by the current `Node` object on each iteration.

        :returns: A bytes objects representing a child subscript of the local or global variable node represented by the calling `Node` object.
        """
        if len(self._subsarray) > 0:
            assert isinstance(self._subsarray, list)
            subscript_subsarray = self._subsarray.copy()
        else:
            subscript_subsarray: List[AnyStr] = []
        subscript_subsarray.append("")
        while True:
            try:
                sub_next = subscript_next(self._name, subscript_subsarray)
                subscript_subsarray[-1] = sub_next
                yield sub_next
            except YDBNodeEnd:
                return


class Key(Node):
    """
    Duplicates Node for backward compatibility with YDBPython versions < 2.0.
    """

    pass


# Defined after Node and Key classes to allow access to them
def lock(nodes: Tuple[Node, Key, Tuple[AnyStr, Tuple[AnyStr]]] = None, timeout_nsec: int = 0) -> None:
    """
    Release any locks held by the process, and attempt to acquire all the locks named by `nodes`. Each element
    of `nodes` must be a tuple containing a bytes-like object representing a YottaDB local or global variable name
    and another tuple of bytes-like objects representing a subscript array. Together, these compose a single YottaDB
    node specification. For example, `("^myglobal", ("sub1", "sub2"))` represents the YottaDB node `^myglobal("sub1","sub2")`.

    The specified locks are released unconditionally, except in the case of an error. On return, the function will
    have acquired all requested locks or none of them. If no locks are requested (`nodes` is empty), the function releases all
    locks and returns `None`.

    :param nodes: A tuple of tuples, each representing a YottaDB local or global variable node.
    :param timeout_nsec: The time in nanoseconds that the function waits to acquire the requested locks.
    :returns: None.
    """
    if nodes is not None:
        nodes = [(node.name, node.subsarray) if isinstance(node, Node) or isinstance(node, Key) else node for node in nodes]
    return _yottadb.lock(nodes=nodes, timeout_nsec=timeout_nsec)


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
