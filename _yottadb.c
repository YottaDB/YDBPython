/****************************************************************
 *                                                              *
 * Copyright (c) 2019 Peter Goss All rights reserved.           *
 *                                                              *
 * Copyright (c) 2019 YottaDB LLC and/or its subsidiaries.      *
 * All rights reserved.                                         *
 *                                                              *
 *  This source code contains the intellectual property         *
 *  of its copyright holder(s), and is made available           *
 *  under a license.  If you do not know the terms of           *
 *  the license, please stop and do not read further.           *
 *                                                              *
 ****************************************************************/


#include <stdbool.h>
#include <Python.h>
#include <libyottadb.h>
#include <ffi.h>

#include "_yottadb.h"





/* function that sets up Exception to have both a code and a message */
PyObject* make_getter_code() {
    const char *code;
    PyObject *dict, *output;

    code =
    "@property\n"
    "def code(self):\n"
    "  try:\n"
    "    return self.args[0]\n"
    "  except IndexError:\n"
    "    return -1\n"
    "@property\n"
    "def message(self):\n"
    "  try:\n"
    "    return self.args[1]\n"
    "  except IndexError:\n"
    "    return ''\n"
    "\n";

    dict = PyDict_New();
    PyDict_SetItemString(dict, "__builtins__", PyEval_GetBuiltins());
    output = PyRun_String(code,Py_file_input,dict,dict);
    if (NULL == output) {
        Py_DECREF(dict);
        return NULL;
    }
    Py_DECREF(output);
    PyDict_DelItemString(dict,"__builtins__"); /* __builtins__ should not be an attribute of the exception */

    return dict;
}

/* LOCAL UTILITY FUNCTIONS */

/* ARRAY OF YDB_BUFFER_T UTILITIES */

/* Routine to create an array of empty ydb_buffer_ts with num elements each with an allocated length of len
 *
 * Parameters:
 *   num    - the number of buffers to allocate in the array
 *   len    - the length of the string to allocate for each of the the ydb_buffer_ts
 *
 * free with free_buffer_array function below
 */
static ydb_buffer_t* empty_buffer_array(int num, int len) {
    int i;
    ydb_buffer_t *return_buffer_array;

    return_buffer_array = (ydb_buffer_t*)malloc((num) * sizeof(ydb_buffer_t));
    for(i = 0; i < num; i++)
        YDB_MALLOC_BUFFER(&return_buffer_array[i], len);

    return return_buffer_array;
}

/* Routine to free an array of ydb_buffer_ts
 *
 * Parameters:
 *   array     - pointer to the array of ydb_buffer_ts to be freed.
 *     len    - number of elements in the array to be freed
 *
 */
static void free_buffer_array(ydb_buffer_t *array, int len) {
    for(int i = 0; i < len; i++)
        YDB_FREE_BUFFER(&((ydb_buffer_t*)array)[0]);
}


/* UTILITIES TO CONVERT BETWEEN SEQUENCES OF PYUNICODE STRING OBJECTS AND AN ARRAY OF YDB_BUFFER_TS */

/* Routine to validate that the PyObject passed to it is indeed an array of python bytes objects.
 *
 * Parameters:
 *   sequence    - the python object to check.
 */
static bool validate_sequence_of_bytes(PyObject *sequence) {
    int i, len_seq;
    PyObject *item, *seq;

    seq = PySequence_Fast(sequence, "argument must be iterable");
    if (!seq || PyBytes_Check(sequence)) /* PyBytes it's self is a sequence */
        return false;

    len_seq = PySequence_Fast_GET_SIZE(seq);
    for (i=0; i < len_seq; i++) { /* check each item for a bytes object */
        item = PySequence_Fast_GET_ITEM(seq, i);
        if (!PyBytes_Check(item))
            return false;
    }
    return true;
}

/* Routine to validate a 'subsarray' argument in many of the wrapped functions. The 'subsarray' argument must be a sequence
 * of Python bytes objects or a Python None. Will set Exception String and return 'false' if invalid and return 'true' otherwise.
 * (Calling function is expected to return NULL to cause the Exception to be raised.)
 *
 * Parameters:
 *   subsarray    - the Python object to validate.
 */
static bool validate_subsarray_object(PyObject *subsarray) {
     if (Py_None != subsarray) {
        if (!validate_sequence_of_bytes(subsarray)) {
            /* raise TypeError */
            PyErr_SetString(PyExc_TypeError, "'subsarray' must be a Sequence (e.g. List or Tuple) containing only bytes or None");
            return false;
        }
    }
    return true;
}

/* Routine to convert a sequence of Python bytes into a C array of ydb_buffer_ts. Routine assumes sequence was already
 * validated with 'validate_sequence_of_bytes' function. The function creates a copy of each Python bytes' data so
 * the resulting array should be freed by using the 'free_buffer_array' function.
 *
 * Parameters:
 *    sequence    - a Python Object that is expected to be a Python Sequence containing Strings.
 */
ydb_buffer_t* convert_py_bytes_sequence_to_ydb_buffer_array(PyObject *sequence) {
    bool done;
    int sequence_len, bytes_len;
    char *bytes_c;
    PyObject *bytes;
    ydb_buffer_t *return_buffer_array;

    sequence_len = PySequence_Length(sequence);
    return_buffer_array = (ydb_buffer_t*)malloc((sequence_len) * sizeof(ydb_buffer_t));
    for(int i = 0; i < sequence_len; i++) {
        bytes = PySequence_GetItem(sequence, i);
        bytes_len = PyBytes_Size(bytes);
        bytes_c = PyBytes_AsString(bytes);
        YDB_MALLOC_BUFFER(&return_buffer_array[i], bytes_len);
        YDB_COPY_STRING_TO_BUFFER(bytes_c, &return_buffer_array[i], done);
        // figure out how to handle error case (done == false)

        Py_DECREF(bytes);
    }
    return return_buffer_array;
}

/* converts an array of ydb_buffer_ts into a sequence (Tuple) of Python strings.
 *
 * Parameters:
 *    buffer_array        - a C array of ydb_buffer_ts
 *    len                - the length of the above array
 */
PyObject* convert_ydb_buffer_array_to_py_tuple(ydb_buffer_t *buffer_array, int len) {
    int i;
    PyObject *return_tuple;

    return_tuple = PyTuple_New(len);
    for(i=0; i < len; i++)
        PyTuple_SetItem(return_tuple, i, Py_BuildValue("y#", buffer_array[i].buf_addr, buffer_array[i].len_used));

    return return_tuple;
}

/* UTILITIES TO CONVERT BETWEEN DATABASE KEYS REPRESENTED USING PYTHON OBJECTS AND YDB C API TYPES */

/* The workhorse routine of a couple of routines that convert from Python objects (varname and subsarray) to YDB keys.
 *
 * Parameters:
 *    dest        - pointer to the YDBKey to fill.
 *    varname    - Python Bytes object representing the varname
 *    subsarray    - array of python Bytes objects representing the array of subcripts
 */

static void load_YDBKey(YDBKey *dest, PyObject *varname, PyObject *subsarray) {
    bool copy_success;
    int len;
    char* bytes_c;
    ydb_buffer_t *varanme_y;

    len = PyBytes_Size(varname);
    bytes_c = PyBytes_AsString(varname);

    varanme_y = (ydb_buffer_t*)calloc(1, sizeof(ydb_buffer_t));
    YDB_MALLOC_BUFFER(varanme_y, len);
    YDB_COPY_STRING_TO_BUFFER(bytes_c, varanme_y, copy_success);
    // raise error on !copy_success

    dest->varname = varanme_y;
    if (Py_None != subsarray) {
        dest->subs_used = PySequence_Length(subsarray);
        dest->subsarray = convert_py_bytes_sequence_to_ydb_buffer_array(subsarray);
    } else {
        dest->subs_used = 0;
    }
}

/* Routine to free a YDBKey structure.
 *
 * Parameters:
 *    key    - pointer to the YDBKey to free.
 */
static void free_YDBKey(YDBKey* key) {
    int i;

    YDB_FREE_BUFFER((key->varname));
    for (i=0; i < key->subs_used; i++)
        YDB_FREE_BUFFER(&((ydb_buffer_t*)key->subsarray)[i]);
}

/* Routine to validate a sequence of Python sequences representing keys. (Used only by lock().)
 * Validation rule:
 *        1) key_sequence must be a sequence
 *        2) each item in key_sequence must be a sequence
 *        3) each item must be a sequence of 1 or 2 sub-items.
 *        4) item[0] must be a bytes object.
 *        5) item[1] either does not exist, is None or a sequence
 *        6) if item[1] is a sequence then it must contain only bytes objects.
 *
 * Parameters:
 *    keys_sequence        - a Python object that is to be validated.
 */
static bool validate_py_keys_sequence_bytes(PyObject* keys_sequence) {
    int i, len_keys;
    PyObject *key, *varname, *subsarray;

    if (!PySequence_Check(keys_sequence)) {
        PyErr_SetString(PyExc_TypeError, "'keys' argument must be a Sequence (e.g. List or Tuple) containing  sequences of 2 values"
                        "the first being a bytes object(varname) and the following being a sequence of bytes objects(subsarray)");
        return false;
    }
    len_keys = PySequence_Length(keys_sequence);
    for (i=0; i < len_keys; i++) {
        key = PySequence_GetItem(keys_sequence, i);
        if (!PySequence_Check(key)) {
            PyErr_Format(PyExc_TypeError, "item %d in 'keys' sequence is not a sequence", i);
            Py_DECREF(key);
            return false;
        } if ((1 != PySequence_Length(key)) && (2 != PySequence_Length(key))) {
            PyErr_Format(PyExc_TypeError, "item %d in 'keys' sequence is not a sequence of 1 or 2 items", i);
            Py_DECREF(key);
            return false;
        }

        varname = PySequence_GetItem(key, 0);
        if (!PyBytes_Check(varname)) {
            PyErr_Format(PyExc_TypeError, "the first value in item %d of 'keys' sequence must be a bytes object", i);
            Py_DECREF(key);
            Py_DECREF(varname);
            return false;
        }
        Py_DECREF(varname);

        if (2 == PySequence_Length(key)) {
            subsarray = PySequence_GetItem(key, 1);
            if (!validate_subsarray_object(subsarray)) {
                /* overwrite Exception string set by 'validate_subsarray_object' to be appropriate for lock context */
                PyErr_Format(PyExc_TypeError,
                                "the second value in item %d of 'keys' sequence must be a sequence of bytes or None", i);
                Py_DECREF(key);
                Py_DECREF(subsarray);
                return false;
            }
            Py_DECREF(subsarray);
        }
        Py_DECREF(key);

    }
    return true;
}

/* Routine to covert a sequence of keys in Python sequences and bytes to an array of YDBKeys. Assumes that the input
 * has already been validated with 'validate_py_keys_sequence' above. Use 'free_YDBKey_array' below to free the returned
 * value.
 *
 * Parameters:
 *    sequence    - a Python object that has already been validated with 'validate_py_keys_sequence' or equivalent.
 */
static YDBKey* convert_key_sequence_to_YDBKey_array(PyObject* sequence) {
    //TODO: change to use fast sequence
    int i, len_keys;
    PyObject *key, *varname, *subsarray;
    YDBKey *ret_keys;
    len_keys = PySequence_Length(sequence);
    ret_keys = (YDBKey*)malloc(len_keys * sizeof(YDBKey));
    for (i=0; i < len_keys; i++) {
        key = PySequence_GetItem(sequence, i);
        varname = PySequence_GetItem(key, 0);
        subsarray = Py_None;

        if (2 == PySequence_Length(key))
            subsarray = PySequence_GetItem(key, 1);
        load_YDBKey(&ret_keys[i], varname, subsarray);
        Py_DECREF(key);
        Py_DECREF(subsarray);
    }
    return ret_keys;
}

/* Routine to free an array of YDBKeys as returned by above 'convert_key_sequence_to_YDBKey_array'.
 *
 * Parameters:
 *    keysarray    - the array that is to be freed.
 *    len        - the number of elements in keysarray.
 */
static void free_YDBKey_array(YDBKey* keysarray, int len) {
    int i;
    for(i = 0; i < len; i++)
        free_YDBKey(&keysarray[i]);
    free(keysarray);
}

/* Routine to help raise a YottaDBError. The caller still needs to return NULL for the Exception to be raised.
 * This routine will check if the message has been set in the error_string_buffer and look it up if not.
 *
 * Parameters:
 *    status                - the error code that is returned by the wrapped ydb_ function.
 *    error_string_buffer    - a ydb_buffer_t that may or may not contain the error message.
 */
static void raise_YottaDBError(int status, ydb_buffer_t* error_string_buffer) {
    int msg_status;
    ydb_buffer_t ignored_buffer;
    PyObject *tuple;

    if (0 == error_string_buffer->len_used) {
        msg_status = ydb_message(status, error_string_buffer);
        if (YDB_ERR_SIMPLEAPINOTALLOWED == msg_status) {
            YDB_MALLOC_BUFFER(&ignored_buffer, YDB_MAX_ERRORMSG)
            ydb_message_t(YDB_NOTTP, &ignored_buffer, status, error_string_buffer);
        }
    }
    tuple = PyTuple_New(2);
    PyTuple_SetItem(tuple, 0, PyLong_FromLong(status));
    PyTuple_SetItem(tuple, 1, Py_BuildValue("s#", error_string_buffer->buf_addr, error_string_buffer->len_used));
    PyErr_SetObject(YottaDBError, tuple);
}


/* SIMPLE AND SIMPLE THREADED API WRAPPERS */

/* FOR ALL PROXY FUNCTIONS BELOW
 * do almost nothing themselves, simply calls wrapper with a flag for which API they mean to call.
 *
 * Parameters:
 *
 *    self        - the object that this method belongs to (in this case it's the _yottadb module.
 *    args        - a Python tuple of the positional arguments passed to the function
 *    kwds        - a Python dictonary of the keyword arguments passed the tho function
 */

/* FOR ALL BELOW WRAPPERS:
 * does all the work to wrap the 2 related functions using the threaded flag to make the few modifications related how the
 * simple and simple threaded APIs are different.
 *
 * Parameters:
 *    self, args, kwds    - same as proxy functions.
 *    threaded             - either true for simple_treaded api or false used by the proxy functions to indicate which API was being called.
 *
 * FOR ALL
 */

/* Wrapper for ydb_data_s and ydb_data_st. */
static PyObject* data(PyObject* self, PyObject* args, PyObject* kwds) {
    bool threaded, copy_success;
    bool return_NULL = false;
    char *varname;
    int varname_len, subs_used, status;
    unsigned int *ret_value;
    uint64_t tp_token;
    PyObject *subsarray, *return_python_int;
    ydb_buffer_t error_string_buffer, varname_y, *subsarray_y;

    /* Defaults for non-required arguments */
    subsarray = Py_None;
    tp_token = YDB_NOTTP;

    /* parse and validate */
    static char *kwlist[] = {"threaded", "varname", "subsarray", "tp_token", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "py#|OK", kwlist, &threaded, &varname, &varname_len, &subsarray, &tp_token))
        return NULL;

    if (!validate_subsarray_object(subsarray))
        return NULL;

    /* Setup for Call */
    YDB_MALLOC_BUFFER(&varname_y, varname_len);
    YDB_COPY_STRING_TO_BUFFER(varname, &varname_y, copy_success);
    if (!copy_success) {
        PyErr_SetString(YDBPythonBugError, "YDB_COPY_STRING_TO_BUFFER failed in data()");
        return_NULL = true;
    }
    SETUP_SUBS(subsarray, subs_used, subsarray_y);
    YDB_MALLOC_BUFFER(&error_string_buffer, YDB_MAX_ERRORMSG);
    ret_value = (unsigned int*) malloc(sizeof(unsigned int));

    /* Call the wrapped function */
    CALL_WRAP_4(threaded, ydb_data_s, ydb_data_st, tp_token, &error_string_buffer, &varname_y, subs_used, subsarray_y, ret_value, status);

    /* check status for Errors and Raise Exception */
    if (0 > status) {
        raise_YottaDBError(status, &error_string_buffer);
        return_NULL = true;
    }

    /* Create Python object to return */
    if (!return_NULL)
        return_python_int = Py_BuildValue("I", *ret_value);

    /* free allocated memory */
    YDB_FREE_BUFFER(&varname_y);
    free_buffer_array(subsarray_y, subs_used);
    free(ret_value);
    YDB_FREE_BUFFER(&error_string_buffer);

    if (return_NULL)
        return NULL;
    else
        return return_python_int;
}

/* Wrapper for ydb_delete_s() and ydb_delete_st() */
static PyObject* delete_wrapper(PyObject* self, PyObject* args, PyObject *kwds) {
    bool threaded, copy_success;
    bool return_NULL = false;
    int deltype, status, varname_len, subs_used;
    char *varname;
    uint64_t tp_token;
    PyObject *subsarray;
    ydb_buffer_t error_string_buffer, varname_y, *subsarray_y;

    /* Defaults for non-required arguments */
    subsarray = Py_None;
    tp_token = YDB_NOTTP;
    deltype = YDB_DEL_NODE;

    /* parse and validate */
    static char* kwlist[] = {"threaded", "varname", "subsarray", "delete_type", "tp_token", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "py#|OiK", kwlist, &threaded, &varname, &varname_len, &subsarray, &deltype, &tp_token))
        return NULL;

    if (!validate_subsarray_object(subsarray))
        return NULL;

    /* Setup for Call */
    YDB_MALLOC_BUFFER(&varname_y, varname_len);
    YDB_COPY_STRING_TO_BUFFER(varname, &varname_y, copy_success);
    if (!copy_success) {
        PyErr_SetString(YDBPythonBugError, "YDB_COPY_STRING_TO_BUFFER failed in delete_wrapper()");
        return_NULL = true;
    }
    SETUP_SUBS(subsarray, subs_used, subsarray_y);
    YDB_MALLOC_BUFFER(&error_string_buffer, YDB_MAX_ERRORMSG);

    /* Call the wrapped function */
    CALL_WRAP_4(threaded, ydb_delete_s, ydb_delete_st, tp_token, &error_string_buffer, &varname_y, subs_used, subsarray_y, deltype, status);


    /* check status for Errors and Raise Exception */
    if (0 > status) {
        raise_YottaDBError(status, &error_string_buffer);
        return_NULL = true;
    }

    /* free allocated memory */
    YDB_FREE_BUFFER(&varname_y);
    free_buffer_array(subsarray_y, subs_used);
    YDB_FREE_BUFFER(&error_string_buffer)
    if (return_NULL)
        return NULL;
    else
        return Py_None;
}

/* Wrapper for ydb_delete_excl_s() and ydb_delete_excl_st() */
static PyObject* delete_excel(PyObject* self, PyObject* args, PyObject *kwds) {
    bool threaded;
    bool return_NULL = false;
    int namecount, status;
    uint64_t tp_token;
    PyObject *varnames;
    ydb_buffer_t error_string_buffer;

    /* Defaults for non-required arguments */
    varnames = Py_None;
    tp_token = YDB_NOTTP;

    /* parse and validate */
    static char* kwlist[] = {"threaded", "varnames", "tp_token", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "p|OK", kwlist, &threaded, &varnames, &tp_token))
        return NULL;

    if((varnames != NULL) && (!validate_sequence_of_bytes(varnames))) {
        PyErr_SetString(PyExc_TypeError, "'varnames' must be an sequence of bytes.");
        return NULL;
    }

    /* Setup for Call */
    YDB_MALLOC_BUFFER(&error_string_buffer, YDB_MAX_ERRORMSG);
    namecount = 0;
    if (varnames != Py_None)
        namecount = PySequence_Length(varnames);
    ydb_buffer_t* varnames_ydb = convert_py_bytes_sequence_to_ydb_buffer_array(varnames);

    CALL_WRAP_2(threaded, ydb_delete_excl_s, ydb_delete_excl_st, tp_token, &error_string_buffer, namecount, varnames_ydb, status);

    /* check status for Errors and Raise Exception */
    if (0 > status) {
        raise_YottaDBError(status, &error_string_buffer);
        /* free allocated memory */
        return_NULL = true;
    }

    /* free allocated memory */
    free_buffer_array(varnames_ydb, namecount);
    YDB_FREE_BUFFER(&error_string_buffer);
    if (return_NULL)
        return NULL;
    else
        return Py_None;
}

/* Wrapper for ydb_get_s() and ydb_get_st() */
static PyObject* get(PyObject* self, PyObject* args, PyObject *kwds) {
    bool threaded, copy_success;
    bool return_NULL = false;
    int subs_used, status, return_length, varname_len;
    char *varname;
    uint64_t tp_token;
    PyObject *subsarray, *return_python_string;
    ydb_buffer_t varname_y, error_string_buffer, ret_value, *subsarray_y;

    /* Defaults for non-required arguments */
    subsarray = Py_None;
    tp_token = YDB_NOTTP;

    /* parse and validate */
    static char* kwlist[] = {"threaded", "varname", "subsarray", "tp_token", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "py#|OK", kwlist, &threaded, &varname, &varname_len, &subsarray, &tp_token))
        return NULL;

    if (!validate_subsarray_object(subsarray))
        return NULL;

    /* Setup for Call */
    YDB_MALLOC_BUFFER(&varname_y, varname_len);
    YDB_COPY_STRING_TO_BUFFER(varname, &varname_y, copy_success);
    if (!copy_success) {
        PyErr_SetString(YDBPythonBugError, "YDB_COPY_STRING_TO_BUFFER failed in get()");
        return_NULL = true;
    }
    SETUP_SUBS(subsarray, subs_used, subsarray_y);
    YDB_MALLOC_BUFFER(&error_string_buffer, YDB_MAX_ERRORMSG);
    YDB_MALLOC_BUFFER(&ret_value, 1024);

    /* Call the wrapped function */
    CALL_WRAP_4(threaded, ydb_get_s, ydb_get_st, tp_token, &error_string_buffer, &varname_y, subs_used, subsarray_y, &ret_value, status);

    /* check to see if length of string was longer than 1024 is so, try again with proper length */
    if (YDB_ERR_INVSTRLEN == status) {
        return_length = ret_value.len_used;
        YDB_MALLOC_BUFFER(&ret_value, return_length);
        /* Call the wrapped function */
        CALL_WRAP_4(threaded, ydb_get_s, ydb_get_st, tp_token, &error_string_buffer, &varname_y, subs_used, subsarray_y, &ret_value, status);
    }

    /* check status for Errors and Raise Exception */
    if (0 > status) {
        raise_YottaDBError(status, &error_string_buffer);
        return_NULL = true;

    }
    /* Create Python object to return */
    if (!return_NULL)
        return_python_string = Py_BuildValue("y#", ret_value.buf_addr, ret_value.len_used);

    /* free allocated memory */
    YDB_FREE_BUFFER(&varname_y);
    free_buffer_array(subsarray_y, subs_used);
    YDB_FREE_BUFFER(&error_string_buffer);
    YDB_FREE_BUFFER(&ret_value);
    if (return_NULL)
        return NULL;
    else
        return return_python_string;
}

/* Wrapper for ydb_incr_s() and ydb_incr_st() */
static PyObject* incr(PyObject* self, PyObject* args, PyObject *kwds) {
    bool threaded, copy_success;
    bool return_NULL = false;
    int status, subs_used, varname_len, increment_len;
    uint64_t tp_token;
    char *varname, *increment;
    PyObject *subsarray, *return_python_string;
    ydb_buffer_t increment_y, error_string_buffer, ret_value, varname_y, *subsarray_y;

    /* Defaults for non-required arguments */
    subsarray = Py_None;
    tp_token = YDB_NOTTP;
    increment = "1";
    increment_len = 1;

    /* parse and validate */
    static char* kwlist[] = {"threaded","varname", "subsarray", "increment", "tp_token", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "py#|Oy#K", kwlist, &threaded, &varname, &varname_len, &subsarray,
                                        &increment, &increment_len, &tp_token))
        return NULL;

    if (!validate_subsarray_object(subsarray))
        return NULL;

    /* Setup for Call */
    YDB_MALLOC_BUFFER(&varname_y, varname_len);
    YDB_COPY_STRING_TO_BUFFER(varname, &varname_y, copy_success);
    if (!copy_success) {
        PyErr_SetString(YDBPythonBugError, "YDB_COPY_STRING_TO_BUFFER failed in incr() for varname");
        return_NULL = true;
    }

    SETUP_SUBS(subsarray, subs_used, subsarray_y);

    YDB_MALLOC_BUFFER(&increment_y, increment_len);
    YDB_COPY_STRING_TO_BUFFER(increment, &increment_y, copy_success);
    if (!copy_success) {
        PyErr_SetString(YDBPythonBugError, "YDB_COPY_STRING_TO_BUFFER failed in incr() for increment");
        return_NULL = true;
    }
    YDB_MALLOC_BUFFER(&error_string_buffer, YDB_MAX_ERRORMSG);
    YDB_MALLOC_BUFFER(&ret_value, 50);

    /* Call the wrapped function */
    CALL_WRAP_5(threaded, ydb_incr_s, ydb_incr_st, tp_token, &error_string_buffer, &varname_y, subs_used, subsarray_y, &increment_y,
                &ret_value, status);

    /* check status for Errors and Raise Exception */
    if (0 > status) {
        raise_YottaDBError(status, &error_string_buffer);
        return_NULL = true;
    }

    /* Create Python object to return */
    if (!return_NULL)
        return_python_string = Py_BuildValue("y#", ret_value.buf_addr, ret_value.len_used);

    /* free allocated memory */
    YDB_FREE_BUFFER(&varname_y);
    free_buffer_array(subsarray_y, subs_used);
    YDB_FREE_BUFFER(&increment_y);
    YDB_FREE_BUFFER(&error_string_buffer);
    YDB_FREE_BUFFER(&ret_value);

    if (return_NULL)
        return NULL;
    else
        return return_python_string;
}

/* Wrapper for ydb_lock_s() and ydb_lock_st() */
static PyObject* lock(PyObject* self, PyObject* args, PyObject *kwds) {
    bool threaded;
    bool return_NULL = false;
    int len_keys, initial_arguments, number_of_arguments;
    uint64_t tp_token;
    unsigned long long timeout_nsec;
    ffi_cif call_interface;
    ffi_type *ret_type;
    PyObject *keys, *keys_default;
    ydb_buffer_t *error_string_buffer;
    YDBKey *keys_ydb;

    /* Defaults for non-required arguments */
    timeout_nsec = 0;
    tp_token = YDB_NOTTP;
    keys_default = PyTuple_New(0);
    keys = keys_default;

    /* parse and validate */
    static char* kwlist[] = {"threaded", "keys", "timeout_nsec", "tp_token", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "p|OKK", kwlist, &threaded, &keys, &timeout_nsec, &tp_token))
        return NULL;

    if (!validate_py_keys_sequence_bytes(keys))
        return NULL;
    len_keys = PySequence_Length(keys);

    /* Setup for Call */
    error_string_buffer = (ydb_buffer_t*)calloc(1, sizeof(ydb_buffer_t));
    YDB_MALLOC_BUFFER(error_string_buffer, YDB_MAX_ERRORMSG);
    keys_ydb = convert_key_sequence_to_YDBKey_array(keys);
    Py_DECREF(keys_default);

    /* build ffi call */
    ret_type = &ffi_type_sint;
    if (threaded)
        initial_arguments = 4;
    else if (!threaded)
        initial_arguments = 2;

    number_of_arguments = initial_arguments + (len_keys * 3);
    ffi_type *arg_types[number_of_arguments];
    void *arg_values[number_of_arguments];
    /* ffi signature */
    if (threaded) {
        arg_types[0] = &ffi_type_uint64; // tptoken
        arg_values[0] = &tp_token; // tptoken
        arg_types[1] = &ffi_type_pointer;// errstr
        arg_values[1] = &error_string_buffer; // errstr
        arg_types[2] = &ffi_type_uint64; // timout_nsec
        arg_values[2] = &timeout_nsec; // timout_nsec
        arg_types[3] = &ffi_type_sint; // namecount
        arg_values[3] = &len_keys; // namecount
    } else if (!threaded) {
        arg_types[0] = &ffi_type_uint64; // timout_nsec
        arg_values[0] = &timeout_nsec; // timout_nsec
        arg_types[1] = &ffi_type_sint; // namecount
        arg_values[1] = &len_keys; // namecount
    }

    for (int i = 0; i < len_keys; i++) {
        int first = initial_arguments + 3*i;
        arg_types[first] = &ffi_type_pointer;// varname
        arg_values[first] = &keys_ydb[i].varname; // varname
        arg_types[first + 1] = &ffi_type_sint; // subs_used
        arg_values[first + 1] = &keys_ydb[i].subs_used; // subs_used
        arg_types[first + 2] = &ffi_type_pointer;// subsarray
        arg_values[first + 2] = &keys_ydb[i].subsarray;// subsarray
    }

    int status; // return value
    if (ffi_prep_cif(&call_interface, FFI_DEFAULT_ABI, number_of_arguments, ret_type, arg_types) == FFI_OK) {
        /* Call the wrapped function */
        if (threaded)
            ffi_call(&call_interface, FFI_FN(ydb_lock_st), &status, arg_values);
        else if (!threaded)
            ffi_call(&call_interface, FFI_FN(ydb_lock_s), &status, arg_values);
    } else {
        PyErr_SetString(PyExc_SystemError, "ffi_prep_cif failed ");
        return_NULL = true;
    }

    /* check for errors */
    if (0 > status) {
        raise_YottaDBError(status, error_string_buffer);
        return_NULL = true;
        return NULL;
    } else if (YDB_LOCK_TIMEOUT == status) {
        PyErr_SetString(YottaDBLockTimeout, "Not able to acquire all requested locks in the specified time.");
        return_NULL = true;
    }

    /* free allocated memory */
    YDB_FREE_BUFFER(error_string_buffer);
    free(error_string_buffer);
    free_YDBKey_array(keys_ydb, len_keys);

    if (return_NULL)
        return NULL;
    else
        return Py_None;
}

/* Wrapper for ydb_lock_decr_s() and ydb_lock_decr_st() */
static PyObject* lock_decr(PyObject* self, PyObject* args, PyObject *kwds) {
    bool threaded, copy_success;
    bool return_NULL = false;
    int status, varname_len, subs_used;
    char *varname;
    uint64_t tp_token;
    PyObject *subsarray;
    ydb_buffer_t error_string_buffer, varname_y, *subsarray_y;

    /* Defaults for non-required arguments */
    subsarray = Py_None;
    tp_token = YDB_NOTTP;

    /* parse and validate */
    static char* kwlist[] = {"threaded", "varname", "subsarray", "tp_token", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "py#|OK", kwlist, &threaded, &varname, &varname_len, &subsarray, &tp_token))
        return NULL;

    if (!validate_subsarray_object(subsarray))
        return NULL;

    /* Setup for Call */
    YDB_MALLOC_BUFFER(&varname_y, varname_len);
    YDB_COPY_STRING_TO_BUFFER(varname, &varname_y, copy_success);
    if (!copy_success) {
        PyErr_SetString(YDBPythonBugError, "YDB_COPY_STRING_TO_BUFFER failed in lock_decr()");
        return_NULL = true;
    }
    SETUP_SUBS(subsarray, subs_used, subsarray_y);
    YDB_MALLOC_BUFFER(&error_string_buffer, YDB_MAX_ERRORMSG);

    /* Call the wrapped function */
    CALL_WRAP_3(threaded, ydb_lock_decr_s, ydb_lock_decr_st, tp_token, &error_string_buffer, &varname_y, subs_used, subsarray_y, status);

    /* check status for Errors and Raise Exception */
    if (0 > status) {
        raise_YottaDBError(status, &error_string_buffer);
        return_NULL = true;
    }

    /* free allocated memory */
    YDB_FREE_BUFFER(&varname_y);
    free_buffer_array(subsarray_y, subs_used);
    YDB_FREE_BUFFER(&error_string_buffer);

    if (return_NULL)
        return NULL;
    else
        return Py_None;
}

/* Wrapper for ydb_lock_incr_s() and ydb_lock_incr_st() */
static PyObject* lock_incr(PyObject* self, PyObject* args, PyObject *kwds) {
    bool threaded, copy_success;
    bool return_NULL = false;
    int status, varname_len, subs_used;
    char *varname;
    uint64_t tp_token;
    unsigned long long timeout_nsec;
    PyObject *subsarray;
    ydb_buffer_t error_string_buffer, varname_y, *subsarray_y;

    /* Defaults for non-required arguments */
    subsarray = Py_None;
    timeout_nsec = 0;
    tp_token = YDB_NOTTP;

    /* parse and validate */
    static char* kwlist[] = {"threaded", "varname", "subsarray","timeout_nsec",  "tp_token", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "py#|OLK", kwlist, &threaded, &varname, &varname_len, &subsarray, &timeout_nsec, &tp_token))
        return NULL;

    if (!validate_subsarray_object(subsarray))
        return NULL;
    /* Setup for Call */
    YDB_MALLOC_BUFFER(&varname_y, varname_len);
    YDB_COPY_STRING_TO_BUFFER(varname, &varname_y, copy_success);
    if (!copy_success) {
        PyErr_SetString(YDBPythonBugError, "YDB_COPY_STRING_TO_BUFFER failed in lock_incr()");
        return_NULL = true;
    }
    SETUP_SUBS(subsarray, subs_used, subsarray_y);
    YDB_MALLOC_BUFFER(&error_string_buffer, YDB_MAX_ERRORMSG);
    /* Call the wrapped function */
    CALL_WRAP_4(threaded, ydb_lock_incr_s, ydb_lock_incr_st, tp_token, &error_string_buffer, timeout_nsec, &varname_y,
                subs_used, subsarray_y, status);

    /* check status for Errors and Raise Exception */
    if (0 > status) {
        raise_YottaDBError(status, &error_string_buffer);
        return_NULL = true;
    } else if (YDB_LOCK_TIMEOUT == status) {
        PyErr_SetString(YottaDBLockTimeout, "Not able to acquire all requested locks in the specified time.");
        return_NULL = true;
    }

    /* free allocated memory */
    YDB_FREE_BUFFER(&varname_y);
    free_buffer_array(subsarray_y, subs_used);
    YDB_FREE_BUFFER(&error_string_buffer);

    if (return_NULL)
        return NULL;
    else
        return Py_None;
}

/* Wrapper for ydb_node_next_s() and ydb_node_next_st() */
static PyObject* node_next(PyObject* self, PyObject* args, PyObject *kwds) {
    bool threaded, copy_success;
    bool return_NULL = false;
    int max_subscript_string, default_ret_subs_used, real_ret_subs_used, ret_subs_used, status, varname_len, subs_used;
    char *varname;
    uint64_t tp_token;
    PyObject *subsarray, *return_tuple;
    ydb_buffer_t error_string_buffer, *ret_subsarray, varname_y, *subsarray_y;

    /* Defaults for non-required arguments */
    subsarray = Py_None;
    tp_token = YDB_NOTTP;

    /* parse and validate */
    static char* kwlist[] = {"threaded", "varname", "subsarray", "tp_token", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "py#|OK", kwlist, &threaded, &varname, &varname_len, &subsarray, &tp_token))
        return NULL;

    if (!validate_subsarray_object(subsarray))
        return NULL;

    /* Setup for Call */
    YDB_MALLOC_BUFFER(&varname_y, varname_len);
    YDB_COPY_STRING_TO_BUFFER(varname, &varname_y, copy_success);
    if (!copy_success) {
        PyErr_SetString(YDBPythonBugError, "YDB_COPY_STRING_TO_BUFFER failed in node_next()");
        return_NULL = true;
    }
    SETUP_SUBS(subsarray, subs_used, subsarray_y);
    YDB_MALLOC_BUFFER(&error_string_buffer, YDB_MAX_ERRORMSG);
    max_subscript_string = 1024;
    default_ret_subs_used = subs_used + 5;
    if (YDB_MAX_SUBS < default_ret_subs_used)
        default_ret_subs_used = YDB_MAX_SUBS;
    real_ret_subs_used = default_ret_subs_used;
    ret_subs_used = default_ret_subs_used;
    ret_subsarray = empty_buffer_array(ret_subs_used, max_subscript_string);

    /* Call the wrapped function */
    CALL_WRAP_5(threaded, ydb_node_next_s, ydb_node_next_st, tp_token, &error_string_buffer, &varname_y, subs_used, subsarray_y,
                &ret_subs_used, ret_subsarray, status);

    /* If not enough buffers in ret_subsarray */
    if (YDB_ERR_INSUFFSUBS == status) {
        free_buffer_array(ret_subsarray, default_ret_subs_used);
        real_ret_subs_used = ret_subs_used;
        ret_subsarray = empty_buffer_array(real_ret_subs_used, max_subscript_string);
        /* recall the wrapped function */
        CALL_WRAP_5(threaded, ydb_node_next_s, ydb_node_next_st, tp_token, &error_string_buffer, &varname_y, subs_used, subsarray_y,
                    &ret_subs_used, ret_subsarray, status);
    }

    /* if a buffer is not long enough */
    while(YDB_ERR_INVSTRLEN == status) {
        max_subscript_string = ret_subsarray[ret_subs_used].len_used;
        free(ret_subsarray[ret_subs_used].buf_addr);
        ret_subsarray[ret_subs_used].buf_addr = (char*)malloc(ret_subsarray[ret_subs_used].len_used*sizeof(char));
        ret_subsarray[ret_subs_used].len_alloc = ret_subsarray[ret_subs_used].len_used;
        ret_subsarray[ret_subs_used].len_used = 0;
        ret_subs_used = real_ret_subs_used;
        /* recall the wrapped function */
        CALL_WRAP_5(threaded, ydb_node_next_s, ydb_node_next_st, tp_token, &error_string_buffer, &varname_y, subs_used, subsarray_y,
                    &ret_subs_used, ret_subsarray, status);
    }

    /* check status for Errors and Raise Exception */
    if (0 > status) {
        raise_YottaDBError(status, &error_string_buffer);
        return_NULL = true;
    }
    /* Create Python object to return */
    return_tuple = convert_ydb_buffer_array_to_py_tuple(ret_subsarray, ret_subs_used);

    /* free allocated memory */
    YDB_FREE_BUFFER(&varname_y);
    free_buffer_array(subsarray_y, subs_used);
    YDB_FREE_BUFFER(&error_string_buffer);
    free_buffer_array(ret_subsarray, real_ret_subs_used);

    if (return_NULL)
        return NULL;
    else
        return return_tuple;
}

/* Wrapper for ydb_node_previous_s() and ydb_node_previous_st() */
static PyObject* node_previous(PyObject* self, PyObject* args, PyObject *kwds) {
    bool threaded, copy_success;
    bool return_NULL = false;
    int max_subscript_string, default_ret_subs_used, real_ret_subs_used, ret_subs_used, status, varname_len, subs_used;
    char *varname;
    uint64_t tp_token;
    PyObject *subsarray, *return_tuple;
    ydb_buffer_t error_string_buffer, *ret_subsarray, varname_y, *subsarray_y;


    /* Defaults for non-required arguments */
    subsarray = Py_None;
    tp_token = YDB_NOTTP;

    /* parse and validate */
    static char* kwlist[] = {"threaded", "varname", "subsarray", "tp_token", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "py#|OK", kwlist, &threaded, &varname, &varname_len, &subsarray, &tp_token))
        return NULL;

    if (!validate_subsarray_object(subsarray))
        return NULL;

    /* Setup for Call */
    YDB_MALLOC_BUFFER(&varname_y, varname_len);
    YDB_COPY_STRING_TO_BUFFER(varname, &varname_y, copy_success);
    if (!copy_success) {
        PyErr_SetString(YDBPythonBugError, "YDB_COPY_STRING_TO_BUFFER failed in node_previous()");
        return_NULL = true;
    }
    SETUP_SUBS(subsarray, subs_used, subsarray_y);
    YDB_MALLOC_BUFFER(&error_string_buffer, YDB_MAX_ERRORMSG);

    max_subscript_string = 1024;
    default_ret_subs_used = subs_used - 1;
    if (0 >= default_ret_subs_used)
        default_ret_subs_used = 1;
    real_ret_subs_used = default_ret_subs_used;
    ret_subs_used = default_ret_subs_used;
    ret_subsarray = empty_buffer_array(ret_subs_used, max_subscript_string);

    /* Call the wrapped function */
    CALL_WRAP_5(threaded, ydb_node_previous_s, ydb_node_previous_st, tp_token, &error_string_buffer, &varname_y, subs_used, subsarray_y,
                &ret_subs_used, ret_subsarray, status);

    /* if a buffer is not long enough */
    while(YDB_ERR_INVSTRLEN == status) {
        max_subscript_string = ret_subsarray[ret_subs_used].len_used;
        free(ret_subsarray[ret_subs_used].buf_addr);
        ret_subsarray[ret_subs_used].buf_addr = (char*) malloc(ret_subsarray[ret_subs_used].len_used*sizeof(char));
        ret_subsarray[ret_subs_used].len_alloc = ret_subsarray[ret_subs_used].len_used;
        ret_subsarray[ret_subs_used].len_used = 0;
        ret_subs_used = real_ret_subs_used;
        /* recall the wrapped function */
        CALL_WRAP_5(threaded, ydb_node_previous_s, ydb_node_previous_st, tp_token, &error_string_buffer, &varname_y,
                    subs_used, subsarray_y, &ret_subs_used, ret_subsarray, status);
    }
    /* check status for Errors and Raise Exception */
    if (0 > status) {
        raise_YottaDBError(status, &error_string_buffer);
        return_NULL = true;
    }

    /* Create Python object to return */
    if (!return_NULL)
        return_tuple = convert_ydb_buffer_array_to_py_tuple(ret_subsarray, ret_subs_used);

    /* free allocated memory */
    YDB_FREE_BUFFER(&varname_y);
    free_buffer_array(subsarray_y, subs_used);
    YDB_FREE_BUFFER(&error_string_buffer);
    free_buffer_array(ret_subsarray, real_ret_subs_used);

    if (return_NULL)
        return NULL;
    else
        return return_tuple;
}

/* Wrapper for ydb_set_s() and ydb_set_st() */
static PyObject* set(PyObject* self, PyObject* args, PyObject *kwds) {
    bool threaded, copy_success;
    bool return_NULL = false;
    int status, varname_len, value_len, subs_used;
    uint64_t tp_token;
    char *varname, *value;
    PyObject *subsarray;
    ydb_buffer_t error_string_buffer, value_buffer, varname_y, *subsarray_y;


    /* Defaults for non-required arguments */
    subsarray = Py_None;
    tp_token = YDB_NOTTP;
    value = "";

    /* parse and validate */
    static char* kwlist[] = {"threaded", "varname", "subsarray", "value", "tp_token", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "py#|Oy#K", kwlist, &threaded, &varname, &varname_len, &subsarray,
                &value, &value_len, &tp_token))
        return NULL;

    if (!validate_subsarray_object(subsarray))
        return NULL;

    /* Setup for Call */
    YDB_MALLOC_BUFFER(&varname_y, varname_len);
    YDB_COPY_STRING_TO_BUFFER(varname, &varname_y, copy_success);
    if (!copy_success) {
        PyErr_SetString(YDBPythonBugError, "YDB_COPY_STRING_TO_BUFFER failed in set() for varname");
        return_NULL = true;
    }
    SETUP_SUBS(subsarray, subs_used, subsarray_y);
    YDB_MALLOC_BUFFER(&error_string_buffer, YDB_MAX_ERRORMSG);
    YDB_MALLOC_BUFFER(&value_buffer, value_len);
    YDB_COPY_STRING_TO_BUFFER(value, &value_buffer, copy_success);
    if (!copy_success) {
        PyErr_SetString(YDBPythonBugError, "YDB_COPY_STRING_TO_BUFFER failed in set() for value");
        return_NULL = true;
    }

    /* Call the wrapped function */
    CALL_WRAP_4(threaded, ydb_set_s, ydb_set_st, tp_token, &error_string_buffer, &varname_y, subs_used, subsarray_y, &value_buffer,
                status);

    /* check status for Errors and Raise Exception */
    if (0 > status) {
        raise_YottaDBError(status, &error_string_buffer);
        return_NULL = true;
    }
    /* free allocated memory */
    YDB_FREE_BUFFER(&varname_y);
    YDB_FREE_BUFFER(&value_buffer);
    free_buffer_array(subsarray_y, subs_used);
    YDB_FREE_BUFFER(&error_string_buffer);

    if (return_NULL)
        return NULL;
    else
        return Py_None;
}

/* Wrapper for ydb_str2zwr_s() and ydb_str2zwr_st() */
static PyObject* str2zwr(PyObject* self, PyObject* args, PyObject *kwds) {
    bool threaded;
    bool return_NULL = false;
    int str_len, status, return_length;
    uint64_t tp_token;
    char *str;
    ydb_buffer_t error_string_buf, str_buf, zwr_buf;

    /* Defaults for non-required arguments */
    str = "";
    str_len = 0;
    tp_token = YDB_NOTTP;

    /* parse and validate */
    static char* kwlist[] = {"threaded", "input", "tp_token", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "py#|K", kwlist, &threaded, &str, &str_len, &tp_token))
        return NULL;

    /* Setup for Call */
    YDB_MALLOC_BUFFER(&error_string_buf, YDB_MAX_ERRORMSG);
    str_buf = (ydb_buffer_t){str_len, str_len, str};
    YDB_MALLOC_BUFFER(&zwr_buf, 1024);

    /* Call the wrapped function */
    CALL_WRAP_2(threaded, ydb_str2zwr_s, ydb_str2zwr_st, tp_token, &error_string_buf, &str_buf, &zwr_buf, status);


    /* recall with properly sized buffer if zwr_buf is not long enough */
    if (YDB_ERR_INVSTRLEN == status) {
        return_length = zwr_buf.len_used;
        YDB_FREE_BUFFER(&zwr_buf);
        YDB_MALLOC_BUFFER(&zwr_buf, return_length);
        /* recall the wrapped function */
        CALL_WRAP_2(threaded, ydb_str2zwr_s, ydb_str2zwr_st, tp_token, &error_string_buf, &str_buf, &zwr_buf, status);
    }

    /* check status for Errors and Raise Exception */
    if (0 > status) {
        raise_YottaDBError(status, &error_string_buf);
        return_NULL = true;
    }

    /* Create Python object to return */
    PyObject* return_value =  Py_BuildValue("y#", zwr_buf.buf_addr, zwr_buf.len_used);

    /* free allocated memory */
    YDB_FREE_BUFFER(&error_string_buf);
    YDB_FREE_BUFFER(&zwr_buf);

    if (return_NULL)
        return NULL;
    else
        return return_value;
}

/* Wrapper for ydb_subscript_next_s() and ydb_subscript_next_st() */
static PyObject* subscript_next(PyObject* self, PyObject* args, PyObject *kwds) {
    bool threaded, copy_success;
    bool return_NULL = false;
    int status, return_length, varname_len, subs_used;
    char *varname;
    uint64_t tp_token;
    PyObject *subsarray, *return_python_string;
    ydb_buffer_t error_string_buffer, ret_value, varname_y, *subsarray_y;

    /* Defaults for non-required arguments */
    subsarray = Py_None;
    tp_token = YDB_NOTTP;

    /* parse and validate */
        static char* kwlist[] = {"threaded", "varname", "subsarray", "tp_token", NULL};
        if (!PyArg_ParseTupleAndKeywords(args, kwds, "py#|OK", kwlist, &threaded, &varname, &varname_len, &subsarray, &tp_token))
            return NULL;

    if (!validate_subsarray_object(subsarray))
        return NULL;

    /* Setup for Call */
    YDB_MALLOC_BUFFER(&varname_y, varname_len);
    YDB_COPY_STRING_TO_BUFFER(varname, &varname_y, copy_success);
    if (!copy_success) {
        PyErr_SetString(YDBPythonBugError, "YDB_COPY_STRING_TO_BUFFER failed in subscript_next()");
        return_NULL = true;
    }
    SETUP_SUBS(subsarray, subs_used, subsarray_y);
    YDB_MALLOC_BUFFER(&error_string_buffer, YDB_MAX_ERRORMSG);
    YDB_MALLOC_BUFFER(&ret_value, 1024);

    /* Call the wrapped function */
    CALL_WRAP_4(threaded, ydb_subscript_next_s, ydb_subscript_next_st, tp_token, &error_string_buffer, &varname_y, subs_used, subsarray_y,
                &ret_value, status);

    /* check to see if length of string was longer than 1024 is so, try again with proper length */
    if (YDB_ERR_INVSTRLEN == status) {
        return_length = ret_value.len_used;
        YDB_FREE_BUFFER(&ret_value);
        YDB_MALLOC_BUFFER(&ret_value, return_length);
        /* recall the wrapped function */
        CALL_WRAP_4(threaded, ydb_subscript_next_s, ydb_subscript_next_st, tp_token, &error_string_buffer, &varname_y,
                    subs_used, subsarray_y, &ret_value, status);
    }
    /* check status for Errors and Raise Exception */
    if (0 > status) {
        raise_YottaDBError(status, &error_string_buffer);
        return_NULL = true;
    }

    /* Create Python object to return */
    if (!return_NULL)
        return_python_string = Py_BuildValue("y#", ret_value.buf_addr, ret_value.len_used);

    /* free allocated memory */
    YDB_FREE_BUFFER(&varname_y);
    free_buffer_array(subsarray_y, subs_used);
    YDB_FREE_BUFFER(&error_string_buffer);
    YDB_FREE_BUFFER(&ret_value);

    if (return_NULL)
        return NULL;
    else
        return return_python_string;
}

/* Wrapper for ydb_subscript_previous_s() and ydb_subscript_previous_st() */
static PyObject* subscript_previous(PyObject* self, PyObject* args, PyObject *kwds) {
    bool threaded, copy_success;
    bool return_NULL = false;
    int status, return_length, varname_len, subs_used;
    char *varname;
    uint64_t tp_token;
    PyObject *subsarray, *return_python_string;
    ydb_buffer_t error_string_buffer, ret_value, varname_y, *subsarray_y;

    /* Defaults for non-required arguments */
    subsarray = Py_None;
    tp_token = YDB_NOTTP;

    /* Setup for Call */
    static char* kwlist[] = {"threaded", "varname", "subsarray", "tp_token", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "py#|OK", kwlist, &threaded, &varname, &varname_len, &subsarray, &tp_token))
        return NULL;

    if (!validate_subsarray_object(subsarray))
        return NULL;

    /* Setup for Call */
    YDB_MALLOC_BUFFER(&varname_y, varname_len);
    YDB_COPY_STRING_TO_BUFFER(varname, &varname_y, copy_success);
    if (!copy_success) {
        PyErr_SetString(YDBPythonBugError, "YDB_COPY_STRING_TO_BUFFER failed in subscript_previous()");
        return_NULL = true;
    }
    SETUP_SUBS(subsarray, subs_used, subsarray_y);
    YDB_MALLOC_BUFFER(&error_string_buffer, YDB_MAX_ERRORMSG);
    YDB_MALLOC_BUFFER(&ret_value, 1024);

    /* Call the wrapped function */
    CALL_WRAP_4(threaded, ydb_subscript_previous_s, ydb_subscript_previous_st, tp_token, &error_string_buffer, &varname_y,
                subs_used, subsarray_y, &ret_value, status);

    /* check to see if length of string was longer than 1024 is so, try again with proper length */
    if (YDB_ERR_INVSTRLEN == status) {
        return_length = ret_value.len_used;
        YDB_FREE_BUFFER(&ret_value);
        YDB_MALLOC_BUFFER(&ret_value, return_length);
        CALL_WRAP_4(threaded, ydb_subscript_previous_s, ydb_subscript_previous_st, tp_token, &error_string_buffer, &varname_y,
                    subs_used, subsarray_y, &ret_value, status);
    }

    /* check status for Errors and Raise Exception */
    if (0 > status) {
        raise_YottaDBError(status, &error_string_buffer);
        return_NULL = true;
    }

    /* Create Python object to return */
    if (!return_NULL)
        return_python_string = Py_BuildValue("y#", ret_value.buf_addr, ret_value.len_used);

    /* free allocated memory */
    YDB_FREE_BUFFER(&varname_y);
    free_buffer_array(subsarray_y, subs_used);
    YDB_FREE_BUFFER(&error_string_buffer);
    YDB_FREE_BUFFER(&ret_value);
    if (return_NULL)
        return NULL;
    else
        return return_python_string;
}

/* Callback functions used by Wrapper for ydb_tp_s() / ydb_tp_st() */

/* Callback Wrapper used by tp_st. The aproach of calling a Python function is a bit of a hack. Here's how it works:
 *    1) This is the callback function always the function passed to called by ydb_tp_st.
 *    2) the actual Python function to be called is passed to this function as the first element in a Python tuple.
 *    3) the positional arguments are passed as the second element and the keyword args are passed as the third.
 *    4) the new tp_token that ydb_tp_st passes to this function as an argument is added to the kwargs dictionary.
 *    5) this function calls calls the python callback funciton with the args and kwargs arguments.
 *    6) if a function raises an exception then this function returns -2 as a way of indicating an error.
 *            (note) the PyErr String is already set so the the function receiving the return value (tp) just needs to return NULL.
 */
static int callback_wrapper_st(uint64_t tp_token, ydb_buffer_t*errstr, void *function_with_arguments) {
    /* this should only ever be called by ydb_tp_st c api via tp below.
     * It assumes that everything passed to it was validated.
     */
    int return_val;
    PyObject *function, *args, *kwargs, *return_value, *tp_token_py;

    function = PyTuple_GetItem(function_with_arguments, 0);
    args = PyTuple_GetItem(function_with_arguments, 1);
    kwargs = PyTuple_GetItem(function_with_arguments, 2);
    tp_token_py = Py_BuildValue("K", tp_token);
    PyDict_SetItemString(kwargs, "tp_token", tp_token_py);
    Py_DECREF(tp_token_py);

    return_value = PyObject_Call(function, args, kwargs);
    if (NULL == return_value) {
        /* function raised an exception */
        return -2; /* MAGIC NUMBER flag to raise exception at the next level up */
    }
    return_val = (int)PyLong_AsLong(return_value);
    Py_DECREF(return_value);
    return return_val;
}
/* Callback Wrapper used by tp_st. The aproach of calling a Python function is a bit of a hack.
 * See notes in 'callback_wrapper_st above
 */
static int callback_wrapper_s(void *function_with_arguments) {
    /* this should only ever be called by ydb_tp_s c api via tp below.
     * It assumes that everything passed to it was validated.
     */
    int return_val;
    PyObject *function, *args, *kwargs, *return_value;

    function = PyTuple_GetItem(function_with_arguments, 0);
    args = PyTuple_GetItem(function_with_arguments, 1);
    kwargs = PyTuple_GetItem(function_with_arguments, 2);
    return_value = PyObject_Call(function, args, kwargs);
    if (NULL == return_value) {
        /* function raised an exception */
        return -2; /* MAGIC NUMBER flag to raise exception at the next level up */
    }
    return_val = (int)PyLong_AsLong(return_value);
    Py_DECREF(return_value);
    return return_val;
}

/* Wrapper for ydb_tp_s() / ydb_tp_st() */
static PyObject* tp(PyObject* self, PyObject* args, PyObject *kwds) {
    bool threaded;
    bool return_NULL = false;
    int namecount, status;
    uint64_t tp_token;
    char *transid;
    PyObject *callback, *callback_args, *callback_kwargs, *varnames, *default_varnames_item,*function_with_arguments;
    ydb_buffer_t error_string_buffer, *varname_buffers;

    /* Defaults for non-required arguments */
    callback_args = PyTuple_New(0);
    callback_kwargs = PyDict_New();
    transid = "BATCH";
    namecount = 1;
    varnames = PyList_New(1); /* place holder. TODO: expose to python call */
    default_varnames_item = Py_BuildValue("y", "*");
    PyList_SetItem(varnames, 0, default_varnames_item); /* default set to special case when all local variables
                                                         * are restored on a restart. */

    tp_token = YDB_NOTTP;

    /* parse and validate */
    static char *kwlist[] = {"threaded", "callback", "args", "kwargs", "transid", "tp_token", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "pO|OOsK", kwlist, &threaded, &callback, &callback_args, &callback_kwargs, &transid, &tp_token))
        return NULL; // raise exception

    /* validate input */
    if (!PyCallable_Check(callback)) {
        PyErr_SetString(PyExc_TypeError, "'callback' must be a callable.");
        return NULL;
    }
    if (!PyTuple_Check(callback_args)) {
        PyErr_SetString(PyExc_TypeError, "'args' must be a tuple. "
                                        "(It will be passed to the callback function as positional arguments.)");
        return NULL;
    }
    if (!PyDict_Check(callback_kwargs)) {
        PyErr_SetString(PyExc_TypeError, "'kwargs' must be a dictionary. "
                                        "(It will be passed to the callback function as keyword arguments.)");
        return NULL;
    }
    /* Setup for Call */
    YDB_MALLOC_BUFFER(&error_string_buffer, YDB_MAX_ERRORMSG);
    function_with_arguments = Py_BuildValue("(OOO)", callback, callback_args, callback_kwargs);
    namecount = PySequence_Length(varnames);
    varname_buffers = convert_py_bytes_sequence_to_ydb_buffer_array(varnames);
    Py_DECREF(varnames);

    /* Call the wrapped function */
    if (threaded)
        status = ydb_tp_st(tp_token, &error_string_buffer, callback_wrapper_st, function_with_arguments, transid,
                            namecount, varname_buffers);
    else if (!threaded)
        status = ydb_tp_s(callback_wrapper_s, function_with_arguments, transid, namecount, varname_buffers);

    /* check status for Errors and Raise Exception */
    if (-2 == status) {/* MAGIC VALUE to indicate that the callback
                     * function raised an exception and should be raised
                     */
        return_NULL = true;
    } else if (0 > status) {
        raise_YottaDBError(status, &error_string_buffer);
        return_NULL = true;
    }
    /* free allocated memory */
    YDB_FREE_BUFFER(&error_string_buffer);
    free(varname_buffers);
    if (return_NULL)
        return NULL;
    else
        return Py_BuildValue("i", status);
}

/* Wrapper for ydb_zwr2str_s() and ydb_zwr2str_st() */
static PyObject* zwr2str(PyObject* self, PyObject* args, PyObject *kwds) {
    bool threaded;
    bool return_NULL = false;
    int zwr_len, status, return_length;
    uint64_t tp_token;
    char *zwr;
    PyObject *return_value;
    ydb_buffer_t error_string_buf, zwr_buf, str_buf;

    /* Defaults for non-required arguments */
    zwr = "";
    zwr_len = 0;
    tp_token = YDB_NOTTP;

    /* parse and validate */
    static char* kwlist[] = {"threaded", "input", "tp_token", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "py#|K", kwlist, &threaded, &zwr, &zwr_len, &tp_token))
        return NULL;

    /* Setup for Call */
    YDB_MALLOC_BUFFER(&error_string_buf, YDB_MAX_ERRORMSG);
    zwr_buf = (ydb_buffer_t){zwr_len, zwr_len, zwr};
    YDB_MALLOC_BUFFER(&str_buf, 1024);

    /* Call the wrapped function */
    CALL_WRAP_2(threaded, ydb_zwr2str_s, ydb_zwr2str_st, tp_token, &error_string_buf, &zwr_buf, &str_buf, status);

    /* recall with properly sized buffer if zwr_buf is not long enough */
    if (YDB_ERR_INVSTRLEN == status) {
        return_length = str_buf.len_used;
        YDB_FREE_BUFFER(&str_buf);
        YDB_MALLOC_BUFFER(&str_buf, return_length);
        /* recall the wrapped function */
        CALL_WRAP_2(threaded, ydb_zwr2str_s, ydb_zwr2str_st, tp_token, &error_string_buf, &zwr_buf, &str_buf, status);
    }

    /* check status for Errors and Raise Exception */
    if (0 > status) {
        raise_YottaDBError(status, &error_string_buf);
        return_NULL = true;
    }

    if (!return_NULL)
        return_value =  Py_BuildValue("y#", str_buf.buf_addr, str_buf.len_used);

    YDB_FREE_BUFFER(&error_string_buf);
    YDB_FREE_BUFFER(&str_buf);

    if (return_NULL)
        return NULL;
    else
        return return_value;
}

/*Comprehensive API
 *Utility Functions
 *
 *    ydb_child_init()
 *    ydb_exit()
 *    ydb_file_id_free() / ydb_file_id_free_t()
 *    ydb_file_is_identical() / ydb_file_is_identical_t()
 *    ydb_file_name_to_id() / ydb_file_name_to_id_t()
 *    ydb_fork_n_core()
 *    ydb_free()
 *    ydb_hiber_start()
 *    ydb_hiber_start_wait_any()
 *    ydb_init()
 *    ydb_malloc()
 *    ydb_message() / ydb_message_t()
 *    ydb_stdout_stderr_adjust() / ydb_stdout_stderr_adjust_t()
 *    ydb_thread_is_main()
 *    ydb_timer_cancel() / ydb_timer_cancel_t()
 *    ydb_timer_start() / ydb_timer_start_t()

 *Calling M Routines
 */



/* Pull everything together into a Python Module */
static PyMethodDef methods[] = {
    /* Simple and Simple Threaded API Functions */
    {"data", (PyCFunction)data, METH_VARARGS | METH_KEYWORDS, "used to learn what type of data is at a node.\n "
                                                                        "0 : There is neither a value nor a subtree, "
                                                                        "i.e., it is undefined.\n"
                                                                        "1 : There is a value, but no subtree\n"
                                                                        "10 : There is no value, but there is a subtree.\n"
                                                                        "11 : There are both a value and a subtree.\n"},
    {"delete", (PyCFunction)delete_wrapper, METH_VARARGS | METH_KEYWORDS, "deletes node value or tree data at node"},
    {"delete_excel", (PyCFunction)delete_excel, METH_VARARGS | METH_KEYWORDS, "delete the trees of all local variables "
                                                                                    "except those in the 'varnames' array"},
    {"get", (PyCFunction)get, METH_VARARGS | METH_KEYWORDS, "returns the value of a node or raises exception"},
    {"incr", (PyCFunction)incr, METH_VARARGS | METH_KEYWORDS, "increments value by the value specified by 'increment'"},

    {"lock", (PyCFunction)lock, METH_VARARGS | METH_KEYWORDS, "..."},

    {"lock_decr", (PyCFunction)lock_decr, METH_VARARGS | METH_KEYWORDS, "Decrements the count of the specified lock held "
                                                                            "by the process. As noted in the Concepts section, a "
                                                                            "lock whose count goes from 1 to 0 is released. A lock "
                                                                            "whose name is specified, but which the process does "
                                                                            "not hold, is ignored."},
    {"lock_incr", (PyCFunction)lock_incr, METH_VARARGS | METH_KEYWORDS, "Without releasing any locks held by the process, "
                                                                            "attempt to acquire the requested lock incrementing it"
                                                                            " if already held."},
    {"node_next", (PyCFunction)node_next, METH_VARARGS | METH_KEYWORDS, "facilitate depth-first traversal of a local or global"
                                                                            " variable tree. returns string tuple of subscripts of"
                                                                            " next node with value."},
    {"node_previous", (PyCFunction)node_previous, METH_VARARGS | METH_KEYWORDS, "facilitate depth-first traversal of a local "
                                                                                    "or global variable tree. returns string tuple"
                                                                                    "of subscripts of previous node with value."},
    {"set", (PyCFunction)set, METH_VARARGS | METH_KEYWORDS, "sets the value of a node or raises exception"},
    {"str2zwr", (PyCFunction)str2zwr, METH_VARARGS | METH_KEYWORDS, "returns the zwrite formatted (Bytes Object) version of the"
                                                                        " Bytes object provided as input."},
    {"subscript_next", (PyCFunction)subscript_next, METH_VARARGS | METH_KEYWORDS, "returns the name of the next subscript at "
                                                                                      "the same level as the one given"},
    {"subscript_previous", (PyCFunction)subscript_previous, METH_VARARGS | METH_KEYWORDS, "returns the name of the previous "
                                                                                              "subscript at the same level as the "
                                                                                              "one given"},
    {"tp", (PyCFunction)tp, METH_VARARGS | METH_KEYWORDS, "transaction"},

    {"zwr2str", (PyCFunction)zwr2str, METH_VARARGS | METH_KEYWORDS, "returns the Bytes Object from the zwrite formated Bytes "
                                                                        "object provided as input."},
    /* API Utility Functions */
    {NULL, NULL, 0, NULL}        /* Sentinel */
};

static struct PyModuleDef _yottadbmodule = {
    PyModuleDef_HEAD_INIT,
    "_yottadb",   /* name of module */
    "A module that provides basic access to YottaDB's c api", /* module documentation, may be NULL */
    -1,       /* size of per-interpreter state of the module, or -1 if the module keeps state in global variables. */
    methods
};

PyMODINIT_FUNC PyInit__yottadb_wrapper(void) {
    PyObject *module = PyModule_Create(&_yottadbmodule);
    if (NULL == module)
        return NULL;

    /* Defining Module 'Constants' */
    PyObject *module_dictionary = PyModule_GetDict(module);
    /* constants defined here for conveniance */
    PyDict_SetItemString(module_dictionary, "YDB_DATA_NO_DATA", Py_BuildValue("i", 0));
    PyDict_SetItemString(module_dictionary, "YDB_DATA_HAS_VALUE_NO_TREE", Py_BuildValue("i", 1));
    PyDict_SetItemString(module_dictionary, "YDB_DATA_NO_VALUE_HAS_TREE", Py_BuildValue("i", 10));
    PyDict_SetItemString(module_dictionary, "YDB_DATA_HAS_VALUE_HAS_TREE", Py_BuildValue("i", 11));
    /* expose constants defined in c */
    PyDict_SetItemString(module_dictionary, "YDB_DEL_TREE", Py_BuildValue("i", YDB_DEL_TREE));
    PyDict_SetItemString(module_dictionary, "YDB_DEL_NODE", Py_BuildValue("i", YDB_DEL_NODE));
    PyDict_SetItemString(module_dictionary, "YDB_SEVERITY_WARNING", Py_BuildValue("i", YDB_SEVERITY_WARNING));
    PyDict_SetItemString(module_dictionary, "YDB_SEVERITY_SUCCESS", Py_BuildValue("i", YDB_SEVERITY_SUCCESS));
    PyDict_SetItemString(module_dictionary, "YDB_SEVERITY_ERROR", Py_BuildValue("i", YDB_SEVERITY_ERROR));
    PyDict_SetItemString(module_dictionary, "YDB_SEVERITY_INFORMATIONAL", Py_BuildValue("i", YDB_SEVERITY_INFORMATIONAL));
    PyDict_SetItemString(module_dictionary, "YDB_SEVERITY_FATAL", Py_BuildValue("i", YDB_SEVERITY_FATAL));
    PyDict_SetItemString(module_dictionary, "YDB_MAX_IDENT", Py_BuildValue("i", YDB_MAX_IDENT));
    PyDict_SetItemString(module_dictionary, "YDB_MAX_NAMES", Py_BuildValue("i", YDB_MAX_NAMES));
    PyDict_SetItemString(module_dictionary, "YDB_MAX_STR", Py_BuildValue("i", YDB_MAX_STR));
    PyDict_SetItemString(module_dictionary, "YDB_MAX_SUBS", Py_BuildValue("i", YDB_MAX_SUBS));
    PyDict_SetItemString(module_dictionary, "YDB_MAX_TIME_NSEC", Py_BuildValue("L", YDB_MAX_TIME_NSEC));
    PyDict_SetItemString(module_dictionary, "YDB_MAX_YDBERR", Py_BuildValue("i", YDB_MAX_YDBERR));
    PyDict_SetItemString(module_dictionary, "YDB_MAX_ERRORMSG", Py_BuildValue("i", YDB_MAX_ERRORMSG));
    PyDict_SetItemString(module_dictionary, "YDB_MIN_YDBERR", Py_BuildValue("i", YDB_MIN_YDBERR));
    PyDict_SetItemString(module_dictionary, "YDB_OK", Py_BuildValue("i", YDB_OK));
    PyDict_SetItemString(module_dictionary, "YDB_INT_MAX", Py_BuildValue("i", YDB_INT_MAX));
    PyDict_SetItemString(module_dictionary, "YDB_TP_RESTART", Py_BuildValue("i", YDB_TP_RESTART));
    PyDict_SetItemString(module_dictionary, "YDB_TP_ROLLBACK", Py_BuildValue("i", YDB_TP_ROLLBACK));
    PyDict_SetItemString(module_dictionary, "YDB_NOTOK", Py_BuildValue("i", YDB_NOTOK));
    PyDict_SetItemString(module_dictionary, "YDB_LOCK_TIMEOUT", Py_BuildValue("i", YDB_LOCK_TIMEOUT));
    PyDict_SetItemString(module_dictionary, "YDB_NOTTP", Py_BuildValue("i", YDB_NOTTP));



    /* Excetpions */
    /* setting up YottaDBError */
    PyObject* exc_dict = make_getter_code();
    if (NULL == exc_dict)
        return NULL;
    
    YottaDBError = PyErr_NewException("_yottadb.YottaDBError",
                                        NULL, // use to pick base class
                                        exc_dict);
    PyModule_AddObject(module,"YottaDBError", YottaDBError);

    /* setting up YottaDBLockTimeout */
    YottaDBLockTimeout = PyErr_NewException("_yottadb.YottaDBLockTimeout",
                                        NULL, // use to pick base class
                                        NULL);
    PyModule_AddObject(module,"YottaDBLockTimeout", YottaDBLockTimeout);

    /* setting up YDBPythonBugError */
    YDBPythonBugError = PyErr_NewException("_yottadb.YDBPythonBugError",
                                        NULL, // use to pick base class
                                        NULL);
    PyModule_AddObject(module,"YDBPythonBugError", YDBPythonBugError);
    return module;
}