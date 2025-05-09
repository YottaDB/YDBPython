/****************************************************************
 *                                                              *
 * Copyright (c) 2019-2021 Peter Goss All rights reserved.      *
 *                                                              *
 * Copyright (c) 2019-2025 YottaDB LLC and/or its subsidiaries. *
 * All rights reserved.                                         *
 *                                                              *
 *  This source code contains the intellectual property         *
 *  of its copyright holder(s), and is made available           *
 *  under a license.  If you do not know the terms of           *
 *  the license, please stop and do not read further.           *
 *                                                              *
 ****************************************************************/

#define _POSIX_C_SOURCE 200809L // Provide access to strnlen, per https://man7.org/linux/man-pages/man7/feature_test_macros.7.html
#include <string.h>
#include <assert.h>
#include <stdbool.h>
#include <stdarg.h>
#include <pthread.h>
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "_yottadb.h"
#include "_yottadbconstants.h"

#define DECREF_AND_RETURN(PY_OBJECT, RET)     \
	{                                     \
		if (NULL != PY_OBJECT) {      \
			Py_DECREF(PY_OBJECT); \
		}                             \
		return RET;                   \
	}

/* Utility structure for maintaining call-in information
 * used in ydb_cip calls.
 *
 * This struct serves as an anchor point for the C call-in routine descriptor
 * used by cip() that provides for less call-in overhead than ci() as the descriptor
 * contains fastpath information filled in by YottaDB after the first call. This allows
 * subsequent calls to have minimal overhead. Because this structure's contents contain
 * pointers to C allocated storage, it is not exposed to Python-level users.
 */
typedef struct {
	char *		   routine_name;
	bool		   has_parm_types;
	ci_name_descriptor ci_info;
	ci_parm_type	   parm_types;
} py_ci_name_descriptor;

// Initialize a global struct to store call-in information
py_ci_name_descriptor ci_info = {.routine_name = NULL, .has_parm_types = FALSE, .ci_info = {.handle = NULL}};

/* Counts the total number of arguments between two integer bitmaps,
 * one representing input arguments and another representing output
 * arguments by bitwise ORing the two integers together and ANDing
 * on each bit in this combined bitmap.
 */
static unsigned int count_args(unsigned int in, unsigned int out) {
	unsigned int count, total;

	total = in | out;
	count = 0;
	while (total) {
		count += (total & 1);
		total >>= 1;
	}
	return count;
}

/* Routine that raises the appropriate Python Error during validation of Python input
 * parameters. The 2 types of errors that will be raised are:
 *     1) TypeError in the case that the parameter is of the wrong type
 *     2) ValueError if the parameter is of the right type but is invalid in some other way (e.g. too long)
 *
 * Parameters:
 *     status  - the error message status number (specific values defined in _yottdb.h)
 *     message - the message to be set in the Python exception.
 */
static void raise_ValidationError(YDBPythonErrorType err_type, char *format_prefix, char *err_format, ...) {
	va_list args;
	char	err_message[YDBPY_MAX_ERRORMSG];
	char	prefixed_err_message[YDBPY_MAX_ERRORMSG];
	int	copied;

	va_start(args, err_format);
	copied = vsnprintf(err_message, YDBPY_MAX_ERRORMSG, err_format, args);
	assert(YDBPY_MAX_ERRORMSG > copied);
	UNUSED(copied);
	va_end(args);

	if (NULL != format_prefix) {
		copied = snprintf(prefixed_err_message, YDBPY_MAX_ERRORMSG, format_prefix, err_message);
	} else {
		copied = snprintf(prefixed_err_message, YDBPY_MAX_ERRORMSG, "%s", err_message);
	}
	assert(0 <= copied);
	if (YDBPY_MAX_ERRORMSG <= copied) {
		char * ellipsis = "...";
		size_t ellipsis_len;

		ellipsis_len = strlen(ellipsis);
		memcpy(&prefixed_err_message[YDBPY_MAX_ERRORMSG - (ellipsis_len + 1)], ellipsis, ellipsis_len);
		assert('\0' == prefixed_err_message[YDBPY_MAX_ERRORMSG - 1]);
	}

	switch (err_type) {
	case YDBPython_TypeError:
		PyErr_SetString(PyExc_TypeError, prefixed_err_message);
		break;
	case YDBPython_ValueError:
		PyErr_SetString(PyExc_ValueError, prefixed_err_message);
		break;
	case YDBPython_OSError:
		PyErr_SetString(PyExc_OSError, err_message);
		break;
	default:
		// Only TypeError and ValueError are possible, so we should never get here.
		assert(FALSE);
		break;
	}
}

/* Convert a PyObject referencing a Python `bytes` or `str` object to a ydb_buffer_t struct
 * and validate the size of resulting buffer to enforce YottaDB variable name and value limits.
 * If set, the `is_varname` flag signals that the object should be validated as a variable,
 * otherwise the object will be validated as a value.
 */
static int anystr_to_buffer(PyObject *object, ydb_buffer_t *buffer, bool is_varname) {
	char *	     bytes;
	bool	     decref_object;
	Py_ssize_t   bytes_ssize;
	unsigned int bytes_len;
	int	     done;

	if (PyUnicode_Check(object)) {
		// Convert Unicode object into Python bytes object
		object = PyUnicode_AsEncodedString(object, "utf-8", "strict"); // New reference
		if (NULL == object) {
			PyErr_SetString(YDBPythonError, "failed to encode Unicode string to bytes object");
			return !YDB_OK;
		}
		decref_object = TRUE;
	} else if (PyBytes_Check(object)) {
		// Object is a bytes object, no Unicode encoding needed
		decref_object = FALSE;
	} else {
		/* Object is not bytes or str (Unicode), but one of these types was expected.
		 * So, raise an exception.
		 */
		raise_ValidationError(YDBPython_TypeError, NULL, YDBPY_ERR_ARG_NOT_BYTES_LIKE);
		return !YDB_OK;
	}

	// Convert Python bytes object to C character string (char *)
	bytes_ssize = PyBytes_Size(object);
	if (INT32_MAX < bytes_ssize) {
		/* Python bytes objects may have more bytes than can be represented by a 32-bit unsigned integer.
		 * If `object` is 1 more than INT32_MAX, `bytes_len` below would be set to 0, falsely indicating a
		 * 0-byte long bytes object. So, we need to detect this integer overflow before calling Py_SAFE_DOWNCAST,
		 * in which case we raise an exception, cleanup, and return the failure to the caller.
		 */
		if (is_varname) {
			raise_ValidationError(YDBPython_ValueError, NULL, YDBPY_ERR_VARNAME_TOO_LONG, bytes_ssize, YDB_MAX_IDENT);
		} else {
			raise_ValidationError(YDBPython_ValueError, NULL, YDBPY_ERR_BYTES_TOO_LONG, bytes_ssize, YDB_MAX_STR);
		}
		if (decref_object) {
			// Optionally cleanup new reference
			Py_DECREF(object);
		}
		return !YDB_OK;
	}
	bytes_len = Py_SAFE_DOWNCAST(bytes_ssize, Py_ssize_t, unsigned int);
	bytes = PyBytes_AsString(object);

	// Allocate and populate YDB buffer
	YDB_MALLOC_BUFFER(buffer, bytes_len + 1); // Null terminator used in some scenarios
	YDB_COPY_BYTES_TO_BUFFER(bytes, bytes_len, buffer, done);
	buffer->buf_addr[buffer->len_used] = '\0';

	// Optionally cleanup new reference
	if (decref_object) {
		Py_DECREF(object);
	}

	// Defer error emission until after optional cleanup to reduce duplication
	if (!done) {
		PyErr_SetString(YDBPythonError, "failed to copy bytes object to buffer array");
		YDB_FREE_BUFFER(buffer);
		return !YDB_OK;
	}

	/* Enforce variable name length limit, i.e. YDB_MAX_IDENT for locals and YDB_MAX_IDENT + 1 for globals.
	 * YDB_MAX_IDENT + 1 is acceptable for global variable names, since '^' does not count toward variable
	 * name length. If the variable name is too long, raise an exception.
	 */
	if (is_varname) {
		if ((('^' == buffer->buf_addr[0]) && ((YDB_MAX_IDENT + 1) < buffer->len_used))
		    || (('^' != buffer->buf_addr[0]) && ((YDB_MAX_IDENT) < buffer->len_used))) {
			raise_ValidationError(YDBPython_ValueError, NULL, YDBPY_ERR_VARNAME_TOO_LONG, buffer->len_used,
					      YDB_MAX_IDENT);
			YDB_FREE_BUFFER(buffer);
			return !YDB_OK;
		}
	} else if ((YDB_MAX_STR) < buffer->len_used) {
		// This is a value and not a variable, so accept up to YDB_MAX_STR length
		raise_ValidationError(YDBPython_ValueError, NULL, YDBPY_ERR_VARNAME_TOO_LONG, buffer->len_used, YDB_MAX_IDENT);
		YDB_FREE_BUFFER(buffer);
		return !YDB_OK;
	}
	return YDB_OK;
}

static int anystr_to_ydb_string_t(PyObject *object, ydb_string_t *buffer) {
	char *	     bytes;
	bool	     decref_object;
	Py_ssize_t   bytes_ssize;
	unsigned int bytes_len;

	if (PyUnicode_Check(object)) {
		// Convert Unicode object into Python bytes object
		object = PyUnicode_AsEncodedString(object, "utf-8", "strict"); // New reference
		if (NULL == object) {
			PyErr_SetString(YDBPythonError, "failed to encode Unicode string to bytes object");
			return !YDB_OK;
		}
		decref_object = TRUE;
	} else if (PyBytes_Check(object)) {
		// Object is a bytes object, no Unicode encoding needed
		decref_object = FALSE;
	} else {
		/* Object is not bytes or str (Unicode). Signal this to
		 * the caller and let it decide whether to issue an error
		 * or check for additional Python types.
		 */
		return YDBPY_CHECK_TYPE;
	}

	// Convert Python bytes object to C character string (char *)
	bytes_ssize = PyBytes_Size(object);
	bytes_len = Py_SAFE_DOWNCAST(bytes_ssize, Py_ssize_t, unsigned int);
	bytes = PyBytes_AsString(object);

	// Allocate and populate YDB buffer
	buffer->address = malloc((bytes_len + 1) * sizeof(char)); // Null terminator used in some scenarios
	memcpy(buffer->address, bytes, bytes_len);
	buffer->address[bytes_len] = '\0';
	buffer->length = bytes_len;

	// Optionally cleanup new reference
	if (decref_object) {
		Py_DECREF(object);
	}
	return YDB_OK;
}

/* Check if a numeric conversion error occurred in Python API code.
 * If so, raise an exception and return TRUE, otherwise just return FALSE.
 */
static int is_conversion_error() {
	PyObject *err_type;

	err_type = PyErr_Occurred();
	if (NULL != err_type) {
		// A non-overflow Python error occurred, so raise a matching exception manually
		PyErr_SetString(err_type, YDBPY_ERR_FAILED_NUMERIC_CONVERSION);
		return TRUE;
	} else {
		return FALSE;
	}
}

static int object_to_ydb_string_t(PyObject *object, ydb_string_t *buffer) {
	int status;

	status = anystr_to_ydb_string_t(object, buffer);
	if (YDBPY_CHECK_TYPE == status) {
		status = YDB_OK;
		buffer->address = calloc(CANONICAL_NUMBER_TO_STRING_MAX, sizeof(char));
		if (PyLong_Check(object)) {
			long num;

			num = PyLong_AsLong(object); // Raises exception if Python int doesn't fit in C long
			if ((-1 == num) && is_conversion_error()) {
				free(buffer->address);
				status = !YDB_OK;
			} else {
				buffer->length = snprintf(buffer->address, CANONICAL_NUMBER_TO_STRING_MAX, "%ld", num);
				assert(buffer->length < CANONICAL_NUMBER_TO_STRING_MAX);
			}
		} else if (PyFloat_Check(object)) {
			double num;

			num = PyFloat_AsDouble(object);
			if ((-1 == num) && is_conversion_error()) {
				free(buffer->address);
				status = !YDB_OK;
			} else {
				buffer->length = snprintf(buffer->address, CANONICAL_NUMBER_TO_STRING_MAX, "%lf", num);
				assert(buffer->length < CANONICAL_NUMBER_TO_STRING_MAX);
			}
		} else {
			/* Object is not str, bytes, int, or float, and so cannot be converted to
			 * a C type accepted by ydb_ci. Cleanup and signal error to caller, who will
			 * raise an exception based on context.
			 */
			free(buffer->address);
			status = !YDB_OK;
		}
	}
	return status;
}

/* Local Utility Functions */
/* Routine to create an array of empty ydb_buffer_ts with num elements each with
 * an allocated length of len
 *
 * Parameters:
 *   num    - the number of buffers to allocate in the array
 *   len    - the length of the string to allocate for each of the the ydb_buffer_ts
 *
 * free with FREE_BUFFER_ARRAY macro
 */
static ydb_buffer_t *create_empty_buffer_array(int num, int len) {
	int	      i;
	ydb_buffer_t *return_buffer_array;

	return_buffer_array = malloc(num * sizeof(ydb_buffer_t));
	for (i = 0; i < num; i++)
		YDB_MALLOC_BUFFER(&return_buffer_array[i], len);
	return return_buffer_array;
}

/* Conversion Utilities */

/* Returns a new PyObject set to the value contained in a YDB buffer.
 *
 * This is accomplished by determining the type of an existing Python object
 * and generating a new PyObject from the value of the YDB buffer via
 * an appropriate intermediate C type.
 *
 * Python types supported are: int, float, bytes, and str.
 *
 * On success, this function creates a new PyObject reference.
 * On error, no references will be created and the error will be signalled
 * by a NULL return value. Note that this function will only raise its own
 * exceptions for failed system calls, while any other exceptions will have
 * occurred within Python library calls and will be raised there. In either
 * case, the caller need not raise any exceptions.
 *
 * Parameters:
 *   object    - the Python object to be type checked
 *   value    - the YDB string containing the value to which the object is to be set
 *
 * Returns:
 *   ret	- a new PyObject, or NULL in case of error
 */
static PyObject *new_object_from_object_and_string(PyObject *object, ydb_string_t *value) {
	PyObject *ret;

	/* Add null terminator at new length, as it may have been
	 * shortened during an M call-in.
	 */
	value->address[value->length] = '\0';
	if (PyLong_Check(object)) {
		long val;

		val = strtol(value->address, NULL, 10);
		if ((ERANGE != errno) && (0 <= val) && (LONG_MAX >= val)) {
			ret = PyLong_FromLong(val);
		} else {
			raise_ValidationError(YDBPython_OSError, NULL, YDBPY_ERR_SYSCALL, "strtol", errno, strerror(errno));
			ret = NULL;
		}
	} else if (PyFloat_Check(object)) {
		double val;

		val = strtod(value->address, NULL);
		if ((ERANGE != errno) && (HUGE_VAL != val) && (-HUGE_VAL != val)) {
			ret = PyFloat_FromDouble(val);
		} else {
			raise_ValidationError(YDBPython_OSError, NULL, YDBPY_ERR_SYSCALL, "strtod", errno, strerror(errno));
			ret = NULL;
		}
	} else if (PyUnicode_Check(object)) {
		ret = PyUnicode_FromStringAndSize(value->address, value->length);
	} else if (PyBytes_Check(object)) {
		ret = PyBytes_FromStringAndSize(value->address, value->length);
	} else {
		/* Any unacceptable type should have been detected during initial
		 * validation of any PyObjects passed from Python to the C API,
		 * so we should never get here. So, assert this.
		 */
		assert(FALSE);
		ret = NULL;
	}

	/* Conversion was successful and a new object was created,
	 * so cleanup the previous object before returning.
	 */
	if (NULL != ret) {
		Py_DECREF(object);
	}
	return ret;
}

/* Confirm that the passed PyObject is a valid Python Sequence,
 * i.e. a Sequence of Python `str` (i.e. Unicode) objects.
 *
 * Validation and error reporting will vary depending on the type of Sequence
 * to be validated.
 *
 * Note: If YDBPython API calls are to accept Python `bytes` objects in
 * addition to `str` objects, this function will need to be updated.
 *
 * Parameters:
 *   object        - the Python object to check
 *   sequence_type - the type of Sequence to check for
 */
static bool is_valid_sequence(PyObject *object, YDBPythonSequenceType sequence_type, char *extra_prefix) {
	int	   max_sequence_len, max_item_len;
	Py_ssize_t i, sequence_len, item_len;
	PyObject * item, *sequence;
	char *	   err_prefix;

	if (Py_None == object) {
		// Allow Python None type
		return TRUE;
	}

	/* Different checks will need to be performed and error messages issued
	 * depending on the type of Sequence we are validating. So, check that
	 * here and initialize relevant variables accordingly.
	 */
	switch (sequence_type) {
	case YDBPython_VarnameSequence:
		max_sequence_len = YDB_MAX_NAMES;
		max_item_len = YDB_MAX_IDENT;
		err_prefix = YDBPY_ERR_VARNAME_INVALID;
		break;
	case YDBPython_SubsarraySequence:
		max_sequence_len = YDB_MAX_SUBS;
		max_item_len = YDB_MAX_STR;
		err_prefix = YDBPY_ERR_SUBSARRAY_INVALID;
		break;
	case YDBPython_NodeSequence:
		max_sequence_len = YDB_MAX_SUBS;
		max_item_len = YDB_MAX_STR;
		/* Aggregate multiple levels of error message nesting into single prefix.
		 * This allows for failures in Sequences of Nodes to be tracked through the
		 * multiple levels of indirection implicated in that case. Specifically, this
		 * allows error messages to report which item in the Node Sequence is faulty, as
		 * well as which item within the Node itself is faulty.
		 */
		err_prefix = extra_prefix;
		break;
	default:
		assert(FALSE);
		/* NULL/0 initialize otherwise unused variables, in case of release builds,
		 * where this assert will not be triggered.
		 */
		err_prefix = NULL;
		max_item_len = 0;
		max_sequence_len = 0;
		break;
	}

	/* Confirm object is a Sequence */
	sequence = PySequence_Fast(object, "argument must be iterable"); // New Reference
	if (!sequence || !(PyTuple_Check(object) || PyList_Check(object))) {
		raise_ValidationError(YDBPython_TypeError, err_prefix, YDBPY_ERR_NOT_LIST_OR_TUPLE);
		if (sequence) {
			DECREF_AND_RETURN(sequence, FALSE);
		} else {
			return FALSE;
		}
	}
	/* Validate Sequence length */
	sequence_len = PySequence_Fast_GET_SIZE(sequence);
	if (max_sequence_len < sequence_len) {
		raise_ValidationError(YDBPython_ValueError, err_prefix, YDBPY_ERR_SEQUENCE_TOO_LONG, sequence_len,
				      max_sequence_len);
		DECREF_AND_RETURN(sequence, FALSE);
	}

	/* Validate Sequence contents */
	for (i = 0; i < sequence_len; i++) {
		item = PySequence_Fast_GET_ITEM(sequence, i); // Borrowed Reference
		/* Validate item type (str or bytes) */
		if (PyUnicode_Check(item)) {
			// Get length of Unicode object by multiplying code points by code point size
			item_len = PyUnicode_GET_LENGTH(item) * PyUnicode_KIND(item);
		} else if (PyBytes_Check(item)) {
			item_len = PyBytes_Size(item);
		} else {
			raise_ValidationError(YDBPython_TypeError, err_prefix, YDBPY_ERR_ITEM_NOT_BYTES_LIKE, i);
			DECREF_AND_RETURN(sequence, FALSE);
		}
		/* Validate item length */
		if ((0 == max_item_len) || (max_item_len < item_len)) {
			raise_ValidationError(YDBPython_ValueError, err_prefix, YDBPY_ERR_BYTES_TOO_LONG, item_len, max_item_len);
			DECREF_AND_RETURN(sequence, FALSE);
		}
	}

	Py_DECREF(sequence);
	return TRUE;
}

/* Routine to convert a sequence of Python bytes objects into a C array of
 * ydb_buffer_ts. Routine assumes sequence was already validated with
 * 'is_valid_sequence' function or the special case macro
 * RETURN_IF_INVALID_SEQUENCE. The function creates a
 * copy of each Python bytes' data so the resulting array should be
 * freed by using the 'FREE_BUFFER_ARRAY' macro.
 *
 * Parameters:
 *    sequence    - a Python Object that is expected to be a Python Sequence containing Strings.
 */
int convert_py_sequence_to_ydb_buffer_array(PyObject *sequence, int sequence_len, ydb_buffer_t *buffer_array) {
	PyObject *bytes, *seq;
	int	  status;

	seq = PySequence_Fast(sequence, "argument must be iterable"); // New Reference
	if (!seq) {
		PyErr_SetString(YDBPythonError, "Can't convert none sequence to buffer array.");
		FREE_BUFFER_ARRAY(buffer_array, 0);
		return !YDB_OK;
	}

	for (int i = 0; i < sequence_len; i++) {

		bytes = PySequence_GetItem(seq, i);			   // New reference
		status = anystr_to_buffer(bytes, &buffer_array[i], FALSE); // Allocates buffer
		Py_DECREF(bytes);
		if (YDB_OK != status) {
			FREE_BUFFER_ARRAY(buffer_array, i);
			Py_DECREF(seq);
			return !YDB_OK;
		}
	}

	Py_DECREF(seq);
	return YDB_OK;
}

int populate_subs_used_and_subsarray(PyObject *pysubs, int *ret_subs_used, ydb_buffer_t **subsarray) {
	int status = YDB_OK;
	int subs_used;

	subs_used = 0;
	(*subsarray) = NULL;
	if (Py_None != pysubs) {
		subs_used = PySequence_Length(pysubs);
		(*subsarray) = malloc(subs_used * sizeof(ydb_buffer_t));
		status = convert_py_sequence_to_ydb_buffer_array(pysubs, subs_used, (*subsarray));
		if (YDB_OK != status) {
			FREE_BUFFER_ARRAY((*subsarray), subs_used);
		}
	}
	(*ret_subs_used) = subs_used;
	return status;
}

/* converts an array of ydb_buffer_ts into a sequence (Tuple) of Python strings.
 *
 * Parameters:
 *    buffer_array       - a C array of ydb_buffer_ts
 *    len                - the length of the above array
 */
PyObject *convert_ydb_buffer_array_to_py_tuple(ydb_buffer_t *buffer_array, int len) {
	int	  i;
	PyObject *return_tuple;

	return_tuple = PyTuple_New(len); // New Reference
	for (i = 0; i < len; i++)
		PyTuple_SetItem(return_tuple, i, Py_BuildValue("y#", buffer_array[i].buf_addr, buffer_array[i].len_used));
	return return_tuple;
}

/* This function will take an already allocated array of YDBNode structures and load it with the
 * data contained in the PyObject arguments.
 *
 * Parameters:
 *    dest       - pointer to the YDBNode to fill.
 *    varname    - Python bytes object representing the varname
 *    subsarray  - array of Python bytes objects representing the array of subscripts
 *                   Note: Because this function calls `convert_py_sequence_to_ydb_buffer_array`
 *                          subsarray should be validated with the
 *                          RETURN_IF_INVALID_SEQUENCE macro.
 */
static bool load_YDBNode(YDBNode *dest, PyObject *varname, PyObject *subsarray) {
	bool	      done;
	Py_ssize_t    len_ssize, sequence_len_ssize;
	unsigned int  len;
	int	      status;
	char *	      bytes_c;
	ydb_buffer_t *varname_y, *subsarray_y;

	if (!PyBytes_Check(varname)) {
		// Convert Unicode object to bytes object
		varname = PyUnicode_AsEncodedString(varname, "utf-8", "strict"); // New reference
		assert(PyBytes_Check(varname));
	}

	len_ssize = PyBytes_Size(varname);
	len = Py_SAFE_DOWNCAST(len_ssize, Py_ssize_t, unsigned int);
	bytes_c = PyBytes_AsString(varname);

	varname_y = malloc(1 * sizeof(ydb_buffer_t));
	YDB_MALLOC_BUFFER(varname_y, len);
	YDB_COPY_BYTES_TO_BUFFER(bytes_c, len, varname_y, done);
	if (!done) {
		YDB_FREE_BUFFER(varname_y);
		free(varname_y);
		PyErr_SetString(YDBPythonError, "failed to copy bytes object to buffer");
		return false;
	}

	dest->varname = varname_y;
	if (Py_None != subsarray) {
		sequence_len_ssize = PySequence_Length(subsarray);
		dest->subs_used = Py_SAFE_DOWNCAST(sequence_len_ssize, Py_ssize_t, unsigned int);
		subsarray_y = malloc(dest->subs_used * sizeof(ydb_buffer_t));
		status = convert_py_sequence_to_ydb_buffer_array(subsarray, dest->subs_used, subsarray_y);
		if (YDB_OK == status) {
			dest->subsarray = subsarray_y;
		} else {
			YDB_FREE_BUFFER(varname_y);
			free(varname_y);
			FREE_BUFFER_ARRAY(subsarray_y, dest->subs_used);
			PyErr_SetString(YDBPythonError, "failed to covert sequence to buffer array");
			return false;
		}
	} else {
		dest->subs_used = 0;
		dest->subsarray = NULL;
	}
	return true;
}

/* Routine to free a YDBNode structure.
 *
 * Parameters:
 *    node    - pointer to the YDBNode to free.
 */
static void free_YDBNode(YDBNode *node) {
	if (NULL != node) {
		YDB_FREE_BUFFER((node->varname));
		FREE_BUFFER_ARRAY(node->subsarray, node->subs_used);
	}
}

/* Routine to validate a sequence of Python sequences representing nodes. (Used
 * only by lock().)
 * Validation rule:
 *      1) node_sequence must be a sequence
 *      2) each item in node_sequence must be a sequence
 *      3) each item must be a sequence of 1 or 2 sub-items.
 *      4) item[0] must be a bytes object.
 *      5) item[1] either does not exist, is None or a sequence
 *      6) if item[1] is a sequence then it must contain only bytes objects.
 *
 * Parameters:
 *    nodes_sequence        - a Python object that is to be validated.
 *    max_len              - the number of nodes that are allowed in the nodes_sequence
 *    error_message        - a preallocated string for storing the reason for validation failure.
 */
static int is_valid_node_sequence(PyObject *nodes_sequence, int max_len) {
	Py_ssize_t i, len_nodes, len_node_seq;
	PyObject * node, *varname, *subsarray, *seq, *node_seq;
	bool	   error_encountered;

	/* validate node sequence type */
	seq = PySequence_Fast(nodes_sequence, "'nodes' argument must be a Sequence"); // New Reference
	if (!(PyTuple_Check(nodes_sequence) || PyList_Check(nodes_sequence))) {
		raise_ValidationError(YDBPython_TypeError, YDBPY_ERR_NODES_INVALID, YDBPY_ERR_NOT_LIST_OR_TUPLE);
		DECREF_AND_RETURN(seq, FALSE);
	}

	/* validate node sequence length */
	len_nodes = PySequence_Fast_GET_SIZE(seq);
	if (max_len < len_nodes) {
		raise_ValidationError(YDBPython_ValueError, YDBPY_ERR_NODES_INVALID, YDBPY_ERR_SEQUENCE_TOO_LONG, len_nodes, max_len);
		DECREF_AND_RETURN(seq, FALSE);
	}

	/* validate each item/node in node sequence */
	error_encountered = FALSE;
	for (i = 0; i < len_nodes; i++) {
		node = PySequence_Fast_GET_ITEM(seq, i); // Borrowed Reference
		if (!(PyTuple_Check(node) || PyList_Check(node))) {
			raise_ValidationError(YDBPython_TypeError, YDBPY_ERR_NODES_INVALID, YDBPY_ERR_NOT_LIST_OR_TUPLE);
			DECREF_AND_RETURN(seq, FALSE);
		}
		node_seq = PySequence_Fast(node, ""); // New Reference
		len_node_seq = PySequence_Fast_GET_SIZE(node_seq);

		if (1 <= len_node_seq) {
			varname = PySequence_Fast_GET_ITEM(node_seq, 0); // Borrowed Reference
			if ((!PyUnicode_Check(varname)) && (!PyBytes_Check(varname))) {
				raise_ValidationError(YDBPython_TypeError, YDBPY_ERR_NODES_INVALID,
						      YDBPY_ERR_VARNAME_NOT_BYTES_LIKE);
				Py_DECREF(node_seq);
				DECREF_AND_RETURN(seq, FALSE);
			}
		} else {
			varname = Py_None;
		}

		if (2 <= len_node_seq) {
			subsarray = PySequence_GetItem(node, 1); // Borrowed Reference
		} else {
			subsarray = Py_None;
		}

		if (!node_seq || ((2 == i) && !(PyTuple_Check(node) || PyList_Check(node)))) {
			/* Validate item/node type [list or tuple]. Only relevant for second
			 * item, i.e. subsarray, not varname
			 */
			raise_ValidationError(YDBPython_TypeError, YDBPY_ERR_NODES_INVALID,
					      YDBPY_ERR_NODE_IN_SEQUENCE_NOT_LIST_OR_TUPLE, i);
			error_encountered = TRUE;
		} else if ((1 != len_node_seq) && (2 != len_node_seq)) {
			/* validate item/node length [1 or 2] */
			raise_ValidationError(YDBPython_ValueError, YDBPY_ERR_NODES_INVALID,
					      YDBPY_ERR_NODE_IN_SEQUENCE_INCORRECT_LENGTH, i);
			error_encountered = TRUE;
		} else if ((!PyUnicode_Check(varname)) && (!PyBytes_Check(varname))) {
			/* validate item/node first element (varname) type */
			raise_ValidationError(YDBPython_TypeError, YDBPY_ERR_NODES_INVALID, YDBPY_ERR_ITEM_NOT_BYTES_LIKE, i);
			error_encountered = TRUE;
		} else if (2 == len_node_seq) {
			/* validate item/node second element (subsarray) if it exists */
			if (Py_None != subsarray) {
				int  copied;
				char tmp_prefix[YDBPY_MAX_ERRORMSG];
				char err_prefix[YDBPY_MAX_ERRORMSG];

				/* Incrementally build up error prefix format string to create
				 * a nested error message that contains full error information
				 * for the validation failure in the given sequence. This is
				 * needed to account for the fact that lock() accepts Sequences
				 * within Sequences. Specifically, it accepts a Sequence of
				 * YDBNodes, which are themselves Sequences. Accordingly, a full
				 * error message notes which YDBNode is invalid as well as which
				 * element of the YDBNode subsarray Sequence is invalid.
				 */
				copied = snprintf(tmp_prefix, YDBPY_MAX_ERRORMSG, YDBPY_ERR_NODES_INVALID,
						  YDBPY_ERR_NODE_IN_SEQUENCE_SUBSARRAY_INVALID);
				assert(copied < YDBPY_MAX_ERRORMSG);
				UNUSED(copied);
				copied = snprintf(err_prefix, YDBPY_MAX_ERRORMSG, tmp_prefix, i, "%s");
				assert(copied < YDBPY_MAX_ERRORMSG);
				UNUSED(copied);

				if (!is_valid_sequence(subsarray, YDBPython_NodeSequence, err_prefix)) {
					error_encountered = TRUE;
				}
			}
		}

		Py_DECREF(node_seq);
		if (error_encountered) {
			DECREF_AND_RETURN(seq, FALSE);
		}
	}
	Py_DECREF(seq);
	return TRUE;
}

/* Takes an already validated (by 'validate_py_nodes_sequence' above) PyObject sequence
 * that represents a series of nodes loads that data into an already allocated array
 * of YDBNodes. (note: 'ret_nodes' should later be freed by 'free_YDBNode_array' below)
 *
 * Parameters:
 *    sequence    - a Python object that has already been validated with 'validate_py_nodes_sequence' or equivalent.
 */
static bool load_YDBNodes_from_node_sequence(PyObject *sequence, Py_ssize_t len_nodes, YDBNode *ret_nodes) {
	bool	   success = true;
	Py_ssize_t i;
	PyObject * node, *varname, *subsarray, *seq, *node_seq;

	seq = PySequence_Fast(sequence, "argument must be iterable"); // New Reference
	if ((NULL == seq) || (0 == len_nodes)) {
		return false;
	}

	for (i = 0; i < len_nodes; i++) {
		node = PySequence_Fast_GET_ITEM(seq, i);			     // Borrowed Reference
		node_seq = PySequence_Fast(node, "argument must be iterable"); // New Reference
		if (NULL == node_seq) {
			Py_DECREF(seq);
			return false;
		}

		varname = PySequence_Fast_GET_ITEM(node_seq, 0); // Borrowed Reference
		if (2 == PySequence_Fast_GET_SIZE(node_seq)) {
			subsarray = PySequence_Fast_GET_ITEM(node_seq, 1); // Borrowed Reference
		} else {
			subsarray = Py_None;
		}
		success = load_YDBNode(&ret_nodes[i], varname, subsarray);
		Py_DECREF(node_seq);
		if (!success)
			break;
	}
	Py_DECREF(seq);
	return success;
}

/* Routine to free an array of YDBNodes as returned by above
 * 'load_YDBNodes_from_node_sequence'.
 *
 * Parameters:
 *    nodesarray    - the array that is to be freed.
 *    len          - the number of elements in nodesarray.
 */
static void free_YDBNode_array(YDBNode *nodesarray, int len) {
	int i;
	if (NULL != nodesarray) {
		for (i = 0; i < len; i++)
			free_YDBNode(&nodesarray[i]);
		free(nodesarray);
	}
}

/* Routine to help raise a YDBError. The caller still needs to return NULL for
 * the Exception to be raised.
 *
 * Parameters:
 *    status                 - the error code that is returned by the wrapped ydb_ function.
 */
static void raise_YDBError(int status) {
	ydb_char_t   error_string[YDBPY_MAX_ERRORMSG];
	ydb_buffer_t error_buffer;
	PyObject *   error_type;
	PyObject *   message;
	int	     copied = 0, zstatus;
	char	     full_error_message[YDBPY_MAX_ERRORMSG + CANONICAL_NUMBER_TO_STRING_MAX];

	/* YDB_ERR_NODEEND and errors in the YDB_DEFER_HANDLER to YDB_INT_MAX
	 * range will not have a value stored in $ZSTATUS, so look up the
	 * error message manually. In the cases of YDB_NOTOK and YDB_DEFER_HANDLER,
	 * which have no valid error message and are not possible in YDBPython,
	 * just use the error code number with no text description.
	 */
	if ((YDB_ERR_TPTIMEOUT == status) || (YDB_ERR_NODEEND == status)
	    || ((YDB_INT_MAX > status) && (YDB_DEFER_HANDLER <= status))) {
		if ((YDB_ERR_TPTIMEOUT == status) || (YDB_ERR_NODEEND == status) || (YDB_LOCK_TIMEOUT == status)) {
			error_buffer.buf_addr = error_string;
			error_buffer.len_alloc = YDBPY_MAX_ERRORMSG;
			error_buffer.len_used = 0;
			zstatus = ydb_message(status, &error_buffer);
			assert(YDB_OK == zstatus);
			error_buffer.buf_addr[error_buffer.len_used] = '\0';
		}

		if (YDB_TP_ROLLBACK == status) {
			copied = snprintf(full_error_message, YDBPY_MAX_ERRORMSG + CANONICAL_NUMBER_TO_STRING_MAX,
					  "%d, %%YDB-TP-ROLLBACK: Transaction not committed.", status);
			error_type = YDBTPRollback;
		} else if (YDB_TP_RESTART == status) {
			copied = snprintf(full_error_message, YDBPY_MAX_ERRORMSG + CANONICAL_NUMBER_TO_STRING_MAX,
					  "%d, %%YDB-TP-RESTART: Restarting transaction.", status);
			error_type = YDBTPRestart;
		} else if (YDB_ERR_TPTIMEOUT == status) {
			copied = snprintf(full_error_message, YDBPY_MAX_ERRORMSG + CANONICAL_NUMBER_TO_STRING_MAX, "%d, %s", status,
					  error_string);
			error_type = YDBTPTimeoutError;
		} else if (YDB_NOTOK == status) {
			copied = snprintf(full_error_message, YDBPY_MAX_ERRORMSG + CANONICAL_NUMBER_TO_STRING_MAX, "%d", status);
			error_type = YDBNotOk;
		} else if (YDB_LOCK_TIMEOUT == status) {
			copied = snprintf(full_error_message, YDBPY_MAX_ERRORMSG + CANONICAL_NUMBER_TO_STRING_MAX, "%d, %s", status,
					  error_string);
			error_type = YDBLockTimeoutError;
		} else if (YDB_DEFER_HANDLER == status) {
			copied = snprintf(full_error_message, YDBPY_MAX_ERRORMSG + CANONICAL_NUMBER_TO_STRING_MAX, "%d", status);
			error_type = YDBDeferHandler;
		} else if (YDB_ERR_NODEEND == status) {
			copied = snprintf(full_error_message, YDBPY_MAX_ERRORMSG + CANONICAL_NUMBER_TO_STRING_MAX, "%d, %s", status,
					  error_string);
			error_type = YDBNodeEnd;
		} else {
			assert(FALSE);
		}
	} else {
		zstatus = ydb_zstatus(error_string, YDB_MAX_ERRORMSG);
		if ((YDB_OK == zstatus) || (YDB_ERR_INVSTRLEN == zstatus)) {
			assert(NULL != error_string);
			copied = snprintf(full_error_message, YDBPY_MAX_ERRORMSG, "%s", error_string);
			error_type = YDBError;
		} else {
			assert(FALSE);
			copied = snprintf(full_error_message, YDBPY_MAX_ERRORMSG, "%d, UNKNOWN error", status);
			error_type = YDBError;
		}
	}

	assert((0 <= copied) && (YDBPY_MAX_ERRORMSG > copied));
	UNUSED(copied);
	message = Py_BuildValue("s", full_error_message); // New Reference
	
	// Note that the below call to Py_BuildValue with a first argument of "y" is done in addition
	// to the call above with "s" due to the reasoning at:
	// https://gitlab.com/YottaDB/Lang/YDBPython/-/merge_requests/61#note_2283744959
	if (NULL == message) {
		message = Py_BuildValue("y", full_error_message); // New Reference
	}
	RAISE_SPECIFIC_ERROR(error_type, message);
	Py_DECREF(message);
}

/* API Wrappers */

/* FOR ALL BELOW WRAPPERS:
 * Each function converts Python types to the appropriate C types and passes them to the matching
 * YottaDB Simple API function then convert the return types and errors into appropriate Python types
 * and return those values
 *
 * Parameters:
 *    self        - the object that this method belongs to (in this case it's the _yottadb module.)
 *    args        - a Python tuple of the positional arguments passed to the function.
 *    kwds        - a Python dictionary of the keyword arguments passed to the function.
 */

/* Initialize a py_ci_name_descriptor struct with the name of a call-in routine.
 * Used by cip() to prepare for a YottaDB call-in.
 */
static int set_routine_name(char *routine_name) {
	assert(NULL != routine_name);
	ci_info.ci_info.rtn_name.length = strnlen(routine_name, YDB_MAX_IDENT);
	if (0 >= ci_info.ci_info.rtn_name.length) {
		PyErr_Format(YDBPythonError, "Failed to initialize call-in information for routine: %s", routine_name);
		return !YDB_OK;
	}
	ci_info.ci_info.rtn_name.length++; // Null terminator
	if (NULL != ci_info.ci_info.rtn_name.address) {
		free(ci_info.ci_info.rtn_name.address);
	}
	ci_info.ci_info.rtn_name.address = malloc((ci_info.ci_info.rtn_name.length) * sizeof(char));
	memcpy(ci_info.ci_info.rtn_name.address, routine_name, ci_info.ci_info.rtn_name.length);
	ci_info.ci_info.handle = NULL;
	ci_info.has_parm_types = FALSE;
	return YDB_OK;
}

/* Cleans up a ci_name_descriptor struct by freeing memory and resetting
 * member values of the global ci_info struct.
 */
static void free_ci_name_descriptor() {
	if (NULL != ci_info.ci_info.rtn_name.address) {
		free(ci_info.ci_info.rtn_name.address);
		ci_info.ci_info.rtn_name.address = NULL;
	}
	ci_info.ci_info.rtn_name.length = 0;
	ci_info.ci_info.handle = NULL;
	ci_info.has_parm_types = FALSE;
}

static PyObject *ci_wrapper(PyObject *args, PyObject *kwds, bool is_cip) {
	bool	      return_null = false;
	int	      status, has_retval;
	PyObject *    routine, *routine_args, *seq, *py_arg, *ret;
	unsigned int  inmask, outmask, io_args, num_args, cur_index, cur_arg;
	ydb_buffer_t  routine_name;
	ydb_string_t *args_ydb;
	ydb_string_t  ret_val;
	gparam_list   arg_values;
	ci_parm_type  parm_types;

	seq = routine_args = NULL;
	has_retval = FALSE;
	ret = NULL; // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC

	// Parse and validate
	static char *kwlist[] = {"routine", "args", "has_retval", NULL};
	// Parsed values are borrowed references, do not Py_DECREF them.
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|Op", kwlist, &routine, &routine_args, &has_retval)) {
		return NULL;
	}
	if (Py_None == routine) {
		raise_ValidationError(YDBPython_ValueError, NULL, YDBPY_ERR_ROUTINE_UNSPECIFIED);
		return NULL;
	}

	// Lookup routine parameter information for construction of argument array
	if (is_cip) {
		return_null = anystr_to_buffer(routine, &routine_name, FALSE);
	} else {
		return_null = anystr_to_buffer(routine, &routine_name, FALSE);
	}
	if (return_null) {
		return NULL;
	}
	assert(routine_name.len_used < routine_name.len_alloc);
	routine_name.buf_addr[routine_name.len_used] = '\0';
	/* Update routine information on initial call to cip() or when
	 * a different routine name is used on a subsequent call.
	 */
	if (is_cip) {
		if ((!ci_info.has_parm_types) || (NULL == ci_info.routine_name)
		    || (!strncmp(ci_info.routine_name, routine_name.buf_addr, YDB_MAX_IDENT))) {
			free_ci_name_descriptor();
			status = set_routine_name(routine_name.buf_addr);
			if (YDB_OK != status) {
				YDB_FREE_BUFFER(&routine_name);
				return NULL;
			}
			status = ydb_ci_get_info(routine_name.buf_addr, &ci_info.parm_types);
			if (YDB_OK != status) {
				raise_YDBError(status);
				YDB_FREE_BUFFER(&routine_name);
				return NULL;
			}
			ci_info.has_parm_types = TRUE;
		}
		parm_types = ci_info.parm_types;
	} else {
		status = ydb_ci_get_info(routine_name.buf_addr, &parm_types);
		if (YDB_OK != status) {
			raise_YDBError(status);
			YDB_FREE_BUFFER(&routine_name);
			return NULL;
		}
	}

	if (NULL == routine_args) {
		num_args = 0;
	} else {
		seq = PySequence_Fast(routine_args, "argument must be iterable"); // New Reference
		if ((!seq) || PyUnicode_Check(routine_args) || PyBytes_Check(routine_args)) {
			if (NULL != seq) {
				Py_DECREF(seq);
			}
			raise_ValidationError(YDBPython_TypeError, NULL, YDBPY_ERR_CALLIN_ARGS_NOT_SEQ);
			YDB_FREE_BUFFER(&routine_name);
			return NULL;
		}
		num_args = Py_SAFE_DOWNCAST(PySequence_Length(seq), Py_ssize_t, unsigned int);
	}

	// Setup for Call
	inmask = parm_types.input_mask;
	outmask = parm_types.output_mask;
	// Get total number of expected arguments
	io_args = count_args(inmask, outmask);
	if ((io_args != num_args) || ((NULL == routine_args) && (0 != io_args))) {
		raise_ValidationError(YDBPython_ValueError, NULL, YDBPY_ERR_INVALID_ARGS, routine_name.buf_addr, io_args, num_args);
		if (NULL != seq) {
			Py_DECREF(seq);
		}
		YDB_FREE_BUFFER(&routine_name);
		return NULL;
	}
	/* In the case of output arguments to ci(), as specified in the call-in table,
	 * an update will be required to the Python object containing the arguments to
	 * be passed to ydb_ci() with the output value for that argument. In that case,
	 * this Python object must be a List, i.e. mutable, and not a Tuple, i.e. immutable.
	 *
	 * Accordingly, check that here and raise an error if there is an output argument
	 * to be updated, but the argument list is immutable.
	 */
	if ((NULL == routine_args || !PyList_Check(routine_args)) && (0 != outmask)) {
		if (NULL != seq) {
			Py_DECREF(seq);
		}
		raise_ValidationError(YDBPython_TypeError, NULL, YDBPY_ERR_IMMUTABLE_OUTPUT_ARGS);
		YDB_FREE_BUFFER(&routine_name);
		return NULL;
	}
	if (0 < num_args) {
		args_ydb = malloc(num_args * sizeof(ydb_string_t));
		for (cur_arg = 0; cur_arg < num_args; cur_arg++) {
			py_arg = PySequence_GetItem(seq, cur_arg);			     // Borrowed Reference
			if (1 == (1 & inmask)) {					     // cur_arg is an input argument
				status = object_to_ydb_string_t(py_arg, &args_ydb[cur_arg]); // Allocates buffer
				if (YDB_OK != status) {
					raise_ValidationError(YDBPython_TypeError, NULL, YDBPY_ERR_INVALID_CI_ARG_TYPE,
							      routine_name.buf_addr, cur_arg + 1);
					FREE_STRING_ARRAY(args_ydb, cur_arg);
					YDB_FREE_BUFFER(&routine_name);
					Py_DECREF(seq);
					return NULL;
				}
			} else {			  // cur_arg is an output argument
				if (0 == (1 & outmask)) { // Check for unexpected parameter
					raise_ValidationError(YDBPython_ValueError, NULL, YDBPY_ERR_CI_PARM_UNDEFINED,
							      routine_name.buf_addr, cur_arg + 1);
					FREE_STRING_ARRAY(args_ydb, cur_arg);
					YDB_FREE_BUFFER(&routine_name);
					Py_DECREF(seq);
					return NULL;
				}
				/* Python caller cannot allocate C variables, so do that here.
				 * Any return value will later be converted into a Python object
				 * to be returned to caller, allowing this string to be freed then.
				 *
				 * Note that the call to object_to_ydb_string_t() is needed to derive
				 * a pre-allocation for output parameters. In the case where the user
				 * passes an empty string, we use a default value.
				 */
				status = object_to_ydb_string_t(py_arg, &args_ydb[cur_arg]); // Allocates buffer
				if (YDB_OK != status) {
					raise_ValidationError(YDBPython_TypeError, NULL, YDBPY_ERR_INVALID_CI_ARG_TYPE,
							      routine_name.buf_addr, cur_arg + 1);
					FREE_STRING_ARRAY(args_ydb, cur_arg);
					YDB_FREE_BUFFER(&routine_name);
					Py_DECREF(seq);
					return NULL;
				}
				/* This is an output only parameter passed as an empty string,
				 * so an initial length cannot be derived from the argument
				 * received from Python. So, allocate a default here.
				 */
				if (0 == args_ydb[cur_arg].length) {
					free(args_ydb[cur_arg].address);
					args_ydb[cur_arg].address = malloc(YDBPY_DEFAULT_OUTBUF * sizeof(char));
					args_ydb[cur_arg].length = YDBPY_DEFAULT_OUTBUF;
				}
			}
			inmask = inmask >> 1;
			outmask = outmask >> 1;
		}
	} else {
		args_ydb = NULL;
	}

	if (has_retval) {
		ret_val.address = malloc(YDB_MAX_STR * sizeof(char));
		ret_val.length = YDB_MAX_STR;
		num_args++; // Include the return value in the variadic argument list
	} else {
		ret_val.address = NULL;
	}
	num_args++; // Include ci_name_descriptor in argument list
	arg_values.n = (intptr_t)num_args;
	num_args--; // Exclude routine name from loops over argument list

	// Populate array of variadic arguments for function call
	cur_index = 0;
	if (is_cip) {
		arg_values.arg[cur_index] = &ci_info.ci_info;
	} else {
		arg_values.arg[cur_index] = routine_name.buf_addr;
	}
	cur_index++;
	if (has_retval) {
		arg_values.arg[cur_index] = &ret_val;
		num_args--; // Exclude ret_val from argument loop
		cur_index++;
	}
	for (cur_arg = 0; cur_arg < num_args; cur_arg++, cur_index++) {
		arg_values.arg[cur_index] = &args_ydb[cur_arg];
	}
	assert((num_args + has_retval + 1) == cur_index); // +1 for ci_name_descriptor

	if (is_cip) {
		status = ydb_call_variadic_plist_func((ydb_vplist_func)&ydb_cip, &arg_values);
	} else {
		status = ydb_call_variadic_plist_func((ydb_vplist_func)&ydb_ci, &arg_values);
	}
	if (YDB_OK != status) {
		FREE_STRING_ARRAY(args_ydb, num_args);
		raise_YDBError(status);
		if (NULL != seq) {
			Py_DECREF(seq);
		}
		if (NULL != ret_val.address) {
			free(ret_val.address);
		}
		YDB_FREE_BUFFER(&routine_name);
		return NULL;
	}

	// Update any output parameters in the argument list passed from Python
	outmask = parm_types.output_mask;
	for (cur_arg = 0; cur_arg < num_args; cur_arg++) {
		if (1 == (1 & outmask)) { // This is an output parameter, so update Python object with output value
			PyObject *new_item, *old_item;

			old_item = PySequence_GetItem(seq, cur_arg);				    // Borrowed Reference
			new_item = new_object_from_object_and_string(old_item, &args_ydb[cur_arg]); // New reference
			if (NULL == new_item) {
				// Exception raised in new_object_from_object_and_string
				return_null = TRUE;
				break;
			}
			PySequence_SetItem(seq, (Py_ssize_t)cur_arg, new_item); // Replace old item with object containing new value
		}
		outmask = outmask >> 1;
	}
	if (!return_null) {
		// Construct Python return value, if a return value was issued. See above comment for details.
		if (has_retval) {
			assert(NULL != ret_val.address);
			ret = Py_BuildValue("s#", ret_val.address, (Py_ssize_t)ret_val.length);
			free(ret_val.address);
		} else {
			Py_INCREF(Py_None);
			ret = Py_None;
		}
	}
	YDB_FREE_BUFFER(&routine_name);
	FREE_STRING_ARRAY(args_ydb, num_args);

	if (return_null) {
		return NULL;
	} else {
		return ret;
	}
}

/* Wrapper for ydb_cip() */
static PyObject *cip(PyObject *self, PyObject *args, PyObject *kwds) {
	UNUSED(self);
	return ci_wrapper(args, kwds, TRUE);
}

/* Wrapper for ydb_ci() */
static PyObject *ci(PyObject *self, PyObject *args, PyObject *kwds) {
	UNUSED(self);
	return ci_wrapper(args, kwds, FALSE);
}

static PyObject *open_ci_table(PyObject *self, PyObject *args, PyObject *kwds) {
	char *	   filename;
	int	   status;
	Py_ssize_t filename_len;
	PyObject * ret;
	uintptr_t  ret_value;

	UNUSED(self);

	/* Parse and validate */
	static char *kwlist[] = {"filename", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "s#", kwlist, &filename, &filename_len))
		return NULL;

	if (0 < filename_len) {
		/* Call the wrapped function */
		status = ydb_ci_tab_open(filename, &ret_value);
		if (YDB_OK != status) {
			raise_YDBError(status);
			return NULL;
		}
		/* Create Python object to return */
		ret = Py_BuildValue("k", ret_value); // New Reference
	} else {
		raise_ValidationError(YDBPython_ValueError, NULL, YDBPY_ERR_EMPTY_FILENAME);
		return NULL;
	}
	return ret;
}

static PyObject *switch_ci_table(PyObject *self, PyObject *args, PyObject *kwds) {
	int	  status;
	PyObject *ret;
	uintptr_t ret_value, handle;

	UNUSED(self);

	/* parse and validate */
	static char *kwlist[] = {"handle", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "k", kwlist, &handle))
		return NULL;

	/* Call the wrapped function */
	status = ydb_ci_tab_switch(handle, &ret_value);
	if (YDB_OK != status) {
		raise_YDBError(status);
		return NULL;
	}
	/* Create Python object to return */
	ret = Py_BuildValue("k", ret_value); // New Reference
	return ret;
}

static PyObject *message(PyObject *self, PyObject *args, PyObject *kwds) {
	int	     err_num, status;
	PyObject *   ret;
	ydb_buffer_t ret_val;

	UNUSED(self);

	/* parse and validate */
	static char *kwlist[] = {"err_num", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "i", kwlist, &err_num))
		return NULL;

	YDB_MALLOC_BUFFER(&ret_val, YDBPY_MAX_ERRORMSG);
	status = ydb_message(err_num, &ret_val);
	if (YDB_OK != status) {
		raise_YDBError(status);
		assert(YDB_ERR_INVSTRLEN != status);
		return NULL;
	}
	ret_val.buf_addr[ret_val.len_used] = '\0';
	/* Create Python object to return */
	ret = Py_BuildValue("s#", ret_val.buf_addr, ret_val.len_used); // New Reference
	return ret;
}

static PyObject *release(PyObject *self) {
	int	     status;
	PyObject *   ret;
	ydb_buffer_t ret_value, varname;
	char	     release[YDBPY_MAX_ERRORMSG];

	UNUSED(self);

	ret_value.buf_addr = release;
	ret_value.len_alloc = YDBPY_MAX_ERRORMSG;
	ret_value.len_used = 0;
	YDB_STRING_TO_BUFFER("$ZYRELEASE", &varname);
	status = ydb_get_s(&varname, 0, NULL, &ret_value);
	if (YDB_OK != status) {
		raise_YDBError(status);
		assert(YDB_ERR_INVSTRLEN != status);
		return NULL;
	}
	ret_value.buf_addr[ret_value.len_used] = '\0';
	/* Create Python object to return */
	ret = Py_BuildValue("s#", ret_value.buf_addr, ret_value.len_used); // New Reference
	return ret;
}

static PyObject *adjust_stdout_stderr(PyObject *self) {
	int status;

	UNUSED(self);

	status = ydb_stdout_stderr_adjust();
	if (YDB_OK != status) {
		raise_YDBError(status);
		return NULL;
	}

	Py_INCREF(Py_None);
	return Py_None;
}

/* Wrapper for ydb_data_s */
static PyObject *data(PyObject *self, PyObject *args, PyObject *kwds) {
	PyObject *    varname_py;
	int	      subs_used, status;
	unsigned int  ret_value;
	PyObject *    subsarray_py, *ret;
	ydb_buffer_t  varname_ydb;
	ydb_buffer_t *subsarray_ydb;

	UNUSED(self);
	ret = NULL;	      // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC
	subs_used = 0;	      // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC
	subsarray_ydb = NULL; // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC
	/* Default values for optional arguments passed from Python */
	subsarray_py = Py_None;

	/* Parse */
	static char *kwlist[] = {"varname", "subsarray", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|O", kwlist, &varname_py, &subsarray_py))
		return NULL;

	/* Validate */
	RETURN_IF_INVALID_SEQUENCE(subsarray_py, YDBPython_SubsarraySequence);

	/* Setup for call */
	INVOKE_ANYSTR_TO_BUFFER(varname_py, varname_ydb, TRUE);
	INVOKE_POPULATE_SUBS_USED_AND_SUBSARRAY_AND_CLEANUP_VARNAME(subsarray_py, subs_used, subsarray_ydb, varname_ydb);

	/* Call the wrapped function */
	status = ydb_data_s(&varname_ydb, subs_used, subsarray_ydb, &ret_value);
	FREE_BUFFER_ARRAY(subsarray_ydb, subs_used);
	YDB_FREE_BUFFER(&varname_ydb);

	if (YDB_OK != status) {
		raise_YDBError(status);
	} else {
		ret = Py_BuildValue("I", ret_value); // New Reference
	}
	return ret;
}

/* Wrapper for ydb_delete_s() */
static PyObject *delete_wrapper(PyObject *self, PyObject *args, PyObject *kwds) {
	int	      deltype, status, subs_used;
	PyObject *    varname_py, *subsarray_py;
	PyObject *    ret;
	ydb_buffer_t  varname_ydb;
	ydb_buffer_t *subsarray_ydb;

	UNUSED(self);
	ret = NULL;
	subs_used = 0;	      // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC
	subsarray_ydb = NULL; // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC
	/* Default values for optional arguments passed from Python */
	subsarray_py = Py_None;
	deltype = YDB_DEL_NODE;

	/* Parse */
	static char *kwlist[] = {"varname", "subsarray", "delete_type", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|Oi", kwlist, &varname_py, &subsarray_py, &deltype)) {
		return NULL;
	}

	/* Validate */
	RETURN_IF_INVALID_SEQUENCE(subsarray_py, YDBPython_SubsarraySequence);

	/* Setup for call */
	INVOKE_ANYSTR_TO_BUFFER(varname_py, varname_ydb, TRUE);
	INVOKE_POPULATE_SUBS_USED_AND_SUBSARRAY_AND_CLEANUP_VARNAME(subsarray_py, subs_used, subsarray_ydb, varname_ydb);

	/* Call the wrapped function */
	status = ydb_delete_s(&varname_ydb, subs_used, subsarray_ydb, deltype);
	YDB_FREE_BUFFER(&varname_ydb);
	FREE_BUFFER_ARRAY(subsarray_ydb, subs_used);

	if (YDB_OK != status) {
		raise_YDBError(status);
	} else {
		Py_INCREF(Py_None);
		ret = Py_None;
	}
	return ret;
}

/* Wrapper for ydb_delete_excl_s() */
static PyObject *delete_except(PyObject *self, PyObject *args, PyObject *kwds) {
	int	      namecount, status;
	ydb_buffer_t *varnames_ydb;
	PyObject *    varnames_py, *ret;

	UNUSED(self);
	ret = NULL;
	/* Default values for optional arguments passed from Python */
	varnames_py = Py_None;

	/* parse and validate */
	static char *kwlist[] = {"varnames", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "|O", kwlist, &varnames_py)) {
		return NULL;
	}
	RETURN_IF_INVALID_SEQUENCE(varnames_py, YDBPython_VarnameSequence);

	/* Setup for Call */
	namecount = 0;
	if (Py_None != varnames_py)
		namecount = PySequence_Length(varnames_py);
	if (0 < namecount) {
		varnames_ydb = malloc(namecount * sizeof(ydb_buffer_t));
		status = convert_py_sequence_to_ydb_buffer_array(varnames_py, namecount, varnames_ydb);
		if (YDB_OK != status) {
			FREE_BUFFER_ARRAY(varnames_ydb, namecount);
			return NULL;
		}
	} else {
		varnames_ydb = NULL;
	}

	status = ydb_delete_excl_s(namecount, varnames_ydb);
	FREE_BUFFER_ARRAY(varnames_ydb, namecount);
	/* Check status for errors and raise Exception */
	if (YDB_OK != status) {
		raise_YDBError(status);
	} else {
		Py_INCREF(Py_None);
		ret = Py_None;
	}
	return ret;
}

/* Wrapper for ydb_get_s() */
static PyObject *get(PyObject *self, PyObject *args, PyObject *kwds) {
	int	      subs_used, status;
	PyObject *    varname_py;
	PyObject *    subsarray_py, *ret;
	ydb_buffer_t  varname_ydb, ret_value;
	ydb_buffer_t *subsarray_ydb;

	UNUSED(self);
	ret = NULL;	      // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC
	subs_used = 0;	      // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC
	subsarray_ydb = NULL; // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC
	/* Default values for optional arguments passed from Python */
	subsarray_py = Py_None;

	/* Parse */
	static char *kwlist[] = {"varname", "subsarray", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|O", kwlist, &varname_py, &subsarray_py))
		return NULL;
	/* Validate */
	RETURN_IF_INVALID_SEQUENCE(subsarray_py, YDBPython_SubsarraySequence);

	/* Setup for call */
	INVOKE_ANYSTR_TO_BUFFER(varname_py, varname_ydb, TRUE);
	INVOKE_POPULATE_SUBS_USED_AND_SUBSARRAY_AND_CLEANUP_VARNAME(subsarray_py, subs_used, subsarray_ydb, varname_ydb);
	YDB_MALLOC_BUFFER(&ret_value, YDBPY_DEFAULT_VALUE_LEN);

	/* Call the wrapped function */
	status = ydb_get_s(&varname_ydb, subs_used, subsarray_ydb, &ret_value);
	/* Check to see if length of string was longer than YDBPY_DEFAULT_VALUE_LEN. If so, try again
	 * with proper length */
	if (YDB_ERR_INVSTRLEN == status) {
		FIX_BUFFER_LENGTH(ret_value);
		/* Call the wrapped function */
		status = ydb_get_s(&varname_ydb, subs_used, subsarray_ydb, &ret_value);
		assert(YDB_ERR_INVSTRLEN != status);
	}
	FREE_BUFFER_ARRAY(subsarray_ydb, subs_used);
	YDB_FREE_BUFFER(&varname_ydb);
	if (YDB_OK != status) {
		raise_YDBError(status);
	} else {
		/* Create Python object to return */
		/* New Reference */
		ret = Py_BuildValue("y#", ret_value.buf_addr, (Py_ssize_t)ret_value.len_used);
	}
	YDB_FREE_BUFFER(&ret_value);
	return ret;
}

/* Wrapper for ydb_incr_s() */
static PyObject *incr(PyObject *self, PyObject *args, PyObject *kwds) {
	int	      status, subs_used;
	Py_ssize_t    increment_py_len;
	PyObject *    varname_py, *increment_py;
	PyObject *    subsarray_py, *ret;
	ydb_buffer_t  increment_ydb, ret_value, varname_ydb;
	ydb_buffer_t *subsarray_ydb;

	UNUSED(self);
	ret = NULL;	      // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC
	subs_used = 0;	      // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC
	subsarray_ydb = NULL; // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC
	/* Default values for optional arguments passed from Python */
	subsarray_py = Py_None;
	increment_py = Py_None;

	/* Parse */
	static char *kwlist[] = {"varname", "subsarray", "increment", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|OO", kwlist, &varname_py, &subsarray_py, &increment_py,
					 &increment_py_len)) {
		return NULL;
	}
	/* Validate */
	RETURN_IF_INVALID_SEQUENCE(subsarray_py, YDBPython_SubsarraySequence);

	/* Setup for Call */
	INVOKE_ANYSTR_TO_BUFFER(varname_py, varname_ydb, TRUE);
	INVOKE_POPULATE_SUBS_USED_AND_SUBSARRAY_AND_CLEANUP_VARNAME(subsarray_py, subs_used, subsarray_ydb, varname_ydb);
	if (Py_None == increment_py) {
		// No value was specified, or it was None, so set node to a default of 1.
		YDB_MALLOC_BUFFER(&increment_ydb, YDBPY_DEFAULT_VALUE_LEN);
		increment_ydb.len_used = snprintf(increment_ydb.buf_addr, YDBPY_DEFAULT_VALUE_LEN, "1");
	} else {
		status = anystr_to_buffer(increment_py, &increment_ydb, FALSE);
		if (YDB_OK != status) {
			YDB_FREE_BUFFER(&varname_ydb);
			FREE_BUFFER_ARRAY(subsarray_ydb, subs_used);
			return NULL;
		}
	}
	YDB_MALLOC_BUFFER(&ret_value, CANONICAL_NUMBER_TO_STRING_MAX);

	/* Call the wrapped function */
	status = ydb_incr_s(&varname_ydb, subs_used, subsarray_ydb, &increment_ydb, &ret_value);
	FREE_BUFFER_ARRAY(subsarray_ydb, subs_used);
	YDB_FREE_BUFFER(&varname_ydb);
	YDB_FREE_BUFFER(&increment_ydb);
	if (YDB_OK != status) {
		raise_YDBError(status);
	} else {
		/* Create Python object to return. Creates a new reference */
		ret = Py_BuildValue("y#", ret_value.buf_addr, (Py_ssize_t)ret_value.len_used);
	}
	YDB_FREE_BUFFER(&ret_value);
	return ret;
}

/* Wrapper for ydb_lock_s() */
static PyObject *lock(PyObject *self, PyObject *args, PyObject *kwds) {
	bool		   return_null = false;
	bool		   success = true;
	int		   len_nodes, status;
	unsigned long long timeout_nsec;
	PyObject *	   nodes_py;
	YDBNode *	   nodes_ydb;

	UNUSED(self);
	/* Default values for optional arguments passed from Python */
	timeout_nsec = 0;
	nodes_py = Py_None;
	nodes_ydb = NULL;

	/* parse and validate */
	static char *kwlist[] = {"nodes", "timeout_nsec", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "|OK", kwlist, &nodes_py, &timeout_nsec))
		return NULL;

	if (Py_None == nodes_py) {
		len_nodes = 0;
	} else {
		if (!is_valid_node_sequence(nodes_py, YDB_LOCK_MAX_NODES)) {
			return NULL;
		}
		len_nodes = Py_SAFE_DOWNCAST(PySequence_Length(nodes_py), Py_ssize_t, int);
	}

	/* Setup for Call */
	if ((Py_None != nodes_py) && (0 < len_nodes)) {
		nodes_ydb = malloc(len_nodes * sizeof(YDBNode));
		success = load_YDBNodes_from_node_sequence(nodes_py, len_nodes, nodes_ydb);
		if (!success) {
			free(nodes_ydb);
			return NULL;
		}
	}

	if (!return_null) {
		gparam_list arg_values;
		int	    cur_node, cur_index;

		arg_values.n = (intptr_t)(YDB_LOCK_MIN_ARGS + (len_nodes * YDB_LOCK_ARGS_PER_NODE));
		arg_values.arg[0] = (void *)(uintptr_t)timeout_nsec;
		arg_values.arg[1] = (void *)(uintptr_t)len_nodes;

		/* Initialize arg_values index to the first location after the elements
		 * initialized above.
		 */
		cur_index = YDB_LOCK_MIN_ARGS;
		if (NULL != nodes_ydb) {
			for (cur_node = 0; cur_node < len_nodes; cur_node++) {
				arg_values.arg[cur_index] = nodes_ydb[cur_node].varname;
				arg_values.arg[cur_index + 1] = (void *)(uintptr_t)nodes_ydb[cur_node].subs_used;
				arg_values.arg[cur_index + 2] = nodes_ydb[cur_node].subsarray;
				cur_index += YDB_LOCK_ARGS_PER_NODE;
			}
		}

		status = ydb_call_variadic_plist_func((ydb_vplist_func)&ydb_lock_s, &arg_values);
		/* check for errors */
		if (YDB_LOCK_TIMEOUT == status) {
			PyErr_SetString(YDBLockTimeoutError, "Not able to acquire all requested locks in the specified time.");
			return_null = true;
		} else if (YDB_OK != status) {
			raise_YDBError(status);
			return_null = true;
		}
	}

	/* Free allocated memory */
	free_YDBNode_array(nodes_ydb, len_nodes);

	if (return_null) {
		return NULL;
	} else {
		Py_INCREF(Py_None);
		return Py_None;
	}
}

/* Wrapper for ydb_lock_decr_s() */
static PyObject *lock_decr(PyObject *self, PyObject *args, PyObject *kwds) {
	int	      status, subs_used;
	PyObject *    varname_py, *subsarray_py;
	PyObject *    ret;
	ydb_buffer_t  varname_ydb;
	ydb_buffer_t *subsarray_ydb;

	UNUSED(self);
	ret = NULL;
	subs_used = 0;	      // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC
	subsarray_ydb = NULL; // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC
	/* Default values for optional arguments passed from Python */
	subsarray_py = Py_None;

	/* Parse and validate */
	static char *kwlist[] = {"varname", "subsarray", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|O", kwlist, &varname_py, &subsarray_py))
		return NULL;
	RETURN_IF_INVALID_SEQUENCE(subsarray_py, YDBPython_SubsarraySequence);

	/* Setup for Call */
	INVOKE_ANYSTR_TO_BUFFER(varname_py, varname_ydb, TRUE);
	INVOKE_POPULATE_SUBS_USED_AND_SUBSARRAY_AND_CLEANUP_VARNAME(subsarray_py, subs_used, subsarray_ydb, varname_ydb);

	/* Call the wrapped function */
	status = ydb_lock_decr_s(&varname_ydb, subs_used, subsarray_ydb);
	YDB_FREE_BUFFER(&varname_ydb);
	FREE_BUFFER_ARRAY(subsarray_ydb, subs_used);
	if (YDB_OK != status) {
		raise_YDBError(status);
	} else {
		Py_INCREF(Py_None);
		ret = Py_None;
	}
	return ret;
}

/* Wrapper for ydb_lock_incr_s() */
static PyObject *lock_incr(PyObject *self, PyObject *args, PyObject *kwds) {
	int		   status, subs_used;
	PyObject *	   varname_py, *subsarray_py;
	PyObject *	   ret;
	unsigned long long timeout_nsec;
	ydb_buffer_t	   varname_ydb;
	ydb_buffer_t *	   subsarray_ydb;

	UNUSED(self);
	ret = NULL;
	subs_used = 0;	      // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC
	subsarray_ydb = NULL; // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC
	/* Default values for optional arguments passed from Python */
	subsarray_py = Py_None;
	timeout_nsec = 0;

	/* Parse and validate */
	static char *kwlist[] = {"varname", "subsarray", "timeout_nsec", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|OK", kwlist, &varname_py, &subsarray_py, &timeout_nsec)) {
		return NULL;
	}
	RETURN_IF_INVALID_SEQUENCE(subsarray_py, YDBPython_SubsarraySequence);

	/* Setup for Call */
	INVOKE_ANYSTR_TO_BUFFER(varname_py, varname_ydb, TRUE);
	INVOKE_POPULATE_SUBS_USED_AND_SUBSARRAY_AND_CLEANUP_VARNAME(subsarray_py, subs_used, subsarray_ydb, varname_ydb);

	/* Call the wrapped function */
	status = ydb_lock_incr_s(timeout_nsec, &varname_ydb, subs_used, subsarray_ydb);
	YDB_FREE_BUFFER(&varname_ydb);
	FREE_BUFFER_ARRAY(subsarray_ydb, subs_used);
	if (YDB_LOCK_TIMEOUT == status) {
		PyErr_SetString(YDBLockTimeoutError, "Not able to acquire all requested locks in the specified time.");
	} else if (YDB_OK != status) {
		raise_YDBError(status);
	} else {
		Py_INCREF(Py_None);
		ret = Py_None;
	}
	return ret;
}

/* Wrapper for ydb_node_next_s() */
static PyObject *node_next(PyObject *self, PyObject *args, PyObject *kwds) {
	int	      max_subscript_string, ret_subsarray_num_elements, ret_subs_used, status, subs_used;
	PyObject *    varname_py;
	PyObject *    subsarray_py, *ret;
	ydb_buffer_t  varname_ydb;
	ydb_buffer_t *ret_subsarray, *subsarray_ydb;

	UNUSED(self);
	ret = NULL;	      // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC
	subs_used = 0;	      // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC
	subsarray_ydb = NULL; // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC
	/* Default values for optional arguments passed from Python */
	subsarray_py = Py_None;

	/* Parse and validate */
	static char *kwlist[] = {"varname", "subsarray", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|O", kwlist, &varname_py, &subsarray_py))
		return NULL;
	RETURN_IF_INVALID_SEQUENCE(subsarray_py, YDBPython_SubsarraySequence);

	/* Setup for Call */
	INVOKE_ANYSTR_TO_BUFFER(varname_py, varname_ydb, TRUE);
	INVOKE_POPULATE_SUBS_USED_AND_SUBSARRAY_AND_CLEANUP_VARNAME(subsarray_py, subs_used, subsarray_ydb, varname_ydb);
	max_subscript_string = YDBPY_DEFAULT_SUBSCRIPT_LEN;
	ret_subsarray_num_elements = YDBPY_DEFAULT_SUBSCRIPT_COUNT;
	ret_subs_used = ret_subsarray_num_elements;
	ret_subsarray = create_empty_buffer_array(ret_subs_used, max_subscript_string);

	/* Call the wrapped function */
	status = ydb_node_next_s(&varname_ydb, subs_used, subsarray_ydb, &ret_subs_used, ret_subsarray);
	/* If not enough buffers in ret_subsarray */
	if (YDB_ERR_INSUFFSUBS == status) {
		FREE_BUFFER_ARRAY(ret_subsarray, ret_subsarray_num_elements);
		ret_subsarray_num_elements = ret_subs_used;
		ret_subsarray = create_empty_buffer_array(ret_subs_used, max_subscript_string);
		/* Re-call the wrapped function */
		status = ydb_node_next_s(&varname_ydb, subs_used, subsarray_ydb, &ret_subs_used, ret_subsarray);
	}
	/* If a buffer is not long enough */
	while (YDB_ERR_INVSTRLEN == status) {
		FIX_BUFFER_LENGTH(ret_subsarray[ret_subs_used]);
		ret_subs_used = ret_subsarray_num_elements;
		/* Re-call the wrapped function */
		status = ydb_node_next_s(&varname_ydb, subs_used, subsarray_ydb, &ret_subs_used, ret_subsarray);
	}
	YDB_FREE_BUFFER(&varname_ydb);
	FREE_BUFFER_ARRAY(subsarray_ydb, subs_used);
	assert(YDB_ERR_INVSTRLEN != status);

	/* Check status for errors and Raise Exception */
	if (YDB_OK != status) {
		raise_YDBError(status);
	} else {
		/* Create Python object to return */
		/* New Reference */
		ret = convert_ydb_buffer_array_to_py_tuple(ret_subsarray, ret_subs_used);
	}
	FREE_BUFFER_ARRAY(ret_subsarray, ret_subsarray_num_elements);
	return ret;
}

/* Wrapper for ydb_node_previous_s() */
static PyObject *node_previous(PyObject *self, PyObject *args, PyObject *kwds) {
	int	      max_subscript_string, ret_subsarray_num_elements, ret_subs_used, status, subs_used;
	PyObject *    varname_py;
	PyObject *    subsarray_py, *ret;
	ydb_buffer_t  varname_ydb;
	ydb_buffer_t *ret_subsarray, *subsarray_ydb;

	UNUSED(self);
	ret = NULL;	      // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC
	subs_used = 0;	      // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC
	subsarray_ydb = NULL; // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC
	/* Default values for optional arguments passed from Python */
	subsarray_py = Py_None;

	/* Parse and validate */
	static char *kwlist[] = {"varname", "subsarray", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|O", kwlist, &varname_py, &subsarray_py))
		return NULL;
	RETURN_IF_INVALID_SEQUENCE(subsarray_py, YDBPython_SubsarraySequence);

	/* Setup for Call */
	INVOKE_ANYSTR_TO_BUFFER(varname_py, varname_ydb, TRUE);
	INVOKE_POPULATE_SUBS_USED_AND_SUBSARRAY_AND_CLEANUP_VARNAME(subsarray_py, subs_used, subsarray_ydb, varname_ydb);
	max_subscript_string = YDBPY_DEFAULT_SUBSCRIPT_LEN;
	ret_subsarray_num_elements = YDBPY_DEFAULT_SUBSCRIPT_COUNT;
	ret_subs_used = ret_subsarray_num_elements;
	ret_subsarray = create_empty_buffer_array(ret_subs_used, max_subscript_string);

	/* Call the wrapped function */
	status = ydb_node_previous_s(&varname_ydb, subs_used, subsarray_ydb, &ret_subs_used, ret_subsarray);
	/* If not enough buffers in ret_subsarray */
	if (YDB_ERR_INSUFFSUBS == status) {
		FREE_BUFFER_ARRAY(ret_subsarray, ret_subsarray_num_elements);
		ret_subsarray_num_elements = ret_subs_used;
		ret_subsarray = create_empty_buffer_array(ret_subs_used, max_subscript_string);
		/* Re-call the wrapped function */
		status = ydb_node_previous_s(&varname_ydb, subs_used, subsarray_ydb, &ret_subs_used, ret_subsarray);
	}
	/* If a buffer is not long enough */
	while (YDB_ERR_INVSTRLEN == status) {
		FIX_BUFFER_LENGTH(ret_subsarray[ret_subs_used]);
		ret_subs_used = ret_subsarray_num_elements;
		/* Re-call the wrapped function */
		status = ydb_node_previous_s(&varname_ydb, subs_used, subsarray_ydb, &ret_subs_used, ret_subsarray);
	}
	FREE_BUFFER_ARRAY(subsarray_ydb, subs_used);
	YDB_FREE_BUFFER(&varname_ydb);
	assert(YDB_ERR_INVSTRLEN != status);

	/* Check status for errors and raise Exception */
	if (YDB_OK != status) {
		raise_YDBError(status);
	} else {
		/* Create Python object to return. Creates a new reference */
		ret = convert_ydb_buffer_array_to_py_tuple(ret_subsarray, ret_subs_used);
	}
	FREE_BUFFER_ARRAY(ret_subsarray, ret_subsarray_num_elements);
	return ret;
}

/* Wrapper for ydb_set_s() */
static PyObject *set(PyObject *self, PyObject *args, PyObject *kwds) {
	int	      status = YDB_OK, subs_used;
	PyObject *    varname_py, *value_py, *subsarray_py;
	PyObject *    ret;
	ydb_buffer_t  value_ydb, varname_ydb;
	ydb_buffer_t *subsarray_ydb;

	UNUSED(self);
	ret = NULL;
	subs_used = 0;	      // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC
	subsarray_ydb = NULL; // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC
	/* Default values for optional arguments passed from Python */
	subsarray_py = Py_None;
	value_py = Py_None;

	/* Parse and validate */
	static char *kwlist[] = {"varname", "subsarray", "value", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|OO", kwlist, &varname_py, &subsarray_py, &value_py)) {
		return NULL;
	}
	RETURN_IF_INVALID_SEQUENCE(subsarray_py, YDBPython_SubsarraySequence);

	/* Setup for Call */
	INVOKE_ANYSTR_TO_BUFFER(varname_py, varname_ydb, TRUE);
	INVOKE_POPULATE_SUBS_USED_AND_SUBSARRAY_AND_CLEANUP_VARNAME(subsarray_py, subs_used, subsarray_ydb, varname_ydb);
	if (Py_None == value_py) {
		// No value was specified, or it was None, so set node to empty string.
		YDB_MALLOC_BUFFER(&value_ydb, YDBPY_DEFAULT_VALUE_LEN);
		value_ydb.buf_addr[0] = '\0';
		value_ydb.len_used = 0;
	} else {
		status = anystr_to_buffer(value_py, &value_ydb, FALSE);
		if (YDB_OK != status) {
			FREE_BUFFER_ARRAY(subsarray_ydb, subs_used);
			YDB_FREE_BUFFER(&varname_ydb);
			return NULL;
		}
	}

	/* Call the wrapped function */
	status = ydb_set_s(&varname_ydb, subs_used, subsarray_ydb, &value_ydb);
	FREE_BUFFER_ARRAY(subsarray_ydb, subs_used);
	YDB_FREE_BUFFER(&varname_ydb);
	YDB_FREE_BUFFER(&value_ydb);

	if (YDB_OK != status) {
		raise_YDBError(status);
	} else {
		Py_INCREF(Py_None);
		ret = Py_None;
	}
	return ret;
}

/* Wrapper for ydb_str2zwr_s() */
static PyObject *str2zwr(PyObject *self, PyObject *args, PyObject *kwds) {
	int	     status;
	Py_ssize_t   str_py_len;
	PyObject *   str_py, *ret;
	ydb_buffer_t str_ydb, zwr_ydb;

	UNUSED(self);
	ret = NULL;

	/* Parse */
	static char *kwlist[] = {"input", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|", kwlist, &str_py, &str_py_len))
		return NULL;

	/* Setup for Call */
	INVOKE_ANYSTR_TO_BUFFER(str_py, str_ydb, FALSE);
	YDB_MALLOC_BUFFER(&zwr_ydb, YDBPY_DEFAULT_VALUE_LEN);

	/* Call the wrapped function */
	status = ydb_str2zwr_s(&str_ydb, &zwr_ydb);
	/* Re-call with properly sized buffer if zwr_buf is not long enough */
	if (YDB_ERR_INVSTRLEN == status) {
		FIX_BUFFER_LENGTH(zwr_ydb);
		/* recall the wrapped function */
		status = ydb_str2zwr_s(&str_ydb, &zwr_ydb);
		assert(YDB_ERR_INVSTRLEN != status);
	}
	YDB_FREE_BUFFER(&str_ydb);

	/* Check status for Errors and Raise Exception */
	if (YDB_OK != status) {
		raise_YDBError(status);
	} else {
		/* Create Python object to return. Creates a new reference */
		ret = Py_BuildValue("y#", zwr_ydb.buf_addr, (Py_ssize_t)zwr_ydb.len_used);
	}
	YDB_FREE_BUFFER(&zwr_ydb);
	return ret;
}

/* Wrapper for ydb_subscript_next_s() */
static PyObject *subscript_next(PyObject *self, PyObject *args, PyObject *kwds) {
	int	      status, subs_used;
	PyObject *    varname_py;
	PyObject *    subsarray_py, *ret;
	ydb_buffer_t  ret_value, varname_ydb;
	ydb_buffer_t *subsarray_ydb;

	UNUSED(self);
	ret = NULL;	      // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC
	subs_used = 0;	      // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC
	subsarray_ydb = NULL; // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC
	/* Default values for optional arguments passed from Python */
	subsarray_py = Py_None;

	/* Parse and validate */
	static char *kwlist[] = {"varname", "subsarray", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|O", kwlist, &varname_py, &subsarray_py)) {
		return NULL;
	}
	RETURN_IF_INVALID_SEQUENCE(subsarray_py, YDBPython_SubsarraySequence);

	/* Setup for Call */
	INVOKE_ANYSTR_TO_BUFFER(varname_py, varname_ydb, TRUE);
	INVOKE_POPULATE_SUBS_USED_AND_SUBSARRAY_AND_CLEANUP_VARNAME(subsarray_py, subs_used, subsarray_ydb, varname_ydb);
	YDB_MALLOC_BUFFER(&ret_value, YDBPY_DEFAULT_SUBSCRIPT_LEN);

	/* Call the wrapped function */
	status = ydb_subscript_next_s(&varname_ydb, subs_used, subsarray_ydb, &ret_value);
	/* Check whether length of string was longer than YDBPY_DEFAULT_SUBSCRIPT_LEN is so, try again
	 * with proper length */
	if (YDB_ERR_INVSTRLEN == status) {
		FIX_BUFFER_LENGTH(ret_value);
		/* recall the wrapped function */
		status = ydb_subscript_next_s(&varname_ydb, subs_used, subsarray_ydb, &ret_value);
		assert(YDB_ERR_INVSTRLEN != status);
	}
	FREE_BUFFER_ARRAY(subsarray_ydb, subs_used);
	YDB_FREE_BUFFER(&varname_ydb);

	if (YDB_OK != status) {
		raise_YDBError(status);
	} else {
		/* Create Python object to return. Creates new reference */
		ret = Py_BuildValue("y#", ret_value.buf_addr, (Py_ssize_t)ret_value.len_used);
	}
	YDB_FREE_BUFFER(&ret_value);
	return ret;
}

/* Wrapper for ydb_subscript_previous_s() */
static PyObject *subscript_previous(PyObject *self, PyObject *args, PyObject *kwds) {
	int	      status, subs_used;
	PyObject *    varname_py;
	PyObject *    subsarray_py, *ret;
	ydb_buffer_t  ret_value, varname_ydb;
	ydb_buffer_t *subsarray_ydb;

	UNUSED(self);
	ret = NULL;	      // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC
	subs_used = 0;	      // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC
	subsarray_ydb = NULL; // Initialize to prevent "maybe-uninitialized" compiler warning on old versions of GCC
	/* Default values for optional arguments passed from Python */
	subsarray_py = Py_None;

	/* Parse and validate */
	static char *kwlist[] = {"varname", "subsarray", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|O", kwlist, &varname_py, &subsarray_py)) {
		return NULL;
	}
	RETURN_IF_INVALID_SEQUENCE(subsarray_py, YDBPython_SubsarraySequence);

	/* Setup for call */
	INVOKE_ANYSTR_TO_BUFFER(varname_py, varname_ydb, TRUE);
	INVOKE_POPULATE_SUBS_USED_AND_SUBSARRAY_AND_CLEANUP_VARNAME(subsarray_py, subs_used, subsarray_ydb, varname_ydb);
	YDB_MALLOC_BUFFER(&ret_value, YDBPY_DEFAULT_SUBSCRIPT_LEN);

	/* Call the wrapped function */
	status = ydb_subscript_previous_s(&varname_ydb, subs_used, subsarray_ydb, &ret_value);

	/* Check whether length of string was longer than YDBPY_DEFAULT_SUBSCRIPT_LEN.
	 * If so, try again with proper length
	 */
	if (YDB_ERR_INVSTRLEN == status) {
		FIX_BUFFER_LENGTH(ret_value);
		status = ydb_subscript_previous_s(&varname_ydb, subs_used, subsarray_ydb, &ret_value);
		assert(YDB_ERR_INVSTRLEN != status);
	}
	YDB_FREE_BUFFER(&varname_ydb);
	FREE_BUFFER_ARRAY(subsarray_ydb, subs_used);

	/* Check status for Errors and Raise Exception */
	if (YDB_OK != status) {
		raise_YDBError(status);
	} else {
		/* Create Python object to return. Creates a new reference */
		ret = Py_BuildValue("y#", ret_value.buf_addr, (Py_ssize_t)ret_value.len_used);
	}
	YDB_FREE_BUFFER(&ret_value);
	return ret;
}

/* Callback functions used by Wrapper for ydb_tp_s() */

/* Callback Wrapper used by tp_st. The approach of calling a Python function is a
 * bit of a hack. Here's how it works:
 *      1) This is the callback function that is always passed to the ydb_tp_st
 *              Simple API function and should only ever be called by ydb_tp_st
 *              via tp() below. It assumes that everything passed to it was validated.
 *      2) The actual Python function to be called is passed to this function
 *              as the first element in a Python tuple.
 *      3) The positional arguments are passed as the second element and the
 *              keyword args are passed as the third.
 *      4) This function calls calls the Python callback function with the args and
 *              kwargs arguments.
 *      5) if a function raises an exception then this function returns TPCALLBACKINVRETVAL
 *              as a way of indicating an error.
 *      Note: the PyErr String is already set so the the function receiving the return
 *              value (tp()) just needs to return NULL.
 */
static int callback_wrapper(void *function_with_arguments) {
	int	  ret_value;
	bool	  decref_args = false;
	bool	  decref_kwargs = false;
	PyObject *function, *args, *kwargs, *ret;
	PyObject *err_object;

	function = PyTuple_GetItem(function_with_arguments, 0); // Borrowed Reference
	args = PyTuple_GetItem(function_with_arguments, 1);	// Borrowed Reference
	kwargs = PyTuple_GetItem(function_with_arguments, 2);	// Borrowed Reference

	if (Py_None == args) {
		args = PyTuple_New(0);
		decref_args = true;
	}
	if (Py_None == kwargs) {
		kwargs = PyDict_New();
		decref_kwargs = true;
	}

	ret = PyObject_Call(function, args, kwargs); // New Reference

	if (decref_args)
		Py_DECREF(args);
	if (decref_kwargs)
		Py_DECREF(kwargs);

	if (NULL == ret) {
		/* No return value was specified, signalling that `function` raised an exception.
		 * So, confirm that an exception was actually raised and determine the value to
		 * return to ydb_tp_s based on that, specifically looking out for YDB_TP_RESTART
		 * and YDB_TP_ROLLBACK.
		 *
		 *      Note: Do not need to `PyErr_SetString` because this or similar operation
		 *              was done when the exception was raised by `function`
		 */
		err_object = PyErr_Occurred();
		/* An exception should have been raised, but PyErr_Occurred() returned NULL.
		 * This should not happen, so assert that here.
		 */
		assert(err_object);
		if (PyErr_GivenExceptionMatches(err_object, YDBTPRestart)) {
			PyErr_Clear();
			return YDB_TP_RESTART;
		} else if (PyErr_GivenExceptionMatches(err_object, YDBTPRollback)) {
			PyErr_Clear();
			return YDB_TP_ROLLBACK;
		} else {
			/* If no TP-related exceptions occurred, return a default of YDB_ERR_TPCALLBACKINVRETVAL
			 * to signal to ydb_tp_s() that callback function is invalid without specifying a particular
			 * TP handling scenario, i.e. YDB_TP_RESTART or YDB_TP_ROLLBACK.
			 */
			return YDB_ERR_TPCALLBACKINVRETVAL;
		}
	} else if (!PyLong_Check(ret)) {
		PyErr_SetString(PyExc_TypeError, "Callback function must return value of type int.");
		return YDB_ERR_TPCALLBACKINVRETVAL;
	}
	ret_value = (int)PyLong_AsLong(ret);
	Py_DECREF(ret);
	return ret_value;
}

/* Wrapper for ydb_tp_s() */
static PyObject *tp(PyObject *self, PyObject *args, PyObject *kwds) {
	bool	      return_null = false;
	int	      namecount, status;
	char *	      transid;
	PyObject *    callback, *callback_args, *callback_kwargs, *varnames_py, *function_with_arguments;
	ydb_buffer_t *varnames_ydb;

	UNUSED(self);
	/* Default values for optional arguments passed from Python */
	callback_args = Py_None;
	callback_kwargs = Py_None;
	transid = "";
	namecount = 0;
	varnames_py = Py_None;

	/* parse and validate */
	static char *kwlist[] = {"callback", "args", "kwargs", "transid", "varnames", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|OOsO", kwlist, &callback, &callback_args, &callback_kwargs, &transid,
					 &varnames_py)) {
		return_null = true;
	}

	/* validate input */
	if (!PyCallable_Check(callback)) {
		PyErr_SetString(PyExc_TypeError, "'callback' must be a callable.");
		return_null = true;
	}
	if ((Py_None != callback_args) && !PyTuple_Check(callback_args)) {
		PyErr_SetString(PyExc_TypeError, "'args' must be a tuple. "
						 "(It will be passed to the callback "
						 "function as positional arguments.)");
		return_null = true;
	}
	if ((Py_None != callback_kwargs) && !PyDict_Check(callback_kwargs)) {
		PyErr_SetString(PyExc_TypeError, "'kwargs' must be a dictionary. "
						 "(It will be passed to the callback function as keyword arguments.)");
		return_null = true;
	}
	RETURN_IF_INVALID_SEQUENCE(varnames_py, YDBPython_VarnameSequence);
	if (!return_null) {
		/* Setup for Call */
		/* New Reference */
		function_with_arguments = Py_BuildValue("(OOO)", callback, callback_args, callback_kwargs);
		if (Py_None != varnames_py)
			namecount = PySequence_Length(varnames_py);

		if (0 < namecount) {
			varnames_ydb = (ydb_buffer_t *)malloc(namecount * sizeof(ydb_buffer_t));
			status = convert_py_sequence_to_ydb_buffer_array(varnames_py, namecount, varnames_ydb);
			if (YDB_OK != status) {
				Py_DECREF(function_with_arguments);
				return NULL;
			}
		} else {
			varnames_ydb = NULL;
		}

		/* Call the wrapped function */
		status = ydb_tp_s(callback_wrapper, function_with_arguments, transid, namecount, varnames_ydb);
		/* Check status for errors and raise exception */
		if (YDB_ERR_TPCALLBACKINVRETVAL == status) {
			// Exception already raised in callback_wrapper
			return_null = true;
		} else if (YDB_OK != status) {
			raise_YDBError(status);
			return_null = true;
		}
		/* Free allocated memory */
		Py_DECREF(function_with_arguments);
		FREE_BUFFER_ARRAY(varnames_ydb, namecount);
	}

	if (return_null) {
		return NULL;
	} else {
		return Py_BuildValue("i", status);
	}
}

/* Wrapper for ydb_zwr2str_s() */
static PyObject *zwr2str(PyObject *self, PyObject *args, PyObject *kwds) {
	int	     status;
	Py_ssize_t   zwr_py_len;
	PyObject *   zwr_py;
	PyObject *   ret;
	ydb_buffer_t zwr_ydb, str_ydb;

	UNUSED(self);
	ret = NULL;

	/* Parse */
	static char *kwlist[] = {"input", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|K", kwlist, &zwr_py, &zwr_py_len))
		return NULL;

	/* Setup for Call */
	INVOKE_ANYSTR_TO_BUFFER(zwr_py, zwr_ydb, FALSE);
	YDB_MALLOC_BUFFER(&str_ydb, YDBPY_DEFAULT_VALUE_LEN);

	/* Call the wrapped function */
	status = ydb_zwr2str_s(&zwr_ydb, &str_ydb);
	/* recall with properly sized buffer if zwr_ydb is not long enough */
	if (YDB_ERR_INVSTRLEN == status) {
		FIX_BUFFER_LENGTH(str_ydb);
		/* recall the wrapped function */
		status = ydb_zwr2str_s(&zwr_ydb, &str_ydb);
		assert(YDB_ERR_INVSTRLEN != status);
	}
	YDB_FREE_BUFFER(&zwr_ydb);

	/* Check status for errors and raise Exception */
	if (YDB_OK != status) {
		raise_YDBError(status);
	} else {
		/* New Reference */
		ret = Py_BuildValue("y#", str_ydb.buf_addr, (Py_ssize_t)str_ydb.len_used);
	}
	YDB_FREE_BUFFER(&str_ydb);
	return ret;
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
/* First we will create an array of structs that represent the methods in the module.
 * (https://docs.python.org/3/c-api/structures.html#c.PyMethodDef)
 * Each struct has 4 elements:
 *      1. The Python name of the method
 *      2. The C function that is to be called when the method is called
 *      3. How the function can be called (all these functions allow for calling with both positional and keyword arguments)
 *      4. The docstring for this method (to be used by help() in the Python REPL)
 * The final struct is a sentinel value to indicate the end of the array (i.e. {NULL, NULL, 0, NULL})
 */
static PyMethodDef methods[] = {
    /* Simple and Simple API Functions */
    {"ci", (PyCFunction)ci, METH_VARARGS | METH_KEYWORDS,
     "call an M routine defined in the call-in table specified by either the ydb_ci environment variable\n"
     "or switch_ci_table() using the arguments passed, if any"},
    {"cip", (PyCFunction)cip, METH_VARARGS | METH_KEYWORDS,
     "call an M routine defined in the call-in table specified by the ydb_ci environment variable\n"
     "or switch_ci_table() using the arguments passed, if any, while using cached call-in\n"
     "information for performance"},
    {"data", (PyCFunction)data, METH_VARARGS | METH_KEYWORDS,
     "used to learn what type of data is at a node.\n "
     "0 : There is neither a value nor a subtree, "
     "i.e., it is undefined.\n"
     "1 : There is a value, but no subtree\n"
     "10 : There is no value, but there is a subtree.\n"
     "11 : There are both a value and a subtree.\n"},
    {"delete", (PyCFunction)delete_wrapper, METH_VARARGS | METH_KEYWORDS, "deletes node value or tree data at node"},
    {"delete_except", (PyCFunction)delete_except, METH_VARARGS | METH_KEYWORDS,
     "delete the trees of all local variables "
     "except those in the 'varnames' array"},
    {"get", (PyCFunction)get, METH_VARARGS | METH_KEYWORDS, "returns the value of a node or raises exception"},
    {"incr", (PyCFunction)incr, METH_VARARGS | METH_KEYWORDS, "increments value by the value specified by 'increment'"},

    {"lock", (PyCFunction)lock, METH_VARARGS | METH_KEYWORDS, "..."},

    {"lock_decr", (PyCFunction)lock_decr, METH_VARARGS | METH_KEYWORDS,
     "Decrements the count of the specified lock held "
     "by the process. As noted in the Concepts section, a "
     "lock whose count goes from 1 to 0 is released. A lock "
     "whose name is specified, but which the process does "
     "not hold, is ignored."},
    {"lock_incr", (PyCFunction)lock_incr, METH_VARARGS | METH_KEYWORDS,
     "Without releasing any locks held by the process, "
     "attempt to acquire the requested lock incrementing it"
     " if already held."},
    {"message", (PyCFunction)message, METH_VARARGS | METH_KEYWORDS,
     "return the message string corresponding to the specified error code number\n"},
    {"node_next", (PyCFunction)node_next, METH_VARARGS | METH_KEYWORDS,
     "facilitate depth-first traversal of a local or global"
     " variable tree. returns string tuple of subscripts of"
     " next node with value."},
    {"node_previous", (PyCFunction)node_previous, METH_VARARGS | METH_KEYWORDS,
     "facilitate depth-first traversal of a local "
     "or global variable tree. returns string tuple"
     "of subscripts of previous node with value."},
    {"open_ci_table", (PyCFunction)open_ci_table, METH_VARARGS | METH_KEYWORDS,
     "open the specified call-in table file to allow calls to functions specified therein using ci() and cip()\n"},
    {"release", (PyCFunction)release, METH_NOARGS,
     "returns the release number of the active YottaDB installation. Equivalent to $ZYRELEASE in M.\n"},
    {"adjust_stdout_stderr", (PyCFunction)adjust_stdout_stderr, METH_NOARGS,
     "Check whether stdout (file descriptor 1) and stderr (file descriptor 2) are the same file, and if so, route stderr writes to "
     "stdout instead.\n"},
    {"set", (PyCFunction)set, METH_VARARGS | METH_KEYWORDS, "sets the value of a node or raises exception"},
    {"str2zwr", (PyCFunction)str2zwr, METH_VARARGS | METH_KEYWORDS,
     "returns the zwrite formatted (Bytes Object) version of the"
     " Bytes object provided as input."},
    {"subscript_next", (PyCFunction)subscript_next, METH_VARARGS | METH_KEYWORDS,
     "returns the name of the next subscript at "
     "the same level as the one given"},
    {"subscript_previous", (PyCFunction)subscript_previous, METH_VARARGS | METH_KEYWORDS,
     "returns the name of the previous "
     "subscript at the same level as the "
     "one given"},
    {"switch_ci_table", (PyCFunction)switch_ci_table, METH_VARARGS | METH_KEYWORDS,
     "switch to the call-in table referenced by the integer held in the passed handle\n"
     "and return the value of the previous handle"},
    {"tp", (PyCFunction)tp, METH_VARARGS | METH_KEYWORDS, "transaction"},

    {"zwr2str", (PyCFunction)zwr2str, METH_VARARGS | METH_KEYWORDS,
     "returns the Bytes Object from the zwrite formated Bytes "
     "object provided as input."},
    /* API Utility Functions */
    {NULL, NULL, 0, NULL} /* Sentinel */
};

/* The _yottadbmodule struct contains the information about the Python module per:
 * https://docs.python.org/3/c-api/module.html#initializing-c-modules
 *
 * A number of PyModuleDef struct members are unused by YDBPython, and so are
 * set to NULL to prevent compiler warnings.
 *
 * For a full list of PyModuleDef struct members, see:
 * https://docs.python.org/3/c-api/module.html#c.PyModuleDef
 */
static struct PyModuleDef _yottadbmodule = {PyModuleDef_HEAD_INIT,
					    "_yottadb",
					    "A module that provides basic access to the YottaDB's Simple API",
					    -1,
					    methods,
					    NULL,
					    NULL,
					    NULL,
					    NULL};

/* The initialization function for _yottadb.
 * This function must be named PyInit_{name of Module}
 */
PyMODINIT_FUNC PyInit__yottadb(void) {
	/* Initialize the module */
	PyObject *module = PyModule_Create(&_yottadbmodule);

	/* Defining Module 'Constants' */
	PyObject *module_dictionary = PyModule_GetDict(module);

	/* Expose constants defined in libyottadb.h */
	ADD_YDBCONSTANTS(module_dictionary)

	/* expose useful constants from libydberrors.h or libydberrors2.h */
	PyDict_SetItemString(module_dictionary, "YDB_ERR_TPTIMEOUT", Py_BuildValue("i", YDB_ERR_TPTIMEOUT));

	/* expose useful constants defined in _yottadb.h */
	PyDict_SetItemString(module_dictionary, "YDB_LOCK_MAX_NODES", Py_BuildValue("i", YDB_LOCK_MAX_NODES));

	/* Adding Exceptions */
	/* Step 1: create exception with PyErr_NewException.
		Arguments: (https://docs.python.org/3/c-api/exceptions.html#c.PyErr_NewException)
	 *              1. Name of the exception in 'module.classname' form
	 *              2. The Base class of the exception (NULL if base class is Pythons base Exception class
	 *              3. A dictionary of class variables and methods. (Usually NULL)
	 * Step 2: Add this Exception to the module using PyModule_AddObject
		(https://docs.python.org/3/c-api/module.html#c.PyModule_AddObject)
	 */
	YDBException = PyErr_NewException("_yottadb.YDBException", NULL, NULL);
	PyModule_AddObject(module, "YDBException", YDBException);

	YDBTPException = PyErr_NewException("_yottadb.YDBTPException", YDBException, NULL);
	PyModule_AddObject(module, "YDBTPException", YDBTPException);

	YDBPythonError = PyErr_NewException("_yottadb.YDBPythonError", YDBException, NULL);
	PyModule_AddObject(module, "YDBPythonError", YDBPythonError);

	YDBError = PyErr_NewException("_yottadb.YDBError", YDBException, NULL);
	PyModule_AddObject(module, "YDBError", YDBError);

	/* Below are custom exceptions to be raised when a YottaDB API function
	 * returns a status that must be propagated to the Python-level user, but
	 * is not, strictly speaking an error. These exceptions thus omit "Error"
	 * from their names and definitions. This list, with the addition of
	 * YDB_ERR_NODEEND, is derived from sr_unix/libyottadb.h in the YottaDB
	 * source code.
	 */
	YDBTPRollback = PyErr_NewException("_yottadb.YDBTPRollback", YDBTPException, NULL);
	PyModule_AddObject(module, "YDBTPRollback", YDBTPRollback);

	YDBTPRestart = PyErr_NewException("_yottadb.YDBTPRestart", YDBTPException, NULL);
	PyModule_AddObject(module, "YDBTPRestart", YDBTPRestart);

	YDBNotOk = PyErr_NewException("_yottadb.YDBNotOk", YDBException, NULL);
	PyModule_AddObject(module, "YDBNotOk", YDBNotOk);

	YDBLockTimeoutError = PyErr_NewException("_yottadb.YDBLockTimeoutError", YDBException, NULL);
	PyModule_AddObject(module, "YDBLockTimeoutError", YDBLockTimeoutError);

	YDBTPTimeoutError = PyErr_NewException("_yottadb.YDBTPTimeoutError", YDBException, NULL);
	PyModule_AddObject(module, "YDBTPTimeoutError", YDBTPTimeoutError);

	YDBDeferHandler = PyErr_NewException("_yottadb.YDBDeferHandler", YDBException, NULL);
	PyModule_AddObject(module, "YDBDeferHandler", YDBDeferHandler);

	YDBNodeEnd = PyErr_NewException("_yottadb.YDBNodeEnd", YDBException, NULL);
	PyModule_AddObject(module, "YDBNodeEnd", YDBNodeEnd);

	/* return the now fully initialized module */
	return module;
}
