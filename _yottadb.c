/****************************************************************
 *                                                              *
 * Copyright (c) 2019-2021 Peter Goss All rights reserved.      *
 *                                                              *
 * Copyright (c) 2019-2021 YottaDB LLC and/or its subsidiaries. *
 * All rights reserved.                                         *
 *                                                              *
 *  This source code contains the intellectual property         *
 *  of its copyright holder(s), and is made available           *
 *  under a license.  If you do not know the terms of           *
 *  the license, please stop and do not read further.           *
 *                                                              *
 ****************************************************************/

#include <assert.h>
#include <stdbool.h>
#include <libyottadb.h>
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "_yottadb.h"
#include "_yottadbexceptions.h"

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

	return_buffer_array = (ydb_buffer_t *)calloc(num, sizeof(ydb_buffer_t));
	for (i = 0; i < num; i++)
		YDB_MALLOC_BUFFER(&return_buffer_array[i], len);

	return return_buffer_array;
}

/* Conversion Utilities */

/* Routine to validate that the PyObject passed to it is indeed an array of
 * Python bytes objects.
 *
 * Parameters:
 *   sequence    - the Python object to check.
 */
static int validate_sequence_of_bytes(PyObject *sequence, int max_sequence_len, int max_bytes_len, char *error_message) {
	int	   ret = YDB_OK;
	Py_ssize_t i, len_seq, len_bytes;
	PyObject * item, *seq;

	/* validate sequence type */
	seq = PySequence_Fast(sequence, "argument must be iterable"); // New Reference
	if (!seq || !(PyTuple_Check(sequence) || PyList_Check(sequence))) {
		FORMAT_ERROR_MESSAGE(YDBPY_ERRMSG_NOT_LIST_OR_TUPLE);
		return YDBPY_INVALID_NOT_LIST_OR_TUPLE;
	}

	/* validate sequence length */
	len_seq = PySequence_Fast_GET_SIZE(seq);
	if (max_sequence_len < len_seq) {
		FORMAT_ERROR_MESSAGE(YDBPY_ERRMSG_SEQUENCE_TOO_LONG, len_seq, max_sequence_len);
		return YDBPY_INVALID_SEQUENCE_TOO_LONG;
	}

	/* validate sequence contents */
	for (i = 0; i < len_seq; i++) {
		item = PySequence_Fast_GET_ITEM(seq, i); // Borrowed Reference
		/* validate item type (bytes) */
		if (!PyBytes_Check(item)) {
			FORMAT_ERROR_MESSAGE(YDBPY_ERRMSG_ITEM_IN_SEQUENCE_NOT_BYTES, i);
			ret = YDBPY_INVALID_ITEM_IN_SEQUENCE_NOT_BYTES;
			break;
		}
		/* validate item length */
		len_bytes = PySequence_Fast_GET_SIZE(item);
		if (max_bytes_len < len_bytes) {
			FORMAT_ERROR_MESSAGE(YDBPY_ERRMSG_BYTES_TOO_LONG, len_bytes, max_bytes_len);
			ret = YDBPY_INVALID_BYTES_TOO_LONG;
			break;
		}
	}
	Py_DECREF(seq);
	return ret;
}

/* Routine to convert a sequence of Python bytes into a C array of
 * ydb_buffer_ts. Routine assumes sequence was already validated with
 * 'validate_sequence_of_bytes' function or the special case macros
 * VALIDATE_SUBSARRAY or VALIDATE_VARNAMES. The function creates a
 * copy of each Python bytes' data so the resulting array should be
 * freed by using the 'FREE_BUFFER_ARRAY' macro.
 *
 * Parameters:
 *    sequence    - a Python Object that is expected to be a Python Sequence containing Strings.
 */
bool convert_py_bytes_sequence_to_ydb_buffer_array(PyObject *sequence, int sequence_len, ydb_buffer_t *buffer_array) {
	bool	     done;
	Py_ssize_t   bytes_ssize;
	unsigned int bytes_len;
	char *	     bytes_c;
	PyObject *   bytes, *seq;

	seq = PySequence_Fast(sequence, "argument must be iterable"); // New Reference
	if (!seq) {
		PyErr_SetString(YDBPythonError, "Can't convert none sequence to buffer array.");
		return false;
	}

	for (int i = 0; i < sequence_len; i++) {
		bytes = PySequence_Fast_GET_ITEM(seq, i); // Borrowed Reference
		bytes_ssize = PyBytes_Size(bytes);
		bytes_len = Py_SAFE_DOWNCAST(bytes_ssize, Py_ssize_t, unsigned int);
		bytes_c = PyBytes_AsString(bytes);
		YDB_MALLOC_BUFFER(&buffer_array[i], bytes_len);
		YDB_COPY_BYTES_TO_BUFFER(bytes_c, bytes_len, &buffer_array[i], done);
		if (false == done) {
			FREE_BUFFER_ARRAY(&buffer_array, i);
			Py_DECREF(seq);
			PyErr_SetString(YDBPythonError, "failed to copy bytes object to buffer array");
			return false;
		}
	}

	Py_DECREF(seq);
	return true;
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

/* This function will take an already allocated array of YDBKey structures and load it with the
 * data contained in the PyObject arguments.
 *
 * Parameters:
 *    dest       - pointer to the YDBKey to fill.
 *    varname    - Python bytes object representing the varname
 *    subsarray  - array of Python bytes objects representing the array of subscripts
 *                   Note: Because this function calls `convert_py_bytes_sequence_to_ydb_buffer_array`
 *                          subsarray should be validated with the VALIDATE_SUBSARRAY macro.
 */
static bool load_YDBKey(YDBKey *dest, PyObject *varname, PyObject *subsarray) {
	bool	      copy_success, convert_success;
	Py_ssize_t    len_ssize, sequence_len_ssize;
	unsigned int  len;
	char *	      bytes_c;
	ydb_buffer_t *varname_y, *subsarray_y;

	len_ssize = PyBytes_Size(varname);
	len = Py_SAFE_DOWNCAST(len_ssize, Py_ssize_t, unsigned int);

	bytes_c = PyBytes_AsString(varname);

	varname_y = (ydb_buffer_t *)calloc(1, sizeof(ydb_buffer_t));
	YDB_MALLOC_BUFFER(varname_y, len);
	YDB_COPY_BYTES_TO_BUFFER(bytes_c, len, varname_y, copy_success);
	if (!copy_success) {
		YDB_FREE_BUFFER(varname_y);
		free(varname_y);
		PyErr_SetString(YDBPythonError, "failed to copy bytes object to buffer");
		return false;
	}

	dest->varname = varname_y;
	if (Py_None != subsarray) {
		sequence_len_ssize = PySequence_Length(subsarray);
		dest->subs_used = Py_SAFE_DOWNCAST(sequence_len_ssize, Py_ssize_t, unsigned int);
		subsarray_y = (ydb_buffer_t *)calloc(dest->subs_used, sizeof(ydb_buffer_t));
		convert_success = convert_py_bytes_sequence_to_ydb_buffer_array(subsarray, dest->subs_used, subsarray_y);
		if (convert_success) {
			dest->subsarray = subsarray_y;
		} else {
			YDB_FREE_BUFFER(varname_y);
			FREE_BUFFER_ARRAY(varname_y, dest->subs_used);
			PyErr_SetString(YDBPythonError, "failed to covert sequence to buffer array");
			return false;
		}
	} else {
		dest->subs_used = 0;
	}
	return true;
}

/* Routine to free a YDBKey structure.
 *
 * Parameters:
 *    key    - pointer to the YDBKey to free.
 */
static void free_YDBKey(YDBKey *key) {
	YDB_FREE_BUFFER((key->varname));
	FREE_BUFFER_ARRAY(key->subsarray, key->subs_used);
}

/* Routine to validate a sequence of Python sequences representing keys. (Used
 * only by lock().)
 * Validation rule:
 *      1) key_sequence must be a sequence
 *      2) each item in key_sequence must be a sequence
 *      3) each item must be a sequence of 1 or 2 sub-items.
 *      4) item[0] must be a bytes object.
 *      5) item[1] either does not exist, is None or a sequence
 *      6) if item[1] is a sequence then it must contain only bytes objects.
 *
 * Parameters:
 *    keys_sequence        - a Python object that is to be validated.
 *    max_len              - the number of keys that are allowed in the keys_sequence
 *    error_message        - a preallocated string for storing the reason for validation failure.
 */
static int validate_py_keys_sequence_bytes(PyObject *keys_sequence, int max_len, char *error_message) {
	int	   ret = YDB_OK;
	int	   num_chars = 0;
	Py_ssize_t i, len_keys, len_key_seq, len_varname;
	PyObject * key, *varname, *subsarray, *seq, *key_seq;
	char	   error_sub_reason[YDBPY_MAX_REASON];

	/* validate key sequence type */
	seq = PySequence_Fast(keys_sequence, "'keys' argument must be a Sequence"); // New Reference
	if (!(PyTuple_Check(keys_sequence) || PyList_Check(keys_sequence))) {
		num_chars = snprintf(error_message, YDBPY_MAX_REASON, YDBPY_ERRMSG_NOT_LIST_OR_TUPLE);
		assert((0 <= num_chars) && (YDBPY_MAX_REASON > num_chars));
		ret = YDBPY_INVALID_NOT_LIST_OR_TUPLE;
	}

	/* validate key sequence length */
	if (YDB_OK == ret) {
		len_keys = PySequence_Fast_GET_SIZE(seq);
		if (max_len < len_keys) {
			num_chars = snprintf(error_message, YDBPY_MAX_REASON, YDBPY_ERRMSG_SEQUENCE_TOO_LONG, len_keys, max_len);
			assert((0 <= num_chars) && (YDBPY_MAX_REASON > num_chars));
			ret = YDBPY_INVALID_SEQUENCE_TOO_LONG;
		}
	}
	/* validate each item/key in key sequence */
	if (YDB_OK == ret) {
		for (i = 0; i < len_keys; i++) {
			key = PySequence_Fast_GET_ITEM(seq, i); // Borrowed Reference
			key_seq = PySequence_Fast(key, "");	// New Reference
			len_key_seq = PySequence_Fast_GET_SIZE(key_seq);
			if (1 <= len_key_seq) {
				varname = PySequence_Fast_GET_ITEM(key_seq, 0); // Borrowed Reference
				len_varname = PySequence_Fast_GET_SIZE(varname);
			} else {
				varname = Py_None;
				len_varname = -1;
			}
			if (2 <= len_key_seq)
				subsarray = PySequence_Fast_GET_ITEM(key, 1); // Borrowed Reference
			else
				subsarray = Py_None;

			/* validate item/key type [list or tuple] */
			if (!key_seq || !(PyTuple_Check(key) || PyList_Check(key))) {
				num_chars
				    = snprintf(error_message, YDBPY_MAX_REASON, YDBPY_ERRMSG_KEY_IN_SEQUENCE_NOT_LIST_OR_TUPLE, i);
				assert((0 <= num_chars) && (YDBPY_MAX_REASON > num_chars));
				ret = YDBPY_INVALID_KEY_IN_SEQUENCE_NOT_LIST_OR_TUPLE;
			}
			/* validate item/key length [1 or 2] */
			else if ((1 != len_key_seq) && (2 != len_key_seq)) {
				num_chars
				    = snprintf(error_message, YDBPY_MAX_REASON, YDBPY_ERRMSG_KEY_IN_SEQUENCE_INCORECT_LENGTH, i);
				assert((0 <= num_chars) && (YDBPY_MAX_REASON > num_chars));
				ret = YDBPY_INVALID_KEY_IN_SEQUENCE_INCORECT_LENGTH;
			}
			/* validate item/key first element (varname) type */
			else if (!PyBytes_Check(varname)) {
				num_chars
				    = snprintf(error_message, YDBPY_MAX_REASON, YDBPY_ERRMSG_KEY_IN_SEQUENCE_VARNAME_NOT_BYTES, i);
				assert((0 <= num_chars) && (YDBPY_MAX_REASON > num_chars));
				ret = YDBPY_INVALID_KEY_IN_SEQUENCE_VARNAME_NOT_BYTES;
			}
			/* validate item/key first element (varname) length */
			else if (YDB_MAX_IDENT < len_varname) {
				num_chars = snprintf(error_message, YDBPY_MAX_REASON, YDBPY_ERRMSG_KEY_IN_SEQUENCE_VARNAME_TOO_LONG,
						     i, len_varname, YDB_MAX_IDENT);
				assert((0 <= num_chars) && (YDBPY_MAX_REASON > num_chars));
				ret = YDBPY_INVALID_KEY_IN_SEQUENCE_VARNAME_TOO_LONG;
			}
			/* validate item/key second element (subsarray) if it exists */
			else if (2 == len_key_seq) {
				if (Py_None != subsarray) {
					ret = validate_sequence_of_bytes(subsarray, YDB_MAX_SUBS, YDB_MAX_STR, error_sub_reason);
					if (YDB_OK != ret) {
						num_chars
						    = snprintf(error_message, YDBPY_MAX_REASON * 2,
							       YDBPY_ERRMSG_KEY_IN_SEQUENCE_SUBSARRAY_INVALID, i, error_sub_reason);
						assert((0 <= num_chars) && (YDBPY_MAX_REASON > num_chars));
					}
				}
			}

			Py_DECREF(key_seq);
			if (YDB_OK != ret)
				break;
		}
	}
	Py_DECREF(seq);
	return ret;
}

/* Takes an already validated (by 'validate_py_keys_sequence' above) PyObject sequence
 * that represents a series of keys loads that data into an already allocated array
 * of YDBKeys. (note: 'ret_keys' should later be freed by 'free_YDBKey_array' below)
 *
 * Parameters:
 *    sequence    - a Python object that has already been validated with 'validate_py_keys_sequence' or equivalent.
 */
static bool load_YDBKeys_from_key_sequence(PyObject *sequence, YDBKey *ret_keys) {
	bool	   success = true;
	Py_ssize_t i, len_keys;
	PyObject * key, *varname, *subsarray, *seq, *key_seq;

	seq = PySequence_Fast(sequence, "argument must be iterable"); // New Reference
	len_keys = PySequence_Fast_GET_SIZE(seq);

	for (i = 0; i < len_keys; i++) {
		key = PySequence_Fast_GET_ITEM(seq, i);			     // Borrowed Reference
		key_seq = PySequence_Fast(key, "argument must be iterable"); // New Reference
		varname = PySequence_Fast_GET_ITEM(key_seq, 0);		     // Borrowed Reference
		subsarray = Py_None;

		if (2 == PySequence_Fast_GET_SIZE(key_seq))
			subsarray = PySequence_Fast_GET_ITEM(key_seq, 1); // Borrowed Reference
		success = load_YDBKey(&ret_keys[i], varname, subsarray);
		Py_DECREF(key_seq);
		if (!success)
			break;
	}
	Py_DECREF(seq);
	return success;
}

/* Routine to free an array of YDBKeys as returned by above
 * 'load_YDBKeys_from_key_sequence'.
 *
 * Parameters:
 *    keysarray    - the array that is to be freed.
 *    len          - the number of elements in keysarray.
 */
static void free_YDBKey_array(YDBKey *keysarray, int len) {
	int i;
	if (NULL != keysarray) {
		for (i = 0; i < len; i++)
			if (NULL != &keysarray[i])
				free_YDBKey(&keysarray[i]);
		free(keysarray);
	}
}

/* Routine to help raise a YDBError. The caller still needs to return NULL for
 * the Exception to be raised. This routine will check if the message has been
 * set in the error_string_buffer and look it up if not.
 *
 * Parameters:
 *    status                 - the error code that is returned by the wrapped ydb_ function.
 *    error_string_buffer    - a ydb_buffer_t that may or may not contain the error message.
 */
static void raise_YDBError(int status, ydb_buffer_t *error_string_buffer, int tp_token) {
	ydb_buffer_t ignored_buffer;
	PyObject *   message;
	int	     num_chars = 0;
	char	     full_error_message[YDB_MAX_ERRORMSG];
	char *	     error_status, *api, *error_name, *error_message;
	char *	     next_field = NULL;
	const char * delim = ",";

	if (0 == error_string_buffer->len_used) {
		YDB_MALLOC_BUFFER(&ignored_buffer, YDB_MAX_ERRORMSG);
		ydb_message_t(tp_token, &ignored_buffer, status, error_string_buffer);
		YDB_FREE_BUFFER(&ignored_buffer);
	}

	if (0 != error_string_buffer->len_used) {
		error_string_buffer->buf_addr[error_string_buffer->len_used] = '\0';
		/* normal error message format */
		error_status = strtok_r(error_string_buffer->buf_addr, delim, &next_field);
		api = strtok_r(NULL, delim, &next_field);
		error_name = strtok_r(NULL, delim, &next_field);
		error_message = strtok_r(NULL, delim, &next_field);
		if (NULL == error_message) {
			/* alternate error message case */
			error_name = (NULL == error_status) ? error_status : "UNKNOWN";
			error_message = (NULL == api) ? api : "";
		}
	} else if (YDB_TP_ROLLBACK == status) {
		error_name = "%YDB-TP-ROLLBACK";
		error_message = " Transaction callback function returned YDB_TP_ROLLBACK.";
	} else {
		error_name = "UNKNOWN";
		error_message = "";
	}

	num_chars = snprintf(full_error_message, YDB_MAX_ERRORMSG, "%s (%d):%s", error_name, status, error_message);
	assert((0 <= num_chars) && (YDB_MAX_ERRORMSG > num_chars));
	message = Py_BuildValue("s", full_error_message); // New Reference

	RAISE_SPECIFIC_ERROR(status, message);
	Py_DECREF(message);
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
static void raise_ValidationError(int status, char *message) {
	if ((YDBPY_TYPE_ERROR_MAX >= status) && (YDBPY_TYPE_ERROR_MIN <= status))
		PyErr_SetString(PyExc_TypeError, message);
	else if ((YDBPY_VALUE_ERROR_MAX >= status) && (YDBPY_VALUE_ERROR_MIN <= status))
		PyErr_SetString(PyExc_ValueError, message);
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

/* Wrapper for ydb_data_s and ydb_data_st. */
static PyObject *data(PyObject *self, PyObject *args, PyObject *kwds) {
	bool	      return_NULL = false;
	char *	      varname_py;
	int	      subs_used, status;
	Py_ssize_t    varname_py_len;
	unsigned int  varname_ydb_len, ret_value_ydb;
	uint64_t      tp_token;
	PyObject *    subsarray_py, *ret_value_py;
	ydb_buffer_t  error_string_buffer, varname_ydb;
	ydb_buffer_t *subsarray_ydb;

	/* Default values for optional arguments passed from Python */
	subsarray_py = Py_None;
	tp_token = YDB_NOTTP;

	/* parse and validate */
	static char *kwlist[] = {"varname", "subsarray", "tp_token", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "y#|OK", kwlist, &varname_py, &varname_py_len, &subsarray_py, &tp_token))
		return NULL;

	/* validate */
	VALIDATE_AND_CONVERT_BYTES_LEN(varname_py_len, varname_ydb_len, YDB_MAX_IDENT, YDBPY_INVALID_VARNAME_TOO_LONG,
				       YDBPY_ERRMSG_VARNAME_TOO_LONG);
	VALIDATE_SUBSARRAY(subsarray_py);

	/* Setup for Call */
	POPULATE_NEW_BUFFER(varname_py, varname_ydb, varname_ydb_len, "data()", return_NULL);
	POPULATE_SUBS_USED_AND_SUBSARRAY(subsarray_py, subs_used, subsarray_ydb, return_NULL);
	YDB_MALLOC_BUFFER(&error_string_buffer, YDB_MAX_ERRORMSG);
	if (!return_NULL) {
		/* Call the wrapped function */
		status = ydb_data_st(tp_token, &error_string_buffer, &varname_ydb, subs_used, subsarray_ydb, &ret_value_ydb);
		/* check status for Errors and Raise Exception */
		if (YDB_OK != status) {
			raise_YDBError(status, &error_string_buffer, tp_token);
			return_NULL = true;
		}

		/* Create Python object to return */
		if (!return_NULL)
			ret_value_py = Py_BuildValue("I", ret_value_ydb); // New Reference
	}

	/* free allocated memory */
	YDB_FREE_BUFFER(&varname_ydb);
	FREE_BUFFER_ARRAY(subsarray_ydb, subs_used);
	YDB_FREE_BUFFER(&error_string_buffer);

	if (return_NULL)
		return NULL;
	else
		return ret_value_py;
}

/* Wrapper for ydb_delete_s() and ydb_delete_st() */
static PyObject *delete_wrapper(PyObject *self, PyObject *args, PyObject *kwds) {
	bool	      return_NULL = false;
	int	      deltype, status, subs_used;
	Py_ssize_t    varname_py_len;
	unsigned int  varname_ydb_len;
	char *	      varname_py;
	uint64_t      tp_token;
	PyObject *    subsarray_py;
	ydb_buffer_t  error_string_buffer, varname_ydb;
	ydb_buffer_t *subsarray_ydb;

	/* Default values for optional arguments passed from Python */
	subsarray_py = Py_None;
	tp_token = YDB_NOTTP;
	deltype = YDB_DEL_NODE;

	/* parse and validate */
	static char *kwlist[] = {"varname", "subsarray", "delete_type", "tp_token", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "y#|OiK", kwlist, &varname_py, &varname_py_len, &subsarray_py, &deltype,
					 &tp_token)) {
		return NULL;
	}

	/* validate varname */
	VALIDATE_AND_CONVERT_BYTES_LEN(varname_py_len, varname_ydb_len, YDB_MAX_IDENT, YDBPY_INVALID_VARNAME_TOO_LONG,
				       YDBPY_ERRMSG_VARNAME_TOO_LONG);
	VALIDATE_SUBSARRAY(subsarray_py);

	/* Setup for Call */
	POPULATE_NEW_BUFFER(varname_py, varname_ydb, varname_ydb_len, "delete_wrapper()", return_NULL);
	POPULATE_SUBS_USED_AND_SUBSARRAY(subsarray_py, subs_used, subsarray_ydb, return_NULL);
	YDB_MALLOC_BUFFER(&error_string_buffer, YDB_MAX_ERRORMSG);
	if (!return_NULL) {
		/* Call the wrapped function */
		status = ydb_delete_st(tp_token, &error_string_buffer, &varname_ydb, subs_used, subsarray_ydb, deltype);
		/* check status for Errors and Raise Exception */
		if (YDB_OK != status) {
			raise_YDBError(status, &error_string_buffer, tp_token);
			return_NULL = true;
		}
	}

	/* free allocated memory */
	YDB_FREE_BUFFER(&varname_ydb);
	FREE_BUFFER_ARRAY(subsarray_ydb, subs_used);
	YDB_FREE_BUFFER(&error_string_buffer)

	if (return_NULL) {
		return NULL;
	} else {
		Py_INCREF(Py_None);
		return Py_None;
	}
}

/* Wrapper for ydb_delete_excl_s() and ydb_delete_excl_st() */
static PyObject *delete_excel(PyObject *self, PyObject *args, PyObject *kwds) {
	bool	     return_NULL = false;
	bool	     success;
	int	     namecount, status;
	uint64_t     tp_token;
	PyObject *   varnames_py;
	ydb_buffer_t error_string_buffer;

	/* Default values for optional arguments passed from Python */
	varnames_py = Py_None;
	tp_token = YDB_NOTTP;

	/* parse and validate */
	static char *kwlist[] = {"varnames", "tp_token", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "|OK", kwlist, &varnames_py, &tp_token))
		return NULL;
	VALIDATE_VARNAMES(varnames_py);

	/* Setup for Call */
	YDB_MALLOC_BUFFER(&error_string_buffer, YDB_MAX_ERRORMSG);
	namecount = 0;
	if (Py_None != varnames_py)
		namecount = PySequence_Length(varnames_py);
	ydb_buffer_t varnames_ydb[namecount];
	if (0 < namecount) {
		success = convert_py_bytes_sequence_to_ydb_buffer_array(varnames_py, namecount, varnames_ydb);
		if (!success)
			return NULL;
	}

	status = ydb_delete_excl_st(tp_token, &error_string_buffer, namecount, varnames_ydb);
	/* check status for Errors and Raise Exception */
	if (YDB_OK != status) {
		raise_YDBError(status, &error_string_buffer, tp_token);
		return_NULL = true;
	}

	/* free allocated memory */
	FREE_BUFFER_ARRAY(varnames_ydb, namecount);
	YDB_FREE_BUFFER(&error_string_buffer);

	if (return_NULL) {
		return NULL;
	} else {
		Py_INCREF(Py_None);
		return Py_None;
	}
}

/* Wrapper for ydb_get_s() and ydb_get_st() */
static PyObject *get(PyObject *self, PyObject *args, PyObject *kwds) {
	bool	      return_NULL = false;
	int	      subs_used, status;
	unsigned int  varname_ydb_len;
	Py_ssize_t    varname_py_len;
	char *	      varname_py;
	uint64_t      tp_token;
	PyObject *    subsarray_py, *ret_value_py;
	ydb_buffer_t  varname_ydb, error_string_buffer, ret_value_ydb;
	ydb_buffer_t *subsarray_ydb;

	/* Default values for optional arguments passed from Python */
	subsarray_py = Py_None;
	tp_token = YDB_NOTTP;

	/* parse and validate */
	static char *kwlist[] = {"varname", "subsarray", "tp_token", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "y#|OK", kwlist, &varname_py, &varname_py_len, &subsarray_py, &tp_token))
		return NULL;
	/* validate varname */
	VALIDATE_AND_CONVERT_BYTES_LEN(varname_py_len, varname_ydb_len, YDB_MAX_IDENT, YDBPY_INVALID_VARNAME_TOO_LONG,
				       YDBPY_ERRMSG_VARNAME_TOO_LONG);
	VALIDATE_SUBSARRAY(subsarray_py);

	/* Setup for Call */
	POPULATE_NEW_BUFFER(varname_py, varname_ydb, varname_ydb_len, "get()", return_NULL);
	POPULATE_SUBS_USED_AND_SUBSARRAY(subsarray_py, subs_used, subsarray_ydb, return_NULL);
	YDB_MALLOC_BUFFER(&error_string_buffer, YDB_MAX_ERRORMSG);
	YDB_MALLOC_BUFFER(&ret_value_ydb, YDBPY_DEFAULT_VALUE_LEN);
	if (!return_NULL) {
		/* Call the wrapped function */
		status = ydb_get_st(tp_token, &error_string_buffer, &varname_ydb, subs_used, subsarray_ydb, &ret_value_ydb);

		/* Check to see if length of string was longer than YDBPY_DEFAULT_VALUE_LEN. If so, try again
		 * with proper length */
		if (YDB_ERR_INVSTRLEN == status) {
			FIX_BUFFER_LENGTH(ret_value_ydb);
			/* Call the wrapped function */
			status = ydb_get_st(tp_token, &error_string_buffer, &varname_ydb, subs_used, subsarray_ydb, &ret_value_ydb);
			assert(YDB_ERR_INVSTRLEN != status);
		}
		/* check status for Errors and Raise Exception */
		if (YDB_OK != status) {
			raise_YDBError(status, &error_string_buffer, tp_token);
			return_NULL = true;
		}
		/* Create Python object to return */
		if (!return_NULL) {
			/* New Reference */
			ret_value_py = Py_BuildValue("y#", ret_value_ydb.buf_addr, (Py_ssize_t)ret_value_ydb.len_used);
		}
	}

	/* free allocated memory */
	YDB_FREE_BUFFER(&varname_ydb);
	FREE_BUFFER_ARRAY(subsarray_ydb, subs_used);
	YDB_FREE_BUFFER(&error_string_buffer);
	YDB_FREE_BUFFER(&ret_value_ydb);

	if (return_NULL)
		return NULL;
	else
		return ret_value_py;
}

/* Wrapper for ydb_incr_s() and ydb_incr_st() */
static PyObject *incr(PyObject *self, PyObject *args, PyObject *kwds) {
	bool	      return_NULL = false;
	int	      status, subs_used;
	Py_ssize_t    varname_py_len, increment_py_len;
	unsigned int  varname_ydb_len, increment_ydb_len;
	uint64_t      tp_token;
	char *	      varname_py, *increment_py;
	PyObject *    subsarray_py, *ret_value_py;
	ydb_buffer_t  increment_ydb, error_string_buffer, ret_value_ydb, varname_ydb;
	ydb_buffer_t *subsarray_ydb;

	/* Default values for optional arguments passed from Python */
	subsarray_py = Py_None;
	tp_token = YDB_NOTTP;
	increment_py = "1";
	increment_py_len = 1;

	/* parse and validate */
	static char *kwlist[] = {"varname", "subsarray", "increment", "tp_token", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "y#|Oy#K", kwlist, &varname_py, &varname_py_len, &subsarray_py, &increment_py,
					 &increment_py_len, &tp_token)) {
		return NULL;
	}

	/* validate varname */
	VALIDATE_AND_CONVERT_BYTES_LEN(varname_py_len, varname_ydb_len, YDB_MAX_IDENT, YDBPY_INVALID_VARNAME_TOO_LONG,
				       YDBPY_ERRMSG_VARNAME_TOO_LONG);
	VALIDATE_SUBSARRAY(subsarray_py);
	VALIDATE_AND_CONVERT_BYTES_LEN(increment_py_len, increment_ydb_len, YDB_MAX_STR, YDBPY_INVALID_BYTES_TOO_LONG,
				       YDBPY_ERRMSG_BYTES_TOO_LONG);

	/* Setup for Call */
	POPULATE_NEW_BUFFER(varname_py, varname_ydb, varname_ydb_len, "incr() for varname", return_NULL);
	POPULATE_SUBS_USED_AND_SUBSARRAY(subsarray_py, subs_used, subsarray_ydb, return_NULL);
	POPULATE_NEW_BUFFER(increment_py, increment_ydb, increment_ydb_len, "incr() for increment", return_NULL);
	YDB_MALLOC_BUFFER(&error_string_buffer, YDB_MAX_ERRORMSG);
	YDB_MALLOC_BUFFER(&ret_value_ydb, MAX_CONONICAL_NUMBER_STRING_MAX);
	if (!return_NULL) {
		/* Call the wrapped function */
		status = ydb_incr_st(tp_token, &error_string_buffer, &varname_ydb, subs_used, subsarray_ydb, &increment_ydb,
				     &ret_value_ydb);
		/* check status for Errors and Raise Exception */
		if (YDB_OK != status) {
			raise_YDBError(status, &error_string_buffer, tp_token);
			return_NULL = true;
		}

		/* Create Python object to return */
		if (!return_NULL) {
			/* New Reference */
			ret_value_py = Py_BuildValue("y#", ret_value_ydb.buf_addr, (Py_ssize_t)ret_value_ydb.len_used);
		}
	}
	/* free allocated memory */
	YDB_FREE_BUFFER(&varname_ydb);
	FREE_BUFFER_ARRAY(subsarray_ydb, subs_used);
	YDB_FREE_BUFFER(&increment_ydb);
	YDB_FREE_BUFFER(&error_string_buffer);
	YDB_FREE_BUFFER(&ret_value_ydb);

	if (return_NULL)
		return NULL;
	else
		return ret_value_py;
}

/* Wrapper for ydb_lock_s() and ydb_lock_st() */
static PyObject *lock(PyObject *self, PyObject *args, PyObject *kwds) {
	bool		   return_NULL = false;
	bool		   success = true;
	int		   len_keys, number_of_arguments, num_chars, first, status;
	uint64_t	   tp_token;
	unsigned long long timeout_nsec;
	PyObject *	   keys_py;
	ydb_buffer_t	   error_string_buffer;
	YDBKey *	   keys_ydb = NULL;
	int		   validation_status;
	char		   validation_error_reason[YDBPY_MAX_REASON * 2];
	char		   validation_error_message[YDBPY_MAX_ERRORMSG];

	/* Default values for optional arguments passed from Python */
	timeout_nsec = 0;
	tp_token = YDB_NOTTP;
	keys_py = Py_None;

	/* parse and validate */
	static char *kwlist[] = {"keys", "timeout_nsec", "tp_token", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "|OKK", kwlist, &keys_py, &timeout_nsec, &tp_token))
		return NULL;
	if (Py_None != keys_py) {
		validation_status = validate_py_keys_sequence_bytes(keys_py, YDB_LOCK_MAX_KEYS, validation_error_reason);
		if (YDB_OK != validation_status) {
			num_chars = snprintf(validation_error_message, YDBPY_MAX_ERRORMSG, YDBPY_ERRMSG_KEYS_INVALID,
					     validation_error_reason);
			assert((0 <= num_chars) && (YDB_MAX_ERRORMSG > num_chars));
			raise_ValidationError(validation_status, validation_error_message);
			return NULL;
		}
	}
	if (Py_None == keys_py)
		len_keys = 0;
	else
		len_keys = Py_SAFE_DOWNCAST(PySequence_Length(keys_py), Py_ssize_t, int);

	/* Setup for Call */
	YDB_MALLOC_BUFFER(&error_string_buffer, YDB_MAX_ERRORMSG);
	if (Py_None != keys_py) {
		keys_ydb = (YDBKey *)calloc(len_keys, sizeof(YDBKey));
		success = load_YDBKeys_from_key_sequence(keys_py, keys_ydb);
		if (!success)
			return_NULL = true;
	}
	if (!return_NULL) {
		number_of_arguments = YDB_LOCK_ST_INIT_ARG_NUMS + (len_keys * YDB_LOCK_ST_NUM_ARGS_PER_KEY);
		void *arg_values[number_of_arguments + 1];
		arg_values[0] = (void *)(uintptr_t)number_of_arguments;
		arg_values[1] = (void *)tp_token;
		arg_values[2] = (void *)&error_string_buffer;
		arg_values[3] = (void *)timeout_nsec;
		arg_values[4] = (void *)(uintptr_t)len_keys;
		for (int i = 0; i < len_keys; i++) {
			first = YDB_LOCK_ST_INIT_ARG_NUMS + 1 + YDB_LOCK_ST_NUM_ARGS_PER_KEY * i;
			arg_values[first] = (void *)keys_ydb[i].varname;
			arg_values[first + 1] = (void *)(uintptr_t)keys_ydb[i].subs_used;
			arg_values[first + 2] = (void *)keys_ydb[i].subsarray;
		}

		status = ydb_call_variadic_plist_func((ydb_vplist_func)&ydb_lock_st, (uintptr_t)arg_values);
		/* check for errors */
		if (YDB_LOCK_TIMEOUT == status) {
			PyErr_SetString(YDBTimeoutError, "Not able to acquire all requested locks in the specified time.");
			return_NULL = true;
		} else if (YDB_OK != status) {
			raise_YDBError(status, &error_string_buffer, tp_token);
			return_NULL = true;
			return NULL;
		}
	}

	/* free allocated memory */
	YDB_FREE_BUFFER(&error_string_buffer);
	free_YDBKey_array(keys_ydb, len_keys);

	if (return_NULL) {
		return NULL;
	} else {
		Py_INCREF(Py_None);
		return Py_None;
	}
}

/* Wrapper for ydb_lock_decr_s() and ydb_lock_decr_st() */
static PyObject *lock_decr(PyObject *self, PyObject *args, PyObject *kwds) {
	bool	      return_NULL = false;
	int	      status, subs_used;
	Py_ssize_t    varname_py_len;
	unsigned int  varname_ydb_len;
	char *	      varname_py;
	uint64_t      tp_token;
	PyObject *    subsarray_py;
	ydb_buffer_t  error_string_buffer, varname_ydb;
	ydb_buffer_t *subsarray_ydb;

	/* Default values for optional arguments passed from Python */
	subsarray_py = Py_None;
	tp_token = YDB_NOTTP;

	/* parse and validate */
	static char *kwlist[] = {"varname", "subsarray", "tp_token", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "y#|OK", kwlist, &varname_py, &varname_py_len, &subsarray_py, &tp_token))
		return NULL;
	/* validate varname */
	VALIDATE_AND_CONVERT_BYTES_LEN(varname_py_len, varname_ydb_len, YDB_MAX_IDENT, YDBPY_INVALID_VARNAME_TOO_LONG,
				       YDBPY_ERRMSG_VARNAME_TOO_LONG);
	VALIDATE_SUBSARRAY(subsarray_py);

	/* Setup for Call */
	POPULATE_NEW_BUFFER(varname_py, varname_ydb, varname_ydb_len, "lock_decr()", return_NULL);
	POPULATE_SUBS_USED_AND_SUBSARRAY(subsarray_py, subs_used, subsarray_ydb, return_NULL);
	YDB_MALLOC_BUFFER(&error_string_buffer, YDB_MAX_ERRORMSG);
	if (!return_NULL) {
		/* Call the wrapped function */
		status = ydb_lock_decr_st(tp_token, &error_string_buffer, &varname_ydb, subs_used, subsarray_ydb);
		/* check status for Errors and Raise Exception */
		if (YDB_OK != status) {
			raise_YDBError(status, &error_string_buffer, tp_token);
			return_NULL = true;
		}
	}

	/* free allocated memory */
	YDB_FREE_BUFFER(&varname_ydb);
	FREE_BUFFER_ARRAY(subsarray_ydb, subs_used);
	YDB_FREE_BUFFER(&error_string_buffer);

	if (return_NULL) {
		return NULL;
	} else {
		Py_INCREF(Py_None);
		return Py_None;
	}
}

/* Wrapper for ydb_lock_incr_s() and ydb_lock_incr_st() */
static PyObject *lock_incr(PyObject *self, PyObject *args, PyObject *kwds) {
	bool		   return_NULL = false;
	int		   status, subs_used;
	Py_ssize_t	   varname_py_len;
	unsigned int	   varname_ydb_len;
	char *		   varname_py;
	uint64_t	   tp_token;
	unsigned long long timeout_nsec;
	PyObject *	   subsarray_py;
	ydb_buffer_t	   error_string_buffer, varname_ydb;
	ydb_buffer_t *	   subsarray_ydb;

	/* Default values for optional arguments passed from Python */
	subsarray_py = Py_None;
	timeout_nsec = 0;
	tp_token = YDB_NOTTP;

	/* parse and validate */
	static char *kwlist[] = {"varname", "subsarray", "timeout_nsec", "tp_token", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "y#|OLK", kwlist, &varname_py, &varname_py_len, &subsarray_py, &timeout_nsec,
					 &tp_token)) {
		return NULL;
	}
	/* validate varname */
	VALIDATE_AND_CONVERT_BYTES_LEN(varname_py_len, varname_ydb_len, YDB_MAX_IDENT, YDBPY_INVALID_VARNAME_TOO_LONG,
				       YDBPY_ERRMSG_VARNAME_TOO_LONG);
	VALIDATE_SUBSARRAY(subsarray_py);

	/* Setup for Call */
	POPULATE_NEW_BUFFER(varname_py, varname_ydb, varname_ydb_len, "lock_incr()", return_NULL);
	POPULATE_SUBS_USED_AND_SUBSARRAY(subsarray_py, subs_used, subsarray_ydb, return_NULL);
	YDB_MALLOC_BUFFER(&error_string_buffer, YDB_MAX_ERRORMSG);
	if (!return_NULL) {
		/* Call the wrapped function */
		status = ydb_lock_incr_st(tp_token, &error_string_buffer, timeout_nsec, &varname_ydb, subs_used, subsarray_ydb);
		/* check status for Errors and Raise Exception */
		if (YDB_LOCK_TIMEOUT == status) {
			PyErr_SetString(YDBTimeoutError, "Not able to acquire all requested locks in the specified time.");
			return_NULL = true;
		} else if (YDB_OK != status) {
			raise_YDBError(status, &error_string_buffer, tp_token);
			return_NULL = true;
		}
	}

	/* free allocated memory */
	YDB_FREE_BUFFER(&varname_ydb);
	FREE_BUFFER_ARRAY(subsarray_ydb, subs_used);
	YDB_FREE_BUFFER(&error_string_buffer);

	if (return_NULL) {
		return NULL;
	} else {
		Py_INCREF(Py_None);
		return Py_None;
	}
}

/* Wrapper for ydb_node_next_s() and ydb_node_next_st() */
static PyObject *node_next(PyObject *self, PyObject *args, PyObject *kwds) {
	bool	      return_NULL = false;
	int	      max_subscript_string, ret_subsarray_num_elements, ret_subs_used, status, subs_used;
	Py_ssize_t    varname_py_len;
	unsigned int  varname_ydb_len;
	char *	      varname_py;
	uint64_t      tp_token;
	PyObject *    subsarray_py, *ret_subsarray_py;
	ydb_buffer_t  error_string_buffer, varname_ydb;
	ydb_buffer_t *ret_subsarray_ydb, *subsarray_ydb;

	/* Default values for optional arguments passed from Python */
	subsarray_py = Py_None;
	tp_token = YDB_NOTTP;

	/* parse and validate */
	static char *kwlist[] = {"varname", "subsarray", "tp_token", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "y#|OK", kwlist, &varname_py, &varname_py_len, &subsarray_py, &tp_token))
		return NULL;

	/* validate varname */
	VALIDATE_AND_CONVERT_BYTES_LEN(varname_py_len, varname_ydb_len, YDB_MAX_IDENT, YDBPY_INVALID_VARNAME_TOO_LONG,
				       YDBPY_ERRMSG_VARNAME_TOO_LONG);
	VALIDATE_SUBSARRAY(subsarray_py);

	/* Setup for Call */
	POPULATE_NEW_BUFFER(varname_py, varname_ydb, varname_ydb_len, "node_next()", return_NULL);
	POPULATE_SUBS_USED_AND_SUBSARRAY(subsarray_py, subs_used, subsarray_ydb, return_NULL);
	YDB_MALLOC_BUFFER(&error_string_buffer, YDB_MAX_ERRORMSG);
	max_subscript_string = YDBPY_DEFAULT_SUBSCRIPT_LEN;
	ret_subsarray_num_elements = YDBPY_DEFAULT_SUBSCRIPT_COUNT;
	ret_subs_used = ret_subsarray_num_elements;
	ret_subsarray_ydb = create_empty_buffer_array(ret_subs_used, max_subscript_string);
	if (!return_NULL) {
		/* Call the wrapped function */
		status = ydb_node_next_st(tp_token, &error_string_buffer, &varname_ydb, subs_used, subsarray_ydb, &ret_subs_used,
					  ret_subsarray_ydb);

		/* If not enough buffers in ret_subsarray */
		if (YDB_ERR_INSUFFSUBS == status) {
			FREE_BUFFER_ARRAY(ret_subsarray_ydb, ret_subsarray_num_elements);
			ret_subsarray_num_elements = ret_subs_used;
			ret_subsarray_ydb = create_empty_buffer_array(ret_subs_used, max_subscript_string);
			/* recall the wrapped function */
			status = ydb_node_next_st(tp_token, &error_string_buffer, &varname_ydb, subs_used, subsarray_ydb,
						  &ret_subs_used, ret_subsarray_ydb);
		}

		/* if a buffer is not long enough */
		while (YDB_ERR_INVSTRLEN == status) {
			FIX_BUFFER_LENGTH(ret_subsarray_ydb[ret_subs_used]);
			ret_subs_used = ret_subsarray_num_elements;
			/* recall the wrapped function */
			status = ydb_node_next_st(tp_token, &error_string_buffer, &varname_ydb, subs_used, subsarray_ydb,
						  &ret_subs_used, ret_subsarray_ydb);
			assert(YDB_ERR_INVSTRLEN != status);
		}
		/* check status for Errors and Raise Exception */
		if (YDB_OK != status) {
			raise_YDBError(status, &error_string_buffer, tp_token);
			return_NULL = true;
		}
		/* Create Python object to return */
		if (!return_NULL) {
			/* New Reference */
			ret_subsarray_py = convert_ydb_buffer_array_to_py_tuple(ret_subsarray_ydb, ret_subs_used);
		}
	}
	/* free allocated memory */
	YDB_FREE_BUFFER(&varname_ydb);
	FREE_BUFFER_ARRAY(subsarray_ydb, subs_used);
	YDB_FREE_BUFFER(&error_string_buffer);
	FREE_BUFFER_ARRAY(ret_subsarray_ydb, ret_subsarray_num_elements);

	if (return_NULL)
		return NULL;
	else
		return ret_subsarray_py;
}

/* Wrapper for ydb_node_previous_s() and ydb_node_previous_st() */
static PyObject *node_previous(PyObject *self, PyObject *args, PyObject *kwds) {
	bool	      return_NULL = false;
	int	      max_subscript_string, ret_subsarray_num_elements, ret_subs_used, status, subs_used;
	Py_ssize_t    varname_py_len;
	unsigned int  varname_ydb_len;
	char *	      varname_py;
	uint64_t      tp_token;
	PyObject *    subsarray_py, *ret_subsarray_py;
	ydb_buffer_t  error_string_buffer, varname_ydb;
	ydb_buffer_t *ret_subsarray_ydb, *subsarray_ydb;

	/* Default values for optional arguments passed from Python */
	subsarray_py = Py_None;
	tp_token = YDB_NOTTP;

	/* parse and validate */
	static char *kwlist[] = {"varname", "subsarray", "tp_token", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "y#|OK", kwlist, &varname_py, &varname_py_len, &subsarray_py, &tp_token))
		return NULL;

	/* validate varname */
	VALIDATE_AND_CONVERT_BYTES_LEN(varname_py_len, varname_ydb_len, YDB_MAX_IDENT, YDBPY_INVALID_VARNAME_TOO_LONG,
				       YDBPY_ERRMSG_VARNAME_TOO_LONG);
	VALIDATE_SUBSARRAY(subsarray_py);

	/* Setup for Call */
	POPULATE_NEW_BUFFER(varname_py, varname_ydb, varname_ydb_len, "node_previous()", return_NULL);
	POPULATE_SUBS_USED_AND_SUBSARRAY(subsarray_py, subs_used, subsarray_ydb, return_NULL);
	YDB_MALLOC_BUFFER(&error_string_buffer, YDB_MAX_ERRORMSG);

	max_subscript_string = YDBPY_DEFAULT_SUBSCRIPT_LEN;
	ret_subsarray_num_elements = YDBPY_DEFAULT_SUBSCRIPT_COUNT;
	ret_subs_used = ret_subsarray_num_elements;
	ret_subsarray_ydb = create_empty_buffer_array(ret_subs_used, max_subscript_string);
	if (!return_NULL) {
		/* Call the wrapped function */
		status = ydb_node_previous_st(tp_token, &error_string_buffer, &varname_ydb, subs_used, subsarray_ydb,
					      &ret_subs_used, ret_subsarray_ydb);

		/* if a buffer is not long enough */
		while (YDB_ERR_INVSTRLEN == status) {
			FIX_BUFFER_LENGTH(ret_subsarray_ydb[ret_subs_used]);
			ret_subs_used = ret_subsarray_num_elements;
			/* recall the wrapped function */
			status = ydb_node_previous_st(tp_token, &error_string_buffer, &varname_ydb, subs_used, subsarray_ydb,
						      &ret_subs_used, ret_subsarray_ydb);
			assert(YDB_ERR_INVSTRLEN != status);
		}
		/* check status for Errors and Raise Exception */
		if (YDB_OK != status) {
			raise_YDBError(status, &error_string_buffer, tp_token);
			return_NULL = true;
		}

		/* Create Python object to return */
		if (!return_NULL) {
			/* New Reference */
			ret_subsarray_py = convert_ydb_buffer_array_to_py_tuple(ret_subsarray_ydb, ret_subs_used);
		}
	}

	/* free allocated memory */
	YDB_FREE_BUFFER(&varname_ydb);
	FREE_BUFFER_ARRAY(subsarray_ydb, subs_used);
	YDB_FREE_BUFFER(&error_string_buffer);
	FREE_BUFFER_ARRAY(ret_subsarray_ydb, ret_subsarray_num_elements);

	if (return_NULL)
		return NULL;
	else
		return ret_subsarray_py;
}

/* Wrapper for ydb_set_s() and ydb_set_st() */
static PyObject *set(PyObject *self, PyObject *args, PyObject *kwds) {
	bool	      return_NULL = false;
	int	      status, subs_used;
	Py_ssize_t    varname_py_len, value_py_len;
	unsigned int  varname_ydb_len, value_ydb_len;
	uint64_t      tp_token;
	char *	      varname_py, *value_py;
	PyObject *    subsarray_py;
	ydb_buffer_t  error_string_buffer, value_ydb, varname_ydb;
	ydb_buffer_t *subsarray_ydb;

	/* Default values for optional arguments passed from Python */
	subsarray_py = Py_None;
	tp_token = YDB_NOTTP;
	value_py = "";
	value_py_len = 0;

	/* parse and validate */
	static char *kwlist[] = {"varname", "subsarray", "value", "tp_token", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "y#|Oy#K", kwlist, &varname_py, &varname_py_len, &subsarray_py, &value_py,
					 &value_py_len, &tp_token)) {
		return NULL;
	}
	/* validate varname */
	VALIDATE_AND_CONVERT_BYTES_LEN(varname_py_len, varname_ydb_len, YDB_MAX_IDENT, YDBPY_INVALID_VARNAME_TOO_LONG,
				       YDBPY_ERRMSG_VARNAME_TOO_LONG);
	VALIDATE_SUBSARRAY(subsarray_py);
	VALIDATE_AND_CONVERT_BYTES_LEN(value_py_len, value_ydb_len, YDB_MAX_STR, YDBPY_INVALID_BYTES_TOO_LONG,
				       YDBPY_ERRMSG_BYTES_TOO_LONG);

	/* Setup for Call */
	POPULATE_NEW_BUFFER(varname_py, varname_ydb, varname_ydb_len, "set() for varname", return_NULL);
	POPULATE_SUBS_USED_AND_SUBSARRAY(subsarray_py, subs_used, subsarray_ydb, return_NULL);
	YDB_MALLOC_BUFFER(&error_string_buffer, YDB_MAX_ERRORMSG);
	POPULATE_NEW_BUFFER(value_py, value_ydb, value_ydb_len, "set() for value", return_NULL);
	if (!return_NULL) {
		/* Call the wrapped function */
		status = ydb_set_st(tp_token, &error_string_buffer, &varname_ydb, subs_used, subsarray_ydb, &value_ydb);
		/* check status for Errors and Raise Exception */
		if (YDB_OK != status) {
			raise_YDBError(status, &error_string_buffer, tp_token);
			return_NULL = true;
		}
	}

	/* free allocated memory */
	YDB_FREE_BUFFER(&varname_ydb);
	YDB_FREE_BUFFER(&value_ydb);
	FREE_BUFFER_ARRAY(subsarray_ydb, subs_used);
	YDB_FREE_BUFFER(&error_string_buffer);

	if (return_NULL)
		return NULL;
	else {
		Py_INCREF(Py_None);
		return Py_None;
	}
}

/* Wrapper for ydb_str2zwr_s() and ydb_str2zwr_st() */
static PyObject *str2zwr(PyObject *self, PyObject *args, PyObject *kwds) {
	bool	     return_NULL = false;
	int	     status;
	Py_ssize_t   str_py_len;
	unsigned int str_ydb_len;
	uint64_t     tp_token;
	char *	     str_py;
	ydb_buffer_t error_string_buf, str_ydb, zwr_ydb;
	PyObject *   zwr_py;

	/* Default values for optional arguments passed from Python */
	str_py = "";
	str_py_len = 0;
	tp_token = YDB_NOTTP;

	/* parse and validate */
	static char *kwlist[] = {"input", "tp_token", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "y#|K", kwlist, &str_py, &str_py_len, &tp_token))
		return NULL;
	VALIDATE_AND_CONVERT_BYTES_LEN(str_py_len, str_ydb_len, YDB_MAX_STR, YDBPY_INVALID_BYTES_TOO_LONG,
				       YDBPY_ERRMSG_BYTES_TOO_LONG2);

	/* Setup for Call */
	POPULATE_NEW_BUFFER(str_py, str_ydb, str_ydb_len, "ydb_str2zwr", return_NULL);
	YDB_MALLOC_BUFFER(&error_string_buf, YDB_MAX_ERRORMSG);
	YDB_MALLOC_BUFFER(&zwr_ydb, YDBPY_DEFAULT_VALUE_LEN);
	if (!return_NULL) {
		/* Call the wrapped function */
		status = ydb_str2zwr_st(tp_token, &error_string_buf, &str_ydb, &zwr_ydb);

		/* recall with properly sized buffer if zwr_buf is not long enough */
		if (YDB_ERR_INVSTRLEN == status) {
			FIX_BUFFER_LENGTH(zwr_ydb);
			/* recall the wrapped function */
			status = ydb_str2zwr_st(tp_token, &error_string_buf, &str_ydb, &zwr_ydb);
			assert(YDB_ERR_INVSTRLEN != status);
		}
		/* check status for Errors and Raise Exception */
		if (YDB_OK != status) {
			raise_YDBError(status, &error_string_buf, tp_token);
			return_NULL = true;
		}

		/* Create Python object to return */
		if (!return_NULL) {
			/* New Reference */
			zwr_py = Py_BuildValue("y#", zwr_ydb.buf_addr, (Py_ssize_t)zwr_ydb.len_used);
		}
	}
	/* free allocated memory */
	YDB_FREE_BUFFER(&str_ydb);
	YDB_FREE_BUFFER(&error_string_buf);
	YDB_FREE_BUFFER(&zwr_ydb);

	if (return_NULL)
		return NULL;
	else
		return zwr_py;
}

/* Wrapper for ydb_subscript_next_s() and ydb_subscript_next_st() */
static PyObject *subscript_next(PyObject *self, PyObject *args, PyObject *kwds) {
	bool	      return_NULL = false;
	int	      status, subs_used;
	Py_ssize_t    varname_py_len;
	unsigned int  varname_ydb_len;
	char *	      varname_py;
	uint64_t      tp_token;
	PyObject *    subsarray_py, *ret_value_py;
	ydb_buffer_t  error_string_buffer, ret_value_ydb, varname_ydb;
	ydb_buffer_t *subsarray_ydb;

	/* Default values for optional arguments passed from Python */
	subsarray_py = Py_None;
	tp_token = YDB_NOTTP;

	/* parse and validate */
	static char *kwlist[] = {"varname", "subsarray", "tp_token", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "y#|OK", kwlist, &varname_py, &varname_py_len, &subsarray_py, &tp_token))
		return NULL;

	/* validate varname */
	VALIDATE_AND_CONVERT_BYTES_LEN(varname_py_len, varname_ydb_len, YDB_MAX_IDENT, YDBPY_INVALID_VARNAME_TOO_LONG,
				       YDBPY_ERRMSG_VARNAME_TOO_LONG);
	VALIDATE_SUBSARRAY(subsarray_py);

	/* Setup for Call */
	POPULATE_NEW_BUFFER(varname_py, varname_ydb, varname_ydb_len, "subscript_next()", return_NULL);
	POPULATE_SUBS_USED_AND_SUBSARRAY(subsarray_py, subs_used, subsarray_ydb, return_NULL);
	YDB_MALLOC_BUFFER(&error_string_buffer, YDB_MAX_ERRORMSG);
	YDB_MALLOC_BUFFER(&ret_value_ydb, YDBPY_DEFAULT_SUBSCRIPT_LEN);
	if (!return_NULL) {
		/* Call the wrapped function */
		status
		    = ydb_subscript_next_st(tp_token, &error_string_buffer, &varname_ydb, subs_used, subsarray_ydb, &ret_value_ydb);

		/* check to see if length of string was longer than YDBPY_DEFAULT_SUBSCRIPT_LEN is so, try again
		 * with proper length */
		if (YDB_ERR_INVSTRLEN == status) {
			FIX_BUFFER_LENGTH(ret_value_ydb);
			/* recall the wrapped function */
			status = ydb_subscript_next_st(tp_token, &error_string_buffer, &varname_ydb, subs_used, subsarray_ydb,
						       &ret_value_ydb);
			assert(YDB_ERR_INVSTRLEN != status);
		}
		/* check status for Errors and Raise Exception */
		if (YDB_OK != status) {
			raise_YDBError(status, &error_string_buffer, tp_token);
			return_NULL = true;
		}

		/* Create Python object to return */
		if (!return_NULL) {
			/* New Reference */
			ret_value_py = Py_BuildValue("y#", ret_value_ydb.buf_addr, (Py_ssize_t)ret_value_ydb.len_used);
		}
	}
	/* free allocated memory */
	YDB_FREE_BUFFER(&varname_ydb);
	FREE_BUFFER_ARRAY(subsarray_ydb, subs_used);
	YDB_FREE_BUFFER(&error_string_buffer);
	YDB_FREE_BUFFER(&ret_value_ydb);

	if (return_NULL)
		return NULL;
	else
		return ret_value_py;
}

/* Wrapper for ydb_subscript_previous_s() and ydb_subscript_previous_st() */
static PyObject *subscript_previous(PyObject *self, PyObject *args, PyObject *kwds) {
	bool	      return_NULL = false;
	int	      status, subs_used;
	Py_ssize_t    varname_py_len;
	unsigned int  varname_ydb_len;
	char *	      varname_py;
	uint64_t      tp_token;
	PyObject *    subsarray_py, *ret_value_py;
	ydb_buffer_t  error_string_buffer, ret_value_ydb, varname_ydb;
	ydb_buffer_t *subsarray_ydb;

	/* Default values for optional arguments passed from Python */
	subsarray_py = Py_None;
	tp_token = YDB_NOTTP;

	/* Setup for Call */
	static char *kwlist[] = {"varname", "subsarray", "tp_token", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "y#|OK", kwlist, &varname_py, &varname_py_len, &subsarray_py, &tp_token))
		return NULL;

	/* validate varname */
	VALIDATE_AND_CONVERT_BYTES_LEN(varname_py_len, varname_ydb_len, YDB_MAX_IDENT, YDBPY_INVALID_VARNAME_TOO_LONG,
				       YDBPY_ERRMSG_VARNAME_TOO_LONG);
	VALIDATE_SUBSARRAY(subsarray_py);

	/* Setup for Call */
	POPULATE_NEW_BUFFER(varname_py, varname_ydb, varname_ydb_len, "subscript_previous()", return_NULL);
	POPULATE_SUBS_USED_AND_SUBSARRAY(subsarray_py, subs_used, subsarray_ydb, return_NULL);
	YDB_MALLOC_BUFFER(&error_string_buffer, YDB_MAX_ERRORMSG);
	YDB_MALLOC_BUFFER(&ret_value_ydb, YDBPY_DEFAULT_SUBSCRIPT_LEN);
	if (!return_NULL) {
		/* Call the wrapped function */
		status = ydb_subscript_previous_st(tp_token, &error_string_buffer, &varname_ydb, subs_used, subsarray_ydb,
						   &ret_value_ydb);

		/* check to see if length of string was longer than YDBPY_DEFAULT_SUBSCRIPT_LEN is so, try again
		 * with proper length */
		if (YDB_ERR_INVSTRLEN == status) {
			FIX_BUFFER_LENGTH(ret_value_ydb);
			status = ydb_subscript_previous_st(tp_token, &error_string_buffer, &varname_ydb, subs_used, subsarray_ydb,
							   &ret_value_ydb);
			assert(YDB_ERR_INVSTRLEN != status);
		}
		/* check status for Errors and Raise Exception */
		if (YDB_OK != status) {
			raise_YDBError(status, &error_string_buffer, tp_token);
			return_NULL = true;
		}

		/* Create Python object to return */
		if (!return_NULL) {
			/* New Reference */
			ret_value_py = Py_BuildValue("y#", ret_value_ydb.buf_addr, (Py_ssize_t)ret_value_ydb.len_used);
		}
	}
	/* free allocated memory */
	YDB_FREE_BUFFER(&varname_ydb);
	FREE_BUFFER_ARRAY(subsarray_ydb, subs_used);
	YDB_FREE_BUFFER(&error_string_buffer);
	YDB_FREE_BUFFER(&ret_value_ydb);

	if (return_NULL)
		return NULL;
	else
		return ret_value_py;
}

/* Callback functions used by Wrapper for ydb_tp_s() / ydb_tp_st() */

/* Callback Wrapper used by tp_st. The approach of calling a Python function is a
 * bit of a hack. Here's how it works:
 *      1) This is the callback function that is always passed to the ydb_tp_st
 *              Simple API function and should only ever be called by ydb_tp_st
 *              via tp() below. It assumes that everything passed to it was validated.
 *      2) The actual Python function to be called is passed to this function
 *              as the first element in a Python tuple.
 *      3) The positional arguments are passed as the second element and the
 *              keyword args are passed as the third.
 *      4) The new tp_token that ydb_tp_st passes to this function as an argument
 *              is added to the kwargs dictionary.
 *      5) This function calls calls the Python callback function with the args and
 *              kwargs arguments.
 *      6) if a function raises an exception then this function returns TPCALLBACKINVRETVAL
 *              as a way of indicating an error.
 *      Note: the PyErr String is already set so the the function receiving the return
 *              value (tp()) just needs to return NULL.
 */
static int callback_wrapper(uint64_t tp_token_ydb, ydb_buffer_t *errstr, void *function_with_arguments) {
	int	  ret_value_ydb;
	bool	  decref_args = false;
	bool	  decref_kwargs = false;
	PyObject *function, *args, *kwargs, *ret_value_py, *tp_token_py;

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

	tp_token_py = Py_BuildValue("K", tp_token_ydb);
	PyDict_SetItemString(kwargs, "tp_token", tp_token_py);
	Py_DECREF(tp_token_py);

	ret_value_py = PyObject_Call(function, args, kwargs); // New Reference

	if (decref_args)
		Py_DECREF(args);
	if (decref_kwargs)
		Py_DECREF(kwargs);

	if (NULL == ret_value_py) {
		/* `function` raised an exception.
		 *      Note: Do not need to `PyErr_SetString` because this or similar operation
		 *              was done when the exception was raised by `function`
		 */
		return YDB_ERR_TPCALLBACKINVRETVAL;
	} else if (!PyLong_Check(ret_value_py)) {
		PyErr_SetString(PyExc_TypeError, "Callback function must return value of type int.");
		return YDB_ERR_TPCALLBACKINVRETVAL;
	}
	ret_value_ydb = (int)PyLong_AsLong(ret_value_py);
	Py_DECREF(ret_value_py);

	return ret_value_ydb;
}

/* Wrapper for ydb_tp_s() / ydb_tp_st() */
static PyObject *tp(PyObject *self, PyObject *args, PyObject *kwds) {
	bool	      return_NULL = false;
	bool	      success;
	int	      namecount, status;
	uint64_t      tp_token;
	char *	      transid;
	PyObject *    callback, *callback_args, *callback_kwargs, *varnames_py, *function_with_arguments;
	ydb_buffer_t  error_string_buffer;
	ydb_buffer_t *varnames_ydb;

	/* Default values for optional arguments passed from Python */
	callback_args = Py_None;
	callback_kwargs = Py_None;
	transid = "BATCH";
	namecount = 0;
	varnames_py = Py_None;
	tp_token = YDB_NOTTP;

	/* parse and validate */
	static char *kwlist[] = {"callback", "args", "kwargs", "transid", "varnames", "tp_token", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|OOsOK", kwlist, &callback, &callback_args, &callback_kwargs, &transid,
					 &varnames_py, &tp_token)) {
		return_NULL = true;
	}

	/* validate input */
	if (!PyCallable_Check(callback)) {
		PyErr_SetString(PyExc_TypeError, "'callback' must be a callable.");
		return_NULL = true;
	}
	if ((Py_None != callback_args) && !PyTuple_Check(callback_args)) {
		PyErr_SetString(PyExc_TypeError, "'args' must be a tuple. "
						 "(It will be passed to the callback "
						 "function as positional arguments.)");
		return_NULL = true;
	}
	if ((Py_None != callback_kwargs) && !PyDict_Check(callback_kwargs)) {
		PyErr_SetString(PyExc_TypeError, "'kwargs' must be a dictionary. "
						 "(It will be passed to the callback function as keyword arguments.)");
		return_NULL = true;
	}
	VALIDATE_VARNAMES(varnames_py);
	if (!return_NULL) {
		/* Setup for Call */
		YDB_MALLOC_BUFFER(&error_string_buffer, YDB_MAX_ERRORMSG);
		/* New Reference */
		function_with_arguments = Py_BuildValue("(OOO)", callback, callback_args, callback_kwargs);
		namecount = 0;
		if (Py_None != varnames_py)
			namecount = PySequence_Length(varnames_py);

		varnames_ydb = (ydb_buffer_t *)calloc(namecount, sizeof(ydb_buffer_t));
		if (0 < namecount) {
			success = convert_py_bytes_sequence_to_ydb_buffer_array(varnames_py, namecount, varnames_ydb);
			if (!success)
				return NULL;
		}

		/* Call the wrapped function */
		status = ydb_tp_st(tp_token, &error_string_buffer, callback_wrapper, function_with_arguments, transid, namecount,
				   varnames_ydb);
		/* check status for Errors and Raise Exception */
		if (YDB_ERR_TPCALLBACKINVRETVAL == status) {
			return_NULL = true;
		} else if (YDB_TP_RESTART == status) {
			PyErr_SetString(YDBTPRestart, "tp() callback function returned 'YDB_TP_RESTART'.");
			return_NULL = true;
		} else if (YDB_TP_ROLLBACK == status) {
			PyErr_SetString(YDBTPRollback, "tp() callback function returned 'YDB_TP_ROLLBACK'.");
			return_NULL = true;
		} else if (YDB_OK != status) {
			raise_YDBError(status, &error_string_buffer, tp_token);
			return_NULL = true;
		}
		/* free allocated memory */
		Py_DECREF(function_with_arguments);
		YDB_FREE_BUFFER(&error_string_buffer);
		free(varnames_ydb);
	}

	if (return_NULL)
		return NULL;
	else
		return Py_BuildValue("i", status);
}

/* Wrapper for ydb_zwr2str_s() and ydb_zwr2str_st() */
static PyObject *zwr2str(PyObject *self, PyObject *args, PyObject *kwds) {
	bool	     return_NULL = false;
	int	     status;
	Py_ssize_t   zwr_py_len;
	unsigned int zwr_ydb_len;
	uint64_t     tp_token;
	char *	     zwr_py;
	PyObject *   str_py;
	ydb_buffer_t error_string_buf, zwr_ydb, str_ydb;

	/* Default values for optional arguments passed from Python */
	zwr_py = "";
	zwr_py_len = 0;
	tp_token = YDB_NOTTP;

	/* parse and validate */
	static char *kwlist[] = {"input", "tp_token", NULL};
	/* Parsed values are borrowed references, do not Py_DECREF them. */
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "y#|K", kwlist, &zwr_py, &zwr_py_len, &tp_token))
		return NULL;
	VALIDATE_AND_CONVERT_BYTES_LEN(zwr_py_len, zwr_ydb_len, YDB_MAX_STR, YDBPY_INVALID_BYTES_TOO_LONG,
				       YDBPY_ERRMSG_BYTES_TOO_LONG2);

	/* Setup for Call */
	POPULATE_NEW_BUFFER(zwr_py, zwr_ydb, zwr_ydb_len, "zwr2str()", return_NULL);
	YDB_MALLOC_BUFFER(&error_string_buf, YDB_MAX_ERRORMSG);
	YDB_MALLOC_BUFFER(&str_ydb, YDBPY_DEFAULT_VALUE_LEN);
	if (!return_NULL) {
		/* Call the wrapped function */
		status = ydb_zwr2str_st(tp_token, &error_string_buf, &zwr_ydb, &str_ydb);
		/* recall with properly sized buffer if zwr_ydb is not long enough */
		if (YDB_ERR_INVSTRLEN == status) {
			FIX_BUFFER_LENGTH(str_ydb);
			/* recall the wrapped function */
			status = ydb_zwr2str_st(tp_token, &error_string_buf, &zwr_ydb, &str_ydb);
			assert(YDB_ERR_INVSTRLEN != status);
		}
		/* check status for Errors and Raise Exception */
		if (YDB_OK != status) {
			raise_YDBError(status, &error_string_buf, tp_token);
			return_NULL = true;
		}
		if (!return_NULL) {
			/* New Reference */
			str_py = Py_BuildValue("y#", str_ydb.buf_addr, (Py_ssize_t)str_ydb.len_used);
		}
	}
	YDB_FREE_BUFFER(&zwr_ydb);
	YDB_FREE_BUFFER(&error_string_buf);
	YDB_FREE_BUFFER(&str_ydb);

	if (return_NULL)
		return NULL;
	else
		return str_py;
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
 * Each strict has 4 elements:
 *      1. The Python name of the method
 *      2. The C function that is to be called when the method is called
 *      3. How the function can be called (all these functions allow for calling with both positional and keyword arguments)
 *      4. The docstring for this method (to be used by help() in the Python REPL)
 * The final struct is a sentinel value to indicate the end of the array (i.e. {NULL, NULL, 0, NULL})
 */
static PyMethodDef methods[] = {
    /* Simple and Simple Threaded API Functions */
    {"data", (PyCFunction)data, METH_VARARGS | METH_KEYWORDS,
     "used to learn what type of data is at a node.\n "
     "0 : There is neither a value nor a subtree, "
     "i.e., it is undefined.\n"
     "1 : There is a value, but no subtree\n"
     "10 : There is no value, but there is a subtree.\n"
     "11 : There are both a value and a subtree.\n"},
    {"delete", (PyCFunction)delete_wrapper, METH_VARARGS | METH_KEYWORDS, "deletes node value or tree data at node"},
    {"delete_excel", (PyCFunction)delete_excel, METH_VARARGS | METH_KEYWORDS,
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
    {"node_next", (PyCFunction)node_next, METH_VARARGS | METH_KEYWORDS,
     "facilitate depth-first traversal of a local or global"
     " variable tree. returns string tuple of subscripts of"
     " next node with value."},
    {"node_previous", (PyCFunction)node_previous, METH_VARARGS | METH_KEYWORDS,
     "facilitate depth-first traversal of a local "
     "or global variable tree. returns string tuple"
     "of subscripts of previous node with value."},
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
    {"tp", (PyCFunction)tp, METH_VARARGS | METH_KEYWORDS, "transaction"},

    {"zwr2str", (PyCFunction)zwr2str, METH_VARARGS | METH_KEYWORDS,
     "returns the Bytes Object from the zwrite formated Bytes "
     "object provided as input."},
    /* API Utility Functions */
    {NULL, NULL, 0, NULL} /* Sentinel */
};

/* The _yottadbmodule struct contains the information about the module
 * (https://docs.python.org/3/c-api/module.html#initializing-c-modules)
 * The elements are:
 *      1. PyModuleDef_HEAD_INIT (Python requires that all modules have this as the first value)
 *      2. The name of the module
 *      3. The modules Doc string. (For use by help() in the Python REPL)
 *      4. Size of per-interpreter state or -1 to keep the module state in global variables.
 *      5. The above defined array of module methods.
 */
static struct PyModuleDef _yottadbmodule
    = {PyModuleDef_HEAD_INIT, "_yottadb", "A module that provides basic access to the YottaDB's Simple API", -1, methods};

/* The initialization function for _yottadb.
 * This function must be named PyInit_{name of Module}
 */
PyMODINIT_FUNC PyInit__yottadb(void) {
	/* Initialize the module */
	PyObject *module = PyModule_Create(&_yottadbmodule);

	/* Defining Module 'Constants' */
	PyObject *module_dictionary = PyModule_GetDict(module);

	/* Expose constants defined in libyottadb.h */
	PyDict_SetItemString(module_dictionary, "YDB_DEL_TREE", Py_BuildValue("i", YDB_DEL_TREE));
	PyDict_SetItemString(module_dictionary, "YDB_DEL_NODE", Py_BuildValue("i", YDB_DEL_NODE));

	PyDict_SetItemString(module_dictionary, "YDB_SEVERITY_WARNING", Py_BuildValue("i", YDB_SEVERITY_WARNING));
	PyDict_SetItemString(module_dictionary, "YDB_SEVERITY_SUCCESS", Py_BuildValue("i", YDB_SEVERITY_SUCCESS));
	PyDict_SetItemString(module_dictionary, "YDB_SEVERITY_ERROR", Py_BuildValue("i", YDB_SEVERITY_ERROR));
	PyDict_SetItemString(module_dictionary, "YDB_SEVERITY_INFORMATIONAL", Py_BuildValue("i", YDB_SEVERITY_INFORMATIONAL));
	PyDict_SetItemString(module_dictionary, "YDB_SEVERITY_FATAL", Py_BuildValue("i", YDB_SEVERITY_FATAL));

	PyDict_SetItemString(module_dictionary, "YDB_DATA_UNDEF", Py_BuildValue("i", YDB_DATA_UNDEF));
	PyDict_SetItemString(module_dictionary, "YDB_DATA_VALUE_NODESC", Py_BuildValue("i", YDB_DATA_VALUE_NODESC));
	PyDict_SetItemString(module_dictionary, "YDB_DATA_NOVALUE_DESC", Py_BuildValue("i", YDB_DATA_NOVALUE_DESC));
	PyDict_SetItemString(module_dictionary, "YDB_DATA_VALUE_DESC", Py_BuildValue("i", YDB_DATA_VALUE_DESC));
	PyDict_SetItemString(module_dictionary, "YDB_DATA_ERROR", Py_BuildValue("i", YDB_DATA_ERROR));

	PyDict_SetItemString(module_dictionary, "YDB_RELEASE", Py_BuildValue("i", YDB_RELEASE));

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

	/* expose useful constants from libydberrors.h or libydberrors2.h */
	PyDict_SetItemString(module_dictionary, "YDB_ERR_TPTIMEOUT", Py_BuildValue("i", YDB_ERR_TPTIMEOUT));

	/* expose useful constants defined in _yottadb.h */
	PyDict_SetItemString(module_dictionary, "YDB_LOCK_MAX_KEYS", Py_BuildValue("i", YDB_LOCK_MAX_KEYS));

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

	YDBTPRollback = PyErr_NewException("_yottadb.YDBTPRollback", YDBTPException, NULL);
	PyModule_AddObject(module, "YDBTPRollback", YDBTPRollback);

	YDBTPRestart = PyErr_NewException("_yottadb.YDBTPRestart", YDBTPException, NULL);
	PyModule_AddObject(module, "YDBTPRestart", YDBTPRestart);

	YDBTimeoutError = PyErr_NewException("_yottadb.YDBTimeoutError", YDBException, NULL);
	PyModule_AddObject(module, "YDBTimeoutError", YDBTimeoutError);

	YDBPythonError = PyErr_NewException("_yottadb.YDBPythonError", YDBException, NULL);
	PyModule_AddObject(module, "YDBPythonError", YDBPythonError);

	YDBError = PyErr_NewException("_yottadb.YDBError", YDBException, NULL);
	PyModule_AddObject(module, "YDBError", YDBError);
	/* add auto generated Exceptions from libydberrors.h or libydberrors2.h */
	ADD_YDBERRORS();

	/* return the now fully initialized module */
	return module;
}