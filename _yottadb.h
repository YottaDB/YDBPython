/****************************************************************
 *                                                              *
 * Copyright (c) 2020-2021 Peter Goss All rights reserved.      *
 *                                                              *
 * Copyright (c) 2020-2025 YottaDB LLC and/or its subsidiaries. *
 * All rights reserved.                                         *
 *                                                              *
 *  This source code contains the intellectual property         *
 *  of its copyright holder(s), and is made available           *
 *  under a license.  If you do not know the terms of           *
 *  the license, please stop and do not read further.           *
 *                                                              *
 ****************************************************************/

#include <libyottadb.h>
#include <Python.h>

#define YDBPY_DEFAULT_VALUE_LEN	       32
#define YDBPY_DEFAULT_SUBSCRIPT_LEN    16
#define YDBPY_DEFAULT_SUBSCRIPT_COUNT  2
#define CANONICAL_NUMBER_TO_STRING_MAX 48

#define YDB_LOCK_MIN_ARGS		2
#define YDB_LOCK_ARGS_PER_NODE		3
#define YDB_CALL_VARIADIC_MAX_ARGUMENTS 36
#define YDB_LOCK_MAX_NODES		(YDB_CALL_VARIADIC_MAX_ARGUMENTS - YDB_LOCK_MIN_ARGS) / YDB_LOCK_ARGS_PER_NODE

/* Large enough to fit any YDB error message, per
 * https://docs.yottadb.com/ProgrammersGuide/extrout.html#ydb-zstatus
 */
#define YDBPY_MAX_ERRORMSG 2048

// Default size to allocate for ci() output parameters
#define YDBPY_DEFAULT_OUTBUF 2048

#define YDBPY_CHECK_TYPE 2

/* Set of acceptable Python error types. Each type is named by prefixing a Python error name with `YDBPython`,
 * with the exception of YDBPython_NoError. This item doesn't represent a Python error, but is included at enum value 0
 * to prevent conflicts with YDB_OK which signals no error with a value of 0.
 */
typedef enum YDBPythonErrorType {
	YDBPython_TypeError = 1,
	YDBPython_ValueError,
	YDBPython_OSError,
} YDBPythonErrorType;

/* Set of acceptable Python Sequence types. Used to enforce correct limits and
 * emit relevant errors when processing Python Sequences passed down from
 * yottadb.py.
 */
typedef enum YDBPythonSequenceType {
	YDBPython_VarnameSequence,
	YDBPython_SubsarraySequence,
	YDBPython_NodeSequence,
} YDBPythonSequenceType;

// TypeError messages
#define YDBPY_ERR_IMMUTABLE_OUTPUT_ARGS                                                                                           \
	"YottaDB call-in argument list is immutable, but routine has output argument(s). Pass argument list as a Python List to " \
	"allow output argument updates."
#define YDBPY_ERR_CALLIN_ARGS_NOT_SEQ "YottaDB call-in arguments must be passed as a Sequence"
#define YDBPY_ERR_INVALID_ARGS	      "YottaDB call-in routine '%s' has incorrect number of parameters: %u expected, got %u"
#define YDBPY_ERR_INVALID_CI_ARG_TYPE \
	"YottaDB call-in routine '%s' parameter %d has invalid type: must be str, bytes, int, or float"
#define YDBPY_ERR_CI_PARM_UNDEFINED		    "YottaDB call-in routine %s parameter %d not defined in call-in table"
#define YDBPY_ERR_NOT_LIST_OR_TUPLE		    "node must be list or tuple."
#define YDBPY_ERR_VARNAME_NOT_BYTES_LIKE	    "varname argument is not a bytes-like object (bytes or str)"
#define YDBPY_ERR_ARG_NOT_BYTES_LIKE		    "argument is not a bytes-like object (bytes or str)"
#define YDBPY_ERR_ITEM_NOT_BYTES_LIKE		    "item %ld is not a bytes-like object (bytes or str)"
#define YDBPY_ERR_NODE_IN_SEQUENCE_NOT_LIST_OR_TUPLE "item %ld is not a list or tuple."
#define YDBPY_ERR_NODE_IN_SEQUENCE_VARNAME_NOT_BYTES "item %ld in node sequence invalid: first element must be of type 'bytes'"

// ValueError messages
#define YDBPY_ERR_EMPTY_FILENAME		   "YottaDB filenames must be one character or longer"
#define YDBPY_ERR_VARNAME_TOO_LONG		   "invalid varname length %ld: max %d"
#define YDBPY_ERR_SEQUENCE_TOO_LONG		   "invalid sequence length %ld: max %d"
#define YDBPY_ERR_BYTES_TOO_LONG		   "invalid bytes length %ld: max %d"
#define YDBPY_ERR_NODE_IN_SEQUENCE_INCORRECT_LENGTH "item %lu must be length 1 or 2."
#define YDBPY_ERR_NODE_IN_SEQUENCE_VARNAME_TOO_LONG "item %ld in node sequence has invalid varname length %ld: max %d."

#define YDBPY_ERR_NODE_IN_SEQUENCE_SUBSARRAY_INVALID "item %ld in node sequence has invalid subsarray: %s"

#define YDBPY_ERR_VARNAME_INVALID     "'varnames' argument invalid: %s"
#define YDBPY_ERR_SUBSARRAY_INVALID   "'subsarray' argument invalid: %s"
#define YDBPY_ERR_NODES_INVALID	      "'nodes' argument invalid: %s"
#define YDBPY_ERR_ROUTINE_UNSPECIFIED "No call-in routine specified. Routine name required for M call-in."

#define YDBPY_ERR_SYSCALL "System call failed: %s, return %d (%s)"

#define YDBPY_ERR_FAILED_NUMERIC_CONVERSION "Failed to convert Python numeric value to internal representation"

// Prevents compiler warnings for variables used only in asserts
#define UNUSED(x) (void)(x)

// Redefine __assert_fail from libc (used by assert.h) to enable custom assert message.
void __assert_fail(const char *assertion, const char *file, unsigned int line, const char *function) {
	fprintf(stderr,
		"Assertion '%s' failed in function '%s' in %s at line %d.\nPlease file a bug report at "
		"https://gitlab.com/YottaDB/Lang/YDBPython/-/issues\n",
		assertion, function, file, line);
	abort();
}

/* A structure that represents a node using YDB C types. used internally for
 * converting between Python and YDB C types.
 */
typedef struct {
	ydb_buffer_t *varname;
	int	      subs_used;
	ydb_buffer_t *subsarray;
} YDBNode;

#define YDB_COPY_BYTES_TO_BUFFER(BYTES, BYTES_LEN, BUFFERP, COPY_DONE) \
	{                                                              \
		if (BYTES_LEN <= (BUFFERP)->len_alloc) {               \
			memcpy((BUFFERP)->buf_addr, BYTES, BYTES_LEN); \
			(BUFFERP)->len_used = BYTES_LEN;               \
			COPY_DONE = TRUE;                              \
		} else {                                               \
			COPY_DONE = FALSE;                             \
		}                                                      \
	}

#define POPULATE_SUBS_USED_AND_SUBSARRAY(SUBSARRAY_PY, SUBSUSED, SUBSARRAY_YDB, RETURN_NULL)                      \
	{                                                                                                         \
		bool success = true;                                                                              \
                                                                                                                  \
		SUBSUSED = 0;                                                                                     \
		SUBSARRAY_YDB = NULL;                                                                             \
		if (Py_None != SUBSARRAY_PY) {                                                                    \
			SUBSUSED = PySequence_Length(SUBSARRAY_PY);                                               \
			SUBSARRAY_YDB = malloc(SUBSUSED * sizeof(ydb_buffer_t));                                  \
			success = convert_py_sequence_to_ydb_buffer_array(SUBSARRAY_PY, SUBSUSED, SUBSARRAY_YDB); \
			if (!success) {                                                                           \
				FREE_BUFFER_ARRAY(SUBSARRAY_YDB, SUBSUSED);                                       \
				RETURN_NULL = true;                                                               \
			}                                                                                         \
		}                                                                                                 \
	}

#define FREE_BUFFER_ARRAY(ARRAY, LEN)                                         \
	{                                                                     \
		if (NULL != ARRAY) {                                          \
			for (int i = 0; i < (LEN); i++) {                     \
				YDB_FREE_BUFFER(&((ydb_buffer_t *)ARRAY)[i]); \
			}                                                     \
			free(ARRAY);                                          \
		}                                                             \
	}

#define FREE_STRING_ARRAY(ARRAY, LEN)                              \
	{                                                          \
		if (NULL != ARRAY) {                               \
			for (unsigned int i = 0; i < (LEN); i++) { \
				free(ARRAY[i].address);            \
			}                                          \
			free(ARRAY);                               \
		}                                                  \
	}

#define RETURN_IF_INVALID_SEQUENCE(SEQUENCE, SEQUENCE_TYPE)              \
	{                                                                \
		if (!is_valid_sequence(SEQUENCE, SEQUENCE_TYPE, NULL)) { \
			return NULL;                                     \
		}                                                        \
	}

#define FIX_BUFFER_LENGTH(BUFFER)                           \
	{                                                   \
		int correct_length = BUFFER.len_used;       \
                                                            \
		YDB_FREE_BUFFER(&BUFFER);                   \
		YDB_MALLOC_BUFFER(&BUFFER, correct_length); \
	}

#define RAISE_SPECIFIC_ERROR(ERROR_TYPE, MESSAGE)     \
	{                                             \
		assert(NULL != MESSAGE);              \
		PyErr_SetObject(ERROR_TYPE, MESSAGE); \
	}

/* Allocate and populate a ydb_buffer_t struct from a Python AnyStr (`str` or `bytes`)
 * object and return on failure.
 */
#define INVOKE_ANYSTR_TO_BUFFER(ANYSTR, BUFFER, IS_VARNAME)               \
	{                                                                 \
		int status;                                               \
                                                                          \
		status = anystr_to_buffer(ANYSTR, &(BUFFER), IS_VARNAME); \
		if (YDB_OK != status) {                                   \
			return NULL;                                      \
		}                                                         \
	}

/* Allocate and populate a ydb_buffer_t array representing a set of subscripts.
 * In case of failure, free the specified CLEANUP_BUF and return.
 */
#define INVOKE_POPULATE_SUBS_USED_AND_SUBSARRAY_AND_CLEANUP_VARNAME(SUBSARRAY_PY, SUBS_USED, SUBSARRAY_YDB, CLEANUP_BUF) \
	{                                                                                                                \
		int status;                                                                                              \
                                                                                                                         \
		status = populate_subs_used_and_subsarray(SUBSARRAY_PY, &(SUBS_USED), &(SUBSARRAY_YDB));                 \
		if (YDB_OK != status) {                                                                                  \
			YDB_FREE_BUFFER(&(CLEANUP_BUF));                                                                 \
			return NULL;                                                                                     \
		}                                                                                                        \
	}

/* PYTHON EXCEPTION DECLARATIONS */

/* YottaDBError represents an error return status from any of the libyottadb
 * functions being wrapped. Since YottaDB returns a status that is a number and
 * has a way to create a message from that number the choice was to preserve
 * both in the python exception. This means we need to extend the exception to
 * accept both. Use raise_YottaDBError function to raise
 */
static PyObject *YDBException;
static PyObject *YDBError;

static PyObject *YDBTPException;
static PyObject *YDBTPRestart;
static PyObject *YDBTPRollback;
static PyObject *YDBNotOk;
static PyObject *YDBDeferHandler;
static PyObject *YDBNodeEnd;

/* YDBLockTimeoutError is a simple exception to indicate that a lock failed due
 * to timeout. */
static PyObject *YDBLockTimeoutError;
/* YDBTPTimeoutError is a simple exception to indicate that a transaction callback
 * function failed due to timeout. */
static PyObject *YDBTPTimeoutError;

/* YDBPythonError is to be raised when there is a possibility for an error to
   occur but that we believe that it should never happen. */
static PyObject *YDBPythonError;
