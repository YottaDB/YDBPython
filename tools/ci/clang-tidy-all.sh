#!/usr/bin/env bash

#################################################################
#								#
# Copyright (c) 2021-2025 YottaDB LLC and/or its subsidiaries.	#
# All rights reserved.						#
#								#
#	This source code contains the intellectual property	#
#	of its copyright holder(s), and is made available	#
#	under a license.  If you do not know the terms of	#
#	the license, please stop and do not read further.	#
#								#
#################################################################

set -e # Fail script if any command fails
set -u # Enable detection of uninitialized variables
set -o pipefail	# this way $? is set to zero only if ALL commands in a pipeline succeed. Else only last command determines $?

ignored_warnings="\
-clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling,\
-clang-diagnostic-gnu-zero-variadic-macro-arguments,\
-clang-analyzer-security.insecureAPI.strcpy"

if ! clang_tidy=$($(git rev-parse --show-toplevel)/tools/ci/find-llvm-tool.sh clang-tidy 8); then
	echo "error: clang-tidy-8 or greater is required"
	exit 1
fi

rm -f temp_warnings.txt
echo "# Run clang-tidy..."
for file in $(find . -name '*.[ch]'); do
	# Direct clang-tidy output to a temporary file for later processing by grep
	$clang_tidy $file --checks="$ignored_warnings" -- -I $ydb_dist -I . -I /usr/include/python3.12 >> temp_warnings.txt 2> /dev/null || true
done

# Extract warning and error messages from clang-tidy call. Since it is not
# straightforward to grep two patterns from one stdin stream, grep them out
# individually into a single file.
echo "# Extract warning and error messages from clang-tidy output..."
grep "warning:" temp_warnings.txt &> clang_tidy_warnings.txt || true # Ignore grep failure when no warnings are present
grep "error:" temp_warnings.txt >> clang_tidy_warnings.txt || true # Ignore grep failure when no errors are present
exit 0
