#!/bin/bash
#################################################################
#								#
# Copyright (c) 2019-2021 YottaDB LLC and/or its subsidiaries.	#
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

warnings="$1"

# These don't impact the warning itself
printf "Remove 'note:' lines... "
sed "/:\( \)\?note: /d" "$warnings" > no_note.txt
echo "OK."

# Extract base filename
# NOTE: this intentionally ignores errors that don't have a filename, since they're an issue with the build process itself.
echo -n "Extracting basename... "
grep "warning:" no_note.txt | cut -d " " -f 1 | sed 's!.*/!!' | cut -d ":" -f 1 > filenames.txt || true # Ignore grep failure when no warnings are present
grep "error:" no_note.txt | cut -d " " -f 1 | sed 's!.*/!!' | cut -d ":" -f 1 >> filenames.txt || true # Ignore grep failure when no errors are present
echo "OK."

# Extract base warning message
echo -n "Extracting warning and error messages... "
grep "warning:" no_note.txt | cut -d " " -f 2- > warnings.txt || true # Ignore grep failure when no warnings are present
grep "error:" no_note.txt | cut -d " " -f 2- >> warnings.txt || true # Ignore grep failure when no errors are present
echo "OK."

# Concatenate filenames with warning messages and sort
echo -n "Combining and sorting filenames with messages... "
paste -d ": " filenames.txt warnings.txt | sort > sorted_warnings.txt
echo "OK."
