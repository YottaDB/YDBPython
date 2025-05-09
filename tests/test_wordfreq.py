#################################################################
#                                                               #
# Copyright (c) 2021-2025 YottaDB LLC and/or its subsidiaries.  #
# All rights reserved.                                          #
#                                                               #
#   This source code contains the intellectual property         #
#   of its copyright holder(s), and is made available           #
#   under a license.  If you do not know the terms of           #
#   the license, please stop and do not read further.           #
#                                                               #
#################################################################
import pytest  # type: ignore # ignore due to pytest not having type annotations
import os

import yottadb


# Constants for loading names and other things with
MAX_VARNAME_LEN = 8
MAX_WORDS_SUBS = 1
MAX_INDEX_SUBS = 2
MAX_WORD_LEN = 128  # Size increased from the C version which uses 64 here
TP_TOKEN = yottadb.YDB_NOTTP


def test_wordfreq(new_db):
    # Initialize all array variable names we are planning to use. Randomly use locals vs globals to store word frequencies.
    # If using globals, delete any previous contents
    if os.getpid() % 2:
        words_var = b"words"
        index_var = b"index"
    else:
        words_var = b"^words"
        yottadb.delete_tree(words_var)
        index_var = b"^index"
        yottadb.delete_tree(index_var)

    # Read lines from input file and count the number of words in each
    with open("tests/wordfreq_input.txt", "r") as input_file:
        lines = input_file.readlines()
        for line in lines:
            line = line.lower()  # Lowercase the current line
            words = line.split()  # Breakup line into words on whitespace
            for word in words:
                yottadb.incr(words_var, (word.encode(),))

    for subscript in yottadb.subscripts(words_var, (b"",)):
        count = yottadb.get(words_var, (subscript,))
        yottadb.set(index_var, (count, subscript), b"")

    with open("wordfreq.out", "w") as output_file:
        for word in reversed(yottadb.subscripts(index_var, (b"",))):
            for count in yottadb.subscripts(index_var, (word, b"")):
                output_file.write(f"{word}\t{count}\n")

    yottadb.delete_tree(words_var)
    yottadb.delete_tree(index_var)


def test_wordfreq_key(new_db):
    yottadb.Key("^words").delete_tree()
    yottadb.Key("^index").delete_tree()
    try:
        with open("tests/wordfreq_input.txt", "r") as input_file:
            words = yottadb.Key("^words")
            for line in input_file.readlines():
                for word in line.split(" "):
                    word = word.strip().lower()
                    if word != "":
                        words[word].incr()

            # Iterate through words and store based on frequency
            index = yottadb.Key("^index")
            for word in words[""]:
                index[words[word.leaf].value][word.leaf].value = ""

            # Print the keys ordered by amount
            with open("wordfreq_key.out", "w") as output_file:
                for key1 in reversed(index):
                    for key2 in yottadb.Key("^index")[key1.name]:
                        output_file.write(f"{key1.name}\t{key2.name}\n")
    except IOError:
        print("Failed to open file")

    yottadb.Key("^words").delete_tree()
    yottadb.Key("^index").delete_tree()
