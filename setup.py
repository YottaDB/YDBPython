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
from setuptools import setup, Extension, find_packages
import os
import pathlib
import subprocess
import re


# Confirm $ydb_dist is set before running tests
YDB_DIST = os.environ.get("ydb_dist")
if YDB_DIST is None:
    print("error: $ydb_dist is not set in the environment")
    print("help: run `source $(pkg-config --variable=prefix yottadb)/ydb_env_set`")
    exit(1)


def run_shell_cmd(cmd: str) -> str:
    return subprocess.run(cmd, stdout=subprocess.PIPE, shell=True).stdout.decode()


def create_constants_from_header_file():
    """
    Programmatically generates Python integer objects from YDB C constants found in libyottadb.h,
    libydberrors.h, and libydberrors2.h and places in _yottadbconstants.h for inclusion in yottadb.py.

    :returns: None.
    """
    YDB_Dir = pathlib.Path(YDB_DIST)
    constants_header = pathlib.Path(".") / "_yottadbconstants.h"
    constant_data = []
    file_paths = [YDB_Dir / "libyottadb.h", YDB_Dir / "libydberrors.h", YDB_Dir / "libydberrors2.h"]
    for file_path in file_paths:
        with file_path.open() as file:
            for line in file.readlines():
                if re.match("#define\\sYDB_\\w*\\s[\\w\\(\\)<]*\\s.*", line) or re.match(
                    "#define\\sDEFAULT_\\w*\\s[\\w\\(\\)]*\\s.*", line
                ):
                    parts = list(filter(lambda string: string != "", line.replace("\n", "").split("\t")))
                    constant_data.append({"name": parts[1], "type": "K"})
                elif re.match("\tYDB_\\w* = [\\w]*.*", line):
                    parts = line.split()
                    constant_data.append({"name": parts[0], "type": "K"})
                elif re.match("#define YDB_ERR\\w* -\\d*.*", line):
                    parts = line.split()
                    constant_data.append({"name": parts[1], "type": "i"})

    header_file_text = ""
    header_file_text += "\n"
    # create macro to add constants to module
    header_file_text += "#define ADD_YDBCONSTANTS(MODULE_DICTIONARY) { \\\n"
    add_constant_template = '    PyDict_SetItemString(MODULE_DICTIONARY, "{c_name}", Py_BuildValue("{int_type}", {c_name})); \\\n'
    for constant_info in constant_data:
        header_file_text += (
            add_constant_template.replace("{c_name}", constant_info["name"])
            .replace("{c_name}", constant_info["name"])
            .replace("{int_type}", constant_info["type"])
        )
    header_file_text += "}\n"
    header_file_text += "\n"

    with constants_header.open("w") as header_file:
        header_file.write(header_file_text)


# Define link and compile argument lists before calling setup() to allow optional
# arguments to be appended prior to compilation.
extra_link_args = ["-lyottadb", "-lffi", "-Wl,-rpath=" + YDB_DIST]
# Set `-Wno-cast-function-type` to suppress 'cast between incompatible function types' warning
# See discussion at: https://gitlab.com/YottaDB/DB/YDBDoc/-/merge_requests/482#note_686747517
extra_compile_args = ["--std=c99", "-Wall", "-Wextra", "-pedantic", "-Wno-cast-function-type"]
# Check whether YDB was compiled with address sanitization (ASAN). If so, compile YDBPython with it also.
use_asan = int(run_shell_cmd("nm $ydb_dist/libyottadb.so | grep -c 'U __asan_init'"))
if use_asan:
    print("YDBPython: YottaDB was compiled with address sanitization (ASAN). Compiling YDBPython WITH ASAN...")
    # Set required environment variables
    # Note that it is acceptable to build YDBPython with `gcc` and ASAN even if `libyottadb.so` was compiled with
    # `clang` and ASAN.
    os.environ["LD_PRELOAD"] = run_shell_cmd("gcc -print-file-name=libasan.so").replace("\n", "")
    os.environ["ASAN_OPTIONS"] = "detect_leaks=0:disable_coredump=0:unmap_shadow_on_exit=1:abort_on_error=1"
    ld_preload = run_shell_cmd("gcc -print-file-name=libasan.so").replace("\n", "")
    # Add ASAN compiler and linker arguments
    extra_link_args.append("-fsanitize=address")
    extra_compile_args.append("-fsanitize=address")
    assert ld_preload == os.environ["LD_PRELOAD"]
else:
    print("YDBPython: YottaDB was NOT compiled with address sanitization (ASAN). Compiling YDBPython WITHOUT ASAN.")


create_constants_from_header_file()


setup(
    name="yottadb",
    version="2.0.0",
    description="A Pythonic API for accessing YottaDB databases.",
    long_description=(pathlib.Path(__file__).parent / "README.md").read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    url="https://yottadb.com",
    project_urls={
        "Documentation": "https://docs.yottadb.com/MultiLangProgGuide/pythonprogram.html",
        "Source Code": "https://gitlab.com/YottaDB/Lang/YDBPython",
        "Issue Tracker": "https://gitlab.com/YottaDB/Lang/YDBPython/-/issues",
    },
    author="YottaDB LLC",
    author_email="info@yottadb.com",
    license="GNU Affero General Public License v3",
    classifiers=[
        "License :: OSI Approved :: GNU Affero General Public License v3",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
    ],
    ext_modules=[
        Extension(
            "_yottadb",
            sources=["_yottadb.c"],
            include_dirs=[YDB_DIST],
            library_dirs=[YDB_DIST],
            undef_macros=["NDEBUG"],  # Comment out to disable asserts if a Release build is desired
            extra_link_args=extra_link_args,
            extra_compile_args=extra_compile_args,
        )
    ],
    py_modules=["yottadb"],
    packages=find_packages(include=["_yottadb", "_yottadb.*", "yottadb"]),
    package_data={"": ["_yottadb.pyi"]},
    include_package_data=True,
    setup_requires=["pytest-runner"],
    tests_require=["pytest"],
    test_suite="test",
)
