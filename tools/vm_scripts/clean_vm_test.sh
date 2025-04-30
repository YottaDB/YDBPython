#!/bin/bash
#################################################################
#                                                               #
# Copyright (c) 2020-2021 Peter Goss All rights reserved.       #
#                                                               #
# Copyright (c) 2020-2025 YottaDB LLC and/or its subsidiaries.  #
# All rights reserved.                                          #
#                                                               #
#   This source code contains the intellectual property         #
#   of its copyright holder(s), and is made available           #
#   under a license.  If you do not know the terms of           #
#   the license, please stop and do not read further.           #
#                                                               #
#################################################################

# This is a utility script intended for manual use by developers when setting up a virtual machine for testing YDBPython.
# The script is to be run from within a guest VM, and will install YottaDB and YDBPython, and then run the YDBPython test suite.
# This script is not intended for use in automated testing.
# See also the following GitLab comment (https://gitlab.com/YottaDB/Lang/YDBPython/-/merge_requests/50#note_1312085723):
# "I've never used this script myself, so I can't really say. It was originally contributed by @gossrock who I think found it useful in his development workflow, though I'm not sure about the details of his setup."

set -e # Fail script if any command fails
set -u # Enable detection of uninitialized variables
set -o pipefail	# this way $? is set to zero only if ALL commands in a pipeline succeed. Else only last command determines $?

# Notes
  # * tested under CentOS 7 and 8 and Ubuntu 18.04 and 20.04
  # * Ubuntu 18.04 and 20.04: install ssh
  # * CentOS 7: after installation use `nmtui` to activate network connection.
  # * scp to freshly installed VM
  # * On Ubuntu unattended upgrades may cause this script to fail if they are running when this script tries to use 'apt'
  #   * wait for them to finish or disable them
  #
  # Usage:
  # bash clean_vm_test.sh [optional URL to gitlab repository]

repository=$1
if [ "$repository" == "" ]; then
  repository="https://gitlab.com/YottaDB/Lang/YDBPython.git"
fi
OS=$(awk -F= '$1=="ID" { print $2 ;}' /etc/os-release)
VERSION=$(awk -F= '$1=="VERSION_ID" { print $2 ;}' /etc/os-release)
OS_LIKE=$(awk -F= '$1=="ID_LIKE" { print $2 ;}' /etc/os-release)
echo $repository
echo $OS
echo $VERSION
echo $OS_LIKE

if [ "$OS_LIKE" == "debian" ]; then
  # update system
  sudo apt update
  sudo apt upgrade -y
  # install necessary packages
    # packages needed for YottaDB Installation are:
      # Ubuntu 18.04: binutils pkg-config
      # Ubuntu 20.04: binutils pkg-config libtinfo5
    # packages need for YDBPython compilation are:
      # Ubuntu 18.04: python3-dev libffi-dev python3-setuptools
      # Ubuntu 20.04: python3-dev libffi-dev gcc
    # packages needed for testing are:
      # Ubuntu 18.04 and 20.04: python3-pip
  sudo apt install -y binutils pkg-config libtinfo5 python3-dev libffi-dev python3-setuptools python3-dev libffi-dev gcc python3-pip
elif [ "$OS_LIKE" == "\"rhel fedora\"" ]; then
  # update system
  sudo yum check-update
  sudo yum update -y
  # install necessary packages
    # packages needed for YottaDB Installation are:
      # CentOS 7 and 8: wget
    # packages needed for getting YDBPython source code:
      # CentOS 7 and 8: git
    # packages need for YDBPython compilation are:
      # CentOS 7 and 8: python3 gcc python3-devel libffi-devel
    # packages needed for testing are:
      # CentOS 8:  python3-pip
  sudo yum install -y wget git python3 gcc python3-devel libffi-devel
fi

# install YottaDB
mkdir /tmp/tmp ; cd /tmp/tmp
wget https://gitlab.com/YottaDB/DB/YDB/raw/master/sr_unix/ydbinstall.sh
chmod +x ydbinstall.sh
if [ "$OS" == "\"centos\"" ] && [ "$VERSION" == "\"7\"" ]; then
  sudo ./ydbinstall.sh --force-install --utf8 --verbose
else
  sudo ./ydbinstall.sh --utf8 --verbose
fi
source $(pkg-config --variable=prefix yottadb)/ydb_env_set

# get YDBPython code
cd ~
git clone $repository

# install YDBPython
cd YDBPython
python3 -m pip install --user .
python3 -m pip install --user pytest psutil
python3 -m pytest tests/
