#!/bin/bash

committed_files=$(git diff --cached --name-only HEAD)
changed_files=$(git diff --name-only | tr "\n" " ")
restore_files=""

git stash -q --keep-index

echo "Checking code format ..."
for file in $committed_files ; do
  formatted=false
  if [[ "$file" == *".c" ]] || [[ "$file" == *".h" ]]; then
    if ! clang-format --dry-run --Werror -style=file "$file" &>/dev/null; then
      echo "    Automatically fixing formatting errors in $file with \"clang-format\"."
      clang-format -i -style=file "$file"
      formatted=true
    fi
  elif [[ "$file" == *".py" ]] || [[ "$file" == *".pyi" ]]; then
    if ! black -q --check "$file"; then
      echo "    Automatically fixing formatting errors in $file with \"black\"."
      black -q "$file"
      formatted=true
    fi
  fi
  if $formatted; then
    git add ${file}
    if ! [[ $changed_files =~ ( |^)$file( |$) ]]; then
      restore_files="$restore_files $file"
    fi
  fi
done

git stash pop -q
if [[ "${restore_files}" != "" ]]; then
  git restore ${restore_files}
fi