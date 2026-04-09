#!/bin/sh

set -eu

unformatted="$(gofmt -l .)"

if [ -n "${unformatted}" ]; then
  echo "gofmt found unformatted files:"
  echo "${unformatted}"
  exit 1
fi
