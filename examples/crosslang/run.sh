#!/usr/bin/env bash

cd "$(dirname "$0")"
NO_COVERAGE=1 bash ../../run.sh . local --import=examples/crosslang-remote/app.py
