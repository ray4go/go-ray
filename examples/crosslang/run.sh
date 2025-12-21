#!/usr/bin/env bash

cd "$(dirname "$0")"

go build -buildmode=c-shared -o ../../output/raytask .
goray --py-defs app.py  ../../output/raytask
