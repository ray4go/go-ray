#!/usr/bin/env bash
# Run goray application, and collect coverage data.> 
# Usage: bash run.sh pkg_path remote|local|build

set -e

if [ -z "$1" ] || [ -z "$2" ]; then
  echo "Usage: $0 pkg_path remote|local|build"
  exit 1
fi

PKG_PATH=$1
MODE=$2

# Remove the first two arguments
shift 2

CUR_DIR=$(pwd)

# Get the absolute path of the current script
PROJ_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export GOCOVERDIR=/tmp/gocover
rm -rf "$GOCOVERDIR"
mkdir -p "$GOCOVERDIR"

cd ${PKG_PATH}
PKG_PATH=$(pwd)  # get absolute path

go build -buildmode=c-shared \
  -cover \
  -covermode=atomic \
  -coverpkg="github.com/ray4go/go-ray/..." \
  -ldflags "-X github.com/ray4go/go-ray/ray/internal/testing.coverageDir=$GOCOVERDIR" \
  -o ${PROJ_PATH}/output/rayapp \
  -gcflags="all=-l -N"  .
# https://go.dev/doc/build-cover

export RAY_RUNTIME_ENV_IGNORE_GITIGNORE=1
cd ${PROJ_PATH}

if [ "$MODE" == "remote" ]; then
  ray job submit --working-dir=./ -- python -m goray.cli --cluster output/rayapp "$@"
elif [ "$MODE" == "local" ]; then
  python -m goray.cli output/rayapp "$@"
fi

cd ${PKG_PATH}

go tool covdata textfmt -i="$GOCOVERDIR" -o=/tmp/cover.out
go tool cover -html=/tmp/cover.out -o=/tmp/cover.html

printf "\nCoverage: \n"
go tool covdata percent -i="$GOCOVERDIR" | grep -Ev 'examples|tests'  # only keep goray sdk code
grep -Ev 'examples|tests' /tmp/cover.out > /tmp/cover-filter.out  # only keep goray sdk code
go tool cover -func=/tmp/cover-filter.out | tail -n 1
echo "Generated coverage report at /tmp/cover.html"
