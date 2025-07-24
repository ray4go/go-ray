#!/usr/bin/env bash

set -ex

WD=$(pwd)
# 获取当前脚本的绝对路径
SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"


cd ${SCRIPT_PATH}/../codegen
go install .
goraygen $WD