#!/usr/bin/env bash
set -e

if [ -n "$1" ] && [ "$1" != "remote" ] && [ "$1" != "local" ] && [ "$1" != "mock" ]; then
  echo "Usage: $0 [remote|local|mock]"
  exit 1
fi

# 获取当前脚本的绝对路径
SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

go build -buildmode=c-shared -o ${SCRIPT_PATH}/../out/raytask -gcflags="all=-l -N"  .
cd ${SCRIPT_PATH}/..

MODE=$1

# 移除第一个参数
# shift 命令会将参数列表向左移动一个位置
# 此时，$1 会变成原来的 $2，$2 会变成原来的 $3，依此类推。
# 而原来的 $1 将会被丢弃。
# 现在，$@ 包含了所有除了第一个参数之外的其他参数
shift

export RAY_RUNTIME_ENV_IGNORE_GITIGNORE=1

if [ "$MODE" == "remote" ]; then
  export SEC_TOKEN_STRING=$(cat ~/.token)
  ray job submit --working-dir=./ -- python -m goray.cli  out/raytask "$@"
elif [ "$MODE" == "local" ]; then
  python -m goray.cli --mode local out/raytask "$@"
elif [ "$MODE" == "mock" ]; then
  python -m goray.cli --mode mock out/raytask "$@"
fi
