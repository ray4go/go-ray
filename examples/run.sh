#!/usr/bin/env bash
set -e

if [ -n "$1" ] && [ "$1" != "remote" ] && [ "$1" != "local" ] && [ "$1" != "mock" ] && [ "$1" != "debug" ]; then
  echo "Usage: $0 [remote|local|mock]"
  exit 1
fi

# 获取当前脚本的绝对路径
SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

go build -buildmode=c-shared -o ${SCRIPT_PATH}/../out/raytask -gcflags="all=-l -N"  .
cd ${SCRIPT_PATH}/..

if [ "$1" == "remote" ]; then
  export SEC_TOKEN_STRING=$(cat ~/.token)
  ray job submit --working-dir=./ -- python -m pydriver.main  out/raytask
elif [ "$1" == "local" ]; then
  python -m pydriver.main --mode local out/raytask
elif [ "$1" == "mock" ]; then
  python -m pydriver.main --mode mock out/raytask
else
  # 使用 delve exec 来以调试模式启动您的应用
  # --headless: 以无头模式运行，不在终端中启动 delve 自己的交互界面
  # --listen=:2345: 在 2345 端口上监听调试器的连接。您可以选择任何未被占用的端口
  # --api-version=2: 必须指定的 API 版本
  # --accept-multiclient: (可选，但推荐) 允许多客户端连接，方便重连
  python -m pydriver.main --mode debug out/raytask & # 在后台运行命令
  lastpid=$!      # $! 是当前 Shell 中最后一个在后台运行的命令的 PID
  echo "Debugging process started with PID: $lastpid"
  sleep 1
  go/1.21.13/bin/dlv attach $lastpid --headless --listen=:2345 --api-version=2 --accept-multiclient
fi
