import os

# 临时方式传递so filepath， ugly
golibpath: str = os.environ.get("GORAY_BIN_PATH", "")



