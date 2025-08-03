"""global state"""

from . import utils
import os


actors = utils.ThreadSafeLocalStore()

# in mock mode, value is actual return value, i.e. (data, code).
# in other modes, value is ray object ref
futures = utils.ThreadSafeLocalStore()

# 临时方式传递so filepath， ugly
golibpath: str = os.environ.get("GORAY_BIN_PATH", "")

debug = False
