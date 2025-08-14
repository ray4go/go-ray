"""global state"""

import os

from . import consts
from . import utils

actors = utils.ThreadSafeLocalStore()

# in mock mode, value is actual return value, i.e. (data, code).
# in other modes, value is ray object ref
futures = utils.ThreadSafeLocalStore()

golibpath: str = os.environ.get(consts.GORAY_BIN_PATH_ENV, "")
pymodulepath: str = os.environ.get(consts.GORAY_PY_MUDULE_PATH_ENV, "")

debug = False
