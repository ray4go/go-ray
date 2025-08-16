"""global state"""

import os

from . import consts, utils

actors = utils.ThreadSafeLocalStore()

# value is ray object ref
futures = utils.ThreadSafeLocalStore()

golibpath: str = os.environ.get(consts.GORAY_BIN_PATH_ENV, "")
pymodulepath: str = os.environ.get(consts.GORAY_PY_MUDULE_PATH_ENV, "")

debug = False
