"""global state"""

import os

from gorayffi import utils
from . import consts

# values are raycore.go2py.PyActorWrapper / x.actor.GoActorWrapper
actors = utils.ThreadSafeLocalStore()

# value is ray object ref
futures = utils.ThreadSafeLocalStore()

golibpath: str = os.environ.get(consts.GORAY_BIN_PATH_ENV, "")
pymodulepath: str = os.environ.get(consts.GORAY_PY_MUDULE_PATH_ENV, "")

debug = False
