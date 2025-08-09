"""global state"""

from . import utils
from . import consts
import os


actors = utils.ThreadSafeLocalStore()

# in mock mode, value is actual return value, i.e. (data, code).
# in other modes, value is ray object ref
futures = utils.ThreadSafeLocalStore()

golibpath: str = os.environ.get(consts.GORAY_BIN_PATH_ENV, "")

debug = False
