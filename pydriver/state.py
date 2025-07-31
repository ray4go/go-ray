from . import utils

actors = utils.ThreadSafeLocalStore()

# in mock mode, value is actual return value, i.e. (data, code).
# in other modes, value is ray object ref
futures = utils.ThreadSafeLocalStore()
