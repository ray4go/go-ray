import logging
import os

GORAY_DEBUG_LOGGING = 'GORAY_DEBUG_LOGGING'

def enable_logger(logger, level=logging.DEBUG):
    """Output logging message to sys.stderr"""
    ch = logging.StreamHandler()
    ch.setLevel(level)
    formatter = logging.Formatter('[%(levelname)s %(asctime)s %(module)s:%(lineno)d %(funcName)s] %(message)s',
                                  datefmt='%y%m%d %H:%M:%S')
    ch.setFormatter(formatter)
    logger.handlers = [ch]
    logger.setLevel(level)
    logger.propagate = False


def init_logger(logger):
    if os.getenv(GORAY_DEBUG_LOGGING):
        enable_logger(logger, level=logging.DEBUG)
