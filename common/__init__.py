# pveBarque/common/__init__.py
"""
Module for storing several global variables and helper functions accessible by
subprocesses and resources. Uses dicts for hacking pointers into python, such
that other classes are able to reference the same mutable object within a dict.
I'm sure there's a more pythonic way of doing this but this is the best solution
 I could conceive.
"""

import redis
import logging

REDIS = redis.Redis()
CONFIG = {}
LOG = logging.getLogger('Barque')
WORKERS = []

def init_redis(**kwargs):
    REDIS.__init__(**kwargs, decode_responses=True)

def set_config(config):
    for k, v in config.items():
        CONFIG[k] = v

def init_logging(config):
    log_format = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')

    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.DEBUG)
    log_console_handler.setFormatter(log_format)

    log_file_handler = logging.FileHandler(config["log_file"])
    log_file_handler.setLevel(logging.INFO)
    log_file_handler.setFormatter(log_format)

    LOG.setLevel(logging.DEBUG)
    LOG.addHandler(log_console_handler)
    LOG.addHandler(log_file_handler)

    LOG.info('Barque logging initialized.')
