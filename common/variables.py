# pvebarque/common/variables.py
"""
Container for several global variables accessible by subprocesses and
resources. Uses dicts for hacking pointers into python, such that other classes
are able to reference the same mutable object within a dict. I'm sure there's a
more pythonic way of doing this but this is the best solution I could conceive.
"""

# import redis


# class BarqueConfig(object):
#     """ Mutable class object to store configuration data """
#     def __init__(self):
#         self._config = {}
#
#     def set_config(self, config):
#         self._config['config'] = config
#
#     @property
#     def get_config(self):
#         return self._config['config']
#
#
# class BarqueRedisWrapper(object):
#     """ Mutable class object to store redis connection object """
#
#     def __init__(self):
#         self._r = {'redis': redis.Redis()}
#
#     def set_redis(self, **kwargs):
#         self._r['redis'] = redis.Redis(**kwargs)
#
#     @property
#     def get(self):
#         return self._r['redis']
