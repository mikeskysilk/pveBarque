# pvebarque/resources/poison.py
from flask import request
from flask_restful import Resource
from auth import AUTH
from common import REDIS

class Poison(Resource):

    @AUTH.login_required
    def post(self, vmid):
        # catch if container does not exist
        if str(vmid) in REDIS.smembers('joblock'):
            REDIS.hset(vmid, 'job', 'poisoned')
            if REDIS.hget(vmid, 'state') == 'error':
                REDIS.hset(vmid, 'state', 'enqueued')
            return {'status': "Attempting to cancel task or error for container: {}".format(vmid)}, 200
        else:
            return {'status': "Container not in job queue, nothing to do"}, 200
