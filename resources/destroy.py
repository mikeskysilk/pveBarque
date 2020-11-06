# pvebarque/resources/destroy.py
import common.utilities as util
from flask import request
from flask_restful import Resource
from auth import AUTH
from common import CONFIG, REDIS


class Destroy(Resource):

    @AUTH.login_required
    def post(self, vmid):
        # catch if container does not exist
        if not util.checkConf(vmid):
            return {'error': "{} is not a valid CTID".format(vmid)}, 400
        # clear error and try again if retry flag in request args
        if 'retry' in request.args and REDIS.hget(vmid, 'state') == 'error':
            REDIS.srem('joblock', vmid)
            # log.debug("Clearing error state")
        # Check if container is available
        if str(vmid) in REDIS.smembers('joblock'):
            return {'error': "VMID locked, another operation is in progress "
                    "for container: {}".format(vmid)}, 409

        REDIS.hset(vmid, 'job', 'destroy')
        REDIS.hset(vmid, 'file', '')
        REDIS.hset(vmid, 'worker', '')
        REDIS.hset(vmid, 'msg', 'destroying')
        REDIS.hset(vmid, 'target_cluster', '')
        REDIS.hset(vmid, 'state', 'enqueued')
        REDIS.sadd('joblock', vmid)

        return {'status': "Deletion requested for {}".format(vmid)}, 202
