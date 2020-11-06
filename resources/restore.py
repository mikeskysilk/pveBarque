# pvebarque/resources/restore.py
from flask_restful import Resource
from flask import request
import os
import common.utilities as util
from auth import AUTH
from common import REDIS
from common import CONFIG


class Restore(Resource):

    @AUTH.login_required
    def post(self, vmid):
        path = None
        # handle destination setting
        if 'dest' in request.args:
            result, response, err = util.checkDest(request.args['dest'])
            if result:
                path = CONFIG['regions'][CONFIG['local']]['storage'][request.args['dest']]
            else:
                return response, err
        else:
            result, response, err = util.checkDest('default')
            if result:
                path = CONFIG['regions'][CONFIG['local']]['storage']['default']
            else:
                return response, err
        # check if file specified
        if 'file' not in request.args:
            return {'error': 'Resource requires a file argument'}, 400
        filename = os.path.splitext(request.args['file'])[0]
        if not filename.split('-')[1] == str(vmid):
            return {'error': 'File name does not match VMID'}, 400
        fileimg = "".join([path, filename, ".lz4"])
        fileconf = "".join([path, filename, ".conf"])

        # catch if container does not exist
        if not util.checkConf(vmid):
            return {'error': "{} is not a valid CTID"}, 400
        # check if backup and config files exist
        if not os.path.isfile(fileimg) and not os.path.isfile(fileconf):
            return {'error': "unable to proceed, backup file or config file (or both) does not exist"}, 400
            # clear error and try again if retry flag in request args
        if 'retry' in request.args and REDIS.hget(vmid, 'state') == 'error':
            REDIS.srem('joblock', vmid)
            # log.debug("Clearing error state for ctid: {}".format(vmid))
        if str(vmid) in REDIS.smembers('joblock'):
            return {'error': "VMID locked, another operation is in progress for container: {}".format(vmid)}, 409

        REDIS.hset(vmid, 'job', 'restore')
        REDIS.hset(vmid, 'file', filename)
        REDIS.hset(vmid, 'worker', '')
        REDIS.hset(vmid, 'msg', '')
        REDIS.hset(vmid, 'dest', path)
        REDIS.hset(vmid, 'state', 'enqueued')
        REDIS.sadd('joblock', vmid)

        return {'status': "restore job created for VMID {}".format(vmid)}, 202
