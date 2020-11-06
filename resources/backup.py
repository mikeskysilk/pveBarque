# pvebarque/resources/backup.py
# pveBarque/resources/backup.py
from flask import request
from flask_restful import Resource
import common.utilities as util
from auth import AUTH
from common import REDIS
from common import CONFIG
from common import LOG


class Backup(Resource):

    @AUTH.login_required
    def post(self, vmid):
        LOG.debug("Backup:POST with args {}".format(request.args))
        dest = ""
        if not util.checkConf(vmid):
            return {'error': "{} is not a valid VMID".format(vmid)}, 400
        # clear error and try again if retry flag in request args
        if 'retry' in request.args and REDIS.hget(vmid, 'state') == 'error':
            REDIS.srem('joblock', vmid)
            # log.debug("Clearing error state for ctid: {}".format(vmid))
        # handle destination setting
        if 'dest' in request.args:
            result, response, err = util.checkDest(request.args['dest'])
            if result:
                dest = CONFIG['regions'][CONFIG['local']]['storage'][request.args['dest']]
            else:
                LOG.warning("Backup:POST:checkDest({}) got error {}".format(request.args['dest'], response))
                return response, err
        else:
            result, response, err = util.checkDest('default')
            if result:
                dest = CONFIG['regions'][CONFIG['local']]['storage']['default']
            else:
                LOG.warning("Backup:POST:checkDest(default) got error {}".format(response))
                return response, err

        if str(vmid) in REDIS.smembers('joblock'):
            return {'error': "VMID locked, another operation is in progress for\
             container: {}".format(vmid),
                    'status': REDIS.hget(vmid, 'state'),
                    'job': REDIS.hget(vmid, 'job')}, 409
            LOG.info("Backup:POST:check_lock VMID in use")
        REDIS.hset(vmid, 'job', "backup")
        REDIS.hset(vmid, 'dest', dest)
        REDIS.hset(vmid, 'state', "enqueued")
        REDIS.hset(vmid, 'msg', "")
        REDIS.sadd('joblock', vmid)
        LOG.info("Backup:POST:success for {} with destination {}".format(vmid, request.args['dest']))
        return {'status': "backup job created for VMID {}".format(vmid)}, 202
