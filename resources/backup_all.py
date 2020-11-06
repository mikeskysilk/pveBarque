# pvebarque/resources/backup_all.py
import os
from flask_restful import Resource
from flask import request
import common.utilities as util
from common import CONFIG, REDIS
from auth import AUTH


class BackupAll(Resource):

    @AUTH.login_required
    def post(self):
        targets = []
        dest = ""
        # handle destination setting
        if 'dest' in request.args:
            result, response, err = util.checkDest(request.args['dest'])
            if result:
                dest = CONFIG['regions'][CONFIG['local']]['storage'][request.args['dest']]
            else:
                return response, err
        else:
            result, response, err = util.checkDest('default')
            if result:
                dest = CONFIG['regions'][CONFIG['local']]['storage']['default']
            else:
                return response, err
        response = []
        for _, _, files in os.walk('/etc/pve/nodes'):
            for f in files:
                if f.endswith(".conf"):
                    targets.append(f.split(".")[0])
        for vmid in targets:
            if str(vmid) in REDIS.smembers('joblock'):
                response.append({vmid: {"status": "error", "message": "CTID locked, another operation is in progress"}})
            else:
                REDIS.hset(vmid, 'job', 'backup')
                REDIS.hset(vmid, 'file', '')
                REDIS.hset(vmid, 'worker', '')
                REDIS.hset(vmid, 'msg', '')
                REDIS.hset(vmid, 'state', 'enqueued')
                REDIS.hset(vmid, 'dest', dest)
                REDIS.sadd('joblock', vmid)
                response.append({vmid: {"status": "enqueued", "message": "Backup job added to queue"}})
        return response
