# pvebarque/resources/delete.py
import os
import common.utilities as util
from flask_restful import Resource
from flask import request
from common import CONFIG, REDIS
from auth import AUTH


class DeleteBackup(Resource):

    @AUTH.login_required
    def post(self, vmid):
        path = None
        if str(vmid) in REDIS.smembers('joblock'):
            return {'error': 'CTID locked, another operation is in progress for container {}'.format(vmid)}, 409
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
        if 'file' in request.args:
            if not request.args['file'].split('-')[1] == str(vmid):
                return {'error': 'File name does not match VMID'}, 400
            REDIS.sadd('joblock', vmid)
            print(request.args['file'])
            fileimg = "".join([path, request.args['file']])
            fileconf = "".join([os.path.splitext(fileimg)[0], ".conf"])
            if os.path.isfile(fileimg):
                os.remove(fileimg)
                if os.path.isfile(fileconf):
                    os.remove(fileconf)
                REDIS.srem('joblock', vmid)
                return {'file removed': os.path.basename(fileimg)}
            else:
                REDIS.srem('joblock', vmid)
                return {'file does not exist': os.path.basename(fileimg)}, 400
        else:
            return {'error': "resource requires a file argument"}, 400
