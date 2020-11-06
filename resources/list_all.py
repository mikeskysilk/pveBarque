# pvebarque/resources/list_all.py
import os
import common.utilities as util
from flask_restful import Resource
from flask import request
from auth import AUTH
from common import CONFIG, REDIS

class ListAllBackups(Resource):

    @AUTH.login_required
    def get(self):
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
        images = []
        confs = []
        for _, _, files in os.walk(path):
            for f in files:
                if f.endswith('.lz4'):
                    images.append(f)
                elif f.endswith('.conf'):
                    confs.append(f)
        return {'backup files': images, 'config files': confs}
