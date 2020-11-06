# pvebarque/resources/list.py
import os
import glob
import common.utilities as util
from flask_restful import Resource
from flask import request
from auth import AUTH
from common import CONFIG, REDIS


class ListBackups(Resource):

    @AUTH.login_required
    def get(self, vmid):
        path = None
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
        files = sorted(
            os.path.basename(f) for f in glob.glob(
                "".join([path, "vm-{}-disk*.lz4".format(vmid)])
                )
            )
        return {'backups': files}
