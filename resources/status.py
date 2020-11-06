# pvebarque/resources/status.py
from flask_restful import Resource
from flask import request
import common.utilities as util
from common import REDIS

class Status(Resource):

    def get(self, vmid):
        # catch if container does not exist
        if not util.checkConf(vmid):
            return {'error': "{} is not a valid CTID"}, 400
        status = REDIS.hget(vmid, 'state')
        msg = REDIS.hget(vmid, 'msg')
        job = REDIS.hget(vmid, 'job')
        file = REDIS.hget(vmid, 'file')
        print(status, msg, job, file)
        return {vmid: {'status': status, 'message': msg, 'job': job, 'file': file}}, 200


class AllStatus(Resource):

    def get(self):
        response = []
        for vmid in REDIS.smembers('joblock'):
            status = REDIS.hget(vmid, 'state')
            msg = REDIS.hget(vmid, 'msg')
            job = REDIS.hget(vmid, 'job')
            file = REDIS.hget(vmid, 'file')
            response.append({vmid: {'status': status, 'message': msg, 'job': job, 'file': file}})
        return response
