# pvebarque/resources/clear_queue.py
from flask_restful import Resource
from flask import request
from common import REDIS

class ClearQueue(Resource):

    def post(self):
        response = []
        for vmid in REDIS.smembers('joblock'):
            status = REDIS.hget(vmid, 'state')
            if status == 'enqueued':
                REDIS.srem('joblock', vmid)
                REDIS.hset(vmid, 'state', 'OK')
                response.append({vmid: {"status": "OK", "message": "Successfully dequeued"}})
            else:
                msg = REDIS.hget(vmid, 'msg')
                job = REDIS.hget(vmid, 'job')
                file = REDIS.hget(vmid, 'file')
                response.append({vmid: {'status': status, 'message': msg, 'job': job, 'file': file}})
        return response
