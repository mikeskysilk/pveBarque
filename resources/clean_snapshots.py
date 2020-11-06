# pvebarque/resources/clean_snapshots.py
from flask import request
from flask_restful import Resource
from common import REDIS

class CleanSnapshots(Resource):

    def post(self):
        response = []
        if 'deep' in request.args:
            REDIS.hset(0, 'job', 'deepscrub')
            REDIS.hset(0, 'state', 'enqueued')
            REDIS.sadd('joblock', 0)
            return {'Status': "Deep scrub in progress"}, 200
        for vmid in REDIS.smembers('joblock'):
            status = REDIS.hget(vmid, 'state')
            msg = REDIS.hget(vmid, 'msg')
            job = REDIS.hget(vmid, 'job')  # REMOVE- used for testing scrub functions
            if (status == 'error') and (msg == "error creating backup snapshot"):
                # add to scrub queue
                # if job == 'backup': #REMOVE - used for testing scrub functions
                # 	REDIS.hset(vmid, 'retry', 'backup')
                REDIS.hset(vmid, 'job', 'scrub')
                REDIS.hset(vmid, 'state', 'enqueued')
