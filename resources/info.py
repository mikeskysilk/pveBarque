# pvebarque/resources/info.py
import datetime
import os
from flask import request
from flask_restful import Resource
from auth import AUTH
from common import CONFIG
from common import REDIS



class Info(Resource):
    # TODO: implement worker status

    def __init__(self, **kwargs):
        # self.redis = kwargs['redis'].get
        # self.cfg = kwargs['config'].get_config
        self.version = kwargs['version']
        self.start_time = kwargs['start_time']

    @AUTH.login_required
    def get(self):
        response = {}
        barque_health = 'OK'
        response['version'] = self.version
        now = datetime.datetime.utcnow().replace(microsecond=0)
        uptime = now - self.start_time
        response['uptime'] = str(uptime)
        worker_task = {}

        # queue status
        set_size = REDIS.scard('joblock')
        active = 0
        errors = 0
        for member in REDIS.smembers('joblock'):
            if REDIS.hget(member, 'state') == "active":
                active += 1
                worker_task[REDIS.hget(member, 'worker')] = {
                    '1': REDIS.hget(member, 'job'),
                    '2': REDIS.hget(member, 'msg'),
                    '3': member}
            if REDIS.hget(member, 'state') == "error":
                errors += 1
                if not barque_health == 'CRITICAL':
                    barque_health = 'WARNING'
        response['Queue'] = {'jobs in queue': set_size,
                             'active': active,
                             'errors': errors}

        # # worker status
        # worker_status = {}
        # for worker in workers:
        #     if worker.is_alive():
        #         healthy = "Alive"
        #     else:
        #         healthy = "Dead"
        #         barqueHealth = 'CRITICAL'
        #     try:
        #         task = workerTask[str(worker.pid)]['1']
        #     except:
        #         task = ""
        #     try:
        #         message = workerTask[str(worker.pid)]['2']
        #     except:
        #         message = ""
        #     try:
        #         container = workerTask[str(worker.pid)]['3']
        #     except:
        #         container = ""
        #     worker_status[worker.pid
        #     ] = {'Health': healthy, 'Job': task, 'Message': message, 'CTID': container}
        # response['workers'] = worker_status
        # print(worker_task)

        # destination status
        dests = {}
        local = CONFIG['local']
        for key, location in CONFIG['regions'][local]['storage'].items():
            if os.path.isdir(location):
                healthy = "OK"
            else:
                healthy = "Down"
                barque_health = 'CRITICAL'
            dests[key] = healthy
        response['Storage'] = dests
        response['Health'] = barque_health
        return response
