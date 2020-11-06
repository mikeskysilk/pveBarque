# pvebarque/resources/count.py
import os
from flask import request
from flask_restful import Resource
from auth import AUTH
from common import REDIS

class Count(Resource):

    @AUTH.login_required
    def get(self):
        result = {}
        hasArgs = False
        if 'cluster' in request.args:
            result['total'] = self.get_total()
            hasArgs = True
        if 'node' in request.args:
            node = request.args['node']
            if node in os.listdir('/etc/pve/nodes'):
                specificNodes = {}
                specificNodes[node] = REDIS.hget('inventory', node)
                result['nodes'] = specificNodes
            else:
                return {'error': "node does not exist in cluster"}, 400
            hasArgs = True
        if hasArgs:
            return result, 200
        total_count = {}
        if 'detailed' in request.args:
            nodes = {}
            aggregate = {}
            aggregate["lxc"] = 0
            aggregate["vm"] = 0
            aggregate["total"] = 0
            for node in os.listdir('/etc/pve/nodes'):
                stats = {}
                stats["lxc"] = int(REDIS.hget('inv_ct', node))
                stats["vm"] = int(REDIS.hget('inv_vm', node))
                stats["total"] = stats["lxc"] + stats["vm"]
                nodes[node] = stats
                aggregate["lxc"] += stats["lxc"]
                aggregate["vm"] += stats["vm"]
                aggregate["total"] += stats["total"]
            return {"nodes": nodes, "aggregate": aggregate}, 200
        for node in os.listdir('/etc/pve/nodes'):
            try:
                total_count[node] = int(REDIS.hget('inv_ct', node)) + int(REDIS.hget('inv_vm', node))
            except:
                continue
        return {'total': self.get_total(), 'nodes': total_count}, 200

    def get_total(self):
        total = 0
        for node in os.listdir('/etc/pve/nodes'):
            total = total + int(REDIS.hget('inv_ct', node)) + int(REDIS.hget('inv_vm', node))
        return total
