# pvebarque/resources/migrate.py
import common.utilities as util
from flask import request
from flask_restful import Resource
from auth import AUTH
from common import CONFIG, REDIS


class Migrate(Resource):

    @AUTH.login_required
    def post(self, vmid):
        # Check if file is specified
        if 'file' not in request.args:
            return {'error': 'Resource requires a file argument'}, 400
        # Check if destination container does not exist
        if not util.checkConf(vmid):
            return {'error': "{} is not a valid CTID"}, 400
        # Check if cluster is specified
        if 'cluster' not in request.args:
            return {'error': 'Resource requires a cluster argument {}'.format(CONFIG['regions'].keys())}, 400
        # clear error and try again if retry flag in request args
        if 'retry' in request.args and REDIS.hget(vmid, 'state') == 'error':
            REDIS.srem('joblock', vmid)
            # log.debug("Clearing error state for ctid: {}".format(vmid))
        # Check if container is available
        if str(vmid) in REDIS.smembers('joblock'):
            return {'error': "VMID locked, another operation is in progress for container: {}".format(vmid)}, 409

        # handle destination setting
        path = ""
        result, _, _ = util.checkDest('default')
        if result:
            path = CONFIG['regions'][CONFIG['local']]['storage']['default']
        else:
            return {'error': "Error setting destination"}, 400

        #determine remote host IP

        #determine remote host path

        # get info of target barque node
        # target_ip = ''
        # target_region, target_cluster_id, blank = re.split('(\d+)',request.args['cluster'].lower())
        # if target_region in barque_ips:
        #     if target_cluster_id in barque_ips[target_region]:
        #         target_ip = barque_ips[target_region][target_cluster_id]
        #     else:
        #         REDIS.hset(vmid, 'state','error')
        #         REDIS.hset(vmid,'msg','cluster number not configured')
        # else:
        #     REDIS.hset(vmid, 'state', 'error')
        #     REDIS.hset(vmid, 'msg', 'Cluster region not configured')

        try:
            target_path = CONFIG['regions'][request.args['cluster'].lower()]['storage'][request.args['dest']]
        except:
            REDIS.hset(vmid, 'state', 'error')
            REDIS.hset(vmid, 'msg', 'Cluster region not configured')
            return {'error': "target host not found"}, 400

        try:
            target_ip = CONFIG['regions'][request.args['cluster'].lower()]['host']
        except:
            REDIS.hset(vmid, 'state', 'error')
            REDIS.hset(vmid, 'msg', 'Cluster region not configured')
            return {'error': "target host not found"}, 400


        REDIS.hset(vmid, 'dest', path)
        REDIS.hset(vmid, 'target_ip', target_ip)
        REDIS.hset(vmid, 'target_path', target_path)
        REDIS.hset(vmid, 'job', 'migrate')
        REDIS.hset(vmid, 'file', request.args['file'])
        REDIS.hset(vmid, 'worker', '')
        REDIS.hset(vmid, 'msg', '')
        REDIS.hset(vmid, 'target_cluster', request.args['cluster'])
        REDIS.hset(vmid, 'state', 'enqueued')
        REDIS.sadd('joblock', vmid)

        return {'status': "migrate job created for CTID {}".format(vmid)}, 202
