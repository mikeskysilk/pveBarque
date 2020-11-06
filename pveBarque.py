#!/usr/bin/python2.7
import configparser
import os
import redis
import subprocess
import logging
import barqueForeman
import barqueWorker
import re
from proxmoxer import ProxmoxAPI
from logging import handlers
from datetime import datetime
from glob import glob
from flask import Flask, request
from flask_httpauth import HTTPBasicAuth
from flask_restful import Resource, Api, reqparse

config = configparser.ConfigParser()
config.read('barque.conf')
# configs
_host = config['flask']['host']  # ip address for API to bind to
_port = int(config['flask']['port'])  # port for API to bind to
cert = config['flask']['cert']  # Location of cert.pem file for SSL
key = config['flask']['key']  # location of key.pem file for SSL
uname = config['flask']['username']  # HTTP Basic Auth username
upass = config['flask']['password']  # HTTP Basic Auth password
path = config['settings']['path']  # Destination path for backups,
                                    # terminating / required
pool = config['settings']['pool']   # Ceph RBD pool, terminating / required.
                                    # Leave empty for default
local_cluster = config['settings']['local']
minions = int(config['settings']['workers'])  # number of worker processes
r_host = config['redis']['host']  # Redis server host
r_port = int(config['redis']['port'])  # Redis server port
r_pw = config['redis']['password']  # Redis server password
barque_storage = {}
barque_ips = {}
for option in config.options('barque_storage'):
    barque_storage[option] = {}
    conf_locations = config.get('barque_storage', option).split(',')
    for location in conf_locations:
        barque_storage[option][location.split(':')[0]] = location.split(':')[1]
for item in config['barque_ips'].items():
    r, c = item[0].split('.')
    v = item[1]
    if r not in barque_ips:
        barque_ips[r] = {}
    barque_ips[r][c] = v
# pmx_clusters={}
# for item in config['cluster'].items():
#     r,c,n = item[0].split('.')
#     v = item[1]
#     if r not in pmx_clusters:
#         pmx_clusters[r] = {}
#     if c not in pmx_clusters[r]:
#         pmx_clusters[r][c] = {}
#     pmx_clusters[r][c][n]=v
_password = config['proxmox']['password']
version = '0.12.13'
starttime = None

# global vars
app = Flask(__name__)
api = Api(app)
auth = HTTPBasicAuth()
r = None
workers = []
admin_auth = {uname: upass}
parser = reqparse.RequestParser()
parser.add_argument('file', 'vmid')

# Logger
log = logging.getLogger('Barque')
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
fh = logging.handlers.WatchedFileHandler('/opt/barque/barque.log')
fh.setLevel(logging.INFO)
fm = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')
ch.setFormatter(fm)
fh.setFormatter(fm)
log.addHandler(ch)
log.addHandler(fh)
log.info('Barque Logging Initialized.')

proxmox = ProxmoxAPI(_host, user='root@pam', password=_password,
                     verify_ssl=False)


class Backupv2(Resource):
    @auth.login_required
    def post(self, vmid):
        dest = ""
        if not checkConfv2(vmid):
            return {'error': "{} is not a valid VMID".format(vmid)}, 400
        # clear error and try again if retry flag in request args
        if 'retry' in request.args and r.hget(vmid, 'state') == 'error':
            r.srem('joblock', vmid)
            log.debug("Clearing error state for ctid: {}".format(vmid))
        # handle destination setting
        if 'dest' in request.args:
            result, response, err = checkDest(request.args['dest'])
            if result:
                dest = barque_storage[local_cluster][request.args['dest']]
            else:
                return response, err
        else:
            result, response, err = checkDest('default')
            if result:
                dest = barque_storage[local_cluster]['default']
            else:
                return response, err

        if str(vmid) in r.smembers('joblock'):
            return {'error': "VMID locked, another operation is in progress for container: {}".format(vmid),
                    'status': r.hget(vmid, 'state'), 'job': r.hget(vmid, 'job')}, 409
        r.hset(vmid, 'job', 'backup')
        r.hset(vmid, 'dest', dest)
        r.hset(vmid, 'state', 'enqueued')
        r.hset(vmid, 'msg', '')
        r.sadd('joblock', vmid)
        return {'status': "backup job created for VMID {}".format(vmid)}, 202

class BackupAll(Resource):
    @auth.login_required
    def post(self):
        targets = []
        dest = ""
        # handle destination setting
        if 'dest' in request.args:
            result, response, err = checkDest(request.args['dest'])
            if result:
                dest = barque_storage[local_cluster][request.args['dest']]
            else:
                return response, err
        else:
            result, response, err = checkDest('default')
            if result:
                dest = barque_storage[local_cluster]['default']
            else:
                return response, err
        response = []
        for paths, dirs, files in os.walk('/etc/pve/nodes'):
            for f in files:
                if f.endswith(".conf"):
                    targets.append(f.split(".")[0])
        for vmid in targets:
            if str(vmid) in r.smembers('joblock'):
                response.append({vmid: {"status": "error", "message": "CTID locked, another operation is in progress"}})
            elif not checkConfv2(vmid):
            	continue
            else:
                r.hset(vmid, 'job', 'backup')
                r.hset(vmid, 'file', '')
                r.hset(vmid, 'worker', '')
                r.hset(vmid, 'msg', '')
                r.hset(vmid, 'state', 'enqueued')
                r.hset(vmid, 'dest', dest)
                r.sadd('joblock', vmid)
                response.append({vmid: {"status": "enqueued", "message": "Backup job added to queue"}})
        return response

class Restorev2(Resource):
    @auth.login_required
    def post(self, vmid):
        path = None
        # handle destination setting
        if 'dest' in request.args:
            result, response, err = checkDest(request.args['dest'])
            if result:
                path = barque_storage[local_cluster][request.args['dest']]
            else:
                return response, err
        else:
            result, response, err = checkDest('default')
            if result:
                path = barque_storage[local_cluster]['default']
            else:
                return response, err
        # check if file specified
        if 'file' not in request.args:
            return {'error': 'Resource requires a file argument'}, 400
        filename = os.path.splitext(request.args['file'])[0]
        if not filename.split('-')[1] == str(vmid):
            return {'error': 'File name does not match VMID'}, 400
        fileimg = "".join([path, filename, ".lz4"])
        fileconf = "".join([path, filename, ".conf"])

        # catch if container does not exist
        if not checkConfv2(vmid):
            return {'error': "{} is not a valid CTID"}, 400
        # check if backup and config files exist
        if not os.path.isfile(fileimg) and not os.path.isfile(fileconf):
            return {'error': "unable to proceed, backup file or config file (or both) does not exist"}, 400
            # clear error and try again if retry flag in request args
        if 'retry' in request.args and r.hget(vmid, 'state') == 'error':
            r.srem('joblock', vmid)
            log.debug("Clearing error state for ctid: {}".format(vmid))
        if str(vmid) in r.smembers('joblock'):
            return {'error': "VMID locked, another operation is in progress for container: {}".format(vmid)}, 409

        r.hset(vmid, 'job', 'restore')
        r.hset(vmid, 'file', filename)
        r.hset(vmid, 'worker', '')
        r.hset(vmid, 'msg', '')
        r.hset(vmid, 'dest', path)
        r.hset(vmid, 'state', 'enqueued')
        r.sadd('joblock', vmid)

        return {'status': "restore job created for VMID {}".format(vmid)}, 202


class ListAllBackups(Resource):
    @auth.login_required
    def get(self):
        path = None

        # handle destination setting
        if 'dest' in request.args:
            result, response, err = checkDest(request.args['dest'])
            if result:
                path = barque_storage[local_cluster][request.args['dest']]
            else:
                return response, err
        else:
            result, response, err = checkDest('default')
            if result:
                path = barque_storage[local_cluster]['default']
            else:
                return response, err
        images = []
        confs = []
        for paths, dirs, files in os.walk(path):
            for f in files:
                if f.endswith('.lz4'):
                    images.append(f)
                elif f.endswith('.conf'):
                    confs.append(f)
        return {'backup files': images, 'config files': confs}


class ListBackups(Resource):
    @auth.login_required
    def get(self, vmid):
        path = None
        if 'dest' in request.args:
            result, response, err = checkDest(request.args['dest'])
            if result:
                path = barque_storage[local_cluster][request.args['dest']]
            else:
                return response, err
        else:
            result, response, err = checkDest('default')
            if result:
                path = barque_storage[local_cluster]['default']
            else:
                return response, err
        files = sorted(os.path.basename(f) for f in glob("".join([path, "vm-{}-disk*.lz4".format(vmid)])))
        return {'backups': files}

class ListBackupsv2(Resource):
    @auth.login_required
    def get(self, vmid):
        path = None
        if 'dest' in request.args:
            result, response, err = checkDest(request.args['dest'])
            if result:
                path = barque_storage[local_cluster][request.args['dest']]
            else:
                return response, err
        else:
            result, response, err = checkDest('default')
            if result:
                path = barque_storage[local_cluster]['default']
            else:
                return response, err
        files = sorted(os.path.basename(f) for f in glob("".join([path, "vm-{}-disk*.lz4".format(vmid)])))
        return {'backups': files}

class DeleteBackup(Resource):
    @auth.login_required
    def post(self, vmid):
        path = None
        if str(vmid) in r.smembers('joblock'):
            return {'error': 'CTID locked, another operation is in progress for container {}'.format(vmid)}, 409
        if 'dest' in request.args:
            result, response, err = checkDest(request.args['dest'])
            if result:
                path = barque_storage[local_cluster][request.args['dest']]
            else:
                return response, err
        else:
            result, response, err = checkDest('default')
            if result:
                path = barque_storage[local_cluster]['default']
            else:
                return response, err
        if 'file' in request.args:
            if not request.args['file'].split('-')[1] == str(vmid):
                return {'error': 'File name does not match VMID'}, 400
            r.sadd('joblock', vmid)
            print(request.args['file'])
            fileimg = "".join([path, request.args['file']])
            fileconf = "".join([os.path.splitext(fileimg)[0], ".conf"])
            if os.path.isfile(fileimg):
                os.remove(fileimg)
                if os.path.isfile(fileconf):
                    os.remove(fileconf)
                r.srem('joblock', vmid)
                return {'file removed': os.path.basename(fileimg)}
            else:
                r.srem('joblock', vmid)
                return {'file does not exist': os.path.basename(fileimg)}, 400
        else:
            return {'error': "resource requires a file argument"}, 400


class Status(Resource):
    @auth.login_required
    def get(self, vmid):
        response = []
        # catch if container does not exist
        if not checkConf(vmid):
            return {'error': "{} is not a valid CTID"}, 400
        status = r.hget(vmid, 'state')
        msg = r.hget(vmid, 'msg')
        job = r.hget(vmid, 'job')
        file = r.hget(vmid, 'file')
        return {vmid: {'status': status, 'message': msg, 'job': job, 'file': file}}, 200


class AllStatus(Resource):
    @auth.login_required
    def get(self):
        response = []
        for worker in workers:
            print(worker)
        for vmid in r.smembers('joblock'):
            status = r.hget(vmid, 'state')
            msg = r.hget(vmid, 'msg')
            job = r.hget(vmid, 'job')
            file = r.hget(vmid, 'file')
            response.append({vmid: {'status': status, 'message': msg, 'job': job, 'file': file}})
        return response


class ClearQueue(Resource):
    @auth.login_required
    def post(self):
        response = []
        for vmid in r.smembers('joblock'):
            status = r.hget(vmid, 'state')
            if status == 'enqueued':
                r.srem('joblock', vmid)
                r.hset(vmid, 'state', 'OK')
                response.append({vmid: {"status": "OK", "message": "Successfully dequeued"}})
            else:
                msg = r.hget(vmid, 'msg')
                job = r.hget(vmid, 'job')
                file = r.hget(vmid, 'file')
                response.append({vmid: {'status': status, 'message': msg, 'job': job, 'file': file}})
        return response


class CleanSnaps(Resource):
    @auth.login_required
    def post(self):
        response = []
        if 'deep' in request.args:
            r.hset(0, 'job', 'deepscrub')
            r.hset(0, 'state', 'enqueued')
            r.sadd('joblock', 0)
            return {'Status': "Deep scrub in progress"}, 200
        for vmid in r.smembers('joblock'):
            status = r.hget(vmid, 'state')
            msg = r.hget(vmid, 'msg')
            job = r.hget(vmid, 'job')  # REMOVE- used for testing scrub functions
            if (status == 'error') and (msg == "error creating backup snapshot"):
                # add to scrub queue
                # if job == 'backup': #REMOVE - used for testing scrub functions
                # 	r.hset(vmid, 'retry', 'backup')
                r.hset(vmid, 'job', 'scrub')
                r.hset(vmid, 'state', 'enqueued')


class Poison(Resource):
    @auth.login_required
    def post(self, vmid):
        # catch if container does not exist
        if str(vmid) in r.smembers('joblock'):
            r.hset(vmid, 'job', 'poisoned')
            if r.hget(vmid, 'state') == 'error':
                r.hset(vmid, 'state', 'enqueued')
            return {'status': "Attempting to cancel task or error for container: {}".format(vmid)}, 200
        else:
            return {'status': "Container not in job queue, nothing to do"}, 200


class Info(Resource):
    @auth.login_required
    def get(self):
        response = {}
        barqueHealth = 'OK'
        response['version'] = version
        now = datetime.utcnow().replace(microsecond=0)
        uptime = now - starttime
        response['uptime'] = str(uptime)
        workerTask = {}
        # queue status
        setSize = r.scard('joblock')
        active = 0
        errors = 0
        for member in r.smembers('joblock'):
            if r.hget(member, 'state') == "active":
                active += 1
                workerTask[r.hget(member, 'worker')] = {'1': r.hget(member, 'job'), '2': r.hget(member, 'msg'),
                                                        '3': member}
            if r.hget(member, 'state') == "error":
                errors += 1
                if not barqueHealth == 'CRITICAL':
                    barqueHealth = 'WARNING'
        response['Queue'] = {'jobs in queue': setSize, 'active': active, 'errors': errors}
        # worker status
        workerStatus = {}
        print(workers)
        for worker in workers:
            if worker.is_alive():
                healthy = "Alive"
            else:
                healthy = "Dead"
                barqueHealth = 'CRITICAL'
            try:
                task = workerTask[str(worker.pid)]['1']
            except:
                task = ""
            try:
                message = workerTask[str(worker.pid)]['2']
            except:
                message = ""
            try:
                container = workerTask[str(worker.pid)]['3']
            except:
                container = ""
            workerStatus[worker.pid
            ] = {'Health': healthy, 'Job': task, 'Message': message, 'CTID': container}
        response['workers'] = workerStatus
        print(workerTask)
        # destination status
        dests = {}
        for spot in barque_storage[local_cluster].keys():
            if os.path.isdir(barque_storage[local_cluster][spot]):
                healthy = "OK"
            else:
                healthy = "Down"
                barqueHealth = 'CRITICAL'
            dests[spot] = healthy
        response['Storage'] = dests
        response['Health'] = barqueHealth
        return response


class AVtoggle(Resource):
    @auth.login_required
    def post(self):
        if 'node' not in request.args:
            return {'error': 'Node argument required beacuse... it needs a node'}, 400
        node = request.args['node']

        convertAVLocks(node)
        # if 'ctid' not in request.args:
        # 	return {'error': 'Container ID required for locking'}, 400
        if 'switch' not in request.args:
            return {'error': 'switch not specified, should be "on" or "off"'}, 400

        # timestamp = time.time()
        if 'ctid' not in request.args:
            return {'error': 'CTID not specified, required for racing lock'}, 400

        ctid = request.args['ctid']

        if request.args['switch'] == 'off':
            ctid = request.args['ctid']

            try:
                print("node: {}, ctid: {}".format(node, ctid))
                r.sadd(node, ctid)
                cmd = subprocess.check_output(
                    "ssh -t root@{} '/opt/sophos-av/bin/savdctl disable'".format(node), shell=True)
                print("disabling antivirus on node: {}, output: {}".format(node, cmd))
            except:
                r.srem(node, ctid)
                return {'state': 'error'}, 200
            return {'state': 'disabling'}, 200

        if request.args['switch'] == 'on':
            r.srem(node, ctid)
            if r.type(node) == "none":
                try:
                    print("node: {}".format(node))
                    cmd = subprocess.check_output(
                        "ssh -t root@{} '/opt/sophos-av/bin/savdctl enable'".format(node), shell=True)
                    r.hdel(node, ctid)
                    print("enabling antivirus on node: {}, output: {}".format(node, cmd))
                except:
                    r.sadd(node, ctid)
                    return {'state': 'error'}, 200
                return {'state': 'enabling'}, 200
            else:
                locked_by = ", ".join(r.sunion(node))
                return {'state': "enabling deferred, another CTID has the lock by {}".format(locked_by)}, 200

    @auth.login_required
    def get(self):
        if 'node' not in request.args:
            return {'error': 'Node argument required beacuse... it needs a node'}, 400
        node = request.args['node']
        active = None
        try:
            print("node: {}".format(node))
            cmd = subprocess.check_output("ssh -t root@{} '/opt/sophos-av/bin/savdstatus'".format(node), shell=True)
            active = cmd.strip()
        except subprocess.CalledProcessError as e:
            active = e.output.strip()
        except:
            print("error getting status")
            return {'error': 'problem getting status'}, 500

        locked_by = list(r.smembers(node))
        if 'scanning is running' in active:
            return {'active': True, 'status': active, "locked_by": locked_by}, 200
        elif 'scanning is not running' in active:
            return {'active': False, 'status': active, "locked_by": locked_by}, 200
        else:
            return {'error': "Problem determining AV status"}, 200


class Migrate(Resource):
    @auth.login_required
    def post(self, vmid):
        # Check if file is specified
        if 'file' not in request.args:
            return {'error': 'Resource requires a file argument'}, 400
        # Check if destination container does not exist
        if not checkConfv2(vmid):
            return {'error': "{} is not a valid CTID"}, 400
        # Check if cluster is specified
        if 'cluster' not in request.args:
            return {'error': 'Resource requires a cluster argument (dtla01, dtla02, dtla03, dtla04, ny01)'}, 400
        # clear error and try again if retry flag in request args
        if 'retry' in request.args and r.hget(vmid, 'state') == 'error':
            r.srem('joblock', vmid)
            log.debug("Clearing error state for ctid: {}".format(vmid))
        # Check if container is available
        if str(vmid) in r.smembers('joblock'):
            return {'error': "VMID locked, another operation is in progress for container: {}".format(vmid)}, 409

        r.hset(vmid, 'job', 'migrate')
        r.hset(vmid, 'file', request.args['file'])
        r.hset(vmid, 'worker', '')
        r.hset(vmid, 'msg', '')
        r.hset(vmid, 'target_cluster', request.args['cluster'])
        r.hset(vmid, 'state', 'enqueued')
        r.sadd('joblock', vmid)

        return {'status': "migrate job created for CTID {}".format(vmid)}, 202

class MigrateV2(Resource):
    @auth.login_required
    def post(self, vmid):
        # Check if file is specified
        if 'file' not in request.args:
            return {'error': 'Resource requires a file argument'}, 400
        # Check if destination container does not exist
        if not checkConfv2(vmid):
            return {'error': "{} is not a valid CTID"}, 400
        # Check if cluster is specified
        if 'cluster' not in request.args:
            return {'error': 'Resource requires a cluster argument {}'.format(barque_storage.keys())}, 400
        # clear error and try again if retry flag in request args
        if 'retry' in request.args and r.hget(vmid, 'state') == 'error':
            r.srem('joblock', vmid)
            log.debug("Clearing error state for ctid: {}".format(vmid))
        # Check if container is available
        if str(vmid) in r.smembers('joblock'):
            return {'error': "VMID locked, another operation is in progress for container: {}".format(vmid)}, 409

        # handle destination setting

        path = ""
        result, _, _ = checkDest('default')
        if result:
            path = barque_storage[local_cluster]['default']
        else:
            return response, err

        #determine remote host IP

        #determine remote host path

        # get info of target barque node
        target_ip = ''
        target_region, target_cluster_id, blank = re.split('(\d+)',request.args['cluster'].lower())
        if target_region in barque_ips:
            if target_cluster_id in barque_ips[target_region]:
                target_ip = barque_ips[target_region][target_cluster_id]
            else:
                r.hset(vmid, 'state','error')
                r.hset(vmid,'msg','cluster number not configured')
        else:
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', 'Cluster region not configured')
        target_path = barque_storage[request.args['cluster'].lower()][request.args['dest']]

        r.hset(vmid, 'dest', path)
        r.hset(vmid, 'target_ip', target_ip)
        r.hset(vmid, 'target_path', target_path)
        r.hset(vmid, 'job', 'migrate')
        r.hset(vmid, 'file', request.args['file'])
        r.hset(vmid, 'worker', '')
        r.hset(vmid, 'msg', '')
        r.hset(vmid, 'target_cluster', request.args['cluster'])
        r.hset(vmid, 'state', 'enqueued')
        r.sadd('joblock', vmid)

        return {'status': "migrate job created for CTID {}".format(vmid)}, 202


class Inventory(Resource):
    @auth.login_required
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
                specificNodes[node] = r.hget('inventory', node)
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
                stats["lxc"] = int(r.hget('inv_ct', node))
                stats["vm"] = int(r.hget('inv_vm', node))
                stats["total"] = stats["lxc"] + stats["vm"]
                nodes[node] = stats
                aggregate["lxc"] += stats["lxc"]
                aggregate["vm"] += stats["vm"]
                aggregate["total"] += stats["total"]
            return {"nodes": nodes, "aggregate": aggregate}, 200
        for node in os.listdir('/etc/pve/nodes'):
            try:
                total_count[node] = int(r.hget('inv_ct', node)) + int(r.hget('inv_vm', node))
            except:
                continue
        return {'total': self.get_total(), 'nodes': total_count}, 200

    def get_total(self):
        total = 0
        for node in os.listdir('/etc/pve/nodes'):
            total = total + int(r.hget('inv_ct', node)) + int(r.hget('inv_vm', node))
        return total


class Destroy(Resource):
    @auth.login_required
    def post(self, vmid):
        # catch if container does not exist
        if not checkConf(vmid):
            return {'error': "{} is not a valid CTID".format(vmid)}, 400
        # clear error and try again if retry flag in request args
        if 'retry' in request.args and r.hget(vmid, 'state') == 'error':
            r.srem('joblock', vmid)
            log.debug("Clearing error state")
        # Check if container is available
        if str(vmid) in r.smembers('joblock'):
            return {'error': "VMID locked, another operation is in progress "
                    "for container: {}".format(vmid)}, 409

        r.hset(vmid, 'job', 'destroy')
        r.hset(vmid, 'file', '')
        r.hset(vmid, 'worker', '')
        r.hset(vmid, 'msg', 'KILL! CRUSH! DESTROY! RAWR!')
        r.hset(vmid, 'target_cluster', '')
        r.hset(vmid, 'state', 'enqueued')
        r.sadd('joblock', vmid)

        return {'status': "Deletion requested for {}".format(vmid)}, 202

class Destroyv2(Resource):
    @auth.login_required
    def post(self, vmid):
        # catch if container does not exist
        if not checkConfv2(vmid):
            return {'error': "{} is not a valid CTID".format(vmid)}, 400
        # clear error and try again if retry flag in request args
        if 'retry' in request.args and r.hget(vmid, 'state') == 'error':
            r.srem('joblock', vmid)
            log.debug("Clearing error state")
        # Check if container is available
        if str(vmid) in r.smembers('joblock'):
            return {'error': "VMID locked, another operation is in progress "
                    "for container: {}".format(vmid)}, 409

        r.hset(vmid, 'job', 'destroy')
        r.hset(vmid, 'file', '')
        r.hset(vmid, 'worker', '')
        r.hset(vmid, 'msg', 'destroying')
        r.hset(vmid, 'target_cluster', '')
        r.hset(vmid, 'state', 'enqueued')
        r.sadd('joblock', vmid)

        return {'status': "Deletion requested for {}".format(vmid)}, 202

api.add_resource(ListAllBackups, '/barque/')
api.add_resource(ListBackups, '/barque/<int:vmid>')
api.add_resource(Backupv2, '/barque/<int:vmid>/backup')
api.add_resource(BackupAll, '/barque/all/backup')
api.add_resource(Restorev2, '/barque/<int:vmid>/restore')
api.add_resource(DeleteBackup, '/barque/<int:vmid>/delete')
api.add_resource(Status, '/barque/<int:vmid>/status')
api.add_resource(AllStatus, '/barque/all/status')
api.add_resource(Info, '/barque/info')
api.add_resource(ClearQueue, '/barque/all/clear')
api.add_resource(CleanSnaps, '/barque/all/clean')
api.add_resource(Poison, '/barque/<int:vmid>/poison')
api.add_resource(AVtoggle, '/barque/avtoggle')
api.add_resource(MigrateV2, '/barque/<int:vmid>/migrate')
api.add_resource(Inventory, '/barque/count')
api.add_resource(Destroyv2, '/barque/<int:vmid>/destroyContainer')
#api.add_resource(Destroyv2, '/barque/<int:vmid>/destroy')


def sanitize():
    for item in r.smembers('joblock'):
        status = r.hget(item, 'state')
        if status != 'error':
            r.srem('joblock', item)
            r.hset(item, 'state', 'OK')


@auth.verify_password
def verify(username, password):
    if not (username and password):
        return False
    return admin_auth.get(username) == password

def checkConfv2(vmid):
    """Sets host and resource type in redis database, returns True if exists."""
    config_file = ""
    config_target = "{}.conf".format(vmid)
    nodelist = os.listdir('/etc/pve/nodes')
    for node in os.listdir('/etc/pve/nodes'):
        if config_target in os.listdir('/etc/pve/nodes/{}/lxc'.format(node)):
            r.hset(vmid, "host", node)
            r.hset(vmid, "type", "ct")
            return True
        if config_target in os.listdir('/etc/pve/nodes/{}/qemu-server'.format(node)):
            r.hset(vmid, "host", node)
            r.hset(vmid, "type", "vm")
            return True
    return False

def checkConf(vmid):
    # catch if container does not exist
    config_file = ""
    config_target = "{}.conf".format(vmid)
    for paths, dirs, files in os.walk('/etc/pve/nodes'):
        if config_target in files:
            config_file = os.path.join(paths, config_target)
            print(config_file)
    if len(config_file) == 0:
        if r.hget(vmid, 'job') == 'destroy':
            return True
        else:
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', '{} is invalid CTID'.format(vmid))
            return False
    return True


def checkDest(dest):
    if dest in barque_storage[local_cluster]:
        directory = barque_storage[local_cluster][dest]
        if os.path.exists(directory):
            return True, None, None
        else:
            subprocess.check_output(
                '/bin/bash /etc/pve/utilities/detect_stale.sh', shell=True)
            if os.path.exists(directory):
                return True, None, None
            else:
                return False, {'error': '{} is not currently accessible'
                               ''.format(directory)}, 500
    else:
        return False, {'error': '{} is not a configured destination'
                       ''.format(dest)}, 400


# remove old style storage
def convertAVLocks(node):
    if r.type(node) != 'hash':
        return
    locked_by = r.hget(node, "ctid")
    r.hdel(node, 'ctid')
    r.sadd(node, locked_by)


if __name__ == '__main__':
    starttime = datetime.utcnow().replace(microsecond=0)
    r = redis.Redis(host=r_host, port=r_port, password=r_pw, db=0)
    sanitize()
    log.info("redis connection successful")
    f = barqueForeman.Foreman(r_host, r_port, r_pw, 0)
    f.start()
    log.debug("Foreman started")
    for i in range(minions):
        p = barqueWorker.Worker(_host,
                                "root@pam",
                                _password,
                                r_host,
                                r_port,
                                r_pw,
                                0,
                                barque_ips,
                                barque_storage,
                                local_cluster)
        workers.append(p)
        p.start()
        log.debug("worker started")
    app.debug = False
    app.run(host=_host,
            port=_port,
            debug=False,
            use_reloader=False,
            ssl_context=(cert, key))
