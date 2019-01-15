#!/usr/bin/python2.7
import configparser
import itertools
import multiprocessing
import os
import redis
import subprocess
import time
import logging
import re
from proxmoxer import ProxmoxAPI
from datetime import datetime
from glob import glob
from json import loads
from shutil import copyfile
from logging import handlers
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
path = config['settings']['path']  # Destination path for backups, terminating / required
pool = config['settings']['pool']  # Ceph RBD pool, terminating / required. Leave empty for default
minions = int(config['settings']['workers'])  # number of worker processes to spawn
r_host = config['redis']['host']  # Redis server host
r_port = int(config['redis']['port'])  # Redis server port
r_pw = config['redis']['password']  # Redis server password
locations = {}  # backup storage destinations
barque_storage = {}
barque_ips = {}
for option in config.options('destinations'):
    locations[option] = config.get('destinations', option)
for option in config.options('barque_storage'):
    barque_storage[option] = config.get('barque_storage', option)
for item in config['barque_ips'].items():
    r,c = item[0].split('.')
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
version = '0.9.0'
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
fh = logging.handlers.WatchedFileHandler('/var/log/barque.log')
fh.setLevel(logging.INFO)
fm = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s -- %(message)s')
ch.setFormatter(fm)
fh.setFormatter(fm)
log.addHandler(ch)
log.addHandler(fh)
log.info('Barque Logging Initialized.')

proxmox = ProxmoxAPI(_host, user='root@pam', password=_password, verify_ssl=False)

class Foreman(multiprocessing.Process):
    stick = None
    r = None
    def run(self):
        r = redis.Redis(host=r_host, port=r_port, password=r_pw)
        self.stick = logging.getLogger('Barque.Foreman')
        self.stick.info("Foreman logging started")
        while True:
            self.stick.debug("Foreman loop tick")
            containers = self.update_inventory()
            self.update_master_list(containers)
            time.sleep(30.0 - (time.time() % 30.0))

    def update_inventory(self):
        containers = {}
        for node in os.listdir('/etc/pve/nodes'):
            self.stick.debug("Getting list for node {}".format(node))
            try:
                socket_list = subprocess.check_output('ssh {} \"awk \'{{ print \$8 }}\' /proc/net/unix | grep /var/lib/lxc/.*/command\"'.format(node), shell=True).split('\n')[:-1]
            except subprocess.CalledProcessError as e:
                self.stick.debug("Error getting containers from {}: {}".format(node, e.output))
                self.stick.debug("Assuming no containers exist, setting to 0")
                containers[node] = "0"
                continue
            containers[node] = len(socket_list)
        return containers

    def update_master_list(self, containers):
        for key in containers:
            r.hset('inventory',key, containers[key])

class Worker(multiprocessing.Process):
    # TODO: Create 'stop container' function
    r = None
    stick = None
    proxconn = None
    name = None

    def run(self):
        licensed_to_live = True
        my = multiprocessing.current_process()
        self.name = 'Process {}'.format(my.pid)
        self.stick = logging.getLogger('Barque.{}'.format(my.name))
        self.stick.debug("Process logging started")
        self.stick.info("{} started".format(self.name))
        r = redis.Redis(host=r_host, port=r_port, password=r_pw)
        self.stick.debug("{} connected to redis".format(self.name))
        
        #		#block for items in joblock
        while licensed_to_live:
            for job in r.smembers('joblock'):
                if r.hget(job, 'state') == 'enqueued':
                    r.hset(job, 'state', 'locked')
                    r.hset(job, 'worker', str(my.pid))
                    self.stick.debug("{} attempting to lock {}".format(self.name, job))
                    time.sleep(0.5)
                    # check if lock belongs to this worker
                    if r.hget(job, 'worker') == str(my.pid):
                        self.stick.debug("{} lock successful, proceeding".format(self.name))
                        task = r.hget(job, 'job')
                        if task == 'backup':
                            self.backup(job)
                        elif task == 'restore':
                            self.restore(job)
                        elif task == 'scrub':
                            self.scrubSnaps(job)
                        elif task == 'migrate':
                            self.migrate(job)
                        elif task == 'poisoned':
                            self.poison(job)
                        elif task == 'deepscrub':
                            self.scrubDeep()
                        else:
                            self.stick.error("{} unable to determine task {}".format(self.name, task))
                    else:
                        self.stick.debug("{} lock unsuccessful, reentering queue".format(self.name))
                # be nice to redis
                time.sleep(0.1)
            time.sleep(5)
        # licensed_to_live = False
        return

    def backup(self, vmid):
        storage = None
        vmdisk = None
        destination = r.hget(vmid, 'dest')
        destHealth = checkDest(destination)
        try:
            self.proxconn = ProxmoxAPI(_host, user='root@pam', password=_password, verify_ssl=False)
        except:
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', 'failed to connect to proxmox')
            return
        if not destHealth:
            cmd = subprocess.check_output('/bin/bash /etc/pve/utilities/detect_stale.sh', shell=True)
            print(cmd)
            destHealth = checkDest(destination)
            if not destHealth:
                r.hset(vmid, 'msg', '{} storage destination is offline, unable to recover')
                r.hset(vmid, 'state', 'error')
                return
        r.hset(vmid, 'state', 'active')
        # vmdisk = 'vm-{}-disk-1'.format(vmid)
        timestamp = datetime.strftime(datetime.utcnow(), "_%Y-%m-%d_%H-%M-%S")
        config_file = ""
        config_target = "{}.conf".format(vmid)
        # get config file
        for paths, dirs, files in os.walk('/etc/pve/nodes'):
            if config_target in files:
                config_file = os.path.join(paths, config_target)
                print(config_file)

        # catch if container does not exist
        if len(config_file) == 0:
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', '{} is invalid CTID'.format(vmid))
            return
        valid = checkConf(vmid)
        print("config exists within worker process? {}".format(valid))
        # get storage info from config file
        parser = configparser.ConfigParser()
        try:
            with open(config_file) as lines:
                lines = itertools.chain(("[root]",), lines)
                parser.read_file(lines)
        except:
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', 'unable to open config file')
            return
        try:
            storage, vmdisk = parser['root']['rootfs'].split(',')[0].split(':')
        except:
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', 'unable to get storage info from config file')
            return
        pool = ''
        try:
            pool = self.proxconn.storage(storage).get()["pool"]
        except:
            r.hset(vmid, 'state','error')
            r.hset(vmid, 'msg', 'unable to get pool information from proxmox')
        print(storage)
        print(vmdisk)
        print(pool)

        # check if poisoned 1
        if r.hget(vmid, 'job') == 'poisoned':
            r.srem('joblock', vmid)
            r.hset(vmid, 'msg', 'Task successfully cancelled')
            r.hset(vmid, 'state', 'OK')
            return

        # create snapshot for backup
        try:
            cmd = subprocess.check_output('rbd snap create {}/{}@barque'.format(pool, vmdisk), shell=True)
        except:
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', 'error creating backup snapshot')
            return

        # copy config file
        config_dest = "".join([destination, vmdisk, timestamp, ".conf"])
        try:
            copyfile(config_file, config_dest)
        except:
            # delete barque snapshot
            cmd = subprocess.check_output('rbd snap rm {}/{}@barque'.format(pool, vmdisk), shell=True)
            r.hset(vmid, 'msg', 'unable to copy config file')
            r.hset(vmid, 'state', 'error')
            return
        # protect snapshot during backup
        cmd = subprocess.check_output('rbd snap protect {}/{}@barque'.format(pool, vmdisk), shell=True)

        # check if poisoned 2
        if r.hget(vmid, 'job') == 'poisoned':
            try:
                # unprotect barque snapshot
                cmd = subprocess.check_output('rbd snap unprotect {}/{}@barque'.format(pool, vmdisk), shell=True)
                # delete barque snapshot
                cmd = subprocess.check_output('rbd snap rm {}/{}@barque'.format(pool, vmdisk), shell=True)
                # delete stored config file
                os.remove(config_dest)
                r.hset(vmid, 'msg', 'Task successfully cancelled')
                r.hset(vmid, 'state', 'OK')
            except:
                r.hset(vmid, 'state', 'error')
                r.hset(vmid, 'msg', 'error removing backup snapshot while poisoned')
                return
            r.srem('joblock', vmid)
            return

        # create compressed backup file from backup snapshot
        dest = "".join([destination, vmdisk, timestamp, ".lz4"])
        args = [
            'rbd export --rbd-concurrent-management-ops 20 --export-format 2 {}/{}@barque - | lz4 -1 - {}'.format(pool,
                                                                                                                 vmdisk,
                                                                                                                 dest)]
        r.hset(vmid, 'msg', 'Creating backup image')
        try:
            cmd = subprocess.check_output(args,
                                          shell=True)  # .split('\n') #run command then convert output to list, splitting on newline
        except:
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', 'unable to aquire rbd image for CTID: {}'.format(vmid))
            # unprotect barque snapshot
            cmd = subprocess.check_output('rbd snap unprotect {}/{}@barque'.format(pool, vmdisk), shell=True)
            # delete barque snapshot
            cmd = subprocess.check_output('rbd snap rm {}/{}@barque'.format(pool, vmdisk), shell=True)
            return

        # check poisoned 3
        if r.hget(vmid, 'job') == 'poisoned':
            try:
                # remove backup file
                os.remove(dest)
                # unprotect barque snapshot
                cmd = subprocess.check_output('rbd snap unprotect {}/{}@barque'.format(pool, vmdisk), shell=True)
                # delete barque snapshot
                cmd = subprocess.check_output('rbd snap rm {}/{}@barque'.format(pool, vmdisk), shell=True)
                # delete stored config file
                os.remove(config_dest)
                r.hset(vmid, 'msg', 'Task successfully cancelled')
                r.hset(vmid, 'state', 'OK')
            except:
                r.hset(vmid, 'state', 'error')
                r.hset(vmid, 'msg', 'error cancelling backup, at poisoned 3')
                return
            r.srem('joblock', vmid)
            return

        # unprotect barque snapshot
        cmd = subprocess.check_output('rbd snap unprotect {}/{}@barque'.format(pool, vmdisk), shell=True)

        # delete barque snapshot
        cmd = subprocess.check_output('rbd snap rm {}/{}@barque'.format(pool, vmdisk), shell=True)

        # mark complete and unlock CTID
        r.hset(vmid, 'state', 'OK')
        r.srem('joblock', vmid)
        return

    def restore(self, vmid):
        try:
            self.proxconn = ProxmoxAPI(_host, user='root@pam', password=_password, verify_ssl=False)
        except:
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', 'failed to connect to proxmox')
            return
        self.stick.debug("{} connected to proxmox".format(self.name))

        config_file = ""
        node = ""
        destination = r.hget(vmid, 'dest')
        destHealth = checkDest(destination)
        if not destHealth:
            cmd = subprocess.check_output('/bin/bash /etc/pve/utilities/detect_stale.sh', shell=True)
            print(cmd)
            destHealth = checkDest(destination)
            if not destHealth:
                r.hset(vmid, 'msg', '{} storage destination is offline, unable to recover')
                r.hset(vmid, 'state', 'error')
                return
        filename = r.hget(vmid, 'file')
        r.hset(vmid, 'state', 'active')

        # vmdisk = 'vm-{}-disk-1'.format(vmid)
        fileimg = "".join([destination, filename, ".lz4"])
        fileconf = "".join([destination, filename, ".conf"])

        # find node hosting container
        config_file = ""
        config_target = "{}.conf".format(vmid)
        for paths, dirs, files in os.walk('/etc/pve/nodes'):
            if config_target in files:
                config_file = os.path.join(paths, config_target)
                print(config_file)
                node = config_file.split('/')[4]
                print(node)
        if len(config_file) == 0:
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', 'unable to locate container')
            return

        # get storage info from running container
        parserCurr = configparser.ConfigParser()
        with open(config_file) as lines:
            lines = itertools.chain(("[root]",), lines)
            parserCurr.read_file(lines)
        try:
            storage_current, vmdisk_current = parserCurr['root']['rootfs'].split(',')[0].split(':')
        except:
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', 'unable to get storage info from active config file')
            return

        try:
            pool = self.proxconn.storage(storage_current).get()["pool"]
        except:
            r.hset(vmid, 'state','error')
            r.hset(vmid, 'msg', 'unable to get pool information from proxmox')

        # get storage info from config file
        parserFinal = configparser.ConfigParser()
        with open(fileconf) as lines:
            lines = itertools.chain(("[root]",), lines)
            parserFinal.read_file(lines)
        try:
            storage_final, vmdisk_final = parserFinal['root']['rootfs'].split(',')[0].split(':')
        except:
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', 'unable to get storage info from backup config file')
            return
        # check if poisoned 1
        if r.hget(vmid, 'job') == 'poisoned':
            self.stick.debug('{}:restore: Reached Poisoned 1'.format(vmid))
            r.srem('joblock', vmid)
            r.hset(vmid, 'msg', 'Task successfully cancelled')
            r.hset(vmid, 'state', 'OK')
            return

        # stop container if not already stopped
        r.hset(vmid, 'msg', 'stopping container')
        self.stick.info('{}:restore: Checking if container is stopped'.format(vmid))
        if not self.proxconn.nodes(node).lxc(vmid).status.current.get()["status"] == "stopped":
            try:
                self.stick.debug('{}:restore: Stopping container'.format(vmid))
                self.proxconn.nodes(node).lxc(vmid).status.stop.create()
            except:
                self.stick.error('{}:restore: unable to stop container - proxmox api request fault')
                r.hset(vmid, 'state', 'error')
                r.hset(vmid, 'msg', 'unable to stop container - proxmox api returned null')
                return
        timeout = time.time() + 60
        while True:  # wait for container to stop
            self.stick.debug('{}:restore: checking if container has stopped'.format(vmid))
            stat = self.proxconn.nodes(node).lxc(vmid).status.current.get()["status"]
            self.stick.debug('{}:restore: container state: {}'.format(vmid, stat))
            if stat == "stopped":
                self.stick.debug('{}:restore: container stopped successfully'.format(vmid))
                break
            elif time.time() > timeout:
                self.stick.error('{}:restore: unable to stop container - timeout')
                r.hset(vmid, 'state', 'error')
                r.hset(vmid, 'msg', 'Unable to stop container - timeout')
                return
            time.sleep(10)

        # make recovery copy of container image
        self.stick.info('{}:restore: Creating disaster recovery image'.format(vmid))
        r.hset(vmid, 'msg', 'creating disaster recovery image')
        imgcpy = subprocess.check_output("rbd cp --rbd-concurrent-management-ops 20 {}/{} {}/{}-barque".format(pool,
                                                                                                        vmdisk_current,
                                                                                                        pool,
                                                                                                        vmdisk_current),
                                         shell=True)
        self.stick.debug('{}:restore: finished creating disaster recovery image: {}/{}-barque'.format(vmid,
                                                                                                     pool,
                                                                                                     vmdisk_current))
        # print("Waiting for poison test2")
        # time.sleep(15)
        # check if poisoned 2
        if r.hget(vmid, 'job') == 'poisoned':
            self.stick.debug('{}:restore: Reached Poisoned 2'.format(vmid))
            try:
                # remove recovery copy
                imgrm = subprocess.check_output("rbd rm {}/{}-barque".format(pool, vmdisk_current), shell=True)
                self.stick.debug('{}:restore: Poisoned 2 - removed disaster recovery image'.format(vmid))
                # un-stop container
                self.proxconn.nodes(node).lxc(vmid).status.start.create()
                self.stick.debug('{}:restore: Poisoned 2 - start container request created'.format(vmid))
                r.hset(vmid, 'msg', 'Task successfully cancelled')
                r.hset(vmid, 'state', 'OK')
            except:
                self.stick.error('{}:restore: Poisoned 2 - error cancelling restore'.format(vmid))
                r.hset(vmid, 'msg', 'Error cancelling restore, at poisoned 2')
                r.hset(vmid, 'state', 'error')
                return
            r.srem('joblock', vmid)
            self.stick.info('{}:restore: Poisoned 2 - Successfully poisoned, worker returning to queue'.format(vmid))
            return

        # delete container storage
        r.hset(vmid, 'msg', 'removing container image')
        self.stick.debug('{}:restore: removing container image'.format(vmid))
        timeout = time.time() + 300
        while True:
            if time.time() > timeout:
                self.stick.error('{}:restore: Unable to remove container image, timed out'.format(vmid))
                r.hset(vmid, 'state', 'error')
                r.hset(vmid, 'msg', 'Unable to remove container image, timed out')
                return
            self.stick.debug('{}:restore: Attempting to remove container image, looped!'.format(vmid))
            found = False
            try:
                for item in self.proxconn.nodes(node).storage(storage_current).content.get():
                    if item['name'] == vmdisk_current:
                        try:
                            found = True
                            self.stick.debug('{}:restore: container image found, attempting to remove (loop)'.format(vmid))
                            self.proxconn.nodes(node).storage(storage_current).content(vmdisk_current).delete()
                        except:
                            pass
                if not found:
                    self.stick.debug('{}:restore: container image not found, continuing with restore'.format(vmid))
                    break # Exit loop if vmdisk not found (i.e. it's deleted)
            except:
                self.stick.error('{}:restore: unable to retrieve list of storage from Proxmox, trying again.')
            time.sleep(30)


        # try:
        #     print("hello")
        # except:
        #     self.stick.warning('{}:restore: unable to delete container image'.format(vmid))
        #     try:  # attempt to force unmap image if has watchers
        #         cmd = subprocess.check_output("rbd unmap -o force {}{}".format(pool, vmdisk_current))
        #     except:
        #         r.hset(vmid, 'state', 'error')
        #         r.hset(vmid, 'msg', "unable to unmap container image")
        #         return
        #     try:  # retry deleting image
        #         self.proxconn.nodes(node).storage(storage_current).content(vmdisk_current).delete()
        #         self.stick.info('{}:restore: Successfully deleted container storage after retry'.format(vmid))
        #     except:
        #         self.stick.error('{}:restore: unable to remove container image'.format(vmid))
        #         r.hset(vmid, 'state', 'error')
        #         r.hset(vmid, 'msg', "unable to remove container image")
        #         return

        # extract lz4 compressed image file
        self.stick.debug('{}:restore: uncompressing backup file'.format(vmid))
        filetarget = "".join([destination, filename, ".img"])
        uncompress = subprocess.check_output("lz4 -d {} {}".format(fileimg, filetarget), shell=True)
        self.stick.debug('{}:restore: uncompressed backup to {}'.format(vmid, filetarget))

        # print("Waiting for poison test3")
        # time.sleep(15)
        # check if poisoned 3
        if r.hget(vmid, 'job') == 'poisoned':
            self.stick.debug('{}:restore: Reached Poisoned 3'.format(vmid))
            try:
                # remove uncompressed image
                os.remove(filetarget)
                # try adding container image
                cmd = subprocess.check_output(
                    "rbd mv {}/{}-barque {}/{}".format(pool, vmdisk_current, pool, vmdisk_current), shell=True)
                print(cmd)
                self.proxconn.nodes(node).lxc(vmid).status.start.create()
                r.hset(vmid, 'msg', 'Task successfully cancelled')
                r.hset(vmid, 'state', 'OK')
            except:
                r.hset(vmid, 'msg', 'Error cancelling restore, at poisoned 3')
                r.hset(vmid, 'state', 'error')
                return
            r.srem('joblock', vmid)
            return

        # import new image
        r.hset(vmid, 'msg', 'importing backup image')
        self.stick.debug('{}:restore: importing backup image'.format(vmid))
        try:
            rbdimp = subprocess.check_output(
                "rbd import --rbd-concurrent-management-ops 20 --export-format 2 {} {}/{}".format(filetarget, pool,
                                                                                                 vmdisk_final),
                shell=True)
            self.stick.debug('{}:restore: backup image imported successfully'.format(vmid))
        except:
            self.stick.error('{}:restore: unable to import backup image'.format(vmid))
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', rbdimp)
            cmd = subprocess.check_output("rbd mv {}/{}-barque {}/{}".format(pool, vmdisk_current, pool, vmdisk_current),
                                          shell=True)
            self.stick.error('{}:restore: successfully recovered original container image'.format(vmid))
            os.remove(filetarget)
            return

        # delete uncompressed image file
        r.hset(vmid, 'msg', 'cleaning up')
        self.stick.info('{}:restore: cleaning up'.format(vmid))
        rmuncomp = subprocess.check_output("rm {}".format(filetarget), shell=True)
        self.stick.debug('{}:restore: removed uncompressed backup image'.format(vmid))

        # delete barque snapshot
        cmd = subprocess.check_output('rbd snap rm {}/{}@barque'.format(pool, vmdisk_final), shell=True)
        self.stick.debug('{}:restore: removed barque snapshot'.format(vmid))
        # image attenuation for kernel params #Removed after switching to format 2
        # imgatten = subprocess.check_output("rbd feature disable {} object-map fast-diff deep-flatten".format(vmdisk), shell=True)
        # print(imgatten)

        # print("Waiting for poison test4")
        # time.sleep(15)
        # check if poisoned 4
        if r.hget(vmid, 'job') == 'poisoned':
            self.stick.debug('{}:restore: Reached Poisoned 4'.format(vmid))
            try:
                # try removing the recovered image
                cmd = subprocess.check_output("rbd rm {}/{}".format(pool, vmdisk_current), shell=True)
                # try adding container image
                cmd = subprocess.check_output(
                    "rbd mv {}/{}-barque {}/{}".format(pool, vmdisk_current, pool, vmdisk_current), shell=True)
                print(cmd)
                self.proxconn.nodes(node).lxc(vmid).status.start.create()
                r.hset(vmid, 'msg', 'Task successfully cancelled')
                r.hset(vmid, 'state', 'OK')
            except:
                r.hset(vmid, 'msg', 'Error cancelling restore, at poisoned 4')
                r.hset(vmid, 'state', 'error')
                return
            r.srem('joblock', vmid)
            return
        # replace config file
        copyfile(fileconf, config_file)
        self.stick.debug('{}:restore: config file replaced'.format(vmid))

        # start container
        self.proxconn.nodes(node).lxc(vmid).status.start.create()
        # time.sleep(5)
        self.stick.debug('{}:restore: start container request sent'.format(vmid))

        # cleanup recovery copy
        cmd = subprocess.check_output("rbd rm {}/{}-barque".format(pool, vmdisk_current), shell=True)
        r.hset(vmid, 'state', 'OK')
        r.srem('joblock', vmid)
        self.stick.info("{}:restore: Restore task complete, worker returning to queue".format(vmid))
        return

    def scrubSnaps(self, vmid):
        r.hset(vmid, 'state', 'active')
        config_file = None
        # vmdisk = 'vm-{}-disk-1'.format(vmid)
        config_target = "{}.conf".format(vmid)
        # get config file
        for paths, dirs, files in os.walk('/etc/pve/nodes'):
            if config_target in files:
                config_file = os.path.join(paths, config_target)
            # print(config_file)
        # get storage info from config file
        parser = configparser.ConfigParser()
        with open(config_file) as lines:
            lines = itertools.chain(("[root]",), lines)
            parser.read_file(lines)
        storage, vmdisk = parser['root']['rootfs'].split(',')[0].split(':')

        try:
            cmd = subprocess.check_output('rbd snap unprotect {}{}@barque'.format(pool, vmdisk), shell=True)
        except:
            # could not unprotect, maybe snap wasn't protected
            try:
                cmd = subprocess.check_output('rbd snap rm {}{}@barque'.format(pool, vmdisk), shell=True)
            except:
                r.hset(vmid, 'state', 'error')
                r.hset(vmid, 'msg', "critical snapshotting error: unable to unprotect or remove")
                return
            # snapshot successfully removed, set OK
            r.hset(vmid, 'state', 'OK')
            r.hset(vmid, 'msg', 'snapshot successfully scrubbed - removed only')
            r.srem('joblock', vmid)
            return
        # snapshot successfully unprotected, attempt removal
        try:
            cmd = subprocess.check_output('rbd snap rm {}{}@barque'.format(pool, vmdisk), shell=True)
        except:
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', 'critical snapshotting error: snap unprotected but unable to remove')
            return
        # snapshot successfully removed, set OK
        r.hset(vmid, 'state', 'OK')
        r.hset(vmid, 'msg', 'snapshot successfully scrubbed - unprotected and removed')
        r.srem('joblock', vmid)
        retry = r.hget(vmid, 'retry')
        # if retry == 'backup': #REMOVE - used for testing snap scrubbing
        # 	r.hset(vmid, 'job', 'backup')
        # 	r.hset(vmid, 'state', 'enqueued')
        return

    def scrubDeep(self):
        r.hset('0', 'state', 'active')
        out = subprocess.check_output('rbd ls {}'.format(pool.strip('/')), shell=True)
        for disk in out.split():
            cmd = subprocess.check_output('rbd snap ls {}{}'.format(pool, disk), shell=True)
            if "barque" in cmd.split():
                vmid = disk.split('-')[1]
                if not r.hget(vmid, 'state') == 'active':
                    r.hset(vmid, 'job', 'scrub')
                    r.hset(vmid, 'state', 'enqueued')
                    r.sadd('joblock', vmid)
                    print('snap found on {}, adding to queue'.format(disk))
            else:
                print("{} clean, moving on".format(disk))
        r.srem('joblock', 0)

    def poison(self, vmid):
        r.srem('joblock', vmid)
        r.hset(vmid, 'state', 'OK')
        r.hset(vmid, 'msg', 'Task successfully cancelled')
        return

    def migrate(self, vmid):
        # TODO: Check if target cluster is known
        # TODO: Add Poisoning
        # TODO: Refactor for alternate storage destinations
        # TODO: Move rate limit to config
        r.hset(vmid, 'state', 'active')
        target_file = r.hget(vmid, 'file')
        target_cluster = r.hget(vmid, 'target_cluster')
        config_file = ""
        node = ""
        dest_disk = ""
        dest_storage = ""
        dest_pool = ""
        try:
            self.proxconn = ProxmoxAPI(_host, user='root@pam', password=_password, verify_ssl=False)
        except:
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', 'failed to connect to proxmox')
            return
        self.stick.debug("{} connected to proxmox".format(self.name))

        ##
        ## Gather Information
        ##

        r.hset(vmid, 'msg', 'collecting information')
        # Find node hosting destingation container, get config_file
        config_target = "{}.conf".format(vmid)
        for paths, dirs, files in os.walk('/etc/pve/nodes'):
            if config_target in files:
                config_file = os.path.join(paths, config_target)
                # print(config_file)
                node = config_file.split('/')[4]
                # print(node)
                self.stick.debug("{}:migrate: config file found on node: {}".format(vmid, node))
        if len(config_file) == 0:
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', 'unable to locate container')
            return

        # get storage info from running container
        parserCurr = configparser.ConfigParser()
        with open(config_file) as lines:
            lines = itertools.chain(("[root]",), lines)
            parserCurr.read_file(lines)
        try:
            dest_storage, dest_disk = parserCurr['root']['rootfs'].split(',')[0].split(':')
            self.stick.info('{}:migrate: Storage info - storage={} disk={}'.format(vmid,dest_storage,dest_disk))
        except:
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', 'unable to get storage info from active config file')
            return

        # get rbd pool from proxmox
        try:
            dest_pool = self.proxconn.storage(dest_storage).get()["pool"]
        except:
            r.hset(vmid, 'state','error')
            r.hset(vmid, 'msg', 'unable to get pool information from proxmox')
        # get info of target barque node
        target_ip =''
        migration_rate='50'
        target_region, target_cluster_id, blank = re.split('(\d+)',target_cluster.lower())
        if target_region in barque_ips:
            if target_cluster_id in barque_ips[target_region]:
                target_ip = barque_ips[target_region][target_cluster_id]
            else:
                r.hset(vmid, 'state','error')
                r.hset(vmid,'msg','cluster number not configured')
        else:
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', 'Cluster region not configured')
        target_path = barque_storage[target_cluster.lower()]

        # check if poisoned 1
        if r.hget(vmid,'job') == "poisoned":
            self.stick.debug("{}:migrate: reached poisoned 1".format(vmid))
            r.hset(vmid,'msg','Task successfully cancelled')
            r.hset(vmid,'state','OK')
            r.srem('joblock', vmid)
            return

        ##
        ## Perform Operations
        ##

        # stop container if not already stopped
        r.hset(vmid, 'msg', 'stopping container')
        self.stick.info('{}:migrate: Checking if container is stopped'.format(vmid))
        if not self.proxconn.nodes(node).lxc(vmid).status.current.get()["status"] == "stopped":
            try:
                self.stick.debug('{}:migrate: Stopping container'.format(vmid))
                self.proxconn.nodes(node).lxc(vmid).status.stop.create()
            except:
                self.stick.error('{}:migrate: unable to stop container - proxmox api request fault')
                r.hset(vmid, 'state', 'error')
                r.hset(vmid, 'msg', 'unable to stop container - proxmox api returned null')
                return
        timeout = time.time() + 60
        while True:  # wait for container to stop
            self.stick.debug('{}:migrate: checking if container has stopped'.format(vmid))
            stat = self.proxconn.nodes(node).lxc(vmid).status.current.get()["status"]
            self.stick.debug('{}:migrate: container state: {}'.format(vmid, stat))
            if stat == "stopped":
                self.stick.debug('{}:migrate: container stopped successfully'.format(vmid))
                break
            elif time.time() > timeout:
                self.stick.error('{}:migrate: unable to stop container - timeout')
                r.hset(vmid, 'state', 'error')
                r.hset(vmid, 'msg', 'Unable to stop container - timeout')
                return
            time.sleep(1)

        try:
            r.hset(vmid, 'msg', 'Fetching backup image')
            print("root@{}".format(target_ip))
            cmd = subprocess.check_output(
                "ssh root@{} \"cat {}{}\" | mbuffer -r {}M | lz4 -d - {}{}.img".format(target_ip,
                                                                                       target_path,
                                                                                       target_file,
                                                                                       migration_rate,
                                                                                       locations[locations.keys()[-1]],
                                                                                       vmid), shell=True)
            # print(cmd)
        except:
            try:
                os.remove("{}{}.img".format(locations[locations.keys()[-1]], vmid))
            except:
                r.hset(vmid, 'msg', 'migrate: Unable to remove the retrieved container image, error fetching backup image')
                r.hset(vmid, 'state','error')
                return
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', 'unable to fetch backup image')
            return

        # check if poisoned 2
        if r.hget(vmid,'job') == "poisoned":
            self.stick.debug("{}:migrate: reached poisoned 2".format(vmid))
            try:
                os.remove("{}{}.img".format(locations[locations.keys()[-1]], vmid))
            except:
                r.hset(vmid, 'msg', 'Poisoned: Unable to remove the retrieved container image')
                r.hset(vmid, 'state','error')
            try:
                self.proxconn.nodes(node).lxc(vmid).status.start.create()
            except:
                r.hset(vmid, 'msg', 'Poisoned: Unable to restart container')
                r.hset(vmid, 'state', 'error')
                return
            r.hset(vmid,'msg','Task successfully cancelled')
            r.hset(vmid,'state','OK')
            r.srem('joblock', vmid)
            return

        self.stick.info('{}:migrate: Creating disaster recovery image'.format(vmid))
        r.hset(vmid, 'msg', 'creating disaster recovery image')
        try:
            imgcpy = subprocess.check_output("rbd cp --rbd-concurrent-management-ops 20 {}/{} {}/{}-barque".format(dest_pool,
                                                                                                        dest_disk,
                                                                                                        dest_pool,
                                                                                                        dest_disk),
                                                                                                        shell=True)
        except:
            try:
                os.remove("{}{}.img".format(locations[locations.keys()[-1]], vmid))
            except:
                r.hset(vmid, 'msg', 'migrate: Unable to remove the retrieved container image, error creating image copy')
                r.hset(vmid, 'state','error')
                return
            try:
                imgrm = subprocess.check_output("rbd rm --rbd-concurrent-management-ops 20 {}/{}-barque".format(
                    dest_pool,
                    dest_disk),
                    shell=True)
            except:
                r.hset(vmid, 'msg', 'migrate: Unable to remove the image copy, error creating image copy')
                r.hset(vmid, 'state','error')
                return
            r.hset(vmid, 'msg', 'migrate: Unable to create disaster recovery copy')
            r.hset(vmid, 'state','error')
            return
        self.stick.debug('{}:migrate: finished creating disaster recovery image: {}/{}-barque'.format(vmid,
                                                                                                     dest_pool,
                                                                                                     dest_disk))

        r.hset(vmid, 'msg', 'removing container image')
        self.stick.debug('{}:migrate: removing container image'.format(vmid))
        timeout = time.time() + 120
        while True:
            if time.time() > timeout:
                self.stick.error('{}:migrate: Unable to remove container image, timed out'.format(vmid))
                try:
                    os.remove("{}{}.img".format(locations[locations.keys()[-1]], vmid))
                except:
                    r.hset(vmid, 'msg', 'migrate: Unable to remove the retrieved container image, error removing container image')
                    r.hset(vmid, 'state','error')
                    return
                try:
                    imgrm = subprocess.check_output("rbd rm --rbd-concurrent-management-ops 20 {}/{}-barque".format(
                        dest_pool,
                        dest_disk),
                        shell=True)
                except:
                    r.hset(vmid, 'msg', 'migrate: Unable to remove the image copy, error removing container image')
                    r.hset(vmid, 'state','error')
                    return
                r.hset(vmid, 'state', 'error')
                r.hset(vmid, 'msg', 'Unable to remove container image, timed out')
                return
            self.stick.debug('{}:migrate: Attempting to remove container image, looped!'.format(vmid))
            found = False
            try:
                for item in self.proxconn.nodes(node).storage(dest_storage).content.get():
                    if item['name'] == dest_disk:
                        try:
                            found = True
                            self.stick.debug(
                                '{}:migrate: container image found, attempting to remove (loop)'.format(vmid))
                            self.proxconn.nodes(node).storage(dest_storage).content(dest_disk).delete()
                        except:
                            pass
                if not found:
                    self.stick.debug('{}:migrate: container image not found, continuing with restore'.format(vmid))
                    break  # Exit loop if vmdisk not found (i.e. it's deleted)
            except:
                self.stick.error('{}:migrate: unable to retrieve list of storage from Proxmox, trying again.')
            time.sleep(5)

        try:
            r.hset(vmid, 'msg', 'Importing disk image')
            cmd = subprocess.check_output(
                "rbd import --export-format 2 {}{}.img {}/{}".format(locations[locations.keys()[-1]], vmid, dest_pool,
                                                                    dest_disk), shell=True)
            #print(cmd)
        except:
            try:
                os.remove("{}{}.img".format(locations[locations.keys()[-1]], vmid))
            except:
                r.hset(vmid, 'msg', 'migrate: Unable to remove the retrieved container image, error removing container image')
                r.hset(vmid, 'state','error')
                return
            try:
                imgrm = subprocess.check_output("rbd rm --rbd-concurrent-management-ops 20 {}/{}-barque".format(
                    dest_pool,
                    dest_disk),
                    shell=True)
            except:
                r.hset(vmid, 'msg', 'migrate: Unable to remove the image copy, error removing container image')
                r.hset(vmid, 'state','error')
                return
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', 'unable to import disk image')
            return

        # check if poisoned 3
        if r.hget(vmid,'job') == "poisoned":
            self.stick.debug("{}:migrate: reached poisoned 3".format(vmid))
            try:
                os.remove("{}{}.img".format(locations[locations.keys()[-1]], vmid))
            except:
                r.hset(vmid, 'msg', 'Poisoned: Unable to remove the retrieved container image')
                r.hset(vmid, 'state','error')
            try:
                # try removing the recovered image
                cmd = subprocess.check_output("rbd rm {}/{}".format(dest_pool, dest_disk), shell=True)
                # try adding container image
                cmd = subprocess.check_output(
                    "rbd mv {}/{}-barque {}/{}".format(dest_pool, dest_disk, dest_pool, dest_disk), shell=True)
            except:
                r.hset(vmid, 'msg', 'Poisoned: Unable to recover original container disk image')
                r.hset(vmid, 'state', 'error')
            try:
                self.proxconn.nodes(node).lxc(vmid).status.start.create()
            except:
                r.hset(vmid, 'msg', 'Poisoned: Unable to restart container')
                r.hset(vmid, 'state', 'error')
                return
            r.hset(vmid,'msg','Task successfully cancelled')
            r.hset(vmid,'state','OK')
            r.srem('joblock', vmid)
            return
        ##
        ## Clean up
        ##

        r.hset(vmid, 'msg', 'Cleaning up...')
        os.remove("{}{}.img".format(locations[locations.keys()[-1]], vmid))
        self.proxconn.nodes(node).lxc(vmid).status.start.create()
        cmd = subprocess.check_output("rbd rm {}/{}-barque".format(dest_pool, dest_disk), shell=True)
        r.hset(vmid, 'state', 'OK')
        r.hset(vmid, 'msg', 'Migration complete')
        r.srem('joblock', vmid)
        return

    def convert(self, vmid):
        # TODO: Check state of storage destination
        # TODO: Add poisoning
        # TODO: Make this work both ways
        node = ""
        config_file = ""
        destination = r.hget(vmid, 'dest')
        filename = r.hget(vmid, 'file')
        r.hset(vmid, 'state', 'active')

        self.proxconn = ProxmoxAPI(_host, user='root@pam', password=_password, verify_ssl=False)
        self.stick.debug("{} connected to proxmox".format(name))

        ## Gather Information
        destHealth = checkDest(destination)
        if not destHealth:
            cmd = subprocess.check_output('/bin/bash /etc/pve/utilities/detect_stale.sh', shell=True)
            print(cmd)
            destHealth = checkDest(destination)
            if not destHealth:
                r.hset(vmid, 'msg', '{} storage destination is offline, unable to recover')
                r.hset(vmid, 'state', 'error')
                return

        # find node hosting container
        config_target = "{}.conf".format(vmid)
        for paths, dirs, files in os.walk('/etc/pve/nodes'):
            if config_target in files:
                config_file = os.path.join(paths, config_target)
                print(config_file)
                node = config_file.split('/')[4]
                print(node)
        if len(config_file) == 0:
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', 'unable to locate container')
            return

        # Get CT config
        ct_conf = self.proxconn.nodes(node).lxc(vmid).config.get()
        ct_storage = ct_conf['rootfs'].split(':')[0]
        disk_image, disk_size = ct_conf['rootfs'].split(':')[1].split(',')

        # Get storage details
        storage = self.proxconn.storage(ct_storage).get()
        if storage['type'] == 'rbd':
            rbd_pool = storage['pool']

        # Generate new storage name
        dest_info = self.proxconn.storage(destination).get()
        export_image = dest_info['path'] + '/' + vmid + '/' + disk_image + '.raw'
        export_info = dest_info['storage'] + ':' + vmid + '/' + disk_image + '.raw'

        ## Stop container
        timeout = time.time() + 60
        while True:  # wait for container to stop
            self.stick.debug('{}:restore: checking if container has stopped'.format(vmid))
            stat = self.proxconn.nodes(node).lxc(vmid).status.current.get()["status"]
            self.stick.debug('{}:restore: container state: {}'.format(vmid, stat))
            if stat == "stopped":
                self.stick.debug('{}:restore: container stopped successfully'.format(vmid))
                break
            elif time.time() > timeout:
                self.stick.error('{}:restore: unable to stop container - timeout')
                r.hset(vmid, 'state', 'error')
                r.hset(vmid, 'msg', 'Unable to stop container - timeout')
                return
            time.sleep(1)


        ## Export rbd image to .raw
            # Backup first maybe? ... just in case...
        subprocess.check_output('qemu-img convert -f raw -O raw rbd:{}/{} {}'.format(rbd_pool,
                                                                                     disk_image,
                                                                                     export_image), shell=True)

        ## Point container config to new image (via proxmox api?)
        self.proxconn.nodes(node).lxc(vmid).config.set(rootfs='{},{}'.format(export_info, disk_size))

        ## Clean up old image
        subprocess.check_output('rbd rm {}/{}'.format(rbd_pool, disk_image), shell=True)

        ## Start Container and exit
        self.proxconn.nodes(node).lxc(vmid).status.start.create()
        r.hset(vmid, 'state', 'OK')
        r.srem('joblock', vmid)

    ####
    ##  Auxiliary Functions
    ####



####                     ####
##  pveBarque API Classes  ##
####                     ####

class Backup(Resource):
    @auth.login_required
    def post(self, vmid):
        dest = ""
        # catch if container does not exist
        if not checkConf(vmid):
            return {'error': "{} is not a valid CTID".format(vmid)}, 400
        # clear error and try again if retry flag in request args
        if 'retry' in request.args and r.hget(vmid, 'state') == 'error':
            r.srem('joblock', vmid)
            log.debug("Clearing error state")
        # handle destination setting
        if 'dest' in request.args:
            result, response, err = checkDest(request.args['dest'])
            if result:
                dest = locations[request.args['dest']]
            else:
                return response, err
        else:
            result, response, err = checkDest(locations.keys()[-1])
            if result:
                dest = locations[locations.keys()[-1]]
            else:
                return response, err
        if str(vmid) in r.smembers('joblock'):
            return {'error': "VMID locked, another operation is in progress for container: {}".format(vmid),
                    'status': r.hget(vmid, 'state'), 'job': r.hget(vmid, 'job')}, 409
        else:
            r.hset(vmid, 'job', 'backup')
            r.hset(vmid, 'file', '')
            r.hset(vmid, 'worker', '')
            r.hset(vmid, 'msg', '')
            r.hset(vmid, 'state', 'enqueued')
            r.hset(vmid, 'dest', dest)
            r.sadd('joblock', vmid)

        return {'status': "backup job created for CTID {}".format(vmid)}, 202


class BackupAll(Resource):
    @auth.login_required
    def post(self):
        targets = []
        dest = ""
        # handle destination setting
        if 'dest' in request.args:
            result, response, err = checkDest(request.args['dest'])
            if result:
                dest = locations[request.args['dest']]
            else:
                return response, err
        else:
            result, response, err = checkDest(locations.keys()[-1])
            if result:
                dest = locations[locations.keys()[-1]]
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


class Restore(Resource):
    @auth.login_required
    def post(self, vmid):
        path = None
        # handle destination setting
        if 'dest' in request.args:
            result, response, err = checkDest(request.args['dest'])
            if result:
                path = locations[request.args['dest']]
            else:
                return response, err
        else:
            result, response, err = checkDest(locations.keys()[-1])
            if result:
                path = locations[locations.keys()[-1]]
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
        if not checkConf(vmid):
            return {'error': "{} is not a valid CTID"}, 400
        # check if backup and config files exist
        if not os.path.isfile(fileimg) and not os.path.isfile(fileconf):
            return {'error': "unable to proceed, backup file or config file (or both) does not exist"}, 400
        if str(vmid) in r.smembers('joblock'):
            return {'error': "VMID locked, another operation is in progress for container: {}".format(vmid)}, 409

        r.hset(vmid, 'job', 'restore')
        r.hset(vmid, 'file', filename)
        r.hset(vmid, 'worker', '')
        r.hset(vmid, 'msg', '')
        r.hset(vmid, 'dest', path)
        r.hset(vmid, 'state', 'enqueued')
        r.sadd('joblock', vmid)

        return {'status': "restore job created for CTID {}".format(vmid)}, 202


class ListAllBackups(Resource):
    @auth.login_required
    def get(self):
        path = None

        # handle destination setting
        if 'dest' in request.args:
            result, response, err = checkDest(request.args['dest'])
            if result:
                path = locations[request.args['dest']]
            else:
                return response, err
        else:
            result, response, err = checkDest(locations.keys()[-1])
            if result:
                path = locations[locations.keys()[-1]]
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
                path = locations[request.args['dest']]
            else:
                return response, err
        else:
            result, response, err = checkDest(locations.keys()[-1])
            if result:
                path = locations[locations.keys()[-1]]
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
                path = locations[request.args['dest']]
            else:
                return response, err
        else:
            result, response, err = checkDest(locations.keys()[-1])
            if result:
                path = locations[locations.keys()[-1]]
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
        for spot in locations.keys():
            if os.path.isdir(locations[spot]):
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
            return {'error': "Problem determining AV status"}, 500


class Migrate(Resource):
    @auth.login_required
    def post(self, vmid):
        # Check if file is specified
        if 'file' not in request.args:
            return {'error': 'Resource requires a file argument'}, 400
        # Check if destination container does not exist
        if not checkConf(vmid):
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

class Inventory(Resource):
    @auth.login_required
    def get(self):
        result = {}
        hasArgs=False
        if 'cluster' in request.args:
            result['total'] = self.get_total()
            hasArgs=True
        if 'node' in request.args:
            node = request.args['node']
            if node in os.listdir('/etc/pve/nodes'):
                specificNodes={}
                specificNodes[node] = r.hget('inventory',node)
                result['nodes'] = specificNodes
            else:
                return {'error':"node does not exist in cluster"},400
            hasArgs=True
        if hasArgs:
            return result, 200
        container_count = {}
        for node in os.listdir('/etc/pve/nodes'):
            try:
                container_count[node] = r.hget('inventory',node)
            except:
                continue
        return {'total': self.get_total(), 'nodes': container_count}, 200

    def get_total(self):
        total = 0
        for node in os.listdir('/etc/pve/nodes'):
            total = total + int(r.hget('inventory',node))
        return total

api.add_resource(ListAllBackups, '/barque/')
api.add_resource(ListBackups, '/barque/<int:vmid>')
api.add_resource(Backup, '/barque/<int:vmid>/backup')
api.add_resource(BackupAll, '/barque/all/backup')
api.add_resource(Restore, '/barque/<int:vmid>/restore')
api.add_resource(DeleteBackup, '/barque/<int:vmid>/delete')
api.add_resource(Status, '/barque/<int:vmid>/status')
api.add_resource(AllStatus, '/barque/all/status')
api.add_resource(Info, '/barque/info')
api.add_resource(ClearQueue, '/barque/all/clear')
api.add_resource(CleanSnaps, '/barque/all/clean')
api.add_resource(Poison, '/barque/<int:vmid>/poison')
api.add_resource(AVtoggle, '/barque/avtoggle')
api.add_resource(Migrate, '/barque/<int:vmid>/migrate')
api.add_resource(Inventory, '/barque/count')


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


def checkConf(vmid):
    # catch if container does not exist
    config_file = ""
    config_target = "{}.conf".format(vmid)
    for paths, dirs, files in os.walk('/etc/pve/nodes'):
        if config_target in files:
            config_file = os.path.join(paths, config_target)
            print(config_file)
    if len(config_file) == 0:
        r.hset(vmid, 'state', 'error')
        r.hset(vmid, 'msg', '{} is invalid CTID'.format(vmid))
        return False
    return True


def checkDest(dest):
    if dest in locations:
        directory = locations[dest]
        if os.path.exists(directory):
            return True, None, None
        else:
            cmd = subprocess.check_output('/bin/bash /etc/pve/utilities/detect_stale.sh', shell=True)
            if os.path.exists(directory):
                return True, None, None
            else:
                return False, {'error': '{} is not currently accessible'.format(directory)}, 500
    else:
        return False, {'error': '{} is not a configured destination'.format(dest)}, 400


# remove old style storage
def convertAVLocks(node):
    if r.type(node) != 'hash':
        return
    locked_by = r.hget(node, "ctid")
    r.hdel(node, 'ctid')
    r.sadd(node, locked_by)


if __name__ == '__main__':
    starttime = datetime.utcnow().replace(microsecond=0)
    r = redis.Redis(host=r_host, port=r_port, password=r_pw)
    sanitize()
    log.info("redis connection successful")
    f = Foreman()
    f.start()
    log.debug("Foreman started")
    for i in range(minions):
        p = Worker()
        workers.append(p)
        p.start()
        log.debug("worker started")
    app.debug=False
    app.run(host=_host, port=_port, debug=False, use_reloader=False, ssl_context=(cert, key))
