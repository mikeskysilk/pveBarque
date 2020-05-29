import itertools
import multiprocessing
import logging
import redis
import time
import os
import subprocess
import configparser
import barqueTarget
from datetime import datetime


class Worker(multiprocessing.Process):

    def run(self):
        global r
        licensed_to_live = True
        my = multiprocessing.current_process()
        self.name = '{}'.format(my.pid)
        self.stick = logging.getLogger('Barque.{}'.format(my.name))
        self.stick.debug("Process logging started")
        self.stick.info("{} started".format(self.name))
        r = redis.Redis(host=self.r_host, port=self.r_port,
                        password=self.r_pw, db=self.r_db)
        self.stick.debug("{} connected to redis".format(self.name))
        self.proxconn = ""

        # block for items in joblock
        while licensed_to_live:
            for job in r.smembers('joblock'):
                if r.hget(job, 'state') == 'enqueued':
                    r.hset(job, 'state', 'locked')
                    r.hset(job, 'worker', str(my.pid))
                    self.stick.debug(
                        "{} attempting to lock {}".format(self.name, job))
                    time.sleep(0.5)
                    # check if lock belongs to this worker
                    if r.hget(job, 'worker') == str(my.pid):
                        self.stick.debug(
                            "{} lock successful, proceeding".format(self.name))
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
                        elif task == 'destroy':
                            self.destroy(job)
                        else:
                            self.stick.error(
                                "{} unable to determine task {}".format(
                                    self.name, task))
                    else:
                        self.stick.debug(
                            "{} lock unsuccessful, reentering queue".format(self.name))
                # be nice to redis
                time.sleep(0.1)
            time.sleep(5)
        # licensed_to_live = False
        return

    def parseStorage(self):
        storages = []
        current = -1
        with open('/etc/pve/storage.cfg', 'r') as file:
            for line in file:
                if line.startswith('\n'):
                    continue
                if line.startswith('#'):
                    continue
                if not line.startswith('\t') and not line.startswith(' '):
                    current = current + 1
                    #print(line)
                    type, title = line.strip().split(": ")
                    storages.append({"name": title, "type": type})
                if line.startswith('\t'):
                    if ' ' in line:
                        k, v = line.strip().split()
                        storages[current][k] = v
        #print(storages)
        return storages

    def getInfo(self, vmid):
        """Sets and gets information for creating target. Returns boolean error,
        dictionary details"""

        details = {}
        details['resource_type'] = r.hget(vmid, 'type')
        details['destination'] = r.hget(vmid, 'dest')
        details['file_target'] = r.hget(vmid, 'file')
        details['host'] = r.hget(vmid, 'host')

        #disgusting legacy code below
        config_file = ""
        pool = ""
        vmdisk = ""
        if details['resource_type'] == "ct":
            config_file = "/etc/pve/nodes/{}/lxc/{}.conf".format(
                details['host'], vmid)
        if details['resource_type'] == "vm":
            config_file = "/etc/pve/nodes/{}/qemu-server/{}.conf".format(
                details['host'], vmid)
        parser = configparser.ConfigParser()
        try:
            with open(config_file) as lines:
                lines = itertools.chain(("[root]",), lines)
                parser.read_file(lines)
        except:
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', 'unable to open config file')
            return True, details
        if details['resource_type'] == "ct":
            try:
                storage, vmdisk = parser['root']['rootfs'].split(',')[
                                                                 0].split(':')
            except Exception:
                #print(e)
                r.hset(vmid, 'state', 'error')
                r.hset(vmid, 'msg', 'unable to get storage info from config file')
                return True, details
        if details['resource_type'] == "vm":
            try:
                storage, vmdisk = parser['root'][parser['root']['bootdisk']].split(',')[0].split(':')
            except Exception as e:
                #print(e)
                r.hset(vmid, 'state', 'error')
                r.hset(vmid, 'msg', 'unable to get storage info from config file')
                return True, details

        for store in self.parseStorage():
            if store['name'] == storage:
                try:
                    pool = store['pool']  # TODO: Fix - breaks if not present
                except Exception as e:
                    #print(e)
                    #print(storage)
                    r.hset(vmid, 'state', 'error')
                    r.hset(vmid, 'msg', 'Error parsing storage')
                    return True, details
        #print(storage)
        #print(vmdisk)
        #print(pool)
        details['ceph_pool'] = pool
        details['ceph_vmdisk'] = vmdisk
        details['proxconn'] = ""
        details['ha_group'] = ""
        #print(details)
        return False, details

    def backup(self, vmid):
        UNDO = True
        r.hset(vmid, 'state', 'active')
        self.stick.info("{}:backup -- Active".format(vmid))
        timestamp = datetime.strftime(datetime.utcnow(), "_%Y-%m-%d_%H-%M-%S")
        err, details = self.getInfo(vmid)
        if err:
            self.stick.error("{}: Error getting details".format(vmid))
            return
        #print(details)
        #print(details['ceph_vmdisk'])
        #print(timestamp)
        details['file_target'] = "".join((details['ceph_vmdisk'], timestamp))
        target = barqueTarget.Target(
            vmid,
            details['resource_type'],   #
            details['host'],            #
            details['destination'],     #
            details['file_target'],     #
            details['proxconn'],        #
            details['ceph_pool'],       #
            details['ceph_vmdisk'],     #
            details['ha_group'])        #

        # check if poisoned 1
        if r.hget(vmid, 'job') == 'poisoned':
            r.srem('joblock', vmid)
            r.hset(vmid, 'msg', 'Task successfully cancelled')
            r.hset(vmid, 'state', 'OK')
            return

        err, message = target.snapshot_create()
        self.stick.debug("{}:backup -- {}".format(vmid, message))
        r.hset(vmid, 'msg', message)
        if err:
            self.stick.warning("{}:backup -- {}".format(vmid, message))
            target.snapshot_create(UNDO)
            r.hset(vmid, 'state', 'error')
            return

        err, message = target.snapshot_protect()
        self.stick.debug("{}:backup -- {}".format(vmid, message))
        r.hset(vmid, 'msg', message)
        if err:
            self.stick.warning("{}:backup -- {}".format(vmid, message))
            target.snapshot_protect(UNDO)
            target.snapshot_create(UNDO)
            r.hset(vmid, 'state', 'error')
            return

        err, message = target.copy_config()
        self.stick.debug("{}:backup -- {}".format(vmid, message))
        r.hset(vmid, 'msg', message)
        if err:
            self.stick.warning("{}:backup -- {}".format(vmid, message))
            r.hset(vmid, 'state', 'error')
            return

        err, message = target.create_backup()
        self.stick.debug("{}:backup -- {}".format(vmid, message))
        r.hset(vmid, 'msg', message)
        if err:
            self.stick.warning("{}:backup -- {}".format(vmid, message))
            target.create_backup(UNDO)
            target.snapshot_protect(UNDO)
            target.snapshot_create(UNDO)
            r.hset(vmid, 'state', 'error')
            return

        err, message = target.snapshot_protect(UNDO)
        self.stick.debug("{}:backup -- {}".format(vmid, message))
        r.hset(vmid, 'msg', message)
        if err:
            self.stick.warning("{}:backup -- {}".format(vmid, message))
            r.hset(vmid, 'state', 'error')
            return

        err, message = target.snapshot_create(UNDO)
        self.stick.debug("{}:backup -- {}".format(vmid, message))
        r.hset(vmid, 'msg', message)
        if err:
            self.stick.warning("{}:backup -- {}".format(vmid, message))
            r.hset(vmid, 'state', 'error')
            return
        # mark complete and unlock CTID
        self.stick.info("{}:backup -- Done".format(vmid))
        r.hset(vmid, 'state', 'OK')
        r.srem('joblock', vmid)
        return

    def restore(self, vmid):
        UNDO = True
        r.hset(vmid, 'state', 'active')
        self.stick.info("{}:restore -- Active".format(vmid))
        err, details = self.getInfo(vmid)
        if err:
            self.stick.error("{}:Error getting details".format(vmid))
            return
        target = barqueTarget.Target(
            vmid,
            details['resource_type'],   #
            details['host'],            #
            details['destination'],     #
            details['file_target'],     #
            details['proxconn'],        #
            details['ceph_pool'],       #
            details['ceph_vmdisk'],     #
            details['ha_group'])        #

        target.ha_remove()  # pass on error, may already have been removed
        error, msg = target.stop()
        self.stick.debug("{}:restore -- {}".format(vmid, msg))
        if error:
            self.stick.warning("{}:restore -- {}".format(vmid, msg))
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', msg)
            return

        err, msg = target.unmap_rbd()
        self.stick.debug("{}:restore -- {}".format(vmid, msg))
        r.hset(vmid, 'msg', msg)
        if err:
            self.stick.warning("{}:restore -- {}".format(vmid, msg))
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', msg)
            return

        err, msg = target.snapshot_protect(UNDO)
        self.stick.debug("{}:restore -- {}".format(vmid, msg))
        r.hset(vmid, 'msg', msg)
        err, msg = target.snapshot_create(UNDO)
        self.stick.debug("{}:restore -- {}".format(vmid, msg))
        r.hset(vmid, 'msg', msg)

        err, msg = target.create_recovery()
        self.stick.debug("{}:restore -- {}".format(vmid, msg))
        r.hset(vmid, 'msg', msg)
        if err:
            self.stick.warning("{}:restore -- {}".format(vmid, msg))
            target.remove_recovery_snapshots()
            target.create_recovery(UNDO)
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', msg)
            return

        # print("removing storage")
        # err, msg = target.remove_storage()
        # r.hset(vmid, 'msg', msg)
        # if err:
        #     target.recover()
        #     r.hset(vmid, 'state', 'error')
        #     r.hset(vmid, 'msg', msg)
        #     return

        err, msg = target.extract_backup_image()
        self.stick.debug("{}:restore -- {}".format(vmid, msg))
        r.hset(vmid, 'msg', msg)
        if err:
            self.stick.warning("{}:restore -- {}".format(vmid, msg))
            target.extract_backup_image(UNDO)
            target.recover()
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', msg)
            return

        err, msg = target.import_backup_image()
        self.stick.debug("{}:restore -- {}".format(vmid, msg))
        r.hset(vmid, 'msg', msg)
        if err:
            self.stick.warning("{}:restore -- {}".format(vmid, msg))
            target.extract_backup_image(UNDO)
            target.recover()
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', msg)
            return

        err, msg = target.remove_recovery_snapshots()
        self.stick.debug("{}:restore -- {}".format(vmid, msg))
        r.hset(vmid, 'msg', msg)
        err, msg = target.create_recovery(UNDO)
        self.stick.debug("{}:restore -- {}".format(vmid, msg))
        r.hset(vmid, 'msg', msg)
        err, msg = target.extract_backup_image(UNDO)
        self.stick.debug("{}:restore -- {}".format(vmid, msg))
        r.hset(vmid, 'msg', msg)
        err, msg = target.snapshot_protect(UNDO)
        self.stick.debug("{}:restore -- {}".format(vmid, msg))
        r.hset(vmid, 'msg', msg)
        err, msg = target.snapshot_create(UNDO)
        self.stick.debug("{}:restore -- {}".format(vmid, msg))
        r.hset(vmid, 'msg', msg)
        err, msg = target.create_config()
        self.stick.debug("{}:restore -- {}".format(vmid, msg))
        r.hset(vmid, 'msg', msg)
        if err:
            self.stick.warning("{}:restore -- {}".format(vmid, msg))
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', msg)
            return
        self.stick.info("{}:restore -- Done".format(vmid))
        r.hset(vmid, 'state', 'OK')
        r.srem('joblock', vmid)
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
            cmd = subprocess.check_output(
                'rbd snap unprotect {}{}@barque'.format(pool, vmdisk), shell=True)
        except:
            # could not unprotect, maybe snap wasn't protected
            try:
                cmd = subprocess.check_output(
                    'rbd snap rm {}{}@barque'.format(pool, vmdisk), shell=True)
            except:
                r.hset(vmid, 'state', 'error')
                r.hset(
                    vmid, 'msg', "critical snapshotting error: unable to unprotect or remove")
                return
            # snapshot successfully removed, set OK
            r.hset(vmid, 'state', 'OK')
            r.hset(vmid, 'msg', 'snapshot successfully scrubbed - removed only')
            r.srem('joblock', vmid)
            return
        # snapshot successfully unprotected, attempt removal
        try:
            cmd = subprocess.check_output(
                'rbd snap rm {}{}@barque'.format(pool, vmdisk), shell=True)
        except:
            r.hset(vmid, 'state', 'error')
            r.hset(
                vmid, 'msg', 'critical snapshotting error: snap unprotected but unable to remove')
            return
        # snapshot successfully removed, set OK
        r.hset(vmid, 'state', 'OK')
        r.hset(vmid, 'msg', 'snapshot successfully scrubbed - unprotected and removed')
        r.srem('joblock', vmid)
        # retry = r.hget(vmid, 'retry')
        # if retry == 'backup': #REMOVE - used for testing snap scrubbing
        # 	r.hset(vmid, 'job', 'backup')
        # 	r.hset(vmid, 'state', 'enqueued')
        return

    def scrubDeep(self):
        r.hset('0', 'state', 'active')
        out = subprocess.check_output(
            'rbd ls {}'.format(pool.strip('/')), shell=True)
        for disk in out.split():
            cmd = subprocess.check_output(
                'rbd snap ls {}{}'.format(pool, disk), shell=True)
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
        UNDO = True
        r.hset(vmid, 'state', 'active')
        self.stick.info("{}:migrate -- Active".format(vmid))

        err, details = self.getInfo(vmid)
        if err:
            self.stick.error("{}:Error getting details".format(vmid))
            return

        # get info of target barque node
        # target_ip = ''
        # migration_rate = '50'
        # target_region, target_cluster_id, blank = re.split('(\d+)',target_cluster.lower())
        # if target_region in self.barque_ips:
        #     if target_cluster_id in self.barque_ips[target_region]:
        #         target_ip = self.barque_ips[target_region][target_cluster_id]
        #     else:
        #         r.hset(vmid, 'state','error')
        #         r.hset(vmid,'msg','cluster number not configured')
        # else:
        #     r.hset(vmid, 'state', 'error')
        #     r.hset(vmid, 'msg', 'Cluster region not configured')
        # target_path = self.barque_storage[target_cluster.lower()]
        migration_rate = '50'

        cmd = subprocess.check_output(
            "ssh root@{} \"cat {}{}\" | mbuffer -r {}M | lz4 -d - {}{}.img".format(
                r.hget(vmid, 'target_ip'),
                r.hget(vmid, 'target_path'),
                r.hget(vmid, 'file'),
                migration_rate,
                r.hget(vmid, 'dest'),
                vmid),
            shell=True)
        target = barqueTarget.Target(
            vmid,
            details['resource_type'],   #
            details['host'],            #
            details['destination'],     #
            details['file_target'],     #
            details['proxconn'],        #
            details['ceph_pool'],       #
            details['ceph_vmdisk'],     #
            details['ha_group'])        #

        target.ha_remove()  # pass on error, may already have been removed
        error, msg = target.stop()
        self.stick.debug("{}:migrate -- {}".format(vmid, msg))
        if error:
            self.stick.warning("{}:migrate -- {}".format(vmid, msg))
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', msg)
            return

        self.stick.debug("unmapping RBD again..?")
        err, msg = target.unmap_rbd()
        self.stick.debug("{}:migrate -- {}".format(vmid, msg))
        r.hset(vmid, 'msg', msg)
        if err:
            self.stick.warning("{}:migrate -- {}".format(vmid, msg))
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', msg)
            return

        self.stick.debug("unprotecting snapshot")
        err, msg = target.snapshot_protect(UNDO)
        self.stick.debug("{}:migrate -- {}".format(vmid, msg))
        r.hset(vmid, 'msg', msg)
        self.stick.debug("removing snapshot")
        err, msg = target.snapshot_create(UNDO)
        self.stick.debug("{}:migrate -- {}".format(vmid, msg))
        r.hset(vmid, 'msg', msg)

        self.stick.debug("creating recovery image")
        err, msg = target.create_recovery()
        self.stick.debug("{}:migrate -- {}".format(vmid, msg))
        r.hset(vmid, 'msg', msg)
        if err:
            self.stick.warning("{}:migrate -- {}".format(vmid, msg))
            target.create_recovery(UNDO)
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', msg)
            return

        # self.stick.debug("removing storage")
        # err, msg = target.remove_storage()
        # r.hset(vmid, 'msg', msg)
        # if err:
        #     target.recover()
        #     r.hset(vmid, 'state', 'error')
        #     r.hset(vmid, 'msg', msg)
        #     return

        # self.stick.debug("extracting backup image") ## Already extracted during transfer
        # err, msg = target.extract_backup_image()
        # r.hset(vmid, 'msg', msg)
        # if err:
        #     target.extract_backup_image(UNDO)
        #     target.recover()
        #     r.hset(vmid, 'state', 'error')
        #     r.hset(vmid, 'msg', msg)
        #     return

        self.stick.debug("importing backup image")
        err, msg = target.import_backup_image()
        self.stick.debug("{}:migrate -- {}".format(vmid, msg))
        r.hset(vmid, 'msg', msg)
        if err:
            self.stick.warning("{}:migrate -- {}".format(vmid, msg))
            target.extract_backup_image(UNDO)
            target.recover()
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', msg)
            return

        self.stick.debug("removing recovery image")
        err, msg = target.remove_recovery_snapshots()
        self.stick.debug("{}:migrate -- {}".format(vmid, msg))
        err, msg = target.create_recovery(UNDO)
        self.stick.debug("{}:migrate -- {}".format(vmid, msg))
        r.hset(vmid, 'msg', msg)
        self.stick.debug("removing extracted backup image")
        err, msg = target.extract_backup_image(UNDO)
        self.stick.debug("{}:migrate -- {}".format(vmid, msg))
        r.hset(vmid, 'msg', msg)
        self.stick.debug("cleaning up snapshots")
        err, msg = target.snapshot_protect(UNDO)
        self.stick.debug("{}:migrate -- {}".format(vmid, msg))
        r.hset(vmid, 'msg', msg)
        err, msg = target.snapshot_create(UNDO)
        self.stick.debug("{}:migrate -- {}".format(vmid, msg))
        r.hset(vmid, 'msg', msg)
        err, msg = target.update_config_disk()
        self.stick.debug("{}:migrate -- {}".format(vmid, msg))
        r.hset(vmid, 'msg', msg)
        err, msg = target.update_config_ostype()
        self.stick.debug("{}:migrate -- {}".format(vmid, msg))
        r.hset(vmid, 'msg', msg)
        # err, msg = target.create_config()
        # r.hset(vmid, 'msg', msg)
        # if err:
        #     r.hset(vmid, 'state', 'error')
        #     r.hset(vmid, 'msg', msg)
        #     return
        self.stick.info("{}:migrate -- Done".format(vmid))
        r.hset(vmid, 'state', 'OK')
        r.srem('joblock', vmid)
        return

    def destroy(self, vmid):
        """'unstoppable' destroy function."""

        UNDO = True
        r.hset(vmid, 'state', 'active')
        self.stick.info("{}:destroy -- Active".format(vmid))
        err, details = self.getInfo(vmid)
        if err:
            self.stick.error("{}:Error getting details".format(vmid))
            return
        target = barqueTarget.Target(
            vmid,
            details['resource_type'],   #
            details['host'],            #
            details['destination'],     #
            details['file_target'],     #
            details['proxconn'],        #
            details['ceph_pool'],       #
            details['ceph_vmdisk'],     #
            details['ha_group'])        #

        target.ha_remove()
        target.stop()
        target.snapshot_protect(UNDO)
        target.snapshot_create(UNDO)
        err, message = target.destroy()

        if not err:
            self.stick.debug("{}:destroy -- {}".format(vmid, message))
            r.hset(vmid, 'state', 'OK')
            r.srem('joblock', vmid)
            return
        self.stick.warning("{}:destroy -- {}".format(vmid, message))

        target.unmap_rbd()
        time.sleep(5)
        err, message = target.destroy()
        self.stick.debug("{}:destroy -- {}".format(vmid, message))
        if not err:
            self.stick.debug("{}:destroy -- {}".format(vmid, message))
            r.hset(vmid, 'state', 'OK')
            r.srem('joblock', vmid)
            return
        self.stick.warning("{}:destroy -- {}".format(vmid, message))

        time.sleep(90)
        err, message = target.destroy()
        self.stick.debug("{}:destroy -- {}".format(vmid, message))
        if not err:
            self.stick.debug("{}:destroy -- {}".format(vmid, message))
            r.hset(vmid, 'state', 'OK')
            r.srem('joblock', vmid)
            return
        self.stick.warning("{}:destroy -- {}".format(vmid, message))

        err, message = target.destroy_config()
        self.stick.debug("{}:destroy -- {}".format(vmid, message))
        if not err:
            self.stick.debug("{}:destroy -- {}".format(vmid, message))
            r.hset(vmid, 'state', 'OK')
            r.srem('joblock', vmid)
            return
        self.stick.warning("{}:destroy -- {}".format(vmid, message))

        r.hset(vmid, 'state', 'error')
        return

    def checkDest(self, dest):
        if dest in self.barque_storage[local_cluster]:
            directory = self.barque_storage[local_cluster][dest]
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

    def __init__(self, proxmox_host, proxmox_user, proxmox_password, r_host, r_port, r_pw, r_db, barque_ips, barque_storage, local_cluster):
        super(Worker, self).__init__()
        self.proxmox_host = proxmox_host
        self.proxmox_user = proxmox_user
        self.proxmox_password = proxmox_password
        self.r_host = r_host
        self.r_port = r_port
        self.r_pw = r_pw
        self.r_db = r_db
        self.barque_ips = barque_ips
        self.barque_storage = barque_storage
        self.local_cluster = local_cluster
