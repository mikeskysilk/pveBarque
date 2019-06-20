import re
import proxmoxer
import itertools
import multiprocessing
import logging
import redis
import time
import os
import subprocess
import configparser
from shutil import copyfile
from proxmoxer import ProxmoxAPI
from datetime import datetime


class Worker(multiprocessing.Process):

    def run(self):
        global r
        licensed_to_live = True
        my = multiprocessing.current_process()
        self.name = 'Process {}'.format(my.pid)
        self.stick = logging.getLogger('Barque.{}'.format(my.name))
        self.stick.debug("Process logging started")
        self.stick.info("{} started".format(self.name))
        r = redis.Redis(host=self.r_host, port=self.r_port, password=self.r_pw)
        self.stick.debug("{} connected to redis".format(self.name))

        # block for items in joblock
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
                        elif task == 'destroy':
                            self.destroy(job)
                        else:
                            self.stick.error(
                                "{} unable to determine task {}".format(
                                    self.name, task))
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
        destHealth = self.checkDest(destination)
        try:
            self.proxconn = ProxmoxAPI(self.proxmox_host,
                                       user=self.proxmox_user,
                                       password=self.proxmox_password,
                                       verify_ssl=False)
        except:
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', 'failed to connect to proxmox')
            return
        if not destHealth:
            cmd = subprocess.check_output('/bin/bash /etc/pve/utilities/detect_stale.sh', shell=True)
            print(cmd)
            destHealth = self.checkDest(destination)
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
            self.proxconn = ProxmoxAPI(self.proxmox_host,
                                       user=self.promox_user,
                                       password=self.proxmox_password,
                                       verify_ssl=False)
        except:
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', 'failed to connect to proxmox')
            return
        self.stick.debug("{} connected to proxmox".format(self.name))
        self.stick.info("{}:restore: Beginning restore process".format(vmid))
        config_file = ""
        node = ""
        destination = r.hget(vmid, 'dest')
        destHealth = self.checkDest(destination)
        if not destHealth:
            cmd = subprocess.check_output('/bin/bash /etc/pve/utilities/detect'
                                          '_stale.sh', shell=True)
            print(cmd)
            destHealth = self.checkDest(destination)
            if not destHealth:
                r.hset(vmid, 'msg', '{} storage destination is offline, unable'
                       ' to recover')
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

        # Remove from HA
        try:
            cmd = subprocess.check_output('ha-manager remove ct:{}'.format(vmid), shell=True)
            self.stick.debug("{}:restore: Removed CT from HA")
        except subprocess.CalledProcessError:
            self.stick.error("{}:restore: Unable to remove CT from HA")
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

        # Check if disk image exists
        exists = True
        try:
            subprocess.check_output('rbd info {}/{}'.format(pool,
                                                            vmdisk_current),
                                    shell=True)
        except subprocess.CalledProcessError:
            exists = False
            self.stick.debug('{}:restore: Disk image not present on first pass'
                             ''.format(vmid))
        # make recovery copy of container image
        if exists:
            self.stick.info('{}:restore: Creating disaster recovery image'
                            ''.format(vmid))
            r.hset(vmid, 'msg', 'creating disaster recovery image')
            imgcpy = subprocess.check_output(
                "rbd cp --rbd-concurrent-management-ops 20 {}/{} {}/{}-barque"
                "".format(pool,
                          vmdisk_current,
                          pool,
                          vmdisk_current),
                shell=True)
            self.stick.debug('{}:restore: finished creating disaster recovery'
                             'image: {}/{}-barque'.format(vmid,
                                                          pool,
                                                          vmdisk_current))
        # print("Waiting for poison test2")
        # time.sleep(15)
        # check if poisoned 2
        if r.hget(vmid, 'job') == 'poisoned':
            self.stick.debug('{}:restore: Reached Poisoned 2'.format(vmid))
            try:
                if exists:
                    # remove recovery copy
                    imgrm = subprocess.check_output("rbd rm {}/{}-barque".format(pool, vmdisk_current), shell=True)
                    self.stick.debug('{}:restore: Poisoned 2 - removed disaster recovery image'.format(vmid))
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

        ##
        # delete container storage
        ##

        # Helper functions which will be used repeatedly
        def unmap_rbd(self, force=False):
            '''
            Uses rbd unmap to unmap the rbd device, over ssh to the target node.
            Accepts boolean argument "force" to include the -o force flag on if True, absent if False
            Returns True if the unmap command was successful.
            '''
            time.sleep(5)
            if force:
                try:
                    cmd = subprocess.check_output(
                        'ssh {} \'rbd unmap -o force {}/{}\''.format(node, pool, vmdisk_current), shell=True
                        )
                except subprocess.CalledProcessError as e:
                    self.stick.error("{}:restore:unmapRBD: Unable to force unmap disk".format(vmid))
                    return False
            else:
                try:
                    cmd = subprocess.check_output(
                        'ssh {} \'rbd unmap {}/{}\''.format(node, pool, vmdisk_current), shell=True
                        )
                except subprocess.CalledProcessError as e:
                    self.stick.error("{}:restore:unmapRBD: Unable to unmap disk, probably still in use".format(vmid))
                    return False
            return True

        def rm_vmdisk(self):
            '''
            Uses rbd rm to remove the disk image.
            Returns True if the rm command was successful.
            '''
            time.sleep(5)
            try:
                cmd = subprocess.check_output('rbd rm {}/{}'.format(pool, vmdisk_current), shell=True)
            except subprocess.CalledProcessError as e:
                self.stick.error('{}:restore:rm_vmdisk Failed to remove disk with rbd rm'.format(vmid))
                return False
            return True

        # Wait some time for everything to settle down before surgery
        time.sleep(30)
        # Check if storage exists
        exists = True
        try:
            subprocess.check_output('rbd info {}/{}'.format(pool, vmdisk_current), shell=True)
        except subprocess.CalledProcessError as e:
            exists = False
            self.stick.debug('{}:restore: Disk image not present on first pass'.format(vmid))

        # Begin process of removing disk
        if exists:
            if rm_vmdisk(self):
                exists = False              # Proceed
            else:                           # Unmap routine
                if unmap_rbd(self):
                    if rm_vmdisk(self):
                        exists = False      # Proceed
                else:  # Unable to unmap, check if CT is stopped
                    try:
                        cmd = subprocess.check_output(
                            'ssh {} \'pct status {}\''.format(node, vmid),
                            shell=True)
                        if cmd.strip('\n').split(' ')[1] == "running":
                            try:
                                cmd = subprocess.check_output(
                                    'ssh {} \'pct stop {}\''.format(node, vmid),
                                    shell=True)
                                time.sleep(30)
                            except subprocess.CalledProcessError:
                                self.stick.debug(
                                    '{}:restore:disk_removal: Unable to stop '
                                    'container'.format(vmid))
                            if unmap_rbd(self):
                                if rm_vmdisk(self):
                                    exists = False
                                # else check for snapshots routine
                            else:
                                time.sleep(30)
                                if unmap_rbd(self, True):
                                    if rm_vmdisk(self):
                                        exists = False
                                    # else check for snapshots routine
                                else:  # holy fuck how did that not work??
                                    r.hset(vmid, 'msg', 'Error unmapping rbd')
                                    r.hset(vmid, 'state', 'error')
                                    return
                        else:   # assume container is stopped
                                # or stopped in error state
                            time.sleep(30)
                            if unmap_rbd(self,True):
                                if rm_vmdisk(self):
                                    exists = False
                                # else check for snapshots routine
                            else: # holy fuck how did that not work??
                                r.hset(vmid, 'msg', 'Error unmapping rbd')
                                r.hset(vmid, 'state', 'error')
                                return
                    except subprocess.CalledProcessError as e:
                        self.stick.debug('{}:restore:disk_removal: Unable to get status of ctid, hoping that it\'s off.'.format(vmid))
        if exists:  # Check for snapshots routine
            snaps = None
            try:  # Check for snapshots
                snaps = subprocess.check_output('rbd snap ls {}/{}'.format(pool, vmdisk_current), shell=True)
            except subprocess.CalledProcessError as e:  # Unable to get snapshots
                pass  # TODO: catch error
            if snaps:  #GodILovePython.jpg
                for snap in snaps.split('\n')[1:-1]:
                    snap_id = snap.split()[1]
                    try:
                        subprocess.check_output('rbd snap rm {}/{}@{}'.format(pool, vmdisk_current, snap_id), shell=True)
                    except subprocess.CalledProcessError as e:
                        # Attempt to unprotect and try removing again
                        self.stick.debug(
                            "{}:restore:disk_removal: Unable to remove snapshot"
                            " {}/{}@{}, attempting to unprotect".format(
                                vmid,
                                pool,
                                vmdisk_current,
                                snap_id
                                )
                            )
                        try:
                            subprocess.check_output('rbd snap unprotect {}/{}@{}'.format(pool, vmdisk_current, snap_id),
                                shell=True)
                            self.stick.debug("{}:restore:disk_removal: Unprotected snapshot {}/{}@{}".format(
                                vmid,
                                pool,
                                vmdisk_current,
                                snap_id
                                )
                             )
                        except subprocess.CalledProcessError:
                            self.stick.error("{}:restore:disk_removal Failed to unprotect snapshot, continuing on.".format(vmid))
                            # Catch and release error, if there is a problem it will error after the next attempt to remove
                        try:
                            subprocess.check_output('rbd snap rm {}/{}@{}'.format(pool, vmdisk_current, snap_id), shell=True)
                            self.stick.debug("{}:restore:disk_removal Removed snapshot {}".format(vmid, snap_id))
                        except:
                            self.stick.error("{}:restore:disk_removal Unable to remove image snapshot {}/{}@{}".format(
                                vmid,
                                pool,
                                vmdisk_current,
                                snap_id
                                )
                            )
                            r.hset(vmid, 'msg', 'Unable to remove snapshots')
                            r.hset(vmid, 'state', 'error')
                            return
                    # continue
                # End For loop
            else: # No snaps found
                self.stick.error("{}:restore:disk_removal Removal failed, and no snaps found. Not sure what to do so crash and burn".format(vmid))
                r.hset(vmid, 'msg', 'Unable to remove container disk, also no snapshots found')
        if exists:
            if rm_vmdisk(self):
                exists = False
            else:
                # Looks like we got a problem.
                r.hset(vmid, 'msg', 'Error removing disk image')
                r.hset(vmid, 'state', 'error')
                return

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
        try:
            cmd = subprocess.check_output('rbd snap unprotect {}/{}@barque'.format(pool, vmdisk_final), shell=True)
            self.stick.debug('{}:restore: unprotected barque snapshot'.format(vmid))
        except subprocess.CalledProcessError as e:
            self.stick.error('{}:restore: Unable to unprotect snapshot, assuming already unprotected'.format(vmid))
        try:
            cmd = subprocess.check_output('rbd snap rm {}/{}@barque'.format(pool, vmdisk_final), shell=True)
            self.stick.debug('{}:restore: removed barque snapshot'.format(vmid))
        except subprocess.CalledProcessError as e:
            self.stick.debug('{}:restore: unable to remove barque snapshot, continuing anyway'.format(vmid))
        # image attenuation for kernel params #Removed after switching to format 2
        # imgatten = subprocess.check_output("rbd feature disable {} object-map fast-diff deep-flatten".format(vmdisk), shell=True)
        # print(imgatten)

        # print("Waiting for poison test4")
        # time.sleep(15)
        # check if poisoned 4
        if r.hget(vmid, 'job') == 'poisoned':
            self.stick.debug('{}:restore: Reached Poisoned 4'.format(vmid))
            try:
                #
                cmd = subprocess.check_output('rbd snap unprotect {}/{}@barque'.format(pool, vmdisk_final), shell=True)
                self.stick.debug('{}:restore: unprotected barque snapshot'.format(vmid))
                # try removing the recovered image
                cmd = subprocess.check_output("rbd rm {}/{}".format(pool, vmdisk_current), shell=True)
                # try adding container image
                cmd = subprocess.check_output(
                    "rbd mv {}/{}-barque {}/{}".format(pool, vmdisk_current, pool, vmdisk_current), shell=True)
                print(cmd)
                r.hset(vmid, 'msg', 'Task successfully cancelled')
                r.hset(vmid, 'state', 'OK')
            except:
                r.hset(vmid, 'msg', 'Error cancelling restore, at poisoned 4')
                r.hset(vmid, 'state', 'error')
                return
            r.srem('joblock', vmid)
            return

        # Get current IP address
        ipline = ""
        with open(config_file, 'r') as current_conf:
            for line in current_conf:
                if line.startswith('net0:'):
                    ipline = line
                    break

        # replace config file, injecting IP address
        with open(config_file, 'w') as current_conf:
            with open(fileconf, 'r') as backup_conf:
                for line in backup_conf:
                    if line.startswith('net0:'):
                        current_conf.write(ipline)
                        continue
                    current_conf.write(line)

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
        # retry = r.hget(vmid, 'retry')
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
            self.proxconn = ProxmoxAPI(
                self.proxmox_host,
                user=self.proxmox_user,
                password=self.proxmox_password,
                verify_ssl=False)
        except Exception:
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', 'failed to connect to proxmox')
            return
        self.stick.debug("{} connected to proxmox".format(self.name))

        ##
        # Gather Information
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
                self.stick.debug("{}:migrate: config file found on node: {}"
                                 "".format(vmid, node))
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
        target_ip = ''
        migration_rate = '50'
        target_region, target_cluster_id, blank = re.split('(\d+)',target_cluster.lower())
        if target_region in self.barque_ips:
            if target_cluster_id in self.barque_ips[target_region]:
                target_ip = self.barque_ips[target_region][target_cluster_id]
            else:
                r.hset(vmid, 'state','error')
                r.hset(vmid,'msg','cluster number not configured')
        else:
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', 'Cluster region not configured')
        target_path = self.barque_storage[target_cluster.lower()]

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
                os.remove("{}{}.img".format(self.locations[self.locations.keys()[-1]], vmid))
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
                os.remove("{}{}.img".format(self.locations[self.locations.keys()[-1]], vmid))
            except:
                r.hset(vmid, 'msg', 'Poisoned: Unable to remove the retrieved container image')
                r.hset(vmid, 'state','error')
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
                os.remove("{}{}.img".format(self.locations[self.locations.keys()[-1]], vmid))
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
                    os.remove("{}{}.img".format(self.locations[self.locations.keys()[-1]], vmid))
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
                "rbd import --export-format 2 {}{}.img {}/{}".format(self.locations[self.locations.keys()[-1]], vmid, dest_pool,
                                                                     dest_disk), shell=True)
            #print(cmd)
        except:
            try:
                os.remove("{}{}.img".format(self.locations[self.locations.keys()[-1]], vmid))
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
                os.remove("{}{}.img".format(self.locations[self.locations.keys()[-1]], vmid))
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
            r.hset(vmid,'msg','Task successfully cancelled')
            r.hset(vmid,'state','OK')
            r.srem('joblock', vmid)
            return
        ##
        ## Clean up
        ##

        # TODO: Remove barque snapshot if present
        # unprotect barque snapshot
        cmd = subprocess.check_output('rbd snap unprotect {}/{}@barque'.format(dest_pool, dest_disk), shell=True)

        # delete barque snapshot
        cmd = subprocess.check_output('rbd snap rm {}/{}@barque'.format(dest_pool, dest_disk), shell=True)

        r.hset(vmid, 'msg', 'Cleaning up...')
        os.remove("{}{}.img".format(self.locations[self.locations.keys()[-1]], vmid))
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

        self.proxconn = ProxmoxAPI(self.proxmox_host, user=self.proxmox_user, password=self.proxmox_password, verify_ssl=False)
        self.stick.debug("{} connected to proxmox".format(self.name))

        ## Gather Information
        destHealth = self.checkDest(destination)
        if not destHealth:
            cmd = subprocess.check_output('/bin/bash /etc/pve/utilities/detect_stale.sh', shell=True)
            print(cmd)
            destHealth = self.checkDest(destination)
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
        export_image = dest_info['path'] + '/' + vmid + '/' + disk_image\
            + '.raw'
        export_info = dest_info['storage'] + ':' + vmid + '/' + disk_image\
            + '.raw'

        # Stop container
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


        # Export rbd image to .raw
            # Backup first maybe? ... just in case...
        subprocess.check_output(
            'qemu-img convert -f raw -O raw rbd:{}/{} {}'.format(
                rbd_pool,
                disk_image,
                export_image),
            shell=True)

        # Point container config to new image (via proxmox api?)
        self.proxconn.nodes(node).lxc(vmid).config.set(rootfs='{},{}'.format(
            export_info, disk_size))

        # Clean up old image
        subprocess.check_output(
            'rbd rm {}/{}'.format(
                rbd_pool,
                disk_image),
            shell=True)

        # exit
        r.hset(vmid, 'state', 'OK')
        r.srem('joblock', vmid)

    def destroy(self, vmid):
        """'unstoppable' destroy function."""
        node = ""
        config_file = ""
        storage = ""
        disk = ""
        pool = ""
        r.hset(vmid, 'state', 'active')

        # Connect to proxmox
        try:
            self.proxconn = ProxmoxAPI(self.proxmox_host,
                                       user=self.proxmox_user,
                                       password=self.proxmox_password,
                                       verify_ssl=False)
        except Exception:
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', 'failed to connect to proxmox')
            return
        self.stick.debug("{} connected to proxmox".format(self.name))

        ##
        # Gather Information
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
                self.stick.debug("{}:destroy: config file found on node: {}"
                                 "".format(vmid, node))
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
            storage, disk = parserCurr['root']['rootfs']\
                .split(',')[0].split(':')
            self.stick.info('{}:destroy: Storage info - storage={} disk={}'
                            ''.format(vmid, storage, disk))
        except Exception:
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg',
                   'unable to get storage info from active config file')
            return

        # get rbd pool from proxmox
        try:
            pool = self.proxconn.storage(storage).get()["pool"]
        except Exception:
            r.hset(vmid, 'state', 'error')
            r.hset(vmid, 'msg', 'unable to get pool information from proxmox')
            return

        ##
        #  Power off container
        ##
        r.hset(vmid, 'msg', 'removing from ha-manager')
        # Remove from HA
        try:
            cmd = subprocess.check_output(
                'ha-manager remove ct:{}'.format(vmid),
                shell=True)
            self.stick.debug("{}:destroy: Removed CT from HA".format(vmid))
        except subprocess.CalledProcessError:
            self.stick.error(
                "{}:destroy: Unable to remove CT from HA".format(vmid))

        # stop container if not already stopped
        r.hset(vmid, 'msg', 'stopping container')
        self.stick.info(
            '{}:destroy: Checking if container is stopped'.format(vmid))
        if not self.proxconn.nodes(
            node).lxc(
                vmid).status.current.get()["status"] == "stopped":
            try:
                self.stick.debug(
                    '{}:destroy: issuing stop command container'.format(vmid))
                self.proxconn.nodes(node).lxc(vmid).status.stop.create()
            except Exception:
                self.stick.error('{}:restore: unable to stop container - '
                                 'proxmox api request fault')
                r.hset(vmid, 'state', 'error')
                r.hset(vmid, 'msg', 'unable to stop container - proxmox api '
                                    'returned null')
                return
            timeout = time.time() + 60
            while True:  # wait for container to stop
                self.stick.debug(
                    '{}:destroy: checking if container has stopped'.format(vmid))
                status = self.proxconn.nodes(
                    node).lxc(vmid).status.current.get()["status"]
                self.stick.debug('{}:restore: container state: {}'.format(
                    vmid,
                    status))
                if status == "stopped":
                    self.stick.debug(
                        '{}:destroy: container stopped successfully'.format(vmid))
                    break
                elif time.time() > timeout:
                    self.stick.error(
                        '{}:destroy: unable to stop container - timeout')
                    r.hset(vmid, 'state', 'error')
                    r.hset(vmid, 'msg', 'Unable to stop container - timeout')
                    return
                time.sleep(10)
        else:
            self.stick.debug(
                '{}:destroy: container already stopped'.format(vmid))
        ##
        # Unmap the RBD disk
        ##

        r.hset(vmid, 'msg', 'Unmapping RBD image, forcefully')
        time.sleep(5)
        try:
            cmd = subprocess.check_output(
                'ssh {} \'rbd unmap -o force /dev/rbd/{}/{}\''.format(
                    node,
                    pool,
                    disk),
                shell=True)
        except subprocess.CalledProcessError:
            self.stick.error("{}:destroy:unmapRBD: Unable to force"
                             " unmap disk".format(vmid))
            time.sleep(10)  # Wait a bit longer, then try again
            try:
                cmd = subprocess.check_output(
                    'ssh {} \'rbd unmap -o force /dev/rbd/{}/{}\''.format(
                        node,
                        pool,
                        disk),
                    shell=True)
            except subprocess.CalledProcessError:
                self.stick.error("{}:destroy: Unable to force"
                                 " unmap disk".format(vmid))
                time.sleep(10)  # Wait a bit longer, then try proxmox delete

        ##
        # Remove snapshots
        ##
        snaps = None
        try: # Check for snapshots
            snaps = subprocess.check_output('rbd snap ls {}/{}'.format(pool, disk), shell=True)
        except subprocess.CalledProcessError as e: # Unable to get snapshots
            pass # TODO: catch error
        if snaps: #GodILovePython.jpg
            for snap in snaps.split('\n')[1:-1]:
                snap_id = snap.split()[1]
                try:
                    subprocess.check_output('rbd snap rm {}/{}@{}'.format(pool, disk, snap_id), shell=True)
                except subprocess.CalledProcessError as e:
                    # Attempt to unprotect and try removing again
                    self.stick.debug(
                        "{}:destory:disk_removal: Unable to remove snapshot"
                        " {}/{}@{}, attempting to unprotect".format(
                            vmid,
                            pool,
                            disk,
                            snap_id
                            )
                        )
                    try:
                        subprocess.check_output('rbd snap unprotect {}/{}@{}'.format(pool, disk, snap_id),
                            shell=True)
                        self.stick.debug("{}:destroy:disk_removal: Unprotected snapshot {}/{}@{}".format(
                            vmid,
                            pool,
                            disk,
                            snap_id
                            )
                         )
                    except subprocess.CalledProcessError:
                        self.stick.error("{}:destroy:disk_removal Failed to unprotect snapshot, continuing on.".format(vmid))
                        # Catch and release error, if there is a problem it will error after the next attempt to remove
                    try:
                        subprocess.check_output('rbd snap rm {}/{}@{}'.format(pool, disk, snap_id), shell=True)
                        self.stick.debug("{}:destroy:disk_removal Removed snapshot {}".format(vmid, snap_id))
                    except:
                        self.stick.error("{}:destroy:disk_removal Unable to remove image snapshot {}/{}@{}".format(
                            vmid,
                            pool,
                            disk,
                            snap_id
                            )
                        )
                        r.hset(vmid, 'msg', 'Unable to remove snapshots')
                        r.hset(vmid, 'state', 'error')
                        return
                # continue
            # End For loop
        else: # No snaps found
            self.stick.error("{}:destroy:disk_removal Removal failed, and no snaps found. Not sure what to do so crash and burn".format(vmid))
            r.hset(vmid, 'msg', 'Unable to remove container disk, also no snapshots found')


        ##
        # Delete container via proxmox
        ##

        r.hset(vmid, 'msg', 'Asking proxmox to delete container')
        try:
            cmd = self.proxconn.nodes(node).lxc(vmid).delete()
            self.stick.info(
                "{}:destroy: Successfully destroyed container".format(vmid))
        except Exception:
            self.stick.error(
                "{}:destroy: Error sending command to proxmox".format(vmid))
            self.stick.debug("{}:destroy: Destroy error: ".format(vmid) + cmd)
        timeout = time.time() + 60
        while time.time() < timeout:
            try:
                self.proxconn.nodes(node).lxc(vmid).status.current.get()
            except proxmoxer.core.ResourceException:
                self.stick.info(
                    '{}:destroy: Container no longer present'.format(vmid))
                break
            except Exception:
                self.stick.error("{}:destroy: Error getting container status "
                                 " from proxmox".format(vmid))
                r.hset(vmid, 'state', 'error')
                r.hset(vmid, 'msg', 'Error getting container status from '
                                    'proxmox')
            time.sleep(10)
        # Check if container was destroyed the first time
        status = ""
        try:
            self.proxconn.nodes(node).lxc(vmid).status.current.get()
            # Still alive, eh?
            time.sleep(120)  # Wait for a while longer, then try proxmox again
            try:
                cmd = self.proxconn.nodes(node).lxc(vmid).delete()
                self.stick.info(
                    "{}:destroy: Successfully destroyed container".format(vmid))
            except Exception:
                self.stick.error(
                    "{}:destroy: Error sending command to proxmox".format(vmid))
                self.stick.debug("{}:destroy: Destroy error: ".format(vmid)
                                 + cmd)
            timeout = time.time() + 60
            while time.time() < timeout:
                try:
                    self.proxconn.nodes(node).lxc(vmid).status.current.get()
                except proxmoxer.core.ResourceException:
                    self.stick.info(
                        '{}:destroy: Container no longer present'.format(vmid))
                    break
                except Exception:
                    self.stick.error(
                        "{}:destroy: Error getting container status from "
                        "proxmox".format(vmid))
                    r.hset(vmid, 'state', 'error')
                    r.hset(vmid, 'msg', 'Error getting container status from '
                                        'proxmox')
                time.sleep(10)

            try:
                self.proxconn.nodes(node).lxc(vmid).status.current.get()
                # give up.
                self.stick.error("{}:destroy: Proxmox is seriously unable to "
                                 "delete the container".format(vmid))
                r.hset(vmid, 'msg', 'Unable to delete container')
                r.hset(vmid, 'state', 'error')
                return
            except proxmoxer.core.ResourceException:
                self.stick.info(
                    '{}:destroy: Container no longer present'.format(vmid))
        except proxmoxer.core.ResourceException:
            self.stick.info(
                '{}:destroy: Container no longer present'.format(vmid))

        r.hset(vmid, 'msg', 'container deleted')
        r.hset(vmid, 'state', 'OK')
        r.srem('joblock', vmid)
        return

    def checkDest(self, dest):
        if dest in self.locations:
            directory = self.locations[dest]
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

    def __init__(self, proxmox_host, proxmox_user, proxmox_password, r_host, r_port, r_pw, barque_ips, barque_storage, locations):
        super(Worker, self).__init__()
        self.proxmox_host = proxmox_host
        self.proxmox_user = proxmox_user
        self.proxmox_password = proxmox_password
        self.r_host = r_host
        self.r_port = r_port
        self.r_pw = r_pw
        self.barque_ips = barque_ips
        self.barque_storage = barque_storage
        self.locations = locations
