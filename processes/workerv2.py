import multiprocessing
import logging
import subprocess
import redis
import time
import processes.functions.pve as pve
import processes.functions.ceph as ceph
import common.exceptions as exc
from common import CONFIG

UNDO = True

class Worker(multiprocessing.Process):

    def __init__(self, name):
        super(Worker, self).__init__()
        self.name = name

        self.stick = logging.getLogger("Barque.{}".format(self.name))
        self.stick.debug(
            "Logging initialized for worker with pid %s",
            multiprocessing.current_process().pid
        )

        self.redis = redis.Redis(
            host=CONFIG['redis']['host'],
            port=CONFIG['redis']['port'],
            password=CONFIG['redis']['password'],
            db=CONFIG['redis']['database'],
            decode_responses=True
        )
        self.stick.debug("redis initialized")

    def run(self):
        while True: # heavens forgive me
            #self.stick.debug("loop of life")
            for vmid in self.redis.smembers('joblock'):
                #self.stick.debug("found %s in joblock", vmid)
                if self.redis.hget(vmid, 'state') == 'enqueued':
                    self.redis.hset(vmid, 'state', 'locked')
                    self.redis.hset(vmid, 'worker', str(self.name))
                    self.stick.debug("attempting to lock vmid %s", vmid)
                    time.sleep(0.5)
                    if self.redis.hget(vmid, 'worker') == str(self.name):
                        self.stick.debug("sucessfully got lock on %s", vmid)
                        task = self.redis.hget(vmid, 'task')
                        if task == 'backup':
                            self.backup(vmid)
                        # elif task == 'restore':
                        #     self.restore(job)
                        # elif task == 'scrub':
                        #     self.scrubSnaps(job)
                        # elif task == 'migrate':
                        #     self.migrate(job)
                        elif task == 'poisoned':
                            self.poison(vmid)
                        # elif task == 'deepscrub':
                        #     self.scrubDeep()
                        elif task == 'destroy':
                            self.destroy(vmid)
                        else:
                            self.stick.error("unable to identify task: %s", task)
                            self.redis.hset(vmid, 'state', 'error')
                            self.redis.hset(vmid, 'msg', 'unable to identify task')
                time.sleep(1)
            time.sleep(5)


    def load_info(self, vmid):
        #TODO: set ceph_pool
        #TODO: set ceph_vmdisk
        #TODO: set resource_type
        #TODO: set file_target as full path of file
        #TODO: set ha_group as current ha_group, default to configured group
        pass


    def create_snapshot(self, vmid, undo=False):
        ceph_pool = self.redis.hget(vmid, "ceph_pool")
        ceph_vmdisk = self.redis.hget(vmid, "ceph_vmdisk")

        if not undo:
            try:
                ceph.snapshot_create(ceph_pool, ceph_vmdisk)
            except exc.CephErrorFatal as err:
                raise exc.WorkerError(err)
            except exc.CephErrorNonFatal as err:
                self.stick.error(err)
        else:
            try:
                ceph.snapshot_remove(ceph_pool, ceph_vmdisk)
            except exc.CephErrorFatal as err:
                raise exc.WorkerError(err)
            except exc.CephErrorNonFatal as err:
                self.stick.error(err)

    def protect_snapshot(self, vmid, undo=False):
        ceph_pool = self.redis.hget(vmid, "ceph_pool")
        ceph_vmdisk = self.redis.hget(vmid, "ceph_vmdisk")

        if not undo:
            try:
                ceph.snapshot_protect(ceph_pool, ceph_vmdisk)
            except exc.CephErrorFatal as err:
                raise exc.WorkerError(err)
            except exc.CephErrorNonFatal as err:
                self.stick.error(err)
        else:
            try:
                ceph.snapshot_unprotect(ceph_pool, ceph_vmdisk)
            except exc.CephErrorFatal as err:
                raise exc.WorkerError(err)
            except exc.CephErrorNonFatal as err:
                self.stick.error(err)

    def backup_save_config(self, vmid, undo=False):
        if not undo:
            try:
                pve.config_copy(
                    vmid,
                    self.redis.hget(vmid, "resource_type"),
                    self.redis.hget(vmid, "host"),
                    self.redis.hget(vmid, "destination"),
                    self.redis.hget(vmid, "file_target")
                )
            except Exception as err:
                raise exc.WorkerError(err)
        else:
            try:
                pve.config_copy_remove(
                    self.redis.hget(vmid, "file_target")
                )
            except Exception as err:
                self.stick.error(err)

    def backup_export_rbd(self, vmid, undo=False):
        ceph_pool = self.redis.hget(vmid, "ceph_pool")
        ceph_vmdisk = self.redis.hget(vmid, "ceph_vmdisk")
        file_target = self.redis.hget(vmid, "file_target")
        if not undo:
            try:
                ceph.backup_export(ceph_pool, ceph_vmdisk, file_target)
            except exc.CephErrorNonFatal as err:
                self.stick.error(err)
            except exc.CephErrorFatal as err:
                raise exc.WorkerError(err)
        else:
            try:
                pve.backup_remove(file_target)
            except Exception as err:
                self.stick.error(err)

    def ha_remove(self, vmid, undo=False):
        resource_type = self.redis.hget(vmid, "resource_type")
        ha_group = self.redis.hget(vmid, "ha_group")
        if not undo:
            try:
                pve.ha_set(vmid, resource_type, ha_group, "removed")
            except exc.PveErrorNonFatal as err:
                self.stick.error(err)
            except exc.PveErrorFatal as err:
                raise exc.WorkerError(err)
        else:
            try:
                pve.ha_set(vmid, resource_type, ha_group, "stopped")
            except exc.PveErrorNonFatal as err:
                self.stick.error(err)
            except exc.PveErrorFatal as err:
                raise exc.WorkerError(err)

    def stop(self, vmid):
        resource_type = self.redis.hget(vmid, "resource_type")
        try:
            pve.stop(vmid, resource_type)
        except exc.PveErrorNonFatal as err:
            self.stick.error(err)



    def backup(self, vmid):
        self.redis.hset(vmid, 'state', 'active')
        self.stick.debug("%s:backup -- Active", vmid)

        try:
            self.load_info(vmid)
            if self.redis.hget(vmid, 'state', 'poisoned'):
                self.redis.hset(vmid, 'state', 'OK')
                self.redis.srem('joblock', vmid)
                self.stick.info("%s:backup -- cancelled successfully", vmid)
                return

            self.create_snapshot(vmid)
            self.protect_snapshot(vmid)
            self.stick.debug("%s:backup -- created and protected snapshot", vmid)

            self.backup_save_config(vmid)
            self.backup_export_rbd(vmid)
            self.stick.info("%s:backup -- created backup files", vmid)
            if self.redis.hget(vmid, 'state', 'poisoned'):
                self.backup_save_config(vmid, UNDO)
                self.backup_export_rbd(vmid, UNDO)
                self.redis.hset(vmid, 'state', 'OK')
                self.redis.srem('joblock', vmid)
                self.stick.info("%s:backup -- cancelled successfully", vmid)
                return

            self.protect_snapshot(vmid, UNDO)
            self.create_snapshot(vmid, UNDO)

        except exc.WorkerError as err:
            self.redis.hset(vmid, 'state', 'error')
            self.redis.hset(vmid, 'msg', err)
            self.stick.error("%s:backup -- encountered error: %s", vmid, err)
            return

        self.redis.hset(vmid, 'state', 'OK')
        self.redis.srem('joblock', vmid)
        self.stick.debug('%s:backup -- backup job complete', vmid)


    def poison(self, vmid):
        state = self.redis.hget(vmid, 'state')
        if state == 'locked':
            self.redis.hset(vmid, 'msg', 'task cancelled')
            self.redis.hset(vmid, 'state', 'OK')
        elif state == 'error':
            self.redis.hset(vmid, 'msg', 'error state cleared')
            self.redis.hset(vmid, 'state', 'OK')
        else:
            self.stick.error("%s:poison -- unable to determine job state")
            self.redis.hset(vmid, 'state', 'error')


    def destroy(self, vmid):
        self.redis.hset(vmid, 'state', 'active')
        self.stick.debug("%s:destroy -- Active", vmid)

        try:
            self.load_info(vmid)
            if self.redis.hget(vmid, 'state', 'poisoned'):
                self.redis.hset(vmid, 'state', 'OK')
                self.redis.srem('joblock', vmid)
                self.stick.info("%s:destroy -- cancelled successfully", vmid)
                return

            self.ha_remove(vmid)
            self.stop(vmid)
            self.protect_snapshot(vmid, UNDO)
            self.create_snapshot(vmid, UNDO)

            self.destroy_normal(vmid)
            if not self.destroy_check_exists(vmid):
                self.stick.info("%s:destroy -- destroyed normally")
                self.redis.hset(vmid, 'msg', 'Destroyed successfully')
                self.redis.hset(vmid, 'state', 'OK')
                self.redis.srem('joblock', vmid)
                return

            self.destroy_unmap_rbd(vmid)
            time.sleep(5)
            self.destroy_normal(vmid)
            if not self.destroy_check_exists(vmid):
                self.stick.info("%s:destroy -- destroyed after unmapping rbd")
                self.redis.hset(vmid, 'msg', 'Destroyed successfully')
                self.redis.hset(vmid, 'state', 'OK')
                self.redis.srem('joblock', vmid)
                return

            time.sleep(90)
            self.destroy_normal(vmid)
            if not self.destroy_check_exists(vmid):
                self.stick.info("%s:destroy -- destroyed after waiting")
                self.redis.hset(vmid, 'msg', 'Destroyed successfully')
                self.redis.hset(vmid, 'state', 'OK')
                self.redis.srem('joblock', vmid)
                return

            self.destroy_remove_config(vmid)
            if not self.destroy_check_exists(vmid):
                self.stick.warning("%s:destroy -- unable to destroy, removed config")
                self.redis.hset(vmid, 'msg', 'Destroyed partially')
                self.redis.hset(vmid, 'state', 'error')
                self.redis.srem('joblock', vmid)
                return

            self.redis.hset(vmid, 'msg', 'Unable to destroy')
            self.redis.hset(vmid, 'state', 'error')
            return

        except exc.WorkerError as err:
            self.redis.hset(vmid, 'state', 'error')
            self.redis.hset(vmid, 'msg', err)
            self.stick.error("%s:backup -- encountered error: %s", vmid, err)
