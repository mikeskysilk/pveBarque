import multiprocessing
import redis
import time
import logging
import subprocess
import os


class Foreman(multiprocessing.Process):

    def get_ct_count(self, node):
        cts = 0
        try:
            cts = int(subprocess.check_output(
                'ssh {} \"grep /var/lib/lxc/.*/command /proc/net/unix | wc -l\"'.format(
                    node),
                shell=True))
        except subprocess.CalledProcessError as e:
            print(e)
            cts = 0
        except Exception as e:
            print(e, cts)
            cts=0
        return cts

    def get_vm_count(self, node):
        vms = 0
        try:
            vms = int(subprocess.check_output(
                "ssh {} \"ls -l /var/run/qemu-server/*.pid | wc -l\"".format(
                    node), shell=True))

        except subprocess.CalledProcessError as e:
            print(e)
            vms = 0
        except Exception as e:
            print(e, vms)
            vms=0
        return vms

    def update_master_list(self):
        """Update redis database with new list of containers"""
        for node in os.listdir('/etc/pve/nodes'):
            try:
                self.redis.hset('inv_ct', node, self.get_ct_count(node))
                self.redis.hset('inv_vm', node, self.get_vm_count(node))
            except Exception as e:
                self.stick.warning("Error updating inventory. Error:{}".format(e))

    def set_update_time(self):
        self.redis.hset('foreman', 'update_time', time.time())

    def run(self):
        self.stick.info("Foreman logging started")
        while True:  # main loop
            self.update_master_list()
            self.set_update_time()
            time.sleep(30.0 - (time.time() % 30.0))

    def __init__(self, config):
        super(Foreman, self).__init__()
        self.cfg = config
        self.stick = logging.getLogger('Barque.Foreman')
        self.redis = redis.Redis(
            host=self.cfg['redis']['host'],
            port=self.cfg['redis']['port'],
            password=self.cfg['redis']['password'],
            db=self.cfg['redis']['database'],
            decode_responses=True
        )
