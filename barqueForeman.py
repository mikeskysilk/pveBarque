import multiprocessing
import redis
import time
import logging
import subprocess
import os


class Foreman(multiprocessing.Process):

    def run(self):
        global r
        r = redis.Redis(host=self.r_host, port=self.r_port,
                        password=self.r_pw, db=self.r_db)
        self.stick = logging.getLogger('Barque.Foreman')
        self.stick.info("Foreman logging started")
        while True:  # main loop
            self.update_master_list()
            self.set_update_time()
            time.sleep(30.0 - (time.time() % 30.0))

    def get_ct_count(self, node):
        cts = 0
        try:
            socket_list = subprocess.check_output(
                'ssh {} \"awk \'{{ print \$8 }}\' /proc/net/unix | '
                'grep /var/lib/lxc/.*/command\"'.format(node),
                shell=True).split('\n')[:-1]
            ctids = {}
            for socket in socket_list:
                ctids[socket] = 1
            cts = len(ctids)
        except subprocess.CalledProcessError:
            cts = 0
        return cts

    def get_vm_count(self, node):
        vms = 0
        try:
            vms = len(subprocess.check_output(
                "ssh {} \"ls -l /var/run/qemu-server/ | grep pid\"".format(
                    node), shell=True).split('\n')[:-1])
        except subprocess.CalledProcessError:
            vms = 0
        return vms

    def update_master_list(self):
        """Update redis database with new list of containers"""
        for node in os.listdir('/etc/pve/nodes'):
            try:
                r.hset('inv_ct', node, self.get_ct_count(node))
                r.hset('inv_vm', node, self.get_vm_count(node))
            except Exception:
                self.stick.warning("Error updating inventory")

    def set_update_time(self):
        r.hset('foreman', 'update_time', time.time())

    def __init__(self, redis_host, redis_port, redis_password, r_db):
        super(Foreman, self).__init__()
        self.r_host = redis_host
        self.r_port = redis_port
        self.r_pw = redis_password
        self.r_db = r_db
