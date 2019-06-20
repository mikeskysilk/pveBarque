import multiprocessing
import redis
import time
import logging
import subprocess
import os
from json import loads


class Foreman(multiprocessing.Process):

    def run(self):
        global r
        r = redis.Redis(host=self.r_host, port=self.r_port, password=self.r_pw)
        self.stick = logging.getLogger('Barque.Foreman')
        self.stick.info("Foreman logging started")
        while True:
            self.stick.debug("Foreman updating inventory")
            containers = self.update_inventory()
            self.update_master_list(containers)
            time.sleep(30.0 - (time.time() % 30.0))

    def update_inventory(self):
        containers = {}
        for node in os.listdir('/etc/pve/nodes'):
            # self.stick.debug("Getting list for node {}".format(node))
            try:
                socket_list = subprocess.check_output(
                    'ssh {} \"awk \'{{ print \$8 }}\' /proc/net/unix | '
                    'grep /var/lib/lxc/.*/command\"'.format(node),
                    shell=True).split('\n')[:-1]
            except subprocess.CalledProcessError:
                containers[node] = "0"
                continue
            containers[node] = len(socket_list)
        return containers

    def update_master_list(self, containers):
        """Update redis database with new list of containers"""
        for key in containers:
            r.hset('inventory', key, containers[key])

    def get_node_utilizations(self):
        node_loads = {}
        for node in os.listdir('/etc/pve/nodes'):
            try:
                node_status = subprocess.check_output(
                    'pvesh get nodes/{}/status'.format(node), shell=True
                    )
            except subprocess.CalledProcessError:
                self.stick.error('Unable to get status of {}'.format(node))
                continue
            try:
                node_load = loads(node_status)['loadavg'][2]
                node_loads[node] = node_load
            except Exception:
                self.stick.error(
                    'Unable to parse status json for node {}'.format(node)
                    )
                continue

    def __init__(self, redis_host, redis_port, redis_password):
        super(Foreman, self).__init__()
        self.r_host = redis_host
        self.r_port = redis_port
        self.r_pw = redis_password
