# pveBarque/common/utilities.py
import os
import subprocess
from common import CONFIG
from common import REDIS

def sanitize():
    pass
    # for item in r.smembers('joblock'):
    #     status = r.hget(item, 'state')
    #     if status != 'error':
    #         r.srem('joblock', item)
    #         r.hset(item, 'state', 'OK')


def checkDest(dest):
    '''
    Helper function which ensures that the storage destination is valid,
    is currently mounted, and will attemt to fix a stale nfs mount.
    Returns: Boolean:Exists, dict:Error message, int:desired error code number
    '''
    if dest in CONFIG['regions'][CONFIG['local']]['storage']:
        directory = CONFIG['regions'][CONFIG['local']]['storage'][dest]
        if os.path.exists(directory):
            return True, None, None
        subprocess.check_output(
            '/bin/bash /etc/pve/utilities/detect_stale.sh', shell=True)
        if os.path.exists(directory):
            return True, None, None
        return False, {'error': '{} is not currently accessible'\
            .format(directory)}, 500
    return False, {'error': '{} is not a configured destination'\
        .format(dest)}, 400

def checkConf(vmid):
    """Sets host and resource type in redis database, returns True if exists."""
    config_target = "{}.conf".format(vmid)
    for node in os.listdir('/etc/pve/nodes'):
        if config_target in os.listdir('/etc/pve/nodes/{}/lxc'.format(node)):
            REDIS.hset(vmid, "host", node)
            REDIS.hset(vmid, "type", "ct")
            return True
        if config_target in os.listdir('/etc/pve/nodes/{}/qemu-server'\
                                        .format(node)):
            REDIS.hset(vmid, "host", node)
            REDIS.hset(vmid, "type", "vm")
            return True
    return False
