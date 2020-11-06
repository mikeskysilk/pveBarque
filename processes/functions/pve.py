from shutil import copyfile
import os
import subprocess
import common.exceptions as exc


def config_copy(vmid, resource_type, host, destination, file_target):
    '''
    no error handling, just pass it up to the calling function
    '''

    #TODO: find a better else case exception

    if resource_type == "ct":
        copyfile(
            'etc/pve/nodes/{}/lxc/{}.conf'.format(host, vmid),
            "{}.conf".format(file_target)
        )
    elif resource_type == "vm":
        copyfile(
            'etc/pve/nodes/{}/qemu-server/{}.conf'.format(host, vmid),
            "{}.conf".format(file_target)
        )
    else:
        raise Exception("Unable to determine resource type")

def config_copy_remove(file_target):
    '''
    no error handling, just pass it up to the calling function
    '''

    os.remove("{}.conf".format(file_target))


def backup_remove(file_target):
    '''
    no error handling, just pass it up to the calling function
    '''

    os.remove("{}.lz4".format(file_target))

def ha_set(vmid, resource_type, ha_group, state):
    '''
    Set Proxmox HA manager state and HA group
    '''
    if resource_type == "ct":
        try:
            subprocess.check_output(
                "ha-manager set ct:{} --state={} --group {}".format(
                    vmid,
                    state,
                    ha_group
                ), shell=True
            )
        except Exception as err:
            raise exc.PveErrorNonFatal(err)
    elif resource_type == "vm":
        try:
            subprocess.check_output(
                "ha-manager set vm:{} --state={} --group {}".format(
                    vmid,
                    state,
                    ha_group
                ), shell=True
            )
        except Exception as err:
            raise exc.PveErrorNonFatal(err)
    else:
        raise exc.PveErrorFatal(
            "Unable to comprehend resource type: {}".format(resource_type)
        )
