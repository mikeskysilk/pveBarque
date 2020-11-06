import os
import subprocess
import common.exceptions as exc

def snapshot_create(ceph_pool, ceph_vmdisk):
    output = ""
    try:
        output = subprocess.check_output(
            'rbd snap ls {}/{}'.format(
                ceph_pool,
                ceph_vmdisk
            ),
            shell=True
        )
    except Exception as err:
        raise exc.CephErrorFatal(
            "error reading snapshots for {}/{}, error: {}".format(
                ceph_pool,
                ceph_vmdisk,
                err
            )
        )

    if "barque" in output:
        try:
            subprocess.check_output(
                'rbd snap rm {}/{}@barque'.format(
                    ceph_pool,
                    ceph_vmdisk
                )
            )
            subprocess.check_output(
                'rbd snap create {}/{}@barque'.format(
                    ceph_pool,
                    ceph_vmdisk
                )
            )

            raise exc.CephErrorNonFatal(
                "Barque snapshot previously existed for {}/{} and has been "
                "replaced.".format(
                    ceph_pool,
                    ceph_vmdisk
                )
            )
        except Exception as err:
            raise exc.CephErrorFatal(
                "Barque snapshot previously existed for {}/{} and could not be "
                "replaced, got error: {}".format(
                    ceph_pool,
                    ceph_vmdisk,
                    err
                )
            )
    else:
        try:
            subprocess.check_output(
                'rbd snap create {}/{}@barque'.format(
                    ceph_pool,
                    ceph_vmdisk
                )
            )
        except Exception as err:
            raise exc.CephErrorFatal(
                "Unable to create barque snapshot for {}/{}, ran into error: {}"
                .format(
                    ceph_pool,
                    ceph_vmdisk,
                    err
                )
            )

def snapshot_remove(ceph_pool, ceph_vmdisk):
    output = ""
    try:
        output = subprocess.check_output(
            'rbd snap ls {}/{}'.format(
                ceph_pool,
                ceph_vmdisk
            ),
            shell=True
        )
    except Exception as err:
        raise exc.CephErrorFatal(
            "error reading snapshots for {}/{}, error: {}".format(
                ceph_pool,
                ceph_vmdisk,
                err
            )
        )
    if "barque" in output:
        try:
            subprocess.check_output(
                'rbd snap rm {}/{}@barque'.format(
                    ceph_pool,
                    ceph_vmdisk
                )
            )
        except Exception as err:
            raise exc.CephErrorFatal(
                "Barque snapshot could not be removed from {}/{}, got error: {}"
                .format(
                    ceph_pool,
                    ceph_vmdisk,
                    err
                )
            )
    else:
        raise exc.CephErrorNonFatal(
            "Barque snapshot could not be found"
        )

def snapshot_protect(ceph_pool, ceph_vmdisk):
    output = ""
    try:
        output = subprocess.check_output(
            'rbd snap ls {}/{}'.format(
                ceph_pool,
                ceph_vmdisk
            ),
            shell=True
        )
    except Exception as err:
        raise exc.CephErrorFatal(
            "error reading snapshots for {}/{}, error: {}".format(
                ceph_pool,
                ceph_vmdisk,
                err
            )
        )
    if "barque" in output:
        try:
            subprocess.check_output(
                'rbd snap protect {}/{}@barque'.format(
                    ceph_pool,
                    ceph_vmdisk
                ),
                shell=True
            )
        except Exception as err:
            raise exc.CephErrorFatal(
                "Unable to protect barque snapshot for {}/{}, got error: {}"
                .format(
                    ceph_pool,
                    ceph_vmdisk,
                    err
                )
            )
    else:
        raise exc.CephErrorFatal(
            "unable to protect barque snapshot, snapshot is missing from {}/{}"
            .format(
                ceph_pool,
                ceph_vmdisk
            )
        )

def snapshot_unprotect(ceph_pool, ceph_vmdisk):
    output = ""
    try:
        output = subprocess.check_output(
            'rbd snap ls {}/{}'.format(
                ceph_pool,
                ceph_vmdisk
            ),
            shell=True
        )
    except Exception as err:
        raise exc.CephErrorFatal(
            "error reading snapshots for {}/{}, error: {}".format(
                ceph_pool,
                ceph_vmdisk,
                err
            )
        )

    if "barque" in output:
        try:
            subprocess.check_output(
                'rbd snap unprotect {}/{}@barque'.format(
                    ceph_pool,
                    ceph_vmdisk
                ),
                shell=True
            )
        except Exception as err:
            raise exc.CephErrorNonFatal(
                "Unable to unprotect barque snapshot for {}/{}, got error: {}"
                .format(
                    ceph_pool,
                    ceph_vmdisk,
                    err
                )
            )
    else:
        raise exc.CephErrorNonFatal(
            "unable to protect barque snapshot, snapshot is missing from {}/{}"
            .format(
                ceph_pool,
                ceph_vmdisk
            )
        )

def backup_export(ceph_pool, ceph_vmdisk, file_target):
    try:
        subprocess.check_output(
            "rbd export --rbd-concurrent-management-ops 20 --export-format 2 "
            "{}/{}@barque - | lz4 -1 - {}.lz4".format(
                ceph_pool,
                ceph_vmdisk,
                file_target
            ), shell=True
        )
    except Exception as err:
        raise exc.CephErrorFatal(
            "Unable to export rbd image for {}/{}, got error: {}".format(
                ceph_pool,
                ceph_vmdisk,
                err
            )
        )
