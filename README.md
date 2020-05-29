# pveBarque
Asynchronous Flask service used to replace the vzdump feature included with Proxmox for systems using Ceph storage.
This service provides basic orchestration and management of backups, restorations, migration between clusters,
in addition to management certain applications specifically implemented by skysilk.

This application must be installed on a Proxmox node connected to the ceph cluster. Additionally, it uses the
clustered file system to access configuration files found on other proxmox nodes rather than the Proxmox API.
A Redis database is required for storing application state. A configurable number of worker processes are 
spawned by the main process, and each worker process independently checks application state and performs work.
