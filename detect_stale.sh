#!/bin/bash
#for mountpoint in $(ls /mnt/pve/)
for mountpoint in $(grep nfs /etc/mtab | awk '{print $2}')
do
read -t1 < <(stat -t "$mountpoint" 2>&-)
if [ -z "$REPLY" ] ; then
  echo "NFS mount $mountpoint stale. Removing..."
  umount -f -l "$mountpoint"
fi
done
