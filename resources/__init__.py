
from resources import av_toggle
from resources import backup_all
from resources import backup
from resources import clean_snapshots
from resources import clear_queue
from resources import count
from resources import delete
from resources import destroy
from resources import info
from resources import list_all
from resources import list_backups
from resources import migrate
from resources import poison
from resources import restore
from resources import status
import common

def __init__(api, version, start_time):
    api.add_resource(list_all.ListAllBackups, '/barque/')
    api.add_resource(list_backups.ListBackups,'/barque/<int:vmid>')
    api.add_resource(backup.Backup, '/barque/<int:vmid>/backup')
    api.add_resource(backup_all.BackupAll, '/barque/all/backup')
    api.add_resource(restore.Restore, '/barque/<int:vmid>/restore')
    api.add_resource(delete.DeleteBackup, '/barque/<int:vmid>/delete')
    api.add_resource(status.Status, '/barque/<int:vmid>/status')
    api.add_resource(status.AllStatus, '/barque/all/status')
    api.add_resource(
        info.Info,
        '/barque/info',
        resource_class_kwargs={
            'redis': common.REDIS,
            'config': common.CONFIG,
            'version': version,
            'start_time': start_time}
        )
    api.add_resource(clear_queue.ClearQueue, '/barque/all/clear')
    api.add_resource(clean_snapshots.CleanSnapshots, '/barque/all/clean')
    api.add_resource(poison.Poison, '/barque/<int:vmid>/poison')
    api.add_resource(av_toggle.AVtoggle, '/barque/avtoggle')
    api.add_resource(migrate.Migrate, '/barque/<int:vmid>/migrate')
    api.add_resource(count.Count, '/barque/count')
    api.add_resource(destroy.Destroy, '/barque/<int:vmid>/destroyContainer')
