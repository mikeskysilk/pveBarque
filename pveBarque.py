from flask import Flask, request
from flask_restful import Resource, Api, reqparse, abort
from json import dumps, loads
from flask_jsonpify import jsonify
from datetime import datetime
from shutil import copyfile
from glob import glob
import subprocess, os, time, json, multiprocessing, redis

#defaults
__host = "192.168.100.11"
__port = 6969
path = "/tank/backup/barque/"
label = "api"

app = Flask(__name__)
api = Api(app)

r = None
r_host = 'localhost'
r_port = 6379
r_pw = '**'
workers = []

parser = reqparse.RequestParser()
parser.add_argument('file','vmid')

class Worker(multiprocessing.Process):
	r = None
	def run(self):
	 	licensed_to_live = True
	 	my = multiprocessing.current_process()
	 	name = 'Process {}'.format(my.pid)
	 	print("{} started".format(name))
	 	r = redis.Redis(host=r_host, port=r_port, password=r_pw)
	 	print("{} connected to redis".format(name))
	# 	#block for items in joblock
		while licensed_to_live:
		 	for job in r.smembers('joblock'):
		 		if r.hget(job, 'state') == 'enqueued':
		 			r.hset(job, 'state', 'locked')
		 			r.hset(job, 'worker', str(my.pid))
		 			print("{} attempting to lock {}".format(name, job))
		 			time.sleep(0.5)
		 			#check if lock belongs to this worker
		 			if r.hget(job, 'worker') == str(my.pid):
		 				print("{} lock successful, proceeding".format(name))
		 				task = r.hget(job, 'job')
		 				if task == 'backup':
		 					self.backup(job)
		 				elif task == 'restore':
		 					self.restore(job)
		 			else:
		 				print("{} lock unsuccessful, reentering queue".format(name))
		 		#be nice to redis
		 		time.sleep(0.1)
		 	time.sleep(5)
		 	#licensed_to_live = False
	 	return
	def backup(self,vmid):
		r.hset(vmid, 'state','active')
		vmdisk = 'vm-{}-disk-1'.format(vmid)
		timestamp = datetime.strftime(datetime.now(),"_%Y-%m-%d_%H-%M")
		config_file = ""
		config_target = "{}.conf".format(vmid)
		#get config file
		for paths, dirs, files in os.walk('/etc/pve/nodes'):
			if config_target in files:
				config_file = os.path.join(paths, config_target)
				print(config_file)
		#catch if container does not exist
		if len(config_file) == 0:
			r.hset(vmid, 'state', 'error')
			r.hset(vmid, 'msg', '{} is invalid CTID'.format(vmid))
			return
		#create snapshot for backup
		try:
			cmd = subprocess.check_output('rbd snap create {}@barque'.format(vmdisk), shell=True)
		except:
			r.hset(vmid, 'state', 'error')
			r.hset(vmid, 'msg', 'error creating backup snapshot')
			return
		#copy config file
		config_dest = "".join([path, vmdisk, timestamp, ".conf"])
		copyfile(config_file, config_dest)
		#protect snapshot during backup
		cmd = subprocess.check_output('rbd snap protect {}@barque'.format(vmdisk), shell=True)
		#create compressed backup file from backup snapshot
		dest = "".join([path, vmdisk, timestamp, ".lz4"])
		args = ['rbd export --export-format 2 {}@barque - | lz4 -9 - {}'.format(vmdisk, dest)]
		r.hset(vmid, 'state', 'Creating backup image')
		try:
			cmd = subprocess.check_output(args, shell=True)#.split('\n') #run command then convert output to list, splitting on newline
		except:
			r.hset(vmid, 'state', 'error')
			r.hset(vmid, 'msg', 'unable to aquire rbd image for CTID: {}'.format(vmid))
			return
		#unprotect barque snapshot
		cmd = subprocess.check_output('rbd snap unprotect {}@barque'.format(vmdisk), shell=True)
		#delete barque snapshot
		cmd = subprocess.check_output('rbd snap rm {}@barque'.format(vmdisk), shell=True)
		#mark complete and unlock CTID
		r.hset(vmid, 'state', 'complete')
		r.srem('joblock', vmid)
		return

	def restore(self, vmid):
		r.hset(vmid, 'state', 'active')
		filename = r.hget(vmid, 'file')
		config_file = ""
		node = ""
		vmdisk = 'vm-{}-disk-1'.format(vmid)
		fileimg = "".join([path, filename, ".lz4"])
		fileconf = "".join([path, filename, ".conf"])
		#find node hosting container
		config_target = "{}.conf".format(vmid)
		for paths, dirs, files in os.walk('/etc/pve/nodes'):
			if config_target in files:
				config_file = os.path.join(paths, config_target)
				node = config_file.split('/')[4]
				print(node)
		#stop container if not already stopped
		r.hset(vmid, 'state', 'stopping container')
		if not loads(subprocess.check_output("pvesh get /nodes/{}/lxc/{}/status/current".format(node,vmid), shell=True))["status"] == "stopped":
			ctstop = subprocess.check_output("pvesh create /nodes/{}/lxc/{}/status/stop".format(node, vmid), shell=True)
		timeout = time.time() + 60
		while True: #wait for container to stop
			stat = loads(subprocess.check_output("pvesh get /nodes/{}/lxc/{}/status/current".format(node,vmid), shell=True))["status"]
			print(stat)
			if stat == "stopped":
				break
			elif time.time() > timeout:
				return "timeout - unable to stop container", 500
		#make recovery copy of container image
		r.hset(vmid, 'state', 'creating disaster recovery image')
		imgcpy = subprocess.check_output("rbd cp {} {}-barque".format(vmdisk, vmdisk), shell=True)
		#delete container storage
		r.hset(vmid, 'state', 'removing container image')
		try:
			imgdel = subprocess.check_output("pvesh delete /nodes/{}/storage/rbd_ct/content/rbd_ct:{}".format(node, vmdisk), shell=True)
			print(imgdel)
		except:
			try: #attempt to force unmap image if has watchers
				cmd = subprocess.check_output("rbd unmap -o force {}".format(vmdisk))
			except:
				r.hset(vmid, 'state', 'error')
				r.hset(vmid, 'msg', "unable to unmap container image", shell=True)
				return
			try: #retry deleting image
				cmd = subprocess.check_output("pvesh delete /nodes/{}/storage/rbd_ct/content/rbd_ct:{}".format(node, vmdisk), shell=True)
			except:
				r.hset(vmid, 'state', 'error')
				r.hset(vmid, 'msg', "unable to remove container image", shell=True)
				return
		#extract lz4 compressed image file
		filetarget = "".join([path, filename, ".img"])
		uncompress = subprocess.check_output("lz4 -d {} {}".format(fileimg, filetarget), shell=True)
		print(uncompress)
		#import new image
		r.hset(vmid, 'state', 'importing backup image')
		try:
			rbdimp = subprocess.check_output("rbd import --export-format 2 {} {}".format(filetarget, vmdisk), shell=True)
			print(rbdimp)
		except:
			r.hset(vmid, 'state','error')
			r.hset(vmid, 'msg', rbdimp)
			cmd = subprocess.check_output("rbd mv {}-barque {}".format(vmdisk, vmdisk), shell=True)
			return
		#delete uncompressed image file
		r.hset(vmid, 'state', 'cleaning up')
		rmuncomp = subprocess.check_output("rm {}".format(filetarget), shell=True)
		print(rmuncomp)
		#delete barque snapshot
		cmd = subprocess.check_output('rbd snap rm {}@barque'.format(vmdisk), shell=True)
		#image attenuation for kernel params #Removed after switching to format 2
		# imgatten = subprocess.check_output("rbd feature disable {} object-map fast-diff deep-flatten".format(vmdisk), shell=True)
		# print(imgatten)
		#replace config file
		copyfile(fileconf, config_file)
		#start container
		ctstart = subprocess.check_output("pvesh create /nodes/{}/lxc/{}/status/start".format(node,vmid), shell=True)
		time.sleep(5)
		print(ctstart)
		#cleanup recovery copy
		cmd = subprocess.check_output("rbd rm {}-barque".format(vmdisk), shell=True)
		r.hset(vmid, 'state','complete')
		r.srem('joblock', vmid)
		return

class Backup(Resource):
	def post(self, vmid):
		if 'retry' in request.args and r.hget(vmid, 'state') == 'error':
			r.srem('joblock', vmid)
		if str(vmid) in r.smembers('joblock'):
			return {'error':"VMID locked, another operation is in progress for container: {}".format(vmid), 
				'status': r.hget(vmid,'state'),'job':r.hget(vmid, 'job')}, 400
		else:
			r.hset(vmid, 'job','backup')
			r.hset(vmid, 'file', '')
			r.hset(vmid, 'worker', '')
			r.hset(vmid, 'state', 'enqueued')
			r.sadd('joblock', vmid)

		return {'status': "backup job created for CTID {}".format(vmid)}, 201

class BackupAll(Resource):
	def post(self):
		targets = []
		response = []
		for paths, dirs, files in os.walk('/etc/pve/nodes'):
			for f in files:
				if f.endswith(".conf"):
					targets.append(f.split(".")[0])
		for vmid in targets:
			if str(vmid) in r.smembers('joblock'):
				response.append({"CTID": vmid, "status" : "error", "message": "CTID locked, another operation is in progress"})
			else:
				r.hset(vmid, 'job', 'backup')
				r.hset(vmid, 'file', '')
				r.hset(vmid, 'worker', '')
				r.hset(vmid, 'state', 'enqueued')
				r.sadd('joblock', vmid)
				response.append({"CTID": vmid, "status":"OK", "message":"Backup job added to queue"})
		return response

class Restore(Resource):
	def post(self,vmid):
		if 'file' not in request.args:
			return {'error': 'Resource requires a file argument'}, 400
		filename = os.path.splitext(request.args['file'])[0]
		fileimg = "".join([path, filename, ".lz4"])
		fileconf = "".join([path, filename, ".conf"])
		
		#check if backup and config files exist
		if not os.path.isfile(fileimg) and not os.path.isfile(fileconf):
			return {'error': "unable to proceed, backup file or config file (or both) does not exist"}, 400	
		if str(vmid) in r.smembers('joblock'):
			return {'error':"VMID locked, another operation is in progress for container: {}".format(vmid)}, 400

		r.hset(vmid, 'job','restore')
		r.hset(vmid, 'file', filename)
		r.hset(vmid, 'worker', '')
		r.hset(vmid, 'state', 'enqueued')
		r.sadd('joblock', vmid)

		return {'status': "restore job created for CTID {}".format(vmid)}, 201

class ListAllBackups(Resource):
	def get(self):
		result = []
		confs = []
		for paths, dirs, files in os.walk(path):
			for f in files:
				if f.endswith('.lz4'):
					result.append(f)
				elif f.endswith('.conf'):
					confs.append(f)
		return {'all backups': result, 'config files': confs}

class ListBackups(Resource):
	def get(self, vmid):
		files = sorted(os.path.basename(f) for f in glob("".join([path, "vm-{}-disk-1*.lz4".format(vmid)])))
		return {'backups': files}
class DeleteBackup(Resource):
	def post(self,vmid):
		if str(vmid) in r.smembers('joblock'):
			return {'error': 'CTID locked, another operation is in progress for container {}'.format(vmid)}, 400
		r.hset(vmid, 'state', 'locked')
		r.hset(vmid, 'job', 'delete')
		r.sadd('joblock', vmid)
		if 'file' in request.args:
			print(request.args['file'])
			fileimg = "".join([path, request.args['file']])
			fileconf = "".join([os.path.splitext(fileimg)[0],".conf"])
			if os.path.isfile(fileimg):
				os.remove(fileimg)
				if os.path.isfile(fileconf):
					os.remove(fileconf)
				r.srem('joblock', vmid)
				return {'file removed': os.path.basename(fileimg)}
			else:
				r.srem('joblock', vmid)
				return {'file does not exist': os.path.basename(fileimg)}
		else:
			r.srem('joblock', vmid)
			return "resource requires a file argument", 400

class Status(Resource):
	def get(self, vmid):
		if str(vmid) in r.smembers('joblock'):
			status = r.hget(vmid, 'state')
			if status == 'error':
				msg = r.hget(vmid, 'msg')
				return {'status': status, 'error message': msg}, 401
			return {'status': status}, 200
		return {'status': "No active tasks"}
class AllStatus(Resource):
	def get(self):
		response = []
		for worker in workers:
			print(worker)
		for vmid in r.smembers('joblock'):
			status = r.hget(vmid, 'state')
			if status == 'error':
				msg = r.hget(vmid, 'msg')
				response.append({"CTID": vmid, "status": "error", "message": msg})
			else:
				response.append({"CTID": vmid, "status": "OK", "message": status})
		return response


api.add_resource(ListAllBackups, '/barque/')
api.add_resource(ListBackups, '/barque/<int:vmid>')
api.add_resource(Backup, '/barque/<int:vmid>/backup')
api.add_resource(BackupAll, '/barque/all/backup')
api.add_resource(Restore, '/barque/<int:vmid>/restore')
api.add_resource(DeleteBackup, '/barque/<int:vmid>/delete')
api.add_resource(Status, '/barque/<int:vmid>/status')
api.add_resource(AllStatus, '/barque/all/status')

def sanitize():
	for item in r.smembers('joblock'):
		status = r.hget(item, 'state')
		if status != 'error':
			r.srem('joblock',item)
if __name__ == '__main__':
	r = redis.Redis(host=r_host, port=r_port, password=r_pw)
	sanitize()
	print("redis connection successful")
	for i in range(5):
		p = Worker()
		workers.append(p)
		p.start()
		print("worker started")
	app.run(host=__host,port=__port, debug=True)