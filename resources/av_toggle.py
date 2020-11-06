# pvebarque/resources/av_toggle.py
from flask_restful import Resource
from flask import request


class AVtoggle(Resource):
    def __init__(self, redis):
        self.r = redis

    def post(self):
        if 'node' not in request.args:
            return {'error':
                    'Node argument required beacuse... it needs a node'}, 400
        node = request.args['node']

        convertAVLocks(node)
        # if 'ctid' not in request.args:
        # 	return {'error': 'Container ID required for locking'}, 400
        if 'switch' not in request.args:
            return {'error': 'switch not specified, should be "on" or "off"'}, 400

        # timestamp = time.time()
        if 'ctid' not in request.args:
            return {'error': 'CTID not specified, required for racing lock'}, 400

        ctid = request.args['ctid']

        if request.args['switch'] == 'off':
            ctid = request.args['ctid']

            try:
                print("node: {}, ctid: {}".format(node, ctid))
                r.sadd(node, ctid)
                cmd = subprocess.check_output(
                    "ssh -t root@{} '/opt/sophos-av/bin/savdctl disable'".format(node), shell=True)
                print("disabling antivirus on node: {}, output: {}".format(node, cmd))
            except:
                r.srem(node, ctid)
                return {'state': 'error'}, 200
            return {'state': 'disabling'}, 200

        if request.args['switch'] == 'on':
            r.srem(node, ctid)
            if r.type(node) == "none":
                try:
                    print("node: {}".format(node))
                    cmd = subprocess.check_output(
                        "ssh -t root@{} '/opt/sophos-av/bin/savdctl enable'".format(node), shell=True)
                    r.hdel(node, ctid)
                    print("enabling antivirus on node: {}, output: {}".format(node, cmd))
                except:
                    r.sadd(node, ctid)
                    return {'state': 'error'}, 200
                return {'state': 'enabling'}, 200
            else:
                locked_by = ", ".join(r.sunion(node))
                return {'state': "enabling deferred, another CTID has the lock by {}".format(locked_by)}, 200


    def get(self):
        if 'node' not in request.args:
            return {'error': 'Node argument required beacuse... it needs a node'}, 400
        node = request.args['node']
        active = None
        try:
            print("node: {}".format(node))
            cmd = subprocess.check_output("ssh -t root@{} '/opt/sophos-av/bin/savdstatus'".format(node), shell=True)
            active = cmd.strip()
        except subprocess.CalledProcessError as e:
            active = e.output.strip()
        except:
            print("error getting status")
            return {'error': 'problem getting status'}, 500

        locked_by = list(r.smembers(node))
        if 'scanning is running' in active:
            return {'active': True, 'status': active, "locked_by": locked_by}, 200
        elif 'scanning is not running' in active:
            return {'active': False, 'status': active, "locked_by": locked_by}, 200
        else:
            return {'error': "Problem determining AV status"}, 500
