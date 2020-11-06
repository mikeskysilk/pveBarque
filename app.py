#!/usr/bin/python3

import redis
import datetime
from flask import Flask
from flask_restful import Api
from flask_httpauth import HTTPBasicAuth
import resources
import common.variables as vars
import common


import auth
# import resources.restore
# import resources.status
# import resources.
from processes import foreman
from processes import workerv2
import common.config_reader as config_reader
import common.utilities as util

VERSION = '1.0.0'
START_TIME = datetime.datetime.utcnow().replace(microsecond=0)

def main():
    # util.sanitize()
    # config.set_config(config_reader.barque_config_read("./config.toml"))
    common.set_config(config_reader.barque_config_read("./config.toml"))
    common.init_logging(common.CONFIG)
    common.init_redis(
        host=common.CONFIG['redis']['host'],
        port=common.CONFIG['redis']['port'],
        password=common.CONFIG['redis']['password'],
        db=common.CONFIG['redis']['database']
    )
    common.LOG.debug(common.CONFIG)

    auth.set_admin_credentials(
        common.CONFIG['username'],
        common.CONFIG['password']
    )

    app = Flask(__name__)
    api = Api(app)
    resources.__init__(api, VERSION, START_TIME)

    # Launch the Foreman daemon
    f = foreman.Foreman(common.CONFIG)
    f.start()
    common.LOG.debug("Started foreman process")

    # Launch the worker daemons
    for i in range(common.CONFIG["workers"]):
        p = workerv2.Worker(name="worker-{}".format(i))
        p.start()
        common.WORKERS.append(p)

    app.testing = True
    app.run(
        host=common.CONFIG['host'],
        port=common.CONFIG['port'],
        use_reloader=False,
        ssl_context=(common.CONFIG['cert'],
                     common.CONFIG['key'])
    )
    common.LOG.debug("Started application server.")


if __name__ == "__main__":
    main()
