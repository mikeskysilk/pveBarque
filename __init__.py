from flask_httpauth import HTTPBasicAuth

authentication = HTTPBasicAuth()
admin_creds = {}


@authentication.verify_password
def verify(username, password):
    if not (username and password):
        return False
    return admin_creds.get(username) == password


def set_credentials(username, password):
    global admin_creds
    admin_creds[username] = password
