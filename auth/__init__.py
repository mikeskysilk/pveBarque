# pvebarque/auth/__init__.py
""" Module containing authentication functions """

from flask_httpauth import HTTPBasicAuth


AUTH = HTTPBasicAuth()
ADMIN_CREDENTIALS = {}

def set_admin_credentials(username, password):
    """ setter function to set credentials for HTTPBasicAuth """
    ADMIN_CREDENTIALS[username] = password

@AUTH.verify_password
def verify(username, password):
    """ HTTPBasicAuth helper function to verify username and password """
    if not (username and password):
        return False
    return ADMIN_CREDENTIALS.get(username) == password
