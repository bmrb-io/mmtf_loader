#!/usr/bin/python

import random
import requests
from requests.auth import HTTPDigestAuth

class AXRSession():
    """ Returns an AXR session. """

    hosts = ['192.92.158.130', '192.92.158.110', '192.92.158.194']
    ports = [8080, 8081, 8082]

    username = 'nmr_uw'
    password = 'nmr#0318'
    meta = '?meta=json'

    def __init__(self):
        self.host = "http://%s:%s/namespace/nmr_uw/3mer/" % (random.choice(self.hosts), random.choice(self.ports)) + '%s'

    def __enter__(self):
        """ Start the session. """

        self.session = requests.Session()
        self.session.auth = HTTPDigestAuth(self.username, self.password)

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ End the current session. """

        # End the HTTP session
        self.session.close()

    def store(self, key, object_):
        r = self.session.put(self.host % key, data=object_)
        return r.ok

    def load(self, key):
        r = self.session.get(self.host % key)
        return r.ok

    def delete(self, key):
        r = self.session.delete(self.host % key)
        return r.ok

    def status(self, key):
        r = self.session.get(self.host % key + self.meta)
        return r.json()

    def mkdir(self, dir_name):
        r = self.session.get(self.host % dir_name, headers={'X-Ampli-Kind': 'Directory'})
        r.raise_for_status()
        return r.ok
