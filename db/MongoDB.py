from pymongo import MongoClient


class MongoDB:
    def __init__(self, user, password, host, port):
        client = MongoClient(f'mongodb://{user}:{password}@{host}:{port}/')
        self._db = client.licitations

    def insert_licitations(self, licitation):
        self._db.licitation.insert_one()