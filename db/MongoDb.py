import os
from datetime import datetime

from pymongo import MongoClient
from pymongo import DESCENDING
from pymongo import ASCENDING


class MongoDb(object):

    def __init__(self):
        self._client = MongoClient(f'mongodb://{os.getenv("MONGODB_USER")}:{os.getenv("MONGODB_PASS")}@{os.getenv("MONGODB_IP")}'
                         f':{os.getenv("MONGODB_PORT")}/')
        self._db = self._client.licitations

    def find_all_licitations(self):
        return self._db.licitation.find().sort("$natural", DESCENDING)

    def find_all_new_licitations(self):
        last_execution = self.find_last_execution()
        if last_execution is not None:
            cursor = self._db.licitation.find({"fecha": {"$lt": last_execution}})
        else:
            cursor = self.find_all_licitations()
        return cursor

    def insert_url(self, execution_time, url):
        self._db.urlAccessLog.insert_one({
            'url': url,
            'execution_time': execution_time,
            'insert_time': datetime.now()
        })

    def find_last_url(self):
        cursor = self._db.urlAccessLog.find().sort([
            ("execution_time", DESCENDING),
            ("insert_time", ASCENDING)]
        ).limit(1)

        try:
            url = cursor.next()['url']
        except StopIteration as si:
            url = None
        return url

    def find_last_execution(self):
        cursor = self._db.urlAccessLog.find().sort([
            ("execution_time", DESCENDING),
            ("insert_time", ASCENDING)]
        ).limit(1)

        if cursor.alive:
            execution_time = cursor.next()['execution_time']
        else:
            execution_time = None
        return execution_time

    def close(self):
        self._client.close()
