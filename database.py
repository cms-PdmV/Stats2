from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import logging


class Database:
    def __init__(self):
        self.logger = logging.getLogger('logger')
        self.client = MongoClient('localhost', 27017)
        self.stats_db = self.client['stats']

    def db_status(self):
        db = self.client.admin
        server_status_result = db.command('serverStatus')
        return server_status_result

    def insert_request_if_does_not_exist(self, request):
        requests_table = self.stats_db['request']
        try:
            inserted_id = requests_table.insert_one(request).inserted_id
        except DuplicateKeyError:
            return None

        return inserted_id

    def update_request(self, request):
        requests_table = self.stats_db['request']
        requests_table.replace_one({'_id': request['_id']}, request)

    def get_request_count(self):
        table = self.stats_db['request']
        return table.count()

    def get_all_requests(self):
        table = self.stats_db['request']
        return list(table.find())

    def get_request(self, request_name):
        table = self.stats_db['request']
        requests = list(table.find({'_id': request_name}))
        if len(requests) > 0:
            return requests[0]
        else:
            return None

    def clear_database(self):
        self.stats_db.request.drop()
