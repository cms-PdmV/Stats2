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

    def delete_request(self, request):
        table = self.stats_db['request']
        table.delete_one({'_id': request})

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

    def query(self, query_dict=None, page=0, page_size=200):
        table = self.stats_db['request']
        if query_dict is not None:
            requests = table.find(query_dict)
        else:
            requests = table.find()

        requests = requests.skip(page * page_size).limit(page_size)
        return list(requests)

    def get_requests_with_dataset(self, dataset):
        table = self.stats_db['request']
        requests = table.find({'OutputDatasets': dataset})

        return list(requests)

    def put_last_seq(self, last_seq):
        table = self.stats_db['timestamps']
        table.replace_one({'_id': 'last_seq'}, {'last_seq': last_seq}, upsert=True)

    def get_last_seq(self):
        table = self.stats_db['timestamps']
        last_seq = list(table.find({'_id': 'last_seq'}))

        if len(last_seq) < 1:
            return 0
        else:
            return last_seq[0]['last_seq']

    def put_last_date(self, timestamp):
        table = self.stats_db['timestamps']
        table.replace_one({'_id': 'dataset_timestamp'}, {'timestamp': timestamp}, upsert=True)

    def get_last_date(self):
        table = self.stats_db['timestamps']
        last_seq = list(table.find({'_id': 'dataset_timestamp'}))

        if len(last_seq) < 1:
            return 0
        else:
            return last_seq[0]['timestamp']

    def get_count_of_requests(self):
        return self.stats_db['request'].count()

    def get_count_of_requests_without_history(self):
        return self.stats_db['request'].find({
            "$or": [{"EventNumberHistory": {"$exists": 0}}, {"EventNumberHistory": {"$eq": []}}]
        }).count()

    def clear_database(self):
        self.stats_db.request.drop()
        self.stats_db.timestamps.drop()
