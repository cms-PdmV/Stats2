from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import logging


class Database:
    def __init__(self):
        self.logger = logging.getLogger('logger')
        self.client = MongoClient('localhost', 27017)
        self.stats_db = self.client['stats']
        self.requests_table = self.stats_db['request']
        self.settings_table = self.stats_db['settings']

    def insert_request_if_does_not_exist(self, request):
        requests_table = self.stats_db['request']
        try:
            inserted_id = requests_table.insert_one(request).inserted_id
        except DuplicateKeyError:
            return None

        return inserted_id

    def delete_request(self, request):
        self.requests_table.delete_one({'_id': request})

    def update_request(self, request):
        self.requests_table.replace_one({'_id': request['_id']}, request)

    def get_request_count(self):
        return self.requests_table.count()

    def get_request(self, request_name):
        return self.requests_table.find_one({'_id': 'request_name'})

    def query_requests(self, query_dict=None, page=0, page_size=200):
        table = self.stats_db['request']
        if query_dict is not None:
            requests = table.find(query_dict)
        else:
            requests = table.find()

        requests = requests.skip(page * page_size).limit(page_size)
        return list(requests)

    def get_requests_with_dataset(self, dataset):
        return self.query_requests({'OutputDatasets': dataset})

    def set_setting(self, setting_name, setting_value):
        settings_dict = self.settings_table.find_one({'_id': 'all_settings'})
        settings_dict[setting_name] = setting_value
        self.settings_table.replace_one({'_id': 'all_settings'}, settings_dict, upsert=True)

    def get_setting(self, setting_name, default_value):
        all_settings = self.settings_table.find_one({'_id': 'all_settings'})
        if all_settings is not None:
            return all_settings.get(setting_name, default_value)
        else:
            return default_value

    def clear_database(self):
        self.stats_db.request.drop()
        self.stats_db.timestamps.drop()
