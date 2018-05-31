from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import logging
import time


class Database:
    def __init__(self):
        self.logger = logging.getLogger('logger')
        self.client = MongoClient('localhost', 27017)
        self.stats_db = self.client['stats']
        self.requests_table = self.stats_db['requests']
        self.settings_table = self.stats_db['settings']

    def insert_request_if_does_not_exist(self, request):
        try:
            request['LastUpdate'] = int(time.time())
            inserted_id = self.requests_table.insert_one(request).inserted_id
        except DuplicateKeyError:
            return None

        return inserted_id

    def delete_request(self, request):
        self.requests_table.delete_one({'_id': request})

    def update_request(self, request):
        request['LastUpdate'] = int(time.time())
        self.requests_table.replace_one({'_id': request['_id']}, request)

    def get_request_count(self):
        return self.requests_table.count()

    def get_request(self, request_name):
        return self.requests_table.find_one({'_id': request_name})

    def query_requests(self, query_dict=None, page=None, page_size=None):
        if query_dict is not None:
            requests = self.requests_table.find(query_dict)
        else:
            requests = self.requests_table.find()

        total = requests.count()
        if page is not None and page_size is not None:
            requests = requests.skip(page * page_size).limit(page_size)
            left = total - (page + 1) * page_size
            if left < 0:
                left = 0
        elif page_size is not None:
            requests = requests.limit(page_size)
            left = 0
        else:
            left = 0

        return list(requests), left, total

    def get_requests_with_dataset(self, dataset):
        requests, _, _ = self.query_requests({'OutputDatasets': dataset})
        return requests

    def set_setting(self, setting_name, setting_value):
        settings_dict = self.settings_table.find_one({'_id': 'all_settings'})
        if settings_dict is None:
            settings_dict = {'_id': 'all_settings'}

        settings_dict[setting_name] = setting_value
        self.settings_table.replace_one({'_id': 'all_settings'}, settings_dict, upsert=True)

    def get_setting(self, setting_name, default_value):
        all_settings = self.settings_table.find_one({'_id': 'all_settings'})
        if all_settings is not None:
            return all_settings.get(setting_name, default_value)
        else:
            return default_value

    def clear_database(self):
        self.requests_table.remove()
        self.settings_table.remove()
