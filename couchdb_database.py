import urllib
import logging
import json


class Database:
    def __init__(self):
        self.logger = logging.getLogger('logger')
        self.database_url = 'http://localhost:5984'
        self.requests_table = self.database_url + '/request'
        self.settings_table = self.database_url + '/settings'

    def insert_request_if_does_not_exist(self, request):
        try:
            self.make_request(self.requests_table, request, 'PUT')
        except urllib.error.HTTPError as err:
            self.logger.error(str(err))

    def delete_request(self, request):
        url = self.requests_table + '/' + request['_id']
        self.make_request(url, request, 'DELETE')

    def update_request(self, request):
        url = self.requests_table + '/' + request['_id']
        self.make_request(url, request, 'PUT')

    def get_request_count(self):
        return self.make_request(self.requests_table)['doc_count']

    def get_request(self, request_name):
        url = self.requests_table + '/' + request_name
        try:
            return self.make_request(url)
        except urllib.error.HTTPError as err:
            self.logger.error(str(err))
            return None

    def query_requests(self, query_dict=None, page=0, page_size=200):
        data = {'limit': page_size, 'skip': page * page_size}
        if query_dict is not None:
            data['selector'] = query_dict
        else:
            data['selector'] = {}

        url = self.requests_table + '/_find'
        return self.make_request(url, data, 'POST')['docs']

    def get_requests_with_dataset(self, dataset):
        self.logger.error('get_requests_with_dataset TO BE IMPLEMENTED %s' % (json.dumps(dataset)))
        return []

    def set_setting(self, setting_name, setting_value):
        self.logger.error('set_setting TO BE IMPLEMENTED %s=%s' % (setting_name, setting_value))

    def get_setting(self, setting_name, default_value):
        self.logger.error('get_setting TO BE IMPLEMENTED %s=%s' % (setting_name, default_value))
        return default_value

    def clear_database(self):
        self.logger.error('clear_database TO BE IMPLEMENTED')

    def make_request(self, url, data=None, method='GET'):
        if data is not None:
            data = json.dumps(data)

        req = urllib.request.Request(url, data=data, method=method)
        if method == 'POST':
            data = data.encode("utf-8")

        req.add_header("Content-Type", "application/json")
        response = json.loads(urllib.request.urlopen(req, data=data).read().decode('utf-8'))
        return response
