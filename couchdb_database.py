from urllib.request import Request, urlopen
from urllib.error import HTTPError
import logging
import json
import time


class Database:
    PAGE_SIZE = 100

    def __init__(self):
        self.logger = logging.getLogger('logger')
        self.database_url = 'http://localhost:5984'
        self.requests_table = self.database_url + '/requests'
        self.requests_output_datasets_view = self.requests_table + '/_design/_designDoc/_view/outputDatasets'
        self.requests_campaigns_view = self.requests_table + '/_design/_designDoc/_view/campaigns'
        self.requests_prepid_view = self.requests_table + '/_design/_designDoc/_view/prepids'
        self.requests_type_view = self.requests_table + '/_design/_designDoc/_view/types'
        self.settings_table = self.database_url + '/settings'

    def update_request(self, request, update_timestamp=True):
        try:
            if update_timestamp:
                request['LastUpdate'] = int(time.time())

            url = self.requests_table + '/' + request['_id']
            self.make_request(url, request, 'PUT')
        except HTTPError as err:
            self.logger.error(str(err))

    def delete_request(self, request_name):
        request = self.get_request(request_name)
        if request is not None and request.get('_rev') is not None:
            rev = request['_rev']
            url = '%s/%s?rev=%s' % (self.requests_table, request_name, rev)
            self.make_request(url, method='DELETE')

    def get_request_count(self):
        return self.make_request(self.requests_table)['doc_count']

    def get_request(self, request_name):
        url = self.requests_table + '/' + request_name
        try:
            return self.make_request(url)
        except HTTPError as err:
            if err.code != 404:
                self.logger.error(str(err))

            return None

    def get_requests_with_prepid(self, prepid, page=0, page_size=PAGE_SIZE, include_docs=False):
        url = '%s?key="%s"&limit=%d&skip=%d&include_docs=%s' % (self.requests_prepid_view,
                                                                prepid,
                                                                page_size,
                                                                page * page_size,
                                                                'True' if include_docs else 'False')
        rows = self.make_request(url)['rows']
        if include_docs:
            return [x['doc'] for x in rows]
        else:
            return [x['id'] for x in rows]

    def get_requests_with_dataset(self, dataset, page=0, page_size=PAGE_SIZE, include_docs=False):
        url = '%s?key="%s"&limit=%d&skip=%d&include_docs=%s' % (self.requests_output_datasets_view,
                                                                dataset,
                                                                page_size,
                                                                page * page_size,
                                                                'True' if include_docs else 'False')
        rows = self.make_request(url)['rows']
        if include_docs:
            return [x['doc'] for x in rows]
        else:
            return [x['id'] for x in rows]

    def get_requests_with_campaign(self, campaign, page=0, page_size=PAGE_SIZE, include_docs=False):
        url = '%s?key="%s"&limit=%d&skip=%d&include_docs=%s' % (self.requests_campaigns_view,
                                                                campaign,
                                                                page_size,
                                                                page * page_size,
                                                                'True' if include_docs else 'False')
        rows = self.make_request(url)['rows']
        if include_docs:
            return [x['doc'] for x in rows]
        else:
            return [x['id'] for x in rows]

    def get_requests_with_type(self, request_type, page=0, page_size=PAGE_SIZE, include_docs=False):
        url = '%s?key="%s"&limit=%d&skip=%d&include_docs=%s' % (self.requests_type_view,
                                                                request_type,
                                                                page_size,
                                                                page * page_size,
                                                                'True' if include_docs else 'False')
        rows = self.make_request(url)['rows']
        if include_docs:
            return [x['doc'] for x in rows]
        else:
            return [x['id'] for x in rows]

    def get_requests(self, page=0, page_size=PAGE_SIZE, include_docs=False):
        url = '%s/_all_docs?limit=%d&skip=%d&include_docs=%s' % (self.requests_table,
                                                                 page_size,
                                                                 page * page_size,
                                                                 'True' if include_docs else 'False')
        rows = self.make_request(url)['rows']
        if include_docs:
            return [x['doc'] for x in rows]
        else:
            return [x['id'] for x in rows]

    def set_setting(self, setting_name, setting_value):
        url = self.settings_table + '/all_settings'
        try:
            settings_dict = self.make_request(url)
        except HTTPError:
            settings_dict = {'_id': 'all_settings'}

        settings_dict[setting_name] = setting_value
        self.make_request(url, settings_dict, 'PUT')

    def get_setting(self, setting_name, default_value):
        url = self.settings_table + '/all_settings'
        try:
            settings_dict = self.make_request(url)
        except HTTPError:
            return default_value

        return settings_dict.get(setting_name, default_value)

    def make_request(self, url, data=None, method='GET'):
        if data is not None:
            data = json.dumps(data)

        req = Request(url, data=data, method=method)
        if (method == 'POST' or method == 'PUT') and data is not None:
            data = data.encode("utf-8")

        req.add_header('Content-Type', 'application/json')
        req.add_header('Authorization', 'Basic c3RhdHM6c3RhdHM=')
        response = json.loads(urlopen(req, data=data).read().decode('utf-8'))
        return response
