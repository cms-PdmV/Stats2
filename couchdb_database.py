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
        self.workflows_table = self.database_url + '/requests'
        self.workflows_output_datasets_view = self.workflows_table + '/_design/_designDoc/_view/outputDatasets'
        self.workflows_campaigns_view = self.workflows_table + '/_design/_designDoc/_view/campaigns'
        self.workflows_prepid_view = self.workflows_table + '/_design/_designDoc/_view/prepids'
        self.workflows_type_view = self.workflows_table + '/_design/_designDoc/_view/types'
        self.settings_table = self.database_url + '/settings'
        self.auth_header = str(open('/home/jrumsevi/stats2_auth.txt', "r").read()).replace('\n', '')

    def update_workflow(self, workflow, update_timestamp=True):
        try:
            if update_timestamp:
                workflow['LastUpdate'] = int(time.time())

            url = self.workflows_table + '/' + workflow['_id']
            self.make_request(url, workflow, 'PUT')
        except HTTPError as err:
            self.logger.error(str(err))

    def delete_workflow(self, workflow_name):
        workflow = self.get_workflow(workflow_name)
        if workflow is not None and workflow.get('_rev') is not None:
            rev = workflow['_rev']
            url = '%s/%s?rev=%s' % (self.workflows_table, workflow_name, rev)
            self.make_request(url, method='DELETE')

    def get_workflow_count(self):
        return self.make_request(self.workflows_table)['doc_count']

    def get_workflow(self, workflow_name):
        url = self.workflows_table + '/' + workflow_name
        try:
            return self.make_request(url)
        except HTTPError as err:
            if err.code != 404:
                self.logger.error(str(err))

            return None

    def get_workflows_with_prepid(self, prepid, page=0, page_size=PAGE_SIZE, include_docs=False):
        url = '%s?key="%s"&limit=%d&skip=%d&include_docs=%s' % (self.workflows_prepid_view,
                                                                prepid,
                                                                page_size,
                                                                page * page_size,
                                                                'True' if include_docs else 'False')
        rows = self.make_request(url)['rows']
        if include_docs:
            return [x['doc'] for x in rows]
        else:
            return [x['id'] for x in rows]

    def get_workflows_with_dataset(self, dataset, page=0, page_size=PAGE_SIZE, include_docs=False):
        url = '%s?key="%s"&limit=%d&skip=%d&include_docs=%s' % (self.workflows_output_datasets_view,
                                                                dataset,
                                                                page_size,
                                                                page * page_size,
                                                                'True' if include_docs else 'False')
        rows = self.make_request(url)['rows']
        if include_docs:
            return [x['doc'] for x in rows]
        else:
            return [x['id'] for x in rows]

    def get_workflows_with_campaign(self, campaign, page=0, page_size=PAGE_SIZE, include_docs=False):
        url = '%s?key="%s"&limit=%d&skip=%d&include_docs=%s' % (self.workflows_campaigns_view,
                                                                campaign,
                                                                page_size,
                                                                page * page_size,
                                                                'True' if include_docs else 'False')
        rows = self.make_request(url)['rows']
        if include_docs:
            return [x['doc'] for x in rows]
        else:
            return [x['id'] for x in rows]

    def get_workflows_with_type(self, workflow_type, page=0, page_size=PAGE_SIZE, include_docs=False):
        url = '%s?key="%s"&limit=%d&skip=%d&include_docs=%s' % (self.workflows_type_view,
                                                                workflow_type,
                                                                page_size,
                                                                page * page_size,
                                                                'True' if include_docs else 'False')
        rows = self.make_request(url)['rows']
        if include_docs:
            return [x['doc'] for x in rows]
        else:
            return [x['id'] for x in rows]

    def get_workflows(self, page=0, page_size=PAGE_SIZE, include_docs=False):
        url = '%s/_all_docs?limit=%d&skip=%d&include_docs=%s' % (self.workflows_table,
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
        req.add_header('Authorization', self.auth_header)
        response = json.loads(urlopen(req, data=data).read().decode('utf-8'))
        return response
