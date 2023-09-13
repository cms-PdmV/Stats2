"""
Module that contains Database class which handles all operations with database
"""
import os
import logging
import json
import time
from urllib.request import Request, urlopen
from urllib.error import HTTPError


class Database:
    """
    Database class handles all actions with database:
    create, read, update, delete and search for documents
    """
    PAGE_SIZE = 100

    def __init__(self):
        self.logger = logging.getLogger('logger')
        self.database_url = os.getenv('DB_URL', 'http://localhost:5984')
        self.workflows_table = self.database_url + '/requests'
        self.workflows_input_dataset_view = self.workflows_table + '/_design/_designDoc/_view/inputDatasets'
        self.workflows_output_datasets_view = self.workflows_table + '/_design/_designDoc/_view/outputDatasets'
        self.workflows_campaigns_view = self.workflows_table + '/_design/_designDoc/_view/campaigns'
        self.workflows_prepid_view = self.workflows_table + '/_design/_designDoc/_view/prepids'
        self.workflows_type_view = self.workflows_table + '/_design/_designDoc/_view/types'
        self.workflows_processing_string_view = self.workflows_table + '/_design/_designDoc/_view/processingStrings'
        self.workflows_requests_view = self.workflows_table + '/_design/_designDoc/_view/requests'
        self.settings_table = self.database_url + '/settings'
        self.auth_header = os.environ.get('STATS_DB_AUTH_HEADER')

    def update_workflow(self, workflow, update_timestamp=True):
        """
        Update workflow in database
        """
        try:
            if update_timestamp:
                workflow['LastUpdate'] = int(time.time())

            url = self.workflows_table + '/' + workflow['_id']
            self.make_request(url, workflow, 'PUT')
        except HTTPError as err:
            self.logger.error('Error updating workflow: %s', err)

    def delete_workflow(self, workflow_name):
        """
        Delete a workflow with a given name
        """
        workflow = self.get_workflow(workflow_name)
        if workflow is not None and workflow.get('_rev') is not None:
            rev = workflow['_rev']
            url = '%s/%s?rev=%s' % (self.workflows_table, workflow_name, rev)
            self.make_request(url, method='DELETE')

    def get_workflow_count(self):
        """
        Return number of workflows in database
        """
        return self.make_request(self.workflows_table)['doc_count']

    def get_workflow(self, workflow_name):
        """
        Fetch a workflow with given name
        """
        url = self.workflows_table + '/' + workflow_name
        try:
            return self.make_request(url)
        except HTTPError as err:
            if err.code != 404:
                self.logger.error(str(err))

            return None

    def get_workflows_with_prepid(self, prepid, page=0, page_size=PAGE_SIZE, include_docs=False):
        """
        Fetch workflows that have certain prepid (prepid of workflow, not request/task)
        """
        include_docs_str = 'True' if include_docs else 'False'
        url = '%s?key="%s"&limit=%d&skip=%d&include_docs=%s' % (self.workflows_prepid_view,
                                                                prepid,
                                                                page_size,
                                                                page * page_size,
                                                                include_docs_str)
        rows = self.make_request(url)['rows']
        if include_docs:
            return [x['doc'] for x in rows]

        return [x['id'] for x in rows]

    def get_workflows_with_input_dataset(self, dataset, page=0, page_size=PAGE_SIZE, include_docs=False):
        """
        Fetch workflows that have certain input dataset
        """
        include_docs_str = 'True' if include_docs else 'False'
        url = '%s?key="%s"&limit=%d&skip=%d&include_docs=%s' % (self.workflows_input_dataset_view,
                                                                dataset,
                                                                page_size,
                                                                page * page_size,
                                                                include_docs_str)
        rows = self.make_request(url)['rows']
        if include_docs:
            return [x['doc'] for x in rows]

        return [x['id'] for x in rows]

    def get_workflows_with_output_dataset(self, dataset, page=0, page_size=PAGE_SIZE, include_docs=False):
        """
        Fetch workflows that have certain output dataset
        """
        include_docs_str = 'True' if include_docs else 'False'
        url = '%s?key="%s"&limit=%d&skip=%d&include_docs=%s' % (self.workflows_output_datasets_view,
                                                                dataset,
                                                                page_size,
                                                                page * page_size,
                                                                include_docs_str)
        rows = self.make_request(url)['rows']
        if include_docs:
            return [x['doc'] for x in rows]

        return [x['id'] for x in rows]

    def get_workflows_with_campaign(self, campaign, page=0, page_size=PAGE_SIZE, include_docs=False):
        """
        Fetch workflows that have certain campaign
        """
        include_docs_str = 'True' if include_docs else 'False'
        url = '%s?key="%s"&limit=%d&skip=%d&include_docs=%s' % (self.workflows_campaigns_view,
                                                                campaign,
                                                                page_size,
                                                                page * page_size,
                                                                include_docs_str)
        rows = self.make_request(url)['rows']
        if include_docs:
            return [x['doc'] for x in rows]

        return [x['id'] for x in rows]

    def get_workflows_with_type(self, workflow_type, page=0, page_size=PAGE_SIZE, include_docs=False):
        """
        Fetch workflows that have certain RequestType
        """
        include_docs_str = 'True' if include_docs else 'False'
        url = '%s?key="%s"&limit=%d&skip=%d&include_docs=%s' % (self.workflows_type_view,
                                                                workflow_type,
                                                                page_size,
                                                                page * page_size,
                                                                include_docs_str)
        rows = self.make_request(url)['rows']
        if include_docs:
            return [x['doc'] for x in rows]

        return [x['id'] for x in rows]

    def get_workflows_with_processing_string(self, workflow_processing_string, page=0, page_size=PAGE_SIZE, include_docs=False):
        """
        Fetch workflows that have certain processing string
        """
        include_docs_str = 'True' if include_docs else 'False'
        url = '%s?key="%s"&limit=%d&skip=%d&include_docs=%s' % (self.workflows_processing_string_view,
                                                                workflow_processing_string,
                                                                page_size,
                                                                page * page_size,
                                                                include_docs_str)
        rows = self.make_request(url)['rows']
        if include_docs:
            return [x['doc'] for x in rows]

        return [x['id'] for x in rows]

    def get_workflows_with_request(self, request_name, page=0, page_size=PAGE_SIZE, include_docs=False):
        """
        Fetch workflows that have certain request
        """
        include_docs_str = 'True' if include_docs else 'False'
        url = '%s?key="%s"&limit=%d&skip=%d&include_docs=%s' % (self.workflows_requests_view,
                                                                request_name,
                                                                page_size,
                                                                page * page_size,
                                                                include_docs_str)
        rows = self.make_request(url)['rows']
        if include_docs:
            return [x['doc'] for x in rows]

        return [x['id'] for x in rows]

    def get_workflows(self, page=0, page_size=PAGE_SIZE, include_docs=False):
        """
        Fetch workflows
        """
        include_docs_str = 'True' if include_docs else 'False'
        url = '%s/_all_docs?limit=%d&skip=%d&include_docs=%s' % (self.workflows_table,
                                                                 page_size,
                                                                 page * page_size,
                                                                 include_docs_str)
        rows = self.make_request(url)['rows']
        if include_docs:
            return [x['doc'] for x in rows]

        return [x['id'] for x in rows]

    def set_setting(self, setting_name, setting_value):
        """
        Save a setting value to database
        """
        url = self.settings_table + '/all_settings'
        try:
            settings_dict = self.make_request(url)
        except HTTPError:
            settings_dict = {'_id': 'all_settings'}

        settings_dict[setting_name] = setting_value
        self.make_request(url, settings_dict, 'PUT')

    def get_setting(self, setting_name, default_value):
        """
        Fetch a setting value from database
        """
        url = self.settings_table + '/all_settings'
        try:
            settings_dict = self.make_request(url)
        except HTTPError:
            return default_value

        return settings_dict.get(setting_name, default_value)

    def make_request(self, url, data=None, method='GET'):
        """
        Make a HTTP request to the actual database api
        """
        if data is not None:
            data = json.dumps(data)

        req = Request(url, data=data, method=method)
        if method in ('POST', 'PUT') and data is not None:
            data = data.encode("utf-8")

        req.add_header('Content-Type', 'application/json')
        if self.auth_header:
            req.add_header('Authorization', self.auth_header)

        response = json.loads(urlopen(req, data=data).read().decode('utf-8'))
        return response
