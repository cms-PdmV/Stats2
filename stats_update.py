import logging
from database import Database
from utils import make_request_with_grid_cert, pick_attributes
import time


class StatsUpdate():
    """
    Update events in the database
    """
    def __init__(self):
        self.logger = logging.getLogger('logger')
        self.database = Database()

    def perform_update(self, name=None):
        if name is not None:
            self.perform_update_one(name)
        else:
            self.perform_update_new()

    def perform_update_one(self, request_name):
        self.logger.info('Will update only one request: %s' % (request_name))
        self.update_one(request_name)
        self.recalculate_requests([request_name])

    def perform_update_new(self):
        update_start = time.time()
        changed_requests, last_seq = self.get_list_of_changed_requests()
        self.logger.info('Will update %d requests' % (len(changed_requests)))
        for request_name in changed_requests:
            self.update_one(request_name)

        update_end = time.time()
        requests_to_recalculate = set(changed_requests).union(set(self.get_list_of_requests_with_changed_datasets()))
        self.recalculate_requests(requests_to_recalculate)
        recalculation_end = time.time()
        self.logger.info('Updated %d requests in %.3fs' % (len(changed_requests),
                                                           (update_end - update_start)))
        self.logger.info('Recalculated %d requests in %.3fs' % (len(requests_to_recalculate),
                                                                (recalculation_end - update_end)))
        self.database.put_last_seq(last_seq)
        self.database.put_last_date(update_start)

    def update_one(self, request_name):
        self.logger.info('Updating %s' % (request_name))
        update_start = time.time()
        req_dict_old = self.database.get_request(request_name)
        if req_dict_old is None:
            req_dict = {'_id': request_name}
            self.logger.info('Inserting %s' % (request_name))
            self.database.insert_request_if_does_not_exist(req_dict)
            req_dict_old = req_dict

        req_dict = self.get_new_dict_from_reqmgr2(request_name)
        req_dict['EventNumberHistory'] = req_dict_old.get('EventNumberHistory', [])
        self.database.update_request(req_dict)
        update_end = time.time()
        self.logger.info('Updated %s in %.3fs' % (request_name, (update_end - update_start)))

    def recalculate_requests(self, request_names):
        for request_name in request_names:
            recalculation_start = time.time()
            self.logger.info('Will recalculate %s' % (request_name))
            request = self.database.get_request(request_name)
            history_entry = self.get_new_history_entry(request)
            added_history_entry = self.add_history_entry_to_request(request, history_entry)
            recalculation_end = time.time()
            if added_history_entry:
                self.database.update_request(request)
                self.logger.info('Recalculated %s in %fs' % (request_name, (recalculation_end - recalculation_start)))
            else:
                self.logger.error('Did not add new history entry for %s' % (request_name))

    def get_new_dict_from_reqmgr2(self, request_name):
        query_url = '/couchdb/reqmgr_workload_cache/%s' % (request_name)
        req_dict = make_request_with_grid_cert(query_url)
        expected_events = self.get_expected_events_with_dict(req_dict)
        req_dict = pick_attributes(req_dict, ['AcquisitionEra',
                                              'Campaign',
                                              'InputDataset',
                                              'Memory',
                                              'OutputDatasets',
                                              'PrepID',
                                              'RequestName',
                                              'RequestPriority',
                                              'RequestTransition',
                                              'RequestType',
                                              'SizePerEvent',
                                              'TimePerEvent'])
        req_dict['RequestTransition'] = [{'Status': tr['Status'],
                                          'UpdateTime': tr['UpdateTime']} for tr in req_dict['RequestTransition']]
        req_dict['_id'] = request_name
        req_dict['TotalEvents'] = expected_events
        req_dict['OutputDatasets'] = self.sort_datasets(req_dict['OutputDatasets'])
        req_dict['EventNumberHistory'] = []
        return req_dict

    def get_event_count_from_dbs(self, dataset_name):
        query_url = '/dbs/prod/global/DBSReader/filesummaries?dataset=%s' % (dataset_name)
        filesummaries = make_request_with_grid_cert(query_url)
        if len(filesummaries) == 0:
            return 0

        return int(filesummaries[0]['num_event'])

    def get_new_history_entry(self, req_dict):
        announced = False
        for transition in req_dict['RequestTransition']:
            if transition['Status'] == 'announced':
                announced = True
                break

        history_entry = {'Time': int(time.time()), 'Datasets': {}}
        for dataset_name in req_dict['OutputDatasets']:
            output_dataset = req_dict['OutputDatasets'][-1:][0]
            events = self.get_event_count_from_dbs(output_dataset)
            if announced:
                history_entry['Datasets'][dataset_name] = {'OpenEvents': 0,
                                                           'DoneEvents': events}
            else:
                history_entry['Datasets'][dataset_name] = {'OpenEvents': events,
                                                           'DoneEvents': 0}

        return history_entry

    def add_history_entry_to_request(self, req_dict, history_entry):
        if req_dict['EventNumberHistory'] is not None and len(req_dict['EventNumberHistory']) > 0:
            last_history_entry = req_dict['EventNumberHistory'][-1:][0]

            needs_append = False
            for dataset_name in history_entry['Datasets']:
                if dataset_name not in last_history_entry['Datasets']:
                    needs_append = True
                    break

                old_open = last_history_entry['Datasets'][dataset_name]['OpenEvents']
                old_done = last_history_entry['Datasets'][dataset_name]['DoneEvents']
                new_open = history_entry['Datasets'][dataset_name]['OpenEvents']
                new_done = history_entry['Datasets'][dataset_name]['DoneEvents']
                if old_open != new_open or old_done != new_done:
                    needs_append = True
                    break

            if not needs_append:
                return False
        else:
            req_dict['EventNumberHistory'] = []

        req_dict['EventNumberHistory'].append(history_entry)
        return True

    def get_expected_events_with_dict(self, req_dict):
        """
        method to takes requests number_of_events/input_ds/block_white_list/run_white_list from rqmgr2 dict
        """
        if 'FilterEfficiency' in req_dict:
            f = float(req_dict['FilterEfficiency'])
        elif 'Task1' in req_dict and 'FilterEfficiency' in req_dict['Task1']:
            f = float(req_dict['Task1']['FilterEfficiency'])
        else:
            f = 1.

        req_type = req_dict.get('RequestType', '').lower()
        if req_type != 'resubmission':
            if req_dict.get('TotalInputFiles', 0) > 0:
                if 'TotalInputEvents' in req_dict:
                    return int(f * req_dict['TotalInputEvents'])

            if 'RequestNumEvents' in req_dict:
                return int(req_dict['RequestNumEvents'])
            elif 'Task1' in req_dict and 'RequestNumEvents' in req_dict['Task1']:
                return int(req_dict['Task1']['RequestNumEvents'])
        else:
            prep_id = req_dict['PrepID']
            query_url = '/reqmgr2/data/request?mask=TotalInputEvents&mask=RequestType&prep_id=%s' % (prep_id)
            ret = make_request_with_grid_cert(query_url)
            ret = ret['result'][0]
            for r in ret:
                if ret[r]['RequestType'].lower() != 'resubmission' and ret[r]['TotalInputEvents'] is not None:
                    return int(f * ret[r]['TotalInputEvents'])

        self.logger.error('%s does not have total events!' % (req_dict['_id']))
        return -1

    def sort_datasets(self, dataset_list):
        """
        takes output_datasets list and sorts it in prioritized way.
        """
        if len(dataset_list) <= 1:
            return dataset_list

        def tierLevel(dataset):
            tier = dataset.split('/')[-1:][0]
            # DQMIO priority is the lowest because it does not produce any events
            # and is used only for some statistical things
            tier_priority = ['DQMIO',
                             'DQM',
                             'ALCARECO',
                             'USER',
                             'RAW-RECO',
                             'GEN-SIM',
                             'SIM-RAW-RECO',
                             'AOD',
                             'DIGI-RECO',
                             'SIM-RECO',
                             'RECO']
            for (p, t) in enumerate(tier_priority):
                if t in tier:
                    return p

            return -1

        dataset_list = sorted(dataset_list, key=tierLevel)
        return dataset_list

    def get_list_of_changed_requests(self):
        last_seq = self.database.get_last_seq()
        query_url = '/couchdb/reqmgr_workload_cache/_changes?since=%d' % (last_seq)
        self.logger.info('Getting the list of all requests since %d from %s' % (last_seq, query_url))
        response = make_request_with_grid_cert(query_url)
        last_seq = int(response['last_seq'])
        req_list = response['results']
        req_list = [req['id'] for req in req_list]
        req_list = list(filter(lambda x: '_design' not in x, req_list))
        self.logger.info('Got %d requests' % (len(req_list)))
        return req_list, last_seq

    def get_updated_dataset_list_from_dbs(self, since_timestamp=0):
        query_url = '/dbs/prod/global/DBSReader/datasets?min_ldate=%d' % (since_timestamp)
        self.logger.info('Getting the list of modified datasets since %d from %s' % (since_timestamp, query_url))
        dataset_list = make_request_with_grid_cert(query_url)
        dataset_list = [dataset['dataset'] for dataset in dataset_list]
        self.logger.info('Got %d datasets' % (len(dataset_list)))
        return dataset_list

    def get_list_of_requests_with_changed_datasets(self):
        self.logger.info('Will get list of changed datasets')
        requests = []
        last_dataset_modification_date = self.database.get_last_date()
        updated_datasets = self.get_updated_dataset_list_from_dbs(since_timestamp=last_dataset_modification_date)
        self.logger.info('Will find if any of changed datasets belong to requests in database')
        for dataset in updated_datasets:
            dataset_requests = self.database.get_requests_with_dataset(dataset)
            for dataset_request in dataset_requests:
                requests.append(dataset_request['_id'])

        self.logger.info('Found %d requests for changed datasets' % (len(requests)))
        return requests
