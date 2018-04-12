import json
import logging
from database import Database
from utils import make_request_with_grid_cert, pick_attributes, get_request_list_from_req_mgr
import time


class StatsUpdate():
    """
    Update events in the database
    """
    def __init__(self):
        self.logger = logging.getLogger('logger')
        self.database = Database()

    def perform_update(self, name=None, days=None):
        if name is not None:
            self.perform_update_one(name)
        elif days is not None:
            self.perform_update_days(days)
        else:
            self.perform_update_all()

    def perform_update_one(self, request_name):
        self.logger.info('Will update only one request: %s' % (request_name))
        self.update_one(request_name)

    def perform_update_days(self, days):
        start_update = time.time()
        requests = get_request_list_from_req_mgr(days)
        request_count = len(requests)
        self.logger.info('Will process %d requests' % (request_count))
        current = 1
        for request_name in requests:
            self.logger.info('Will process request %d/%d' % (current, request_count))
            self.update_one(request_name)
            current += 1

        end_update = time.time()
        self.logger.info('Updated %d requests in %.3fs\n' % (request_count,
                                                             (end_update - start_update)))

    def perform_update_all(self):
        requests = get_request_list_from_req_mgr()
        self.logger.info('Will process %d requests' % (len(requests)))
        for request_name in requests:
            self.update_one(request_name)

    def update_one(self, request_name):
        self.logger.info('Updating %s' % (request_name))
        start_update = time.time()
        self.database.insert_request_if_does_not_exist({'_id': request_name})
        req_dict_old = self.database.get_request(request_name)
        req_dict_new = self.get_new_dict_from_reqmgr2(req_dict_old)
        self.add_history_to_new_request(req_dict_new, req_dict_old.get('EventNumberHistory', None))

        self.database.update_request(req_dict_new)
        end_update = time.time()
        self.logger.info('Updated %s in %.3fs\n' % (request_name,
                                                    (end_update - start_update)))

    def get_new_dict_from_reqmgr2(self, req_dict_old):
        req_name = req_dict_old['_id']
        url = 'https://cmsweb.cern.ch/reqmgr2/data/request?name=%s' % (req_name)

        response = make_request_with_grid_cert(url)
        req_dict_new = json.loads(response)
        req_dict_new = req_dict_new['result'][0][req_name]
        expected_events = req_dict_old.get('TotalEvents', 0)
        if expected_events <= 0:
            expected_events = self.get_expected_events_with_dict(req_dict_new)

        req_dict_new = pick_attributes(req_dict_new, ['AcquisitionEra',
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
        req_dict_new['RequestTransition'] = [{'Status': tr['Status'],
                                              'UpdateTime': tr['UpdateTime']} for tr in req_dict_new['RequestTransition']]
        req_dict_new['_id'] = req_name
        req_dict_new['TotalEvents'] = expected_events
        req_dict_new['OutputDatasets'] = self.sort_datasets(req_dict_new['OutputDatasets'])

        self.add_number_of_events(req_dict_new)
        return req_dict_new

    def get_event_count_from_dbs(self, dataset_name):
        dbs3_url = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/'
        filesummaries = json.loads(make_request_with_grid_cert(dbs3_url + 'filesummaries?dataset=%s' % (dataset_name)))
        if len(filesummaries) == 0:
            # self.logger.warning('No summaries for %s' % (dataset_name))
            return 0

        return int(filesummaries[0]['num_event'])

    def add_number_of_events(self, req_dict):
        announced = False
        for transition in req_dict['RequestTransition']:
            if transition['Status'] == 'announced':
                announced = True
                break

        number_of_events = {'Time': int(time.time()), 'Datasets': {}}
        for dataset_name in req_dict['OutputDatasets']:
            output_dataset = req_dict['OutputDatasets'][-1:][0]
            events = self.get_event_count_from_dbs(output_dataset)
            if announced:
                number_of_events['Datasets'][dataset_name] = {'OpenEvents': 0,
                                                              'DoneEvents': events}
            else:
                number_of_events['Datasets'][dataset_name] = {'OpenEvents': events,
                                                              'DoneEvents': 0}

        req_dict['EventNumberHistory'] = [number_of_events]

    def add_history_to_new_request(self, new_request, old_history):
        if old_history is not None and len(old_history) > 0:
            last_old_history_entry = old_history[-1:][0]
            last_new_history_entry = new_request['EventNumberHistory'][0]

            needs_append = False
            for dataset_name in last_new_history_entry['Datasets']:
                if dataset_name not in last_old_history_entry['Datasets']:
                    needs_append = True
                    break

                old_open = last_old_history_entry['Datasets'][dataset_name]['OpenEvents']
                old_done = last_old_history_entry['Datasets'][dataset_name]['DoneEvents']
                new_open = last_new_history_entry['Datasets'][dataset_name]['OpenEvents']
                new_done = last_new_history_entry['Datasets'][dataset_name]['DoneEvents']
                if old_open != new_open or old_done != new_done:
                    needs_append = True
                    break

            if needs_append:
                new_request['EventNumberHistory'] = old_history.append(last_new_history_entry)

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
            # self.logger.info('Resubmission %s, will get TotalInputEvents' % (req_dict['_id']))
            prep_id = req_dict['PrepID']
            url = 'https://cmsweb.cern.ch/reqmgr2/data/request?mask=TotalInputEvents&mask=RequestType&prep_id=%s' % (prep_id)
            ret = make_request_with_grid_cert(url)
            ret = json.loads(ret)
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
