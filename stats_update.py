import logging
import time
import argparse
import json
from utils import setup_console_logging
from database import Database
from utils import make_request_with_grid_cert, pick_attributes


class StatsUpdate():
    """
    Update request info in Stats2 database.
    """

    __SKIPPABLE_STATUS = set(["rejected",
                              "aborted",
                              "failed",
                              "rejected-archived",
                              "aborted-archived",
                              "failed-archived",
                              "aborted-completed"])

    def __init__(self):
        self.logger = logging.getLogger('logger')
        self.database = Database()

    def perform_update(self, request_name=None):
        """
        Perform update for specific request if request name is given or for all changed
        requests if no name is specified.
        """
        if request_name is not None:
            self.perform_update_one(request_name)
        else:
            self.perform_update_new()

        self.logger.info('Requests after update %d' % (self.database.get_request_count()))

    def perform_update_one(self, request_name):
        """
        Perform update for specific request: fetch new dictionary from RequestManager
        and update open/done event cound.info
        """
        self.logger.info('Will update only one request: %s' % (request_name))
        self.update_one(request_name)
        self.recalculate_one(request_name)

    def perform_update_new(self):
        """
        Perform update for all requests that changed since last update and recalculate
        open/done events for files that changed since last update.info
        """
        update_start = time.time()
        changed_requests, deleted_requests, last_seq = self.get_list_of_changed_requests()
        initial_update = self.database.get_request_count() == 0
        self.logger.info('Will delete %d requests' % (len(deleted_requests)))
        for request_name in deleted_requests:
            try:
                self.delete_one(request_name)
            except Exception as e:
                self.logger.error('Exception while deleting %s:%s' % (request_name, str(e)))

        self.logger.info('Will update %d requests' % (len(changed_requests)))
        for request_name in changed_requests:
            try:
                self.update_one(request_name)
            except Exception as e:
                self.logger.error('Exception while updating %s:%s' % (request_name, str(e)))

        update_end = time.time()
        if initial_update:
            self.logger.info('Will update event count for all requests because all of them are new')
            requests_to_recalculate = set(changed_requests)
        else:
            requests_to_recalculate = set(changed_requests).union(set(self.get_list_of_requests_with_changed_datasets()))

        for request_name in requests_to_recalculate:
            try:
                self.recalculate_one(request_name)
            except Exception as e:
                self.logger.error('Exception while recalculating %s:%s' % (request_name, str(e)))

        recalculation_end = time.time()
        self.database.set_setting('last_reqmgr_sequence', int(last_seq))
        self.database.set_setting('last_dbs_update_date', int(update_start))
        self.logger.info('Updated and deleted %d/%d requests in %.3fs' % (len(changed_requests), len(deleted_requests),
                                                                          (update_end - update_start)))
        self.logger.info('Updated open/done events for %d requests in %.3fs' % (len(requests_to_recalculate),
                                                                                (recalculation_end - update_end)))

    def update_one(self, request_name):
        """
        Action to update one request's dictionary from RequestManager. If no such
        request exist in database, new one will be created.
        """
        self.logger.info('Updating %s' % (request_name))
        update_start = time.time()
        req_dict = self.get_new_dict_from_reqmgr2(request_name)
        req_transitions = req_dict.get('RequestTransition', [])
        for req_transition in req_transitions:
            if req_transition['Status'] in self.__SKIPPABLE_STATUS:
                self.logger.info('Skipping and deleting %s because it\'s status is %s' % (request_name, req_transition['Status']))
                self.database.delete_request(request_name)
                return

        req_dict_old = self.database.get_request(request_name)
        if req_dict_old is None:
            req_dict_old = {'_id': request_name}
            self.logger.info('Inserting %s' % (request_name))
            self.database.insert_request_if_does_not_exist(req_dict_old)

        req_dict['EventNumberHistory'] = req_dict_old.get('EventNumberHistory', [])
        self.database.update_request(req_dict)
        update_end = time.time()
        self.logger.info('Updated %s in %.3fs' % (request_name, (update_end - update_start)))

    def delete_one(self, request_name):
        """
        Action to delete one request from database.
        """
        self.logger.info('Deleting %s' % (request_name))
        delete_start = time.time()
        self.database.delete_request(request_name)
        delete_end = time.time()
        self.logger.info('Deleted %s in %.3fs' % (request_name, (delete_end - delete_start)))

    def recalculate_one(self, request_name):
        """
        Action to update open/done events for request.
        """
        recalc_start = time.time()
        self.logger.info('Will update open/done events for %s' % (request_name))
        request = self.database.get_request(request_name)
        if request is None:
            self.logger.warning('Request %s will not be recalculated because it\'s no longer in database' % (request_name))
            return

        history_entry = self.get_new_history_entry(request)
        added_history_entry = self.add_history_entry_to_request(request, history_entry)
        recalc_end = time.time()
        if added_history_entry:
            self.database.update_request(request)
            self.logger.info('Updated open/done events for %s in %fs' % (request_name, (recalc_end - recalc_start)))
        else:
            self.logger.error('Did not add new history entry for %s' % (request_name))

    def get_new_dict_from_reqmgr2(self, request_name):
        """
        Get request dictionary from RequestManager.
        """
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
                                          'UpdateTime': tr['UpdateTime']} for tr in req_dict.get('RequestTransition', [])]
        req_dict['_id'] = request_name
        req_dict['TotalEvents'] = expected_events
        req_dict['OutputDatasets'] = self.sort_datasets(req_dict['OutputDatasets'])
        req_dict['EventNumberHistory'] = []
        req_dict['RequestPriority'] = int(req_dict.get('RequestPriority', 0))
        return req_dict

    def get_event_count_from_dbs(self, dataset_name):
        """
        Get event count for specified dataset from DBS.
        """
        query_url = '/dbs/prod/global/DBSReader/filesummaries?dataset=%s' % (dataset_name)
        filesummaries = make_request_with_grid_cert(query_url)
        if len(filesummaries) == 0:
            return 0

        return int(filesummaries[0]['num_event'])

    def get_new_history_entry(self, req_dict):
        """
        Form a new history entry dictionary for given request.
        """
        announced = False
        for transition in req_dict['RequestTransition']:
            if transition['Status'] == 'announced':
                announced = True
                break

        history_entry = {'Time': int(time.time()), 'Datasets': {}}
        for dataset_name in req_dict['OutputDatasets']:
            events = self.get_event_count_from_dbs(dataset_name)
            if announced:
                history_entry['Datasets'][dataset_name] = {'OpenEvents': 0,
                                                           'DoneEvents': events}
            else:
                history_entry['Datasets'][dataset_name] = {'OpenEvents': events,
                                                           'DoneEvents': 0}

        return history_entry

    def add_history_entry_to_request(self, req_dict, history_entry):
        """
        Add history entry to request if such entry does not exist.
        """
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
        Get number of expected events of a request.
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

            if 'RequestNumEvents' in req_dict and req_dict['RequestNumEvents'] is not None:
                return int(req_dict['RequestNumEvents'])
            elif 'Task1' in req_dict and 'RequestNumEvents' in req_dict['Task1']:
                return int(req_dict['Task1']['RequestNumEvents'])
            elif 'Step1' in req_dict and 'RequestNumEvents' in req_dict['Step1']:
                return int(req_dict['Step1']['RequestNumEvents'])

        else:
            prep_id = req_dict['PrepID']
            query_url = '/reqmgr2/data/request?mask=TotalInputEvents&mask=RequestType&prep_id=%s' % (prep_id)
            ret = make_request_with_grid_cert(query_url)
            ret = ret['result']
            if len(ret) > 0:
                ret = ret[0]
                for r in ret:
                    if ret[r]['RequestType'].lower() != 'resubmission' and ret[r]['TotalInputEvents'] is not None:
                        return int(f * ret[r]['TotalInputEvents'])

        self.logger.error('%s does not have total events!' % (req_dict['_id']))
        return -1

    def sort_datasets(self, dataset_list):
        """
        Sort dataset list by specific priority list.
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
                             'LHE',
                             'GEN',
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
        """
        Get list of requests that changed in RequestManager since last update.
        """
        last_seq = self.database.get_setting('last_reqmgr_sequence', 0)
        query_url = '/couchdb/reqmgr_workload_cache/_changes?since=%d' % (last_seq)
        self.logger.info('Getting the list of all requests since %d from %s' % (last_seq, query_url))
        response = make_request_with_grid_cert(query_url)
        last_seq = int(response['last_seq'])
        req_list = response['results']
        changed_req_list = list(filter(lambda x: not x.get('deleted', False), req_list))
        changed_req_list = [req['id'] for req in changed_req_list]
        changed_req_list = list(filter(lambda x: '_design' not in x, changed_req_list))
        deleted_req_list = list(filter(lambda x: x.get('deleted', False), req_list))
        deleted_req_list = [req['id'] for req in deleted_req_list]
        deleted_req_list = list(filter(lambda x: '_design' not in x, deleted_req_list))
        self.logger.info('Got %d updated requests. Got %d deleted requests.' % (len(changed_req_list), len(deleted_req_list)))
        return changed_req_list, deleted_req_list, last_seq

    def get_updated_dataset_list_from_dbs(self, since_timestamp=0):
        """
        Get list of datasets that changed since last update.
        """
        query_url = '/dbs/prod/global/DBSReader/datasets?min_ldate=%d' % (since_timestamp)
        self.logger.info('Getting the list of modified datasets since %d from %s' % (since_timestamp, query_url))
        dataset_list = make_request_with_grid_cert(query_url)
        dataset_list = [dataset['dataset'] for dataset in dataset_list]
        self.logger.info('Got %d datasets' % (len(dataset_list)))
        return dataset_list

    def get_list_of_requests_with_changed_datasets(self):
        """
        Get list of requests whose datasets changed since last update.
        """
        self.logger.info('Will get list of changed datasets')
        requests = set()
        last_dataset_modification_date = self.database.get_setting('last_dbs_update_date', 0)
        updated_datasets = self.get_updated_dataset_list_from_dbs(since_timestamp=last_dataset_modification_date)
        self.logger.info('Will find if any of changed datasets belong to requests in database')
        for dataset in updated_datasets:
            dataset_requests = self.database.get_requests_with_dataset(dataset)
            self.logger.info('%d requests contain %s' % (len(dataset_requests), dataset))
            for dataset_request in dataset_requests:
                requests.add(dataset_request['_id'])

        self.logger.info('Found %d requests for changed datasets' % (len(requests)))
        return requests


def main():
    setup_console_logging()
    logger = logging.getLogger('logger')
    parser = argparse.ArgumentParser(description='Stats2 update')
    parser.add_argument('--action',
                        choices=['update', 'see', 'drop'],
                        required=True,
                        help='Action to be performed.')
    parser.add_argument('--name',
                        required=False,
                        help='Request to be updated.')
    args = vars(parser.parse_args())
    logger.info('Arguments %s' % (str(args)))

    action = args.get('action', None)
    name = args.get('name', None)

    if action == 'update':
        stats_update = StatsUpdate()
        stats_update.perform_update(name)
    elif action == 'see':
        request = Database().get_request(name)
        print(json.dumps(request, indent=4))
    elif action == 'drop':
        Database().clear_database()


if __name__ == '__main__':
    main()
