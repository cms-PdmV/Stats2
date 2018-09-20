import logging
import time
import argparse
import json
import traceback
from utils import setup_console_logging
from couchdb_database import Database
from utils import make_cmsweb_request, pick_attributes, make_simple_request


class StatsUpdate():
    """
    Update request info in Stats2 database.
    """

    __SKIPPABLE_STATUS = set(['rejected',
                              'aborted',
                              'failed',
                              'rejected-archived',
                              'aborted-archived',
                              'failed-archived',
                              'aborted-completed'])

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
        and update event recalculation
        """
        self.logger.info('Will update only one request: %s' % (request_name))
        self.update_one(request_name)
        self.recalculate_one(request_name)

    def perform_update_new(self):
        """
        Perform update for all requests that changed since last update and recalculate
        events for files that changed since last update
        """
        update_start = time.time()
        changed_requests, deleted_requests, last_seq = self.get_list_of_changed_requests()
        self.logger.info('Will delete %d requests' % (len(deleted_requests)))
        for request_name in deleted_requests:
            try:
                self.delete_one(request_name)
            except Exception as e:
                self.logger.error('Exception while deleting %s:%s' % (request_name, str(e)))

        self.logger.info('Will update %d requests' % (len(changed_requests)))
        for index, request_name in enumerate(changed_requests):
            try:
                self.logger.info('Will update %d/%d request' % (index + 1, len(changed_requests)))
                self.update_one(request_name)
            except Exception as e:
                self.logger.error('Exception while updating %s:%s\nTraceback:%s' % (request_name,
                                                                                    str(e),
                                                                                    traceback.format_exc()))

        update_end = time.time()
        self.logger.info('Finished updating requests')
        self.logger.info('Will update event count')
        changed_datasets = self.get_list_of_requests_with_changed_datasets()
        requests_to_recalculate = set(changed_requests).union(set(changed_datasets))

        self.logger.info('Will update event count for %d requests' % (len(requests_to_recalculate)))
        for index, request_name in enumerate(requests_to_recalculate):
            try:
                self.logger.info('Will update event count for %d/%d' % (index + 1, len(requests_to_recalculate)))
                self.recalculate_one(request_name)
            except Exception as e:
                self.logger.error('Exception while updating event count %s:%s\nTraceback:%s' % (request_name,
                                                                                                str(e),
                                                                                                traceback.format_exc()))

        recalculation_end = time.time()
        self.database.set_setting('last_reqmgr_sequence', int(last_seq))
        self.database.set_setting('last_dbs_update_date', int(update_start))
        self.logger.info('Updated and deleted %d/%d requests in %.3fs' % (len(changed_requests), len(deleted_requests),
                                                                          (update_end - update_start)))
        self.logger.info('Updated event count for %d requests in %.3fs' % (len(requests_to_recalculate),
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
                self.logger.info('Skipping and deleting %s because it\'s status is %s' % (request_name,
                                                                                          req_transition['Status']))
                self.database.delete_request(request_name)
                return

        req_dict_old = self.database.get_request(request_name)
        if req_dict_old is None:
            req_dict_old = {'_id': request_name}
            self.logger.info('Inserting %s' % (request_name))
            self.database.update_request(req_dict_old)
            req_dict_old = self.database.get_request(request_name)
            # self.steal_history_from_old_stats(req_dict_old)

        req_dict['_rev'] = req_dict_old['_rev']
        req_dict['EventNumberHistory'] = req_dict_old.get('EventNumberHistory', [])
        req_dict['OutputDatasets'] = self.sort_datasets(req_dict['OutputDatasets'])
        self.database.update_request(req_dict)
        update_end = time.time()
        self.logger.info('Updated %s in %.3fs' % (request_name, (update_end - update_start)))

    def delete_one(self, request_name):
        """
        Action to delete one request from database.
        """
        self.logger.info('Deleting %s' % (request_name))
        self.database.delete_request(request_name)
        self.logger.info('Deleted %s' % (request_name))

    def recalculate_one(self, request_name):
        """
        Action to update event count for request.
        """
        recalc_start = time.time()
        self.logger.info('Updating event count for %s' % (request_name))
        request = self.database.get_request(request_name)
        if request is None:
            self.logger.warning('Will not update %s event count because it\'s no longer in database' % (request_name))
            return

        history_entry = self.get_new_history_entry(request)
        added_history_entry = self.add_history_entry_to_request(request, history_entry)
        recalc_end = time.time()
        if added_history_entry:
            self.database.update_request(request)
            self.logger.info('Updated event count for %s in %fs' % (request_name, (recalc_end - recalc_start)))
        else:
            self.logger.info('Did not update event count for %s' % (request_name))

    def get_new_dict_from_reqmgr2(self, request_name):
        """
        Get request dictionary from RequestManager.
        """
        url = '/couchdb/reqmgr_workload_cache/%s' % (request_name)
        req_dict = make_cmsweb_request(url)
        expected_events = self.get_expected_events_with_dict(req_dict)
        campaigns = self.get_campaigns_from_request(req_dict)
        req_dict = pick_attributes(req_dict, ['AcquisitionEra',
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
        req_dict['Campaigns'] = campaigns
        req_dict['OutputDatasets'] = self.sort_datasets(req_dict['OutputDatasets'])
        req_dict['EventNumberHistory'] = []
        req_dict['RequestPriority'] = int(req_dict.get('RequestPriority', 0))
        return req_dict

    def get_event_count_from_dbs(self, dataset_name):
        """
        Get event count for specified dataset from DBS.
        """
        query_url = '/dbs/prod/global/DBSReader/filesummaries?dataset=%s' % (dataset_name)
        filesummaries = make_cmsweb_request(query_url)
        if len(filesummaries) == 0:
            return 0

        return int(filesummaries[0]['num_event'])

    def get_new_history_entry(self, req_dict, depth=0):
        """
        Form a new history entry dictionary for given request.
        """
        output_datasets = req_dict.get('OutputDatasets', [])
        output_datasets_set = set(output_datasets)
        if len(output_datasets) == 0:
            return None

        history_entry = {'Time': int(time.time()), 'Datasets': {}}
        dataset_list_url = '/dbs/prod/global/DBSReader/datasetlist'
        dbs_dataset_list = make_cmsweb_request(dataset_list_url, {'dataset': output_datasets, 'detail': 1})
        for dbs_dataset in dbs_dataset_list:
            dataset_name = dbs_dataset['dataset']
            history_entry['Datasets'][dataset_name] = {'Type': dbs_dataset['dataset_access_type'],
                                                       'Events': self.get_event_count_from_dbs(dataset_name)}
            output_datasets_set.remove(dataset_name)

        for dataset in output_datasets_set:
            history_entry['Datasets'][dataset] = {'Type': 'NONE',
                                                  'Events': 0}

        if len(history_entry['Datasets']) != len(output_datasets):
            self.logger.error('Wrong number of datasets for %s, returning None' % (req_dict['_id']))
            return None

        return history_entry

    def add_history_entry_to_request(self, req_dict, new_history_entry):
        """
        Add history entry to request if such entry does not exist.
        """
        if new_history_entry is None:
            return False

        new_dict_string = json.dumps(new_history_entry['Datasets'], sort_keys=True)
        history_entries = req_dict['EventNumberHistory']
        for history_entry in history_entries:
            old_dict_string = json.dumps(history_entry['Datasets'], sort_keys=True)
            if new_dict_string == old_dict_string:
                return False

        history_entries.append(new_history_entry)
        # self.logger.info(json.dumps(history_entry, indent=2))
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
            elif 'Task1' in req_dict and 'InputDataset' in req_dict['Task1']:
                return self.get_event_count_from_dbs(req_dict['Task1']['InputDataset'])
            elif 'Step1' in req_dict and 'InputDataset' in req_dict['Step1']:
                return self.get_event_count_from_dbs(req_dict['Step1']['InputDataset'])

        else:
            prep_id = req_dict['PrepID']
            url = '/reqmgr2/data/request?mask=TotalInputEvents&mask=RequestType&prep_id=%s' % (prep_id)
            ret = make_cmsweb_request(url)
            ret = ret['result']
            if len(ret) > 0:
                ret = ret[0]
                for r in ret:
                    if ret[r]['RequestType'].lower() != 'resubmission' and ret[r]['TotalInputEvents'] is not None:
                        return int(f * ret[r]['TotalInputEvents'])

        self.logger.error('%s does not have total events!' % (req_dict['_id']))
        return -1

    def get_campaigns_from_request(self, req_dict):
        """
        Get list of campaigns or acquisition eras in tasks. If there are no tasks, request's
        campaign or acquisition era will be used
        """
        task_number = 1
        campaigns = []
        while True:
            task_name = 'Task%s' % task_number
            if task_name not in req_dict:
                break

            if 'Campaign' in req_dict[task_name]\
                    and req_dict[task_name]['Campaign'] is not None\
                    and len(req_dict[task_name]['Campaign']) > 0:
                campaigns.append(req_dict[task_name]['Campaign'])
            elif 'AcquisitionEra' in req_dict[task_name]\
                    and req_dict[task_name]['AcquisitionEra'] is not None\
                    and len(req_dict[task_name]['AcquisitionEra']) > 0:
                campaigns.append(req_dict[task_name]['AcquisitionEra'])

            task_number += 1

        if len(campaigns) == 0:
            if 'Campaign' in req_dict\
                    and req_dict['Campaign'] is not None\
                    and len(req_dict['Campaign']) > 0:
                campaigns.append(req_dict['Campaign'])
            elif 'AcquisitionEra' in req_dict\
                    and req_dict['AcquisitionEra'] is not None\
                    and len(req_dict['AcquisitionEra']) > 0:
                campaigns.append(req_dict['AcquisitionEra'])

        return campaigns

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
            tier_priority = ['USER',
                             'FEVT',
                             'RAW-HLT',
                             'ALCARECO',
                             'ALCAPROMPT',
                             'HLT',
                             'DQM',
                             'DQMIO',
                             'DQMROOT',
                             'GEN-SIM-RECODEBUG',
                             'GEN-SIM-DIGI-RECODEBUG',
                             'GEN-SIM-RAWDEBUG',
                             'GEN-SIM-RAW-HLTDEBUG',
                             'GEN-SIM-RAW-HLTDEBUG-RECO',
                             'GEN-SIM-RAW-HLTDEBUG-RECODEBUG',
                             'GEN-SIM-DIGI-RAW-HLTDEBUG-RECO',
                             'GEN-SIM-DIGI-RAW-HLTDEBUG',
                             'GEN-SIM-DIGI-HLTDEBUG-RECO',
                             'GEN-SIM-DIGI-HLTDEBUG',
                             'FEVTDEBUGHLT',
                             'GEN-RAWDEBUG',
                             'RAWDEBUG',
                             'RECODEBUG',
                             'HLTDEBUG',
                             'RAWRECOSIMHLT',
                             'RAW-RECOSIMHLT',
                             'RECOSIMHLT',
                             'FEVTHLTALL',
                             'PREMIXRAW',
                             'PREMIX-RAW',
                             'RAW',
                             'RAW-RECO',
                             'LHE',
                             'GEN',
                             'GEN-RAW',
                             'GEN-SIM',
                             'SIM',
                             'DIGI',
                             'DIGI-RECO',
                             'RECO',
                             'RAWAODSIM',
                             'GEN-SIM-RECO',
                             'GEN-SIM-RAW',
                             'GEN-SIM-RAW-HLT',
                             'GEN-SIM-RAW-RECO',
                             'GEN-SIM-DIGI',
                             'GEN-SIM-DIGI-RECO',
                             'GEN-SIM-DIGI-RAW',
                             'GEN-SIM-DIGI-RAW-RECO',
                             'AOD',
                             'AODSIM',
                             'MINIAOD',
                             'MINIAODSIM',
                             'NANOAOD',
                             'NANOAODSIM']

            for (p, t) in enumerate(tier_priority):
                if t.upper() == tier:
                    return p

            return -1

        dataset_list = sorted(dataset_list, key=tierLevel)
        return dataset_list

    def get_list_of_changed_requests(self):
        """
        Get list of requests that changed in RequestManager since last update.
        """
        last_seq = self.database.get_setting('last_reqmgr_sequence', 0)
        url = '/couchdb/reqmgr_workload_cache/_changes?since=%d' % (last_seq)
        self.logger.info('Getting the list of all requests since %d from %s' % (last_seq, url))
        response = make_cmsweb_request(url)
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
        url = '/dbs/prod/global/DBSReader/datasets?min_ldate=%d&dataset_access_type=*' % (since_timestamp)
        self.logger.info('Getting the list of modified datasets since %d from %s' % (since_timestamp, url))
        dataset_list = make_cmsweb_request(url)
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
            dataset_requests = self.database.get_requests_with_dataset(dataset, page_size=1000)
            self.logger.info('%d requests contain %s' % (len(dataset_requests), dataset))
            requests.update(dataset_requests)

        requests_from_wmstats = self.get_active_requests_from_wmstats()
        requests.update(set(requests_from_wmstats))

        self.logger.info('Found %d requests for changed datasets' % (len(requests)))
        return requests

    def get_active_requests_from_wmstats(self):
        """
        Get list of requests which are currently putting data to DBS.
        """
        self.logger.info('Will get list of requests which are currently putting data to DBS')
        url = '/wmstatsserver/data/filtered_requests?mask=RequestName'
        request_list = make_cmsweb_request(url).get('result', [])
        request_list = [request['RequestName'] for request in request_list]

        self.logger.info('Found %d requests which are currently putting data to DBS' % (len(request_list)))
        return request_list

    def steal_history_from_old_stats(self, req_dict):
        from time import strptime, mktime
        self.logger.info('Stealing history for %s from old Stats... ;)' % (req_dict['_id']))
        if 'EventNumberHistory' not in req_dict:
            req_dict['EventNumberHistory'] = []

        try:
            stats_url = "http://vocms074:5984/stats/%s" % (req_dict['_id'])
            stats_req = make_simple_request(stats_url)
            stats_history = stats_req.get('pdmv_monitor_history', [])
            for stats_history_entry in stats_history:
                timestamp = mktime(strptime(stats_history_entry['pdmv_monitor_time']))
                new_history_entry = {'Time': int(timestamp), 'Datasets': {}}
                for dataset, events_dict in stats_history_entry.get('pdmv_dataset_statuses', {}).items():
                    type_in_stats = events_dict.get('pdmv_status_in_DAS', 'NONE')
                    if not type_in_stats:
                        type_in_stats = 'NONE'

                    events_in_stats = int(events_dict.get('pdmv_evts_in_DAS', 0))
                    new_history_entry['Datasets'][dataset] = {'Events': events_in_stats,
                                                              'Type': type_in_stats}

                self.add_history_entry_to_request(req_dict, new_history_entry)

            def sort_by_time(history_entry):
                return history_entry['Time']

            req_dict['EventNumberHistory'].sort(key=sort_by_time)
        except Exception as ex:
            self.logger.error(ex)


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
