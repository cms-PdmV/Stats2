import logging
import time
import argparse
import json
import traceback
from utils import setup_console_logging
from couchdb_database import Database
from utils import make_cmsweb_request, pick_attributes, make_simple_request
import subprocess


class StatsUpdate():
    """
    Update workflow info in Stats2 database.
    """

    def __init__(self):
        self.logger = logging.getLogger('logger')
        self.database = Database()
        self.dataset_event_cache = {}
        self.dataset_info_cache = {}

    def perform_update(self, workflow_name=None, trigger_prod=False, trigger_dev=False):
        """
        Perform update for specific workflow if workflow name is given or for all changed
        workflows if no name is specified.
        """
        if workflow_name is not None:
            self.perform_update_one(workflow_name, trigger_prod, trigger_dev)
        else:
            self.perform_update_new(trigger_prod, trigger_dev)

        self.logger.info('Workflows after update %d' % (self.database.get_workflow_count()))

    def perform_update_one(self, workflow_name, trigger_prod=False, trigger_dev=False):
        """
        Perform update for specific workflow: fetch new dictionary from RequestManager
        and update event recalculation
        """
        self.logger.info('Will update only one workflow: %s' % (workflow_name))
        self.update_one(workflow_name)
        self.recalculate_one(workflow_name)

    def perform_update_new(self, trigger_prod=False, trigger_dev=False):
        """
        Perform update for all workflows that changed since last update and recalculate
        events for files that changed since last update
        """
        update_start = time.time()
        changed_workflows, deleted_workflows, last_seq = self.get_list_of_changed_workflows()
        self.logger.info('Will delete %d workflows' % (len(deleted_workflows)))
        for workflow_name in deleted_workflows:
            try:
                self.delete_one(workflow_name)
            except Exception as e:
                self.logger.error('Exception while deleting %s:%s' % (workflow_name, str(e)))

        previously_crashed_workflows = self.get_list_of_previously_crashed_workflows()
        self.logger.info('Have %d workflows that crashed during last update' % (len(previously_crashed_workflows)))
        changed_workflows = set(changed_workflows).union(set(previously_crashed_workflows))
        self.logger.info('Will update %d workflows' % (len(changed_workflows)))
        for index, workflow_name in enumerate(changed_workflows):
            try:
                self.logger.info('Will update %d/%d workflow' % (index + 1, len(changed_workflows)))
                self.update_one(workflow_name, trigger_prod, trigger_dev)
                self.remove_from_list_of_crashed_workflows(workflow_name)
            except Exception as e:
                self.add_to_list_of_crashed_workflows(workflow_name)
                self.logger.error('Exception while updating %s:%s\nTraceback:%s' % (workflow_name,
                                                                                    str(e),
                                                                                    traceback.format_exc()))

        update_end = time.time()
        self.logger.info('Finished updating workflows')
        self.logger.info('Will update event count')
        changed_datasets = self.get_list_of_workflows_with_changed_datasets()
        workflows_to_recalculate = set(changed_workflows).union(set(changed_datasets))

        self.logger.info('Will update event count for %d workflows' % (len(workflows_to_recalculate)))
        for index, workflow_name in enumerate(workflows_to_recalculate):
            try:
                self.logger.info('Will update event count for %d/%d' % (index + 1, len(workflows_to_recalculate)))
                self.recalculate_one(workflow_name, trigger_prod, trigger_dev)
                self.remove_from_list_of_crashed_workflows(workflow_name)
            except Exception as e:
                self.add_to_list_of_crashed_workflows(workflow_name)
                self.logger.error('Exception while updating event count %s:%s\nTraceback:%s' % (workflow_name,
                                                                                                str(e),
                                                                                                traceback.format_exc()))

        recalculation_end = time.time()
        self.database.set_setting('last_reqmgr_sequence', int(last_seq))
        self.database.set_setting('last_dbs_update_date', int(update_start))
        self.logger.info('Updated and deleted %d/%d workflows in %.3fs' % (len(changed_workflows), len(deleted_workflows),
                                                                           (update_end - update_start)))
        self.logger.info('Updated event count for %d workflows in %.3fs' % (len(workflows_to_recalculate),
                                                                            (recalculation_end - update_end)))

    def update_one(self, workflow_name, trigger_prod=False, trigger_dev=False):
        """
        Action to update one workflow's dictionary from RequestManager. If no such
        workflow exist in database, new one will be created.
        """
        self.logger.info('Updating %s' % (workflow_name))
        update_start = time.time()
        wf_dict = self.get_new_dict_from_reqmgr2(workflow_name)
        wf_dict_old = self.database.get_workflow(workflow_name)
        if wf_dict_old is None:
            wf_dict_old = {'_id': workflow_name}
            self.logger.info('Inserting %s' % (workflow_name))
            self.database.update_workflow(wf_dict_old)
            wf_dict_old = self.database.get_workflow(workflow_name)
            # self.steal_history_from_old_stats(wf_dict_old)

        wf_dict['_rev'] = wf_dict_old['_rev']
        wf_dict['EventNumberHistory'] = wf_dict_old.get('EventNumberHistory', [])
        wf_dict['OutputDatasets'] = self.sort_datasets(wf_dict['OutputDatasets'])
        old_wf_dict_string = json.dumps(wf_dict_old, sort_keys=True)
        new_wf_dict_string = json.dumps(wf_dict, sort_keys=True)
        update_end = time.time()
        if old_wf_dict_string != new_wf_dict_string:
            self.database.update_workflow(wf_dict)
            self.logger.info('Updated %s in %.3fs' % (workflow_name, (update_end - update_start)))
            self.trigger_outside(workflow_name, trigger_prod, trigger_dev)
        else:
            self.logger.info('Did not update %s because it did not change. Time: %.3fs' % (workflow_name,
                                                                                           (update_end - update_start)))

    def delete_one(self, workflow_name):
        """
        Action to delete one workflow from database.
        """
        self.logger.info('Deleting %s' % (workflow_name))
        self.database.delete_workflow(workflow_name)
        self.logger.info('Deleted %s' % (workflow_name))

    def recalculate_one(self, workflow_name, trigger_prod=False, trigger_dev=False):
        """
        Action to update event count for workflow.
        """
        recalc_start = time.time()
        self.logger.info('Updating event count for %s' % (workflow_name))
        workflow = self.database.get_workflow(workflow_name)
        if workflow is None:
            self.logger.warning('Will not update %s event count because it\'s no longer in database' % (workflow_name))
            return

        history_entry = self.get_new_history_entry(workflow)
        added_history_entry = self.add_history_entry_to_workflow(workflow, history_entry)
        recalc_end = time.time()
        if added_history_entry:
            self.database.update_workflow(workflow)
            self.logger.info('Updated event count for %s in %.3fs' % (workflow_name, (recalc_end - recalc_start)))
            self.trigger_outside(workflow_name, trigger_prod, trigger_dev)
        else:
            self.logger.info('Did not update event count for %s because it did not change. '
                             'Time: %.3fs' % (workflow_name,
                                              (recalc_end - recalc_start)))

    def get_new_dict_from_reqmgr2(self, workflow_name):
        """
        Get workflow dictionary from RequestManager.
        """
        url = '/couchdb/reqmgr_workload_cache/%s' % (workflow_name)
        wf_dict = make_cmsweb_request(url)
        expected_events = self.get_expected_events_with_dict(wf_dict)
        campaigns = self.get_campaigns_from_workflow(wf_dict)
        requests = self.get_requests_from_workflow(wf_dict)
        attributes = ['AcquisitionEra',
                      'CMSSWVersion',
                      'InputDataset',
                      'OutputDatasets',
                      'PrepID',
                      'ProcessingString',
                      'RequestName',
                      'RequestPriority',
                      'RequestTransition',
                      'RequestType',
                      'SizePerEvent',
                      'TimePerEvent']
        wf_dict = pick_attributes(wf_dict, attributes)
        wf_dict['RequestTransition'] = [{'Status': tr['Status'],
                                         'UpdateTime': tr['UpdateTime']} for tr in wf_dict.get('RequestTransition', [])]
        wf_dict['_id'] = workflow_name
        wf_dict['TotalEvents'] = expected_events
        wf_dict['Campaigns'] = campaigns
        wf_dict['Requests'] = requests
        wf_dict['OutputDatasets'] = self.sort_datasets(self.flat_list(wf_dict['OutputDatasets']))
        wf_dict['EventNumberHistory'] = []
        wf_dict['RequestPriority'] = int(wf_dict.get('RequestPriority', 0))
        if 'ProcessingString' in wf_dict and not isinstance(wf_dict['ProcessingString'], str):
            del wf_dict['ProcessingString']

        if 'PrepID' in wf_dict and wf_dict['PrepID'] is None:
            del wf_dict['PrepID']

        return wf_dict

    def flat_list(self, given_list):
        """
        Make list of lists to flat list
        """
        new_list = []
        for element in given_list:
            if not isinstance(element, list):
                new_list.append(element)
            else:
                new_list += self.flat_list(element)

        return new_list

    def get_event_count_from_dbs(self, dataset_name):
        """
        Get event count for specified dataset from DBS.
        """
        if dataset_name in self.dataset_event_cache:
            num_event = self.dataset_event_cache[dataset_name]
            self.logger.info('Found number of events (%s) for %s in cache' % (num_event, dataset_name))
            return num_event

        query_url = '/dbs/prod/global/DBSReader/filesummaries?dataset=%s' % (dataset_name)
        filesummaries = make_cmsweb_request(query_url)
        num_event = 0
        if len(filesummaries) != 0:
            num_event = int(filesummaries[0]['num_event'])

        self.dataset_event_cache[dataset_name] = num_event
        return num_event

    def get_new_history_entry(self, wf_dict, depth=0):
        """
        Form a new history entry dictionary for given workflow.
        """
        output_datasets = wf_dict.get('OutputDatasets', [])
        if len(output_datasets) == 0:
            return None

        output_datasets_set = set(output_datasets)
        history_entry = {'Time': int(time.time()), 'Datasets': {}}
        dataset_list_url = '/dbs/prod/global/DBSReader/datasetlist'
        output_datasets_to_query = []
        for output_dataset in set(output_datasets):
            if output_dataset in self.dataset_info_cache:
                cache_entry = self.dataset_info_cache[output_dataset]
                self.logger.info('Found %s dataset info in cache: %s %s' % (output_dataset,
                                                                            cache_entry['Type'],
                                                                            cache_entry['Events']))
                history_entry['Datasets'][output_dataset] = cache_entry
                output_datasets_set.remove(output_dataset)
            else:
                output_datasets_to_query.append(output_dataset)

        if output_datasets_to_query:
            dbs_dataset_list = make_cmsweb_request(dataset_list_url, {'dataset': output_datasets_to_query, 'detail': 1})
        else:
            self.logger.info('Not doing a request to %s because all datasets were in cache' % (dataset_list_url))
            dbs_dataset_list = []

        for dbs_dataset in dbs_dataset_list:
            dataset_name = dbs_dataset['dataset']
            history_entry['Datasets'][dataset_name] = {'Type': dbs_dataset['dataset_access_type'],
                                                       'Events': self.get_event_count_from_dbs(dataset_name)}
            self.dataset_info_cache[dataset_name] = dict(history_entry['Datasets'][dataset_name])
            self.logger.info('Setting %s events and %s type for %s (%s)' % (history_entry['Datasets'][dataset_name]['Events'],
                                                                            history_entry['Datasets'][dataset_name]['Type'],
                                                                            dataset_name,
                                                                            wf_dict.get('_id')))
            output_datasets_set.remove(dataset_name)

        for dataset_name in output_datasets_set:
            history_entry['Datasets'][dataset_name] = {'Type': 'NONE',
                                                       'Events': 0}
            self.dataset_info_cache[dataset_name] = dict(history_entry['Datasets'][dataset_name])
            self.logger.info('Setting %s events and %s type for %s (%s)' % (history_entry['Datasets'][dataset_name]['Events'],
                                                                            history_entry['Datasets'][dataset_name]['Type'],
                                                                            dataset_name,
                                                                            wf_dict.get('_id')))

        if len(history_entry['Datasets']) != len(set(output_datasets)):
            self.logger.error('Wrong number of datasets for %s. '
                              'New history item - %s, '
                              'output datasets - %s, '
                              'returning None' % (wf_dict['_id'],
                                                  len(history_entry['Datasets']),
                                                  len(output_datasets)))
            return None

        return history_entry

    def add_history_entry_to_workflow(self, wf_dict, new_history_entry):
        """
        Add history entry to workflow if such entry does not exist.
        """
        if new_history_entry is None:
            return False

        new_dict_string = json.dumps(new_history_entry['Datasets'], sort_keys=True)
        history_entries = wf_dict['EventNumberHistory']
        for history_entry in history_entries:
            old_dict_string = json.dumps(history_entry['Datasets'], sort_keys=True)
            if new_dict_string == old_dict_string:
                return False

        history_entries.append(new_history_entry)
        # self.logger.info(json.dumps(history_entry, indent=2))
        return True

    def get_expected_events_with_dict(self, wf_dict):
        """
        Get number of expected events of a workflow.
        """
        if 'FilterEfficiency' in wf_dict:
            f = float(wf_dict['FilterEfficiency'])
        elif 'Task1' in wf_dict and 'FilterEfficiency' in wf_dict['Task1']:
            f = float(wf_dict['Task1']['FilterEfficiency'])
        elif 'Step1' in wf_dict and 'FilterEfficiency' in wf_dict['Step1']:
            f = float(wf_dict['Step1']['FilterEfficiency'])
        else:
            f = 1.

        wf_type = wf_dict.get('RequestType', '').lower()
        if wf_type != 'resubmission':
            if wf_dict.get('TotalInputFiles', 0) > 0:
                if 'TotalInputEvents' in wf_dict:
                    return int(f * wf_dict['TotalInputEvents'])

            if 'RequestNumEvents' in wf_dict and wf_dict['RequestNumEvents'] is not None:
                return int(wf_dict['RequestNumEvents'])
            elif 'Task1' in wf_dict and 'RequestNumEvents' in wf_dict['Task1']:
                return int(wf_dict['Task1']['RequestNumEvents'])
            elif 'Step1' in wf_dict and 'RequestNumEvents' in wf_dict['Step1']:
                return int(wf_dict['Step1']['RequestNumEvents'])
            elif 'Task1' in wf_dict and 'InputDataset' in wf_dict['Task1']:
                return self.get_event_count_from_dbs(wf_dict['Task1']['InputDataset'])
            elif 'Step1' in wf_dict and 'InputDataset' in wf_dict['Step1']:
                return self.get_event_count_from_dbs(wf_dict['Step1']['InputDataset'])

        else:
            prep_id = wf_dict['PrepID']
            url = '/reqmgr2/data/request?mask=TotalInputEvents&mask=RequestType&prep_id=%s' % (prep_id)
            ret = make_cmsweb_request(url)
            ret = ret['result']
            if len(ret) > 0:
                ret = ret[0]
                for r in ret:
                    if ret[r]['RequestType'].lower() != 'resubmission' and ret[r]['TotalInputEvents'] is not None:
                        return int(f * ret[r]['TotalInputEvents'])

        self.logger.error('%s does not have total events!' % (wf_dict['_id']))
        return -1

    def get_campaigns_from_workflow(self, wf_dict):
        """
        Get list of campaigns or acquisition eras in tasks. If there are no tasks, workflow's
        campaign or acquisition era will be used
        """
        task_number = 1
        campaigns = []
        # Check whether it's a TaskChain or a StepChain
        if 'StepChain' in wf_dict:
            task_format = 'Step%s'
        else:
            task_format = 'Task%s'

        while True:
            task_name = task_format % task_number
            if task_name not in wf_dict:
                break

            if 'Campaign' in wf_dict[task_name]\
                    and wf_dict[task_name]['Campaign'] is not None\
                    and len(wf_dict[task_name]['Campaign']) > 0:
                campaigns.append(wf_dict[task_name]['Campaign'])
            elif 'AcquisitionEra' in wf_dict[task_name]\
                    and wf_dict[task_name]['AcquisitionEra'] is not None\
                    and len(wf_dict[task_name]['AcquisitionEra']) > 0:
                campaigns.append(wf_dict[task_name]['AcquisitionEra'])

            task_number += 1

        if len(campaigns) == 0:
            if 'Campaign' in wf_dict\
                    and wf_dict['Campaign'] is not None\
                    and len(wf_dict['Campaign']) > 0:
                campaigns.append(wf_dict['Campaign'])
            elif 'AcquisitionEra' in wf_dict\
                    and wf_dict['AcquisitionEra'] is not None\
                    and len(wf_dict['AcquisitionEra']) > 0:
                campaigns.append(wf_dict['AcquisitionEra'])

        return campaigns

    def get_requests_from_workflow(self, wf_dict):
        """
        Get list of request prepids
        """
        task_number = 1
        requests = []
        # Check whether it's a TaskChain or a StepChain
        if 'StepChain' in wf_dict:
            task_format = 'Step%s'
        else:
            task_format = 'Task%s'

        while True:
            task_name = task_format % task_number
            if task_name not in wf_dict:
                break

            if 'PrepID' in wf_dict[task_name]\
                    and wf_dict[task_name]['PrepID'] is not None\
                    and len(wf_dict[task_name]['PrepID']) > 0:
                requests.append(wf_dict[task_name]['PrepID'])

            task_number += 1

        return requests

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

    def get_list_of_changed_workflows(self):
        """
        Get list of workflows that changed in RequestManager since last update.
        """
        last_seq = self.database.get_setting('last_reqmgr_sequence', 0)
        url = '/couchdb/reqmgr_workload_cache/_changes?since=%d' % (last_seq)
        self.logger.info('Getting the list of all workflows since %d from %s' % (last_seq, url))
        response = make_cmsweb_request(url)
        last_seq = int(response['last_seq'])
        wf_list = response['results']
        changed_wf_list = list(filter(lambda x: not x.get('deleted', False), wf_list))
        changed_wf_list = [wf['id'] for wf in changed_wf_list]
        changed_wf_list = list(filter(lambda x: '_design' not in x, changed_wf_list))
        deleted_wf_list = list(filter(lambda x: x.get('deleted', False), wf_list))
        deleted_wf_list = [wf['id'] for wf in deleted_wf_list]
        deleted_wf_list = list(filter(lambda x: '_design' not in x, deleted_wf_list))
        self.logger.info('Got %d updated workflows. Got %d deleted workflows.' % (len(changed_wf_list), len(deleted_wf_list)))
        return changed_wf_list, deleted_wf_list, last_seq

    def get_updated_dataset_list_from_dbs(self, since_timestamp=0):
        """
        Get list of datasets that changed since last update.
        """
        url = '/dbs/prod/global/DBSReader/datasets?min_ldate=%d&dataset_access_type=*' % (since_timestamp)
        self.logger.info('Getting the list of modified datasets since %d from %s' % (since_timestamp, url))
        dataset_list = make_cmsweb_request(url)
        if dataset_list is None:
            self.logger.error('Could not get list of modified datasets since %d from %s' % (since_timestamp, url))

        dataset_list = [dataset['dataset'] for dataset in dataset_list]
        self.logger.info('Got %d datasets' % (len(dataset_list)))
        return dataset_list

    def get_list_of_workflows_with_changed_datasets(self):
        """
        Get list of workflows whose datasets changed since last update.
        """
        self.logger.info('Will get list of changed datasets')
        workflows = set()
        last_dataset_modification_date = self.database.get_setting('last_dbs_update_date', 0)
        updated_datasets = self.get_updated_dataset_list_from_dbs(since_timestamp=last_dataset_modification_date)
        self.logger.info('Will find if any of changed datasets belong to workflows in database')
        for dataset in updated_datasets:
            dataset_workflows = self.database.get_workflows_with_dataset(dataset, page_size=1000)
            self.logger.info('%d workflows contain %s' % (len(dataset_workflows), dataset))
            workflows.update(dataset_workflows)

        workflows_from_wmstats = self.get_active_workflows_from_wmstats()
        workflows.update(set(workflows_from_wmstats))

        self.logger.info('Found %d workflows for changed datasets' % (len(workflows)))
        return workflows

    def get_active_workflows_from_wmstats(self):
        """
        Get list of workflows which are currently putting data to DBS.
        """
        self.logger.info('Will get list of workflows which are currently putting data to DBS')
        url = '/wmstatsserver/data/filtered_requests?mask=RequestName'
        workflow_list = make_cmsweb_request(url, timeout=600, keep_open=False)
        if workflow_list is None:
            self.logger.error('Could not get list of workflows from wmstats')

        workflow_list = workflow_list.get('result', [])
        workflow_list = [workflow['RequestName'] for workflow in workflow_list]

        self.logger.info('Found %d workflows which are currently putting data to DBS' % (len(workflow_list)))
        return workflow_list

    def steal_history_from_old_stats(self, wf_dict):
        from time import strptime, mktime
        self.logger.info('Stealing history for %s from old Stats... ;)' % (wf_dict['_id']))
        if 'EventNumberHistory' not in wf_dict:
            wf_dict['EventNumberHistory'] = []

        try:
            stats_url = "http://vocms074:5984/stats/%s" % (wf_dict['_id'])
            stats_wf = make_simple_request(stats_url)
            stats_history = stats_wf.get('pdmv_monitor_history', [])
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

                self.add_history_entry_to_workflow(wf_dict, new_history_entry)

            def sort_by_time(history_entry):
                return history_entry['Time']

            wf_dict['EventNumberHistory'].sort(key=sort_by_time)
        except Exception as ex:
            self.logger.error(ex)

    def get_list_of_previously_crashed_workflows(self):
        """
        Return list of workflows that failed during previous update
        """
        workflows = self.database.get_setting('failed_workflows', [])
        return list(set(workflows))

    def remove_from_list_of_crashed_workflows(self, workflow_name):
        """
        Remove workflow from list of failed workflows that should be updated during next update
        """
        workflows = self.get_list_of_previously_crashed_workflows()
        if workflow_name in set(workflows):
            workflows = [x for x in workflows if x != workflow_name]
            self.database.set_setting('failed_workflows', workflows)

    def add_to_list_of_crashed_workflows(self, workflow_name):
        """
        Add workflow to list of failed workflows that should be updated during next update
        """
        workflows = self.get_list_of_previously_crashed_workflows()
        if workflow_name not in set(workflows):
            workflows.append(workflow_name)
            self.database.set_setting('failed_workflows', workflows)

    def trigger_outside(self, workflow_name, trigger_prod=False, trigger_dev=False):
        """
        Trigger something outside (McM) when workflow is updated
        """
        outside_urls = []
        if trigger_prod:
            outside_urls.append({'url': 'https://cms-pdmv.cern.ch/mcm/restapi/requests/fetch_stats_by_wf/%s' % (workflow_name),
                                 'cookie': 'prod_cookie.txt'})

        if trigger_dev:
            outside_urls.append({'url': 'https://cms-pdmv-dev.cern.ch/mcm/restapi/requests/fetch_stats_by_wf/%s' % (workflow_name),
                                 'cookie': 'dev_cookie.txt'})

        for outside in outside_urls:
            try:
                self.logger.info('Triggering outside (McM) for %s' % (workflow_name))
                args = ['curl',
                        outside['url'],
                        '-s',  # Silent
                        '-k',  # Ignore invalid https certificate
                        '-L',  # Follow 3xx codes
                        '-m 60',  # Timeout 60s
                        '-w %{http_code}',  # Return only HTTP code
                        '-o /dev/null']
                if outside.get('cookie'):
                    self.logger.info('Append cookie "%s" while making request for %s' % (outside['cookie'], workflow_name))
                    args += ['--cookie', outside['cookie']]

                args = ' '.join(args)
                proc = subprocess.Popen(args, stdout=subprocess.PIPE, shell=True)
                code = proc.communicate()[0]
                code = int(code)
                self.logger.info('HTTP code %s for %s' % (code, workflow_name))
            except Exception as ex:
                self.logger.error('Exception while trigerring %s for %s. Exception: %s' % (outside['url'],
                                                                                           workflow_name,
                                                                                           str(ex)))


def main():
    setup_console_logging()
    logger = logging.getLogger('logger')
    parser = argparse.ArgumentParser(description='Stats2 update')
    parser.add_argument('--action',
                        choices=['update', 'see'],
                        required=True,
                        help='Action to be performed.')
    parser.add_argument('--name',
                        required=False,
                        help='Workflow to be updated.')
    parser.add_argument('--trigger-prod',
                        required=False,
                        action='store_true',
                        help='Trigger production McM to update')
    parser.add_argument('--trigger-dev',
                        required=False,
                        action='store_true',
                        help='Trigger development McM to update')
    args = vars(parser.parse_args())
    logger.info('Arguments %s' % (str(args)))

    action = args.get('action', None)
    name = args.get('name', None)
    trigger_prod = args.get('trigger_prod', False)
    trigger_dev = args.get('trigger_dev', False)

    if action == 'update':
        stats_update = StatsUpdate()
        stats_update.perform_update(name, trigger_prod, trigger_dev)
    elif action == 'see':
        workflow = Database().get_workflow(name)
        print(json.dumps(workflow, indent=4))


if __name__ == '__main__':
    main()
