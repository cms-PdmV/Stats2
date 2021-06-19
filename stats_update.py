"""
Module that has StatsUpdate class which performs updates in Stats2
"""
import logging
import time
import argparse
import json
import traceback
import os
import subprocess
from couchdb_database import Database
from utils import make_cmsweb_request, make_cmsweb_prod_request, pick_attributes, setup_console_logging


class StatsUpdate():
    """
    Update workflows in Stats2 database.
    """

    def __init__(self):
        self.logger = logging.getLogger('logger')
        self.database = Database()
        # Cache for DBS filesummaries calls
        self.dataset_filesummaries_cache = {}
        # Cache for DBS dataset info + filiesummaries calls
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

        self.logger.info('Workflows after update %d', self.database.get_workflow_count())

    def perform_update_one(self, workflow_name, trigger_prod=False, trigger_dev=False):
        """
        Perform update for specific workflow: fetch new dictionary from RequestManager
        and update event recalculation
        """
        self.logger.info('Will update only one workflow: %s', workflow_name)
        self.update_one(workflow_name, trigger_prod, trigger_dev)
        self.recalculate_one(workflow_name)

    def perform_update_new(self, trigger_prod=False, trigger_dev=False):
        """
        Perform update for all workflows that changed since last update and recalculate
        events for files that changed since last update
        """
        update_start = time.time()
        changed_workflows, deleted_workflows, last_seq = self.get_list_of_changed_workflows()
        self.logger.info('Will delete %d workflows', len(deleted_workflows))
        for workflow_name in deleted_workflows:
            try:
                self.delete_one(workflow_name)
            except Exception as ex:
                self.logger.error('Exception while deleting %s:%s', workflow_name, str(ex))

        previously_crashed_workflows = self.get_list_of_previously_crashed_workflows()
        self.logger.info('Have %d workflows that crashed during last update',
                         len(previously_crashed_workflows))
        changed_workflows = set(changed_workflows).union(set(previously_crashed_workflows))
        self.logger.info('Will update %d workflows', len(changed_workflows))
        for index, workflow_name in enumerate(changed_workflows):
            try:
                self.logger.info('Will update %d/%d workflow', index + 1, len(changed_workflows))
                self.update_one(workflow_name, trigger_prod, trigger_dev)
                self.remove_from_list_of_crashed_workflows(workflow_name)
            except Exception as ex:
                self.add_to_list_of_crashed_workflows(workflow_name)
                self.logger.error('Exception while updating %s:%s\nTraceback:%s',
                                  workflow_name,
                                  str(ex),
                                  traceback.format_exc())

        update_end = time.time()
        self.logger.info('Finished updating workflows')
        self.logger.info('Will update event count')
        related_workflows = self.get_workflows_with_same_output(changed_workflows)
        self.logger.info('There are %s related workflows to %s changed workflows',
                         len(related_workflows),
                         len(changed_workflows))
        changed_datasets = self.get_list_of_workflows_with_changed_datasets()
        workflows_to_recalculate = set(changed_workflows).union(set(changed_datasets))
        workflows_to_recalculate.update(related_workflows)
        self.logger.info('Will update event count for %d workflows', len(workflows_to_recalculate))
        for index, workflow_name in enumerate(workflows_to_recalculate):
            try:
                self.logger.info('Will update event count for %d/%d',
                                 index + 1,
                                 len(workflows_to_recalculate))
                self.recalculate_one(workflow_name, trigger_prod, trigger_dev)
                self.remove_from_list_of_crashed_workflows(workflow_name)
            except Exception as ex:
                self.add_to_list_of_crashed_workflows(workflow_name)
                self.logger.error('Exception while updating event count %s:%s\nTraceback:%s',
                                  workflow_name,
                                  str(ex),
                                  traceback.format_exc())

        recalculation_end = time.time()
        self.database.set_setting('last_reqmgr_sequence', int(last_seq))
        self.database.set_setting('last_dbs_update_date', int(update_start))
        self.logger.info('Updated and deleted %d/%d workflows in %.3fs',
                         len(changed_workflows), len(deleted_workflows),
                         (update_end - update_start))
        self.logger.info('Updated event count for %d workflows in %.3fs',
                         len(workflows_to_recalculate),
                         (recalculation_end - update_end))

    def update_one(self, workflow_name, trigger_prod=False, trigger_dev=False):
        """
        Action to update one workflow's dictionary from RequestManager. If no such
        workflow exist in database, new one will be created.
        """
        self.logger.info('Updating %s', workflow_name)
        update_start = time.time()
        wf_dict = self.get_new_dict_from_reqmgr2(workflow_name)
        wf_dict_old = self.database.get_workflow(workflow_name)
        if wf_dict_old is None:
            wf_dict_old = {'_id': workflow_name}
            self.logger.info('Inserting %s', workflow_name)
            self.database.update_workflow(wf_dict_old)
            wf_dict_old = self.database.get_workflow(workflow_name)

        wf_dict['_rev'] = wf_dict_old['_rev']
        wf_dict['EventNumberHistory'] = wf_dict_old.get('EventNumberHistory', [])
        wf_dict['OutputDatasets'] = self.sort_datasets(wf_dict['OutputDatasets'])
        old_wf_dict_string = json.dumps(wf_dict_old, sort_keys=True)
        new_wf_dict_string = json.dumps(wf_dict, sort_keys=True)
        update_end = time.time()
        if old_wf_dict_string != new_wf_dict_string:
            self.database.update_workflow(wf_dict)
            self.logger.info('Updated %s in %.3fs', workflow_name, (update_end - update_start))
            self.trigger_outside(wf_dict, trigger_prod, trigger_dev)
        else:
            self.logger.info('Did not update %s because it did not change. Time: %.3fs',
                             workflow_name,
                             (update_end - update_start))

    def delete_one(self, workflow_name):
        """
        Action to delete one workflow from database.
        """
        self.logger.info('Deleting %s', workflow_name)
        self.database.delete_workflow(workflow_name)
        self.logger.info('Deleted %s', workflow_name)

    def recalculate_one(self, workflow_name, trigger_prod=False, trigger_dev=False):
        """
        Action to update event count for workflow.
        """
        recalc_start = time.time()
        self.logger.info('Updating event count for %s', workflow_name)
        workflow = self.database.get_workflow(workflow_name)
        if workflow is None:
            self.logger.warning('Will not update %s event count because it\'s no longer in database',
                                workflow_name)
            return

        history_entry = self.get_new_history_entry(workflow)
        added_history_entry = self.add_history_entry_to_workflow(workflow, history_entry)
        recalc_end = time.time()
        if added_history_entry:
            self.database.update_workflow(workflow)
            self.logger.info('Updated event count for %s in %.3fs',
                             workflow_name,
                             (recalc_end - recalc_start))
            self.trigger_outside(workflow, trigger_prod, trigger_dev)
        else:
            self.logger.info('Did not update event count for %s because it did not change. Time: %.3fs',
                             workflow_name,
                             (recalc_end - recalc_start))

    def get_new_dict_from_reqmgr2(self, workflow_name):
        """
        Get workflow dictionary from RequestManager.
        """
        url = f'/couchdb/reqmgr_workload_cache/{workflow_name}'
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
        if 'Task1' in wf_dict and 'InputDataset' in wf_dict['Task1']:
            wf_dict['InputDataset'] = wf_dict['Task1']['InputDataset']
        elif 'Step1' in wf_dict and 'InputDataset' in wf_dict['Step1']:
            wf_dict['InputDataset'] = wf_dict['Step1']['InputDataset']

        if 'Task1' in wf_dict and 'ProcessingString' in wf_dict['Task1']:
            wf_dict['ProcessingString'] = wf_dict['Task1']['ProcessingString']
        elif 'Step1' in wf_dict and 'ProcessingString' in wf_dict['Step1']:
            wf_dict['ProcessingString'] = wf_dict['Step1']['ProcessingString']

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

    def __get_filesummaries_from_dbs(self, dataset_name, dataset_access_type=None):
        """
        Get file summary from DBS for given dataset
        """
        query_url = f'/dbs/prod/global/DBSReader/filesummaries?dataset={dataset_name}'
        if dataset_access_type in ('PRODUCTION', 'VALID'):
            query_url += '&validFileOnly=1'

        filesummaries = make_cmsweb_prod_request(query_url)
        if filesummaries:
            return filesummaries[0]

        return {}

    def get_workflows_with_same_output(self, workflow_names):
        """
        Get list of workflow names that have the same output datasets as given workflows
        """
        datasets = set()
        for workflow_name in workflow_names:
            workflow = self.database.get_workflow(workflow_name)
            datasets.update(workflow.get('OutputDatasets', []))

        same_output_workflows = set()
        for dataset in datasets:
            dataset_workflows = self.database.get_workflows_with_output_dataset(dataset, page_size=1000)
            same_output_workflows.update(dataset_workflows)

        return same_output_workflows

    def get_event_count_from_dbs(self, dataset_name, dataset_access_type=None):
        """
        Get event count for specified dataset from DBS.
        """
        if dataset_name not in self.dataset_filesummaries_cache:
            file_summary = self.__get_filesummaries_from_dbs(dataset_name, dataset_access_type)
            self.dataset_filesummaries_cache[dataset_name] = file_summary
        else:
            file_summary = self.dataset_filesummaries_cache[dataset_name]

        num_event = int(file_summary.get('num_event', 0))
        return num_event

    def get_dataset_size_from_dbs(self, dataset_name):
        """
        Get size for specified dataset from DBS.
        """
        if dataset_name not in self.dataset_filesummaries_cache:
            file_summary = self.__get_filesummaries_from_dbs(dataset_name)
            self.dataset_filesummaries_cache[dataset_name] = file_summary
        else:
            file_summary = self.dataset_filesummaries_cache[dataset_name]

        file_size = int(file_summary.get('file_size', 0))
        return file_size

    def get_new_history_entry(self, wf_dict):
        """
        Form a new history entry dictionary for given workflow.
        """
        output_datasets = wf_dict.get('OutputDatasets')
        if not output_datasets:
            return None

        output_datasets_set = set(output_datasets)
        history_entry = {'Time': int(time.time()), 'Datasets': {}}
        dataset_list_url = '/dbs/prod/global/DBSReader/datasetlist'
        output_datasets_to_query = []
        for output_dataset in set(output_datasets):
            if output_dataset in self.dataset_info_cache:
                # Trying to find type, events and size in cache
                cache_entry = self.dataset_info_cache[output_dataset]
                self.logger.info('Found %s dataset info in cache. Type: %s, events: %s, size: %s',
                                 output_dataset,
                                 cache_entry['Type'],
                                 cache_entry['Events'],
                                 cache_entry['Size'])
                history_entry['Datasets'][output_dataset] = cache_entry
                output_datasets_set.remove(output_dataset)
            else:
                # Add dataset to list of datasets that are not in cache
                output_datasets_to_query.append(output_dataset)

        if output_datasets_to_query:
            # Get datasets that were not in cache
            dbs_dataset_list = make_cmsweb_prod_request(dataset_list_url,
                                                        {'dataset': output_datasets_to_query,
                                                         'detail': 1})
        else:
            self.logger.info('Not doing a request to %s because all datasets were in cache',
                             dataset_list_url)
            dbs_dataset_list = []

        for dbs_dataset in dbs_dataset_list:
            # Get events and size for newly queried datasets and add them to cache
            dataset_name = dbs_dataset['dataset']
            dataset_access_type = dbs_dataset['dataset_access_type']
            dataset_events = self.get_event_count_from_dbs(dataset_name, dataset_access_type)
            dataset_size = self.get_dataset_size_from_dbs(dataset_name)
            history_entry['Datasets'][dataset_name] = {'Type': dataset_access_type,
                                                       'Events': dataset_events,
                                                       'Size': dataset_size}
            # Put a copy to cache
            self.dataset_info_cache[dataset_name] = dict(history_entry['Datasets'][dataset_name])
            self.logger.info('Setting %s events, %s size and %s type for %s (%s)',
                             dataset_events,
                             dataset_size,
                             dataset_access_type,
                             dataset_name,
                             wf_dict.get('_id'))
            output_datasets_set.remove(dataset_name)

        for dataset_name in output_datasets_set:
            # Datasets that were not in the cache and not in response of query, make them NONE type with 0 events and 0 size
            dataset_access_type = 'NONE'
            dataset_events = 0
            dataset_size = 0
            # Setting defaults
            history_entry['Datasets'][dataset_name] = {'Type': dataset_access_type,
                                                       'Events': dataset_events,
                                                       'Size': dataset_size}
            # Put a copy to cache
            self.dataset_info_cache[dataset_name] = dict(history_entry['Datasets'][dataset_name])
            self.logger.info('Setting %s events, %s size and %s type for %s (%s)',
                             dataset_events,
                             dataset_size,
                             dataset_access_type,
                             dataset_name,
                             wf_dict.get('_id'))

        if len(history_entry['Datasets']) != len(set(output_datasets)):
            self.logger.error('Wrong number of datasets for %s. '
                              'New history item - %s, '
                              'output datasets - %s, '
                              'returning None',
                              wf_dict['_id'],
                              len(history_entry['Datasets']),
                              len(output_datasets))
            return None

        return history_entry

    def add_history_entry_to_workflow(self, wf_dict, new_history_entry):
        """
        Add history entry to workflow if such entry does not exist.
        """
        if new_history_entry is None:
            return False

        if not new_history_entry.get('Datasets', []):
            # No datasets, no point in adding this entry
            return False

        new_dict_string = json.dumps(new_history_entry['Datasets'], sort_keys=True)
        history_entries = sorted(wf_dict['EventNumberHistory'],
                                 key=lambda entry: entry.get('Time', 0))
        if history_entries:
            last_dict_string = json.dumps(history_entries[-1]['Datasets'], sort_keys=True)
            if new_dict_string == last_dict_string:
                return False

        history_entries.append(new_history_entry)
        wf_dict['EventNumberHistory'] = history_entries
        # self.logger.info(json.dumps(history_entry, indent=2))
        return True

    def get_expected_events_with_dict(self, wf_dict):
        """
        Get number of expected events of a workflow.
        """
        if 'FilterEfficiency' in wf_dict:
            filter_eff = float(wf_dict['FilterEfficiency'])
        elif 'Task1' in wf_dict and 'FilterEfficiency' in wf_dict['Task1']:
            filter_eff = float(wf_dict['Task1']['FilterEfficiency'])
        elif 'Step1' in wf_dict and 'FilterEfficiency' in wf_dict['Step1']:
            filter_eff = float(wf_dict['Step1']['FilterEfficiency'])
        else:
            filter_eff = 1.

        wf_type = wf_dict.get('RequestType', '').lower()
        if wf_type != 'resubmission':
            if wf_dict.get('TotalInputFiles', 0) > 0:
                if 'TotalInputEvents' in wf_dict:
                    return int(filter_eff * wf_dict['TotalInputEvents'])

            if 'RequestNumEvents' in wf_dict and wf_dict['RequestNumEvents'] is not None:
                return int(wf_dict['RequestNumEvents'])

            if 'Task1' in wf_dict and 'RequestNumEvents' in wf_dict['Task1']:
                return int(wf_dict['Task1']['RequestNumEvents'])

            if 'Step1' in wf_dict and 'RequestNumEvents' in wf_dict['Step1']:
                return int(wf_dict['Step1']['RequestNumEvents'])

            if 'Task1' in wf_dict and 'InputDataset' in wf_dict['Task1']:
                return self.get_event_count_from_dbs(wf_dict['Task1']['InputDataset'])

            if 'Step1' in wf_dict and 'InputDataset' in wf_dict['Step1']:
                return self.get_event_count_from_dbs(wf_dict['Step1']['InputDataset'])

        else:
            prep_id = wf_dict['PrepID']
            url = f'/reqmgr2/data/request?mask=TotalInputEvents&mask=RequestType&prep_id={prep_id}'
            ret = make_cmsweb_request(url)
            ret = ret['result']
            if ret:
                ret = ret[0]
                for request_name in ret:
                    if ret[request_name]['RequestType'].lower() != 'resubmission' and ret[request_name]['TotalInputEvents'] is not None:
                        return int(filter_eff * ret[request_name]['TotalInputEvents'])

        self.logger.error('%s does not have total events!', wf_dict['_id'])
        return -1

    def get_campaigns_from_workflow(self, wf_dict):
        """
        Get list of campaigns or acquisition eras in tasks. If there are no tasks, workflow's
        campaign or acquisition era will be used
        """
        task_number = 1
        # Preven infinite loop
        max_tasks = 999
        campaigns = []
        # Check whether it's a TaskChain or a StepChain
        if 'StepChain' in wf_dict:
            task_format = 'Step%s'
        else:
            task_format = 'Task%s'

        while max_tasks > 0:
            max_tasks -= 1
            task_name = task_format % task_number
            if task_name not in wf_dict:
                break

            if wf_dict[task_name].get('Campaign'):
                campaigns.append(wf_dict[task_name]['Campaign'])
            elif wf_dict[task_name].get('AcquisitionEra'):
                campaigns.append(wf_dict[task_name]['AcquisitionEra'])

            task_number += 1

        if not campaigns:
            if wf_dict.get('Campaign'):
                campaigns.append(wf_dict['Campaign'])
            elif wf_dict.get('AcquisitionEra'):
                campaigns.append(wf_dict['AcquisitionEra'])

        return campaigns

    def get_requests_from_workflow(self, wf_dict):
        """
        Get list of request prepids
        """
        task_number = 1
        # Preven infinite loop
        max_tasks = 999
        requests = []
        # Check whether it's a TaskChain or a StepChain
        if 'StepChain' in wf_dict:
            task_format = 'Step%s'
        else:
            task_format = 'Task%s'

        while max_tasks > 0:
            max_tasks -= 1
            task_name = task_format % task_number
            if task_name not in wf_dict:
                break

            if wf_dict[task_name].get('PrepID'):
                requests.append(wf_dict[task_name]['PrepID'])

            task_number += 1

        return requests

    def sort_datasets(self, dataset_list):
        """
        Sort dataset list by specific priority list.
        """
        if len(dataset_list) <= 1:
            return dataset_list

        def tier_priority(dataset):
            dataset_tier = dataset.split('/')[-1:][0]
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

            for (priority, tier) in enumerate(tier_priority):
                if tier.upper() == dataset_tier:
                    return priority

            return -1

        dataset_list = sorted(dataset_list, key=tier_priority)
        return dataset_list

    def get_list_of_changed_workflows(self):
        """
        Get list of workflows that changed in RequestManager since last update.
        """
        last_seq = self.database.get_setting('last_reqmgr_sequence', 0)
        url = f'/couchdb/reqmgr_workload_cache/_changes?since={last_seq}'
        self.logger.info('Getting the list of all workflows since %d from %s', last_seq, url)
        response = make_cmsweb_request(url)
        last_seq = int(response['last_seq'])
        wf_list = response['results']
        changed_wf_list = list(filter(lambda x: not x.get('deleted', False), wf_list))
        changed_wf_list = [wf['id'] for wf in changed_wf_list]
        changed_wf_list = list(filter(lambda x: '_design' not in x, changed_wf_list))
        deleted_wf_list = list(filter(lambda x: x.get('deleted', False), wf_list))
        deleted_wf_list = [wf['id'] for wf in deleted_wf_list]
        deleted_wf_list = list(filter(lambda x: '_design' not in x, deleted_wf_list))
        self.logger.info('Got %d updated workflows. Got %d deleted workflows.',
                         len(changed_wf_list),
                         len(deleted_wf_list))
        return changed_wf_list, deleted_wf_list, last_seq

    def get_updated_dataset_list_from_dbs(self, since_timestamp=0):
        """
        Get list of datasets that changed since last update.
        """
        url = f'/dbs/prod/global/DBSReader/datasets?min_ldate={since_timestamp}&dataset_access_type=*'
        self.logger.info('Getting the list of modified datasets since %d from %s',
                         since_timestamp,
                         url)
        dataset_list = make_cmsweb_prod_request(url)
        if dataset_list is None:
            self.logger.error('Could not get list of modified datasets since %d from %s',
                              since_timestamp,
                              url)

        dataset_list = [dataset['dataset'] for dataset in dataset_list]
        self.logger.info('Got %d datasets', len(dataset_list))
        return dataset_list

    def get_list_of_workflows_with_changed_datasets(self):
        """
        Get list of workflows whose datasets changed since last update.
        """
        self.logger.info('Will get list of changed datasets')
        workflows = set()
        last_dataset_modification_date = max(0, self.database.get_setting('last_dbs_update_date', 0) - 300) # 300s margin
        updated_datasets = self.get_updated_dataset_list_from_dbs(since_timestamp=last_dataset_modification_date)
        self.logger.info('Will find if any of changed datasets belong to workflows in database')
        for dataset in updated_datasets:
            dataset_workflows = self.database.get_workflows_with_output_dataset(dataset, page_size=1000)
            self.logger.info('%d workflows contain %s', len(dataset_workflows), dataset)
            workflows.update(dataset_workflows)

        workflows_from_wmstats = self.get_active_workflows_from_wmstats()
        workflows.update(set(workflows_from_wmstats))

        self.logger.info('Found %d workflows for changed datasets', len(workflows))
        return workflows

    def get_active_workflows_from_wmstats(self):
        """
        Get list of workflows which are currently putting data to DBS.
        """
        self.logger.info('Will get list of workflows which are currently putting data to DBS')
        url = '/wmstatsserver/data/filtered_requests?mask=RequestName'
        try:
            workflow_list = make_cmsweb_request(url, timeout=600, keep_open=False)
        except AttributeError as ae:
            self.logger.error(ae)
            workflow_list = None

        if workflow_list is None:
            self.logger.error('Could not get list of workflows from wmstats')
            return []

        workflow_list = workflow_list.get('result', [])
        workflow_list = [workflow['RequestName'] for workflow in workflow_list]

        self.logger.info('Found %d workflows which are currently putting data to DBS',
                         len(workflow_list))
        return workflow_list

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

    def trigger_outside(self, workflow, trigger_prod=False, trigger_dev=False):
        """
        Trigger something outside (McM) when workflow is updated
        """
        workflow_name = workflow['_id']
        workflow_type = workflow.get('RequestType')
        outside_urls = []
        self.logger.info('Trigger outside for %s (%s)', workflow_name, workflow_type)
        if trigger_prod:
            if workflow_type.lower() == 'rereco' or workflow.get('PrepID', '').startswith('ReReco-'):
                outside_urls.append({'url': 'https://cms-pdmv.cern.ch/rereco/api/requests/update_workflows',
                                     'cookie': 'prod_cookie.txt',
                                     'data': {'prepid': workflow.get('PrepID', '')},
                                     'method': 'POST'})
            elif 'RVCMSSW' in workflow_name:
                outside_urls.append({'url': 'https://cms-pdmv.cern.ch/relval/api/relvals/update_workflows',
                                     'cookie': 'prod_cookie.txt',
                                     'data': {'prepid': workflow.get('PrepID', '')},
                                     'method': 'POST'})
            else:
                outside_urls.append({'url': f'https://cms-pdmv.cern.ch/mcm/restapi/requests/fetch_stats_by_wf/{workflow_name}',
                                     'cookie': 'prod_cookie.txt'})

        if trigger_dev:
            if workflow_type.lower() == 'rereco' or workflow.get('PrepID', '').startswith('ReReco-'):
                outside_urls.append({'url': 'https://cms-pdmv-dev.cern.ch/rereco/api/requests/update_workflows',
                                     'cookie': 'dev_cookie.txt',
                                     'data': {'prepid': workflow.get('PrepID', '')},
                                     'method': 'POST'})
            elif 'RVCMSSW' in workflow_name:
                outside_urls.append({'url': 'https://cms-pdmv-dev.cern.ch/relval/api/relvals/update_workflows',
                                     'cookie': 'dev_cookie.txt',
                                     'data': {'prepid': workflow.get('PrepID', '')},
                                     'method': 'POST'})
            else:
                outside_urls.append({'url': f'https://cms-pdmv-dev.cern.ch/mcm/restapi/requests/fetch_stats_by_wf/{workflow_name}',
                                     'cookie': 'dev_cookie.txt'})

        for outside in outside_urls:
            try:
                self.logger.info('Triggering outside for %s', workflow_name)
                args = ['curl',
                        '-X',
                        outside.get('method', 'GET'),
                        outside['url'],
                        '-s',  # Silent
                        '-k',  # Ignore invalid https certificate
                        '-L',  # Follow 3xx codes
                        '-m 20',  # Timeout 20s
                        '-w %{http_code}',  # Return only HTTP code
                        '-o /dev/null']
                if outside.get('cookie'):
                    self.logger.info('Append cookie "%s" while making request for %s',
                                     outside['cookie'],
                                     workflow_name)
                    args += ['--cookie', outside['cookie']]

                if outside.get('data'):
                    self.logger.info('Adding data "%s" while making request for %s',
                                     outside['data'],
                                     workflow_name)
                    args += ['-d', '\'%s\'' % (json.dumps(outside['data']))]
                    args += ['-H', '"Content-Type: application/json"']

                args = ' '.join(args)
                proc = subprocess.Popen(args, stdout=subprocess.PIPE, shell=True)
                code = proc.communicate()[0]
                code = int(code)
                self.logger.info('HTTP code %s for %s', code, workflow_name)
            except Exception as ex:
                self.logger.error('Exception while trigerring %s for %s. Exception: %s',
                                  outside['url'],
                                  workflow_name,
                                  str(ex))


def main():
    """
    Main function that parses arguments and starts the update
    """
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
    logger.info('Arguments %s', str(args))

    action = args.get('action', None)
    name = args.get('name', None)
    trigger_prod = args.get('trigger_prod', False)
    trigger_dev = args.get('trigger_dev', False)

    if action == 'update':
        if not os.environ.get('STATS_DB_AUTH_HEADER'):
            logger.error('STATS_DB_AUTH_HEADER is missing')
            return

        stats_update = StatsUpdate()
        stats_update.perform_update(name, trigger_prod, trigger_dev)
    elif action == 'see':
        workflow = Database().get_workflow(name)
        print(json.dumps(workflow, indent=4))


if __name__ == '__main__':
    main()
