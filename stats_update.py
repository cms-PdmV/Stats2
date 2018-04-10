import json
import logging
from database import Database
from utils import make_request_with_grid_cert, pick_attributes, get_request_list_from_req_mgr


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
        self.update_one(request_name, True)

    def perform_update_days(self, days):
        requests = get_request_list_from_req_mgr(days)
        self.logger.info('Will process %d requests' % (len(requests)))
        for request_name in requests:
            self.update_one(request_name)

    def perform_update_all(self):
        requests = self.database.get_all_requests()
        requests = [req['_id'] for req in requests]
        self.logger.info('Will process %d requests' % (len(requests)))
        for request_name in requests:
            self.update_one(request_name)

    def update_one(self, request_name, force=False):
        self.logger.info('Updating %s' % (request_name))
        req_dict = self.get_dict_from_reqmgr2(request_name)
        if len(req_dict['result']) != 1:
            self.logger.warning('Length for %s from reqmgr2 is not 1!' % (request_name))

        req_dict = req_dict['result'][0][request_name]
        expected_events = self.get_expected_events_withdict(req_dict)  # self.get_expected_events(req_dict)
        req_dict = pick_attributes(req_dict, ['Campaign',
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
        req_dict['RequestTransition'] = [{'Status': transition['Status'],
                                          'UpdateTime': transition['UpdateTime']} for transition in req_dict['RequestTransition']]
        req_dict['_id'] = request_name
        req_dict['Events'] = []
        req_dict['TotalEvents'] = expected_events
        req_dict['OutputDatasets'] = self.sort_datasets(req_dict['OutputDatasets'])
        last_dataset_open = 0
        last_dataset_done = 0
        if len(req_dict['OutputDatasets']) > 0:
            output_dataset = req_dict['OutputDatasets'][-1:][0]
            req_dict['OutputDataset'] = output_dataset
            events = self.get_event_count_from_dbs(output_dataset)
            open_events = events
            done_events = 0
            for transition in req_dict['RequestTransition']:
                if transition['Status'] == 'announced':
                    open_events = 0
                    done_events = events
                    break

            last_dataset_open = open_events
            last_dataset_done = done_events
            req_dict['Events'].append({'Dataset': output_dataset,
                                       'OpenEvents': open_events,
                                       'DoneEvents': done_events})
        req_dict['LastDatasetOpen'] = last_dataset_open
        req_dict['LastDatasetDone'] = last_dataset_done
        self.database.update_request(req_dict)
        self.logger.info('Updated %s' % (request_name))

    def get_dict_from_reqmgr2(self, req_name):
        url = 'https://cmsweb.cern.ch/reqmgr2/data/request?name=%s' % (req_name)
        # self.logger.info('Will get dict from %s' % (url))
        response = make_request_with_grid_cert(url)
        req_dict = json.loads(response)
        return req_dict

    def get_event_count_from_dbs(self, dataset_name):
        dbs3_url = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/'
        import json
        filesummaries = json.loads(make_request_with_grid_cert(dbs3_url + 'filesummaries?dataset=%s' % (dataset_name)))
        # print(json.dumps(filesummaries, indent=4))
        # blocksummaries = json.loads(make_request_with_grid_cert(dbs3_url + 'blocksummaries?dataset=%s' % (dataset_name)))
        # print(json.dumps(blocksummaries, indent=4))
        # blocks = json.loads(make_request_with_grid_cert(dbs3_url + 'blocks?dataset=%s' % (dataset_name)))
        # print(json.dumps(blocks, indent=4))
        # for block_name in blocks:
        #     name = block_name['block_name'].replace("#", "%23")
        #     print(name)
        #     block_summary = json.loads(make_request_with_grid_cert(dbs3_url + 'blocksummaries?block_name=%s' % (name)))
        #     print(json.dumps(block_summary, indent=4))

        # detaildatasets = json.loads(make_request_with_grid_cert(dbs3_url + 'datasets', {"dataset": [dataset_name],
        #                                                                                 "detail": True}))
        # print(json.dumps(detaildatasets, indent=4))
        if len(filesummaries) > 1:
            self.logger.warning('More than 1 dataset summary? %s' % (dataset_name))

        if len(filesummaries) == 0:
            # self.logger.warning('No summaries for %s' % (dataset_name))
            return 0

        return int(filesummaries[0]['num_event'])

    def get_expected_events_withdict(self, req_dict):
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
            if 'RequestNumEvents' in req_dict:
                # self.logger.info('RequestNumEvents %d' % (req_dict['RequestNumEvents']))
                return req_dict['RequestNumEvents']
            elif 'Task1' in req_dict and 'RequestNumEvents' in req_dict['Task1']:
                # self.logger.info('Task1.RequestNumEvents %d' % (req_dict['Task1']['RequestNumEvents']))
                return req_dict['Task1']['RequestNumEvents']
            elif 'TotalInputEvents' in req_dict:
                # self.logger.info('TotalInputEvents %d' % (req_dict['TotalInputEvents']))
                return int(f * req_dict['TotalInputEvents'])
        else:
            # self.logger.info('Resubmission %s, will get TotalInputEvents' % (req_dict['_id']))
            url = 'https://cmsweb.cern.ch/reqmgr2/data/request?mask=TotalInputEvents&mask=RequestType&prep_id=%s' % (req_dict['PrepID'])
            ret = make_request_with_grid_cert(url)
            ret = json.loads(ret)
            ret = ret['result'][0]
            for r in ret:
                if ret[r]['RequestType'].lower() != 'resubmission' and ret[r]['TotalInputEvents'] is not None:
                    return int(f * ret[r]['TotalInputEvents'])

        self.logger.error('Should not reach this yet....')
        return -2

        # if 'RequestNumEvents' in req_dict:
        #     rne = req_dict['RequestNumEvents']
        # elif 'RequestSizeEvents' in req_dict:
        #     rne = req_dict['request']['schema']['RequestSizeEvents']
        # elif 'Task1' in req_dict and 'RequestNumEvents' in req_dict['Task1']:
        #     rne = req_dict['Task1']['RequestNumEvents']
        # else:
        #     rne = None

        # if rne is not None:
        #     return rne

        # dbs3_url = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/'
        # if 'FilterEfficiency' in req_dict:
        #     f = float(req_dict['FilterEfficiency'])
        # elif 'Task1' in req_dict and 'FilterEfficiency' in req_dict['Task1']:
        #     f = float(req_dict['Task1']['FilterEfficiency'])
        # else:
        #     f = 1.

        # if 'InputDataset' in req_dict:
        #     ids = [req_dict['InputDataset']]
        # elif 'Task1' in req_dict and 'InputDataset' in req_dict['Task1']:
        #     ids = [req_dict['Task1']['InputDataset']]
        # else:
        #     ids = []

        # if 'BlockWhitelist' in req_dict:
        #     bwl = req_dict['BlockWhitelist']
        # elif 'Task1' in req_dict and 'BlockWhitelist' in req_dict['Task1']:
        #     bwl = req_dict['Task1']['BlockWhitelist']
        # else:
        #     bwl = []

        # if 'RunWhitelist' in req_dict:
        #     rwl = req_dict['RunWhitelist']
        # elif 'Task1' in req_dict and 'RunWhitelist' in req_dict['Task1']:
        #     rwl = req_dict['Task1']['RunWhitelist']
        # else:
        #     rwl = []

        # ids = set(ids)
        # bwl = set(bwl)
        # rwl = set(rwl)

        # s = 0.
        # for input_dataset in ids:
        #     if len(rwl) > 0:
        #         self.logger.info('Will count total events for %s from runs' % (input_dataset))
        #         for run in rwl:
        #             self.logger.info("Checking run: %s" % (run))
        #             ret = make_request_with_grid_cert(dbs3_url + "filesummaries?dataset=%s&run_num=%s" % (input_dataset, run))
        #             ret = json.loads(ret)
        #             try:
        #                 s += int(ret[0]["num_event"])
        #             except Exception:
        #                 self.logger.error('%s does not have event for %s' % (input_dataset, run))
        #     else:
        #         self.logger.info('Will count total events for %s from blocks' % (input_dataset))
        #         blocks = make_request_with_grid_cert(dbs3_url + "blocks?dataset=%s" % (input_dataset))
        #         blocks = json.loads(blocks)
        #         block_names = set([block['block_name'] for block in blocks])
        #         if len(bwl) > 0:
        #             self.logger.info('Will count total events for %s from block white list' % (input_dataset))
        #             bwl = list(filter(lambda x: '#' in x and x in block_names, bwl))
        #         else:
        #             self.logger.info('Will count total events for %s from all blocks' % (input_dataset))

        #         for block_name in block_names:
        #             block_name = block_name.replace("#", "%23")  # encode # to HTML URL
        #             block_data = make_request_with_grid_cert(dbs3_url + "blocksummaries?block_name=%s" % (block_name))
        #             block_data = json.loads(block_data)
        #             s += block_data[0]["num_event"]
        # return int(s * f)

    def sort_datasets(self, dataset_list):
        """
        takes output_datasets list and sorts it in prioritized way.
        returns: first ouput dataset, output_dataset list
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
        # import pprint
        # pprint.pprint(dataset_list)

        return dataset_list
