import json
import logging
from connection_wrapper import ConnectionWrapper
from logging import handlers
import time


__connection_wrappers = {}
__LOG_FORMAT = '[%(asctime)s][%(filename)s:%(lineno)d][%(levelname)s] %(message)s'


def get_request_list_from_req_mgr(since=0):
    logger = logging.getLogger('logger')
    host_url = 'https://cmsweb.cern.ch'
    query_url = '/couchdb/reqmgr_workload_cache/_changes?since=%d' % (since)

    logger.info('Getting the list of all requests since %d from %s' % (since, host_url + query_url))
    response = make_request_with_grid_cert(host_url, query_url)
    last_seq = int(response['last_seq'])
    req_list = response['results']
    logger.info('Got %d requests' % (len(req_list)))
    req_list = [req['id'] for req in req_list]
    req_list = list(filter(lambda x: '_design' not in x, req_list))

    return req_list, last_seq


def get_updated_dataset_list_from_dbs(since_timestamp=0):
    logger = logging.getLogger('logger')
    host_url = 'https://cmsweb.cern.ch'
    query_url = '/dbs/prod/global/DBSReader/datasets?min_ldate=%d' % (since_timestamp)

    logger.info('Getting the list of modified datasets since %d from %s' % (since_timestamp, host_url + query_url))
    dataset_list = make_request_with_grid_cert(host_url, query_url)
    logger.info('Got %d datasets' % (len(dataset_list)))
    dataset_list = [dataset['dataset'] for dataset in dataset_list]

    return dataset_list


def make_request_with_grid_cert(host_url, query_url):
    connection_wrapper = __connection_wrappers.get(host_url)
    if connection_wrapper is None:
        connection_wrapper = ConnectionWrapper(host_url)
        __connection_wrappers[host_url] = connection_wrapper

    # start = time.time()
    response = connection_wrapper.api(query_url).decode('utf-8')
    # end = time.time()
    # print('%s took %fs' % (query_url, end - start))
    return json.loads(response)


def pick_attributes(old_dict, attributes, skip_non_existing=True):
    new_dict = {}
    for attribute in attributes:
        if attribute in old_dict:
            new_dict[attribute] = old_dict[attribute]
        elif not skip_non_existing:
            new_dict[attribute] = None

    return new_dict


def setup_file_logging():
    # Max log file size - 5Mb
    max_log_file_size = 1024 * 1024 * 5
    max_log_file_count = 5
    log_file_name = 'stats_update_logs.log'
    logger = logging.getLogger('logger')
    logger.setLevel(logging.INFO)
    handler = handlers.RotatingFileHandler(log_file_name,
                                           'a',
                                           max_log_file_size,
                                           max_log_file_count)
    formatter = logging.Formatter(fmt=__LOG_FORMAT, datefmt='%d/%b/%Y:%H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def setup_console_logging():
    logging.basicConfig(format=__LOG_FORMAT, level=logging.INFO)
