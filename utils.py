import json
import logging
from connection_wrapper import ConnectionWrapper
from logging import handlers


__connection_wrappers = {}
# __LOG_FORMAT = '[%(asctime)s][%(filename)s:%(lineno)d][%(levelname)s] %(message)s'
__LOG_FORMAT = '[%(asctime)s][%(levelname)s] %(message)s'


def make_request_with_grid_cert(query_url):
    host_url = 'https://cmsweb.cern.ch'
    connection_wrapper = __connection_wrappers.get(host_url)
    if connection_wrapper is None:
        connection_wrapper = ConnectionWrapper(host_url)
        __connection_wrappers[host_url] = connection_wrapper

    response = connection_wrapper.api(query_url).decode('utf-8')
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
