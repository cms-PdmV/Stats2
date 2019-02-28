import json
import logging
from urllib.request import Request, urlopen
from connection_wrapper import ConnectionWrapper
from logging import handlers


__connection_wrappers = {}
# __LOG_FORMAT = '[%(asctime)s][%(filename)s:%(lineno)d][%(levelname)s] %(message)s'
__LOG_FORMAT = '[%(asctime)s][%(levelname)s] %(message)s'


def make_cmsweb_request(query_url, data=None):
    host_url = 'https://cmsweb.cern.ch'
    connection_wrapper = __connection_wrappers.get(host_url)
    if connection_wrapper is None:
        connection_wrapper = ConnectionWrapper(host_url)
        __connection_wrappers[host_url] = connection_wrapper

    response = connection_wrapper.api('GET' if data is None else 'POST', query_url, data)
    return json.loads(response.decode('utf-8'))


def make_simple_request(url):
    req = Request(url)
    return json.loads(urlopen(req).read().decode('utf-8'))


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
    log_file_name = 'logs.log'
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
