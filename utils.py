"""
Module that holds helper functions
"""
import json
import logging
import time
from connection_wrapper import ConnectionWrapper


__CONNECTION_WRAPPERS = {}


def make_cmsweb_request(query_url, data=None, timeout=90, keep_open=True):
    """
    Make a request to https://cmsweb.cern.ch
    """
    return make_request('https://cmsweb.cern.ch', query_url, data, timeout, keep_open)


def make_cmsweb_prod_request(query_url, data=None, timeout=90, keep_open=True):
    """
    Make a request to https://cmsweb-prod.cern.ch
    """
    return make_request('https://cmsweb-prod.cern.ch', query_url, data, timeout, keep_open)


def make_request(host, query_url, data=None, timeout=90, keep_open=True):
    """
    Make a HTTP request. Use connection wrapper to keep connection alive
    and add necessary grid certificates for authentication
    """
    connection_wrapper_key = f'{host}___{timeout}___{keep_open}'
    connection_wrapper = __CONNECTION_WRAPPERS.get(connection_wrapper_key)
    if connection_wrapper is None:
        connection_wrapper = ConnectionWrapper(host, timeout, keep_open)
        __CONNECTION_WRAPPERS[connection_wrapper_key] = connection_wrapper

    method = 'GET' if data is None else 'POST'
    logger = logging.getLogger('logger')
    request_start_time = time.time()
    response = connection_wrapper.api(method, query_url, data)
    request_finish_time = time.time()
    time_taken = request_finish_time - request_start_time
    if not data:
        logger.info('%s request to %s%s took %.3fs', method, host, query_url, time_taken)
    else:
        logger.info('%s request to %s%s with data \n%s\n took %.3fs',
                    method,
                    host,
                    query_url,
                    json.dumps(data, indent=2, sort_keys=True),
                    time_taken)

    return json.loads(response.decode('utf-8'))


def pick_attributes(old_dict, attributes, skip_non_existing=True):
    """
    Pick requested key value pairs from a dictionary and return a new dictionary
    """
    new_dict = {}
    for attribute in attributes:
        if attribute in old_dict:
            new_dict[attribute] = old_dict[attribute]
        elif not skip_non_existing:
            new_dict[attribute] = None

    return new_dict


def setup_console_logging():
    """
    Set default logging format and level
    """
    log_format = '[%(asctime)s][%(levelname)s] %(message)s'
    logging.basicConfig(format=log_format, level=logging.INFO)


def get_unique_list(input_list):
    """
    Return a new list of unique elemets only while preserving original order
    """
    new_list = []
    for element in input_list:
        if element not in new_list:
            new_list.append(element)

    return new_list


def get_nice_size(size, base=1000.0):
    """
    Convert number of bytes to string with KB, MB, GB, TB suffixes
    """
    if size < base:
        return '%sB' % (size)

    if size < base**2:
        return '%.2fKB' % (size / base)

    if size < base**3:
        return '%.2fMB' % (size / base**2)

    if size < base**4:
        return '%.2fGB' % (size / base**3)

    return '%.2fTB' % (size / base**4)

def comma_separate_thousands(number):
    return '{:,}'.format(int(number))
