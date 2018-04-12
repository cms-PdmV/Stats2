import datetime
import json
import logging
from connection_wrapper import ConnectionWrapper


connection_wrappers = {}


def get_request_list_from_req_mgr(last_days=None):
    logger = logging.getLogger('logger')
    host_url = 'https://cmsweb.cern.ch'
    query_url = '/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/'
    if last_days is not None:
        today = datetime.datetime.now()
        past = today - datetime.timedelta(days=last_days)

        query_url += 'bydate?startkey=[%d,%d,%d,0,0,0]&endkey=[%d,%d,%d,23,59,59]' % (past.year,
                                                                                      past.month,
                                                                                      past.day,
                                                                                      today.year,
                                                                                      today.month,
                                                                                      today.day)
    else:
        query_url += 'bystatusandtype'

    logger.info('Getting the list of all requests from %s' % (host_url + query_url))
    response = make_request_with_grid_cert(host_url, query_url)
    req_list = response['rows']
    logger.info('Got %d requests' % (len(req_list)))
    if last_days is not None:
        req_list = [req['value']['RequestName'] for req in req_list]
    else:
        req_list = [req['key'][0] for req in req_list]

    return req_list


def make_request_with_grid_cert(host_url, query_url):
    connection_wrapper = connection_wrappers.get(host_url)
    if connection_wrapper is None:
        connection_wrapper = ConnectionWrapper(host_url)
        connection_wrappers[host_url] = connection_wrapper

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
