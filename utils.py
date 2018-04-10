from ssl import SSLContext
import urllib.request
import datetime
import json
import logging


def get_request_list_from_req_mgr(last_days=None):
    logger = logging.getLogger('logger')
    url = 'https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/'
    if last_days is not None:
        today = datetime.datetime.now()
        past = today - datetime.timedelta(days=last_days)

        url += 'bydate?startkey=[%d,%d,%d,0,0,0]&endkey=[%d,%d,%d,23,59,59]' % (past.year,
                                                                                past.month,
                                                                                past.day,
                                                                                today.year,
                                                                                today.month,
                                                                                today.day)
    else:
        url += 'bystatusandtype'

    logger.info('Getting the list of all requests from %s' % (url))
    response = make_request_with_grid_cert(url)
    req_list = json.loads(response)['rows']
    logger.info('Got %d requests' % (len(req_list)))
    if last_days is not None:
        req_list = [req['value']['RequestName'] for req in req_list]
    else:
        req_list = [req['key'][0] for req in req_list]

    return req_list


def make_request_with_grid_cert(url, post_data=None):
    # openssl pkcs12 -in path.p12 -out newfile.crt.pem -clcerts -nokeys
    # openssl pkcs12 -in path.p12 -out newfile.key.pem -nocerts -nodes
    ssl_context = SSLContext()
    ssl_context.load_cert_chain(certfile='/afs/cern.ch/user/j/jrumsevi/private/user.crt.pem',
                                keyfile='/afs/cern.ch/user/j/jrumsevi/private/user.key.pem')

    if post_data is None:
        datareq = urllib.request.Request(url)
    else:
        encoded_data = urllib.parse.urlencode(post_data).encode("utf-8")
        datareq = urllib.request.Request(url, encoded_data)
        print('POST!')

    datareq.add_header('authenticated_wget', "The ultimate wgetter")
    datareq.add_header('Accept', 'application/json')
    response = urllib.request.urlopen(datareq, context=ssl_context).read().decode('utf-8')
    return response


def pick_attributes(old_dict, attributes, skip_non_existing=True):
    new_dict = {}
    for attribute in attributes:
        if attribute in old_dict:
            new_dict[attribute] = old_dict[attribute]
        elif not skip_non_existing:
            new_dict[attribute] = None

    return new_dict
