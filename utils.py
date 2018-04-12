from ssl import SSLContext
import urllib.request
import datetime
import json
import logging
import time
import http


# openssl pkcs12 -in path.p12 -out newfile.crt.pem -clcerts -nokeys
# openssl pkcs12 -in path.p12 -out newfile.key.pem -nocerts -nodes
ssl_context = SSLContext()
ssl_context.load_cert_chain(certfile='/afs/cern.ch/user/j/jrumsevi/private/user.crt.pem',
                            keyfile='/afs/cern.ch/user/j/jrumsevi/private/user.key.pem')


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
    # start = time.time()
    # if post_data is None:
    #     datareq = urllib.request.Request(url)
    # else:
    #     encoded_data = urllib.parse.urlencode(post_data).encode("utf-8")
    #     datareq = urllib.request.Request(url, encoded_data)

    # datareq.add_header('authenticated_wget', "The ultimate wgetter")
    # datareq.add_header('Accept', 'application/json')
    # response = urllib.request.urlopen(datareq, context=ssl_context).read().decode('utf-8')

    response = cw.api(url).decode('utf-8')

    # end = time.time()
    # logger = logging.getLogger('logger')

    # logger.info(response)
    # logger.info('Request took %.3fs. %s' % (end - start, url))
    return response


def pick_attributes(old_dict, attributes, skip_non_existing=True):
    new_dict = {}
    for attribute in attributes:
        if attribute in old_dict:
            new_dict[attribute] = old_dict[attribute]
        elif not skip_non_existing:
            new_dict[attribute] = None

    return new_dict















class ConnectionWrapper():
    """
    Wrapper class to re-use existing connection to DBS3Reader
    """
    def __init__(self):
        # TO-DO:
        # add a parameter to pass DBS3 url, in case we want to use different address
        self.connection = None
        self.connection_attempts = 3
        self.wmagenturl = 'cmsweb.cern.ch'

    def init_connection(self, url):
        return http.client.HTTPSConnection(url,
                                           port=443,
                                           cert_file='/afs/cern.ch/user/j/jrumsevi/private/user.crt.pem',
                                           key_file='/afs/cern.ch/user/j/jrumsevi/private/user.key.pem')

    def refresh_connection(self, url):
        self.connection = self.init_connection(url)

    def abort(self, reason=""):
        raise Exception("Something went wrong. Aborting. " + reason)

    def httpget(self, conn, query):
        query = query.replace('#', '%23').replace('https://' + self.wmagenturl, '')
        conn.request("GET", query, headers={"Accept": "application/json"})
        try:
            response = conn.getresponse()
        except http.client.BadStatusLine:
            raise RuntimeError('Something is really wrong')
        if response.status != 200:
            logger = logging.getLogger('logger')
            logger.info("Problems (%d) with %s: %s" % (response.status, query, response.read()))
            return None

        return response.read()

    def api(self, url):
        """Constructs query and returns DBS3 response
        """
        if not self.connection:
            self.refresh_connection(self.wmagenturl)

        # this way saves time for creating connection per every request
        for i in range(self.connection_attempts):
            try:
                res = self.httpget(self.connection, url)
                break
            except Exception:
                # most likely connection terminated
                self.refresh_connection(self.wmagenturl)
        try:
            return res
        except:
            self.abort("Could not load the answer %s" % (url))


cw = ConnectionWrapper()
