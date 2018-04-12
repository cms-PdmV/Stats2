import http.client
import logging


class ConnectionWrapper():
    """
    Wrapper class to re-use existing connection
    """
    def __init__(self, host):
        self.connection = None
        self.connection_attempts = 3
        self.wmagenturl = host.replace('https://', '').replace('http://', '')

    def init_connection(self, url):
        return http.client.HTTPSConnection(url,
                                           port=443,
                                           cert_file='/afs/cern.ch/user/j/jrumsevi/private/user.crt.pem',
                                           key_file='/afs/cern.ch/user/j/jrumsevi/private/user.key.pem')

    def refresh_connection(self, url):
        logger = logging.getLogger('logger')
        logger.info('Refreshing connection')
        self.connection = self.init_connection(url)

    def abort(self, reason=""):
        raise Exception("Something went wrong. Aborting. " + reason)

    def httpget(self, conn, query):
        query = query.replace('#', '%23')
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
