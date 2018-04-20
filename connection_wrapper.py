import http.client
import logging
import os


class ConnectionWrapper():
    """
    Wrapper class to re-use existing connection
    """
    def __init__(self, host):
        self.connection = None
        self.connection_attempts = 3
        self.wmagenturl = host.replace('https://', '').replace('http://', '')
        self.cert_file = os.getenv('USERCRT', None)
        self.key_file = os.getenv('USERKEY', None)

    def init_connection(self, url):
        if self.cert_file is None or self.key_file is None:
            raise Exception('Missing USERCRT or USERKEY environment variables')
            return None

        return http.client.HTTPSConnection(url,
                                           port=443,
                                           cert_file=self.cert_file,
                                           key_file=self.key_file)

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
