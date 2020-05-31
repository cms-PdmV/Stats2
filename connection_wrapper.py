"""
Connection Wrapper module
"""
import http.client
import logging
import os
import json


class ConnectionWrapper():
    """
    Wrapper class to re-use existing connection
    """
    def __init__(self, host, timeout, keep_open):
        self.connection = None
        self.connection_attempts = 3
        self.host_url = host.replace('https://', '').replace('http://', '')
        self.cert_file = os.getenv('USERCRT', None)
        self.key_file = os.getenv('USERKEY', None)
        self.logger = logging.getLogger('logger')
        self.timeout = timeout
        self.keep_open = keep_open

    def init_connection(self, url):
        """
        Create a new connection
        """
        if self.cert_file is None or self.key_file is None:
            self.cert_file = os.getenv('USERCRT', None)
            self.key_file = os.getenv('USERKEY', None)

        if self.cert_file is None or self.key_file is None:
            raise Exception('Missing USERCRT or USERKEY environment variables')

        return http.client.HTTPSConnection(url,
                                           port=443,
                                           cert_file=self.cert_file,
                                           key_file=self.key_file,
                                           timeout=self.timeout)

    def refresh_connection(self, url):
        """
        Recreate a connection
        """
        self.logger.info('Refreshing connection')
        self.connection = self.init_connection(url)

    def api(self, method, url, data):
        """
        Make a HTTP request with given method, url and data
        """
        if not self.connection:
            self.refresh_connection(self.host_url)

        url = url.replace('#', '%23')
        # this way saves time for creating connection per every request
        for _ in range(self.connection_attempts):
            try:
                data = json.dumps(data) if data else None
                self.connection.request(method, url, data, headers={'Accept': 'application/json'})
                response = self.connection.getresponse()
                if response.status != 200:
                    logger = logging.getLogger('logger')
                    logger.info('Problems (%d) with [%s] %s: %s',
                                response.status,
                                method,
                                url,
                                response.read())
                    return None

                response_to_return = response.read()
                if not self.keep_open:
                    logger = logging.getLogger('logger')
                    logger.info('Closing connection for %s. Timeout %s',
                                self.host_url,
                                self.timeout)
                    self.connection.close()
                    self.connection = None

                return response_to_return
            # except http.client.BadStatusLine:
            #     raise RuntimeError('Something is really wrong')
            except Exception as ex:
                self.logger.error('Exception while doing [%s] to %s: %s', method, url, str(ex))
                # most likely connection terminated
                self.refresh_connection(self.host_url)

        self.logger.error('Connection wrapper failed after %d attempts', self.connection_attempts)
        return None
