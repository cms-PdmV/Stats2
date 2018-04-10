from utils import get_request_list_from_req_mgr
import logging
from database import Database


class StatsInsert():
    """
    Insert new events that are not present in current database.
    """
    def __init__(self):
        self.logger = logging.getLogger('logger')
        self.database = Database()

    def perform_insert(self, days=None):
        """
        Insert new events that were created in last 'days' days
        """
        self.logger.info('Before insert: %d requests' % (self.database.get_request_count()))
        requests = get_request_list_from_req_mgr(days)
        self.logger.info('Got %d requests from ReqMgr' % (len(requests)))
        for request_name in requests:
            self.insert_one(request_name)

        self.logger.info('After insert: %d requests' % (self.database.get_request_count()))

    def insert_one(self, request_name):
        """
        Create a dict with only _id parameter and insert into database
        """
        self.logger.info('Creating empty %s' % (request_name))
        request_dict = {'_id': request_name}
        self.database.insert_request_if_does_not_exist(request_dict)
