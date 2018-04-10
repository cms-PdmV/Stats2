import argparse
import logging
from logging import handlers
from stats_insert import StatsInsert
from stats_update import StatsUpdate
from database import Database
import json


LOG_FORMAT = '[%(asctime)s][%(filename)s:%(lineno)d][%(levelname)s] %(message)s'


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
    formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt='%d/%b/%Y:%H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def setup_console_logging():
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)


def main():
    setup_console_logging()
    logger = logging.getLogger('logger')
    parser = argparse.ArgumentParser(description='Stats2 insert and update')
    parser.add_argument('--action',
                        choices=['insert', 'update', 'see', 'drop'],
                        required=True,
                        help='Action to be performed.')
    parser.add_argument('--name',
                        required=False,
                        help='Request to be updated.')
    parser.add_argument('--days',
                        type=int,
                        help='Number of days to fetch')
    args = vars(parser.parse_args())
    logger.info('Arguments %s' % (str(args)))

    action = args.get('action', None)
    name = args.get('name', None)
    days = args.get('days', None)

    if action == 'insert':
        stats_insert = StatsInsert()
        stats_insert.perform_insert(days)
    elif action == 'update':
        stats_update = StatsUpdate()
        stats_update.perform_update(name, days)
    elif action == 'see':
        request = Database().get_request(name)
        print(json.dumps(request, indent=4))
    elif action == 'drop':
        Database().clear_database()


if __name__ == '__main__':
    main()
