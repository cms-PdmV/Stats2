import argparse
import logging
from stats_update import StatsUpdate
from database import Database
import json
from utils import setup_console_logging


def main():
    setup_console_logging()
    logger = logging.getLogger('logger')
    parser = argparse.ArgumentParser(description='Stats2 update')
    parser.add_argument('--action',
                        choices=['update', 'see', 'drop'],
                        required=True,
                        help='Action to be performed.')
    parser.add_argument('--name',
                        required=False,
                        help='Request to be updated.')
    args = vars(parser.parse_args())
    logger.info('Arguments %s' % (str(args)))

    action = args.get('action', None)
    name = args.get('name', None)

    if action == 'update':
        stats_update = StatsUpdate()
        stats_update.perform_update(name)
    elif action == 'see':
        request = Database().get_request(name)
        print(json.dumps(request, indent=4))
    elif action == 'drop':
        Database().clear_database()


if __name__ == '__main__':
    main()
