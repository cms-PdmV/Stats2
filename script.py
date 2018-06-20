import couchdb_database
import time


if __name__ == '__main__':
    couch_db = couchdb_database.Database()

    page = 0
    requests = [{}]
    while len(requests) > 0:
        requests = couch_db.get_requests(page=page, page_size=1000, include_docs=True)
        print('Page ' + str(page))
        page += 1
        for req in requests:
            if 'EventNumberHistory' not in req and req['_id'] != '_design/_designDoc':
                print('id: %s. Last update: %s' % (req['_id'], time.strftime('%Y-%m-%d %H:%M', time.localtime(req['LastUpdate']))))
                # print('id: %s' % (req['_id']))
