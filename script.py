import couchdb_database
import database
# import json


if __name__ == '__main__':
    couch_db = couchdb_database.Database()
    mongo_db = database.Database()

    page = 0
    requests = [{}]
    while len(requests) > 0:
        requests, _, _ = mongo_db.query_requests(None, page=page, page_size=1000)
        print('Page ' + str(page))
        page += 1
        for req in requests:
            try:
                # print(json.dumps(req, indent=2))
                print(req['_id'])
                couch_db.insert_request(req)
            except Exception as ex:
                print('Couch exception ' + str(ex))
