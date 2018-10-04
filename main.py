from flask import Flask, render_template, request, make_response
from flask_restful import Api
from couchdb_database import Database
from utils import setup_file_logging, make_simple_request
import json
import time


app = Flask(__name__,
            static_folder="./html/static",
            template_folder="./html")
api = Api(app)


def check_with_old_stats(requests):
    """
    Delete this if Stats2 is used as prod service
    """
    for req in requests:
        stats_url = "http://vocms074:5984/stats/%s" % req['_id']
        try:
            stats_req = make_simple_request(stats_url)
            req['OldCompletion'] = '%.2f' % (float(stats_req['pdmv_completion_in_DAS']))
            if stats_req['pdmv_expected_events'] == req['TotalEvents']:
                req['TotalEventsEqual'] = 'equal'
            else:
                req['TotalEventsEqual'] = 'not_equal'
                req['TotalEventsStats'] = stats_req['pdmv_expected_events']
        except:
            req['TotalEventsEqual'] = 'not_found'


@app.route('/')
@app.route('/<int:page>')
def index(page=0):
    database = Database()
    prepid = request.args.get('prepid')
    dataset = request.args.get('dataset')
    campaign = request.args.get('campaign')
    request_type = request.args.get('type')
    request_name = request.args.get('request_name')
    check = request.args.get('check')
    if page < 0:
        page = 0

    if request_name is not None:
        req = database.get_request(request_name)
        if req is not None:
            requests = [req]
        else:
            requests = []

    else:
        if prepid is not None:
            requests = database.get_requests_with_prepid(prepid, page=page, include_docs=True)
        elif dataset is not None:
            requests = database.get_requests_with_dataset(dataset, page=page, include_docs=True)
        elif campaign is not None:
            requests = database.get_requests_with_campaign(campaign, page=page, include_docs=True)
        elif request_type is not None:
            requests = database.get_requests_with_type(request_type, page=page, include_docs=True)
        else:
            requests = database.get_requests(page=page, include_docs=True)

    if check is not None:
        check_with_old_stats(requests)

    pages = [page, page > 0, database.PAGE_SIZE == len(requests)]
    requests = list(filter(lambda req: '_design' not in req['_id'], requests))
    for req in requests:
        req['DonePercent'] = '0.00'
        req['OpenPercent'] = '0.00'
        req['LastDatasetType'] = 'NONE'
        req['LastDataset'] = ''
        req['DoneEvents'] = '0'
        req['LastUpdate'] = time.strftime('%Y&#8209;%m&#8209;%d&nbsp;%H:%M:%S', time.localtime(req['LastUpdate']))

        if len(req['OutputDatasets']) == 0:
            continue

        if len(req['EventNumberHistory']) == 0:
            continue

        last_dataset = req['OutputDatasets'][-1:][0]
        last_history = req['EventNumberHistory'][-1:][0]
        if last_dataset not in last_history['Datasets']:
            continue

        calculated_dataset = last_history['Datasets'][last_dataset]
        dataset_type = calculated_dataset['Type']
        req['LastDatasetType'] = dataset_type
        req['LastDataset'] = last_dataset
        done_events = calculated_dataset['Events']
        req['DoneEvents'] = done_events
        if 'TotalEvents' not in req:
            continue

        if req['TotalEvents'] > 0:
            total_events = req['TotalEvents']
            req['DonePercent'] = '%.2f' % (done_events / total_events * 100.0)

    return render_template('index.html',
                           requests=requests,
                           total_requests=database.get_request_count(),
                           pages=pages,
                           query=request.query_string.decode('utf-8'))


@app.route('/get/<string:request_name>')
def get_one(request_name):
    database = Database()
    request = database.get_request(request_name)
    if request is None:
        response = make_response("{}", 404)
    else:
        response = make_response(json.dumps(request), 200)

    response.headers['Content-Type'] = 'application/json'
    return response


@app.route('/view_json/<string:request_name>')
def get_nice_json(request_name):
    database = Database()
    request = database.get_request(request_name)
    if request is None:
        response = make_response("{}", 404)
    else:
        response = make_response(json.dumps(request, indent=4, sort_keys=True), 200)

    response.headers['Content-Type'] = 'application/json'
    return response


def run_flask():
    setup_file_logging()
    app.run(host='0.0.0.0',
            port=443,
            debug=True,
            threaded=True,
            ssl_context=('/home/jrumsevi/localhost.crt', '/home/jrumsevi/localhost.key'))


if __name__ == '__main__':
    run_flask()
