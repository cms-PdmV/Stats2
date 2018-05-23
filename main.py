from flask import Flask, render_template, request, redirect, make_response
from flask_restful import Api
from database import Database
from utils import setup_console_logging, make_simple_request
from stats_update import StatsUpdate
import json
import re
import logging


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
def index(page=1):
    database = Database()
    prepid = request.args.get('prepid')
    dataset = request.args.get('dataset')
    campaign = request.args.get('campaign')
    request_name = request.args.get('request_name')
    check = request.args.get('check')
    if page < 1:
        page = 1

    if request_name is not None:
        req = database.get_request(request_name)
        pages = [-1, 1, -1]
        if req is not None:
            requests = [req]
            total = 1
        else:
            requests = []
            total = 0

    else:
        if prepid is not None:
            requests, left, total = database.query_requests({'PrepID': prepid}, page - 1)
        elif dataset is not None:
            requests, left, total = database.query_requests({'OutputDatasets': re.compile(dataset, re.IGNORECASE)}, page - 1)
        elif campaign is not None:
            requests, left, total = database.query_requests({'Campaigns': re.compile(campaign, re.IGNORECASE)}, page - 1)
        else:
            requests, left, total = database.query_requests(page=page - 1)

        pages = [page - 1, page, page + 1 if left > 0 else -1]

    if check is not None:
        check_with_old_stats(requests)

    for req in requests:
        req['DonePercent'] = '0.00'
        req['OpenPercent'] = '0.00'
        req['LastDatasetType'] = 'NONE'
        req['LastDataset'] = ''
        if req['TotalEvents'] > 0 and len(req['OutputDatasets']) > 0 and len(req['EventNumberHistory']) > 0:
            last_dataset = req['OutputDatasets'][-1:][0]
            last_history = req['EventNumberHistory'][-1:][0]
            if last_dataset not in last_history['Datasets']:
                continue

            calculated_dataset = last_history['Datasets'][last_dataset]
            done_events = calculated_dataset['Events']
            dataset_type = calculated_dataset['Type']
            total_events = req['TotalEvents']
            req['DonePercent'] = '%.2f' % (done_events / total_events * 100.0)
            req['LastDatasetType'] = dataset_type
            req['LastDataset'] = last_dataset

    return render_template('index.html',
                           requests=requests,
                           pages=pages,
                           total_requests=total,
                           query=request.query_string.decode('utf-8'))


@app.route('/update/<string:request_name>')
def update(request_name):
    StatsUpdate().perform_update(request_name=request_name)
    return redirect("/1?request_name=" + request_name, code=302)


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


@app.route('/count_campaign/<string:campaign_name>')
def count_campaign(campaign_name):
    database = Database()
    requests, _, total = database.query_requests({'Campaigns': re.compile(campaign_name, re.IGNORECASE),
                                                  'RequestType': {'$ne': 'Resubmission'}},
                                                 page=0,
                                                 page_size=1000000)
    total_expected_events = 0
    total_done_events = 0
    total_open_events = 0
    sums = dict()
    for req in requests:
        prepid = req.get('PrepID')
        if prepid is not None:
            if prepid in sums:
                raise Exception('%s already exist' % (prepid))
            else:
                total_events = req.get('TotalEvents')
                if len(req['EventNumberHistory']) == 0:
                    continue

                last_dataset = req['EventNumberHistory'][-1:][0]['Datasets']
                if len(last_dataset) == 0:
                    continue

                last_dataset = last_dataset[req['OutputDatasets'][-1:][0]]
                if last_dataset['Type'].lower() == 'valid':
                    done_events = last_dataset['Events']
                    open_events = 0
                else:
                    done_events = 0
                    open_events = last_dataset['Events']

                sums[prepid] = {'total_events': total_events,
                                'open_events': open_events,
                                'done_events': done_events}
        else:
            raise Exception('Prepid is NONE')

    for prepid, req_dict in sums.items():
        total_expected_events += req_dict['total_events']
        total_done_events += req_dict['done_events']
        total_open_events += req_dict['open_events']

    response = make_response(json.dumps({'workflows': len(requests),
                                         'prepids': len(sums),
                                         'total_events': total_expected_events,
                                         'open_events': total_open_events,
                                         'done_events': total_done_events,
                                         'total_in_das': total_done_events + total_open_events}), 200)
    response.headers['Content-Type'] = 'application/json'
    return response


@app.route('/update_campaign/<string:campaign_name>')
def update_campaign(campaign_name):
    database = Database()
    requests, _, total = database.query_requests({'Campaigns': re.compile(campaign_name, re.IGNORECASE)},
                                                 page=0,
                                                 page_size=1000000)

    updater = StatsUpdate()
    logger = logging.getLogger('logger')
    for index, req in enumerate(requests):
        updater.perform_update(request_name=req['_id'])
        logger.info('Updated %d/%d' % (index, len(requests)))

    return redirect("/1?campaign=" + campaign_name, code=302)


def run_flask():
    setup_console_logging()
    app.run(host='0.0.0.0',
            port=443,
            debug=True,
            threaded=True,
            ssl_context=('/home/jrumsevi/localhost.crt', '/home/jrumsevi/localhost.key'))


if __name__ == '__main__':
    run_flask()
