from flask import Flask, render_template, request, redirect, make_response
from flask_restful import Api
from database import Database
from utils import setup_console_logging, make_simple_request
from stats_update import StatsUpdate
import json

app = Flask(__name__)
api = Api(app)


def check_with_old_stats(requests):
    """
    Delete this if Stats2 is used as prod service
    """
    for req in requests:
        stats_url = "http://vocms074:5984/stats/%s" % req['RequestName']
        try:
            stats_req = make_simple_request(stats_url)
            req['OldCompletion'] = stats_req['pdmv_completion_in_DAS']
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
    request_name = request.args.get('request_name')
    check = request.args.get('check')

    if request_name is not None:
        requests = [database.get_request(request_name)]
    else:
        if prepid is not None:
            requests = database.query({'PrepID': prepid}, page)
        elif dataset is not None:
            requests = database.query({'OutputDatasets': dataset}, page)
        elif campaign is not None:
            requests = database.query({'Campaign': campaign}, page)
        else:
            requests = database.query(page=page)

    if check is not None:
        check_with_old_stats(requests)

    return render_template('index.html',
                           requests=requests,
                           page=page,
                           total_requests=database.get_count_of_requests(),
                           requests_without_history=database.get_count_of_requests_without_history(),
                           query=request.query_string.decode('utf-8'))


@app.route('/update/<string:request_name>')
def update(request_name):
    StatsUpdate().perform_update(request_name=request_name)
    return redirect("/0?request_name=" + request_name, code=302)


@app.route('/get/<string:request_name>')
def get_one(request_name):
    database = Database()
    request = database.get_request(request_name)
    if request is None:
        response = make_response({}, 404)
    else:
        response = make_response(json.dumps(request), 200)

    response.headers['Content-Type'] = 'application/json'
    return response


@app.route('/view_json/<string:request_name>')
def get_nice_json(request_name):
    database = Database()
    request = database.get_request(request_name)
    if request is None:
        response = make_response({}, 404)
    else:
        response = make_response(json.dumps(request, indent=4), 200)

    response.headers['Content-Type'] = 'application/json'
    return response


def run_flask():
    setup_console_logging()
    app.run(host='0.0.0.0',
            port=5000,
            debug=True,
            threaded=True)


if __name__ == '__main__':
    run_flask()
