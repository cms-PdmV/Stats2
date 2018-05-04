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
        stats_url = "http://vocms074:5984/stats/%s" % req['_id']
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


def get_jenkins_rss_feed():
    import feedparser
    f = feedparser.parse('http://instance3:8080/job/Stats2Update/rssAll')
    html = 'Last updates:<ul style="font-size: 0.75em">'
    for e in f['entries'][:5]:
        html += '<li><a href="%s">%s</a></li>' % (e.get('link', ''), e.get('title', ''))

    html += '</ul>'
    return html


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
            requests = database.query_requests({'PrepID': prepid}, page)
        elif dataset is not None:
            requests = database.query_requests({'OutputDatasets': dataset}, page)
        elif campaign is not None:
            requests = database.query_requests({'Campaign': campaign}, page)
        else:
            requests = database.query_requests(page=page)

    if check is not None:
        check_with_old_stats(requests)

    return render_template('index.html',
                           requests=requests,
                           page=page,
                           total_requests=database.get_request_count(),
                           query=request.query_string.decode('utf-8'),
                           rss=get_jenkins_rss_feed())


@app.route('/update/<string:request_name>')
def update(request_name):
    StatsUpdate().perform_update(request_name=request_name)
    return redirect("/0?request_name=" + request_name, code=302)


@app.route('/missing')
def missing():
    missing_requests = []
    database = Database()
    all_docs_url = 'http://vocms074:5984/stats/_all_docs'
    all_docs = make_simple_request(all_docs_url)['rows']
    for stats_doc in all_docs:
        if database.get_request(stats_doc['id']) is None:
            missing_requests.append(stats_doc['id'])

    response = make_response(json.dumps({'missing_requests_in_stats2': missing_requests}, indent=4), 200)
    response.headers['Content-Type'] = 'application/json'
    return response


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
